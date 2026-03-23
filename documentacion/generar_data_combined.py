import psycopg2
import json, re, os
from datetime import datetime, timedelta

# ── CONFIG ─────────────────────────────────────────────────────────────────────
DB_CFG = dict(
    host=os.environ.get('DB_HOST', ''),
    port=int(os.environ.get('DB_PORT', 5432)),
    database=os.environ.get('DB_NAME', 'neondb'),
    user=os.environ.get('DB_USER', ''),
    password=os.environ.get('DB_PASSWORD', ''),
    sslmode="require"
)
BO_PHONE = "5215568660814"

OPERADORES = {
    "5215532008563": {"nombre": "Guillermo Arteaga Campos",            "ruta": "RUTA 3",  "tel": "+525532008563"},
    "5217298931795": {"nombre": "Alejandro Vergara Tellez",            "ruta": "RUTA 6",  "tel": "+527298931795"},
    "5215531137339": {"nombre": "Jacqueline Zahireth Vázquez Alonso",  "ruta": "RUTA 8",  "tel": "+525531137339"},
    "5215541445114": {"nombre": "Juan Antonio Carmona Hernandez",      "ruta": "RUTA 9",  "tel": "+525541445114"},
    "5215612204197": {"nombre": "Aureliano Lopez Martinez",            "ruta": "RUTA 10", "tel": "+525612204197"},
    "5215640050783": {"nombre": "Cristopher Robin Blanco Diaz",        "ruta": "RUTA 15", "tel": "+525640050783"},
    "5215569348039": {"nombre": "Isai Castillo Cruz",                  "ruta": "RUTA 20", "tel": "+525569348039"},
    "5215665336966": {"nombre": "Victor Paz Lara",                     "ruta": "RUTA 23", "tel": "+525665336966"},
}

RUTAS_CSV = {
    "RUTA 3":  ["3823462","3820184","3811164","3823451","3811157","3806343","3823442","3823448","3820190","3823471","3823470"],
    "RUTA 6":  ["3820725","3823459","3823414","3823423","3821655","3823422","3823433"],
    "RUTA 8":  ["3820943","3812719","3823331","3811154","3823438","3823439","3823432","3813406","3823330","3823434","3823428","3823435","3818083","3823425"],
    "RUTA 9":  ["3823430","3821650","3821719","3811140","3811176","3815709","3795939","3806371","3823457"],
    "RUTA 10": ["3821049","3823400","3821067","3823376","3820192","3823446","3823456","3823452","3820183","3820188","3814882","3823372","3823371","3823368","3823350","3816709","3823332"],
    "RUTA 15": ["3815705","3820187","3823440","3823454","3815703","3823450","3820193","3823443","3823447","3811138","3823465","3823458","3823460"],
    "RUTA 20": ["3812064","3823444","3811152","3814951","3823327","3819545","3823417","3823382","3812542","3823343","3814880","3814859","3823349","3817942","3823472"],
    "RUTA 23": ["3811150","3815713","3823463","3823441","3823449","3823464","3823466","3801295","3823461","3811155","3823455","3823453"],
}
BOP_TO_RUTA = {bop: ruta for ruta, bops in RUTAS_CSV.items() for bop in bops}
ALL_BOPS = set(BOP_TO_RUTA.keys())

def fmt_hour(dt):
    local = dt - timedelta(hours=6)
    return local.strftime("[%H:%M]")

def extract_bop_ids(text):
    if not text:
        return []
    candidates = re.findall(r'\b(\d{7})\b', text)
    return [c for c in candidates if c in ALL_BOPS]

def parse_driver_report(text):
    if not text:
        return None
    text_clean = re.sub(r'\s+', ' ', text.strip())
    bops = extract_bop_ids(text_clean)
    if not bops:
        return None
    bop = bops[0]
    m = re.search(r'(?:estatus|status)[:\s]*([^\n]+)', text_clean, re.I)
    status_raw = ""
    if m:
        status_raw = m.group(1).strip()
        status_raw = re.split(r'observaciones?|obs\.?', status_raw, flags=re.I)[0].strip().rstrip('|').strip()
    m2 = re.search(r'(?:observaciones?|obs\.?)[:\s]*(.+)', text_clean, re.I)
    obs = m2.group(1).strip() if m2 else ""
    m3 = re.search(r'(?:punto)[:\s]*(\w+)', text_clean, re.I)
    punto = m3.group(1) if m3 else "?"
    return {"bop": bop, "punto": punto, "status_raw": status_raw, "obs": obs}

def parse_bo_response(text):
    if not text:
        return None
    bops = extract_bop_ids(text)
    if not bops:
        return None
    bop = bops[0]
    m = re.search(r'Estatus[:\*\s]+\*?([^\n\*]+)\*?', text, re.I)
    bo_status = m.group(1).strip().strip('*').strip() if m else "N/A"
    m2 = re.search(r'(?:Motivo|Comentario)[:\s]*\n(.+?)(?:\n\n|Instrucciones|$)', text, re.I | re.DOTALL)
    bo_obs = ""
    if m2:
        bo_obs = re.sub(r'\*', '', m2.group(1)).strip()
    return {"bop": bop, "bo_status": bo_status, "bo_obs": bo_obs}

def is_exitoso(status_raw):
    s = (status_raw or "").lower()
    return any(x in s for x in ['exit', 'entregado', 'entrega', 'éxito', 'exito', 'ok'])

# ── QUERY ──────────────────────────────────────────────────────────────────────
conn = psycopg2.connect(**DB_CFG)
cur = conn.cursor()
cur.execute("""
    SELECT sender_phone, whapi_type, sent_at, content, raw_json
    FROM raw_messages
    WHERE sent_at::date = '2026-03-18'
      AND whapi_type IN ('text','image','location')
    ORDER BY sent_at ASC
""")
rows = cur.fetchall()
cur.close(); conn.close()

# ── PROCESAR ───────────────────────────────────────────────────────────────────
bop_reports  = {}
bo_responses = {}
loc_by_phone = {}
img_by_bop   = {}
drv_wa_name  = {}

for phone, wtype, sent_at, content, raw_json in rows:
    if not phone:
        continue
    from_name = raw_json.get('from_name') if raw_json else None
    if from_name and phone not in drv_wa_name:
        drv_wa_name[phone] = from_name

    if wtype == 'location':
        if phone not in OPERADORES:
            continue
        loc_data = raw_json.get('location', {}) if raw_json else {}
        lat = loc_data.get('latitude') or loc_data.get('lat')
        lon = loc_data.get('longitude') or loc_data.get('lng')
        if lat and lon:
            loc_by_phone.setdefault(phone, []).append((sent_at, float(lat), float(lon)))
        continue

    text_content = None
    if wtype == 'text':
        text_content = content
    elif wtype == 'image' and raw_json:
        text_content = (raw_json.get('image') or {}).get('caption', '')
    if not text_content:
        continue

    if phone == BO_PHONE:
        r = parse_bo_response(text_content)
        if r:
            bop = r['bop']
            label = f"{fmt_hour(sent_at)} +52 55 6866 0814: \"{text_content}\""
            if bop not in bo_responses:
                bo_responses[bop] = {'bo_status': r['bo_status'], 'bo_obs': r['bo_obs'], 'raw_texts': []}
            bo_responses[bop]['raw_texts'].append(label)
        continue

    if phone not in OPERADORES:
        continue

    wa_name = drv_wa_name.get(phone, OPERADORES[phone]['nombre'].split()[0])
    r = parse_driver_report(text_content)
    if r:
        bop = r['bop']
        label = f"{fmt_hour(sent_at)} {wa_name}: {text_content}"
        if bop not in bop_reports:
            bop_reports[bop] = {
                'phone': phone, 'from_name': wa_name,
                'punto': r['punto'], 'status_raw': r['status_raw'],
                'obs': r['obs'], 'sent_at': sent_at, 'raw_texts': []
            }
        bop_reports[bop]['raw_texts'].append(label)
        if wtype == 'image':
            img_by_bop[bop] = img_by_bop.get(bop, 0) + 1
    else:
        bops_in_text = extract_bop_ids(text_content)
        for bop in bops_in_text:
            if bop in bop_reports:
                bop_reports[bop]['raw_texts'].append(f"{fmt_hour(sent_at)} {wa_name}: {text_content}")

def get_location_urls(phone, report_time, window_minutes=45):
    locs = loc_by_phone.get(phone, [])
    urls = []
    for loc_time, lat, lon in locs:
        diff = abs((loc_time - report_time).total_seconds()) / 60
        if diff <= window_minutes:
            urls.append(f"https://maps.google.com/?q={lat},{lon}")
    return list(set(urls))

# ── DETALLE ────────────────────────────────────────────────────────────────────
detalle = []
all_reported_bops = set()

for bop, rep in bop_reports.items():
    phone = rep['phone']
    wa_name = rep['from_name']
    exitoso = is_exitoso(rep['status_raw'])
    status_final = "Éxito" if exitoso else "Fallido / Incidencia"
    bo = bo_responses.get(bop, {})
    bo_status = bo.get('bo_status', 'N/A') if bo else 'N/A'
    bo_obs = bo.get('bo_obs', 'N/A') if bo else 'N/A'
    raw_bo = bo.get('raw_texts', []) if bo else []
    evidencias = img_by_bop.get(bop, 0)
    urls = get_location_urls(phone, rep['sent_at'])

    detalle.append({
        "bop": bop,
        "driver": wa_name,
        "ruta": BOP_TO_RUTA.get(bop, "?"),
        "status_final": status_final,
        "evidencias": evidencias,
        "urls": urls,
        "driver_status": rep['status_raw'] or "sin estatus",
        "driver_obs": rep['obs'] or "",
        "bo_status": bo_status,
        "bo_obs": bo_obs,
        "raw_drv_msgs": rep['raw_texts'],
        "raw_bo_msgs": raw_bo
    })
    all_reported_bops.add(bop)

for bop, bo in bo_responses.items():
    if bop in all_reported_bops:
        continue
    detalle.append({
        "bop": bop,
        "driver": "Sólo BO",
        "ruta": BOP_TO_RUTA.get(bop, "?"),
        "status_final": "Fallido / Incidencia",
        "evidencias": 0,
        "urls": [],
        "driver_status": "N/A",
        "driver_obs": "N/A",
        "bo_status": bo.get('bo_status', 'N/A'),
        "bo_obs": bo.get('bo_obs', 'N/A'),
        "raw_drv_msgs": [],
        "raw_bo_msgs": bo.get('raw_texts', [])
    })
    all_reported_bops.add(bop)

detalle.sort(key=lambda x: (x['ruta'], x['bop']))

# ── RUTAS ──────────────────────────────────────────────────────────────────────
rutas_out = []
for ruta, bops_asignados in RUTAS_CSV.items():
    phone = next((p for p, o in OPERADORES.items() if o['ruta'] == ruta), None)
    op_info = OPERADORES.get(phone, {})
    reportados = [b for b in bops_asignados if b in all_reported_bops]
    faltantes  = [b for b in bops_asignados if b not in all_reported_bops]
    rutas_out.append({
        "ruta": ruta,
        "driver_nombre": op_info.get('nombre', '?'),
        "driver_tel": op_info.get('tel', '?'),
        "total_asignado": len(bops_asignados),
        "total_reportado": len(reportados),
        "total_faltante": len(faltantes),
        "faltantes_bops": faltantes
    })

total_asignados  = sum(r['total_asignado']  for r in rutas_out)
total_reportados = sum(r['total_reportado'] for r in rutas_out)
total_sin        = sum(r['total_faltante']  for r in rutas_out)

new_data = {
    "kpis": {
        "total_asignados": total_asignados,
        "total_reportados": total_reportados,
        "total_sin_reporte": total_sin
    },
    "rutas": rutas_out,
    "detalle_reportados": detalle
}

# ── INSERTAR EN data_combined.js ───────────────────────────────────────────────
combined_path = r"D:\Usuario\Desktop\documentacion\dashboard_18_marzo\data_combined.js"

with open(combined_path, "r", encoding="utf-8") as f:
    content = f.read()

# Verificar si ya existe la clave 2026-03-18 y eliminarla si existe
DATE_KEY = '"2026-03-18"'
if DATE_KEY in content:
    # Reemplazar entry existente: encontrar el bloque y removerlo
    # Buscar ", "2026-03-18": {" o '"2026-03-18": {' al inicio
    # Estrategia: reconstruir desde cero con python json
    # Extraer el JS y parsear
    print("Clave 2026-03-18 ya existía, se reemplazará")

# Quitar el "const DB = " y el ";" final para parsear el JSON
# El archivo tiene formato: const DB = { ... };
js_body = content.strip()
# Remover "const DB = " del inicio
js_body = re.sub(r'^const\s+DB\s*=\s*', '', js_body)
# Remover ";" del final
js_body = js_body.rstrip().rstrip(';').rstrip()

# Parsear el objeto JS como JSON (asumiendo que es JSON válido)
try:
    db_obj = json.loads(js_body)
except Exception as e:
    print(f"Error parseando data_combined.js: {e}")
    # Insertar manualmente al final
    db_obj = None

if db_obj is not None:
    db_obj["2026-03-18"] = new_data
    new_content = "const DB = " + json.dumps(db_obj, ensure_ascii=False, indent=2, default=str) + ";\n"
else:
    # Inserción manual: remover "};" del final y añadir nueva entrada
    # Encontrar la última "}" que cierra el DB
    last_close = content.rfind('};')
    if last_close != -1:
        new_entry = ',\n  "2026-03-18": ' + json.dumps(new_data, ensure_ascii=False, indent=2, default=str)
        new_content = content[:last_close] + new_entry + '\n};'
    else:
        print("ERROR: no se encontró el cierre '};'")
        exit(1)

with open(combined_path, "w", encoding="utf-8") as f:
    f.write(new_content)

print(f"\ndata_combined.js actualizado!")
print(f"  Total asignados:   {total_asignados}")
print(f"  Total reportados:  {total_reportados}")
print(f"  Total sin reporte: {total_sin}")
print(f"  BOPs en detalle:   {len(detalle)}")
print(f"\nDetalle por ruta:")
for r in rutas_out:
    bar = "#" * r['total_reportado'] + "." * r['total_faltante']
    print(f"  {r['ruta']:8s} [{bar:17s}] rep={r['total_reportado']:2d}/{r['total_asignado']:2d}  falt={r['total_faltante']:2d}")
