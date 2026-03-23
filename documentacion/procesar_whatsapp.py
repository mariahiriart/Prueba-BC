import psycopg2
import json, re, sys, os
from datetime import datetime, timedelta

# Uso: python procesar_whatsapp.py [YYYY-MM-DD]
# Si no se pasa fecha, usa el día de hoy.
QUERY_DATE = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y-%m-%d')

# ── CONFIGURACIÓN ──────────────────────────────────────────────────────────────
DB_CFG = dict(
    host=os.environ.get('DB_HOST', ''),
    port=int(os.environ.get('DB_PORT', 5432)),
    database=os.environ.get('DB_NAME', 'neondb'),
    user=os.environ.get('DB_USER', ''),
    password=os.environ.get('DB_PASSWORD', ''),
    sslmode="require"
)
BO_PHONE = "5215568660814"

# Mapeo teléfono WhatsApp → datos del operador
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

# BOPs asignados por ruta (del CSV Rutas_18_Marzo_Fijo.csv)
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

# ── HELPERS ────────────────────────────────────────────────────────────────────
def fmt_hour(dt):
    local = dt - timedelta(hours=6)
    return local.strftime("[%H:%M]")

def extract_bop_ids(text):
    if not text:
        return []
    # 1. Extracción por etiqueta: "ID BOP: 3824293" o "IdBop: [ #3817981 ]"
    labeled = re.findall(r'(?:ID\s*BOP|IdBop)[:\s\*\[#\s]*(\d{7})', text, re.I)
    if labeled:
        return labeled
    # 2. Fallback: cualquier número de 7 dígitos que esté en la lista conocida
    candidates = re.findall(r'\b(\d{7})\b', text)
    return [c for c in candidates if c in ALL_BOPS]

def is_bo_format(text):
    """Detecta mensajes de back-office por formato de contenido."""
    return bool(re.search(r'IdBop|🧾', text or '', re.I))

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
    # Extraer operador y ruta del texto si no están en el mapa de teléfonos
    m4 = re.search(r'(?:operador)[:\s]*(.+?)(?:\n|$)', text_clean, re.I)
    operador_txt = m4.group(1).strip() if m4 else None
    m5 = re.search(r'(?:ruta)[:\s]*(\w+)', text_clean, re.I)
    ruta_txt = f"RUTA {m5.group(1)}" if m5 else None
    return {"bop": bop, "punto": punto, "status_raw": status_raw, "obs": obs,
            "operador_txt": operador_txt, "ruta_txt": ruta_txt}

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

# ── QUERY DB ───────────────────────────────────────────────────────────────────
conn = psycopg2.connect(**DB_CFG)
cur = conn.cursor()
print(f"Procesando fecha: {QUERY_DATE}")
cur.execute("""
    SELECT sender_phone, whapi_type, sent_at, content, raw_json
    FROM raw_messages
    WHERE sent_at::date = %s
      AND whapi_type IN ('text','image','location')
    ORDER BY sent_at ASC
""", (QUERY_DATE,))
rows = cur.fetchall()
cur.close()
conn.close()
print(f"Mensajes cargados del DB: {len(rows)}")

# ── PROCESAR MENSAJES ──────────────────────────────────────────────────────────
bop_reports  = {}   # bop -> {phone, from_name, punto, status_raw, obs, sent_at, raw_texts}
bo_responses = {}   # bop -> {bo_status, bo_obs, raw_texts}
loc_by_phone = {}   # phone -> [(sent_at, lat, lon)]
img_by_bop   = {}   # bop -> int
drv_wa_name  = {}   # phone -> WA display name

for phone, wtype, sent_at, content, raw_json in rows:
    if not phone:
        continue

    from_name = raw_json.get('from_name') if raw_json else None
    if from_name and phone not in drv_wa_name:
        drv_wa_name[phone] = from_name

    # LOCATIONS
    if wtype == 'location':
        if phone not in OPERADORES:
            continue
        loc_data = raw_json.get('location', {}) if raw_json else {}
        lat = loc_data.get('latitude') or loc_data.get('lat')
        lon = loc_data.get('longitude') or loc_data.get('lng')
        if lat and lon:
            loc_by_phone.setdefault(phone, []).append((sent_at, float(lat), float(lon)))
        continue

    # Obtener texto
    text_content = None
    if wtype == 'text':
        text_content = content
    elif wtype == 'image' and raw_json:
        text_content = (raw_json.get('image') or {}).get('caption', '')

    if not text_content:
        continue

    # BO RESPONSE — detectar por teléfono conocido O por formato de contenido
    if phone == BO_PHONE or is_bo_format(text_content):
        r = parse_bo_response(text_content)
        if r:
            bop = r['bop']
            phone_label = phone or "BO"
            label = f"{fmt_hour(sent_at)} {phone_label}: \"{text_content}\""
            if bop not in bo_responses:
                bo_responses[bop] = {'bo_status': r['bo_status'], 'bo_obs': r['bo_obs'], 'raw_texts': []}
            bo_responses[bop]['raw_texts'].append(label)
        continue

    # DRIVER REPORT — teléfono conocido O formato de driver (tiene "ID BOP" + "Estatus")
    is_driver_format = bool(
        re.search(r'ID\s*BOP', text_content, re.I) and
        re.search(r'estatus|status', text_content, re.I)
    )
    if phone not in OPERADORES and not is_driver_format:
        continue

    wa_name = drv_wa_name.get(phone) or (OPERADORES[phone]['nombre'].split()[0] if phone in OPERADORES else phone)

    r = parse_driver_report(text_content)
    if r:
        bop = r['bop']
        # Nombre: WA name > texto del mensaje > teléfono
        display_name = wa_name or r.get('operador_txt') or phone
        # Ruta: mapa de teléfonos > texto del mensaje > "?"
        op_info = OPERADORES.get(phone, {})
        ruta_report = op_info.get('ruta') or r.get('ruta_txt') or BOP_TO_RUTA.get(bop, '?')
        label = f"{fmt_hour(sent_at)} {display_name}: {text_content}"
        if bop not in bop_reports:
            bop_reports[bop] = {
                'phone': phone, 'from_name': display_name,
                'ruta_override': ruta_report,
                'punto': r['punto'], 'status_raw': r['status_raw'],
                'obs': r['obs'], 'sent_at': sent_at, 'raw_texts': []
            }
        bop_reports[bop]['raw_texts'].append(label)
        if wtype == 'image':
            img_by_bop[bop] = img_by_bop.get(bop, 0) + 1
    else:
        # Mensaje sin BOP parseado pero puede tener BOP mencionado
        bops_in_text = extract_bop_ids(text_content)
        for bop in bops_in_text:
            if bop in bop_reports:
                wa_name = drv_wa_name.get(phone, phone)
                bop_reports[bop]['raw_texts'].append(f"{fmt_hour(sent_at)} {wa_name}: {text_content}")

print(f"BOPs con reporte driver:  {len(bop_reports)}")
print(f"BOPs con respuesta BO:    {len(bo_responses)}")

# ── FUNCIÓN UBICACIONES ────────────────────────────────────────────────────────
def get_location_urls(phone, report_time, window_minutes=45):
    locs = loc_by_phone.get(phone, [])
    urls = []
    for loc_time, lat, lon in locs:
        diff = abs((loc_time - report_time).total_seconds()) / 60
        if diff <= window_minutes:
            urls.append(f"https://maps.google.com/?q={lat},{lon}")
    return list(set(urls))

# ── CONSTRUIR detalle_reportados ───────────────────────────────────────────────
detalle = []
all_reported_bops = set()

for bop, rep in bop_reports.items():
    phone = rep['phone']
    op_info = OPERADORES.get(phone, {})
    wa_name = rep['from_name']
    exitoso = is_exitoso(rep['status_raw'])
    status_final = "Éxito" if exitoso else "Fallido / Incidencia"
    bo = bo_responses.get(bop, {})
    bo_status = bo.get('bo_status', 'N/A') if bo else 'N/A'
    bo_obs = bo.get('bo_obs', 'N/A') if bo else 'N/A'
    raw_bo = bo.get('raw_texts', []) if bo else []
    evidencias = img_by_bop.get(bop, 0)
    urls = get_location_urls(phone, rep['sent_at'])

    ruta_final = rep.get('ruta_override') or BOP_TO_RUTA.get(bop, "?")
    detalle.append({
        "bop": bop,
        "driver": wa_name,
        "ruta": ruta_final,
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

# BOPs con solo respuesta BO (sin mensaje del driver)
for bop, bo in bo_responses.items():
    if bop in all_reported_bops:
        continue
    ruta = BOP_TO_RUTA.get(bop, "?")
    detalle.append({
        "bop": bop,
        "driver": "Sólo BO",
        "ruta": ruta,
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

# ── CONSTRUIR rutas ────────────────────────────────────────────────────────────
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

# ── KPIs ───────────────────────────────────────────────────────────────────────
total_asignados  = sum(r['total_asignado']  for r in rutas_out)
total_reportados = sum(r['total_reportado'] for r in rutas_out)
total_sin        = sum(r['total_faltante']  for r in rutas_out)

print(f"\n=== RESULTADO FINAL ===")
print(f"Total asignados:   {total_asignados}")
print(f"Total reportados:  {total_reportados}")
print(f"Total sin reporte: {total_sin}")
print(f"BOPs en detalle:   {len(detalle)}")
print(f"\nDetalle por ruta:")
for r in rutas_out:
    bar = "#" * r['total_reportado'] + "." * r['total_faltante']
    print(f"  {r['ruta']:8s} [{bar:17s}] rep={r['total_reportado']:2d}/{r['total_asignado']:2d} | {r['driver_nombre']}")

# ── GENERAR data.js ────────────────────────────────────────────────────────────
output = {
    "kpis": {
        "total_asignados": total_asignados,
        "total_reportados": total_reportados,
        "total_sin_reporte": total_sin
    },
    "rutas": rutas_out,
    "detalle_reportados": detalle
}

js_content = "const dashboardData = " + json.dumps(output, ensure_ascii=False, indent=2, default=str) + ";\n"

dashboard_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"dashboard_{QUERY_DATE.replace('-', '_')}")
os.makedirs(dashboard_dir, exist_ok=True)
out_path = os.path.join(dashboard_dir, "data.js")
with open(out_path, "w", encoding="utf-8") as f:
    f.write(js_content)

print(f"\ndata.js actualizado: {out_path}")

# ── DEBUG: BOPs que no se pudieron parsear ──
parsed_bops = set(bop_reports.keys()) | set(bo_responses.keys())
missing_completely = [b for b in ALL_BOPS if b not in parsed_bops]
print(f"\nBOPs sin ningun mensaje encontrado ({len(missing_completely)}):")
for b in missing_completely[:20]:
    print(f"  {b} ({BOP_TO_RUTA[b]})")
