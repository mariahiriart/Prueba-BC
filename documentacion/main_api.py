from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
import uvicorn, json, re, os, threading, urllib.request, time as _time
from datetime import datetime
import openpyxl

# ── CONFIGURACIÓN ──────────────────────────────────────────────────────────────
TOKEN     = os.environ.get('WHAPI_TOKEN', '')
CHAT_ID   = '120363349733984596@g.us'   # Seguimiento JCR (grupo principal)
JACOB_PHONE   = '5215625585843'
ROBERTO_PHONE = '5215586931845'
XLSX_CHAT_ID  = '120363349579190170@g.us'
ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY', '')

AUTHORIZED_CHAT_IDS = [
    '120363349733984596@g.us',
    '120363419653209546@g.us',
    '120363423645957323@g.us',
    '120363349579190170@g.us',
    '120363400981379542@g.us',
    '120363380129878437@g.us',
]

BO_PHONES = {'5215568660814', '5215528551646', '5215530313942', '5215580510043'}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ── ESTADO EN MEMORIA ──────────────────────────────────────────────────────────
state_lock        = threading.Lock()
bop_reports       = {}   # bop   -> dict con status, media, msgs, etc.
bo_responses      = {}   # bop   -> dict con bo_status, msgs
last_bop_by_phone = {}   # phone -> bop  (último BOP reportado por ese driver)
rutas_csv         = {}   # ruta  -> [bops]
bop_to_ruta       = {}   # bop   -> ruta
driver_names      = {}   # ruta  -> nombre real del driver (de imagen de Roberto)
today_str         = ''

# ── HELPERS DE PARSEO ──────────────────────────────────────────────────────────
def extract_bop(text):
    if not text:
        return []
    labeled = re.findall(r'(?:ID\s*BOP|IdBop|ID\s*Bop)[:\s\*\[#\s]*(\d{7})', text, re.I)
    if labeled:
        return labeled
    return re.findall(r'\b(\d{7})\b', text)

def is_bo_fmt(text):
    return bool(re.search(r'IdBop|🧾', text or '', re.I))

def parse_driver(text):
    if not text:
        return None
    bops = extract_bop(text)
    if not bops:
        return None
    tc   = re.sub(r'\s+', ' ', text.strip())
    bop  = bops[0]
    m    = re.search(r'(?:estatus|status)[:\s]*([^\n]+)', tc, re.I)
    status = ''
    if m:
        status = m.group(1).strip()
        status = re.split(r'observaciones?|obs\.?', status, flags=re.I)[0].strip().rstrip('|').strip()
    m2   = re.search(r'(?:observaciones?|obs\.?)[:\s]*(.+)', tc, re.I)
    obs  = m2.group(1).strip() if m2 else ''
    m3   = re.search(r'(?:punto)[:\s]*(\w+)', tc, re.I)
    m4   = re.search(r'(?:ruta)[:\s]*(\w+)', tc, re.I)
    return {
        'bop':    bop,
        'punto':  m3.group(1) if m3 else '?',
        'status': status,
        'obs':    obs,
        'ruta':   f'RUTA {m4.group(1)}' if m4 else None,
    }

def parse_bo(text):
    if not text:
        return None
    bops = extract_bop(text)
    if not bops:
        return None
    bop = bops[0]
    m   = re.search(r'Estatus[:\*\s]+\*?([^\n\*]+)\*?', text, re.I)
    bo_status = m.group(1).strip().strip('*').strip() if m else 'N/A'
    m2  = re.search(r'(?:Motivo|Comentario)[:\s]*\n(.+?)(?:\n\n|Instrucciones|$)', text, re.I | re.DOTALL)
    bo_obs = re.sub(r'\*', '', m2.group(1)).strip() if m2 else ''
    return {'bop': bop, 'bo_status': bo_status, 'bo_obs': bo_obs}

def is_exitoso(s):
    return any(x in (s or '').lower() for x in ['exit', 'entregado', 'entrega', 'exito', 'ok'])

MEXICO_OFFSET = -6 * 3600  # CST = UTC-6

def mexico_now():
    """Hora actual en México usando time.time() que siempre es UTC puro."""
    return datetime.utcfromtimestamp(_time.time() + MEXICO_OFFSET)

def fmt_hour(ts):
    """Convierte timestamp POSIX a hora de México."""
    return datetime.utcfromtimestamp(ts + MEXICO_OFFSET).strftime('[%H:%M]')

# ── CARGAR XLSX DEL DÍA ────────────────────────────────────────────────────────
def load_xlsx(fecha_str):
    global rutas_csv, bop_to_ruta
    d = fecha_str.split('-')[2]
    fname = os.path.join(BASE_DIR, f'rutas_{d}_mzo.xlsx')
    if not os.path.exists(fname):
        print(f'[API] xlsx no encontrado: {fname}')
        return
    wb = openpyxl.load_workbook(fname)
    ws = wb.active
    rutas = {}
    for row in ws.iter_rows(min_row=2, values_only=True):
        vehiculo = row[1]
        bop      = str(row[5]).strip() if row[5] else None
        if not vehiculo or not bop or len(bop) != 7:
            continue
        ruta = vehiculo.strip()
        rutas.setdefault(ruta, [])
        if bop not in rutas[ruta]:
            rutas[ruta].append(bop)
    with state_lock:
        rutas_csv   = rutas
        bop_to_ruta = {b: r for r, bops in rutas.items() for b in bops}
    total = sum(len(v) for v in rutas.values())
    print(f'[API] Rutas cargadas: {len(rutas)} rutas, {total} BOPs desde {fname}')

# ── ASIGNACIÓN DRIVER↔RUTA DESDE IMAGEN DE ROBERTO ────────────────────────────
def _descargar_media(media_id):
    """Descarga un archivo de media de Whapi y retorna los bytes."""
    url = f'https://gate.whapi.cloud/media/{media_id}'
    headers = {'Authorization': f'Bearer {TOKEN}', 'User-Agent': 'Mozilla/5.0'}
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()

def procesar_imagen_asignacion(img_id):
    """Descarga la imagen de asignación de Roberto y extrae driver→ruta via Claude Vision."""
    global driver_names
    try:
        img_data = _descargar_media(img_id)
    except Exception as e:
        print(f'[ASIG] Error descargando imagen {img_id}: {e}', flush=True)
        return

    # Guardar copia local
    img_path = os.path.join(BASE_DIR, 'asignacion_hoy.jpg')
    with open(img_path, 'wb') as f:
        f.write(img_data)
    print(f'[ASIG] Imagen guardada: {img_path} ({len(img_data)} bytes)', flush=True)

    if not ANTHROPIC_API_KEY:
        print('[ASIG] Sin ANTHROPIC_API_KEY — imagen guardada pero no procesada.', flush=True)
        return

    import anthropic, base64 as _b64
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    img_b64 = _b64.standard_b64encode(img_data).decode('utf-8')

    try:
        resp = client.messages.create(
            model='claude-haiku-4-5-20251001',
            max_tokens=600,
            messages=[{
                'role': 'user',
                'content': [
                    {'type': 'image', 'source': {
                        'type': 'base64', 'media_type': 'image/jpeg', 'data': img_b64
                    }},
                    {'type': 'text', 'text':
                        'Esta tabla tiene columnas "Nombre" (numero de ruta/operador) y "Nombre Completo" (nombre real del driver). '
                        'Extrae cada fila como JSON array: [{"ruta": N, "nombre": "Nombre Completo"}]. '
                        'Solo el JSON array, sin texto ni markdown extra.'}
                ]
            }]
        )
        raw = resp.content[0].text.strip()
        # Limpiar posible markdown
        raw = re.sub(r'^```[a-z]*\n?', '', raw).rstrip('`').strip()
        assignments = json.loads(raw)
        new_names = {f'RUTA {a["ruta"]}': a['nombre'] for a in assignments if 'ruta' in a and 'nombre' in a}
        with state_lock:
            driver_names.update(new_names)
        # Persistir en disco
        dn_path = os.path.join(BASE_DIR, 'driver_names.json')
        with open(dn_path, 'w', encoding='utf-8') as f:
            json.dump(driver_names, f, ensure_ascii=False, indent=2)
        print(f'[ASIG] Asignaciones cargadas: {new_names}', flush=True)
    except Exception as e:
        print(f'[ASIG] Error procesando con Claude Vision: {e}', flush=True)

def cargar_driver_names_desde_disco():
    """Carga driver_names desde archivo JSON si existe."""
    global driver_names
    dn_path = os.path.join(BASE_DIR, 'driver_names.json')
    if os.path.exists(dn_path):
        try:
            with open(dn_path, encoding='utf-8') as f:
                driver_names = json.load(f)
            print(f'[ASIG] Driver names cargados desde disco: {len(driver_names)} rutas', flush=True)
        except Exception as e:
            print(f'[ASIG] Error leyendo driver_names.json: {e}', flush=True)

# ── DESCARGA AUTOMÁTICA DE XLSX DESDE JACOB ────────────────────────────────────
def descargar_xlsx_de_jacob(doc_id, filename):
    """Descarga el xlsx que manda Jacob y recarga rutas si es para hoy."""
    m = re.search(r'para\s+(\d+)\s+MZO', filename, re.I)
    if not m:
        print(f'[XLSX] No se pudo extraer día del nombre: {filename}', flush=True)
        return
    day = int(m.group(1))
    out_name = f'rutas_{day:02d}_mzo.xlsx'
    out_path = os.path.join(BASE_DIR, out_name)

    try:
        data = _descargar_media(doc_id)
        with open(out_path, 'wb') as f:
            f.write(data)
        print(f'[XLSX] Descargado: {out_name} ({len(data)} bytes)', flush=True)
        hoy = mexico_now()
        if day == hoy.day:
            load_xlsx(hoy.strftime('%Y-%m-%d'))
            print(f'[XLSX] Rutas recargadas para hoy (dia {day})', flush=True)
        else:
            print(f'[XLSX] Guardado para el dia {day}, se usara en el arranque de ese dia', flush=True)
    except Exception as e:
        print(f'[XLSX] Error descargando {doc_id}: {e}', flush=True)

# ── REGENERAR data.js ──────────────────────────────────────────────────────────
def regenerar_dashboard():
    with state_lock:
        reps  = dict(bop_reports)
        bos   = dict(bo_responses)
        rutas = dict(rutas_csv)
        b2r   = dict(bop_to_ruta)

    detalle   = []
    all_bops  = set()

    for bop, rep in reps.items():
        bo      = bos.get(bop, {})
        exitoso = is_exitoso(rep['status'])
        detalle.append({
            'bop':           bop,
            'driver':        rep['nombre'],
            'ruta':          b2r.get(bop) or rep['ruta'] or '?',
            'status_final':  'Exito' if exitoso else 'Fallido / Incidencia',
            'evidencias':    rep.get('imgs', 0),
            'media':         rep.get('media', []),
            'driver_status': rep['status'] or 'sin estatus',
            'driver_obs':    rep['obs'],
            'bo_status':     bo.get('bo_status', 'N/A'),
            'bo_obs':        bo.get('bo_obs', ''),
            'raw_drv_msgs':  rep.get('msgs', []),
            'raw_bo_msgs':   bo.get('msgs', []),
            'ultima_hora':   rep['hora'],
        })
        all_bops.add(bop)

    for bop, bo in bos.items():
        if bop in all_bops:
            continue
        detalle.append({
            'bop': bop, 'driver': 'Solo BO',
            'ruta': b2r.get(bop, '?'),
            'status_final': 'Fallido / Incidencia',
            'evidencias': 0,
            'driver_status': 'N/A', 'driver_obs': '',
            'bo_status':    bo.get('bo_status', 'N/A'),
            'bo_obs':       bo.get('bo_obs', ''),
            'raw_drv_msgs': [], 'raw_bo_msgs': bo.get('msgs', []),
            'ultima_hora':  bo.get('hora', ''),
        })
        all_bops.add(bop)

    detalle.sort(key=lambda x: (x['ruta'], x['bop']))

    total    = len(detalle)
    exitosos = sum(1 for d in detalle if d['status_final'] == 'Exito')
    con_bo   = sum(1 for d in detalle if d['bo_status'] != 'N/A')

    # Resumen por ruta (cruzado con xlsx)
    with state_lock:
        dn = dict(driver_names)
    rutas_out = []
    for ruta, bops_asig in sorted(rutas.items(), key=lambda x: int(x[0].split()[-1])):
        reportados = [b for b in bops_asig if b in all_bops]
        faltantes  = [b for b in bops_asig if b not in all_bops]
        # Prioridad: nombre real de imagen > nombre de reporte WhatsApp > ?
        driver = dn.get(ruta) or next(
            (d['driver'] for d in detalle if d['ruta'] == ruta and d['driver'] != 'Solo BO'), '?'
        )
        rutas_out.append({
            'ruta': ruta, 'driver': driver,
            'total_asignado':  len(bops_asig),
            'total_reportado': len(reportados),
            'total_faltante':  len(faltantes),
            'faltantes_bops':  faltantes,
        })

    output = {
        'generado_at':  mexico_now().strftime('%Y-%m-%d %H:%M:%S'),
        'kpis': {
            'total_asignados': sum(r['total_asignado']  for r in rutas_out) or total,
            'total_reportados': sum(r['total_reportado'] for r in rutas_out),
            'total_sin_reporte': sum(r['total_faltante'] for r in rutas_out),
            'exitosos':   exitosos,
            'fallidos':   total - exitosos,
            'con_bo':     con_bo,
            'pct_exito':  round(exitosos / total * 100, 1) if total else 0,
        },
        'rutas':              rutas_out,
        'detalle_reportados': detalle,
    }

    out_dir = os.path.join(BASE_DIR, f'dashboard_{today_str.replace("-","_")}')
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, 'data.js')
    tmp_path = out_path + '.tmp'
    with open(tmp_path, 'w', encoding='utf-8') as f:
        f.write('const dashboardData = ' + json.dumps(output, ensure_ascii=False, indent=2, default=str) + ';\n')
    os.replace(tmp_path, out_path)
    return output['kpis'], len(detalle)

# ── PROCESAR UN MENSAJE ────────────────────────────────────────────────────────
def _add_media_to_bop(bop, item):
    """Agrega un item de media al BOP correspondiente (thread-safe, llamar con lock)."""
    if bop and bop in bop_reports:
        bop_reports[bop].setdefault('media', []).append(item)
        if item['type'] == 'image':
            bop_reports[bop]['imgs'] = bop_reports[bop].get('imgs', 0) + 1

def procesar_mensaje(msg):
    phone  = msg.get('from', '')
    nombre = msg.get('from_name') or phone
    ts     = msg.get('timestamp', 0)
    mtype  = msg.get('type', '')
    hora   = fmt_hour(ts) if ts else ''

    # ── Ubicación ──────────────────────────────────────────────────────────────
    if mtype == 'location':
        loc = msg.get('location', {})
        lat = loc.get('latitude') or loc.get('lat')
        lon = loc.get('longitude') or loc.get('lng')
        if lat and lon:
            item = {
                'type':    'location',
                'preview': loc.get('preview', ''),
                'url':     f'https://maps.google.com/?q={float(lat)},{float(lon)}',
                'lat':     float(lat), 'lon': float(lon),
            }
            with state_lock:
                last_bop = last_bop_by_phone.get(phone)
                _add_media_to_bop(last_bop, item)
                print(f'[RT] LOC phone={phone} -> BOP={last_bop}', flush=True)
        return

    # ── Imagen ─────────────────────────────────────────────────────────────────
    if mtype == 'image':
        img = msg.get('image') or {}
        caption = img.get('caption', '') or ''
        item = {
            'type':    'image',
            'preview': img.get('preview', ''),
            'id':      img.get('id', ''),
            'caption': caption,
        }
        with state_lock:
            last_bop = last_bop_by_phone.get(phone)
            _add_media_to_bop(last_bop, item)
            print(f'[RT] IMG phone={phone} -> BOP={last_bop} caption={caption[:30]}', flush=True)
        # Seguir procesando si la caption tiene un reporte
        if not caption:
            return
        text = caption
    elif mtype == 'text':
        text = (msg.get('text') or {}).get('body', '')
        if not text:
            return
    else:
        return

    # ── Mensajes del BO ────────────────────────────────────────────────────────
    if phone in BO_PHONES or is_bo_fmt(text):
        r = parse_bo(text)
        if r:
            bop = r['bop']
            with state_lock:
                if bop not in bo_responses:
                    bo_responses[bop] = {'bo_status': r['bo_status'], 'bo_obs': r['bo_obs'],
                                         'msgs': [], 'hora': hora}
                else:
                    bo_responses[bop]['bo_status'] = r['bo_status']
                    bo_responses[bop]['bo_obs']     = r['bo_obs']
                    bo_responses[bop]['hora']        = hora
                bo_responses[bop]['msgs'].append(f'{hora} {nombre}: {text[:80]}')
            print(f'[RT] BO  BOP={bop} status={r["bo_status"]}', flush=True)
        return

    # ── Reporte del driver ─────────────────────────────────────────────────────
    is_drv = bool(
        re.search(r'ID\s*BOP|ID\s*Bop', text, re.I) and
        re.search(r'estatus|status', text, re.I)
    )
    if not is_drv:
        return

    r = parse_driver(text)
    if r:
        bop = r['bop']
        with state_lock:
            ruta_real = bop_to_ruta.get(bop) or r['ruta'] or '?'
            if bop not in bop_reports:
                bop_reports[bop] = {
                    'phone': phone, 'nombre': nombre, 'ruta': ruta_real,
                    'punto': r['punto'], 'status': r['status'], 'obs': r['obs'],
                    'ts': ts, 'hora': hora, 'msgs': [], 'imgs': 0, 'media': [],
                }
            else:
                bop_reports[bop]['status'] = r['status']
                bop_reports[bop]['obs']    = r['obs']
                bop_reports[bop]['hora']   = hora
            bop_reports[bop]['msgs'].append(f'{hora} {nombre}: {text[:100]}')
            # Registrar como último BOP de este driver
            last_bop_by_phone[phone] = bop
        print(f'[RT] DRV BOP={bop} status={r["status"]} ruta={ruta_real}', flush=True)

# ── INICIALIZACIÓN: cargar mensajes del día desde Whapi ───────────────────────
def init_today():
    global today_str
    today_str = mexico_now().strftime('%Y-%m-%d')
    print(f'[API] Inicializando día {today_str}...', flush=True)

    load_xlsx(today_str)
    cargar_driver_names_desde_disco()

    # Descargar mensajes de hoy ya existentes desde Whapi
    d_start = int(datetime(int(today_str[:4]), int(today_str[5:7]), int(today_str[8:10]), 0, 0, 0).timestamp())
    d_end   = int(datetime(int(today_str[:4]), int(today_str[5:7]), int(today_str[8:10]), 23, 59, 59).timestamp())

    headers = {
        'Authorization': f'Bearer {TOKEN}',
        'Accept':        'application/json',
        'User-Agent':    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    }
    count    = 0
    offset   = 0
    oldest   = 9999999999
    msgs_hoy = []

    print(f'[API] Descargando mensajes previos de hoy...', flush=True)
    while oldest > d_start:
        url = f'https://gate.whapi.cloud/messages/list/{CHAT_ID}?count=100&offset={offset}'
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=15) as r:
                data = json.loads(r.read())
        except Exception as e:
            print(f'[API] Error descargando histórico: {e}', flush=True)
            break

        msgs = data.get('messages', [])
        if not msgs:
            break

        for m in msgs:
            ts = m.get('timestamp', 0)
            if d_start <= ts <= d_end:
                msgs_hoy.append(m)
            oldest = min(oldest, ts)

        if len(msgs) < 100:
            break
        offset += 100

    print(f'[API] {len(msgs_hoy)} mensajes de hoy cargados. Procesando...', flush=True)
    for m in msgs_hoy:
        procesar_mensaje(m)

    kpis, total = regenerar_dashboard()
    print(f'[API] Estado inicial: {total} BOPs | exitosos={kpis["exitosos"]} fallidos={kpis["fallidos"]}', flush=True)
    print(f'[API] Dashboard listo: dashboard_{today_str.replace("-","_")}/data.js', flush=True)

# ── FASTAPI APP ────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app):
    # Arrancar inicialización en hilo separado para no bloquear
    t = threading.Thread(target=init_today, daemon=True)
    t.start()
    yield

app = FastAPI(title='Silent Listener RT - Brightcell/JCR v2.0', lifespan=lifespan)

# Servir dashboards (hoy en vivo + históricos)
_dash_dir = os.path.join(BASE_DIR, f'dashboard_{mexico_now().strftime("%Y_%m_%d")}')
os.makedirs(_dash_dir, exist_ok=True)
app.mount('/dashboard',   StaticFiles(directory=_dash_dir, html=True), name='dashboard')

for _d, _slug in [('dashboard_2026_03_20', 'dashboard20'), ('dashboard_2026_03_21', 'dashboard21')]:
    _p = os.path.join(BASE_DIR, _d)
    if os.path.isdir(_p):
        app.mount(f'/{_slug}', StaticFiles(directory=_p, html=True), name=_slug)

@app.get('/')
def health():
    with state_lock:
        total    = len(bop_reports) + len(set(bo_responses) - set(bop_reports))
        exitosos = sum(1 for r in bop_reports.values() if is_exitoso(r['status']))
    return {
        'status':    'online',
        'version':   '2.0',
        'fecha':     today_str,
        'bops_vivos': total,
        'exitosos':  exitosos,
    }

@app.get('/status')
def get_status():
    kpis, total = regenerar_dashboard()
    return {'ok': True, 'kpis': kpis, 'total_bops': total}

@app.get('/api/data')
def api_data():
    """Devuelve el estado actual en memoria como JSON (sin leer disco)."""
    with state_lock:
        reps  = dict(bop_reports)
        bos   = dict(bo_responses)
        rutas = dict(rutas_csv)
        b2r   = dict(bop_to_ruta)

    detalle  = []
    all_bops = set()

    for bop, rep in reps.items():
        bo = bos.get(bop, {})
        exitoso = is_exitoso(rep['status'])
        media = rep.get('media', [])   # media asociada directamente al BOP
        detalle.append({
            'bop': bop, 'driver': rep['nombre'],
            'ruta': b2r.get(bop) or rep['ruta'] or '?',
            'status_final': 'Exito' if exitoso else 'Fallido / Incidencia',
            'evidencias': rep['imgs'],
            'media': media,
            'driver_status': rep['status'] or 'sin estatus',
            'driver_obs': rep['obs'],
            'bo_status': bo.get('bo_status', 'N/A'),
            'bo_obs': bo.get('bo_obs', ''),
            'ultima_hora': rep['hora'],
        })
        all_bops.add(bop)

    for bop, bo in bos.items():
        if bop in all_bops:
            continue
        detalle.append({
            'bop': bop, 'driver': 'Solo BO',
            'ruta': b2r.get(bop, '?'),
            'status_final': 'Fallido / Incidencia',
            'evidencias': 0, 'media': [],
            'driver_status': 'N/A', 'driver_obs': '',
            'bo_status': bo.get('bo_status', 'N/A'),
            'bo_obs': bo.get('bo_obs', ''),
            'ultima_hora': bo.get('hora', ''),
        })
        all_bops.add(bop)

    detalle.sort(key=lambda x: (x['ruta'], x['bop']))
    total    = len(detalle)
    exitosos = sum(1 for d in detalle if d['status_final'] == 'Exito')
    con_bo   = sum(1 for d in detalle if d['bo_status'] != 'N/A')

    with state_lock:
        dn = dict(driver_names)
    rutas_out = []
    for ruta, bops_asig in sorted(rutas.items(), key=lambda x: int(x[0].split()[-1])):
        reportados = [b for b in bops_asig if b in all_bops]
        faltantes  = [b for b in bops_asig if b not in all_bops]
        driver = dn.get(ruta) or next(
            (d['driver'] for d in detalle if d['ruta'] == ruta and d['driver'] != 'Solo BO'), '?'
        )
        rutas_out.append({
            'ruta': ruta, 'driver': driver,
            'total_asignado': len(bops_asig),
            'total_reportado': len(reportados),
            'total_faltante': len(faltantes),
            'faltantes_bops': faltantes,
        })

    return {
        'generado_at': mexico_now().strftime('%Y-%m-%d %H:%M:%S'),
        'kpis': {
            'total_asignados':   sum(r['total_asignado']  for r in rutas_out) or total,
            'total_reportados':  sum(r['total_reportado'] for r in rutas_out),
            'total_sin_reporte': sum(r['total_faltante']  for r in rutas_out),
            'exitosos':  exitosos,
            'fallidos':  total - exitosos,
            'con_bo':    con_bo,
            'pct_exito': round(exitosos / total * 100, 1) if total else 0,
        },
        'rutas': rutas_out,
        'detalle_reportados': detalle,
    }

@app.post('/webhook/whatsapp')
async def webhook(request: Request):
    try:
        data = await request.json()
    except Exception:
        return {'status': 'error', 'reason': 'invalid json'}

    messages = data.get('messages', [])
    if not messages and 'chat_id' in data:
        messages = [data]

    procesados = 0
    for msg in messages:
        chat_id = msg.get('chat_id', '')
        if chat_id not in AUTHORIZED_CHAT_IDS:
            continue
        # Procesar en hilo para no bloquear la respuesta HTTP
        t = threading.Thread(target=_procesar_y_actualizar, args=(msg,), daemon=True)
        t.start()
        procesados += 1

    return {'status': 'ok', 'procesados': procesados}

def _procesar_y_actualizar(msg):
    chat_id = msg.get('chat_id', '')

    # Documento xlsx de Jacob -> descarga y recarga rutas
    if msg.get('type') == 'document' and msg.get('from') == JACOB_PHONE:
        doc = msg.get('document') or {}
        fname = doc.get('filename', '')
        doc_id = doc.get('id', '')
        if fname.lower().endswith('.xlsx') and doc_id:
            print(f'[XLSX] Jacob envio: {fname} id={doc_id}', flush=True)
            descargar_xlsx_de_jacob(doc_id, fname)
        return

    # Imagen de Roberto en chat de coordinacion -> asignacion driver->ruta
    if (msg.get('type') == 'image' and msg.get('from') == ROBERTO_PHONE
            and chat_id == XLSX_CHAT_ID):
        img_id = (msg.get('image') or {}).get('id', '')
        if img_id:
            print(f'[ASIG] Roberto envio imagen de asignacion: {img_id}', flush=True)
            threading.Thread(target=procesar_imagen_asignacion, args=(img_id,), daemon=True).start()
        return

    procesar_mensaje(msg)
    try:
        kpis, total = regenerar_dashboard()
        ts  = msg.get('timestamp', 0)
        hora = fmt_hour(ts) if ts else ''
        nombre = msg.get('from_name') or msg.get('from', '?')
        print(f'[RT] {hora} {nombre} -> dashboard actualizado | BOPs={total} exit={kpis["exitosos"]} fall={kpis["fallidos"]}', flush=True)
    except Exception as e:
        print(f'[RT] Error regenerando dashboard: {e}', flush=True)

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
