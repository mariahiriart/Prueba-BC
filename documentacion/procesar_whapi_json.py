import json, sys, re, os
import openpyxl
from datetime import datetime, timedelta
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

BO_PHONES = {'5215568660814', '5215528551646', '5215530313942', '5215580510043'}

def load_rutas_from_xlsx(fname):
    """Carga RUTAS_CSV y BOP_TO_RUTA desde el xlsx de Jacob."""
    if not os.path.exists(fname):
        return {}, {}
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
    bop_to_ruta = {bop: ruta for ruta, bops in rutas.items() for bop in bops}
    return rutas, bop_to_ruta

MEXICO_OFFSET = -6 * 3600  # CST = UTC-6

def fmt_hour(ts):
    return datetime.utcfromtimestamp(ts + MEXICO_OFFSET).strftime('[%H:%M]')

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
    bop = bops[0]
    tc = re.sub(r'\s+', ' ', text.strip())
    m = re.search(r'(?:estatus|status)[:\s]*([^\n]+)', tc, re.I)
    status = ''
    if m:
        status = m.group(1).strip()
        status = re.split(r'observaciones?|obs\.?', status, flags=re.I)[0].strip().rstrip('|').strip()
    m2 = re.search(r'(?:observaciones?|obs\.?)[:\s]*(.+)', tc, re.I)
    obs = m2.group(1).strip() if m2 else ''
    m3 = re.search(r'(?:punto)[:\s]*(\w+)', tc, re.I)
    punto = m3.group(1) if m3 else '?'
    m4 = re.search(r'(?:ruta)[:\s]*(\w+)', tc, re.I)
    ruta = f'RUTA {m4.group(1)}' if m4 else None
    return {'bop': bop, 'punto': punto, 'status': status, 'obs': obs, 'ruta': ruta}

def parse_bo(text):
    if not text:
        return None
    bops = extract_bop(text)
    if not bops:
        return None
    bop = bops[0]
    m = re.search(r'Estatus[:\*\s]+\*?([^\n\*]+)\*?', text, re.I)
    bo_status = m.group(1).strip().strip('*').strip() if m else 'N/A'
    m2 = re.search(r'(?:Motivo|Comentario)[:\s]*\n(.+?)(?:\n\n|Instrucciones|$)', text, re.I | re.DOTALL)
    bo_obs = re.sub(r'\*', '', m2.group(1)).strip() if m2 else ''
    return {'bop': bop, 'bo_status': bo_status, 'bo_obs': bo_obs}

def is_exitoso(s):
    return any(x in (s or '').lower() for x in ['exit', 'entregado', 'entrega', 'exito', 'ok'])

def procesar_dia(msgs, fecha_str, rutas_csv=None, bop_to_ruta=None):
    rutas_csv    = rutas_csv    or {}
    bop_to_ruta  = bop_to_ruta or {}
    bop_reports  = {}
    bo_responses = {}
    loc_by_phone   = {}   # phone -> [(ts, lat, lon)]
    media_by_phone = {}   # phone -> [(ts, media_item)]

    for m in msgs:
        phone  = m.get('from', '')
        nombre = m.get('from_name') or phone
        ts     = m.get('timestamp', 0)
        mtype  = m.get('type', '')

        # Ubicación
        if mtype == 'location':
            loc = m.get('location', {})
            lat = loc.get('latitude') or loc.get('lat')
            lon = loc.get('longitude') or loc.get('lng')
            if lat and lon:
                lat, lon = float(lat), float(lon)
                loc_by_phone.setdefault(phone, []).append((ts, lat, lon))
                item = {
                    'type':    'location',
                    'preview': loc.get('preview', ''),
                    'url':     f'https://maps.google.com/?q={lat},{lon}',
                    'lat':     lat,
                    'lon':     lon,
                }
                media_by_phone.setdefault(phone, []).append((ts, item))
            continue

        # Imagen
        if mtype == 'image':
            img = m.get('image') or {}
            preview = img.get('preview', '')
            caption = img.get('caption', '') or ''
            media_item = {
                'type':    'image',
                'preview': preview,
                'id':      img.get('id', ''),
                'caption': caption,
            }
            media_by_phone.setdefault(phone, []).append((ts, media_item))
            # Seguir procesando caption como texto de reporte si aplica
            if not caption:
                continue
            text = caption
        elif mtype == 'text':
            text = (m.get('text') or {}).get('body', '')
            if not text:
                continue
        else:
            continue

        hora = fmt_hour(ts)

        # BO
        if phone in BO_PHONES or is_bo_fmt(text):
            r = parse_bo(text)
            if r:
                bop = r['bop']
                if bop not in bo_responses:
                    bo_responses[bop] = {'bo_status': r['bo_status'], 'bo_obs': r['bo_obs'], 'msgs': []}
                bo_responses[bop]['msgs'].append(f'{hora} {nombre}: {text[:80]}')
            continue

        is_drv_fmt = bool(
            re.search(r'ID\s*BOP|ID\s*Bop', text, re.I) and
            re.search(r'estatus|status', text, re.I)
        )
        if not is_drv_fmt:
            continue

        r = parse_driver(text)
        if r:
            bop = r['bop']
            if bop not in bop_reports:
                ruta_real = bop_to_ruta.get(bop) or r['ruta'] or '?'
                bop_reports[bop] = {
                    'phone': phone, 'nombre': nombre,
                    'ruta': ruta_real,
                    'punto': r['punto'], 'status': r['status'],
                    'obs': r['obs'], 'ts': ts, 'msgs': [], 'imgs': 0
                }
            bop_reports[bop]['msgs'].append(f'{hora} {nombre}: {text[:100]}')
            if mtype == 'image':
                bop_reports[bop]['imgs'] += 1

    def get_media_items(phone, ts, window=45):
        """Devuelve todos los items multimedia del driver en ventana de ±window minutos."""
        items = []
        seen_urls = set()
        for mt, item in media_by_phone.get(phone, []):
            if abs(mt - ts) / 60 <= window:
                # Deduplicar por URL/id
                key = item.get('url') or item.get('id') or str(mt)
                if key not in seen_urls:
                    seen_urls.add(key)
                    items.append(item)
        return items

    detalle = []
    all_bops = set()

    for bop, rep in bop_reports.items():
        bo = bo_responses.get(bop, {})
        exitoso = is_exitoso(rep['status'])
        media = get_media_items(rep['phone'], rep['ts'])
        detalle.append({
            'bop': bop, 'driver': rep['nombre'], 'ruta': rep['ruta'],
            'status_final': 'Exito' if exitoso else 'Fallido / Incidencia',
            'evidencias': rep['imgs'],
            'media': media,
            'driver_status': rep['status'] or 'sin estatus',
            'driver_obs': rep['obs'],
            'bo_status': bo.get('bo_status', 'N/A'),
            'bo_obs': bo.get('bo_obs', ''),
            'raw_drv_msgs': rep['msgs'],
            'raw_bo_msgs': bo.get('msgs', []),
            'ultima_hora': fmt_hour(rep['ts']) if rep['ts'] else '',
        })
        all_bops.add(bop)

    for bop, bo in bo_responses.items():
        if bop in all_bops:
            continue
        detalle.append({
            'bop': bop, 'driver': 'Solo BO', 'ruta': '?',
            'status_final': 'Fallido / Incidencia', 'evidencias': 0, 'media': [],
            'driver_status': 'N/A', 'driver_obs': '',
            'bo_status': bo.get('bo_status', 'N/A'), 'bo_obs': bo.get('bo_obs', ''),
            'raw_drv_msgs': [], 'raw_bo_msgs': bo.get('msgs', []),
            'ultima_hora': '',
        })
        all_bops.add(bop)

    detalle.sort(key=lambda x: (x['ruta'], x['bop']))

    total    = len(detalle)
    exitosos = sum(1 for d in detalle if d['status_final'] == 'Exito')
    con_bo   = sum(1 for d in detalle if d['bo_status'] != 'N/A')

    # Resumen por operador
    por_operador = {}
    for d in detalle:
        drv = d['driver']
        if drv == 'Solo BO':
            continue
        if drv not in por_operador:
            por_operador[drv] = {'exitos': 0, 'fallidos': 0, 'ruta': d['ruta']}
        if d['status_final'] == 'Exito':
            por_operador[drv]['exitos'] += 1
        else:
            por_operador[drv]['fallidos'] += 1

    # Resumen por ruta cruzado con xlsx
    rutas_out = []
    for ruta, bops_asignados in sorted(rutas_csv.items(), key=lambda x: int(x[0].split()[-1])):
        reportados = [b for b in bops_asignados if b in all_bops]
        faltantes  = [b for b in bops_asignados if b not in all_bops]
        driver_nombre = next((d['driver'] for d in detalle if d['ruta'] == ruta and d['driver'] != 'Solo BO'), '?')
        rutas_out.append({
            'ruta': ruta, 'driver': driver_nombre,
            'total_asignado':  len(bops_asignados),
            'total_reportado': len(reportados),
            'total_faltante':  len(faltantes),
            'faltantes_bops':  faltantes
        })

    output = {
        'kpis': {
            'total_asignados': sum(r['total_asignado']  for r in rutas_out) or total,
            'total_reportados': sum(r['total_reportado'] for r in rutas_out),
            'total_sin_reporte': sum(r['total_faltante'] for r in rutas_out),
            'total_bops':      total,
            'exitosos':        exitosos,
            'fallidos':        total - exitosos,
            'con_bo':          con_bo,
            'pct_exito':       round(exitosos / total * 100, 1) if total else 0
        },
        'rutas':              rutas_out,
        'por_operador':       por_operador,
        'detalle_reportados': detalle,
        'generado_at':        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }

    fecha_dir = fecha_str.replace('-', '_')
    out_dir = f'dashboard_{fecha_dir}'
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, 'data.js')
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write('const dashboardData = ' + json.dumps(output, ensure_ascii=False, indent=2, default=str) + ';\n')

    imgs_total = sum(len(d['media']) for d in detalle)
    print(f'=== {fecha_str} ===')
    print(f'  BOPs driver: {len(bop_reports)} | BOPs BO: {len(bo_responses)} | Total: {total}')
    print(f'  Exitosos: {exitosos} | Fallidos: {total-exitosos} | % Exito: {output["kpis"]["pct_exito"]}%')
    print(f'  Items multimedia asociados: {imgs_total}')
    print(f'  Guardado: {out_path}')
    print()
    return output


XLSX_MAP = {
    '2026-03-20': 'rutas_20_mzo.xlsx',
    '2026-03-21': 'rutas_21_mzo.xlsx',
    '2026-03-23': 'rutas_23_mzo.xlsx',
}

if __name__ == '__main__':
    import sys
    dias = sys.argv[1:] if len(sys.argv) > 1 else ['2026-03-20', '2026-03-21']
    for dia in dias:
        d = dia.split('-')[2]
        fname_json = f'msgs_dia{d}.json'
        if not os.path.exists(fname_json):
            print(f'No existe {fname_json}, saltando.')
            continue
        with open(fname_json, encoding='utf-8') as f:
            msgs = json.load(f)
        xlsx = XLSX_MAP.get(dia)
        rutas_csv, bop_to_ruta = load_rutas_from_xlsx(xlsx) if xlsx else ({}, {})
        procesar_dia(msgs, dia, rutas_csv, bop_to_ruta)
