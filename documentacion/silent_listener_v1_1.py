import os
import json
import re
from datetime import datetime
from database import save_raw_message, save_driver_report, save_bo_closure

# ── CONSTANTS ──────────────────────────────────────────────────────────────────
BO_PHONE = "5215568660814"

OPERADORES = {
    "5215532008563": {"nombre": "Guillermo Arteaga Campos",            "ruta": "RUTA 3"},
    "5217298931795": {"nombre": "Alejandro Vergara Tellez",            "ruta": "RUTA 6"},
    "5215531137339": {"nombre": "Jacqueline Zahireth Vázquez Alonso",  "ruta": "RUTA 8"},
    "5215541445114": {"nombre": "Juan Antonio Carmona Hernandez",      "ruta": "RUTA 9"},
    "5215612204197": {"nombre": "Aureliano Lopez Martinez",            "ruta": "RUTA 10"},
    "5215640050783": {"nombre": "Cristopher Robin Blanco Diaz",        "ruta": "RUTA 15"},
    "5215569348039": {"nombre": "Isai Castillo Cruz",                  "ruta": "RUTA 20"},
    "5215665336966": {"nombre": "Victor Paz Lara",                     "ruta": "RUTA 23"},
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
ALL_BOPS = {bop for bops in RUTAS_CSV.values() for bop in bops}

# ── PARSING HELPERS ────────────────────────────────────────────────────────────
def extract_bop_ids(text):
    if not text:
        return []
    # 1. Extracción por etiqueta: "ID BOP: 3824293" o "IdBop: [ #3817981 ]"
    labeled = re.findall(r'(?:ID\s*BOP|IdBop)[:\s\*\[#\s]*(\d{7})', text, re.I)
    if labeled:
        return labeled
    # 2. Fallback: 7 dígitos en lista conocida
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
    bo_obs = re.sub(r'\*', '', m2.group(1)).strip() if m2 else ""
    m3 = re.search(r'Instrucciones[:\s]*\n(.+?)(?:\n\n|$)', text, re.I | re.DOTALL)
    instrucciones = re.sub(r'\*', '', m3.group(1)).strip() if m3 else ""
    m4 = re.search(r'(?:codigo_intento|intento)[:\s]*([^\n]+)', text, re.I)
    codigo_intento = m4.group(1).strip() if m4 else ""
    return {"bop": bop, "bo_status": bo_status, "bo_obs": bo_obs,
            "instrucciones": instrucciones, "codigo_intento": codigo_intento}

# ── CONTENT EXTRACTION ─────────────────────────────────────────────────────────
def extract_content(msg_data):
    """Extract text content from message, handling text and image captions."""
    msg_type = msg_data.get('type', '')
    if msg_type == 'text':
        text = msg_data.get('text')
        if isinstance(text, dict):
            return text.get('body', '')
        return msg_data.get('content', '')
    elif msg_type == 'image':
        image = msg_data.get('image', {})
        if isinstance(image, dict):
            return image.get('caption', '')
    return ''

# ── CLASSIFICATION ─────────────────────────────────────────────────────────────
def classify_message(sender_phone, content):
    """Rule-based classification using sender phone and/or content format."""
    # BO: teléfono conocido O formato IdBop/🧾
    if sender_phone == BO_PHONE or is_bo_format(content):
        return "CIERRE_BO" if extract_bop_ids(content) else "NO_CLASIFICADO"
    # Driver: teléfono conocido O formato con "ID BOP" + "Estatus"
    is_driver_fmt = bool(
        re.search(r'ID\s*BOP', content, re.I) and
        re.search(r'estatus|status', content, re.I)
    )
    if sender_phone in OPERADORES or is_driver_fmt:
        return "REPORTE" if extract_bop_ids(content) else "INSTRUCCION"
    return "NO_CLASIFICADO"

# ── MAIN PROCESSOR ─────────────────────────────────────────────────────────────
def process_message(msg_data):
    """Main processing loop for incoming WhatsApp message."""
    print(f"[SilentListener] Processing message from chat {msg_data.get('chat_id')}", flush=True)
    try:
        # 1. Save RAW
        msg_id = save_raw_message(msg_data)
        if not msg_id:
            print("[SilentListener] ERROR saving RAW message.", flush=True)
            return

        print(f"[SilentListener] Message {msg_id} saved to RAW.", flush=True)

        sender_phone = msg_data.get('from', '')
        content = extract_content(msg_data)

        if not content:
            print(f"[SilentListener] Message {msg_id} has no text content, skipping classification.", flush=True)
            return

        # 2. Classify
        category = classify_message(sender_phone, content)
        print(f"[SilentListener] Msg {msg_id} from {sender_phone} → {category}", flush=True)

        # 3. Extract and Save
        if category == "REPORTE":
            parsed = parse_driver_report(content)
            if parsed:
                op_info = OPERADORES.get(sender_phone, {})
                save_driver_report(msg_id, {
                    "ruta":          op_info.get('ruta', '?'),
                    "punto":         parsed['punto'],
                    "id_bop":        parsed['bop'],
                    "estatus":       parsed['status_raw'],
                    "observaciones": parsed['obs'],
                })
                print(f"[SilentListener] REPORTE saved for BOP {parsed['bop']}", flush=True)
            else:
                print(f"[SilentListener] REPORTE: no se pudo parsear BOP/estatus del contenido.", flush=True)

        elif category == "CIERRE_BO":
            parsed = parse_bo_response(content)
            if parsed:
                save_bo_closure(msg_id, {
                    "id_bop":         parsed['bop'],
                    "codigo_cierre":  parsed['bo_status'],
                    "detalle":        parsed['bo_obs'],
                    "instrucciones":  parsed['instrucciones'],
                    "codigo_intento": parsed['codigo_intento'],
                })
                print(f"[SilentListener] CIERRE_BO saved for BOP {parsed['bop']}", flush=True)
            else:
                print(f"[SilentListener] CIERRE_BO: no se pudo parsear BOP del contenido.", flush=True)

    except Exception as e:
        print(f"[SilentListener] FATAL ERROR processing message: {e}", flush=True)
