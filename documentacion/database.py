import psycopg2
from psycopg2.extras import RealDictCursor
import json
import os
from datetime import datetime

# ── DATABASE CONFIGURATION (Neon PostgreSQL) ──────────────────────────────────
DB_CFG = dict(
    host=os.environ.get('DB_HOST', ''),
    port=int(os.environ.get('DB_PORT', 5432)),
    database=os.environ.get('DB_NAME', 'neondb'),
    user=os.environ.get('DB_USER', ''),
    password=os.environ.get('DB_PASSWORD', ''),
    sslmode="require"
)

# ── GROUP ID MAPPING (Chat ID String -> Group UUID) ─────────────────────────────
# Based on whatsapp_groups table inspection
GROUP_MAP = {
    "120363419653209546@g.us": "77d5a479-481a-4914-a529-042af31f946f", # Andrea Reyes
    "120363423645957323@g.us": "9c146e27-5d25-41e9-9892-669b91e08436", # B2B BCL (Assumption/Verify)
    "120363349733984596@g.us": "c3e38722-e3a1-435a-93be-16677f594514", # Seguimiento JCR
    "120363349579190170@g.us": "57ced152-a970-4f8f-8a48-9fa8c1f1537e", # Operacion JCR-Brightcell
    "120363400981379542@g.us": "2657cc8f-6ad6-4541-8bd5-6575fcb0d564", # Asistencia
    "120363380129878437@g.us": "a76447de-d3e2-4675-960b-07bee7092a2c", # Itzel Martinez
}

def get_connection():
    return psycopg2.connect(**DB_CFG)

def save_raw_message(msg_data):
    chat_id = msg_data.get('chat_id')
    group_uuid = GROUP_MAP.get(chat_id)
    
    if not group_uuid:
        # Fallback if ID is not mapped: try to find it dynamically
        print(f"[Database] Group UUID not found in cache for {chat_id}, trying dynamic lookup...")
        try:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute("SELECT id FROM whatsapp_groups WHERE chat_id = %s", (chat_id,))
            res = cur.fetchone()
            if res:
                group_uuid = res[0]
                GROUP_MAP[chat_id] = group_uuid
            cur.close(); conn.close()
        except Exception as e:
            print(f"[Database] Error in dynamic group lookup for {chat_id}: {e}")

    if not group_uuid:
        print(f"[Database] Critical: No group_id found for chat {chat_id}. Message cannot be saved due to NOT NULL constraint.")
        return None

    conn = get_connection()
    cur = conn.cursor()
    try:
        ts = msg_data.get('timestamp')
        sent_at = datetime.fromtimestamp(ts) if ts else datetime.now()
        cur.execute("""
            INSERT INTO raw_messages (
                sender_phone, content, raw_json, sent_at, whapi_type,
                group_id, source, message_type
            ) VALUES (%s, %s, %s, %s, %s, %s, 'api_webhook_v1.1', %s)
            RETURNING id
        """, (
            msg_data.get('from'),
            msg_data.get('text', {}).get('body') if isinstance(msg_data.get('text'), dict) else msg_data.get('content'),
            json.dumps(msg_data),
            sent_at,
            msg_data.get('type'),
            group_uuid,
            'unclassified'
        ))
        msg_id = cur.fetchone()[0]
        conn.commit()
        return msg_id
    except Exception as e:
        print(f"[Database] Error saving raw message: {e}")
        conn.rollback()
        return None
    finally:
        cur.close(); conn.close()

def save_driver_report(msg_id, data):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO driver_reports (
                raw_message_id, route_number, stop_number, idbop, status, observations, reported_at, parsed_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            msg_id, 
            str(data.get('ruta')), 
            int(data.get('punto')) if str(data.get('punto')).isdigit() else None, 
            str(data.get('id_bop')), 
            data.get('estatus'), 
            data.get('observaciones'),
            datetime.now(), # reported_at
            datetime.now()  # parsed_at
        ))
        conn.commit()
    except Exception as e:
        print(f"[Database] Error saving driver report: {e}")
        conn.rollback()
    finally:
        cur.close(); conn.close()

def save_bo_closure(msg_id, data):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO bo_closures (
                raw_message_id, idbop, status_code, comment_simpliroute, instruction_code, closed_at, parsed_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            msg_id, 
            str(data.get('id_bop')), 
            data.get('codigo_cierre'), 
            data.get('detalle'), 
            data.get('instrucciones'),
            datetime.now(), # closed_at
            datetime.now()  # parsed_at
        ))
        conn.commit()
    except Exception as e:
        print(f"[Database] Error saving BO closure: {e}")
        conn.rollback()
    finally:
        cur.close(); conn.close()
