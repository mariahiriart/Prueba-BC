import psycopg2
from psycopg2.extras import RealDictCursor

DB_CFG = dict(
    host="ep-royal-fog-amdxgnd5-pooler.c-5.us-east-1.aws.neon.tech",
    port=5432, database="neondb",
    user="neondb_owner", password="npg_MtVIvY13mjGw", sslmode="require"
)

def verify_latest():
    conn = psycopg2.connect(**DB_CFG)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    print("--- ULTIMOS MENSAJES RAW ---")
    cur.execute("SELECT id, sender_phone, source, ingested_at FROM raw_messages WHERE source = 'api_webhook_v1.1' ORDER BY ingested_at DESC LIMIT 1")
    res = cur.fetchone()
    if res:
        print(res)
        msg_id = res['id']
        
        print("\n--- ULTIMO REPORTE DE CONDUCTOR ---")
        cur.execute("SELECT * FROM driver_reports WHERE raw_message_id = %s", (msg_id,))
        rep = cur.fetchone()
        if rep:
            print(rep)
        else:
            print(f"No hay reporte para el mensaje {msg_id}")
    else:
        print("No hay mensajes raw.")

    cur.close(); conn.close()

if __name__ == "__main__":
    verify_latest()
