import psycopg2

DB_CFG = dict(
    host="ep-royal-fog-amdxgnd5-pooler.c-5.us-east-1.aws.neon.tech",
    port=5432, database="neondb",
    user="neondb_owner", password="npg_MtVIvY13mjGw", sslmode="require"
)

def inspect_schema():
    conn = psycopg2.connect(**DB_CFG)
    cur = conn.cursor()
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='raw_messages'")
    cols = [r[0] for r in cur.fetchall()]
    print(f"Columns in raw_messages: {cols}")
    cur.close(); conn.close()

if __name__ == "__main__":
    inspect_schema()
