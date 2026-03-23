import psycopg2

DB_CFG = dict(
    host="ep-royal-fog-amdxgnd5-pooler.c-5.us-east-1.aws.neon.tech",
    port=5432, database="neondb",
    user="neondb_owner", password="npg_MtVIvY13mjGw", sslmode="require"
)

def list_tables():
    conn = psycopg2.connect(**DB_CFG)
    cur = conn.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    for row in cur.fetchall():
        print(row[0])
    cur.close(); conn.close()

if __name__ == "__main__":
    list_tables()
