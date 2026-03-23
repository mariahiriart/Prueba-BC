import psycopg2

DB_CFG = dict(
    host="ep-royal-fog-amdxgnd5-pooler.c-5.us-east-1.aws.neon.tech",
    port=5432, database="neondb",
    user="neondb_owner", password="npg_MtVIvY13mjGw", sslmode="require"
)

def inspect_constraints():
    conn = psycopg2.connect(**DB_CFG)
    cur = conn.cursor()
    cur.execute("""
        SELECT conname, pg_get_constraintdef(c.oid)
        FROM pg_constraint c
        JOIN pg_namespace n ON n.oid = c.connamespace
        WHERE conrelid = 'raw_messages'::regclass
    """)
    for row in cur.fetchall():
        print(f"Constraint: {row[0]:30} | Def: {row[1]}")
    cur.close(); conn.close()

if __name__ == "__main__":
    inspect_constraints()
