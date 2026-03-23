import psycopg2

DB_CFG = dict(
    host="ep-royal-fog-amdxgnd5-pooler.c-5.us-east-1.aws.neon.tech",
    port=5432, database="neondb",
    user="neondb_owner", password="npg_MtVIvY13mjGw", sslmode="require"
)

def inspect_lengths():
    conn = psycopg2.connect(**DB_CFG)
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name, character_maximum_length 
        FROM information_schema.columns 
        WHERE table_name='raw_messages'
    """)
    for row in cur.fetchall():
        print(f"Col: {row[0]:20} | MaxLength: {row[1]}")
    cur.close(); conn.close()

if __name__ == "__main__":
    inspect_lengths()
