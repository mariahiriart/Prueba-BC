import psycopg2

DB_CFG = dict(
    host="ep-royal-fog-amdxgnd5-pooler.c-5.us-east-1.aws.neon.tech",
    port=5432, database="neondb",
    user="neondb_owner", password="npg_MtVIvY13mjGw", sslmode="require"
)

def inspect_whatsapp_groups():
    conn = psycopg2.connect(**DB_CFG)
    cur = conn.cursor()
    cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name='whatsapp_groups'")
    print("Columns in whatsapp_groups:")
    for row in cur.fetchall():
        print(f"Col: {row[0]:20} | Type: {row[1]}")
    
    cur.execute("SELECT * FROM whatsapp_groups")
    print("\nData in whatsapp_groups:")
    for row in cur.fetchall():
        print(row)
    cur.close(); conn.close()

if __name__ == "__main__":
    inspect_whatsapp_groups()
