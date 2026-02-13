import os
import psycopg

# Load DB URL
db_url = "postgresql://neondb_owner:npg_YtcqIl6J0ogf@ep-odd-dew-agyg7avg.c-2.eu-central-1.aws.neon.tech/neondb?sslmode=require"

def check():
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'tokens' AND column_name = 'chain_id';")
            print(f"Schema: {cur.fetchone()}")
            cur.execute("SELECT address, chain_id FROM tokens LIMIT 1;")
            print(f"Sample: {cur.fetchone()}")

if __name__ == '__main__':
    check()
