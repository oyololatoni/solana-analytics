import os
import json
import subprocess
import sys
import psycopg

# Load DB URL
db_url = "postgresql://neondb_owner:npg_YtcqIl6J0ogf@ep-odd-dew-agyg7avg.c-2.eu-central-1.aws.neon.tech/neondb?sslmode=require"

def ingest():
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT address FROM tokens WHERE discovery_class = 'NEW_LISTING_CALIBRATION' AND eligibility_status = 'PRE_ELIGIBLE';")
            mints = [row[0] for row in cur.fetchall()]
    
    print(f"Found {len(mints)} tokens to ingest.")
    for mint in mints:
        print(f"Ingesting {mint}...")
        subprocess.run([sys.executable, "scripts/ingest_token.py", mint], check=False)

if __name__ == "__main__":
    ingest()
