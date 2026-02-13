import os
import json
import psycopg

# Load DB URL
db_url = "postgresql://neondb_owner:npg_YtcqIl6J0ogf@ep-odd-dew-agyg7avg.c-2.eu-central-1.aws.neon.tech/neondb?sslmode=require"

def fix():
    if not os.path.exists("candidate_tokens.json"):
        print("No candidate_tokens.json found.")
        # Try finding the 12 tokens by searching for those that were accepted earlier but mislabeled
        # Actually, let's just query for LOW_ACTIVITY tokens that have detected_at in the last hour
        query = "SELECT address FROM tokens WHERE discovery_class = 'LOW_ACTIVITY' AND detected_at > now() - interval '2 hours';"
    else:
        with open("candidate_tokens.json", "r") as f:
            candidates = json.load(f)
        if not candidates:
            # If candidate_tokens.json is empty because of a skip, we need to find them in the DB
             query = "SELECT address FROM tokens WHERE discovery_class = 'LOW_ACTIVITY' AND detected_at > now() - interval '2 hours';"
        else:
            mints = [c["mint"] for c in candidates]
            query = f"SELECT address FROM tokens WHERE address IN ({','.join(['%s']*len(mints))});"
            params = tuple(mints)

    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
             # Find candidates to fix
             if 'params' in locals():
                 cur.execute(query, params)
             else:
                 cur.execute(query)
             
             rows = cur.fetchall()
             print(f"Found {len(rows)} tokens to fix.")
             for row in rows:
                 addr = row[0]
                 cur.execute("UPDATE tokens SET discovery_class = 'NEW_LISTING_CALIBRATION' WHERE address = %s", (addr,))
                 print(f"Fixed: {addr}")
        conn.commit()

if __name__ == "__main__":
    fix()
