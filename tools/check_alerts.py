
import os
import sys
import psycopg
from pathlib import Path

# Add project root needed for config
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DATABASE_URL

def check():
    print("Checking alerts table...")
    try:
        conn = psycopg.connect(DATABASE_URL)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM alerts")
            count = cur.fetchone()[0]
            print(f"Alerts count: {count}")
            
            if count > 0:
                cur.execute("SELECT * FROM alerts LIMIT 1")
                print(f"Sample: {cur.fetchone()}")
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check()
