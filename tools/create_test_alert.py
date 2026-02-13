
import os
import sys
import psycopg
from pathlib import Path

# Add project root needed for config
sys.path.insert(0, str(Path(__file__).parent.parent))
from app.core.config import DATABASE_URL, TRACKED_TOKENS

def create_test_alert():
    mint = list(TRACKED_TOKENS)[0]
    print(f"Creating test alert for {mint}...")
    
    conn = psycopg.connect(DATABASE_URL)
    with conn.cursor() as cur:
        # Check if exists first to avoid dupes not needed
        cur.execute("""
            INSERT INTO alerts (token_mint, metric, condition, value, cooldown_minutes)
            VALUES (%s, 'swap_count_1h', 'gt', 0, 1)
            RETURNING id
        """, (mint,))
        alert_id = cur.fetchone()
        print(f"Created Alert ID: {alert_id[0]}")
        conn.commit()
    conn.close()

if __name__ == "__main__":
    create_test_alert()
