import os
import psycopg

# Load DB URL
db_url = "postgresql://neondb_owner:npg_YtcqIl6J0ogf@ep-odd-dew-agyg7avg.c-2.eu-central-1.aws.neon.tech/neondb?sslmode=require"

def test():
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            # Test 1: Check existing value
            cur.execute("SELECT address, discovery_class FROM tokens WHERE address = 'coqRkaaKeUygDPhuS3mrmrj6DiHjeQJc2rFbT2YfxWn';")
            row = cur.fetchone()
            print(f"Current Row: {row}")
            
            # Test 2: Try update with CASE
            d_class = "TEST_OVERWRITE"
            mint = 'coqRkaaKeUygDPhuS3mrmrj6DiHjeQJc2rFbT2YfxWn'
            
            # First set to CALIBRATION for test
            cur.execute("UPDATE tokens SET discovery_class = 'NEW_LISTING_CALIBRATION' WHERE address = %s", (mint,))
            print("Set to CALIBRATION")
            
            cur.execute("""
                UPDATE tokens 
                SET discovery_class = CASE 
                    WHEN discovery_class LIKE 'NEW_LISTING%%' THEN discovery_class 
                    ELSE %s 
                END 
                WHERE address = %s
                RETURNING discovery_class
            """, (d_class, mint))
            new_val = cur.fetchone()
            print(f"Value after CASE update: {new_val[0]}")
        conn.commit()

if __name__ == "__main__":
    test()
