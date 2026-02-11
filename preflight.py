# preflight.py
import os
import sys
try:
    import psycopg
    conn = psycopg.connect(DATABASE_URL, connect_timeout=5)
    conn.close()
except Exception as e:
    print("[PREFLIGHT][FAIL] database connection failed")
    print(e)
    sys.exit(1)

print("[PREFLIGHT] database connection OK")
print("[PREFLIGHT] SUCCESS")

