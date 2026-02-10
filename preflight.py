# preflight.py
import sys
from config import DATABASE_URL, HELIUS_WEBHOOK_SECRET, TRACKED_TOKENS

print("[PREFLIGHT] starting")

# ---- sanity checks ----

# 1. Credentials exist
if not DATABASE_URL or "***" in DATABASE_URL:
    print("[PREFLIGHT][FAIL] DATABASE_URL missing or redacted")
    sys.exit(1)

if not HELIUS_WEBHOOK_SECRET:
    print("[PREFLIGHT][FAIL] HELIUS_WEBHOOK_SECRET missing")
    sys.exit(1)

if not TRACKED_TOKENS:
    print("[PREFLIGHT][FAIL] TRACKED_TOKENS missing or empty")
    sys.exit(1)

print("[PREFLIGHT] config values present")

# 2. Critical Imports
try:
    import api.main
except Exception as e:
    print("[PREFLIGHT][FAIL] import api.main failed")
    print(e)
    sys.exit(1)

print("[PREFLIGHT] api.main import OK")

# 3. Database Connectivity
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

