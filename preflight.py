# preflight.py
import os
import sys

print("[PREFLIGHT] starting")

# ---- required env vars ----
required = [
    "DATABASE_URL",
    "HELIUS_WEBHOOK_SECRET",
    "TRACKED_TOKENS",
]

missing = [k for k in required if not os.environ.get(k)]
if missing:
    print("[PREFLIGHT][FAIL] missing env vars:", missing)
    sys.exit(1)

print("[PREFLIGHT] env vars OK")

# ---- import check ----
try:
    import api.main
except Exception as e:
    print("[PREFLIGHT][FAIL] import api.main failed")
    print(e)
    sys.exit(1)

print("[PREFLIGHT] api.main import OK")

# ---- database check ----
try:
    import psycopg
    conn = psycopg.connect(os.environ["DATABASE_URL"], connect_timeout=5)
    conn.close()
except Exception as e:
    print("[PREFLIGHT][FAIL] database connection failed")
    print(e)
    sys.exit(1)

print("[PREFLIGHT] database connection OK")
print("[PREFLIGHT] SUCCESS")

