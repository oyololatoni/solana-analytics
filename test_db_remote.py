# test_db_remote.py
import psycopg
import os

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

print("Connecting to Neon...")
conn = psycopg.connect(DATABASE_URL, connect_timeout=10)
print("Connected successfully")
conn.close()
