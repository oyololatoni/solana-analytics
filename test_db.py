import psycopg

print("Connecting...")
conn = psycopg.connect(
    "postgresql://postgres@localhost/postgres",
    connect_timeout=3
)
print("Connected!")
conn.close()

