import os

# Database
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://neondb_owner:npg_YtcqIl6J0ogf@"
    "ep-odd-dew-agyg7avg.c-2.eu-central-1.aws.neon.tech/"
    "neondb?sslmode=require",
)

# Webhook auth
HELIUS_WEBHOOK_SECRET = os.environ.get("HELIUS_WEBHOOK_SECRET", "")

# Tokens we care about (comma-separated env var)
TRACKED_TOKENS = {
    t for t in os.environ.get("TRACKED_TOKENS", "").split(",") if t
}

# Ingestion control
INGESTION_ENABLED = os.environ.get("INGESTION_ENABLED", "1") == "1"

print("TRACKED_TOKENS =", TRACKED_TOKENS)

