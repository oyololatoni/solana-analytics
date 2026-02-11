import os

# Database
DATABASE_URL = os.environ.get("DATABASE_URL")

# Webhook auth
# Webhook auth & RPC
HELIUS_WEBHOOK_SECRET = os.environ.get("HELIUS_WEBHOOK_SECRET", "")
HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY", HELIUS_WEBHOOK_SECRET)

# Tokens we care about (comma-separated env var)
# Tokens we care about (comma-separated env var)
TRACKED_TOKENS = {
    t for t in os.environ.get("TRACKED_TOKENS", "").split(",") if t
}

# Optional Labels: "Mint:Name,Mint:Name"
raw_labels = os.environ.get("TOKEN_LABELS", "")
TOKEN_LABELS = {}
if raw_labels:
    for pair in raw_labels.split(","):
        if ":" in pair:
            mint, name = pair.split(":", 1)
            TOKEN_LABELS[mint.strip()] = name.strip()

def get_token_name(mint):
    return TOKEN_LABELS.get(mint, f"{mint[:4]}...{mint[-4:]}")


# Ingestion control
INGESTION_ENABLED = os.environ.get("INGESTION_ENABLED", "1") == "1"

print("TRACKED_TOKENS =", TRACKED_TOKENS)

