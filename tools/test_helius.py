import os
import sys

# Add project root to path
sys.path.append(os.getcwd())

from api.helius import fetch_token_metadata
from config import HELIUS_API_KEY

print(f"API Key: {HELIUS_API_KEY[:4]}...{HELIUS_API_KEY[-4:] if HELIUS_API_KEY else 'None'}")

mint = "So11111111111111111111111111111111111111112"
print(f"Fetching metadata for {mint}...")

data = fetch_token_metadata(mint)
print("Result:", data)
