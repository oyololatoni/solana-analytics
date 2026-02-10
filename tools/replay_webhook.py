import os
import json
import requests
from dotenv import load_dotenv

# Load local-only env vars
load_dotenv(".env.local")

HELIUS_WEBHOOK_SECRET = os.environ["HELIUS_WEBHOOK_SECRET"]

WEBHOOK_URL = "http://127.0.0.1:8000/webhooks/helius"

