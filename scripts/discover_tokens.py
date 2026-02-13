
import asyncio
import os
import sys
import json
import logging
import requests
import time
import random
from datetime import datetime, timezone, timedelta

# Add project root
sys.path.insert(0, os.getcwd())

# Load .env explicitly
if not os.environ.get("HELIUS_API_KEY"):
    env_path = ".env.local"
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                if "=" in line and not line.strip().startswith("#"):
                    k, v = line.split("=", 1)
                    os.environ[k.strip()] = v.strip().strip('"').strip("'")

from app.core.db import get_db_connection, init_db

# 1. Discovery Mode
MODE = "CALIBRATION" 

# Config
HELIUS_KEYS = []
if os.getenv("HELIUS_API_KEY"): HELIUS_KEYS.append(os.getenv("HELIUS_API_KEY"))
if os.getenv("HELIUS_API_KEY_2"): HELIUS_KEYS.append(os.getenv("HELIUS_API_KEY_2"))

# Data Sources
GECKO_NEW_POOLS_URL = "https://api.geckoterminal.com/api/v2/networks/solana/new_pools"
DEX_PROFILES_URL = "https://api.dexscreener.com/token-profiles/latest/v1"
DEX_TOKENS_URL = "https://api.dexscreener.com/latest/dex/tokens/"
DEX_SEARCH_URL = "https://api.dexscreener.com/latest/dex/search"

# 4. Liquidity Pre-Filter Rule
MIN_LIQUIDITY = 50000

# 5. DexScreener Enrichment Rules
MAX_BATCH_SIZE = 30

# 8. Volume Threshold Rule
MIN_VOLUME = 10000

# 14. Discovery Isolation Rule
DISCOVERY_CLASS = f"NEW_LISTING_{MODE}"

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("stage1_discovery")

def get_helius_key():
    if not HELIUS_KEYS: return None
    return random.choice(HELIUS_KEYS)

def fetch_mint_creation_time_via_helius(mint):
    """
    3. Token Age Rule: Computed using First Signature Block Time via Helius.
    """
    key = get_helius_key()
    if not key: return None
    rpc_url = f"https://mainnet.helius-rpc.com/?api-key={key}"
    before_sig = None
    
    MAX_AGE_SECONDS = 14 * 86400
    now_ts = time.time()
    cutoff_ts = now_ts - MAX_AGE_SECONDS
    last_ts = now_ts

    MAX_HOOPS = 3 
    
    for i in range(MAX_HOOPS):
        params = {"limit": 1000}
        if before_sig: params["before"] = before_sig
            
        payload = {
            "jsonrpc": "2.0",
            "id": f"age-{mint}",
            "method": "getSignaturesForAddress",
            "params": [mint, params] 
        }
        
        try:
            r = requests.post(rpc_url, json=payload, timeout=5)
            if r.status_code != 200: return None
            
            sigs = r.json().get("result", [])
            if not sigs: return last_ts if i > 0 else None
            
            oldest_sig = sigs[-1]
            oldest_ts = oldest_sig.get("blockTime")
            if oldest_ts: last_ts = oldest_ts
            
            if oldest_ts and oldest_ts < cutoff_ts:
                return oldest_ts 
            
            if len(sigs) < 1000:
                return oldest_ts 
            
            before_sig = oldest_sig.get("signature")
            time.sleep(0.1) 
            
        except Exception: return None

    return last_ts 

async def resolve_candidates_bulk(candidates, stats):
    """
    Aggregated Enrichment Loop
    """
    logger.info(f"Checking {len(candidates)} unique candidates...")
    final_list = []
    
    # Batch processing
    mints = list(candidates.keys())
    for i in range(0, len(mints), MAX_BATCH_SIZE):
        batch = mints[i:i+MAX_BATCH_SIZE]
        url = f"{DEX_TOKENS_URL}{','.join(batch)}"
        try:
            r = requests.get(url, timeout=10)
            if r.status_code != 200: continue
            data = r.json()
            pairs = data.get("pairs", [])
            
            token_pairs = {}
            for p in pairs:
                base = p.get("baseToken", {}).get("address")
                if base not in token_pairs: token_pairs[base] = []
                token_pairs[base].append(p)
            
            for mint in batch:
                if mint not in token_pairs:
                    stats['reject_pair'] += 1
                    continue
                
                candidate_pairs = token_pairs[mint]
                valid_pairs = []
                for p in candidate_pairs:
                    if p.get("dexId") == "raydium":
                        quote = p.get("quoteToken", {}).get("symbol", "").upper()
                        if quote in ["SOL", "USDC", "USDT"]:
                            valid_pairs.append(p)
                
                if not valid_pairs:
                    stats['reject_pair'] += 1
                    continue
                    
                best_pair = max(valid_pairs, key=lambda x: float(x.get("volume", {}).get("h24", 0) or 0))
                
                liq = float(best_pair.get("liquidity", {}).get("usd", 0) or 0)
                if liq < MIN_LIQUIDITY:
                    stats['reject_liq'] += 1
                    continue
                    
                vol = float(best_pair.get("volume", {}).get("h24", 0) or 0)
                if vol < MIN_VOLUME:
                    stats['reject_volume'] += 1
                    continue
                    
                creation_ts = fetch_mint_creation_time_via_helius(mint)
                if not creation_ts:
                    stats['reject_age'] += 1
                    continue
                
                age_hours = (time.time() - creation_ts) / 3600
                if age_hours > (14 * 24):
                    stats['reject_age'] += 1
                    continue
                    
                final_list.append({
                    "mint": mint,
                    "created_at": datetime.fromtimestamp(creation_ts, tz=timezone.utc).isoformat(),
                    "primary_pair_address": best_pair.get("pairAddress"),
                    "base_token": best_pair.get("baseToken", {}).get("symbol"),
                    "quote_token": best_pair.get("quoteToken", {}).get("symbol"),
                    "initial_liq": liq,
                    "discovery_class": DISCOVERY_CLASS
                })
        except Exception: pass
        time.sleep(0.5)
            
    return final_list

async def get_existing_tokens():
    await init_db()
    existing = set()
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT address FROM tokens")
                rows = await cur.fetchall()
                for r in rows: existing.add(r[0])
    except Exception: pass
    return existing

async def discover_loop():
    logger.info(f"Starting Multi-Source Discovery (MODE={MODE})...")
    existing_db = await get_existing_tokens()
    
    stats = {
        "processed": 0,
        "reject_db": 0,
        "reject_liq": 0,
        "reject_age": 0,
        "reject_pair": 0,
        "reject_volume": 0
    }
    
    unique_candidates = {} # mint -> source

    # SOURCE 1: GeckoTerminal New Pools (Raw Stream)
    try:
        r = requests.get(GECKO_NEW_POOLS_URL, timeout=10)
        if r.status_code == 200:
            for p in r.json().get("data", []):
                mint = p.get("relationships", {}).get("base_token", {}).get("data", {}).get("id", "").replace("solana_", "")
                if mint and mint not in existing_db: unique_candidates[mint] = "gecko_new"
    except Exception: pass

    # SOURCE 2: DexScreener Search "Raydium" (Standard migration stream)
    search_queries = ["Raydium", "SOL", "USDC"]
    for q in search_queries:
        try:
            r = requests.get(f"{DEX_SEARCH_URL}?q={q}", timeout=10)
            if r.status_code == 200:
                for p in r.json().get("pairs", []):
                    if p.get("chainId") == "solana":
                        mint = p.get("baseToken", {}).get("address")
                        if mint and mint not in existing_db: unique_candidates[mint] = "dex_search"
        except Exception: pass
        time.sleep(0.2)

    # SOURCE 3: DexScreener Token Profiles (Active Social)
    try:
        r = requests.get(DEX_PROFILES_URL, timeout=10)
        if r.status_code == 200:
            for p in r.json():
                mint = p.get("tokenAddress")
                if p.get("chainId") == "solana" and mint and mint not in existing_db:
                    unique_candidates[mint] = "dex_profile"
    except Exception: pass

    stats['processed'] = len(unique_candidates)
    logger.info(f"Aggregated {len(unique_candidates)} unique candidates for evaluation.")
    
    if unique_candidates:
        final_candidates = await resolve_candidates_bulk(unique_candidates, stats)
    else:
        final_candidates = []
            
    logger.info("--- Rejection Summary ---")
    logger.info(f"Unique Scanned: {stats['processed']}")
    logger.info(f"Liquidity < $50k: {stats['reject_liq']}")
    logger.info(f"Age > 14 Days: {stats['reject_age']}")
    logger.info(f"Raydium/Pair Rejects: {stats['reject_pair']}")
    logger.info(f"Volume < $10k: {stats['reject_volume']}")
    logger.info(f"ACCEPTED: {len(final_candidates)}")
    logger.info("-------------------------")

    with open("candidate_tokens.json", "w") as f:
        json.dump(final_candidates, f, indent=2)
    
    if final_candidates:
        try:
            async with get_db_connection() as conn:
                async with conn.cursor() as cur:
                     for c in final_candidates:
                         await cur.execute("""
                            INSERT INTO tokens (
                                address, primary_pair_address, pair_validated, 
                                created_at_chain, detected_at, is_active, 
                                eligibility_status, discovery_class,
                                base_token_symbol, quote_token_symbol,
                                chain_id
                            )
                            VALUES (%s, %s, TRUE, %s, %s, TRUE, 'PRE_ELIGIBLE', %s, %s, %s, (SELECT id FROM chains WHERE name = 'solana'))
                            ON CONFLICT (address) DO UPDATE SET 
                                pair_validated = TRUE, 
                                primary_pair_address = %s,
                                eligibility_status = 'PRE_ELIGIBLE',
                                discovery_class = %s
                         """, (
                             c['mint'], c['primary_pair_address'], 
                             c['created_at'], c['created_at'], 
                             c['discovery_class'], 
                             c['base_token'], c['quote_token'],
                             c['primary_pair_address'],
                             c['discovery_class']
                         ))
                await conn.commit()
            logger.info(f"Saved {len(final_candidates)} NEW candidates.")
        except Exception as e:
            logger.error(f"DB Save Failed: {e}")

if __name__ == "__main__":
    asyncio.run(discover_loop())
