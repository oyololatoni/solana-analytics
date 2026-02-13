
import os
import json
import time
import queue
import threading
import subprocess
import psycopg
import sys
from datetime import datetime

# Add project root
sys.path.insert(0, os.getcwd())

print("DEBUG: ORCHESTRATOR START", flush=True)

# ============================================
# CONFIGURATION
# ============================================

TARGET_RESOLVED = 400
MAX_CANDIDATE_TOKENS = 700
MAX_CONCURRENT_WORKERS = 3
POLL_INTERVAL_SECONDS = 5

# Load Env
if not os.environ.get("DATABASE_URL"):
    env_path = ".env.local"
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                if "DATABASE_URL" in line and not line.strip().startswith("#"):
                    os.environ["DATABASE_URL"] = line.split("=", 1)[1].strip().strip('"').strip("'")

DB_DSN = os.getenv("DATABASE_URL")

# ============================================
# DATABASE HELPERS
# ============================================

def get_db_connection():
    return psycopg.connect(DB_DSN)

def get_resolved_count():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM lifecycle_labels;")
                return cur.fetchone()[0]
    except Exception as e:
        print(f"[WARN] DB Check Failed: {e}")
        return 0

def get_processed_token_set():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Tokens that are successfully processed through the pipeline
                cur.execute("SELECT address FROM tokens WHERE eligibility_status IN ('ELIGIBLE', 'REJECTED');")
                return {row[0] for row in cur.fetchall()}
    except Exception as e:
        print(f"[WARN] get_processed_token_set failed: {e}", flush=True)
        return set()

def get_pending_tokens():
    """Fetch tokens from DB that need ingestion (PRE_ELIGIBLE but low trade count)"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT t.address, t.created_at_chain, t.primary_pair_address
                    FROM tokens t
                    LEFT JOIN trades tr ON t.id = tr.token_id
                    WHERE t.eligibility_status = 'PRE_ELIGIBLE'
                    GROUP BY t.address, t.created_at_chain, t.primary_pair_address
                    HAVING COUNT(tr.id) < 20;
                """)
                rows = cur.fetchall()
                print(f"[DEBUG] Found {len(rows)} pending PRE_ELIGIBLE tokens in DB", flush=True)
                return [{"mint": r[0], "created_at": r[1].isoformat() if r[1] else None, "primary_pair_address": r[2]} for r in rows]
    except Exception as e:
        print(f"[WARN] Failed to fetch pending: {e}", flush=True)
        return []

# ============================================
# DISCOVERY PHASE
# ============================================

def run_discovery():
    print("Running Stage 1: Token Discovery...")
    subprocess.run([sys.executable, "scripts/discover_tokens.py"], check=False)
    
    print("Running Stage 2/3: Gatekeeper (Precheck)...")
    subprocess.run([sys.executable, "scripts/stage2_3_precheck.py"], check=False)

    if not os.path.exists("backfill_queue.json"):
        print("No backfill_queue.json found.")
        return []

    with open("backfill_queue.json") as f:
        tokens = json.load(f)

    return tokens[:MAX_CANDIDATE_TOKENS]

# ============================================
def process_token(token):
    mint = token["mint"]
    created_at = token["created_at"]
    pair = token.get("primary_pair_address")
    
    # Run Ingestion
    print(f"[Worker] Ingesting {mint} (created: {created_at}, pair: {pair})...", flush=True)
    try:
        # Pass all 3 required arguments
        cmd = [sys.executable, "scripts/ingest_token.py", mint, str(created_at), str(pair)]
        print(f"[Worker] Running: {' '.join(cmd)}", flush=True)
        subprocess.run(cmd, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"[Worker] Failed to ingest {mint}: {e}", flush=True)
        return False

# ============================================
# THREAD WORKER
# ============================================

def worker_thread(q, processed_set, currently_queued):
    while True:
        token = q.get() # Wait indefinitely for work
        if token is None: # Exit signal
            q.task_done()
            break

        mint = token["mint"]
        try:
            if mint not in processed_set:
                success = process_token(token)
                if success:
                    processed_set.add(mint)
        finally:
            if mint in currently_queued:
                currently_queued.remove(mint)
            q.task_done()

# ============================================
# ORCHESTRATOR MAIN LOOP
# ============================================

def run_periodic_tasks():
    print("[Orchestrator] Running Periodic Tasks (Eligibility -> Features -> Labels)...")
    try:
        subprocess.run([sys.executable, "scripts/stage5_eligibility.py"], check=False)
        subprocess.run([sys.executable, "scripts/stage6_features.py"], check=False)
        subprocess.run([sys.executable, "scripts/stage7_labels.py"], check=False)
    except Exception as e:
        print(f"[Orchestrator] Task Error: {e}")

def main():
    if not DB_DSN:
        print("DATABASE_URL not set")
        return

    print(f"[Orchestrator] Starting Reactive Backfill. Target Resolved: {TARGET_RESOLVED}")
    
    token_queue = queue.Queue()
    processed_set = get_processed_token_set()
    currently_queued = set()
    
    # Start persistent workers
    threads = []
    for _ in range(MAX_CONCURRENT_WORKERS):
        t = threading.Thread(target=worker_thread, args=(token_queue, processed_set, currently_queued))
        t.daemon = True # Ensure they die when main dies
        t.start()
        threads.append(t)

    resolved_count = get_resolved_count()
    
    while resolved_count < TARGET_RESOLVED:
        # 1. Discovery & Fetch Pending
        candidates = run_discovery()
        pending = get_pending_tokens()
        all_candidates = candidates + pending
        
        # 1.1 Refresh processed set to avoid redundant ingestion
        processed_set_update = get_processed_token_set()
        processed_set.update(processed_set_update)
        
        # 2. Add new candidates to queue
        to_queue_count = 0
        for c in all_candidates:
            mint = c['mint']
            if mint not in processed_set and mint not in currently_queued:
                token_queue.put(c)
                currently_queued.add(mint)
                to_queue_count += 1
        
        if to_queue_count > 0:
            print(f"[Orchestrator] Queued {to_queue_count} new tokens. Queue Size: {token_queue.qsize()}")

        # 3. Periodic Pipeline Tasks (REACTIVE: Runs every loop iteration)
        run_periodic_tasks()
        
        # 4. Check End Condition
        resolved_count = get_resolved_count()
        print(f"[Orchestrator] Current Resolved Tokens: {resolved_count}/{TARGET_RESOLVED} | Queue: {token_queue.qsize()}")
        
        if resolved_count >= TARGET_RESOLVED:
            print("[Orchestrator] Checkpoint Reached! Stopping.")
            break
            
        time.sleep(POLL_INTERVAL_SECONDS)

    print("[Orchestrator] Done.")

if __name__ == "__main__":
    main()
