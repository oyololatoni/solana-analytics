#!/usr/bin/env python3
"""
Full-stack deployment script for Solana Analytics.

Deploys changes across all infrastructure layers in the correct order:
  1. Neon Postgres (schema migrations)
  2. Fly.io (application code)
  3. Helius (webhook verification)

Usage:
    python tools/deploy.py                  # Full deployment
    python tools/deploy.py --dry-run        # Preview mode
    python tools/deploy.py --db-only        # Database changes only
    python tools/deploy.py --skip-db        # Skip database, deploy code only
"""
import os
import sys
import subprocess
import time
import requests
import psycopg
from pathlib import Path

# ─── Config ───────────────────────────────────────────────────────────
# Add project root to path so we can import config
sys.path.insert(0, str(Path(__file__).parent.parent))
from app.core.config import DATABASE_URL as CONFIG_DB_URL

_env_db = os.environ.get("DATABASE_URL", "")
DATABASE_URL = _env_db if _env_db and "***" not in _env_db else CONFIG_DB_URL
FLY_APP = "solana-analytics"
SCHEMA_DIR = Path(__file__).parent.parent / "schema"

# Colors
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RESET = "\033[0m"
BOLD = "\033[1m"

def banner(msg):
    print(f"\n{CYAN}{BOLD}{'='*60}{RESET}")
    print(f"{CYAN}{BOLD}  {msg}{RESET}")
    print(f"{CYAN}{BOLD}{'='*60}{RESET}\n")

def ok(msg):
    print(f"  {GREEN}✓{RESET} {msg}")

def warn(msg):
    print(f"  {YELLOW}⚠{RESET} {msg}")

def fail(msg):
    print(f"  {RED}✗{RESET} {msg}")

def step(msg):
    print(f"\n{BOLD}▶ {msg}{RESET}")


# ─── Step 1: Neon Postgres ────────────────────────────────────────────

def deploy_database(dry_run=False):
    banner("STEP 1: Neon Postgres — Schema Migrations")

    if not DATABASE_URL:
        fail("DATABASE_URL not set. Cannot connect to Neon.")
        return False

    db_host = DATABASE_URL.split("@")[-1].split("/")[0] if "@" in DATABASE_URL else "local"
    step(f"Connecting to: {db_host}")

    try:
        conn = psycopg.connect(DATABASE_URL, connect_timeout=10)
        ok("Connected to Neon Postgres")
    except Exception as e:
        fail(f"Connection failed: {e}")
        return False

    cur = conn.cursor()

    # Ensure migrations tracking table
    step("Checking migrations table")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            id SERIAL PRIMARY KEY,
            migration_file TEXT NOT NULL UNIQUE,
            applied_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    conn.commit()
    ok("schema_migrations table ready")

    # Check applied migrations
    cur.execute("SELECT migration_file FROM schema_migrations ORDER BY migration_file")
    applied = {row[0] for row in cur.fetchall()}

    # Get pending migrations
    migration_files = sorted(SCHEMA_DIR.glob("*.sql"))
    pending = [(f.name, f.read_text()) for f in migration_files if f.name not in applied]

    if not pending:
        ok("No pending migrations — schema is up to date")
        cur.close()
        conn.close()
        return True

    step(f"Applying {len(pending)} pending migration(s)")

    for filename, sql in pending:
        print(f"\n  📄 {filename}")

        if dry_run:
            preview = sql.strip().split("\n")[:5]
            for line in preview:
                print(f"     {YELLOW}{line}{RESET}")
            if len(sql.strip().split("\n")) > 5:
                print(f"     {YELLOW}...{RESET}")
            warn("[DRY RUN] Skipped")
            continue

        try:
            cur.execute(sql)
            cur.execute(
                "INSERT INTO schema_migrations (migration_file) VALUES (%s)",
                (filename,)
            )
            conn.commit()
            ok(f"Applied: {filename}")
        except Exception as e:
            conn.rollback()
            fail(f"Failed: {filename} — {e}")
            cur.close()
            conn.close()
            return False

    # Verify current constraint
    step("Verifying events table constraint")
    cur.execute("""
        SELECT i.relname, array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum))
        FROM pg_class t
        JOIN pg_index ix ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
        WHERE t.relname = 'events' AND ix.indisunique = true
        GROUP BY i.relname
    """)
    for idx_name, cols in cur.fetchall():
        print(f"    Index: {idx_name} → columns: {cols}")

    cur.close()
    conn.close()
    ok("Database deployment complete")
    return True


# ─── Step 2: Fly.io ──────────────────────────────────────────────────

def deploy_flyio(dry_run=False):
    banner("STEP 2: Fly.io — Application Deployment")

    # Check fly CLI is available
    step("Checking Fly CLI")
    try:
        result = subprocess.run(["fly", "version"], capture_output=True, text=True, timeout=10)
        if result.returncode != 0:
            fail("Fly CLI not found or not authenticated")
            return False
        ok(f"Fly CLI: {result.stdout.strip()}")
    except FileNotFoundError:
        fail("Fly CLI not installed. Install: curl -L https://fly.io/install.sh | sh")
        return False

    # Check app status
    step(f"Checking app: {FLY_APP}")
    try:
        result = subprocess.run(
            ["fly", "status", "-a", FLY_APP],
            capture_output=True, text=True, timeout=15
        )
        if result.returncode == 0:
            ok(f"App '{FLY_APP}' is accessible")
        else:
            warn(f"Could not reach app: {result.stderr.strip()}")
    except Exception as e:
        warn(f"Status check failed: {e}")

    if dry_run:
        warn("[DRY RUN] Would run: fly deploy -a " + FLY_APP)
        return True

    # Deploy
    step("Deploying to Fly.io")
    print(f"    Running: fly deploy -a {FLY_APP}\n")

    result = subprocess.run(
        ["fly", "deploy", "-a", FLY_APP],
        cwd=str(Path(__file__).parent.parent),
        timeout=300
    )

    if result.returncode != 0:
        fail("Fly.io deployment failed")
        return False

    ok("Fly.io deployment complete")

    # Wait for deployment to stabilize
    step("Waiting for deployment to stabilize (15s)")
    time.sleep(15)

    return True


# ─── Step 3: Helius Webhook Verification ─────────────────────────────

# ─── Step 0: Preflight ────────────────────────────────────────────────
def run_preflight():
    banner("STEP 0: Preflight — Safety Checks")
    step("Running local preflight checks")
    try:
        # Run preflight.py using the same python interpreter
        result = subprocess.run(
            [sys.executable, "preflight.py"],
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            print(result.stdout)
            print(result.stderr)
            fail("Preflight checks failed! Fix issues before deploying.")
            return False
        
        ok("Preflight passed: Env vars, imports, and DB connection safe.")
        return True
    except Exception as e:
        fail(f"Could not run preflight: {e}")
        return False

# ─── Step 3: Helius Webhook Verification ─────────────────────────────

def verify_helius(dry_run=False):
    banner("STEP 3: Post-Deployment Verification")

    step("Checking webhook endpoint health")

    # Try to hit the app's root or health endpoint
    app_url = f"https://{FLY_APP}.fly.dev"
    
    # Check /health (DB connectivity)
    step("Verifying Remote Health & DB Connectivity")
    try:
        r = requests.get(f"{app_url}/health", timeout=10)
        if r.status_code == 200:
            data = r.json()
            if data.get("database") == "connected":
                ok(f"Remote DB connected: {data}")
            else:
                fail(f"Remote DB disconnected! {data}")
        else:
            warn(f"Health endpoint returned {r.status_code}")
    except Exception as e:
         warn(f"Health check failed: {e}")

    # Check metrics health endpoint
    step("Testing /metrics/ingestion-stats (Config Verification)")
    try:
        r = requests.get(f"{app_url}/metrics/ingestion-stats", timeout=10)
        if r.status_code == 200:
            ok("Ingestion stats endpoint accessible")
        else:
            warn(f"Ingestion stats endpoint returned: {r.status_code}")
    except Exception as e:
        warn(f"Ingestion stats check failed: {e}")

    # Verify webhook endpoint exists (without sending auth)
    step("Testing webhook endpoint reachability")
    try:
        r = requests.post(
            f"{app_url}/webhooks/helius",
            json=[],
            timeout=10
        )
        if r.status_code == 401:
            ok("Webhook endpoint is live (returned 401 — auth required, as expected)")
        elif r.status_code == 200:
            ok("Webhook endpoint returned 200")
        else:
            warn(f"Webhook endpoint returned: {r.status_code}")
    except Exception as e:
        warn(f"Webhook endpoint not reachable: {e}")

    step("Helius webhook configuration")
    print(f"    {YELLOW}Note: Helius webhook URL should be:{RESET}")
    print(f"    {BOLD}{app_url}/webhooks/helius{RESET}")
    print(f"\n    {YELLOW}Verify in Helius dashboard that:{RESET}")
    print(f"    • Webhook URL = {app_url}/webhooks/helius")
    print(f"    • Auth header uses your HELIUS_WEBHOOK_SECRET")
    print(f"    • Transaction types include SWAP events")
    print(f"    • Account addresses include your tracked tokens")

    ok("Verification complete")
    return True


# ─── Main ─────────────────────────────────────────────────────────────

def main():
    dry_run = "--dry-run" in sys.argv
    db_only = "--db-only" in sys.argv
    skip_db = "--skip-db" in sys.argv

    banner("SOLANA ANALYTICS — FULL STACK DEPLOYMENT")
    print(f"  Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print(f"  App:  {FLY_APP}")
    print(f"  DB:   {'skip' if skip_db else 'deploy'}")
    print(f"  Code: {'skip' if db_only else 'deploy'}")

    success = True

    # Step 1: Database
    if not skip_db:
        if not deploy_database(dry_run):
            fail("Database deployment failed — aborting")
            return 1

    # Step 1.5: Preflight (Safety Guarantee)
    # Check AFTER DB deploy ensures tables exist.
    if not run_preflight():
        return 1

    if db_only:
        banner("DONE (database only)")
        return 0

    # Step 2: Fly.io
    if not deploy_flyio(dry_run):
        fail("Fly.io deployment failed")
        success = False

    # Step 3: Verify Remote State
    verify_helius(dry_run)

    # Summary
    banner("DEPLOYMENT SUMMARY")
    if success:
        ok("All deployment steps completed successfully!")
        print(f"\n  {BOLD}Next steps:{RESET}")
        print(f"  1. Monitor logs:  fly logs -a {FLY_APP}")
        print(f"  2. Check health:  curl https://{FLY_APP}.fly.dev/health")
        print(f"  3. Watch stats:   curl https://{FLY_APP}.fly.dev/metrics/ingestion-stats")
    else:
        fail("Some deployment steps failed — review output above")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
