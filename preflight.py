import os
import sys
import importlib
import time
import psycopg

# ANSI colors
GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"
YELLOW = "\033[93m"

def print_pass(msg):
    print(f"{msg}: {GREEN}PASS{RESET}")

def print_fail(msg, error=None):
    print(f"{msg}: {RED}FAIL{RESET}")
    if error:
        print(f"  Error: {error}")

def check_env():
    print("Checking Environment Variables...", end=" ")
    
    # 1. DATABASE_URL
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print_fail("\nDATABASE_URL missing")
        return False
    if "postgres" not in db_url:
        print_fail("\nDATABASE_URL invalid scheme")
        return False

    # 2. HELIUS_WEBHOOK_SECRET
    if not os.environ.get("HELIUS_WEBHOOK_SECRET"):
        print_fail("\nHELIUS_WEBHOOK_SECRET missing")
        return False

    # 3. TRACKED_TOKENS
    tokens_str = os.environ.get("TRACKED_TOKENS", "")
    tokens = [t.strip() for t in tokens_str.split(",") if t.strip()]
    enabled = os.environ.get("INGESTION_ENABLED", "1") == "1"
    
    if enabled and not tokens:
        print(f"\n{YELLOW}WARNING: Ingestion enabled but no tokens tracked.{RESET}", end=" ")
    
    print(f"{GREEN}PASS{RESET}")
    return True

def check_imports():
    print("Checking Code Integrity (Imports)...", end=" ")
    modules = ["api.main", "api.webhooks", "api.metrics", "api.db", "worker"]
    for mod in modules:
        try:
            importlib.import_module(mod)
        except ImportError as e:
            print_fail(f"\nFailed to import {mod}", e)
            return False
        except SyntaxError as e:
            print_fail(f"\nSyntax error in {mod}", e)
            return False
        except Exception as e:
            print_fail(f"\nUnexpected error importing {mod}", e)
            return False
            
    print(f"{GREEN}PASS{RESET}")
    return True

def check_db():
    print("Checking Database Connectivity...", end=" ")
    db_url = os.environ.get("DATABASE_URL")
    try:
        with psycopg.connect(db_url, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                # Check events table
                cur.execute("SELECT to_regclass('public.events')")
                if not cur.fetchone()[0]:
                    print_fail("\nTable 'events' missing")
                    return False
                
                # Check ingestion_stats
                cur.execute("SELECT to_regclass('public.ingestion_stats')")
                if not cur.fetchone()[0]:
                    print_fail("\nTable 'ingestion_stats' missing")
                    return False
                    
                # Check raw_webhooks (new architecture)
                cur.execute("SELECT to_regclass('public.raw_webhooks')")
                if not cur.fetchone()[0]:
                    print_fail("\nTable 'raw_webhooks' missing (Apply Migration 005!)")
                    return False

    except psycopg.OperationalError as e:
        print_fail("\nConnection failed", e)
        return False
    except Exception as e:
        print_fail("\nUnexpected DB error", e)
        return False

    print(f"{GREEN}PASS{RESET}")
    return True

def run_preflight():
    print(f"\n🚀 {YELLOW}Running Deployment Gate Checks...{RESET}\n")
    
    if not check_env():
        sys.exit(1)
        
    if not check_imports():
        sys.exit(1)
        
    if not check_db():
        sys.exit(1)

    print(f"\n{GREEN}✅ All systems go! Ready for launch.{RESET}\n")
    sys.exit(0)

if __name__ == "__main__":
    run_preflight()
