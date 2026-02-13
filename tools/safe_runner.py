import subprocess
import sys
import time
import re

def monitor_process(command, max_consecutive_errors=20):
    """
    Runs a command and monitors its output for infinite loops or excessive errors.
    """
    print(f"Starting Watchdog for: {command}")
    
    process = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )
    
    consecutive_429s = 0
    total_429s = 0
    
    try:
        while True:
            line = process.stdout.readline()
            if not line and process.poll() is not None:
                break
                
            if line:
                print(line.strip()) # Stream output to user
                
                # Check for 429
                if "429" in line:
                    consecutive_429s += 1
                    total_429s += 1
                elif "Saved" in line or "Fetched" in line:
                    # Reset on success
                    consecutive_429s = 0
                    
                # Threshold Check
                if consecutive_429s > max_consecutive_errors:
                    print(f"\n[WATCHDOG] ALERT: Detected {consecutive_429s} consecutive Rate Limit errors.")
                    print("[WATCHDOG] Terminating process to save credits.")
                    process.terminate()
                    return

    except KeyboardInterrupt:
        process.terminate()
        
    print(f"Process finished. Total 429 errors encountered: {total_429s}")

if __name__ == "__main__":
    # Hardcoded command for safety/ease of use
    cmd = "export $(grep -v '^#' .env.local | xargs) && ./venv/bin/python scripts/backfill_token_level.py"
    monitor_process(cmd)
