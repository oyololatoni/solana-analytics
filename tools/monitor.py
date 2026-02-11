import os
import time
import requests
import psycopg
import sys
from datetime import datetime, timezone

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL, INGESTION_ENABLED

# CONFIGURATION
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")
POLL_INTERVAL_SECONDS = 60    # Check every minute
WINDOW_MINUTES = 10           # Look back 10 minutes
ALERT_COOLDOWN_SECONDS = 900  # 15 minutes between same-type alerts
IGNORED_RATIO_THRESHOLD = 0.8 # > 80%

# ANSI colors
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RESET = "\033[0m"

class Monitor:
    def __init__(self):
        self.last_alerts = {}

    def send_alert(self, title, message):
        """
        Sends alert if not in cooldown.
        """
        now = time.time()
        last_time = self.last_alerts.get(title, 0)
        
        if now - last_time < ALERT_COOLDOWN_SECONDS:
            print(f"{YELLOW}[SKIP] Alert '{title}' suppressed (cooldown){RESET}")
            return

        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        full_msg = f"[{timestamp}] {title}: {message}"
        
        # 1. Log to stderr (captured by Fly logs)
        print(f"{RED}🚨 ALERT: {full_msg}{RESET}", file=sys.stderr)

        # 2. Send to Slack
        if SLACK_WEBHOOK_URL:
            try:
                payload = {
                    "text": f"🚨 *{title}*\n{message}",
                    "blocks": [
                        {
                            "type": "section",
                            "text": {"type": "mrkdwn", "text": f"🚨 *{title}*\n{message}"}
                        },
                        {
                            "type": "context",
                            "elements": [{"type": "mrkdwn", "text": f"Time: {timestamp} | Env: {os.environ.get('FLY_APP_NAME', 'local')}"}]
                        }
                    ]
                }
                requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=5)
            except Exception as e:
                print(f"{YELLOW}Failed to send Slack alert: {e}{RESET}", file=sys.stderr)
        
        self.last_alerts[title] = now

    def check(self):
        try:
            with psycopg.connect(DATABASE_URL) as conn:
                with conn.cursor() as cur:
                    # Query stats for the last WINDOW_MINUTES
                    cur.execute(
                        """
                        SELECT 
                            COALESCE(SUM(events_received), 0) as total_events,
                            COALESCE(SUM(swaps_inserted), 0) as total_inserted,
                            COALESCE(SUM(swaps_ignored), 0) as total_ignored,
                            COALESCE(SUM(ignored_ingestion_disabled), 0) as total_disabled
                        FROM ingestion_stats
                        WHERE created_at > NOW() - INTERVAL '%s minutes'
                        """,
                        (WINDOW_MINUTES,)
                    )
                    
                    row = cur.fetchone()
                    if not row:
                        return # Should not happen with aggregate

                    total_events, total_inserted, total_ignored, total_disabled = row
                    
                    # 1. No Activity
                    if total_events == 0:
                        self.send_alert(
                            "No Ingestion Activity",
                            f"Zero events received in last {WINDOW_MINUTES}m.\nCheck Helius/Fly status."
                        )
                        return

                    # 2. Silent Failure (Events > 0, Inserted = 0)
                    if total_inserted == 0 and total_disabled < total_events:
                         self.send_alert(
                            "Ingestion Active but 0 Inserts",
                            f"Received {total_events} events, inserted 0.\nPossible schema/token mismatch."
                        )

                    # 3. Safe Mode Stuck
                    if total_disabled > 0 and total_disabled == total_events:
                        self.send_alert(
                            "Safe Mode Active",
                            f"Ingestion DISABLED via config.\nAll {total_events} events rejected."
                        )

                    # 4. High Ignore Ratio
                    if total_events > 0 and total_disabled == 0:
                        ratio = total_ignored / total_events
                        if ratio > IGNORED_RATIO_THRESHOLD:
                            self.send_alert(
                                "High Ignore Ratio",
                                f"Ignoring {ratio:.1%} of events ({total_ignored}/{total_events}).\nCheck payloads."
                            )
                            
                    print(f"{GREEN}[OK] {total_events} events, {total_inserted} inserted{RESET}")

        except Exception as e:
            print(f"{RED}Monitor Loop Error: {e}{RESET}", file=sys.stderr)


if __name__ == "__main__":
    monitor = Monitor()
    print(f"Starting Monitor (Window: {WINDOW_MINUTES}m)...")
    while True:
        monitor.check()
        time.sleep(POLL_INTERVAL_SECONDS)
