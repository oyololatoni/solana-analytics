import asyncio
import os
import sys
import requests
from datetime import datetime, timezone
import psycopg
from pathlib import Path

# Add project root needed for config
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DATABASE_URL, SLACK_WEBHOOK_URL

# Poll interval in seconds
POLL_INTERVAL = 60

async def check_alerts():
    print("Starting Alert Engine...")
    print(f"Checking every {POLL_INTERVAL} seconds.")
    
    if not SLACK_WEBHOOK_URL:
        print("Warning: SLACK_WEBHOOK_URL not set. Notifications will fail.")

    while True:
        try:
            # Connect to DB
            async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
                async with conn.cursor() as cur:
                    # 1. Fetch active alerts not in cooldown
                    await cur.execute("""
                        SELECT id, token_mint, metric, condition, value, cooldown_minutes, last_triggered_at
                        FROM alerts
                        WHERE last_triggered_at IS NULL 
                           OR last_triggered_at < NOW() - (cooldown_minutes * INTERVAL '1 minute')
                    """)
                    alerts = await cur.fetchall()
                    
                    if not alerts:
                        print(f"[{datetime.now().time()}] No active alerts to check.")
                    else:
                        print(f"[{datetime.now().time()}] Checking {len(alerts)} alerts...")

                    for row in alerts:
                        alert_id, mint, metric, condition, threshold, cooldown, last_triggered = row
                        
                        current_value = 0.0
                        
                        # 2. Calculate Metric
                        if metric == 'volume_1h':
                            await cur.execute("""
                                SELECT COALESCE(SUM(amount), 0) FROM events 
                                WHERE token_mint = %s 
                                AND block_time > NOW() - INTERVAL '1 hour'
                            """, (mint,))
                            res = await cur.fetchone()
                            current_value = float(res[0])
                        elif metric == 'swap_count_1h':
                            await cur.execute("""
                                SELECT COUNT(*) FROM events 
                                WHERE token_mint = %s 
                                AND block_time > NOW() - INTERVAL '1 hour'
                            """, (mint,))
                            res = await cur.fetchone()
                            current_value = float(res[0])
                        
                        # 3. Evaluate
                        triggered = False
                        if condition == 'gt' and current_value > float(threshold):
                            triggered = True
                        elif condition == 'lt' and current_value < float(threshold):
                            triggered = True
                            
                        # 4. Trigger
                        if triggered:
                            print(f"  🚨 ALERT {alert_id}: {metric} {current_value} {condition} {threshold}")
                            
                            # Notify Slack
                            if SLACK_WEBHOOK_URL:
                                msg = {
                                    "text": f"🚨 *Alert Triggered*\nToken: `{mint[:8]}...`\nMetric: *{metric}*\nValue: `{current_value}`\nCondition: `{condition} {threshold}`"
                                }
                                try:
                                    requests.post(SLACK_WEBHOOK_URL, json=msg, timeout=5)
                                except Exception as e:
                                    print(f"Failed to send Slack: {e}")
                            
                            # Update DB (mark as triggered)
                            await cur.execute("""
                                UPDATE alerts 
                                SET last_triggered_at = NOW() 
                                WHERE id = %s
                            """, (alert_id,))
                            await conn.commit()
                            
        except Exception as e:
            print(f"Error in alert loop: {e}")
            
        await asyncio.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    try:
        asyncio.run(check_alerts())
    except KeyboardInterrupt:
        print("Stopped.")
