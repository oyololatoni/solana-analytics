#!/bin/bash
set -e

echo "[start_worker.sh] Launching services..."

# Start Monitor in background with restart loop
(
  while true; do
    echo "[start_worker.sh] Starting Monitor..."
    python tools/monitor.py
    echo "[start_worker.sh] Monitor exited with code $?. Restarting in 5s..."
    sleep 5
  done
) &

# Start Worker in foreground (Main Process)
# If this exits, the container stops (and Fly restarts it)
echo "[start_worker.sh] Starting Worker..."
exec python worker.py
