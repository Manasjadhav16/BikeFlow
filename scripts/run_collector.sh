#!/usr/bin/env bash
# Start the live GBFS collector in the background (nohup) and write a PID file.
# Extra arguments are passed to src.collector, e.g. scripts/run_collector.sh --max-minutes 600
set -euo pipefail
cd "$(dirname "$0")/.."
PID_FILE=data/live/collector.pid
LOG=data/live/collector.log
mkdir -p data/live

if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "collector already running (PID $(cat "$PID_FILE")); stop it with scripts/stop_collector.sh"
  exit 0
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] starting collector $*" >> "$LOG"
nohup .venv/bin/python -u -m src.collector "$@" >> "$LOG" 2>&1 &
PID=$!
echo "$PID" > "$PID_FILE"

# Keep the Mac from idle/system sleep while the collector runs; caffeinate exits with it.
if command -v caffeinate >/dev/null 2>&1; then
  nohup caffeinate -i -s -w "$PID" >/dev/null 2>&1 &
fi

sleep 3
if ! kill -0 "$PID" 2>/dev/null; then
  echo "collector failed to start; last log lines:" >&2
  tail -20 "$LOG" >&2
  rm -f "$PID_FILE"
  exit 1
fi
echo "collector started (PID $PID); log: $LOG; health: outputs/metrics/collector_health.jsonl"
