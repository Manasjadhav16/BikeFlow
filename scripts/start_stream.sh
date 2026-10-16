#!/usr/bin/env bash
# Start the live alert engine (src/stream_alerts.py) in the background. Needs Kafka running
# (scripts/start_kafka.sh) and something publishing to station_status (collector --kafka).
# Extra arguments go to src.stream_alerts.
set -euo pipefail
cd "$(dirname "$0")/.."
PID_FILE=data/live/stream_alerts.pid
LOG=data/live/stream_alerts.log
mkdir -p data/live
if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "alert engine already running (PID $(cat "$PID_FILE"))"; exit 0
fi
free_gb=$(df -g . | awk 'NR==2 {print $4}')
if [ "$free_gb" -lt 4 ]; then echo "only ${free_gb} GB free disk (< 4 GB): not starting" >&2; exit 1; fi
if ! nc -z 127.0.0.1 9092 2>/dev/null; then echo "Kafka is not running: scripts/start_kafka.sh" >&2; exit 1; fi
nohup .venv/bin/python -u -m src.stream_alerts "$@" >> "$LOG" 2>&1 &
echo $! > "$PID_FILE"
sleep 5
kill -0 "$(cat "$PID_FILE")" 2>/dev/null || { echo "alert engine failed to start; see $LOG" >&2; rm -f "$PID_FILE"; exit 1; }
echo "alert engine started (PID $(cat "$PID_FILE")); log: $LOG; alerts: data/live/alerts/, latest: data/live/latest_state.parquet"
