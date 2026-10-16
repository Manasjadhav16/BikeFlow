#!/usr/bin/env bash
# Stop the alert engine gracefully (SIGINT -> the streaming query stops; the checkpoint lets it resume).
set -euo pipefail
cd "$(dirname "$0")/.."
PID_FILE=data/live/stream_alerts.pid
if [ ! -f "$PID_FILE" ] || ! kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "alert engine not running"; rm -f "$PID_FILE"; exit 0
fi
PID=$(cat "$PID_FILE")
if ! ps -p "$PID" -o command= | grep -q "src.stream_alerts"; then
  echo "PID $PID is not the alert engine; not touching it" >&2; exit 1
fi
kill -INT "$PID"
for _ in $(seq 1 90); do
  kill -0 "$PID" 2>/dev/null || { rm -f "$PID_FILE"; echo "alert engine stopped (PID $PID)"; exit 0; }
  sleep 1
done
echo "no exit after 90 s; sending SIGTERM" >&2
kill -TERM "$PID" 2>/dev/null || true
rm -f "$PID_FILE"
exit 1
