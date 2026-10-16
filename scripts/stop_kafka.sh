#!/usr/bin/env bash
# Stop the project's Kafka broker cleanly (SIGTERM, wait for controlled shutdown).
set -euo pipefail
cd "$(dirname "$0")/.."
PID_FILE=data/kafka/kafka.pid
if [ ! -f "$PID_FILE" ] || ! kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "Kafka not running"; rm -f "$PID_FILE"; exit 0
fi
PID=$(cat "$PID_FILE")
if ! ps -p "$PID" -o command= | grep -q "kafka.Kafka"; then
  echo "PID $PID is not Kafka; not touching it" >&2; exit 1
fi
kill -TERM "$PID"
for _ in $(seq 1 60); do
  kill -0 "$PID" 2>/dev/null || { rm -f "$PID_FILE"; echo "Kafka stopped (PID $PID)"; exit 0; }
  sleep 1
done
echo "Kafka did not stop within 60 s; sending SIGKILL" >&2
kill -KILL "$PID" 2>/dev/null || true
rm -f "$PID_FILE"
exit 1
