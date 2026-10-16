#!/usr/bin/env bash
# Stop the collector cleanly: SIGTERM -> it flushes buffered rows and exits.
set -euo pipefail
cd "$(dirname "$0")/.."
PID_FILE=data/live/collector.pid

if [ ! -f "$PID_FILE" ]; then
  echo "no PID file ($PID_FILE): collector not running?"
  exit 0
fi
PID=$(cat "$PID_FILE")
if ! kill -0 "$PID" 2>/dev/null; then
  echo "PID $PID is not running; removing stale PID file"
  rm -f "$PID_FILE"
  exit 0
fi
if ! ps -p "$PID" -o command= | grep -q "src.collector"; then
  echo "PID $PID is not the collector ($(ps -p "$PID" -o command=)); not touching it" >&2
  exit 1
fi

kill -TERM "$PID"
for _ in $(seq 1 30); do
  kill -0 "$PID" 2>/dev/null || { rm -f "$PID_FILE"; echo "collector stopped (PID $PID), buffers flushed"; exit 0; }
  sleep 1
done
echo "collector did not exit within 30 s; sending SIGKILL (rows buffered since the last flush are lost)" >&2
kill -KILL "$PID" 2>/dev/null || true
rm -f "$PID_FILE"
exit 1
