#!/usr/bin/env bash
# Start the live alert engine (src/stream_alerts.py) in the background. Needs Kafka running
# (scripts/start_kafka.sh) and something publishing to station_status (collector --kafka).
# Extra arguments go to src.stream_alerts.
#
# Startup is checked: if the engine dies, the last 15 lines of its log are printed. If the
# failure looks like an incompatible checkpoint (state/offset/checkpoint errors), the checkpoint
# is moved aside (never deleted) to <checkpoint>.incompatible-<time> and the start is retried once.
set -euo pipefail
cd "$(dirname "$0")/.."
PID_FILE="${STREAM_PID:-data/live/stream_alerts.pid}"
LOG="${STREAM_LOG:-data/live/stream_alerts.log}"
CK=data/checkpoints/stream_alerts
args=("$@")
for ((i = 0; i < ${#args[@]}; i++)); do
  if [ "${args[$i]}" = "--checkpoint" ] && [ $((i + 1)) -lt ${#args[@]} ]; then CK="${args[$((i + 1))]}"; fi
done
mkdir -p data/live "$(dirname "$LOG")"
touch "$LOG"

if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "alert engine already running (PID $(cat "$PID_FILE"))"; exit 0
fi
free_gb=$(df -g . | awk 'NR==2 {print $4}')
if [ "$free_gb" -lt 4 ]; then echo "only ${free_gb} GB free disk (< 4 GB): not starting" >&2; exit 1; fi
if ! nc -z 127.0.0.1 9092 2>/dev/null; then echo "Kafka is not running: scripts/start_kafka.sh" >&2; exit 1; fi

# Returns 0 when the engine is up, 1 when it died (log section printed by the caller).
start_once() {
  START_LINE=$(($(wc -l < "$LOG" 2>/dev/null || echo 0) + 1))
  nohup .venv/bin/python -u -m src.stream_alerts ${args[@]+"${args[@]}"} >> "$LOG" 2>&1 &
  echo $! > "$PID_FILE"
  local pid; pid=$(cat "$PID_FILE")
  for _ in $(seq 1 120); do
    if ! kill -0 "$pid" 2>/dev/null; then return 1; fi
    # healthy once the query is running and has either finished a batch or survived 60 s
    if tail -n +"$START_LINE" "$LOG" | grep -q "alert engine START"; then
      if tail -n +"$START_LINE" "$LOG" | grep -qE "^\[.*\] batch [0-9]+:"; then return 0; fi
      [ "$SECONDS" -ge "$((T0 + 60))" ] && return 0
    fi
    sleep 1
  done
  kill -0 "$pid" 2>/dev/null
}

show_failure() {
  local section; section=$(tail -n +"$START_LINE" "$LOG" | grep -vE "FutureWarning|require_minimum_pandas")
  echo "alert engine failed to start." >&2
  echo "first error: $(echo "$section" | grep -m1 -E "ERROR|Exception|Error:|error:" | cut -c1-300)" >&2
  echo "last 15 lines of $LOG (Java stack frames omitted):" >&2
  echo "$section" | grep -vE "^[[:space:]]+at |^[[:space:]]*\.\.\. [0-9]+ (more|common)" | tail -15 >&2
}

T0=$SECONDS
if start_once; then
  echo "alert engine started (PID $(cat "$PID_FILE")); log: $LOG; alerts: data/live/alerts/, latest: data/live/latest_state.parquet"
  exit 0
fi
show_failure
rm -f "$PID_FILE"
if tail -n +"$START_LINE" "$LOG" | grep -qiE "checkpoint|STATE_STORE|StateSchema|state schema|offset log|INCOMPATIBLE|metadata" \
   && [ -d "$CK" ]; then
  aside="$CK.incompatible-$(date +%Y%m%d-%H%M%S)"
  mv "$CK" "$aside"
  echo "the failure looks checkpoint-related: moved $CK -> $aside and retrying once" >&2
  T0=$SECONDS
  if start_once; then
    echo "alert engine started with a fresh checkpoint (PID $(cat "$PID_FILE")); old checkpoint kept in $aside"
    exit 0
  fi
  show_failure
  rm -f "$PID_FILE"
fi
exit 1
