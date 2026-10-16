#!/usr/bin/env bash
# End-to-end test of the alert engine on a COPY of recorded data, using TEMPORARY Kafka topics.
# Never touches data/live/status or the station_status / bikeflow_alerts topics.
#
#   scripts/test_stream_replay.sh [status dir to copy] [replay speed]
set -euo pipefail
cd "$(dirname "$0")/.."
SRC="${1:-data/test_live/status}"
SPEED="${2:-25}"
OUT=data/test_stream; CK=data/checkpoints/test_stream; COPY=data/replay_copy
IN_TOPIC=bikeflow_replay_test; ALERT_TOPIC=bikeflow_alerts_test
export KAFKA_JVM_PERFORMANCE_OPTS="-server -XX:+UseG1GC -Djava.awt.headless=true"
K="$(brew --prefix kafka)/bin"
say() { echo "[$(date +%H:%M:%S)] $*"; }

free_gb=$(df -g . | awk 'NR==2 {print $4}')
[ "$free_gb" -ge 4 ] || { say "STOP: only ${free_gb} GB free disk (< 4 GB)"; exit 1; }
nc -z 127.0.0.1 9092 || { say "Kafka not running: scripts/start_kafka.sh"; exit 1; }

say "START copy $SRC -> $COPY/status (snapshot)"
rm -rf "$OUT" "$CK" "$COPY"; mkdir -p "$OUT" "$COPY"
cp -R "$SRC" "$COPY/status"
say "END copy ($(du -sh "$COPY/status" | cut -f1))"

say "START temporary topics"
for t in "$IN_TOPIC" "$ALERT_TOPIC"; do
  "$K/kafka-topics" --bootstrap-server localhost:9092 --delete --if-exists --topic "$t" >/dev/null 2>&1 || true
done
sleep 2
"$K/kafka-topics" --bootstrap-server localhost:9092 --create --topic "$IN_TOPIC" --partitions 3 --replication-factor 1 >/dev/null
"$K/kafka-topics" --bootstrap-server localhost:9092 --create --topic "$ALERT_TOPIC" --partitions 1 --replication-factor 1 >/dev/null
say "END temporary topics ($IN_TOPIC, $ALERT_TOPIC)"

say "START alert engine (background, 5 s trigger, stops when idle)"
.venv/bin/python -u -m src.stream_alerts --topic "$IN_TOPIC" --alert-topic "$ALERT_TOPIC" \
  --starting-offsets earliest --trigger-seconds 5 --stop-when-idle 45 \
  --out-dir "$OUT" --checkpoint "$CK" > "$OUT/stream.log" 2>&1 &
SPID=$!
for _ in $(seq 1 90); do grep -q "alert engine START" "$OUT/stream.log" && break; sleep 1; done
sleep 10  # let the first (empty) micro-batch start

say "START replay at ${SPEED}x"
set +e
.venv/bin/python -u -m src.replay --source "$COPY/status" --topic "$IN_TOPIC" --speed "$SPEED" 2>&1 | grep -vE "^\s*$"
rc=${PIPESTATUS[0]}
set -e
[ "$rc" -eq 0 ] || { say "FAILED replay ($rc)"; kill -INT "$SPID" 2>/dev/null || true; exit "$rc"; }
say "END replay"

say "waiting for the alert engine to drain and stop"
set +e
wait "$SPID"; src=$?
set -e
grep -E "^\[" "$OUT/stream.log" | tail -4
[ "$src" -eq 0 ] || { say "FAILED alert engine ($src); see $OUT/stream.log"; exit "$src"; }
say "END alert engine"

say "START scoring"
.venv/bin/python -m src.score_alerts --alerts-out "$OUT" --status-dir "$COPY/status" \
  --output outputs/metrics/alert_scores.json > "$OUT/scores.json"
say "END scoring -> outputs/metrics/alert_scores.json"
"$K/kafka-get-offsets" --bootstrap-server localhost:9092 --topic "$ALERT_TOPIC" | sed 's/^/alerts in Kafka topic /'
for t in "$IN_TOPIC" "$ALERT_TOPIC"; do "$K/kafka-topics" --bootstrap-server localhost:9092 --delete --topic "$t"; done
say "temporary topics deleted; DONE"
