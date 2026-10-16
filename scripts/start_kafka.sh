#!/usr/bin/env bash
# Start a single-node Kafka broker (KRaft, no ZooKeeper) with a 512 MB heap and its data
# inside the project (data/kafka/, git-ignored, safe to wipe), then create the topics.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$(pwd)
# G1 settings without the option newer JDKs warn about (applies to the broker and the CLI tools).
export KAFKA_JVM_PERFORMANCE_OPTS="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:+ExplicitGCInvokesConcurrent -Djava.awt.headless=true"
KAFKA_BIN="${KAFKA_BIN:-$(brew --prefix kafka)/bin}"
DATA="$ROOT/data/kafka"
CONF="$DATA/server.properties"
PID_FILE="$DATA/kafka.pid"
mkdir -p "$DATA/app-logs"

free_gb=$(df -g "$ROOT" | awk 'NR==2 {print $4}')
if [ "$free_gb" -lt 4 ]; then echo "only ${free_gb} GB free disk (< 4 GB): not starting Kafka" >&2; exit 1; fi

if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "Kafka already running (PID $(cat "$PID_FILE"))"
else
  sed "s|@KAFKA_DATA@|$DATA|" config/kafka/server.properties.template > "$CONF"
  if [ ! -f "$DATA/logs/meta.properties" ]; then
    echo "formatting Kafka storage in $DATA/logs (first start)"
    "$KAFKA_BIN/kafka-storage" format -t "$("$KAFKA_BIN/kafka-storage" random-uuid)" -c "$CONF" >/dev/null
  fi
  export KAFKA_HEAP_OPTS="-Xms256M -Xmx512M"
  export LOG_DIR="$DATA/app-logs"
  "$KAFKA_BIN/kafka-server-start" -daemon "$CONF"
  for _ in $(seq 1 60); do
    if "$KAFKA_BIN/kafka-broker-api-versions" --bootstrap-server localhost:9092 >/dev/null 2>&1; then break; fi
    sleep 1
  done
  PID=$(pgrep -f "kafka.Kafka $CONF" | head -1 || true)
  if [ -z "$PID" ]; then echo "Kafka did not start; see $DATA/app-logs/server.log" >&2; exit 1; fi
  echo "$PID" > "$PID_FILE"
  echo "Kafka started (PID $PID), broker localhost:9092, data in $DATA"
fi

# Topics (idempotent). Retention 24 h is also set per topic.
"$KAFKA_BIN/kafka-topics" --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic station_status --partitions 3 --replication-factor 1 --config retention.ms=86400000 >/dev/null
"$KAFKA_BIN/kafka-topics" --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic bikeflow_alerts --partitions 1 --replication-factor 1 --config retention.ms=86400000 >/dev/null
"$KAFKA_BIN/kafka-topics" --bootstrap-server localhost:9092 --describe --topic station_status,bikeflow_alerts \
  | grep -E "^Topic:" | awk -F'\t' '{print $1" | "$3" | "$4}'
