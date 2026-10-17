#!/usr/bin/env bash
# Build the dashboard's replay (demo) data from recorded status in data/live, without touching
# the live topics, checkpoint or outputs: separate topics, checkpoint and output (data/replay_out).
#
#   scripts/make_replay.sh                                   # busiest recorded 3-hour window
#   scripts/make_replay.sh 2026-10-16T20:00:00Z 2026-10-16T23:00:00Z
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=data/replay_out; CK=data/checkpoints/replay_demo
TOPIC=bikeflow_replay_demo; ALERT_TOPIC=bikeflow_replay_demo_alerts
WARMUP_MIN=35
PY=.venv/bin/python
export KAFKA_JVM_PERFORMANCE_OPTS="-server -XX:+UseG1GC -Djava.awt.headless=true"
K="$(brew --prefix kafka)/bin"
say() { echo "[$(date +%H:%M:%S)] $*"; }

free_gb=$(df -g . | awk 'NR==2 {print $4}')
[ "$free_gb" -ge 4 ] || { say "STOP: only ${free_gb} GB free disk (< 4 GB)"; exit 1; }
nc -z 127.0.0.1 9092 >/dev/null 2>&1 || { say "Kafka not running: scripts/start_kafka.sh"; exit 1; }

if [ $# -ge 2 ]; then START="$1"; END="$2"; else
  say "START picking the busiest recorded 3-hour window"
  W=$("$PY" -m src.replay_window --status-dir data/live/status --hours 3 2>/dev/null)
  START=$(echo "$W" | "$PY" -c "import sys,json; print(json.load(sys.stdin)['start'])")
  END=$(echo "$W" | "$PY" -c "import sys,json; print(json.load(sys.stdin)['end'])")
  say "END window $START -> $END"
fi
WSTART=$("$PY" -c "import pandas as pd; print((pd.Timestamp('$START') - pd.Timedelta(minutes=$WARMUP_MIN)).strftime('%Y-%m-%dT%H:%M:%SZ'))")

say "START copy recorded status (read-only source data/live/status -> $OUT/source)"
rm -rf "$OUT" "$CK"; mkdir -p "$OUT/source"
cp -R data/live/status "$OUT/source/status"
find "$OUT/source/status" -name ".*" -delete
say "END copy ($(du -sh "$OUT/source/status" | cut -f1))"

say "START temporary replay topics"
for t in "$TOPIC" "$ALERT_TOPIC"; do "$K/kafka-topics" --bootstrap-server localhost:9092 --delete --if-exists --topic "$t" >/dev/null 2>&1 || true; done
sleep 2
for t in "$TOPIC" "$ALERT_TOPIC"; do
  "$K/kafka-topics" --bootstrap-server localhost:9092 --create --topic "$t" --partitions 1 --replication-factor 1 >/dev/null
done
say "END temporary replay topics"

say "START replay $WSTART -> $END (incl. ${WARMUP_MIN} min warm-up) into $TOPIC"
"$PY" -u -m src.replay --source "$OUT/source/status" --topic "$TOPIC" --speed 0 --start "$WSTART" --end "$END" \
  2>&1 | grep -E "^\{|replay (START|END)"
say "END replay"

say "START alert engine over the replay (batches of ~one feed refresh, state history on)"
set +e
"$PY" -u -m src.stream_alerts --topic "$TOPIC" --alert-topic "$ALERT_TOPIC" --starting-offsets earliest \
  --available-now --max-offsets 2500 --state-history --out-dir "$OUT" --checkpoint "$CK" > "$OUT/stream.log" 2>&1
rc=$?
set -e
grep -E "^\[" "$OUT/stream.log" | tail -2
[ "$rc" -eq 0 ] || { say "FAILED alert engine ($rc); see $OUT/stream.log"; exit "$rc"; }
say "END alert engine"

say "START scoring"
"$PY" -m src.score_alerts --alerts-out "$OUT" --status-dir "$OUT/source/status" --output "$OUT/alert_scores.json" >/dev/null
say "END scoring"

"$PY" - "$OUT" "$START" "$END" "$WARMUP_MIN" <<'PY'
import json, sys
from pathlib import Path
out, start, end, warm = Path(sys.argv[1]), sys.argv[2], sys.argv[3], int(sys.argv[4])
batches = [json.loads(l) for l in open(out / "alert_batches.jsonl")]
meta = {"window_start": start, "window_end": end, "warmup_min": warm, "batches": len(batches),
        "batches_in_window": sum(b["as_of"] >= start for b in batches),
        "first_as_of": batches[0]["as_of"], "last_as_of": batches[-1]["as_of"]}
(out / "replay_meta.json").write_text(json.dumps(meta, indent=2))
print(json.dumps(meta))
PY
for t in "$TOPIC" "$ALERT_TOPIC"; do "$K/kafka-topics" --bootstrap-server localhost:9092 --delete --topic "$t"; done
say "replay topics deleted; DONE -> $OUT"
