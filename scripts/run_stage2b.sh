#!/usr/bin/env bash
# Stage 2b: weather -> features -> 4 GBTs (one at a time) -> Skellam evaluation -> charts.
# Stops at the first failing step; prints a timestamped line at the start and end of each step.
#
#   scripts/run_stage2b.sh            # everything
#   SKIP_DATA=1 scripts/run_stage2b.sh  # reuse existing weather + features
set -euo pipefail
cd "$(dirname "$0")/.."
LOG="${LOG:-outputs/metrics/build_stage2b.log}"
PY=.venv/bin/python

step() {
  local label="$1"; shift
  echo "[$(date +%H:%M:%S)] START $label"
  set +e
  "$PY" -u -m "$@" 2>&1 | tee -a "$LOG" | grep --line-buffered -E "^\[|progress|test |Error|Traceback|STOP"
  local rc=${PIPESTATUS[0]}
  set -e
  if [ "$rc" -ne 0 ]; then
    echo "[$(date +%H:%M:%S)] FAILED ($rc): $label -- see $LOG" >&2
    exit "$rc"
  fi
  echo "[$(date +%H:%M:%S)] END $label"
}

if [ -z "${SKIP_DATA:-}" ]; then
  step "weather download" src.weather
  step "features (11 months)" src.build_features --force
fi
# Weather models: 20% station-day sample, 60 iterations, depth 6. Departures, then arrivals.
step "GBT departures (weather)" src.train_flow --target departures --variant weather --sample-pct 20 --max-iter 60 --max-depth 6
step "GBT arrivals (weather)"   src.train_flow --target arrivals   --variant weather --sample-pct 20 --max-iter 60 --max-depth 6
# Ablation without weather: 10% sample, 40 iterations (time budget), depth 6.
step "GBT departures (no weather)" src.train_flow --target departures --variant no_weather --sample-pct 10 --max-iter 40 --max-depth 6
step "GBT arrivals (no weather)"   src.train_flow --target arrivals   --variant no_weather --sample-pct 10 --max-iter 40 --max-depth 6
step "evaluation (Skellam)" src.eval_flow
step "charts" src.flow_charts
[ -n "$(ls -A data/tmp_spark 2>/dev/null)" ] && echo "warning: leftover Spark scratch dirs in data/tmp_spark" >&2 || true
echo "[$(date +%H:%M:%S)] DONE stage 2b"
