#!/usr/bin/env bash
# Rebuild stations, demand_15min (+ EDA/charts) and the data-quality summary from
# data/processed/trips_clean. Stops at the first failing step.
#
#   scripts/build_features.sh [log file]
set -euo pipefail
cd "$(dirname "$0")/.."
LOG="${1:-outputs/metrics/build_stage1b.log}"
PY=.venv/bin/python

step() {
  echo "=== $*"
  set +e
  "$PY" -u -m "$@" 2>&1 | tee -a "$LOG" | grep --line-buffered -E "progress|\[disk\]|matched|demand rows|QUARANTINED|Error|Traceback|STOP"
  local rc=${PIPESTATUS[0]}
  set -e
  if [ "$rc" -ne 0 ]; then
    echo "FAILED ($rc): $* -- see $LOG" >&2
    exit "$rc"
  fi
}

step src.stations
step src.aggregate_demand
step src.clean --summary-only
[ -d data/tmp_spark ] && echo "warning: data/tmp_spark left behind" >&2 || true
echo "=== done"
