#!/usr/bin/env bash
# Stage 2: features -> train/evaluate -> alert evaluation -> charts. Stops at the first failing step.
#
#   scripts/run_stage2.sh [sample pct] [log file]
set -euo pipefail
cd "$(dirname "$0")/.."
PCT="${1:-20}"
LOG="${2:-outputs/metrics/build_stage2.log}"
PY=.venv/bin/python

step() {
  echo "=== $*"
  set +e
  "$PY" -u -m "$@" 2>&1 | tee -a "$LOG" | grep --line-buffered -E "progress|\[disk\]|best model|strong|\.png|Error|Traceback|STOP"
  local rc=${PIPESTATUS[0]}
  set -e
  if [ "$rc" -ne 0 ]; then
    echo "FAILED ($rc): $* -- see $LOG" >&2
    exit "$rc"
  fi
}

step src.build_features
step src.train_model --sample-pct "$PCT"
step src.eval_alerts
step src.model_charts
[ -d data/tmp_spark ] && echo "warning: data/tmp_spark left behind" >&2 || true
echo "=== done"
