#!/usr/bin/env bash
# Stop the dashboard backend and frontend started by scripts/start_dashboard.sh.
set -euo pipefail
cd "$(dirname "$0")/.."
D=data/dashboard
for name in frontend backend; do
  f="$D/$name.pid"
  if [ -f "$f" ] && kill -0 "$(cat "$f")" 2>/dev/null; then
    pid=$(cat "$f")
    if ps -p "$pid" -o command= | grep -qE "uvicorn backend.app.main|vite"; then
      kill -TERM "$pid"
      for _ in $(seq 1 15); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
      kill -0 "$pid" 2>/dev/null && kill -KILL "$pid" 2>/dev/null || true
      echo "$name stopped (PID $pid)"
    else
      echo "PID $pid is not the dashboard $name; not touching it" >&2
    fi
  else
    echo "$name not running"
  fi
  rm -f "$f"
done
