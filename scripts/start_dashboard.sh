#!/usr/bin/env bash
# Start the dashboard: FastAPI backend on :8000 and the Vite dev server on :5173 (both in the
# background). Read-only over data/live and data/replay_out; PIDs and logs in data/dashboard/.
set -euo pipefail
cd "$(dirname "$0")/.."
D="$(pwd)/data/dashboard"
mkdir -p "$D"

running() { [ -f "$1" ] && kill -0 "$(cat "$1")" 2>/dev/null; }

if running "$D/backend.pid"; then echo "backend already running (PID $(cat "$D/backend.pid"))"; else
  nohup .venv/bin/uvicorn backend.app.main:app --host 127.0.0.1 --port 8000 > "$D/backend.log" 2>&1 &
  echo $! > "$D/backend.pid"
fi
if [ ! -d frontend/node_modules ]; then (cd frontend && npm install --no-audit --no-fund --loglevel=error); fi
if running "$D/frontend.pid"; then echo "frontend already running (PID $(cat "$D/frontend.pid"))"; else
  cd frontend
  nohup node node_modules/vite/bin/vite.js --host 127.0.0.1 --port 5173 --strictPort > "$D/frontend.log" 2>&1 &
  echo $! > "$D/frontend.pid"
  cd ..
fi

for port in 8000 5173; do
  for _ in $(seq 1 30); do nc -z 127.0.0.1 "$port" 2>/dev/null && break; sleep 1; done
  nc -z 127.0.0.1 "$port" 2>/dev/null || { echo "port $port did not open; see data/dashboard/*.log" >&2; exit 1; }
done
echo "dashboard: http://127.0.0.1:5173  (API http://127.0.0.1:8000/api/health)"
