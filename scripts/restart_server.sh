#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[INFO] restarting worker + webapp..."

"$ROOT_DIR/scripts/stop_server.sh" || true
sleep 1
"$ROOT_DIR/scripts/run_server.sh"

sleep 1

PID_DIR="$ROOT_DIR/.run"
WORKER_PID_FILE="$PID_DIR/worker.pid"
WEBAPP_PID_FILE="$PID_DIR/webapp.pid"

is_pid_running() {
  local pid="$1"
  [[ -n "$pid" ]] || return 1
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  kill -0 "$pid" >/dev/null 2>&1
}

find_running_pid() {
  local pattern="$1"
  local pid=""
  pid="$(pgrep -f "$pattern" | head -n1 || true)"
  if is_pid_running "$pid"; then
    echo "$pid"
    return 0
  fi
  return 1
}

worker_ok="no"
webapp_ok="no"
if [[ -f "$WORKER_PID_FILE" ]]; then
  pid="$(tr -d '[:space:]' < "$WORKER_PID_FILE" || true)"
  if is_pid_running "$pid"; then
    worker_ok="yes (pid=$pid)"
  fi
fi
if [[ "$worker_ok" == "no" ]]; then
  pid="$(find_running_pid "[p]ython(3)? -m worker.run" || true)"
  if [[ -n "$pid" ]]; then
    echo "$pid" > "$WORKER_PID_FILE"
    worker_ok="yes (pid=$pid, pid file repaired)"
  fi
fi
if [[ -f "$WEBAPP_PID_FILE" ]]; then
  pid="$(tr -d '[:space:]' < "$WEBAPP_PID_FILE" || true)"
  if is_pid_running "$pid"; then
    webapp_ok="yes (pid=$pid)"
  fi
fi
if [[ "$webapp_ok" == "no" ]]; then
  pid="$(find_running_pid "[u]vicorn webapp.main:app.*--port 8501" || true)"
  if [[ -n "$pid" ]]; then
    echo "$pid" > "$WEBAPP_PID_FILE"
    webapp_ok="yes (pid=$pid, pid file repaired)"
  fi
fi

echo "[OK] restart complete"
echo "[INFO] worker running: ${worker_ok}"
echo "[INFO] webapp running: ${webapp_ok}"
