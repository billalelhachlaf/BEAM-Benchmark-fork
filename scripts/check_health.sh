#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PORT="${WEBAPP_PORT:-8501}"
BASE_URL="http://127.0.0.1:${PORT}"

is_pid_running() {
  local pid="$1"
  [[ -n "$pid" ]] || return 1
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  kill -0 "$pid" >/dev/null 2>&1
}

find_running_pid() {
  local pattern="$1"
  local pid=""
  if ! command -v pgrep >/dev/null 2>&1; then
    return 1
  fi
  pid="$(pgrep -f "$pattern" | head -n1 || true)"
  if is_pid_running "$pid"; then
    echo "$pid"
    return 0
  fi
  return 1
}

status_pid() {
  local file="$1"
  local name="$2"
  local pattern="$3"
  if [[ ! -f "$file" ]]; then
    local discovered=""
    if discovered="$(find_running_pid "$pattern")"; then
      echo "$discovered" > "$file"
      echo "[OK] $name running (pid=$discovered, pid file created)"
      return
    fi
    echo "[WARN] $name pid file missing"
    return
  fi
  local pid
  pid="$(tr -d '[:space:]' < "$file" || true)"
  if is_pid_running "$pid"; then
    echo "[OK] $name running (pid=$pid)"
  else
    local discovered=""
    if discovered="$(find_running_pid "$pattern")"; then
      echo "$discovered" > "$file"
      echo "[OK] $name running (pid=$discovered, pid file repaired from stale: $pid)"
      return
    fi
    echo "[WARN] $name not running (stale pid file: $pid)"
  fi
}

if [[ ! -f /.dockerenv ]]; then
  status_pid "$ROOT_DIR/.run/webapp.pid" "webapp" "[u]vicorn webapp.main:app.*--port ${PORT}"
  status_pid "$ROOT_DIR/.run/worker.pid" "worker" "[p]ython(3)? -m worker.run"
fi

if curl -fsS "$BASE_URL/" >/dev/null 2>&1; then
  echo "[OK] webapp HTTP reachable at $BASE_URL"
else
  echo "[WARN] webapp HTTP not reachable at $BASE_URL"
fi

python_bin="python"
[[ -x "$ROOT_DIR/.venv/bin/python" ]] && python_bin="$ROOT_DIR/.venv/bin/python"

"$python_bin" - <<'PY'
import sqlite3
from pathlib import Path

db = Path('jobs.db')
if not db.exists():
    print('[WARN] jobs.db not found')
else:
    with sqlite3.connect(db) as conn:
        jobs = conn.execute('select count(*) from jobs').fetchone()[0]
        running = conn.execute("select count(*) from jobs where status in ('running','queued')").fetchone()[0]
    print(f'[OK] jobs.db present | jobs={jobs} active={running}')
PY
