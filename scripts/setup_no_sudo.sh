#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

WITH_DEV=0
AUTO_START=1

usage() {
  cat <<'EOF'
Usage: bash scripts/setup_no_sudo.sh [options]

Options:
  --dev         Also install requirements-dev.txt
  --no-start    Prepare environment but do not start services
  -h, --help    Show this help

This script is designed for environments without sudo.
It sets up a user-local virtualenv, installs dependencies, initializes jobs.db,
and starts BEAM on port 8501.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dev)
      WITH_DEV=1
      shift
      ;;
    --no-start)
      AUTO_START=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERR] Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if ! command -v python3 >/dev/null 2>&1; then
  echo "[ERR] python3 not found in PATH." >&2
  exit 1
fi

PY_VER="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")')"
echo "[INFO] python3 detected: ${PY_VER}"

if [[ ! -d .venv ]]; then
  echo "[INFO] creating .venv"
  if python3 -m venv .venv >/dev/null 2>&1; then
    echo "[OK] virtualenv created with python3 -m venv"
  else
    echo "[WARN] python3 -m venv failed (likely missing python3-venv). Falling back to user-local virtualenv."
    python3 -m pip install --user --upgrade pip virtualenv
    "$HOME/.local/bin/virtualenv" -p python3 .venv
    echo "[OK] virtualenv created with ~/.local/bin/virtualenv"
  fi
fi

# shellcheck disable=SC1091
source .venv/bin/activate

python -m pip install --upgrade pip setuptools wheel
python -m pip install -r requirements.txt
if [[ "$WITH_DEV" -eq 1 ]]; then
  python -m pip install -r requirements-dev.txt
fi

mkdir -p .run logs Download data .run/db_backups

if [[ ! -f .env && -f .env.example ]]; then
  cp .env.example .env
  echo "[INFO] created .env from .env.example"
fi

python - <<'PY'
from beam import db
db.init_db()
print('[OK] jobs.db initialized/migrated')
PY

if [[ "$AUTO_START" -eq 1 ]]; then
  bash scripts/run_server.sh
  bash scripts/check_health.sh
  echo "[OK] setup complete. Open: http://<server-ip>:8501"
else
  echo "[OK] setup complete (not started)."
  echo "[NEXT] run: bash scripts/run_server.sh"
fi
