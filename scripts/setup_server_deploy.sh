#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_DIR="${PROJECT_DIR:-$ROOT_DIR}"
APP_HOST="${APP_HOST:-127.0.0.1}"
APP_PORT="${APP_PORT:-8501}"
SERVER_NAME="${SERVER_NAME:-_}"
INSTALL_NGINX=1
INSTALL_SYSTEMD=1

usage() {
  cat <<'EOF'
Usage: bash scripts/setup_server_deploy.sh [options]

Options:
  --project-dir <path>   Project root path (default: current repo root)
  --server-name <name>   Nginx server_name (domain or IP, default: _)
  --app-port <port>      Internal webapp port (default: 8501)
  --skip-nginx           Do not configure nginx
  --skip-systemd         Do not configure systemd services
  -h, --help             Show this help

This script:
1) bootstraps python deps via scripts/bootstrap_vm.sh
2) creates systemd units for webapp + worker
3) configures nginx on port 80 as reverse proxy to webapp
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-dir)
      PROJECT_DIR="$2"
      shift 2
      ;;
    --server-name)
      SERVER_NAME="$2"
      shift 2
      ;;
    --app-port)
      APP_PORT="$2"
      shift 2
      ;;
    --skip-nginx)
      INSTALL_NGINX=0
      shift
      ;;
    --skip-systemd)
      INSTALL_SYSTEMD=0
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

if [[ "$EUID" -ne 0 ]]; then
  echo "[ERR] Run as root (or with sudo)." >&2
  exit 1
fi

if [[ ! -d "$PROJECT_DIR" ]]; then
  echo "[ERR] project dir not found: $PROJECT_DIR" >&2
  exit 1
fi

if ! id -u "${SUDO_USER:-}" >/dev/null 2>&1; then
  echo "[ERR] Could not detect non-root user (SUDO_USER)." >&2
  echo "      Launch with: sudo bash scripts/setup_server_deploy.sh" >&2
  exit 1
fi

APP_USER="$SUDO_USER"
PROJECT_DIR="$(cd "$PROJECT_DIR" && pwd)"

echo "[INFO] project dir: $PROJECT_DIR"
echo "[INFO] app user: $APP_USER"
echo "[INFO] internal app bind: $APP_HOST:$APP_PORT"
echo "[INFO] nginx server_name: $SERVER_NAME"

cd "$PROJECT_DIR"

if [[ -f "$PROJECT_DIR/scripts/bootstrap_vm.sh" ]]; then
  echo "[INFO] bootstrapping python environment"
  sudo -u "$APP_USER" bash "$PROJECT_DIR/scripts/bootstrap_vm.sh"
else
  echo "[ERR] missing bootstrap script: $PROJECT_DIR/scripts/bootstrap_vm.sh" >&2
  exit 1
fi

if [[ "$INSTALL_SYSTEMD" -eq 1 ]]; then
  echo "[INFO] writing systemd services"
  cat > /etc/systemd/system/beam-webapp.service <<EOF
[Unit]
Description=BEAM web application (uvicorn)
After=network-online.target
Wants=network-online.target
StartLimitIntervalSec=60
StartLimitBurst=10

[Service]
Type=simple
User=$APP_USER
Group=$APP_USER
WorkingDirectory=$PROJECT_DIR
EnvironmentFile=-$PROJECT_DIR/.env
Environment=WEBAPP_HOST=$APP_HOST
Environment=WEBAPP_PORT=$APP_PORT
ExecStartPre=/usr/bin/test -x $PROJECT_DIR/.venv/bin/python
ExecStart=$PROJECT_DIR/.venv/bin/python -m uvicorn webapp.main:app --host $APP_HOST --port $APP_PORT
Restart=on-failure
RestartSec=2
TimeoutStartSec=30
KillMode=mixed
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ProtectHome=false
ReadWritePaths=$PROJECT_DIR

[Install]
WantedBy=multi-user.target
EOF

  cat > /etc/systemd/system/beam-worker.service <<EOF
[Unit]
Description=BEAM background worker
After=network-online.target beam-webapp.service
Wants=network-online.target
StartLimitIntervalSec=60
StartLimitBurst=10

[Service]
Type=simple
User=$APP_USER
Group=$APP_USER
WorkingDirectory=$PROJECT_DIR
EnvironmentFile=-$PROJECT_DIR/.env
Environment=MAX_CONCURRENT_JOBS=2
Environment=JOB_POLL_INTERVAL=1
ExecStartPre=/usr/bin/test -x $PROJECT_DIR/.venv/bin/python
ExecStart=$PROJECT_DIR/.venv/bin/python -m worker.run
Restart=on-failure
RestartSec=2
TimeoutStartSec=30
KillMode=mixed
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ProtectHome=false
ReadWritePaths=$PROJECT_DIR

[Install]
WantedBy=multi-user.target
EOF

  systemctl daemon-reload
  systemctl enable --now beam-webapp.service
  systemctl enable --now beam-worker.service
fi

if [[ "$INSTALL_NGINX" -eq 1 ]]; then
  echo "[INFO] installing/configuring nginx"
  if command -v apt-get >/dev/null 2>&1; then
    apt-get update
    apt-get install -y nginx
  elif command -v dnf >/dev/null 2>&1; then
    dnf install -y nginx
  elif command -v yum >/dev/null 2>&1; then
    yum install -y nginx
  else
    echo "[ERR] unsupported package manager (need apt/dnf/yum)." >&2
    exit 1
  fi

  cat > /etc/nginx/sites-available/beam-app <<EOF
server {
    listen 80;
    server_name $SERVER_NAME;

    client_max_body_size 100M;

    location / {
        proxy_pass http://$APP_HOST:$APP_PORT;
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
}
EOF

  ln -sfn /etc/nginx/sites-available/beam-app /etc/nginx/sites-enabled/beam-app
  rm -f /etc/nginx/sites-enabled/default

  nginx -t
  systemctl enable --now nginx
  systemctl restart nginx
fi

echo "[OK] deployment setup finished"
echo "[INFO] web url: http://$SERVER_NAME (port 80)"
echo "[INFO] service status:"
systemctl --no-pager --full status beam-webapp.service | sed -n '1,12p' || true
systemctl --no-pager --full status beam-worker.service | sed -n '1,12p' || true
systemctl --no-pager --full status nginx | sed -n '1,12p' || true
echo "[INFO] useful commands:"
echo "  sudo systemctl restart beam-webapp beam-worker nginx"
echo "  sudo journalctl -u beam-webapp -u beam-worker -f"
