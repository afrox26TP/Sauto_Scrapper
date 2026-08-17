#!/usr/bin/env bash
set -euo pipefail

APP_ROOT=/opt/autoidx
APP_DIR="$APP_ROOT/app"
VENV_DIR="$APP_ROOT/venv"
TUNNEL_ID=a376e6bf-f9ac-439d-86fa-f3108e5494ad
TUNNEL_NAME=sauto-api-autoidx
CREDENTIAL_SOURCE="/tmp/${TUNNEL_ID}.json"

if [[ ! -f /tmp/autoidx-deploy.tgz ]]; then
  echo "Missing /tmp/autoidx-deploy.tgz" >&2
  exit 1
fi
if [[ ! -f "$CREDENTIAL_SOURCE" ]]; then
  echo "Missing $CREDENTIAL_SOURCE" >&2
  exit 1
fi

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends ca-certificates curl python3 python3-pip python3-venv

if ! command -v cloudflared >/dev/null 2>&1; then
  curl -fsSL -o /tmp/cloudflared.deb \
    https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64.deb
  dpkg -i /tmp/cloudflared.deb
fi

if ! id autoidx >/dev/null 2>&1; then
  useradd --system --home-dir "$APP_ROOT" --create-home --shell /usr/sbin/nologin autoidx
fi

systemctl stop autoidx-api autoidx-static autoidx-cloudflared 2>/dev/null || true
rm -rf "$APP_DIR"
mkdir -p "$APP_DIR"
tar -xzf /tmp/autoidx-deploy.tgz -C "$APP_DIR"
python3 -m venv "$VENV_DIR"
"$VENV_DIR/bin/python" -m pip install --upgrade pip
"$VENV_DIR/bin/python" -m pip install -r "$APP_DIR/requirements.txt"
chown -R autoidx:autoidx "$APP_ROOT"
chmod 600 "$APP_DIR/web-api/.env"

install -d -m 0750 -o root -g autoidx /etc/cloudflared
install -m 0640 -o root -g autoidx "$CREDENTIAL_SOURCE" "/etc/cloudflared/${TUNNEL_ID}.json"
cat >/etc/cloudflared/config.yml <<EOF
tunnel: ${TUNNEL_NAME}
credentials-file: /etc/cloudflared/${TUNNEL_ID}.json

ingress:
  - hostname: api.autoidx.cz
    service: http://127.0.0.1:8000

  - hostname: autoidx.cz
    path: /api/*
    service: http://127.0.0.1:8000
  - hostname: autoidx.cz
    service: http://127.0.0.1:5173

  - hostname: www.autoidx.cz
    path: /api/*
    service: http://127.0.0.1:8000
  - hostname: www.autoidx.cz
    service: http://127.0.0.1:5173

  - hostname: app.autoidx.cz
    path: /api/*
    service: http://127.0.0.1:8000
  - hostname: app.autoidx.cz
    service: http://127.0.0.1:5173

  - service: http_status:404
EOF
chown root:autoidx /etc/cloudflared/config.yml
chmod 0640 /etc/cloudflared/config.yml

cat >/etc/systemd/system/autoidx-api.service <<EOF
[Unit]
Description=AutoIDX FastAPI backend
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=autoidx
Group=autoidx
WorkingDirectory=${APP_DIR}/web-api
Environment=PYTHONUNBUFFERED=1
ExecStart=${VENV_DIR}/bin/python -m uvicorn app:app --host 127.0.0.1 --port 8000
Restart=always
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true
ProtectHome=true

[Install]
WantedBy=multi-user.target
EOF

cat >/etc/systemd/system/autoidx-static.service <<EOF
[Unit]
Description=AutoIDX static frontend fallback
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=autoidx
Group=autoidx
WorkingDirectory=${APP_DIR}/web-ui/dist
ExecStart=${VENV_DIR}/bin/python -m http.server 5173 --bind 127.0.0.1
Restart=always
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true
ProtectHome=true

[Install]
WantedBy=multi-user.target
EOF

cat >/etc/systemd/system/autoidx-cloudflared.service <<EOF
[Unit]
Description=AutoIDX Cloudflare Tunnel
After=network-online.target autoidx-api.service autoidx-static.service
Wants=network-online.target
Requires=autoidx-api.service autoidx-static.service

[Service]
Type=simple
User=autoidx
Group=autoidx
ExecStart=/usr/bin/cloudflared --no-autoupdate --config /etc/cloudflared/config.yml tunnel run ${TUNNEL_NAME}
Restart=always
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true
ProtectHome=true
ProtectSystem=full

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now autoidx-api autoidx-static autoidx-cloudflared
rm -f /tmp/autoidx-deploy.tgz "$CREDENTIAL_SOURCE" /tmp/cloudflared.deb

for service in autoidx-api autoidx-static autoidx-cloudflared; do
  systemctl is-active --quiet "$service"
  echo "$service: active"
done
curl -fsS http://127.0.0.1:8000/api/health >/dev/null
curl -fsS http://127.0.0.1:5173 >/dev/null
echo "Local health checks passed."
