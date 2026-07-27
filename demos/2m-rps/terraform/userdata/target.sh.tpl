#!/usr/bin/env bash
set -euxo pipefail

# ---- build the demo target (tiny dependency-free Go API) --------------------
dnf install -y golang
mkdir -p /opt/demo-target
cat >/opt/demo-target/main.go <<'MAINGO'
${main_go}
MAINGO
(cd /opt/demo-target && go build -o /usr/local/bin/demo-target main.go)

# ---- kernel tuning to absorb ~170k rps of keep-alive traffic -----------------
cat >/etc/sysctl.d/90-demo-target.conf <<'SYSCTL'
fs.file-max = 2097152
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65535
net.core.netdev_max_backlog = 250000
net.ipv4.tcp_fin_timeout = 15
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
SYSCTL
sysctl --system

# ---- run --------------------------------------------------------------------
cat >/etc/systemd/system/demo-target.service <<'UNIT'
[Unit]
Description=demo target API (2M rps demo)
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/demo-target -addr :8080
Restart=always
RestartSec=2
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable --now demo-target
