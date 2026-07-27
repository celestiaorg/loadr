#!/usr/bin/env bash
set -euxo pipefail

# ---- install loadr ----------------------------------------------------------
curl -fsSL --retry 5 --retry-all-errors -o /tmp/loadr.tar.gz "${binary_url}"
tar xzf /tmp/loadr.tar.gz -C /tmp
install -m 0755 /tmp/loadr-*/loadr /usr/local/bin/loadr

# ---- kernel: the controller terminates one gRPC stream per agent plus the UI;
# defaults are nearly enough, raise fds and backlog anyway ---------------------
cat >/etc/sysctl.d/90-loadr.conf <<'SYSCTL'
fs.file-max = 1048576
net.core.somaxconn = 65535
SYSCTL
sysctl --system

# ---- run --------------------------------------------------------------------
cat >/etc/systemd/system/loadr-controller.service <<'UNIT'
[Unit]
Description=loadr controller (2M rps demo)
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/loadr controller --bind 0.0.0.0:7625 --ui-bind 0.0.0.0:6464
Restart=always
RestartSec=2
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable --now loadr-controller
