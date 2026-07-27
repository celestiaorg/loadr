#!/usr/bin/env bash
set -euxo pipefail

# ---- install loadr ----------------------------------------------------------
curl -fsSL --retry 5 --retry-all-errors -o /tmp/loadr.tar.gz "${binary_url}"
tar xzf /tmp/loadr.tar.gz -C /tmp
install -m 0755 /tmp/loadr-*/loadr /usr/local/bin/loadr

# ---- kernel tuning for a high-connection-rate generator ---------------------
# The binding constraints at ~150k+ rps/agent are fds, ephemeral ports and
# socket backlogs — not bandwidth. Keep-alive does most of the work; these
# stop the kernel from getting in the way of the rest.
cat >/etc/sysctl.d/90-loadr.conf <<'SYSCTL'
fs.file-max = 2097152
net.ipv4.ip_local_port_range = 1024 65535
net.ipv4.tcp_tw_reuse = 1
net.ipv4.tcp_fin_timeout = 15
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65535
net.core.netdev_max_backlog = 250000
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
SYSCTL
sysctl --system

# ---- run --------------------------------------------------------------------
# Restart=always doubles as boot ordering: if the agent comes up before the
# controller, it just retries until registration succeeds (agents also
# reconnect with backoff on their own once registered).
cat >/etc/systemd/system/loadr-agent.service <<'UNIT'
[Unit]
Description=loadr agent ${agent_index} (2M rps demo)
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/loadr agent --join ${controller_ip}:7625 --name agent-${agent_index}-%H
Restart=always
RestartSec=2
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable --now loadr-agent
