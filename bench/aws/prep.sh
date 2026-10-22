#!/usr/bin/env bash
# Prepare one benchmark node (run as root):  prep.sh server|lg|builder
# - kernel and fd limits for ~100K connections
# - server: local NVMe as XFS on /data, Docker (CouchDB runs in a container)
# - builder: toolchain for couchdb-rs and couchbench
set -euxo pipefail
role=$1

cat > /etc/sysctl.d/90-couchbench.conf <<'EOF'
fs.file-max = 4194304
fs.nr_open = 4194304
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65535
net.core.netdev_max_backlog = 65535
net.ipv4.ip_local_port_range = 1024 65535
net.ipv4.tcp_tw_reuse = 1
vm.max_map_count = 1048576
EOF
sysctl --system >/dev/null
cat > /etc/security/limits.d/90-couchbench.conf <<'EOF'
* soft nofile 2097152
* hard nofile 2097152
EOF

dnf install -y -q python3 procps-ng sysstat tar gzip rsync

case "$role" in
  server)
    dev=$(lsblk -dn -o NAME,MODEL | awk '/Instance Storage/{print "/dev/"$1; exit}')
    if ! mountpoint -q /data; then
      mkfs.xfs -f "$dev"
      mkdir -p /data
      mount -o noatime "$dev" /data
    fi
    chown ec2-user: /data
    dnf install -y -q docker
    # Containers get their fd limit from `docker run --ulimit` (AL2023's
    # docker.service already sets a default-ulimit flag).
    rm -f /etc/docker/daemon.json
    systemctl enable --now docker
    usermod -aG docker ec2-user
    ;;
  builder)
    dnf install -y -q gcc gcc-c++ clang clang-devel cmake make git perl
    ;;
esac
echo "prep $role done"
