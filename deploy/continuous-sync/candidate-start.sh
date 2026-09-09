#!/usr/bin/env bash
# Runs only on a freshly provisioned, explicitly owned experiment host.
set -euo pipefail
cd /root/genesis-candidate
python3 - <<'PY'
import json,socket
from pathlib import Path
x=json.loads(Path('owner.json').read_text())
assert x['leg'] in ('baseline','candidate')
assert socket.gethostname()=='zakura-genesis-'+x['launch_run_id']+'-'+x['leg']
assert not Path('/root/zakura').exists()
PY
trap 'systemctl stop zakura.service 2>/dev/null || true' EXIT
export DEBIAN_FRONTEND=noninteractive
export NEEDRESTART_MODE=l
systemctl stop apt-daily.timer apt-daily-upgrade.timer
apt-get -o DPkg::Lock::Timeout=600 update
apt-get -o DPkg::Lock::Timeout=600 install -y build-essential pkg-config clang libclang-dev libssl-dev cmake protobuf-compiler git curl zstd logrotate
systemctl mask --runtime apt-daily.timer apt-daily-upgrade.timer apt-daily.service apt-daily-upgrade.service
systemctl stop apt-daily.service apt-daily-upgrade.service
volume_name=$(python3 -c 'import json; print(json.load(open("owner.json"))["volume_name"])')
volume_device="/dev/disk/by-id/scsi-0DO_Volume_${volume_name}"
[[ "$(blkid -s TYPE -o value "$volume_device")" == ext4 ]]
mkdir -p /var/lib/zakura
mount "$volume_device" /var/lib/zakura
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o /root/rustup-init.sh
sh /root/rustup-init.sh -y --profile minimal --default-toolchain 1.98.1
export PATH="/root/.cargo/bin:$PATH"
export RUSTUP_TOOLCHAIN=1.98.1
git clone --filter=blob:none --no-checkout https://github.com/zakura-core/zakura.git /root/zakura
python3 candidate-host.py
python3 continuous-sync.py --config /root/genesis-candidate/controller.toml run --once
