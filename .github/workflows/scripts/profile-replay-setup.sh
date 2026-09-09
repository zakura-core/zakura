#!/usr/bin/env bash
# Prepare only the new replay droplet and its exact cloned state volume.
set -euo pipefail
volume=$1
[[ "$volume" =~ ^zakura-pr-perf-vol-[0-9]+-(baseline|primary)(-nyc1)?$ ]]
cloud-init status --wait >/dev/null 2>&1
systemctl stop apt-daily.timer apt-daily-upgrade.timer
for _ in $(seq 1 180); do
  if ! systemctl is-active --quiet apt-daily.service && ! systemctl is-active --quiet apt-daily-upgrade.service; then break; fi
  sleep 5
done
if systemctl is-active --quiet apt-daily.service || systemctl is-active --quiet apt-daily-upgrade.service; then
  echo 'Package maintenance is still active' >&2
  exit 1
fi
if pgrep -x zakurad >/dev/null; then echo 'Unexpected node on new replay host' >&2; exit 1; fi
# shellcheck source=/dev/null
[[ -f "$HOME/.cargo/env" ]] && . "$HOME/.cargo/env"
mkdir -p /root/out
{ rustc --version; cargo --version; uname -a; } > /root/out/replay-toolchain.txt
dpkg-query -W > /root/out/packages-before.txt
lscpu -J > /root/out/host-cpu.json
DEV="/dev/disk/by-id/scsi-0DO_Volume_${volume}"
for _ in $(seq 1 30); do [[ -e "$DEV" ]] && break; sleep 2; done
[[ -e "$DEV" ]]
mkdir -p /mnt/snapshots
if mountpoint -q /mnt/snapshots; then
  [[ "$(readlink -f "$DEV")" == "$(readlink -f "$(findmnt -n -o SOURCE --target /mnt/snapshots)")" ]]
else
  mount "$DEV" /mnt/snapshots
fi
[[ -d /mnt/snapshots/sandblast ]]
