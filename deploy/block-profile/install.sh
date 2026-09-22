#!/usr/bin/env bash
# Install onto a dedicated Linux host with an already mounted, quota-enabled profiler volume.
set -euo pipefail
if [[ $# != 2 || $(id -u) != 0 ]]; then
  echo "usage (root): install.sh /path/to/zakura-profile-explorer NODE_USER" >&2
  exit 2
fi
binary=$(realpath "$1")
node_user=$2
source_dir=$(cd "$(dirname "$0")" && pwd)
[[ -x "$binary" ]]
id "$node_user" >/dev/null
mountpoint -q /srv/zakura-profile
fstype=$(findmnt -n -o FSTYPE --target /srv/zakura-profile)
options=$(findmnt -n -o OPTIONS --target /srv/zakura-profile)
[[ "$fstype" == ext4 && ",$options," == *,grpquota,* ]] || {
  echo 'Use a dedicated ext4 volume mounted with grpquota before installing. This script never formats disks.' >&2
  exit 1
}
getent group zakura-profile >/dev/null || groupadd --system zakura-profile
id zakura-profile >/dev/null 2>&1 || useradd --system --gid zakura-profile --home-dir /nonexistent --shell /usr/sbin/nologin zakura-profile
usermod -a -G zakura-profile "$node_user"
install -d -m 2770 -o zakura-profile -g zakura-profile /srv/zakura-profile/data
install -d -m 2700 -o zakura-profile -g zakura-profile /srv/zakura-profile/data/reports
# 97,656,250 KiB is exactly 100,000,000,000 bytes. Group ownership covers every producer.
setquota -g zakura-profile 87890625 97656250 0 0 /srv/zakura-profile
quota_state=$(LC_ALL=C quotaon -p -g /srv/zakura-profile)
[[ "$quota_state" == *" is on" ]] || {
  echo 'Group quota enforcement is not confirmed. Enable it before installing profiler services.' >&2
  exit 1
}
install -m 0755 "$binary" /usr/local/bin/zakura-profile-explorer
install -d -m 0755 /opt/zakura-profile
install -m 0755 "$source_dir/sample.py" "$source_dir/report.py" /opt/zakura-profile/
install -m 0644 "$source_dir"/*.service "$source_dir"/*.timer "$source_dir"/*.slice /etc/systemd/system/
systemctl daemon-reload
echo 'Installed. Configure the node socket, then start the services as described in README.md.'
