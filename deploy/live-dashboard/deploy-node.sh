#!/usr/bin/env bash
# Activate a completed build on the disposable dashboard node only.
set -euo pipefail
host="${1:?Usage: deploy-node.sh <host> <full-commit> <build-unit>}"
revision="${2:?Supply the exact full build commit}"
unit="${3:?Supply the completed build unit}"
[[ "$host" =~ ^[a-zA-Z0-9][a-zA-Z0-9._@-]*$ ]]
[[ "$revision" =~ ^[a-f0-9]{40}$ ]]
[[ "$unit" =~ ^zakura-dashboard-build(-v[0-9]+)?(\.service)?$ ]]
ssh -o BatchMode=yes "$host" bash -s -- "$revision" "$unit" <<'REMOTE'
set -euo pipefail
revision="$1"
unit="$2"
root=/opt/zakura-dashboard-node
source=/root/workspace/zakura
[[ -f /etc/zakura-dashboard-node/zakurad.toml ]]
! systemctl is-active --quiet zakurad
[[ "$(systemctl show "$unit" -p ActiveState --value)" == inactive ]]
[[ "$(systemctl show "$unit" -p ExecMainStatus --value)" == 0 ]]
[[ "$(git -C "$source" rev-parse HEAD)" == "$revision" ]]
[[ -z "$(git -C "$source" status --porcelain)" ]]
version="$("$source/target/release/zakurad" --version)"
case "$version" in *"${revision:0:8}"*) ;; *) echo "Binary does not match the requested commit" >&2; exit 1;; esac
[[ -S /run/zakura-live-dashboard/node-events.sock ]]
release="$root/releases/$revision"
[[ ! -e "$release" ]]
install -D -m 755 "$source/target/release/zakurad" "$release/bin/zakurad"
ln -s bin/zakurad "$release/zakurad"
previous="$(readlink "$root/current")"
backup="$(mktemp -d "$root/rollback.XXXXXXXX")"
cp /etc/systemd/system/zakura-dashboard-node.service "$backup/node.service"
printf '%s\n' "$previous" > "$backup/previous-release"
rollback() {
  echo "Node activation failed. Restoring the previous binary and unit." >&2
  systemctl stop zakura-dashboard-node || true
  ln -sfn "$previous" "$root/current"
  cp "$backup/node.service" /etc/systemd/system/zakura-dashboard-node.service
  systemctl daemon-reload
  systemctl start zakura-dashboard-node
}
trap rollback ERR
systemctl stop zakura-dashboard-node
ln -sfn "$release" "$root/current"
install -m 644 /opt/zakura-live-dashboard/current/deploy/live-dashboard/standalone/zakura-dashboard-node.service /etc/systemd/system/zakura-dashboard-node.service
systemctl daemon-reload
systemctl start zakura-dashboard-node
ready=false
for attempt in {1..90}; do
  if curl --fail --silent --max-time 2 http://127.0.0.1:8080/ready >/dev/null; then ready=true; break; fi
  [[ "$(systemctl show zakura-dashboard-node -p ExecMainStatus --value)" == 0 ]] || break
  sleep 2
done
[[ "$ready" == true ]]
systemctl is-active --quiet zakura-dashboard-node
trap - ERR
printf 'Activated %s\nRollback files: %s\n' "$version" "$backup"
REMOTE
