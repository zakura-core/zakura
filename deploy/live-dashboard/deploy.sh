#!/usr/bin/env bash
# Deploy only the dashboard on its dedicated host. The node is not restarted.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
dashboard_host="${1:?Usage: deploy.sh <dedicated-ssh-host>}"
if [[ ! "$dashboard_host" =~ ^[a-zA-Z0-9][a-zA-Z0-9._@-]*$ ]]; then
  echo "Invalid SSH host" >&2
  exit 1
fi
if [[ -n "$(git status --porcelain -- deploy/live-dashboard)" ]]; then
  echo "Commit dashboard changes before deploying." >&2
  exit 1
fi
revision="$(git rev-parse HEAD)"
git archive --format=tar HEAD deploy/live-dashboard |
  ssh -o BatchMode=yes "$dashboard_host" "mkdir -p /opt/zakura-live-dashboard/releases/$revision && tar -xf - -C /opt/zakura-live-dashboard/releases/$revision"
ssh -o BatchMode=yes "$dashboard_host" bash -s -- "$revision" <<'REMOTE'
set -euo pipefail
revision="$1"
root=/opt/zakura-live-dashboard
release="$root/releases/$revision"
candidate="$release/deploy/live-dashboard/standalone/Caddyfile"
if [[ ! -f /etc/zakura-dashboard-node/zakurad.toml ]] || systemctl is-active --quiet zakurad; then
  echo "This deployment requires a dedicated dashboard host, not a fleet node." >&2
  exit 1
fi
if [[ -e "$root/current" ]] && ! cmp -s /etc/caddy/Caddyfile "$root/current/deploy/live-dashboard/standalone/Caddyfile"; then
  echo "Live Caddyfile has drifted from the previous dashboard release. Reconcile it first." >&2
  exit 1
fi
caddy validate --config "$candidate" --adapter caddyfile
python3 -m unittest discover -s "$release/deploy/live-dashboard/tests" -q
# D-Bus needs a persistent identity for unprivileged systemd property reads.
id zakura-dashboard-web >/dev/null 2>&1 ||
  useradd --system --home-dir /var/lib/zakura-live-dashboard --shell /usr/sbin/nologin zakura-dashboard-web
previous="$(readlink "$root/current" || true)"
backup="$(mktemp -d "$root/rollback.XXXXXXXX")"
cp /etc/caddy/Caddyfile "$backup/Caddyfile"
if [[ -f "$root/build.env" ]]; then cp "$root/build.env" "$backup/build.env"; fi
if [[ -f /etc/systemd/system/zakura-live-dashboard.service ]]; then
  cp /etc/systemd/system/zakura-live-dashboard.service "$backup/dashboard.service"
fi
if [[ -f /etc/systemd/system/zakura-live-dashboard.socket ]]; then
  cp /etc/systemd/system/zakura-live-dashboard.socket "$backup/dashboard.socket"
fi
socket_was_active=false
if systemctl is-active --quiet zakura-live-dashboard.socket; then socket_was_active=true; fi
rollback() {
  systemctl stop zakura-live-dashboard || true
  if [[ "$socket_was_active" == false ]]; then
    systemctl disable --now zakura-live-dashboard.socket || true
  fi
  echo "Dashboard deployment failed. Restoring previous configuration." >&2
  cp "$backup/Caddyfile" /etc/caddy/Caddyfile
  systemctl reload caddy
  if [[ -n "$previous" ]]; then
    ln -sfn "$previous" "$root/current"
    cp "$backup/build.env" "$root/build.env"
    cp "$backup/dashboard.service" /etc/systemd/system/zakura-live-dashboard.service
    if [[ -f "$backup/dashboard.socket" ]]; then
      cp "$backup/dashboard.socket" /etc/systemd/system/zakura-live-dashboard.socket
    fi
    systemctl daemon-reload
    systemctl restart zakura-live-dashboard
  else
    systemctl disable --now zakura-live-dashboard || true
    if [[ "$(readlink "$root/current")" == "$release" ]]; then unlink "$root/current"; fi
  fi
}
trap rollback ERR
ln -sfn "$release" "$root/current"
printf 'DASHBOARD_BUILD=%s\n' "$revision" > "$root/build.env"
chmod 644 "$root/build.env"
install -m 644 "$release/deploy/live-dashboard/dashboard.service" /etc/systemd/system/zakura-live-dashboard.service
# Keep an existing event socket alive while the receiver is replaced.
if [[ "$socket_was_active" == false ]]; then
  systemctl stop zakura-live-dashboard
  if [[ -S /run/zakura-live-dashboard/node-events.sock ]]; then
    unlink /run/zakura-live-dashboard/node-events.sock
  fi
fi
install -d -o zakura-dashboard-web -g zakura-dashboard-web -m 750 /run/zakura-live-dashboard
install -m 644 "$release/deploy/live-dashboard/dashboard.socket" /etc/systemd/system/zakura-live-dashboard.socket
systemctl daemon-reload
systemctl enable --now zakura-live-dashboard.socket
systemctl enable zakura-live-dashboard
systemctl restart zakura-live-dashboard
ready=false
for attempt in {1..15}; do
  if curl --fail --silent --max-time 2 http://127.0.0.1:8095/healthz | python3 -c 'import json,sys; assert json.load(sys.stdin)["build"] == sys.argv[1]' "$revision"; then ready=true; break; fi
  sleep 2
done
[[ "$ready" == true ]]
# Match the node unit's identity and supplementary group without sending fake data.
runuser -u zakura-dashboard-node -g zakura-dashboard-node -G zakura-dashboard-web --   test -w /run/zakura-live-dashboard/node-events.sock
install -m 644 "$candidate" /etc/caddy/Caddyfile
systemctl reload caddy
curl --fail --silent --max-time 10 -H "Host: 146.190.146.239" http://127.0.0.1/api/overview |
  python3 -c 'import json,sys; assert json.load(sys.stdin)["build"] == sys.argv[1]' "$revision"
trap - ERR
echo
echo "Dashboard deployed. Public hostname: https://gui.valargroup.dev/ (requires DNS)."
echo "Build: $revision"
echo "Rollback files: $backup"
REMOTE
