#!/usr/bin/env bash
# Deploy a committed branch to the existing mainnet gateway, without touching Zakura.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
dashboard_host="${1:-us-east-0}"
if [[ ! "$dashboard_host" =~ ^[a-zA-Z0-9._@-]+$ ]]; then
  echo "Invalid SSH host" >&2
  exit 1
fi
if [[ -n "$(git status --porcelain -- deploy/live-dashboard deploy/gateway/mainnet/Caddyfile)" ]]; then
  echo "Commit dashboard and gateway changes before deploying." >&2
  exit 1
fi
revision="$(git rev-parse HEAD)"
base="$(git merge-base HEAD origin/main)"
base_config_hash="$(git show "$base:deploy/gateway/mainnet/Caddyfile" | shasum -a 256 | awk '{print $1}')"
git archive --format=tar HEAD deploy/live-dashboard deploy/gateway/mainnet/Caddyfile |
  ssh -o BatchMode=yes "$dashboard_host" "mkdir -p /opt/zakura-live-dashboard/releases/$revision && tar -xf - -C /opt/zakura-live-dashboard/releases/$revision"
ssh -o BatchMode=yes "$dashboard_host" bash -s -- "$revision" "$base_config_hash" <<'REMOTE'
set -euo pipefail
revision="$1"
base_config_hash="$2"
root=/opt/zakura-live-dashboard
release="$root/releases/$revision"
candidate="$release/deploy/gateway/mainnet/Caddyfile"
live_config_hash="$(sha256sum /etc/caddy/Caddyfile | awk '{print $1}')"
candidate_hash="$(sha256sum "$candidate" | awk '{print $1}')"
if [[ "$live_config_hash" != "$base_config_hash" && "$live_config_hash" != "$candidate_hash" ]]; then
  echo "Live Caddyfile differs from both base and candidate. Reconcile it before deploying." >&2
  exit 1
fi
caddy validate --config "$candidate" --adapter caddyfile
python3 -m unittest discover -s "$release/deploy/live-dashboard/tests" -q
previous="$(readlink "$root/current" || true)"
backup="$(mktemp -d "$root/rollback.XXXXXXXX")"
cp /etc/caddy/Caddyfile "$backup/Caddyfile"
if [[ -f "$root/build.env" ]]; then cp "$root/build.env" "$backup/build.env"; fi
if [[ -f /etc/systemd/system/zakura-live-dashboard.service ]]; then
  cp /etc/systemd/system/zakura-live-dashboard.service "$backup/dashboard.service"
fi
rollback() {
  echo "Dashboard deployment failed. Restoring previous configuration." >&2
  cp "$backup/Caddyfile" /etc/caddy/Caddyfile
  systemctl reload caddy
  if [[ -n "$previous" ]]; then
    ln -sfn "$previous" "$root/current"
    cp "$backup/build.env" "$root/build.env"
    cp "$backup/dashboard.service" /etc/systemd/system/zakura-live-dashboard.service
    systemctl daemon-reload
    systemctl restart zakura-live-dashboard
  else
    systemctl disable --now zakura-live-dashboard || true
  fi
}
trap rollback ERR
ln -sfn "$release" "$root/current"
printf 'DASHBOARD_BUILD=%s\n' "$revision" > "$root/build.env"
chmod 644 "$root/build.env"
install -m 644 "$release/deploy/live-dashboard/dashboard.service" /etc/systemd/system/zakura-live-dashboard.service
systemctl daemon-reload
systemctl enable zakura-live-dashboard
systemctl restart zakura-live-dashboard
ready=false
for attempt in {1..15}; do
  if curl --fail --silent --max-time 2 http://127.0.0.1:8095/healthz >/dev/null; then ready=true; break; fi
  sleep 2
done
[[ "$ready" == true ]]
install -m 644 "$candidate" /etc/caddy/Caddyfile
systemctl reload caddy
curl --fail --silent --max-time 10 https://status-mainnet.valargroup.dev/live/healthz
curl --fail --silent --max-time 10 https://status-mainnet.valargroup.dev/data >/dev/null
curl --fail --silent --max-time 10 https://zakura-broadcast.valargroup.dev/healthz >/dev/null
trap - ERR
echo
echo "Live: https://status-mainnet.valargroup.dev/live/"
echo "Build: $revision"
echo "Rollback files: $backup"
REMOTE
