#!/usr/bin/env bash
# Runs ON the ephemeral PR-node droplet (zakura-pr-node.yml): mounts the cloned
# state volume, checks out the PR commit in the baked repo clone, builds zakurad
# incrementally against the baked cargo cache via deploy.py (over the baked
# root@localhost loopback), deploys it as the zakurad systemd service, and
# monitors it for the requested duration.
#
# Config via /root/run.env (sourced by the caller before exec):
#   GH_REPO / GH_CLONE_TOKEN  repo slug + per-run token for the PR-ref fetch
#   MODE                      tip | sandblast | genesis
#   NETWORK                   mainnet | testnet
#   SHA / REFSPEC             commit to test + refspec that reaches it
#   DURATION_MINUTES          how long to monitor the running node
#   VOLUME_NAME               state volume name ("" in genesis mode)
set -euo pipefail

OUT_DIR=/root/out
NOTES="$OUT_DIR/notes.md"
mkdir -p "$OUT_DIR"

note() {
  echo "$1"
  echo "- $1" >> "$NOTES"
}

cloud-init status --wait >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------- #
# State: mount the per-run clone of the baked volume snapshot
# ---------------------------------------------------------------------------- #

if [ "$MODE" = "genesis" ]; then
  STATE_CACHE_DIR=/var/lib/zakura
  STORAGE_MODE=archive
  mkdir -p "$STATE_CACHE_DIR"
  df -h /
else
  DEV="/dev/disk/by-id/scsi-0DO_Volume_${VOLUME_NAME}"
  for _ in $(seq 1 30); do [ -e "$DEV" ] && break; sleep 2; done
  [ -e "$DEV" ] || { echo "state volume device not found: $DEV" >&2; exit 1; }
  mkdir -p /mnt/snapshots
  mount "$DEV" /mnt/snapshots
  STATE_CACHE_DIR="/mnt/snapshots/${MODE}"
  case "$MODE" in
    tip)       STORAGE_MODE=pruned ;;
    sandblast) STORAGE_MODE=archive ;;
    *)         echo "unknown snapshot mode: $MODE" >&2; exit 1 ;;
  esac
  [ -d "$STATE_CACHE_DIR" ] || { echo "no ${MODE}/ state on the volume" >&2; exit 1; }
  df -h /mnt/snapshots
fi

# ---------------------------------------------------------------------------- #
# Source: fetch the PR ref into the baked clone
# ---------------------------------------------------------------------------- #

cd /root/zakura
# git-over-HTTPS wants basic auth (the bearer form is API-only); this is the
# same header actions/checkout configures.
GIT_AUTH=$(printf 'x-access-token:%s' "${GH_CLONE_TOKEN}" | base64 -w0)
git -c http.extraheader="AUTHORIZATION: basic ${GIT_AUTH}" \
  fetch --no-tags origin "${REFSPEC}"
git checkout --detach "${SHA}"

# ---------------------------------------------------------------------------- #
# Preflight: baked state DB format vs the PR tree's format
# ---------------------------------------------------------------------------- #

CODE_VER=$(grep -oE 'DATABASE_FORMAT_VERSION: .* [0-9]+' zebra-state/src/constants.rs | grep -oE '[0-9]+' | tail -n1)
if [ "$MODE" != "genesis" ]; then
  DIR_VER=$(find "$STATE_CACHE_DIR/state" -mindepth 1 -maxdepth 1 -type d -name 'v*' 2>/dev/null | \
    sed 's#.*/v##' | sort -n | tail -1)
  if [ -z "$DIR_VER" ]; then
    note "**WARNING:** no state/v* directory found under \`$STATE_CACHE_DIR\` — the node will sync from scratch."
  elif [ "$DIR_VER" = "$CODE_VER" ]; then
    note "State DB format v${DIR_VER} matches the PR tree."
  elif [ "$DIR_VER" = "$((CODE_VER - 1))" ]; then
    note "State snapshot is v${DIR_VER}, PR tree is v${CODE_VER}: zakurad restores the previous major format in place (a format upgrade runs during the test)."
  else
    note "**WARNING: DB format mismatch** — snapshot is v${DIR_VER} but the PR tree is v${CODE_VER}. The baked state will be ignored and the node syncs from scratch; re-bake the image or use genesis mode."
  fi
fi

# ---------------------------------------------------------------------------- #
# Build + deploy via deploy.py against the baked loopback SSH identity
# ---------------------------------------------------------------------------- #

case "$NETWORK" in
  mainnet) NET_TOML=Mainnet ;;
  testnet) NET_TOML=Testnet ;;
  *) echo "unknown network: $NETWORK" >&2; exit 1 ;;
esac

cat > /root/fleet.toml <<TOML
[[nodes]]
name = "pr-node"
ssh_string = "root@localhost"
commit = "${SHA}"
network = "${NET_TOML}"
state_cache_dir = "${STATE_CACHE_DIR}"
storage_mode = "${STORAGE_MODE}"
rpc_listen_addr = "127.0.0.1:8232"
rpc_enable_cookie_auth = false
metrics_endpoint = "127.0.0.1:9999"
TOML

export CARGO_TARGET_DIR=/root/cargo-target
BUILD_START=$(date +%s)
python3 deploy/deployer/deploy.py build --config /root/fleet.toml
note "Incremental build took $(( $(date +%s) - BUILD_START ))s (warm baked cache)."

python3 deploy/deployer/deploy.py deploy --config /root/fleet.toml
python3 deploy/deployer/deploy.py status --config /root/fleet.toml || true

# ---------------------------------------------------------------------------- #
# Monitor for the requested duration, then package outputs
# ---------------------------------------------------------------------------- #

MONITOR_RC=0
python3 /root/pr-node-monitor.py \
  --duration-minutes "${DURATION_MINUTES}" \
  --interval 30 \
  --rpc-url http://127.0.0.1:8232 \
  --service zakurad \
  --log-file /var/log/zakura/zakura.log \
  --notes "$NOTES" \
  --meta "mode=${MODE},network=${NETWORK},sha=${SHA}" \
  --out "$OUT_DIR" || MONITOR_RC=$?

tail -n 2000 /var/log/zakura/zakura.log > "$OUT_DIR/zakura-tail.log" 2>/dev/null || true
zstd -T0 -q -f /var/log/zakura/zakura.log -o "$OUT_DIR/zakura-full.log.zst" 2>/dev/null || true

exit "$MONITOR_RC"
