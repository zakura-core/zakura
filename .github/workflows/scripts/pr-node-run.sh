#!/usr/bin/env bash
# Runs ON the ephemeral PR-node droplet (zakura-pr-node.yml): mounts the cloned
# state volume, checks out the PR commit in the baked repo clone, builds zakurad
# incrementally against the baked cargo cache via deploy.py (over the baked
# root@localhost loopback), deploys it as the zakurad systemd service, and
# monitors it for the requested duration.
#
# Config via /root/run.env (sourced by the caller before exec):
#   GH_REPO / GH_CLONE_TOKEN  repo slug + per-run token for the PR-ref fetch
#   MODE                      tip | pre-checkpoint | sandblast | genesis
#   NETWORK                   mainnet | testnet
#   SHA / REFSPEC             commit to test + refspec that reaches it
#   DURATION_MINUTES          how long to monitor the running node
#   VOLUME_NAME               state volume name ("" in genesis mode)
#   P2P_STACK                 default | legacy | zakura | dual
#   MAX_CKPT                  required handoff height in pre-checkpoint mode
#   SNAPSHOT_HEIGHT           baked state height, when encoded in the snapshot name
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
  # pre-checkpoint boots the tip/ state of an older baked volume, so the run
  # syncs through the last checkpoint handoff instead of starting above it.
  case "$MODE" in
    tip)            STATE_SUBDIR=tip;       STORAGE_MODE=pruned ;;
    pre-checkpoint) STATE_SUBDIR=tip;       STORAGE_MODE=pruned ;;
    sandblast)      STATE_SUBDIR=sandblast; STORAGE_MODE=archive ;;
    *)              echo "unknown snapshot mode: $MODE" >&2; exit 1 ;;
  esac
  STATE_CACHE_DIR="/mnt/snapshots/${STATE_SUBDIR}"
  [ -d "$STATE_CACHE_DIR" ] || { echo "no ${MODE}/ state on the volume" >&2; exit 1; }
  df -h /mnt/snapshots
fi

# This branch is a disposable harness. The binaries still come from the exact PR head.
[ "$SHA" = "343dec5790b9318528bed191243ed91af75cbf7d" ] || { echo "wrong PR head" >&2; exit 1; }
[ "$MODE" = "tip" ] && [ "$NETWORK" = "mainnet" ] && [ "$P2P_STACK" = "dual" ] || {
  echo "paired smoke requires tip/mainnet/dual for the seed" >&2; exit 1;
}
PAIRED_STATE_CACHE_DIR=/mnt/snapshots/paired-client
[ ! -e "$PAIRED_STATE_CACHE_DIR" ] || { echo "paired fixture already exists" >&2; exit 1; }
TASK_COPY_BYTES=$(du -sb "$STATE_CACHE_DIR" | cut -f1)
TASK_FREE_BYTES=$(df -B1 --output=avail /mnt/snapshots | tail -1 | tr -d ' ')
[ "$TASK_FREE_BYTES" -gt "$(( TASK_COPY_BYTES + 10 * 1024 * 1024 * 1024 ))" ] || {
  echo "not enough free space for a separate downloader snapshot" >&2; exit 1;
}
# The primary snapshot has not been opened by the node yet. Never copy a live DB.
cp -a --reflink=auto "$STATE_CACHE_DIR" "$PAIRED_STATE_CACHE_DIR"
note "Copied the unopened snapshot for a separate native-only downloader."

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
rm -f /root/run.env
unset GH_CLONE_TOKEN GIT_AUTH

# ---------------------------------------------------------------------------- #
# Preflight: baked state DB format vs the PR tree's format
# ---------------------------------------------------------------------------- #

CODE_VER=$(grep -oE 'DATABASE_FORMAT_VERSION: .* [0-9]+' crates/zakura-state/src/constants.rs | grep -oE '[0-9]+' | tail -n1)
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

# Enforce the loopback SSH config regardless of what the image baked: this
# droplet regenerated its host keys on first boot, so any recorded localhost
# host key is stale and would fail deploy.py's connections as a changed key.
cat > /root/.ssh/config <<'CFG'
Host localhost
    IdentityFile /root/.ssh/pr_node_loopback
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
CFG
chmod 600 /root/.ssh/config
rm -f /root/.ssh/known_hosts
ssh -o BatchMode=yes root@localhost true

case "$NETWORK" in
  mainnet) NET_TOML=Mainnet ;;
  testnet) NET_TOML=Testnet ;;
  *) echo "unknown network: $NETWORK" >&2; exit 1 ;;
esac
case "$P2P_STACK" in
  default|legacy|zakura|dual) ;;
  *) echo "unknown P2P stack: $P2P_STACK" >&2; exit 1 ;;
esac

cat > /root/fleet.toml <<TOML
[[nodes]]
name = "pr-node"
ssh_string = "root@localhost"
commit = "${SHA}"
network = "${NET_TOML}"
state_cache_dir = "${STATE_CACHE_DIR}"
storage_mode = "${STORAGE_MODE}"
p2p_stack = "legacy"
checkpoint_sync = true
vct_fast_sync = false
rpc_listen_addr = "127.0.0.1:8232"
rpc_enable_cookie_auth = false
metrics_endpoint = "127.0.0.1:9999"

[nodes.zakura]
listen_addr = "127.0.0.1:8234"
bootstrap_peers = []
dev_network = "pr945-mainnet-smoke-20260911"
trace_dir = "/var/log/zakura/seed-traces"
TOML

export CARGO_TARGET_DIR=/root/cargo-target
BUILD_START=$(date +%s)
python3 deploy/deployer/deploy.py build --config /root/fleet.toml
note "Incremental build took $(( $(date +%s) - BUILD_START ))s (warm baked cache)."

# Read the restored DB directly before networking starts. Snapshot names are a
# picker optimization, not trusted proof of the handoff start height.
MONITOR_CROSSING_ARGS=()
if [ "$MODE" = "pre-checkpoint" ]; then
  [[ "$MAX_CKPT" =~ ^[0-9]+$ ]] || {
    note "**FAILED:** pre-checkpoint mode requires a numeric max checkpoint."
    exit 1
  }
  MONITOR_CROSSING_ARGS=(
    --required-start-below "$MAX_CKPT"
    --stop-after-height "$MAX_CKPT"
    --required-finalized-at-least "$MAX_CKPT"
    --require-vct-fast-blocks
  )
  cat > /root/tip-height.toml <<TOML
[state]
storage_mode = "$STORAGE_MODE"
TOML
  TIP_OUTPUT=$(
    /root/cargo-target/release/zakurad -c /root/tip-height.toml tip-height \
      --cache-dir "$STATE_CACHE_DIR" \
      --network "$NET_TOML" 2>&1
  ) || {
    note "**FAILED:** could not read the restored database tip before starting the node."
    printf '%s\n' "$TIP_OUTPUT" >&2
    exit 1
  }
  VERIFIED_START_HEIGHT=$(printf '%s\n' "$TIP_OUTPUT" | awk '/^[0-9]+$/ { height=$1 } END { print height }')
  [[ "$VERIFIED_START_HEIGHT" =~ ^[0-9]+$ ]] || {
    note "**FAILED:** restored database tip-height output was not numeric."
    printf '%s\n' "$TIP_OUTPUT" >&2
    exit 1
  }
  if [ "$VERIFIED_START_HEIGHT" -ge "$MAX_CKPT" ]; then
    note "**FAILED: no handoff crossing** — restored database height ${VERIFIED_START_HEIGHT} is at or above max checkpoint ${MAX_CKPT}."
    exit 1
  fi
  MONITOR_CROSSING_ARGS+=(--known-start-height "$VERIFIED_START_HEIGHT")
  if [ -n "$SNAPSHOT_HEIGHT" ]; then
    [[ "$SNAPSHOT_HEIGHT" =~ ^[0-9]+$ ]] || {
      note "**FAILED:** selected snapshot has a non-numeric baked height."
      exit 1
    }
    if [ "$SNAPSHOT_HEIGHT" -ne "$VERIFIED_START_HEIGHT" ]; then
      note "**FAILED:** snapshot name height ${SNAPSHOT_HEIGHT} does not match restored database height ${VERIFIED_START_HEIGHT}."
      exit 1
    fi
  fi
  note "pre-checkpoint: verified database height ${VERIFIED_START_HEIGHT} is $((MAX_CKPT - VERIFIED_START_HEIGHT)) blocks below max checkpoint ${MAX_CKPT}."
fi

python3 deploy/deployer/deploy.py deploy --config /root/fleet.toml
python3 deploy/deployer/deploy.py status --config /root/fleet.toml || true

# ---------------------------------------------------------------------------- #
# Monitor for the requested duration, then package outputs
# ---------------------------------------------------------------------------- #

# Give the native downloader a supplier with complete blocks and tree roots.
# The untouched secondary snapshot remains at its original height.
SEED_CKPT=$(tail -1 crates/zakura-chain/src/parameters/checkpoint/main-checkpoints.txt | cut -d' ' -f1)
[[ "$SEED_CKPT" =~ ^[0-9]+$ ]] || { echo "cannot read seed checkpoint" >&2; exit 1; }
SEED_READY_HEIGHT=$((SEED_CKPT + 200))
note "Priming the seed over legacy P2P with VCT fast sync disabled, through height ${SEED_READY_HEIGHT}. The native downloader keeps VCT fast sync enabled."
PRIME_RC=0
python3 /root/pr-node-monitor.py \
  --duration-minutes 15 --interval 10 \
  --rpc-url http://127.0.0.1:8232 --metrics-url http://127.0.0.1:9999/metrics \
  --service zakurad --log-file /var/log/zakura/zakura.log \
  --stop-after-height "$SEED_READY_HEIGHT" \
  --required-finalized-at-least "$SEED_CKPT" \
  --meta "mode=seed-priming,network=${NETWORK},vct_fast_sync=false,p2p_stack=legacy" \
  --out "$OUT_DIR/seed-priming" || PRIME_RC=$?
systemctl stop zakurad
cp /var/log/zakura/zakura.log "$OUT_DIR/seed-priming/zakura.log"
[ "$PRIME_RC" -eq 0 ] || exit "$PRIME_RC"
python3 - <<'ENABLE_NATIVE_SEED'
from pathlib import Path
p=Path('/root/fleet.toml')
s=p.read_text()
assert 'p2p_stack = "legacy"' in s
p.write_text(s.replace('p2p_stack = "legacy"', 'p2p_stack = "dual"'))
ENABLE_NATIVE_SEED
python3 deploy/deployer/deploy.py deploy --config /root/fleet.toml
note "Seed priming passed. Restarted the same seed binary with the isolated native endpoint enabled."

PAIR_RC=0
python3 -u /root/pr-node-paired-smoke.py \
  --state "$PAIRED_STATE_CACHE_DIR" --duration-minutes "$DURATION_MINUTES" &
PAIR_PID=$!
MONITOR_RC=0
python3 /root/pr-node-monitor.py \
  --duration-minutes "${DURATION_MINUTES}" \
  --interval 30 \
  --rpc-url http://127.0.0.1:8232 \
  --metrics-url http://127.0.0.1:9999/metrics \
  --service zakurad \
  --log-file /var/log/zakura/zakura.log \
  --notes "$NOTES" \
  --meta "mode=${MODE},network=${NETWORK},sha=${SHA}" \
  "${MONITOR_CROSSING_ARGS[@]}" \
  --out "$OUT_DIR" || MONITOR_RC=$?

wait "$PAIR_PID" || PAIR_RC=$?
cp -a /var/log/zakura/seed-traces "$OUT_DIR/seed-traces" 2>/dev/null || true
python3 - <<'PAIR_SUMMARY'
import json
from pathlib import Path
root=Path('/root/out')
pair=json.loads((root/'paired/summary.json').read_text())
summary=json.loads((root/'summary.json').read_text())
summary['paired_smoke']=pair
if not pair['pass']:
    summary['verdict']='failed'
(root/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
with (root/'summary.md').open('a') as f:
    f.write('\n## Paired mainnet smoke\n\n')
    f.write('Two full nodes on one disposable host. The seed follows public mainnet over legacy P2P. The downloader uses only the PR 945 native protocol over QUIC.\n\n')
    f.write('Result: '+('PASS' if pair['pass'] else 'FAIL')+'\n\n')
    for phase in pair['phases']:
        f.write(f"- {phase['phase']}: height {phase['start_height'] if 'start_height' in phase else pair['start_height']} to {phase['height']}, {phase['native_bodies']} native bodies, matching block hash {phase['block_hash']}\n")
    if pair.get('error'): f.write(pair['error']+'\n')
PAIR_SUMMARY
if [ "$PAIR_RC" -ne 0 ]; then MONITOR_RC=$PAIR_RC; fi

tail -n 2000 /var/log/zakura/zakura.log > "$OUT_DIR/zakura-tail.log" 2>/dev/null || true
zstd -T0 -q -f /var/log/zakura/zakura.log -o "$OUT_DIR/zakura-full.log.zst" 2>/dev/null || true

exit "$MONITOR_RC"
