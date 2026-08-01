#!/usr/bin/env bash
# Runs ON the bake droplet (zakura-pr-node-bake.yml): installs build deps and
# rustup, clones the repo and warms a release cargo cache, bakes a loopback SSH
# identity so deploy.py can target root@localhost on the run droplets, fills the
# attached per-network volumes with extracted chain state, and cleans the
# droplet for imaging.
#
# Config via /root/bake.env (sourced by the caller before exec):
#   GH_REPO                  owner/name of this repository
#   GH_CLONE_TOKEN           token used once for the clone; the remote URL is
#                            reset token-free afterwards, nothing is baked
#   MAINNET_VOLUME_NAME      DO volume that gets tip/ + sandblast/ mainnet state
#   TESTNET_VOLUME_NAME      DO volume that gets tip/ testnet state
#   APPROACH_VOLUME_NAME     DO volume synced to just below the VCT handoff
#   REBUILD_APPROACH_FROM_SANDBLAST  one-time approach-state rebuild switch
#   TIP_MAINNET_LATEST_JSON  latest.json pointer for the mainnet pruned tip
#   SANDBLAST_URL            pinned pre-spam-region mainnet archive snapshot
#   SANDBLAST_SHA256         its sha256
#   TESTNET_SNAPSHOTS_BASE   testnet snapshots site (serves /snapshots.json)
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

# A freshly-booted droplet runs apt at boot (cloud-init / unattended-upgrades);
# wait for it to release the dpkg lock instead of racing it.
cloud-init status --wait >/dev/null 2>&1 || true
for _ in $(seq 1 120); do pgrep -x apt-get >/dev/null || break; sleep 5; done
apt-get -o DPkg::Lock::Timeout=600 update -qq
apt-get -o DPkg::Lock::Timeout=600 install -y -qq \
  build-essential clang cmake pkg-config libssl-dev protobuf-compiler \
  git curl zstd jq python3
# CPU-profiling tools for zakura-perf-bench.yml (best-effort: perf still gets
# installed at run time if the kernel-matched package is missing here);
# libc6-dbg restores glibc's internal symbols for stack sampling
apt-get -o DPkg::Lock::Timeout=600 install -y -qq \
  "linux-tools-$(uname -r)" 2>/dev/null \
  || apt-get -o DPkg::Lock::Timeout=600 install -y -qq linux-tools-generic 2>/dev/null || true
apt-get -o DPkg::Lock::Timeout=600 install -y -qq libc6-dbg 2>/dev/null || true

# --------------------------------------------------------------------------- #
# Rust toolchain + repo clone + warm release build
# --------------------------------------------------------------------------- #

curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable
# deploy.py runs bare `cargo` from a non-login SSH shell where ~/.cargo/env has
# not been sourced, so the toolchain must be reachable from the default PATH.
ln -sf /root/.cargo/bin/cargo /root/.cargo/bin/rustc /root/.cargo/bin/rustup /usr/local/bin/
# flamegraph renderer + Rust v0 symbol demangler for zakura-perf-bench.yml
# (best-effort; the run script falls back gracefully without either)
cargo install inferno --locked >/dev/null 2>&1 || true
cargo install rustfilt --locked >/dev/null 2>&1 || true
ln -sf /root/.cargo/bin/inferno-flamegraph /root/.cargo/bin/rustfilt \
  /usr/local/bin/ 2>/dev/null || true

git clone "https://x-access-token:${GH_CLONE_TOKEN}@github.com/${GH_REPO}.git" /root/zakura
# Strip the token from the baked image; run droplets fetch with a fresh
# per-run token via an http.extraheader instead.
git -C /root/zakura remote set-url origin "https://github.com/${GH_REPO}.git"
rm -f /root/bake.env
unset GH_CLONE_TOKEN

# Warm the shared target dir that deploy.py's per-run worktree builds reuse.
cd /root/zakura
export CARGO_TARGET_DIR=/root/cargo-target
cargo build --release --locked -p zakura
/root/cargo-target/release/zakurad --version

# --------------------------------------------------------------------------- #
# Kresko: the mempool-load workload generator (zakura-mempool-load.yml)
# --------------------------------------------------------------------------- #
# Worth baking because it is expensive and constant: it compiles ~730 crates,
# including RocksDB, the halo2/Orchard proving stack, and the whole node (it
# pins its own zakura version, so it does not track the ref under test and does
# not change between A/B legs). Building it per run costs ~20 minutes every time
# for a binary that is byte-identical.
#
# The ref is recorded so a run asking for a different one rebuilds rather than
# silently using a stale binary.

KRESKO_BAKE_REPO="${KRESKO_BAKE_REPO:-https://github.com/valargroup/kresko.git}"
KRESKO_BAKE_REF="${KRESKO_BAKE_REF:-main}"

if [ "$REBUILD_APPROACH_FROM_SANDBLAST" != "true" ]; then
  if [ ! -d /root/kresko ]; then
    git clone "${KRESKO_BAKE_REPO}" /root/kresko
  fi
  git -C /root/kresko fetch --no-tags origin "${KRESKO_BAKE_REF}"
  git -C /root/kresko checkout --detach FETCH_HEAD
  # Own target dir, for the same reasons as mempool-load-run.sh: the binary
  # must land where that script looks for it, and kresko must not share a
  # cargo cache with zakurad.
  ( cd /root/kresko && CARGO_TARGET_DIR=/root/kresko/target cargo build --release )
  test -x /root/kresko/target/release/kresko
  git -C /root/kresko rev-parse HEAD > /root/kresko/.baked-ref
  echo "baked kresko at $(cat /root/kresko/.baked-ref)"
fi

# --------------------------------------------------------------------------- #
# Loopback SSH identity: deploy.py drives the node over root@localhost
# --------------------------------------------------------------------------- #

if [ ! -f /root/.ssh/pr_node_loopback ]; then
  ssh-keygen -t ed25519 -N '' -f /root/.ssh/pr_node_loopback
fi
grep -qxF "$(cat /root/.ssh/pr_node_loopback.pub)" /root/.ssh/authorized_keys 2>/dev/null || \
  cat /root/.ssh/pr_node_loopback.pub >> /root/.ssh/authorized_keys
# No host-key checking for loopback: droplets created from this image
# regenerate their SSH host keys on first boot, so a baked known_hosts entry
# would make every deploy.py connection fail with a changed-key error.
cat > /root/.ssh/config <<'CFG'
Host localhost
    IdentityFile /root/.ssh/pr_node_loopback
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
CFG
chmod 600 /root/.ssh/config
ssh -o BatchMode=yes root@localhost true

# --------------------------------------------------------------------------- #
# Fill the state volumes
# --------------------------------------------------------------------------- #

mount_volume() {
  local mnt="$2" dev="/dev/disk/by-id/scsi-0DO_Volume_$1"
  for _ in $(seq 1 30); do [ -e "$dev" ] && break; sleep 2; done
  [ -e "$dev" ] || { echo "volume device not found: $dev" >&2; return 1; }
  blkid "$dev" >/dev/null 2>&1 || mkfs.ext4 -q "$dev"
  mkdir -p "$mnt"
  mount "$dev" "$mnt"
}

# Download to the (large) volume, verify sha256 when given, extract into
# <mount>/<mode>/ so the node's state cache_dir can point straight at it,
# and assert the expected state/v*/<network> tree came out.
fetch_state() {
  local url="$1" sha="$2" dest="$3" network="$4"
  local tarball="${dest%/}.tar.zst"
  echo "Fetching ${url} -> ${dest}"
  df -h "$(dirname "$dest")"
  # --retry-all-errors + -C - resumes interrupted multi-GB transfers instead of
  # failing the whole bake (plain --retry does not cover mid-stream resets).
  curl -fL --retry 8 --retry-delay 15 --retry-all-errors -C - \
    -o "$tarball" "$url"
  if [ -n "$sha" ]; then
    echo "${sha}  ${tarball}" | sha256sum -c -
  fi
  mkdir -p "$dest"
  zstd -dc "$tarball" | tar -x -C "$dest"
  rm -f "$tarball"
  ls -d "$dest"/state/v*/"$network" >/dev/null || {
    echo "extracted state not found under ${dest}/state/v*/${network}" >&2
    return 1
  }
  echo "Restored $(ls -d "$dest"/state/v*/"$network")"
}

MAINNET_MNT=/mnt/bake-mainnet
TESTNET_MNT=/mnt/bake-testnet
APPROACH_MNT=/mnt/bake-approach
mount_volume "$MAINNET_VOLUME_NAME" "$MAINNET_MNT"
mount_volume "$TESTNET_VOLUME_NAME" "$TESTNET_MNT"
mount_volume "$APPROACH_VOLUME_NAME" "$APPROACH_MNT"

# Mainnet sandblast: pinned archive just before the 2022 spam region.
fetch_state "$SANDBLAST_URL" "$SANDBLAST_SHA256" "$MAINNET_MNT/sandblast" mainnet

# Mainnet VCT approach state. Existing pruned snapshots cannot be rolled back
# reliably because pruning removes transaction data rollback-state needs.
# Build the rare handoff fixture forward from the retained archive instead.
MAX_CKPT=$(tail -1 zakura-chain/src/parameters/checkpoint/main-checkpoints.txt | cut -d' ' -f1)
[[ "$MAX_CKPT" =~ ^[0-9]+$ ]] || {
  echo "could not determine Mainnet max checkpoint" >&2
  exit 1
}
APPROACH_H=$((MAX_CKPT - 100))
if [ "$REBUILD_APPROACH_FROM_SANDBLAST" = "true" ]; then
  mkdir -p "$APPROACH_MNT/tip"
  cp -a "$MAINNET_MNT/sandblast/." "$APPROACH_MNT/tip/"
  find "$APPROACH_MNT/tip" -name LOCK -delete
  rm -rf "$APPROACH_MNT/tip/non_finalized_state"
  cat > /root/approach.toml <<TOML
[network]
network = "Mainnet"
listen_addr = "0.0.0.0:8233"
p2p_stack = "zakura"

[state]
cache_dir = "$APPROACH_MNT/tip"
storage_mode = "pruned"
debug_stop_at_height = $APPROACH_H

[consensus]
checkpoint_sync = true
vct_fast_sync = true

[metrics]
endpoint_addr = "127.0.0.1:9999"

[tracing]
filter = "info"
TOML
  echo "Syncing Mainnet VCT approach state to height=$APPROACH_H handoff=$MAX_CKPT"
  set +e
  timeout 4h /root/cargo-target/release/zakurad -c /root/approach.toml start \
    2>&1 | tee /root/approach-sync.log
  ZAKURAD_STATUS=${PIPESTATUS[0]}
  set -e
  if [ "$ZAKURAD_STATUS" -ne 0 ] &&
    ! grep -q "stopping at configured height.*height=Height($APPROACH_H)" /root/approach-sync.log
  then
    echo "approach sync exited unexpectedly with status $ZAKURAD_STATUS" >&2
    exit "$ZAKURAD_STATUS"
  fi
  cat > /root/inspect-approach.toml <<TOML
[state]
storage_mode = "pruned"
TOML
  set +e
  TIP_OUTPUT=$(
    /root/cargo-target/release/zakurad -c /root/inspect-approach.toml tip-height \
      --cache-dir "$APPROACH_MNT/tip" \
      --network Mainnet 2>&1
  )
  TIP_STATUS=$?
  set -e
  VERIFIED_APPROACH_H=$(printf '%s\n' "$TIP_OUTPUT" |
    awk '/^[0-9]+$/ { height=$1 } END { print height }')
  if [ "$TIP_STATUS" -eq 0 ] && [ -n "$VERIFIED_APPROACH_H" ]; then
    [ "$VERIFIED_APPROACH_H" = "$APPROACH_H" ] || {
      echo "approach sync stopped at $VERIFIED_APPROACH_H, expected $APPROACH_H" >&2
      exit 1
    }
  else
    echo "::warning::tip-height could not reopen the flushed fixture; using the exact configured-stop log height"
    printf '%s\n' "$TIP_OUTPUT" >&2
  fi
  echo "$APPROACH_H" > /root/mainnet-approach-height
else
  echo "Keeping the retained approach snapshot; dispatch with rebuild_approach_from_sandblast=true to replace it"

  # Mainnet tip: resolve the daily pruned snapshot through its latest.json pointer.
  TIP_META=$(curl -fsSL --retry 3 "$TIP_MAINNET_LATEST_JSON")
  TIP_URL=$(echo "$TIP_META" | jq -er '.url')
  TIP_SHA=$(echo "$TIP_META" | jq -er '.sha256')
  echo "Mainnet tip: $(echo "$TIP_META" | jq -r '"\(.filename) height=\(.height) db=\(.db_format_version)"')"
  echo "$TIP_META" | jq -er '.height | select(type == "number" and floor == .)' \
    > /root/mainnet-state-height
  fetch_state "$TIP_URL" "$TIP_SHA" "$MAINNET_MNT/tip" mainnet

  # Testnet tip: newest enabled pruned entry from the snapshots site metadata.
  TESTNET_META=$(curl -fsSL --retry 3 "$TESTNET_SNAPSHOTS_BASE/snapshots.json")
  ENTRY=$(echo "$TESTNET_META" | jq -er \
    '[.snapshots[] | select(.enabled and .kind == "pruned")] | sort_by(.published) | last')
  [ "$ENTRY" != "null" ] || { echo "no enabled pruned testnet snapshot found" >&2; exit 1; }
  TN_FILE=$(echo "$ENTRY" | jq -er '.file')
  TN_SHA=$(echo "$ENTRY" | jq -er '.sha256')
  TN_BASE=$(echo "$TESTNET_META" | jq -r '.siteBaseUrl // empty')
  echo "Testnet tip: $(echo "$ENTRY" | jq -r '"\(.file) height=\(.height) db=\(.dbFormat)"')"
  echo "$ENTRY" | jq -er '.height | select(type == "number" and floor == .)' \
    > /root/testnet-state-height
  if [ -n "$TN_BASE" ] && curl -fsIL --retry 2 "${TN_BASE}/files/${TN_FILE}" >/dev/null 2>&1; then
    fetch_state "${TN_BASE}/files/${TN_FILE}" "$TN_SHA" "$TESTNET_MNT/tip" testnet
  else
    fetch_state "${TESTNET_SNAPSHOTS_BASE}/files/${TN_FILE}" "$TN_SHA" "$TESTNET_MNT/tip" testnet
  fi
fi

sync
umount "$MAINNET_MNT" "$TESTNET_MNT" "$APPROACH_MNT"

# --------------------------------------------------------------------------- #
# Clean the droplet for imaging
# --------------------------------------------------------------------------- #

apt-get clean
truncate -s 0 /etc/machine-id
# Without this, droplets created from the image skip first-boot cloud-init and
# never receive the CI's DO SSH key (which looks like a network failure).
cloud-init clean --logs

echo "Bake complete."
