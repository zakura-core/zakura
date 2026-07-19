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

# --------------------------------------------------------------------------- #
# Rust toolchain + repo clone + warm release build
# --------------------------------------------------------------------------- #

curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable
# deploy.py runs bare `cargo` from a non-login SSH shell where ~/.cargo/env has
# not been sourced, so the toolchain must be reachable from the default PATH.
ln -sf /root/.cargo/bin/cargo /root/.cargo/bin/rustc /root/.cargo/bin/rustup /usr/local/bin/

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
mount_volume "$MAINNET_VOLUME_NAME" "$MAINNET_MNT"
mount_volume "$TESTNET_VOLUME_NAME" "$TESTNET_MNT"

# Mainnet tip: resolve the daily pruned snapshot through its latest.json pointer.
TIP_META=$(curl -fsSL --retry 3 "$TIP_MAINNET_LATEST_JSON")
TIP_URL=$(echo "$TIP_META" | jq -er '.url')
TIP_SHA=$(echo "$TIP_META" | jq -er '.sha256')
echo "Mainnet tip: $(echo "$TIP_META" | jq -r '"\(.filename) height=\(.height) db=\(.db_format_version)"')"
fetch_state "$TIP_URL" "$TIP_SHA" "$MAINNET_MNT/tip" mainnet

# Mainnet sandblast: pinned archive just before the 2022 spam region.
fetch_state "$SANDBLAST_URL" "$SANDBLAST_SHA256" "$MAINNET_MNT/sandblast" mainnet

# Testnet tip: newest enabled pruned entry from the snapshots site metadata.
TESTNET_META=$(curl -fsSL --retry 3 "$TESTNET_SNAPSHOTS_BASE/snapshots.json")
ENTRY=$(echo "$TESTNET_META" | jq -er \
  '[.snapshots[] | select(.enabled and .kind == "pruned")] | sort_by(.published) | last')
[ "$ENTRY" != "null" ] || { echo "no enabled pruned testnet snapshot found" >&2; exit 1; }
TN_FILE=$(echo "$ENTRY" | jq -er '.file')
TN_SHA=$(echo "$ENTRY" | jq -er '.sha256')
TN_BASE=$(echo "$TESTNET_META" | jq -r '.siteBaseUrl // empty')
echo "Testnet tip: $(echo "$ENTRY" | jq -r '"\(.file) height=\(.height) db=\(.dbFormat)"')"
if [ -n "$TN_BASE" ] && curl -fsIL --retry 2 "${TN_BASE}/files/${TN_FILE}" >/dev/null 2>&1; then
  fetch_state "${TN_BASE}/files/${TN_FILE}" "$TN_SHA" "$TESTNET_MNT/tip" testnet
else
  fetch_state "${TESTNET_SNAPSHOTS_BASE}/files/${TN_FILE}" "$TN_SHA" "$TESTNET_MNT/tip" testnet
fi

sync
umount "$MAINNET_MNT" "$TESTNET_MNT"

# --------------------------------------------------------------------------- #
# Clean the droplet for imaging
# --------------------------------------------------------------------------- #

apt-get clean
truncate -s 0 /etc/machine-id
# Without this, droplets created from the image skip first-boot cloud-init and
# never receive the CI's DO SSH key (which looks like a network failure).
cloud-init clean --logs

echo "Bake complete."
