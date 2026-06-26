#!/usr/bin/env bash
# One-time (or after a DB format-version bump): produce the two pruned start-state
# snapshots that the "Sync confidence" CI consumes, by rewinding a recent full
# mainnet Zebra snapshot down to each window start and pruning it.
#
# Run on a host with ~750 GB-1 TB of free working disk (the ~250 GB tarball plus
# one ~500 GB extract at a time) and the Zebra build deps. Requires s3cmd
# configured for the destination Spaces bucket.
#
# Override via env as needed:
#   REPO        checkout of this repo at the branch under test (for the tools + version)
#   WORK        big working dir
#   SNAP_URL    a recent FULL (archive) mainnet snapshot at height >= 3,375,000
#   DEST_BUCKET destination Space the consume workflow reads state from
set -euo pipefail

REPO="${REPO:-$HOME/zebra}"
WORK="${WORK:-/mnt/work}"
SNAP_URL="${SNAP_URL:-https://zebra-snapshots.nyc3.cdn.digitaloceanspaces.com/mainnet/zebra-mainnet-20260622T030732Z-3386347.tar.zst}"
DEST_BUCKET="${DEST_BUCKET:-zebra-sync-confidence-ci}"

cd "${REPO}"
cargo build --release --bin zebra-rollback-state --bin zebra-prune-state
ROLLBACK="${REPO}/target/release/zebra-rollback-state"
PRUNE="${REPO}/target/release/zebra-prune-state"
VER="$(grep -oE 'DATABASE_FORMAT_VERSION: .* [0-9]+' zebra-state/src/constants.rs | grep -oE '[0-9]+' | tail -n1)"

mkdir -p "${WORK}"
cd "${WORK}"
[ -f head.tar.zst ] || curl -fL "${SNAP_URL}" -o head.tar.zst

# window:start-height  (both must be <= the snapshot height)
for pair in "pre-nu62:3358006" "post-nu62:3375000"; do
  key="${pair%%:*}"
  height="${pair##*:}"
  echo "=== ${key}: rewind to ${height}, prune, upload ==="
  rm -rf state
  mkdir state
  tar --use-compress-program=zstd -xf head.tar.zst -C state
  # NOTE: --cache-dir must point at the dir that CONTAINS state/v${VER}/mainnet/.
  # Adjust the path if your snapshot tar nests the cache dir under a subdir.
  "${ROLLBACK}" --height "${height}" --network Mainnet --cache-dir "${WORK}/state"
  "${PRUNE}" --network Mainnet --cache-dir "${WORK}/state" --tx-retention 10000 --confirm
  tar --use-compress-program='zstd -T0' -cf "${key}.tar.zst" -C "${WORK}/state" .
  s3cmd put "${key}.tar.zst" "s3://${DEST_BUCKET}/sync-confidence/state/v${VER}/mainnet/${key}.tar.zst"
  rm -rf state "${key}.tar.zst"
done

echo "Done. Uploaded pre-nu62 and post-nu62 under s3://${DEST_BUCKET}/sync-confidence/state/v${VER}/mainnet/"
