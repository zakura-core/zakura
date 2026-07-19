#!/usr/bin/env bash
# Export a Mainnet release-state bundle from a quiesced state copy and publish
# it to R2: an immutable release-state/v1/<height>/ bundle plus the mutable
# latest.json pointer consumed by the update-release-state workflow.
# Run after the snapshot job, against the same quiesced state directory.
# See README.md in this directory for host wiring, and
# docs/design/verified-commitment-trees.md, section 16, for the design.
#
# Usage: publish-release-state.sh <quiesced-zakura-cache-dir>
#
# Required environment:
#   RELEASE_STATE_R2_REMOTE   rclone destination, e.g. "r2:zakura-artifacts"
#   RELEASE_STATE_PUBLIC_BASE public HTTPS base serving that destination's
#                             release-state prefix, e.g.
#                             "https://zakura-release.valargroup.dev/release-state"
# Optional environment:
#   ZAKURA_CHECKPOINTS_BIN    zakura-checkpoints binary (default: on PATH),
#                             built with --features zakura-checkpoints-offline
#   RELEASE_STATE_KEEP        immutable bundles to retain (default 4)

set -euo pipefail

STATE_DIR=${1:?usage: publish-release-state.sh <quiesced-zakura-cache-dir>}
: "${RELEASE_STATE_R2_REMOTE:?set RELEASE_STATE_R2_REMOTE to an rclone destination}"
: "${RELEASE_STATE_PUBLIC_BASE:?set RELEASE_STATE_PUBLIC_BASE to the public HTTPS base URL}"
BIN=${ZAKURA_CHECKPOINTS_BIN:-zakura-checkpoints}
KEEP=${RELEASE_STATE_KEEP:-4}
# A zero or malformed KEEP would make `head -n -"$KEEP"` select every bundle,
# purging the one latest.json points at.
if ! [[ "$KEEP" =~ ^[1-9][0-9]*$ ]]; then
    echo "RELEASE_STATE_KEEP must be a positive integer, got: ${KEEP@Q}" >&2
    exit 1
fi
REMOTE_PREFIX="${RELEASE_STATE_R2_REMOTE%/}/release-state"

STAGE=$(mktemp -d)
trap 'rm -rf "$STAGE"' EXIT

sha256_of() {
    python3 -c 'import hashlib, sys; print(hashlib.sha256(open(sys.argv[1], "rb").read()).hexdigest())' "$1"
}

"$BIN" \
    --state-cache-dir "$STATE_DIR" \
    --full-list \
    --mainnet-frontier-output "$STAGE/mainnet-frontier.bin" \
    > "$STAGE/main-checkpoints.txt"

HEIGHT=$(tail -1 "$STAGE/main-checkpoints.txt" | cut -d' ' -f1)
BLOCK_HASH=$(tail -1 "$STAGE/main-checkpoints.txt" | cut -d' ' -f2)
GENERATED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)

HEIGHT="$HEIGHT" BLOCK_HASH="$BLOCK_HASH" GENERATED_AT="$GENERATED_AT" \
    python3 - "$STAGE" <<'PY'
import hashlib, json, os, sys

stage = sys.argv[1]
files = {}
for name in ("main-checkpoints.txt", "mainnet-frontier.bin"):
    data = open(os.path.join(stage, name), "rb").read()
    files[name] = {"size": len(data), "sha256": hashlib.sha256(data).hexdigest()}

meta = {
    "schema_version": 1,
    "network": "Mainnet",
    "height": int(os.environ["HEIGHT"]),
    "block_hash": os.environ["BLOCK_HASH"],
    "generated_at": os.environ["GENERATED_AT"],
    "files": files,
    "generator": {"name": "zakura-checkpoints", "mode": "offline"},
}
with open(os.path.join(stage, "meta.json"), "w", encoding="utf-8") as out:
    json.dump(meta, out, indent=2)
    out.write("\n")
PY

# Immutability with idempotence: a bundle directory is written once. A
# re-export of the same quiesced state reproduces the same data files (only
# the meta timestamp differs), so an existing bundle whose file digests match
# is reused as-is and only the pointer is refreshed; different contents at the
# same height mean timestamp-free determinism broke and a human should look.
BUNDLE_REMOTE="$REMOTE_PREFIX/v1/$HEIGHT"
if rclone lsf "$BUNDLE_REMOTE/meta.json" >/dev/null 2>&1; then
    rclone copyto "$BUNDLE_REMOTE/meta.json" "$STAGE/existing-meta.json"
    python3 - "$STAGE" <<'PY'
import json, os, sys

stage = sys.argv[1]
existing = json.load(open(os.path.join(stage, "existing-meta.json"), encoding="utf-8"))
staged = json.load(open(os.path.join(stage, "meta.json"), encoding="utf-8"))
if existing.get("files") != staged["files"] or existing.get("block_hash") != staged["block_hash"]:
    print("existing bundle at this height has different contents", file=sys.stderr)
    sys.exit(1)
PY
    cp "$STAGE/existing-meta.json" "$STAGE/meta.json"
    GENERATED_AT=$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["generated_at"])' "$STAGE/meta.json")
    echo "bundle v1/$HEIGHT already published; refreshing the pointer" >&2
else
    # Data files first, meta.json last, so a partially uploaded bundle is
    # never resolvable through a pointer.
    rclone copyto "$STAGE/main-checkpoints.txt" "$BUNDLE_REMOTE/main-checkpoints.txt"
    rclone copyto "$STAGE/mainnet-frontier.bin" "$BUNDLE_REMOTE/mainnet-frontier.bin"
    rclone copyto "$STAGE/meta.json" "$BUNDLE_REMOTE/meta.json"
    echo "published bundle v1/$HEIGHT ($BLOCK_HASH)" >&2
fi
META_SHA256=$(sha256_of "$STAGE/meta.json")

cat > "$STAGE/latest.json" <<EOF
{
  "schema_version": 1,
  "network": "Mainnet",
  "height": $HEIGHT,
  "block_hash": "$BLOCK_HASH",
  "generated_at": "$GENERATED_AT",
  "meta_url": "${RELEASE_STATE_PUBLIC_BASE%/}/v1/$HEIGHT/meta.json",
  "meta_sha256": "$META_SHA256"
}
EOF
rclone copyto "$STAGE/latest.json" "$REMOTE_PREFIX/latest.json"
echo "pointer now at height $HEIGHT" >&2

# Retention: keep the newest $KEEP immutable bundles.
rclone lsf --dirs-only "$REMOTE_PREFIX/v1/" 2>/dev/null \
    | tr -d '/' | grep -E '^[0-9]+$' | sort -n | head -n -"$KEEP" \
    | while read -r old_height; do
        [ -n "$old_height" ] || continue
        echo "pruning bundle v1/$old_height" >&2
        rclone purge "$REMOTE_PREFIX/v1/$old_height"
    done
