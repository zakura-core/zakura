#!/usr/bin/env bash
# Export a Mainnet release-state bundle from an archive node's state and publish
# it to R2: an immutable release-state/v1/<height>/ bundle plus the mutable
# latest.json pointer consumed by the update-release-state workflow.
#
# The exporter opens the database as a read-only RocksDB secondary, so the node
# does not have to be stopped. It must be an *archive* node: the frontier grid
# covers the heights below the checkpoint, which a pruned database no longer
# holds. See README.md in this directory for host wiring, and
# docs/design/verified-commitment-trees.md, section 16, for the design.
#
# Usage: publish-release-state.sh <archive-node-zakura-cache-dir>
#
# Required environment:
#   RELEASE_STATE_R2_REMOTE   rclone destination, e.g. "r2:zakura-artifacts"
#   RELEASE_STATE_PUBLIC_BASE public HTTPS base serving that destination's
#                             release-state prefix, e.g.
#                             "https://zakura-release.valargroup.dev/release-state"
# Optional environment:
#   ZAKURA_CHECKPOINTS_BIN    zakura-checkpoints binary (default: on PATH),
#                             built with --features zakura-checkpoints-offline
#   RELEASE_STATE_GRID_COST_MS
#                             per-entry frontier grid cost budget in ms
#                             (default: whatever the exporter defaults to). Only
#                             affects entries added after the resumed prefix;
#                             published entries are carried forward verbatim.
#   RELEASE_STATE_KEEP        immutable bundles to retain (default 4)
#   RELEASE_STATE_LOCK_FILE   host-local publisher lock
#                             (default: /tmp/zakura-release-state-publish.lock)

set -euo pipefail

STATE_DIR=${1:?usage: publish-release-state.sh <archive-node-zakura-cache-dir>}
: "${RELEASE_STATE_R2_REMOTE:?set RELEASE_STATE_R2_REMOTE to an rclone destination}"
: "${RELEASE_STATE_PUBLIC_BASE:?set RELEASE_STATE_PUBLIC_BASE to the public HTTPS base URL}"
BIN=${ZAKURA_CHECKPOINTS_BIN:-zakura-checkpoints}
KEEP=${RELEASE_STATE_KEEP:-4}
LOCK_FILE=${RELEASE_STATE_LOCK_FILE:-/tmp/zakura-release-state-publish.lock}
# A zero or malformed KEEP would make `head -n -"$KEEP"` select every bundle,
# purging the one latest.json points at.
if ! [[ "$KEEP" =~ ^[1-9][0-9]*$ ]]; then
    echo "RELEASE_STATE_KEEP must be a positive integer, got: ${KEEP@Q}" >&2
    exit 1
fi
REMOTE_PREFIX="${RELEASE_STATE_R2_REMOTE%/}/release-state"
# Left empty by default so the exporter's own budget applies, rather than pinning
# a second copy of it here that would drift when the exporter's default changes.
GRID_ARGS=()
if [ -n "${RELEASE_STATE_GRID_COST_MS:-}" ]; then
    if ! [[ "$RELEASE_STATE_GRID_COST_MS" =~ ^[1-9][0-9]*$ ]]; then
        echo "RELEASE_STATE_GRID_COST_MS must be a positive integer, got: ${RELEASE_STATE_GRID_COST_MS@Q}" >&2
        exit 1
    fi
    GRID_ARGS+=(--frontier-grid-target-cost-ms "$RELEASE_STATE_GRID_COST_MS")
fi

# The publisher is intentionally single-host. Serializing the complete
# export/upload/pointer/retention transaction prevents overlapping snapshot or
# manual runs from regressing latest.json or racing same-height meta uploads.
# Multiple publisher hosts require object-store conditional writes, not this
# host-local lock.
if ! command -v flock >/dev/null 2>&1; then
    echo "release-state publication requires flock (normally provided by util-linux)" >&2
    exit 1
fi
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
    echo "another release-state publisher is already running (lock: $LOCK_FILE)" >&2
    exit 1
fi

STAGE=$(mktemp -d)
trap 'rm -rf "$STAGE"' EXIT

sha256_of() {
    python3 -c 'import hashlib, sys; print(hashlib.sha256(open(sys.argv[1], "rb").read()).hexdigest())' "$1"
}

# List one remote object, printing nothing when it is absent. Only a clean
# empty listing (or rclone's "directory not found", exit 3, for a prefix that
# does not exist yet) means absent: any other rclone failure aborts, so a
# transient list error can never masquerade as absence and bypass the
# pointer-regression or bundle-immutability guards below.
list_remote_object() {
    local target=$1 listing status
    status=0
    listing=$(rclone lsf "$target" 2>"$STAGE/lsf-stderr") || status=$?
    if [ "$status" -ne 0 ] && [ "$status" -ne 3 ]; then
        echo "rclone lsf failed for $target (exit $status):" >&2
        cat "$STAGE/lsf-stderr" >&2
        exit 1
    fi
    if [ "$status" -eq 0 ]; then
        printf '%s' "$listing"
    fi
}

# Read the pointer before exporting. Its bundle carries the previously published
# frontier grid, and resuming from that grid means this run scans only the blocks
# above its last entry instead of the whole chain from genesis. The pointer height
# is reused further down for the regression guard.
POINTER_LISTING=$(list_remote_object "$REMOTE_PREFIX/latest.json")
POINTER_HEIGHT=
if [ -n "$POINTER_LISTING" ]; then
    rclone copyto "$REMOTE_PREFIX/latest.json" "$STAGE/existing-latest.json"
    POINTER_HEIGHT=$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["height"])' \
        "$STAGE/existing-latest.json")

    # A bundle published before the grid joined the release state has no grid to
    # resume from. That is not an error: the run falls back to a full walk, which
    # is what the first grid-bearing export has to do anyway.
    PREVIOUS_GRID="$REMOTE_PREFIX/v1/$POINTER_HEIGHT/mainnet-frontier-grid.bin"
    if [ -n "$(list_remote_object "$PREVIOUS_GRID")" ]; then
        rclone copyto "$PREVIOUS_GRID" "$STAGE/previous-frontier-grid.bin"
        GRID_ARGS+=(--mainnet-frontier-grid-input "$STAGE/previous-frontier-grid.bin")
        echo "resuming the frontier grid from bundle v1/$POINTER_HEIGHT" >&2
    else
        echo "bundle v1/$POINTER_HEIGHT has no frontier grid; building one from genesis" >&2
    fi
fi

# One run, one coupled release state: the frontier, subtree roots, and frontier
# grid are all generated for the checkpoint this run selects.
"$BIN" \
    --state-cache-dir "$STATE_DIR" \
    --full-list \
    --mainnet-frontier-output "$STAGE/mainnet-frontier.bin" \
    --mainnet-subtree-output "$STAGE/mainnet-treestate-subtrees.bin" \
    --mainnet-frontier-grid-output "$STAGE/mainnet-frontier-grid.bin" \
    ${GRID_ARGS[@]+"${GRID_ARGS[@]}"} \
    > "$STAGE/main-checkpoints.txt"

HEIGHT=$(tail -1 "$STAGE/main-checkpoints.txt" | cut -d' ' -f1)
BLOCK_HASH=$(tail -1 "$STAGE/main-checkpoints.txt" | cut -d' ' -f2)
GENERATED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)

# Never move the pointer backwards: an export from stale state would regress
# latest.json, and retention could then purge the very bundle it points at.
if [ -n "$POINTER_HEIGHT" ] && [ "$POINTER_HEIGHT" -gt "$HEIGHT" ]; then
    echo "refusing to publish height $HEIGHT below the current pointer height $POINTER_HEIGHT; stale state?" >&2
    exit 1
fi

HEIGHT="$HEIGHT" BLOCK_HASH="$BLOCK_HASH" GENERATED_AT="$GENERATED_AT" \
    python3 - "$STAGE" <<'PY'
import hashlib, json, os, sys

stage = sys.argv[1]
files = {}
for name in (
    "main-checkpoints.txt",
    "mainnet-frontier.bin",
    "mainnet-treestate-subtrees.bin",
    "mainnet-frontier-grid.bin",
):
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
# re-export of the same stopped-node state reproduces the same data files (only
# the meta timestamp differs), so an existing bundle whose file digests match
# is reused as-is and only the pointer is refreshed; different contents at the
# same height mean timestamp-free determinism broke and a human should look.
BUNDLE_REMOTE="$REMOTE_PREFIX/v1/$HEIGHT"
BUNDLE_LISTING=$(list_remote_object "$BUNDLE_REMOTE/meta.json")
if [ -n "$BUNDLE_LISTING" ]; then
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
    rclone copyto "$STAGE/mainnet-treestate-subtrees.bin" "$BUNDLE_REMOTE/mainnet-treestate-subtrees.bin"
    rclone copyto "$STAGE/mainnet-frontier-grid.bin" "$BUNDLE_REMOTE/mainnet-frontier-grid.bin"
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
