#!/usr/bin/env bash
# End-to-end release-state rehearsal against a real, quiesced Mainnet state.
# Exercises every line the merged pipeline runs, with no R2 and no GitHub side
# effects: offline export, publisher (local rclone backend), and a replay of
# the update-release-state.yml import/validate block in this checkout.
#
# Intended for a zakura-pr-node droplet (dispatch zakura-pr-node.yml for this
# branch with network=mainnet snapshot_mode=tip, SSH in, stop zakurad), but
# any host with this checkout plus a quiesced synced Mainnet state works.
#
# Usage: deploy/release-state/rehearsal.sh <quiesced-zakura-cache-dir>
#
# Everything is written under a scratch directory except the import replay,
# which temporarily modifies the four release-state files in this checkout and
# restores them on exit.

set -euo pipefail

STATE_DIR=${1:?usage: rehearsal.sh <quiesced-zakura-cache-dir>}
cd "$(dirname "$0")/../.."

CHECKPOINTS=zakura-chain/src/parameters/checkpoint/main-checkpoints.txt
FRONTIER=zakura-state/src/service/finalized_state/vct/mainnet-frontier.bin
PROVENANCE=zakura-state/src/service/finalized_state/vct/mainnet-frontier.json
EOS_FILE=zakurad/src/components/sync/end_of_support.rs

say() { printf '\n==> %s\n' "$*"; }
die() { echo "rehearsal FAILED: $*" >&2; exit 1; }

say "[0/4] Preflight"
[ -f "$CHECKPOINTS" ] || die "run from a zakura checkout (missing $CHECKPOINTS)"
[ -d "$STATE_DIR" ] || die "state dir $STATE_DIR does not exist"
if pgrep -x zakurad >/dev/null 2>&1; then
    die "zakurad is running; quiesce first (systemctl stop zakura || pkill -x zakurad)"
fi
git diff --quiet -- "$CHECKPOINTS" "$FRONTIER" "$PROVENANCE" "$EOS_FILE" \
    || die "release-state files are already modified in this checkout"
command -v rclone >/dev/null 2>&1 || {
    say "installing rclone for the publisher rehearsal"
    apt-get update -qq && apt-get install -y -qq rclone
}
WORK=$(mktemp -d)
restore() {
    git checkout --quiet -- "$CHECKPOINTS" "$FRONTIER" "$PROVENANCE" "$EOS_FILE" || true
    rm -rf "$WORK"
}
trap restore EXIT
COMMITTED_MAX=$(tail -1 "$CHECKPOINTS" | cut -d' ' -f1)

say "[1/4] Building the offline exporter"
cargo build --locked --release -p zakura-utils \
    --features zakura-checkpoints-offline --bin zakura-checkpoints
BIN=target/release/zakura-checkpoints

say "[2/4] Export smoke test"
"$BIN" --state-cache-dir "$STATE_DIR" --full-list \
    --mainnet-frontier-output "$WORK/frontier-1.bin" \
    > "$WORK/checkpoints-1.txt" 2> "$WORK/export-1.log"
sed -n '3,5p' "$WORK/export-1.log" || true

COMMITTED_SIZE=$(stat -c%s "$CHECKPOINTS")
head -c "$COMMITTED_SIZE" "$WORK/checkpoints-1.txt" | cmp -s - "$CHECKPOINTS" \
    || die "committed checkpoint list is not a byte-identical prefix of the export"
[ "$(stat -c%s "$WORK/checkpoints-1.txt")" -gt "$COMMITTED_SIZE" ] \
    || die "export did not extend the committed checkpoint list"
.github/scripts/validate-checkpoints.sh "$WORK/checkpoints-1.txt"

TIP=$(grep -oE 'finalized tip [0-9]+' "$WORK/export-1.log" | grep -oE '[0-9]+$')
LAST=$(tail -1 "$WORK/checkpoints-1.txt" | cut -d' ' -f1)
[ "$LAST" -gt "$COMMITTED_MAX" ] || die "terminal $LAST is not above committed max $COMMITTED_MAX"
GAP=$((TIP - LAST))
{ [ "$GAP" -ge 0 ] && [ "$GAP" -lt 400 ]; } \
    || die "terminal $LAST is $GAP blocks below DB tip $TIP (expected 0..399)"
FRONTIER_HEIGHT=$(python3 -c 'import struct, sys; print(struct.unpack("<I", open(sys.argv[1], "rb").read(4))[0])' "$WORK/frontier-1.bin")
[ "$FRONTIER_HEIGHT" -eq "$LAST" ] \
    || die "frontier height $FRONTIER_HEIGHT does not equal terminal checkpoint $LAST"

"$BIN" --state-cache-dir "$STATE_DIR" --full-list \
    --mainnet-frontier-output "$WORK/frontier-2.bin" \
    > "$WORK/checkpoints-2.txt" 2>/dev/null
cmp -s "$WORK/checkpoints-1.txt" "$WORK/checkpoints-2.txt" || die "checkpoint export is not deterministic"
cmp -s "$WORK/frontier-1.bin" "$WORK/frontier-2.bin" || die "frontier export is not deterministic"
echo "export OK: $((TIP - COMMITTED_MAX)) new blocks, terminal $LAST, tip $TIP, deterministic"

say "[3/4] Publisher rehearsal (local rclone backend)"
export RELEASE_STATE_R2_REMOTE="$WORK/fake-r2"
export RELEASE_STATE_PUBLIC_BASE="https://zakura-release.valargroup.dev/release-state"
export ZAKURA_CHECKPOINTS_BIN="$BIN"
deploy/release-state/publish-release-state.sh "$STATE_DIR"
BUNDLE="$WORK/fake-r2/release-state/v1/$LAST"
for f in meta.json main-checkpoints.txt mainnet-frontier.bin; do
    [ -f "$BUNDLE/$f" ] || die "published bundle is missing $f"
done
python3 - "$WORK/fake-r2/release-state" "$LAST" <<'PY'
import hashlib, json, sys
root, height = sys.argv[1], sys.argv[2]
latest = json.load(open(f"{root}/latest.json"))
meta_bytes = open(f"{root}/v1/{height}/meta.json", "rb").read()
assert latest["height"] == int(height), "pointer height mismatch"
assert latest["meta_sha256"] == hashlib.sha256(meta_bytes).hexdigest(), "pointer meta digest mismatch"
meta = json.loads(meta_bytes)
for name, entry in meta["files"].items():
    data = open(f"{root}/v1/{height}/{name}", "rb").read()
    assert len(data) == entry["size"], f"{name} size mismatch"
    assert hashlib.sha256(data).hexdigest() == entry["sha256"], f"{name} digest mismatch"
print("bundle digests OK")
PY
deploy/release-state/publish-release-state.sh "$STATE_DIR" 2> "$WORK/republish.log" \
    || die "same-state republish is not idempotent"
grep -q "already published" "$WORK/republish.log" || die "republish did not take the reuse path"
echo "publisher OK: immutable bundle + pointer at $LAST, idempotent republish"

say "[4/4] Workflow import replay in this checkout"
BUNDLE_HEIGHT=$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["height"])' "$BUNDLE/meta.json")
[ "$BUNDLE_HEIGHT" -gt "$COMMITTED_MAX" ] || die "bundle does not advance the committed list"
head -c "$COMMITTED_SIZE" "$BUNDLE/main-checkpoints.txt" | cmp -s - "$CHECKPOINTS" \
    || die "bundle checkpoint list does not extend the committed list byte-for-byte"
cp "$BUNDLE/main-checkpoints.txt" "$CHECKPOINTS"
cp "$BUNDLE/mainnet-frontier.bin" "$FRONTIER"
python3 - "$BUNDLE/meta.json" "$CHECKPOINTS" "$FRONTIER" "$PROVENANCE" <<'PY'
import hashlib, json, sys
meta_path, checkpoints, frontier, provenance_path = sys.argv[1:5]
meta = json.load(open(meta_path))
frontier_bytes = open(frontier, "rb").read()
provenance = {
    "schema_version": 1,
    "network": "Mainnet",
    "source": "release-state-bundle",
    "generated_at": meta["generated_at"],
    "finalized_height": meta["height"],
    "finalized_hash": meta["block_hash"],
    "checkpoints_sha256": hashlib.sha256(open(checkpoints, "rb").read()).hexdigest(),
    "frontier_sha256": hashlib.sha256(frontier_bytes).hexdigest(),
    "frontier_size": len(frontier_bytes),
    "meta_sha256": hashlib.sha256(open(meta_path, "rb").read()).hexdigest(),
}
with open(provenance_path, "w", encoding="utf-8") as out:
    json.dump(provenance, out, indent=2)
    out.write("\n")
PY
EOS_HEIGHT=$((BUNDLE_HEIGHT + 3456))
FORMATTED=$(echo "$EOS_HEIGHT" | awk '{n=$0; r=""; for(i=length(n);i>0;i--) { r=substr(n,i,1) r; if((length(n)-i)%3==2 && i>1) r="_" r }; print r}')
CURRENT_EOS=$(grep -oE 'ESTIMATED_RELEASE_HEIGHT: u32 = [0-9_]+' "$EOS_FILE" | grep -oE '[0-9_]+$' | tr -d '_')
if [ "$CURRENT_EOS" -lt "$EOS_HEIGHT" ]; then
    sed -i "s/ESTIMATED_RELEASE_HEIGHT: u32 = [0-9_]*/ESTIMATED_RELEASE_HEIGHT: u32 = ${FORMATTED}/" "$EOS_FILE"
fi

.github/scripts/validate-checkpoints.sh "$CHECKPOINTS"
scripts/check-release-state.sh
CHANGED=$(git diff --name-only | sort)
EXPECTED=$(printf '%s\n' "$EOS_FILE" "$CHECKPOINTS" "$FRONTIER" "$PROVENANCE" | sort)
[ "$CHANGED" = "$EXPECTED" ] || die "import changed unexpected files: $CHANGED"
REMOVED=$(git diff --numstat -- "$CHECKPOINTS" | cut -f2)
[ "${REMOVED:-0}" = "0" ] || die "checkpoint import removed $REMOVED committed lines"
cargo test --locked -p zakura-state --lib -- frontier sprout_change

say "REHEARSAL PASSED: export, publish, and import all verified at height $LAST"
echo "The checkout's release-state files will now be restored."
