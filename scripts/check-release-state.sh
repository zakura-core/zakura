#!/usr/bin/env bash
# Verify the committed Mainnet release state without cargo: the checkpoint
# list, VCT frontier, and provenance record must identify the same finalized
# block. Used by `make pre-release-state` and the update-release-state
# workflow; the cargo-side twin is the `embedded_mainnet_final_frontiers_parse`
# unit test. See docs/design/verified-commitment-trees.md, section 16.3.
#
# Release gate: rejects `legacy-bootstrap` provenance so a release cannot ship
# state that predates the release-state pipeline. Export
# ZAKURA_ALLOW_BOOTSTRAP_RELEASE_STATE=1 for the documented emergency override.
# Warns (never fails) when the committed state is older than 14 days.

set -euo pipefail

cd "$(dirname "$0")/.."

ZAKURA_ALLOW_BOOTSTRAP_RELEASE_STATE="${ZAKURA_ALLOW_BOOTSTRAP_RELEASE_STATE:-0}" \
    python3 - <<'PY'
import hashlib
import json
import os
import struct
import sys
from datetime import datetime, timedelta, timezone

CHECKPOINTS = "zakura-chain/src/parameters/checkpoint/main-checkpoints.txt"
FRONTIER = "zakura-state/src/service/finalized_state/vct/mainnet-frontier.bin"
PROVENANCE = "zakura-state/src/service/finalized_state/vct/mainnet-frontier.json"
REQUIRED_KEYS = {
    "schema_version",
    "network",
    "source",
    "generated_at",
    "finalized_height",
    "finalized_hash",
    "checkpoints_sha256",
    "frontier_sha256",
    "frontier_size",
}
OPTIONAL_KEYS = {"meta_sha256"}
STALE_WARNING = timedelta(days=14)


def fail(message: str) -> None:
    print(f"release-state check failed: {message}", file=sys.stderr)
    sys.exit(1)


def is_hex_digest(value: object, length: int = 64) -> bool:
    return (
        isinstance(value, str)
        and len(value) == length
        and all(c in "0123456789abcdef" for c in value)
    )


try:
    provenance = json.loads(open(PROVENANCE, encoding="utf-8").read())
except (OSError, ValueError) as error:
    fail(f"cannot read {PROVENANCE}: {error}")

if not isinstance(provenance, dict):
    fail("provenance must be a JSON object")
missing = REQUIRED_KEYS - set(provenance)
unknown = set(provenance) - REQUIRED_KEYS - OPTIONAL_KEYS
if missing:
    fail(f"provenance is missing keys: {', '.join(sorted(missing))}")
if unknown:
    fail(f"provenance has unknown keys: {', '.join(sorted(unknown))}")
if provenance["schema_version"] != 1:
    fail("unsupported provenance schema_version")
if provenance["network"] != "Mainnet":
    fail("provenance network must be Mainnet")

height = provenance["finalized_height"]
if not isinstance(height, int) or isinstance(height, bool) or not 0 < height < 2**32:
    fail("finalized_height must be a block height")
if not is_hex_digest(provenance["finalized_hash"]):
    fail("finalized_hash must be a 64-character hex block hash")

checkpoints = open(CHECKPOINTS, "rb").read()
if hashlib.sha256(checkpoints).hexdigest() != provenance["checkpoints_sha256"]:
    fail(f"{CHECKPOINTS} digest does not match the provenance record")
if not checkpoints.endswith(b"\n"):
    fail(f"{CHECKPOINTS} must end with a newline")
try:
    tail_height, tail_hash = checkpoints.decode().splitlines()[-1].split(" ")
except (UnicodeDecodeError, ValueError):
    fail(f"{CHECKPOINTS} terminal line is not a 'HEIGHT HASH' record")
if int(tail_height) != height:
    fail(
        f"terminal checkpoint height {tail_height} does not match "
        f"provenance finalized_height {height}"
    )
if tail_hash != provenance["finalized_hash"]:
    fail("terminal checkpoint hash does not match provenance finalized_hash")

frontier = open(FRONTIER, "rb").read()
if len(frontier) != provenance["frontier_size"]:
    fail(f"{FRONTIER} size {len(frontier)} does not match the provenance record")
if hashlib.sha256(frontier).hexdigest() != provenance["frontier_sha256"]:
    fail(f"{FRONTIER} digest does not match the provenance record")
(frontier_height,) = struct.unpack("<I", frontier[:4])
if frontier_height != height:
    fail(
        f"embedded frontier height {frontier_height} does not match "
        f"provenance finalized_height {height}"
    )

source = provenance["source"]
meta_sha256 = provenance.get("meta_sha256")
if source == "release-state-bundle":
    if not is_hex_digest(meta_sha256):
        fail("bundle provenance must bind a 64-character meta_sha256")
elif source == "legacy-bootstrap":
    if meta_sha256 is not None:
        fail("bootstrap provenance must not claim a bundle meta digest")
    if os.environ.get("ZAKURA_ALLOW_BOOTSTRAP_RELEASE_STATE") != "1":
        fail(
            "committed release state is still the legacy bootstrap; run the "
            "'Update Mainnet release state' workflow and merge its PR, or set "
            "ZAKURA_ALLOW_BOOTSTRAP_RELEASE_STATE=1 for an emergency release"
        )
else:
    fail(f"unsupported provenance source {source!r}")

try:
    generated_at = datetime.fromisoformat(
        str(provenance["generated_at"]).replace("Z", "+00:00")
    )
except ValueError:
    fail("generated_at must be an RFC 3339 timestamp")
if generated_at.tzinfo is None:
    fail("generated_at must include a timezone")
age = datetime.now(timezone.utc) - generated_at
if age > STALE_WARNING:
    print(
        f"warning: committed release state is {age.days} days old; "
        "consider refreshing before the release",
        file=sys.stderr,
    )

print(
    f"committed Mainnet release state is coupled at height {height} "
    f"({provenance['finalized_hash']}, source {source})"
)
PY
