#!/usr/bin/env bash
# Verify the committed Mainnet release state without cargo: the checkpoint
# list, VCT frontier, historical subtree roots, and manifest must identify the
# same finalized block, and the pinned frontier grid dependency must name the
# same checkpoint. Used by `make pre-release-state` and the update-release-state
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
import re
import struct
import sys
from datetime import datetime, timedelta, timezone

CHECKPOINTS = "crates/zakura-chain/src/parameters/checkpoint/main-checkpoints.txt"
FRONTIER = "crates/zakura-state/src/service/finalized_state/vct/mainnet-frontier.bin"
SUBTREES = "crates/zakura-state/src/service/finalized_state/vct/mainnet-subtrees.bin"
FRONTIER_GRID = (
    "crates/zakura-state/src/service/finalized_state/vct/mainnet-frontier-grid.bin"
)
PROVENANCE = "crates/zakura-state/src/service/finalized_state/vct/mainnet-vct-manifest.json"
WORKSPACE_MANIFEST = "Cargo.toml"
LOCKFILE = "Cargo.lock"
# The workspace dependency key, which is renamed: the published package name is read out of the
# pin so this script never has to know it.
ASSETS_DEPENDENCY = "zakura-assets"
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
    "subtrees_sha256",
    "subtrees_size",
}
# The frontier grid joined the bundle after the other three artifacts, so a manifest
# without it is a valid older one. Half a record, or a record without its file, is not.
FRONTIER_GRID_KEYS = {
    "frontier_grid_sha256",
    "frontier_grid_size",
    "frontier_grid_entries",
}
OPTIONAL_KEYS = {"meta_sha256"} | FRONTIER_GRID_KEYS
STALE_WARNING = timedelta(days=14)
# NetworkUpgrade::Nu6_3 (Ironwood) activation on Mainnet; kept in sync with
# crates/zakura-chain/src/parameters/constants.rs. Frontiers at or above activation
# must carry the fourth (Ironwood) tree blob, because the node parser defaults
# a missing fourth blob to the empty tree.
IRONWOOD_ACTIVATION_HEIGHT = 3_428_143


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
if len(frontier) < 4:
    fail(f"{FRONTIER} is truncated before its height field")
(frontier_height,) = struct.unpack("<I", frontier[:4])
if frontier_height != height:
    fail(
        f"embedded frontier height {frontier_height} does not match "
        f"provenance finalized_height {height}"
    )

# Structural framing check: the height must be followed by 3 or 4 length-prefixed
# tree blobs (Sapling, Orchard, Sprout, and optionally Ironwood) covering the file
# exactly. Tree-content validity stays with the cargo-side
# embedded_mainnet_final_frontiers_parse test; this catches truncated or padded
# bytes without a cargo build.
offset = 4
blobs = 0
while offset < len(frontier):
    if offset + 4 > len(frontier):
        fail(f"{FRONTIER} tree blob length prefix is truncated at byte {offset}")
    (blob_len,) = struct.unpack_from("<I", frontier, offset)
    offset += 4
    if offset + blob_len > len(frontier):
        fail(f"{FRONTIER} tree blob at byte {offset - 4} extends past the end of the file")
    offset += blob_len
    blobs += 1
allowed_blobs = (4,) if height >= IRONWOOD_ACTIVATION_HEIGHT else (3, 4)
if blobs not in allowed_blobs:
    fail(
        f"{FRONTIER} must frame {' or '.join(map(str, allowed_blobs))} tree blobs "
        f"for height {height}, found {blobs}"
    )

subtrees = open(SUBTREES, "rb").read()
if len(subtrees) != provenance["subtrees_size"]:
    fail(f"{SUBTREES} size {len(subtrees)} does not match the provenance record")
if hashlib.sha256(subtrees).hexdigest() != provenance["subtrees_sha256"]:
    fail(f"{SUBTREES} digest does not match the provenance record")

subtree_prefix = struct.Struct("<8sHBIIII")
subtree_header_len = subtree_prefix.size + 32
if len(subtrees) < subtree_header_len:
    fail(f"{SUBTREES} is truncated before its payload")
magic, version, network, subtree_handoff, *counts = subtree_prefix.unpack_from(subtrees)
if magic != b"ZKVCTST1" or version != 1 or network != 1:
    fail(f"{SUBTREES} has invalid Mainnet subtree-root framing")
if subtree_handoff != height:
    fail(
        f"embedded subtree handoff {subtree_handoff} does not match "
        f"provenance finalized_height {height}"
    )
if any(count > 2**16 for count in counts):
    fail(f"{SUBTREES} declares too many subtree roots")
subtree_payload = subtrees[subtree_header_len:]
if len(subtree_payload) != sum(counts) * (2 + 4 + 32):
    fail(f"{SUBTREES} payload length does not match its record counts")
if (
    hashlib.sha256(subtrees[:subtree_prefix.size] + subtree_payload).digest()
    != subtrees[subtree_prefix.size:subtree_header_len]
):
    fail(f"{SUBTREES} digest does not match its header")

grid_keys = FRONTIER_GRID_KEYS & set(provenance)
if grid_keys and grid_keys != FRONTIER_GRID_KEYS:
    fail(
        "provenance has a partial frontier grid record, missing "
        f"{', '.join(sorted(FRONTIER_GRID_KEYS - grid_keys))}"
    )
# The grid is published as a crates.io package rather than committed, so what this script can
# check without cargo is that the pin and the provenance record describe the same checkpoint.
# The bytes themselves are bound to the manifest by the `embedded_mainnet_final_frontiers_parse`
# unit test, which has the pinned dependency in hand; their framing is checked by
# `FrontierArtifact::decode`, by `scripts/pack-assets-crate.py` before publication, and by
# `zakurad verify-historical-treestates` before import.
if os.path.exists(FRONTIER_GRID):
    fail(
        f"{FRONTIER_GRID} is committed; the frontier grid ships as the pinned "
        f"{ASSETS_DEPENDENCY} dependency so it never enters git history"
    )

try:
    workspace_manifest = open(WORKSPACE_MANIFEST, encoding="utf-8").read()
except OSError as error:
    fail(f"cannot read {WORKSPACE_MANIFEST}: {error}")

pin = re.search(
    rf'^{re.escape(ASSETS_DEPENDENCY)} = \{{(?P<body>[^}}]*)\}}$',
    workspace_manifest,
    re.MULTILINE,
)
if not grid_keys:
    if pin is not None:
        fail(
            f"{WORKSPACE_MANIFEST} pins {ASSETS_DEPENDENCY} but the provenance record does not "
            "describe a frontier grid"
        )
else:
    if pin is None:
        fail(
            f"provenance describes a frontier grid but {WORKSPACE_MANIFEST} does not pin "
            f"{ASSETS_DEPENDENCY}"
        )
    requirement = re.search(r'version = "(?P<requirement>[^"]+)"', pin.group("body"))
    if requirement is None:
        fail(f"{ASSETS_DEPENDENCY} must pin a version")
    requirement = requirement.group("requirement")
    # Reviewed bytes, not a compatible range: a caret or wildcard requirement would let a
    # different artifact satisfy the same pin.
    if not requirement.startswith("="):
        fail(f"{ASSETS_DEPENDENCY} must pin an exact version, found {requirement!r}")
    pinned_version = requirement[1:]

    published = re.search(r'package = "(?P<package>[^"]+)"', pin.group("body"))
    published_name = published.group("package") if published else ASSETS_DEPENDENCY

    # The version is `0.<last_checkpoint>.<revision>`, so the pin itself states which checkpoint
    # the payload covers. This is what makes a pin bump without a manifest bump, or the reverse,
    # a failure rather than a silent divergence.
    parts = pinned_version.split(".")
    if len(parts) != 3 or parts[0] != "0" or not parts[1].isdigit() or not parts[2].isdigit():
        fail(
            f"{ASSETS_DEPENDENCY} version {pinned_version!r} is not "
            "0.<last_checkpoint>.<revision>"
        )
    if int(parts[1]) != height:
        fail(
            f"{ASSETS_DEPENDENCY} pins checkpoint {parts[1]}, not provenance finalized_height "
            f"{height}"
        )

    try:
        lock = open(LOCKFILE, encoding="utf-8").read()
    except OSError as error:
        fail(f"cannot read {LOCKFILE}: {error}")
    # `rest` stops at the blank line that ends a lockfile package block. Letting it run past
    # that would find some later package's source and checksum and pass on anything.
    locked = re.search(
        r'^\[\[package\]\]\nname = "'
        + re.escape(published_name)
        + r'"\nversion = "(?P<version>[^"]+)"\n(?P<rest>(?:[^\[\n].*\n)*)',
        lock,
        re.MULTILINE,
    )
    if locked is None:
        fail(f"{LOCKFILE} does not lock {published_name}")
    if locked.group("version") != pinned_version:
        fail(
            f"{LOCKFILE} locks {published_name} {locked.group('version')}, but "
            f"{WORKSPACE_MANIFEST} pins {pinned_version}"
        )
    # A path or git override resolves without a registry checksum, which would drop the strongest
    # byte-level pin the lockfile provides. Local overrides belong in `--config`, not in a commit.
    if 'source = "registry+https://github.com/rust-lang/crates.io-index"' not in locked.group(
        "rest"
    ):
        fail(f"{LOCKFILE} must resolve {published_name} from crates.io")
    if "checksum = " not in locked.group("rest"):
        fail(f"{LOCKFILE} must carry a registry checksum for {published_name}")

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
