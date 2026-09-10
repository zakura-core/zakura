#!/usr/bin/env python3
"""Import a verified Mainnet release-state bundle into a Zakura checkout."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import struct
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

CHECKPOINTS = Path("crates/zakura-chain/src/parameters/checkpoint/main-checkpoints.txt")
FRONTIER = Path("crates/zakura-state/src/service/finalized_state/vct/mainnet-frontier.bin")
PROVENANCE = Path("crates/zakura-state/src/service/finalized_state/vct/mainnet-vct-manifest.json")
SUBTREES = Path("crates/zakura-state/src/service/finalized_state/vct/mainnet-subtrees.bin")
EOS_FILE = Path("crates/zakurad/src/components/sync/end_of_support.rs")
EOS_PATTERN = re.compile(r"(ESTIMATED_RELEASE_HEIGHT: u32 = )([0-9_]+)")
SUBTREE_BUNDLE_NAME = "mainnet-treestate-subtrees.bin"
FRONTIER_GRID_BUNDLE_NAME = "mainnet-frontier-grid.bin"
# magic, version, network, grid spacing, last checkpoint, entry count.
FRONTIER_GRID_HEADER_PREFIX = struct.Struct("<8sHBIII")
FRONTIER_GRID_HEADER_LEN = FRONTIER_GRID_HEADER_PREFIX.size + 32
FRONTIER_GRID_VERSION = 1
MAX_FRONTIER_GRID_ENTRIES = 16_000_000
# A height and three u32 blob length prefixes; empty tree blobs are legal framing.
MIN_FRONTIER_GRID_ENTRY_LEN = 4 + 4 + 4 + 4
SUBTREE_HEADER_PREFIX = struct.Struct("<8sHBIIII")
SUBTREE_HEADER_LEN = SUBTREE_HEADER_PREFIX.size + 32
SUBTREE_RECORD_LEN = 2 + 4 + 32
SUBTREE_VERSION = 1
MAX_SUBTREE_RECORDS = 2**16
RESOLUTION_KEYS = {
    "height",
    "block_hash",
    "generated_at",
    "meta_url",
    "meta_sha256",
}


class BundleImportError(RuntimeError):
    """The verified bundle cannot safely extend the committed release state."""


def _load_resolution(path: Path) -> dict[str, Any]:
    try:
        resolution = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise BundleImportError(f"cannot read release-state resolution: {error}") from error
    if not isinstance(resolution, dict) or set(resolution) != RESOLUTION_KEYS:
        raise BundleImportError("release-state resolution has unexpected keys")
    height = resolution["height"]
    if isinstance(height, bool) or not isinstance(height, int) or not 0 < height < 2**32:
        raise BundleImportError("release-state resolution height is invalid")
    for key in RESOLUTION_KEYS - {"height"}:
        if not isinstance(resolution[key], str) or not resolution[key]:
            raise BundleImportError(f"release-state resolution {key} is invalid")
    return resolution


def _checkpoint_height(checkpoints: bytes, label: str) -> int:
    try:
        height, _hash = checkpoints.decode().splitlines()[-1].split(" ")
        return int(height)
    except (UnicodeDecodeError, ValueError, IndexError) as error:
        raise BundleImportError(f"{label} has an invalid terminal checkpoint") from error


def _write_json(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(value, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _subtree_digest(prefix: bytes, payload: bytes) -> bytes:
    digest = hashlib.sha256()
    digest.update(prefix)
    digest.update(payload)
    return digest.digest()


def _frontier_grid_digest(prefix: bytes, payload: bytes) -> bytes:
    digest = hashlib.sha256()
    digest.update(prefix)
    digest.update(payload)
    return digest.digest()


def _subtree_pools(data: bytes, label: str) -> tuple[int, bytes, bytes, bytes]:
    if len(data) < SUBTREE_HEADER_LEN:
        raise BundleImportError(f"{label} is a truncated subtree-root artifact")

    magic, version, network, last_checkpoint, *counts = SUBTREE_HEADER_PREFIX.unpack_from(data)
    if magic != b"ZKVCTST1":
        raise BundleImportError(f"{label} is not a subtree-root artifact")
    if version != SUBTREE_VERSION:
        raise BundleImportError(f"{label} has unsupported subtree-root version {version}")
    if network != 1:
        raise BundleImportError(f"{label} is not a Mainnet subtree-root artifact")
    if any(count > MAX_SUBTREE_RECORDS for count in counts):
        raise BundleImportError(f"{label} declares too many subtree-root records")

    payload = data[SUBTREE_HEADER_LEN:]
    expected_payload_len = sum(counts) * SUBTREE_RECORD_LEN
    if len(payload) != expected_payload_len:
        raise BundleImportError(f"{label} has invalid subtree-root framing")
    expected_digest = data[SUBTREE_HEADER_PREFIX.size:SUBTREE_HEADER_LEN]
    if _subtree_digest(data[:SUBTREE_HEADER_PREFIX.size], payload) != expected_digest:
        raise BundleImportError(f"{label} has an invalid subtree-root digest")

    pools = []
    offset = 0
    for count in counts:
        end = offset + count * SUBTREE_RECORD_LEN
        pools.append(payload[offset:end])
        offset = end
    return last_checkpoint, pools[0], pools[1], pools[2]


def _frontier_grid(data: bytes, label: str) -> tuple[int, bytes]:
    """Validate a frontier grid's framing, returning its checkpoint and payload."""

    if len(data) < FRONTIER_GRID_HEADER_LEN:
        raise BundleImportError(f"{label} is a truncated frontier grid")

    magic, version, network, _spacing, last_checkpoint, count = (
        FRONTIER_GRID_HEADER_PREFIX.unpack_from(data)
    )
    if magic != b"ZKVCTFR1":
        raise BundleImportError(f"{label} is not a frontier grid")
    if version != FRONTIER_GRID_VERSION:
        raise BundleImportError(f"{label} has unsupported frontier grid version {version}")
    if network != 1:
        raise BundleImportError(f"{label} is not a Mainnet frontier grid")
    if count > MAX_FRONTIER_GRID_ENTRIES:
        raise BundleImportError(f"{label} declares too many frontier grid entries")

    payload = data[FRONTIER_GRID_HEADER_LEN:]
    expected_digest = data[FRONTIER_GRID_HEADER_PREFIX.size:FRONTIER_GRID_HEADER_LEN]
    if _frontier_grid_digest(data[:FRONTIER_GRID_HEADER_PREFIX.size], payload) != expected_digest:
        raise BundleImportError(f"{label} has an invalid frontier grid digest")
    # A producer can create a valid digest over an invalid frame, so reject impossible counts
    # before walking the payload.
    if count * MIN_FRONTIER_GRID_ENTRY_LEN > len(payload):
        raise BundleImportError(f"{label} declares more entries than its payload holds")

    offset = 0
    previous: int | None = None
    for _ in range(count):
        (height,) = struct.unpack_from("<I", payload, offset)
        offset += 4
        if previous is not None and height <= previous:
            raise BundleImportError(f"{label} entries are not in increasing height order")
        previous = height
        for _pool in range(3):
            if offset + 4 > len(payload):
                raise BundleImportError(f"{label} has truncated frontier grid framing")
            (blob_len,) = struct.unpack_from("<I", payload, offset)
            offset += 4 + blob_len
            if offset > len(payload):
                raise BundleImportError(f"{label} has truncated frontier grid framing")
    if offset != len(payload):
        raise BundleImportError(f"{label} has trailing frontier grid bytes")

    # An entry at or above the checkpoint describes a height the grid does not cover, and the
    # format itself does not bound them.
    if previous is not None and previous >= last_checkpoint:
        raise BundleImportError(
            f"{label} has an entry at height {previous}, at or above its checkpoint "
            f"{last_checkpoint}"
        )

    return last_checkpoint, payload


def _prepare_frontier_grid_import(
    previous_grid: Path | None,
    bundle: Path,
    expected_last_checkpoint: int,
) -> bytes:
    """Validate the bundle's frontier grid against the published one and the checkpoint.

    The grid is not committed: it ships as a pinned crates.io package. The append-only check
    therefore compares against whatever grid the repository currently pins, which the caller
    materialises and passes in, rather than against a file in the tree.
    """

    bundle_path = bundle / FRONTIER_GRID_BUNDLE_NAME

    try:
        bundle_bytes = bundle_path.read_bytes()
    except OSError as error:
        raise BundleImportError(f"cannot read {FRONTIER_GRID_BUNDLE_NAME}: {error}") from error
    bundle_last_checkpoint, bundle_payload = _frontier_grid(
        bundle_bytes, FRONTIER_GRID_BUNDLE_NAME
    )
    # A node whose fast-sync handoff sits above the grid's checkpoint fails closed at startup, so
    # the grid has to advance in lockstep with the checkpoint list it ships beside.
    if bundle_last_checkpoint != expected_last_checkpoint:
        raise BundleImportError(
            f"{FRONTIER_GRID_BUNDLE_NAME} covers checkpoint {bundle_last_checkpoint}, not the "
            f"bundle's checkpoint height {expected_last_checkpoint}"
        )

    if previous_grid is not None:
        try:
            previous_bytes = previous_grid.read_bytes()
        except OSError as error:
            raise BundleImportError(f"cannot read {previous_grid}: {error}") from error
        _, previous_payload = _frontier_grid(previous_bytes, str(previous_grid))
        # Entry heights are a function of the chain, so a later export extends an earlier one
        # rather than rewriting it. A payload that is not a prefix means the generator changed
        # grid, or the entries themselves moved.
        # A prefix match also rules out a shorter grid, so truncation needs no separate check.
        if not bundle_payload.startswith(previous_payload):
            raise BundleImportError("bundle rewrites published frontier grid entries")

    return bundle_bytes


def _prepare_subtree_import(
    repo_root: Path,
    bundle: Path,
    expected_last_checkpoint: int,
) -> bytes:
    committed_path = repo_root / SUBTREES
    bundle_path = bundle / SUBTREE_BUNDLE_NAME

    try:
        bundle_bytes = bundle_path.read_bytes()
    except OSError as error:
        raise BundleImportError(f"cannot read {SUBTREE_BUNDLE_NAME}: {error}") from error
    bundle_last_checkpoint, *bundle_pools = _subtree_pools(bundle_bytes, SUBTREE_BUNDLE_NAME)
    if bundle_last_checkpoint != expected_last_checkpoint:
        raise BundleImportError(
            f"{SUBTREE_BUNDLE_NAME} last_checkpoint {bundle_last_checkpoint} does not match "
            f"checkpoint height {expected_last_checkpoint}"
        )

    if committed_path.exists():
        try:
            committed_bytes = committed_path.read_bytes()
        except OSError as error:
            raise BundleImportError(f"cannot read {SUBTREES}: {error}") from error
        _, *committed_pools = _subtree_pools(committed_bytes, str(SUBTREES))
        for name, old, new in zip(
            ("sapling", "orchard", "ironwood"),
            committed_pools,
            bundle_pools,
        ):
            if not new.startswith(old):
                raise BundleImportError(
                    f"bundle rewrites committed {name} subtree roots"
                )

    return bundle_bytes


def import_bundle(
    repo_root: Path,
    bundle: Path,
    resolution_path: Path,
    *,
    floor_eos: bool = True,
    previous_frontier_grid: Path | None = None,
) -> dict[str, Any]:
    """Import a newer bundle while requiring an append-only checkpoint history."""

    resolution = _load_resolution(resolution_path)
    checkpoint_path = repo_root / CHECKPOINTS
    frontier_path = repo_root / FRONTIER
    provenance_path = repo_root / PROVENANCE
    subtree_path = repo_root / SUBTREES

    try:
        committed_checkpoints = checkpoint_path.read_bytes()
        bundle_checkpoints = (bundle / CHECKPOINTS.name).read_bytes()
        bundle_frontier = (bundle / FRONTIER.name).read_bytes()
    except OSError as error:
        raise BundleImportError(f"cannot read release-state input: {error}") from error

    committed_height = _checkpoint_height(committed_checkpoints, str(CHECKPOINTS))
    bundle_height = resolution["height"]
    result = {
        **resolution,
        "committed_height": committed_height,
        "has_changes": bundle_height > committed_height,
    }
    print(f"bundle height {bundle_height}, committed height {committed_height}")
    if not result["has_changes"]:
        print("The committed checkpoint list is already at or above the bundle; nothing to do.")
        return result

    if not bundle_checkpoints.startswith(committed_checkpoints):
        raise BundleImportError(
            "bundle checkpoint list does not extend the committed list byte-for-byte; "
            "publisher and repository are on different selection grids"
        )
    if _checkpoint_height(bundle_checkpoints, "bundle checkpoint list") != bundle_height:
        raise BundleImportError("bundle checkpoint height does not match the resolution")

    subtree_bytes = _prepare_subtree_import(repo_root, bundle, bundle_height)
    grid_bytes = _prepare_frontier_grid_import(previous_frontier_grid, bundle, bundle_height)

    checkpoint_path.write_bytes(bundle_checkpoints)
    frontier_path.write_bytes(bundle_frontier)
    subtree_path.parent.mkdir(parents=True, exist_ok=True)
    subtree_path.write_bytes(subtree_bytes)
    print("imported subtree-root artifact")
    # The grid itself is published to crates.io by the workflow, not written into the tree.
    print("imported frontier grid")

    _write_json(
        provenance_path,
        {
            "schema_version": 1,
            "network": "Mainnet",
            "source": "release-state-bundle",
            "generated_at": resolution["generated_at"],
            "finalized_height": bundle_height,
            "finalized_hash": resolution["block_hash"],
            "checkpoints_sha256": hashlib.sha256(bundle_checkpoints).hexdigest(),
            "frontier_sha256": hashlib.sha256(bundle_frontier).hexdigest(),
            "frontier_size": len(bundle_frontier),
            "subtrees_sha256": hashlib.sha256(subtree_bytes).hexdigest(),
            "subtrees_size": len(subtree_bytes),
            "frontier_grid_sha256": hashlib.sha256(grid_bytes).hexdigest(),
            "frontier_grid_size": len(grid_bytes),
            "frontier_grid_entries": FRONTIER_GRID_HEADER_PREFIX.unpack_from(grid_bytes)[5],
            "meta_sha256": resolution["meta_sha256"],
        },
    )

    # The publish step names a crate version from this, and the PR body quotes it, so the
    # identity of the grid that was validated has to leave this function.
    result["frontier_grid_sha256"] = hashlib.sha256(grid_bytes).hexdigest()
    result["frontier_grid_size"] = len(grid_bytes)
    result["frontier_grid_entries"] = FRONTIER_GRID_HEADER_PREFIX.unpack_from(grid_bytes)[5]

    if floor_eos:
        eos_path = repo_root / EOS_FILE
        try:
            eos_text = eos_path.read_text(encoding="utf-8")
        except OSError as error:
            raise BundleImportError(f"cannot read {EOS_FILE}: {error}") from error
        match = EOS_PATTERN.search(eos_text)
        if match is None:
            raise BundleImportError(f"cannot find ESTIMATED_RELEASE_HEIGHT in {EOS_FILE}")
        current_eos = int(match.group(2).replace("_", ""))
        eos_floor = bundle_height + 3456
        if current_eos < eos_floor:
            formatted = f"{eos_floor:_}"
            eos_path.write_text(
                EOS_PATTERN.sub(rf"\g<1>{formatted}", eos_text, count=1),
                encoding="utf-8",
            )
            print(f"floored ESTIMATED_RELEASE_HEIGHT at {formatted}")
        else:
            print(f"ESTIMATED_RELEASE_HEIGHT {current_eos} already at or above the floor")

    return result


def _self_test() -> int:
    def subtree_artifact(last_checkpoint: int, *pools: bytes) -> bytes:
        payload = b"".join(pools)
        prefix = SUBTREE_HEADER_PREFIX.pack(
            b"ZKVCTST1",
            SUBTREE_VERSION,
            1,
            last_checkpoint,
            *(len(pool) // SUBTREE_RECORD_LEN for pool in pools),
        )
        return prefix + _subtree_digest(prefix, payload) + payload

    def grid_entry(height: int, *blobs: bytes) -> bytes:
        entry = struct.pack("<I", height)
        for blob in blobs:
            entry += struct.pack("<I", len(blob)) + blob
        return entry

    def frontier_grid(last_checkpoint: int, *entries: bytes) -> bytes:
        payload = b"".join(entries)
        prefix = FRONTIER_GRID_HEADER_PREFIX.pack(
            b"ZKVCTFR1",
            FRONTIER_GRID_VERSION,
            1,
            1,
            last_checkpoint,
            len(entries),
        )
        return prefix + _frontier_grid_digest(prefix, payload) + payload

    class SelfTest(unittest.TestCase):
        def setUp(self) -> None:
            self.scratch = tempfile.TemporaryDirectory()
            self.root = Path(self.scratch.name)
            for relative in (CHECKPOINTS, FRONTIER, PROVENANCE, SUBTREES, EOS_FILE):
                (self.root / relative).parent.mkdir(parents=True, exist_ok=True)
            (self.root / CHECKPOINTS).write_text("1 aa\n", encoding="utf-8")
            (self.root / FRONTIER).write_bytes(b"old")
            (self.root / SUBTREES).write_bytes(subtree_artifact(1, b"", b"", b""))
            # Stands in for the grid the repository currently pins, which the workflow
            # materialises from the registry before importing.
            self.previous_grid = self.root / "previous-frontier-grid.bin"
            self.previous_grid.write_bytes(
                frontier_grid(1, grid_entry(0, b"s", b"o", b"i"))
            )
            (self.root / EOS_FILE).write_text(
                "const ESTIMATED_RELEASE_HEIGHT: u32 = 1_000;\n",
                encoding="utf-8",
            )
            self.bundle = self.root / "bundle"
            self.bundle.mkdir()
            (self.bundle / CHECKPOINTS.name).write_text("1 aa\n2 bb\n", encoding="utf-8")
            (self.bundle / FRONTIER.name).write_bytes(b"new")
            (self.bundle / SUBTREE_BUNDLE_NAME).write_bytes(
                subtree_artifact(2, b"", b"", b"")
            )
            (self.bundle / FRONTIER_GRID_BUNDLE_NAME).write_bytes(
                frontier_grid(
                    2,
                    grid_entry(0, b"s", b"o", b"i"),
                    grid_entry(1, b"ss", b"oo", b"ii"),
                )
            )
            self.resolution = self.root / "resolution.json"
            self.resolution.write_text(
                json.dumps(
                    {
                        "height": 2,
                        "block_hash": "bb",
                        "generated_at": "2026-08-05T00:00:00Z",
                        "meta_url": "https://example.test/meta.json",
                        "meta_sha256": "cc",
                    }
                ),
                encoding="utf-8",
            )

        def tearDown(self) -> None:
            self.scratch.cleanup()

        def test_import_and_no_op(self) -> None:
            result = import_bundle(self.root, self.bundle, self.resolution)
            self.assertTrue(result["has_changes"])
            self.assertEqual((self.root / CHECKPOINTS).read_text(), "1 aa\n2 bb\n")
            self.assertEqual(
                json.loads((self.root / PROVENANCE).read_text())["finalized_height"],
                2,
            )
            self.assertIn("3_458", (self.root / EOS_FILE).read_text())
            self.assertFalse(import_bundle(self.root, self.bundle, self.resolution)["has_changes"])

        def test_rewritten_history_is_rejected(self) -> None:
            (self.bundle / CHECKPOINTS.name).write_text("1 cc\n2 bb\n", encoding="utf-8")
            with self.assertRaisesRegex(BundleImportError, "byte-for-byte"):
                import_bundle(self.root, self.bundle, self.resolution)

        def test_resolution_height_mismatch_is_rejected(self) -> None:
            resolution = json.loads(self.resolution.read_text(encoding="utf-8"))
            resolution["height"] = 3
            self.resolution.write_text(json.dumps(resolution), encoding="utf-8")
            (self.bundle / CHECKPOINTS.name).write_text(
                "1 aa\n2 bb\n4 dd\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(BundleImportError, "does not match"):
                import_bundle(self.root, self.bundle, self.resolution)

        def test_subtree_roots_are_imported_append_only(self) -> None:
            old = subtree_artifact(1, b"a" * SUBTREE_RECORD_LEN, b"", b"")
            new = subtree_artifact(
                2,
                b"a" * SUBTREE_RECORD_LEN + b"b" * SUBTREE_RECORD_LEN,
                b"c" * SUBTREE_RECORD_LEN,
                b"",
            )
            (self.root / SUBTREES).write_bytes(old)
            (self.bundle / SUBTREE_BUNDLE_NAME).write_bytes(new)

            import_bundle(self.root, self.bundle, self.resolution)

            self.assertEqual((self.root / SUBTREES).read_bytes(), new)
            provenance = json.loads((self.root / PROVENANCE).read_text())
            self.assertEqual(
                provenance["subtrees_sha256"],
                hashlib.sha256(new).hexdigest(),
            )
            self.assertEqual(provenance["subtrees_size"], len(new))

        def test_rewritten_subtree_roots_are_rejected(self) -> None:
            old = subtree_artifact(1, b"a" * SUBTREE_RECORD_LEN, b"", b"")
            rewritten = subtree_artifact(2, b"b" * SUBTREE_RECORD_LEN, b"", b"")
            (self.root / SUBTREES).write_bytes(old)
            (self.bundle / SUBTREE_BUNDLE_NAME).write_bytes(rewritten)

            with self.assertRaisesRegex(BundleImportError, "rewrites committed sapling"):
                import_bundle(self.root, self.bundle, self.resolution)

        def test_missing_subtree_artifact_is_rejected(self) -> None:
            (self.bundle / SUBTREE_BUNDLE_NAME).unlink()
            with self.assertRaisesRegex(BundleImportError, SUBTREE_BUNDLE_NAME):
                import_bundle(self.root, self.bundle, self.resolution)

        def test_frontier_grid_is_recorded_but_not_written(self) -> None:
            result = import_bundle(
                self.root,
                self.bundle,
                self.resolution,
                previous_frontier_grid=self.previous_grid,
            )

            bundled = (self.bundle / FRONTIER_GRID_BUNDLE_NAME).read_bytes()
            # The grid ships as a published crate, so importing must record it without ever
            # putting the bytes in the tree.
            self.assertFalse(
                (
                    self.root
                    / "crates/zakura-state/src/service/finalized_state/vct"
                    / FRONTIER_GRID_BUNDLE_NAME
                ).exists()
            )
            provenance = json.loads((self.root / PROVENANCE).read_text())
            self.assertEqual(
                provenance["frontier_grid_sha256"],
                hashlib.sha256(bundled).hexdigest(),
            )
            self.assertEqual(provenance["frontier_grid_size"], len(bundled))
            self.assertEqual(provenance["frontier_grid_entries"], 2)
            self.assertEqual(result["frontier_grid_sha256"], provenance["frontier_grid_sha256"])
            self.assertEqual(result["frontier_grid_entries"], 2)

        def test_frontier_grid_append_only_check_is_optional(self) -> None:
            # The very first publish has nothing to extend, so the check has to be skippable
            # without silently accepting a rewrite when a previous grid is available.
            result = import_bundle(self.root, self.bundle, self.resolution)
            self.assertEqual(result["frontier_grid_entries"], 2)

        def test_rewritten_frontier_grid_is_rejected(self) -> None:
            (self.bundle / FRONTIER_GRID_BUNDLE_NAME).write_bytes(
                frontier_grid(
                    2,
                    grid_entry(0, b"S", b"o", b"i"),
                    grid_entry(1, b"ss", b"oo", b"ii"),
                )
            )
            with self.assertRaisesRegex(BundleImportError, "rewrites published frontier grid"):
                import_bundle(
                    self.root,
                    self.bundle,
                    self.resolution,
                    previous_frontier_grid=self.previous_grid,
                )

        def test_truncated_frontier_grid_is_rejected(self) -> None:
            # Replacing a published grid with a shorter one drops entries that consumers may
            # already anchor on, so it has to fail the same way a rewrite does.
            self.previous_grid.write_bytes(
                frontier_grid(
                    2,
                    grid_entry(0, b"s", b"o", b"i"),
                    grid_entry(1, b"ssss", b"oooo", b"iiii"),
                )
            )
            (self.bundle / FRONTIER_GRID_BUNDLE_NAME).write_bytes(
                frontier_grid(2, grid_entry(0, b"s", b"o", b"i"))
            )
            with self.assertRaisesRegex(BundleImportError, "rewrites published frontier grid"):
                import_bundle(
                    self.root,
                    self.bundle,
                    self.resolution,
                    previous_frontier_grid=self.previous_grid,
                )

        def test_frontier_grid_for_another_checkpoint_is_rejected(self) -> None:
            (self.bundle / FRONTIER_GRID_BUNDLE_NAME).write_bytes(
                frontier_grid(3, grid_entry(0, b"s", b"o", b"i"))
            )
            with self.assertRaisesRegex(BundleImportError, "covers checkpoint 3"):
                import_bundle(self.root, self.bundle, self.resolution)

        def test_frontier_grid_entry_above_its_checkpoint_is_rejected(self) -> None:
            (self.bundle / FRONTIER_GRID_BUNDLE_NAME).write_bytes(
                frontier_grid(
                    2,
                    grid_entry(0, b"s", b"o", b"i"),
                    grid_entry(9, b"ss", b"oo", b"ii"),
                )
            )
            with self.assertRaisesRegex(BundleImportError, "at or above its checkpoint"):
                import_bundle(self.root, self.bundle, self.resolution)

        def test_tampered_frontier_grid_payload_is_rejected(self) -> None:
            grid = bytearray(
                frontier_grid(2, grid_entry(0, b"s", b"o", b"i"), grid_entry(1, b"ss", b"oo", b"ii"))
            )
            grid[-1] ^= 0xFF
            (self.bundle / FRONTIER_GRID_BUNDLE_NAME).write_bytes(bytes(grid))
            with self.assertRaisesRegex(BundleImportError, "invalid frontier grid digest"):
                import_bundle(self.root, self.bundle, self.resolution)

        def test_frontier_grid_count_beyond_payload_is_rejected(self) -> None:
            grid = bytearray(frontier_grid(2, grid_entry(0, b"s", b"o", b"i")))
            struct.pack_into("<I", grid, 19, 1_000)
            payload = grid[FRONTIER_GRID_HEADER_LEN:]
            grid[
                FRONTIER_GRID_HEADER_PREFIX.size:FRONTIER_GRID_HEADER_LEN
            ] = _frontier_grid_digest(
                grid[:FRONTIER_GRID_HEADER_PREFIX.size],
                payload,
            )
            (self.bundle / FRONTIER_GRID_BUNDLE_NAME).write_bytes(bytes(grid))
            with self.assertRaisesRegex(BundleImportError, "more entries than its payload"):
                import_bundle(self.root, self.bundle, self.resolution)

        def test_missing_frontier_grid_is_rejected(self) -> None:
            (self.bundle / FRONTIER_GRID_BUNDLE_NAME).unlink()
            with self.assertRaisesRegex(BundleImportError, FRONTIER_GRID_BUNDLE_NAME):
                import_bundle(self.root, self.bundle, self.resolution)

        def test_import_can_leave_eos_for_release_preparation(self) -> None:
            before = (self.root / EOS_FILE).read_text(encoding="utf-8")
            result = import_bundle(
                self.root,
                self.bundle,
                self.resolution,
                floor_eos=False,
            )
            self.assertTrue(result["has_changes"])
            self.assertEqual((self.root / EOS_FILE).read_text(encoding="utf-8"), before)

    suite = unittest.defaultTestLoader.loadTestsFromTestCase(SelfTest)
    return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--bundle", type=Path)
    parser.add_argument("--resolution", type=Path)
    parser.add_argument("--result-out", type=Path)
    parser.add_argument(
        "--previous-frontier-grid",
        type=Path,
        help="grid the repository currently pins, for the append-only check",
    )
    parser.add_argument(
        "--no-eos-floor",
        action="store_true",
        help="leave ESTIMATED_RELEASE_HEIGHT unchanged",
    )
    args = parser.parse_args()
    if args.self_test:
        return _self_test()
    if not (args.bundle and args.resolution and args.result_out):
        parser.error("--bundle, --resolution, and --result-out are required")
    try:
        result = import_bundle(
            args.repo_root,
            args.bundle,
            args.resolution,
            floor_eos=not args.no_eos_floor,
            previous_frontier_grid=args.previous_frontier_grid,
        )
        _write_json(args.result_out, result)
    except BundleImportError as error:
        print(f"release-state import failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
