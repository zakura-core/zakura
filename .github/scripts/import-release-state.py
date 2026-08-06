#!/usr/bin/env python3
"""Import a verified Mainnet release-state bundle into a Zakura checkout."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

CHECKPOINTS = Path("crates/zakura-chain/src/parameters/checkpoint/main-checkpoints.txt")
FRONTIER = Path("crates/zakura-state/src/service/finalized_state/vct/mainnet-frontier.bin")
PROVENANCE = Path("crates/zakura-state/src/service/finalized_state/vct/mainnet-frontier.json")
EOS_FILE = Path("crates/zakurad/src/components/sync/end_of_support.rs")
EOS_PATTERN = re.compile(r"(ESTIMATED_RELEASE_HEIGHT: u32 = )([0-9_]+)")
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


def import_bundle(
    repo_root: Path,
    bundle: Path,
    resolution_path: Path,
    *,
    floor_eos: bool = True,
) -> dict[str, Any]:
    """Import a newer bundle while requiring an append-only checkpoint history."""

    resolution = _load_resolution(resolution_path)
    checkpoint_path = repo_root / CHECKPOINTS
    frontier_path = repo_root / FRONTIER
    provenance_path = repo_root / PROVENANCE

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

    checkpoint_path.write_bytes(bundle_checkpoints)
    frontier_path.write_bytes(bundle_frontier)
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
            "meta_sha256": resolution["meta_sha256"],
        },
    )

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
    class SelfTest(unittest.TestCase):
        def setUp(self) -> None:
            self.scratch = tempfile.TemporaryDirectory()
            self.root = Path(self.scratch.name)
            for relative in (CHECKPOINTS, FRONTIER, PROVENANCE, EOS_FILE):
                (self.root / relative).parent.mkdir(parents=True, exist_ok=True)
            (self.root / CHECKPOINTS).write_text("1 aa\n", encoding="utf-8")
            (self.root / FRONTIER).write_bytes(b"old")
            (self.root / EOS_FILE).write_text(
                "const ESTIMATED_RELEASE_HEIGHT: u32 = 1_000;\n",
                encoding="utf-8",
            )
            self.bundle = self.root / "bundle"
            self.bundle.mkdir()
            (self.bundle / CHECKPOINTS.name).write_text("1 aa\n2 bb\n", encoding="utf-8")
            (self.bundle / FRONTIER.name).write_bytes(b"new")
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
        )
        _write_json(args.result_out, result)
    except BundleImportError as error:
        print(f"release-state import failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
