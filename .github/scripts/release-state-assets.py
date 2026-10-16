#!/usr/bin/env python3
"""Select, validate, and verify assets for one immutable release-state bundle."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

from release_state_assets import AssetError, check_identity, publication_needed, verify_visible
from ready_release_state import (
    ROOT, FETCH, IMPORT, PACK, fresh, previous_grid, select, validate_checkpoint_identity,
)


def validate_candidate(root: Path, bundle: Path, selected: dict) -> None:
    """Check a candidate in scratch space, then prove subtree roots before publication."""
    check_identity(selected)
    with tempfile.TemporaryDirectory(prefix="zakura-validate-bundle-") as scratch:
        scratch = Path(scratch)
        # Recheck preserved inputs against the immutable metadata, never against latest.json.
        resolution_path = scratch / "resolution.json"
        resolution = FETCH.resolve_pinned_bundle(
            selected["meta_url"], selected["meta_sha256"], scratch / "verified", resolution_path,
        )
        expected = select(root, scratch / "verified", resolution)
        if expected != selected:
            raise AssetError("preserved selection differs from the immutable bundle")
        for name in FETCH.FILE_LIMITS:
            if (bundle / name).read_bytes() != (scratch / "verified" / name).read_bytes():
                raise AssetError(f"preserved bundle file differs from verified metadata: {name}")
        validate_checkpoint_identity(bundle, resolution)
        subprocess.run(["bash", str(ROOT / ".github/scripts/validate-checkpoints.sh"),
                        str(bundle / IMPORT.CHECKPOINTS.name)], check=True, timeout=30)
        old_grid = previous_grid(root, selected["name"])
        committed_height = IMPORT._checkpoint_height((root / IMPORT.CHECKPOINTS).read_bytes(), "committed list")
        if selected["height"] < committed_height:
            raise AssetError("committed state advanced beyond this candidate; resolve a new bundle")
        if selected["height"] == committed_height:
            for relative, name in ((IMPORT.CHECKPOINTS, IMPORT.CHECKPOINTS.name),
                                   (IMPORT.FRONTIER, IMPORT.FRONTIER.name),
                                   (IMPORT.SUBTREES, IMPORT.SUBTREE_BUNDLE_NAME)):
                if (root / relative).read_bytes() != (bundle / name).read_bytes():
                    raise AssetError("same-height candidate rewrites committed release state")
            if old_grid.read_bytes() != (bundle / PACK.GRID_NAME).read_bytes():
                raise AssetError("same-height candidate rewrites the pinned frontier grid")
        else:
            temporary_root = scratch / "checkout"
            for relative in (IMPORT.CHECKPOINTS, IMPORT.FRONTIER, IMPORT.SUBTREES, IMPORT.PROVENANCE):
                target = temporary_root / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(root / relative, target)
            IMPORT.import_bundle(temporary_root, bundle, resolution_path,
                                 floor_eos=False, previous_frontier_grid=old_grid)
    subprocess.run(
        ["cargo", "run", "--release", "--locked", "-p", "zakura", "--bin", "zakurad", "--",
         "verify-historical-treestates", "--network", "Mainnet",
         "--subtree-input", str(bundle / IMPORT.SUBTREE_BUNDLE_NAME),
         "--frontier-input", str(bundle / IMPORT.FRONTIER.name),
         "--frontier-grid-input", str(bundle / PACK.GRID_NAME)],
        cwd=root, check=True, timeout=3600,
    )


def fetch_candidate(output: Path) -> None:
    latest = os.environ.get("LATEST_URL", "")
    if not latest:
        raise AssetError("MAINNET_RELEASE_STATE_LATEST_URL must be set")
    output.mkdir(parents=True, exist_ok=True)
    FETCH.resolve_bundle(latest, output / "bundle", output / "resolution.json")


def publish(root: Path, needed: bool, *, command_env=None, fixture_registry=False) -> None:
    if needed is not True:
        raise AssetError("publication requires an explicit absent-version decision")
    args = ["cargo", "publish", "--manifest-path", "crates/zakura-assets/Cargo.toml", "--allow-dirty", "--no-verify"]
    if fixture_registry:
        args += ["--registry", "fixture"]
    subprocess.run(args, cwd=root, check=True, timeout=600, **({"env": command_env} if command_env else {}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("fetch", "select", "validate", "check", "publish", "verify"))
    parser.add_argument("--state", type=Path, required=True)
    args = parser.parse_args()
    state = args.state.resolve()
    if args.operation == "fetch":
        fetch_candidate(state)
        return
    if args.operation == "select":
        selected = select(ROOT, state / "bundle", json.loads((state / "resolution.json").read_text()))
        (state / "selection.json").write_text(json.dumps(selected, indent=2) + "\n")
        return
    selected = json.loads((state / "selection.json").read_text())
    fresh(selected["generated_at"])
    if args.operation == "validate":
        validate_candidate(ROOT, state / "bundle", selected)
    elif args.operation == "check":
        needed = publication_needed(selected)
        (state / "publication.json").write_text(json.dumps({"needed": needed}) + "\n")
        with open(os.environ["GITHUB_OUTPUT"], "a") as output:
            print(f"needed={str(needed).lower()}", file=output)
    elif args.operation == "publish":
        if (os.environ.get("GITHUB_REPOSITORY") != "zakura-core/zakura"
                or os.environ.get("GITHUB_REF") != "refs/heads/main"):
            raise AssetError("crate publication is restricted to canonical main")
        publish(ROOT, json.loads((state / "publication.json").read_text())["needed"])
    elif args.operation == "verify":
        verify_visible(selected)


if __name__ == "__main__":
    main()
