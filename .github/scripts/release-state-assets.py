#!/usr/bin/env python3
"""Select, validate, and verify assets for one immutable release-state bundle."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

from release_state_assets import AssetError, check_identity, publication_needed, verify_visible

ROOT = Path(__file__).resolve().parents[2]


def load_tool(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


FETCH = load_tool("fetch_release_state", ROOT / ".github/scripts/fetch-release-state.py")
IMPORT = load_tool("import_release_state", ROOT / ".github/scripts/import-release-state.py")
PACK = load_tool("pack_assets", ROOT / "scripts/pack-assets-crate.py")


def select(root: Path, bundle: Path, resolution: dict) -> dict:
    grid = (bundle / PACK.GRID_NAME).read_bytes()
    height, _entries, _payload = PACK.parse_grid(grid, str(bundle))
    if height != resolution["height"]:
        raise AssetError("grid and selected bundle have different checkpoint heights")
    committed = IMPORT._checkpoint_height((root / IMPORT.CHECKPOINTS).read_bytes(), "committed list")
    result = {**resolution, "has_changes": height > committed,
              "name": PACK.crate_name(root / "crates/zakura-assets"),
              "version": f"0.{height}.0", "grid_sha256": hashlib.sha256(grid).hexdigest()}
    check_identity(result)
    return result


def previous_grid(root: Path, name: str) -> Path:
    result = subprocess.run(["cargo", "metadata", "--format-version", "1", "--locked"],
                            cwd=root, check=True, capture_output=True, text=True, timeout=300)
    packages = [p for p in json.loads(result.stdout)["packages"] if p["name"] == name]
    if len(packages) != 1:
        raise AssetError("expected exactly one currently pinned frontier grid")
    return Path(packages[0]["manifest_path"]).parent / "src/mainnet-frontier-grid.bin"


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
    mode = os.environ.get("MODE") or "update"
    meta_url, meta_sha = os.environ.get("META_URL", ""), os.environ.get("META_SHA256", "")
    request = os.environ.get("REQUEST_ID", "")
    output.mkdir(parents=True, exist_ok=True)
    if mode == "ensure-assets":
        if not meta_url or not meta_sha or not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", request):
            raise AssetError("ensure-assets requires immutable metadata URL, digest, and request ID")
        FETCH.resolve_pinned_bundle(meta_url, meta_sha, output / "bundle", output / "resolution.json")
    elif mode == "update":
        if meta_url or meta_sha or request:
            raise AssetError("pinned metadata and request ID require ensure-assets mode")
        latest = os.environ.get("LATEST_URL", "")
        if not latest:
            raise AssetError("MAINNET_RELEASE_STATE_LATEST_URL must be set")
        FETCH.resolve_bundle(latest, output / "bundle", output / "resolution.json")
    else:
        raise AssetError("unknown release-state mode")


def publish(root: Path, needed: bool) -> None:
    if needed is not True:
        raise AssetError("publication requires an explicit absent-version decision")
    subprocess.run(["cargo", "publish", "--manifest-path", "crates/zakura-assets/Cargo.toml",
                    "--allow-dirty", "--no-verify"], cwd=root, check=True, timeout=600)


def require_existing(state: Path, selected: dict) -> None:
    # Package into scratch space so a dry run never changes the checked-out crate manifest.
    with tempfile.TemporaryDirectory(prefix="zakura-dry-run-asset-") as scratch:
        crate = Path(scratch) / "crate"
        shutil.copytree(ROOT / "crates/zakura-assets", crate)
        packed = PACK.pack(state / "bundle" / PACK.GRID_NAME, crate, 0)
        if packed["sha256"] != selected["grid_sha256"] or packed["version"] != selected["version"]:
            raise AssetError("packaged dry-run asset differs from the selection")
    if publication_needed(selected):
        raise AssetError("non-publishing dry run requires the matching asset to be published through Update Mainnet release state")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("fetch", "select", "validate", "check", "publish", "verify", "require-existing"))
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
    if args.operation == "require-existing":
        require_existing(state, selected)
    elif args.operation == "validate":
        validate_candidate(ROOT, state / "bundle", selected)
    elif args.operation == "check":
        needed = publication_needed(selected)
        (state / "publication.json").write_text(json.dumps({"needed": needed}) + "\n")
        with open(os.environ["GITHUB_OUTPUT"], "a") as output:
            print(f"needed={str(needed).lower()}", file=output)
    elif args.operation == "publish":
        publish(ROOT, json.loads((state / "publication.json").read_text())["needed"])
    elif args.operation == "verify":
        verify_visible(selected)
        receipt = {key: selected[key] for key in (
            "meta_url", "meta_sha256", "height", "name", "version", "grid_sha256")}
        needed = json.loads((state / "publication.json").read_text())["needed"]
        receipt.update(schema_version=1,
                       request_id=os.environ.get("REQUEST_ID") or f'update-{os.environ["GITHUB_RUN_ID"]}-{os.environ["GITHUB_RUN_ATTEMPT"]}',
                       outcome="published" if needed else "reused",
                       producer_run_id=int(os.environ["GITHUB_RUN_ID"]),
                       producer_sha=os.environ["GITHUB_SHA"],
                       producer_attempt=int(os.environ["GITHUB_RUN_ATTEMPT"]))
        (state / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")


if __name__ == "__main__":
    main()
