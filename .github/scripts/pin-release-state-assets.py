#!/usr/bin/env python3
"""Pin the published frontier grid matching an imported release-state bundle."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import re
import subprocess
import tarfile
import tomllib
import urllib.error
import urllib.request
from pathlib import Path


PIN = re.compile(r'^(zakura-assets = \{[^}]*version = ")=[^"]+("[^}]*\})$', re.MULTILINE)


def pin_assets(root: Path, imported: dict) -> None:
    if not imported["has_changes"]:
        return
    height = imported["height"]
    if isinstance(height, bool) or not isinstance(height, int) or not 0 < height < 2**32:
        raise ValueError("imported checkpoint height must be a positive u32")
    # The state-update workflow publishes revision zero for each checkpoint.
    version = f"0.{height}.0"
    manifest = root / "Cargo.toml"
    text = manifest.read_text(encoding="utf-8")
    dependency = tomllib.loads(text)["workspace"]["dependencies"]["zakura-assets"]
    name = dependency.get("package", "zakura-assets")
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", name):
        raise ValueError("invalid asset package name")
    updated, count = PIN.subn(lambda match: f"{match[1]}={version}{match[2]}", text)
    if count != 1:
        raise ValueError("Cargo.toml must carry exactly one exact zakura-assets pin")

    request = urllib.request.Request(
        f"https://static.crates.io/crates/{name}/{name}-{version}.crate",
        headers={"User-Agent": "zakura-release-automation"},
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            archive = response.read()
    except urllib.error.HTTPError as error:
        if error.code == 404:
            raise ValueError(
                f"{name} {version} is not available; run Update Mainnet release state "
                "to publish the matching grid before preparing the release"
            ) from error
        raise
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as package:
        grid = package.extractfile(f"{name}-{version}/src/mainnet-frontier-grid.bin")
        if grid is None:
            raise ValueError("published asset package has no frontier grid")
        with grid:
            digest = hashlib.file_digest(grid, "sha256").hexdigest()
    if digest != imported["frontier_grid_sha256"]:
        raise ValueError(f"published {name} {version} differs from the verified bundle")

    # Resolve only this package; keep unrelated locked dependencies in place.
    manifest.write_text(updated, encoding="utf-8")
    subprocess.run(
        ["cargo", "update", "-p", name, "--precise", version], cwd=root, check=True,
    )
    print(f"Pinned {name} {version}, matching the verified frontier grid")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--import-result", type=Path, required=True)
    args = parser.parse_args()
    pin_assets(Path.cwd(), json.loads(args.import_result.read_text(encoding="utf-8")))


if __name__ == "__main__":
    main()
