#!/usr/bin/env python3
"""Check rustdoc for changed workspace crates and their local dependents."""

import argparse
import json
from pathlib import Path
import subprocess
import time

from affected_semver_packages import package_graph


GLOBAL_INPUTS = {
    "Cargo.toml", "Cargo.lock", "rust-toolchain.toml", ".cargo/config.toml",
    ".github/workflows/lint.yml", ".github/workflows/tests-unit.yml",
    ".github/workflows/scripts/check_pr_docs.py",
    ".github/workflows/scripts/test_check_pr_docs.py",
    ".github/workflows/scripts/affected_semver_packages.py",
}


def affected_packages(metadata, changed_files):
    """Include unpublished crates and reverse normal/build dependencies; fail broad on unknown Rust inputs."""
    packages, roots, reverse_dependencies = package_graph(metadata)
    workspace = Path(metadata["workspace_root"]).resolve()
    selected = set()
    for name in changed_files:
        path = Path(name)
        if name in GLOBAL_INPUTS or name.startswith(".github/actions/setup-zakura-build/"):
            return sorted(package["name"] for package in packages.values())
        absolute = (workspace / path).resolve()
        owners = [package_id for package_id, root in roots.items() if absolute.is_relative_to(root)]
        if owners:
            selected.add(max(owners, key=lambda package_id: len(roots[package_id].parts)))
        elif path.suffix == ".rs" or path.name in {"Cargo.toml", "Cargo.lock"} or name.startswith("crates/"):
            # Deleted packages and Rust inputs outside a package have no current owner.
            return sorted(package["name"] for package in packages.values())
    pending = list(selected)
    while pending:
        for dependent in reverse_dependencies[pending.pop()] - selected:
            selected.add(dependent)
            pending.append(dependent)
    return sorted(packages[package_id]["name"] for package_id in selected)


def main(argv=None):
    """Resolve a complete Git diff and run one shared-cache rustdoc invocation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    parser.add_argument("--head", required=True)
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[3]
    metadata = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"], cwd=root, text=True,
    ))
    paths = subprocess.check_output(
        ["git", "diff", "--name-only", "--no-renames", "-z", args.base, args.head, "--"], cwd=root,
    ).decode().split("\0")
    packages = affected_packages(metadata, [path for path in paths if path])
    if not packages:
        print("No workspace documentation inputs changed.")
        return 0
    print("Checking documentation: " + ", ".join(packages), flush=True)
    command = ["cargo", "doc", "--locked", "--no-deps", "--all-features",
               "--document-private-items", "--target-dir", str(root / "target/internal")]
    for package in packages:
        command.extend(["--package", package])
    started = time.monotonic()
    try:
        return subprocess.run(command, cwd=root, check=False).returncode
    finally:
        print(f"Rustdoc elapsed: {time.monotonic() - started:.1f}s", flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
