#!/usr/bin/env python3
"""Require the owned Rust script crate in supported release dependency graphs."""

from pathlib import Path
import subprocess

root = Path(__file__).resolve().parent.parent
owned = root / "crates/zcash-script"
for features in ["default-release-binaries", "default-release-binaries,portable"]:
    result = subprocess.run(
        ["cargo", "tree", "--locked", "-p", "zakura", "--features", features,
         "--edges", "normal,build", "--prefix", "none", "--format", "{p}"],
        cwd=root, text=True, capture_output=True, check=True,
    )
    packages = result.stdout.splitlines()
    assert not any(package.startswith("libzcash_script ") for package in packages), (
        f"C++ script dependency remains in {features}"
    )
    scripts = {package.removesuffix(" (*)") for package in packages if package.startswith("zcash_script ")}
    assert scripts == {f"zcash_script v0.4.5 ({owned})"}, (
        f"release graph must use exactly one owned script crate: {scripts}"
    )
    print(f"{features}: owned Rust script crate; no C++ script dependency")
