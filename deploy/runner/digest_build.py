#!/usr/bin/env python3
"""Build one pinned comparison binary into the existing deployer cache, without deployment."""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys


def build(revision, lane, output):
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("build requires a full immutable revision")
    if lane not in {"us-west-0", "canada-0"}:
        raise ValueError("comparison builds require one of the two approved node lanes")
    root = Path(__file__).resolve().parents[2]
    actual = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True, timeout=10).strip()
    if actual != revision:
        raise ValueError("checkout does not match the requested revision")
    # Separate lanes prevent workspace-relative Cargo fingerprints from crossing revisions.
    os.environ["CARGO_TARGET_DIR"] = f"/root/.cache/zakura-digest-perf-target/{lane}"
    spec = importlib.util.spec_from_file_location("digest_deployer", root / "deploy/deployer/deploy.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    compiler = subprocess.check_output(["rustc", "--version", "--verbose"], text=True, timeout=10)
    binary = module.build_commit(root, revision, force=True)
    receipt = {
        "revision": revision,
        "lane": lane,
        "host": os.uname().nodename,
        "compiler": compiler,
        "profile": "release",
        "package": "zakura",
        "features": [],
        "rustflags": os.environ.get("RUSTFLAGS", ""),
        "cargo_target_dir": os.environ["CARGO_TARGET_DIR"],
        "binary": str(binary),
        "sha256": hashlib.file_digest(binary.open("rb"), "sha256").hexdigest(),
        "version": subprocess.check_output([str(binary), "--version"], text=True, timeout=10).strip(),
    }
    Path(output).write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--lane", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    build(args.revision, args.lane, args.output)


if __name__ == "__main__":
    main()
