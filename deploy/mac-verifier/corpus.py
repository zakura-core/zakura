#!/usr/bin/env python3
"""Run exactly one deterministic case per invocation on a pinned native checkout."""
import argparse
import os
from pathlib import Path
import platform
import re
import subprocess
import time

from common import atomic_json, digest, read_json


def run(source, output, manifest):
    source, output = Path(source).resolve(), Path(output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    specification = read_json(manifest)
    def git(*args):
        return subprocess.check_output(["git", "-C", str(source), *args], text=True).strip()
    if git("rev-parse", "HEAD") != specification["source_sha"] or git("status", "--porcelain"):
        raise ValueError("corpus requires clean pinned source checkout")
    architecture = platform.machine()
    system = platform.system()
    if (system, architecture) not in (("Linux", "x86_64"), ("Darwin", "arm64")):
        raise ValueError("corpus requires native Linux x86_64 or macOS ARM64")
    toolchain = specification["toolchain"]
    rustc = subprocess.check_output(["rustc", "+" + toolchain, "-vV"], text=True)
    expected_host = "aarch64-apple-darwin" if system == "Darwin" else "x86_64-unknown-linux-gnu"
    if f"host: {expected_host}" not in rustc:
        raise ValueError("toolchain host does not match native execution")
    receipt = {"source_sha": git("rev-parse", "HEAD"), "toolchain": rustc,
               "architecture": architecture, "os": platform.platform(), "features": "default",
               "cargo_lock_sha256": digest(source / "Cargo.lock"),
               "manifest_sha256": digest(manifest), "cases": [], "passed": False}
    atomic_json(output / "corpus-receipt.json", receipt)
    environment = {**os.environ, "CARGO_BUILD_JOBS": "1", "CARGO_TERM_COLOR": "never"}
    for variable in ("CARGO_BUILD_TARGET", "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS"):
        environment.pop(variable, None)
    for index, case in enumerate(specification["tests"]):
        log = output / f"case-{index}.log"
        started = time.time()
        with log.open("w") as stream:
            result = subprocess.run(
                ["cargo", "+" + toolchain, "test", "-p", specification["crate"], "--lib", "--locked",
                 "--release", case, "--", "--exact", "--nocapture"], cwd=source,
                env=environment, stdout=stream, stderr=subprocess.STDOUT, timeout=3 * 3600,
            )
        passed = result.returncode == 0 and re.search(
            r"test result: ok\. 1 passed; 0 failed; 0 ignored;", log.read_text()) is not None
        receipt["cases"].append({"name": case, "passed": passed, "duration_seconds": time.time() - started,
                                  "log_sha256": digest(log)})
        atomic_json(output / "corpus-receipt.json", receipt)
        if not passed:
            raise ValueError(f"corpus case failed or did not execute exactly once: {case}")
    receipt["passed"] = True
    atomic_json(output / "corpus-receipt.json", receipt)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--manifest", default=str(Path(__file__).with_name("corpus.json")))
    args = parser.parse_args()
    run(args.source, args.output, args.manifest)
