#!/usr/bin/env python3
"""Build the pinned WOTS-only demo and run it against an isolated regtest node.

Reads the node's Git dependency pins, checks the resolved node and harness graphs,
builds both binaries, and starts the disposable-key scenario. Cargo.lock files
record the selected sources. No existing node or wallet database is used.
"""

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tomllib


def expected_sources(node: Path) -> dict[str, str]:
    """Read the authoritative Git revisions from the node workspace manifest."""
    with (node / "Cargo.toml").open("rb") as stream:
        dependencies = tomllib.load(stream)["workspace"]["dependencies"]
    return {
        dependency.get("package", name): f"git+{dependency['git']}?rev={dependency['rev']}#{dependency['rev']}"
        for name, dependency in dependencies.items()
        if isinstance(dependency, dict) and "git" in dependency and "rev" in dependency
    }


def verify_sources(packages: list[dict], expected: dict[str, str]) -> None:
    """Reject source drift and duplicate identities for each selected dependency."""
    for name, source in expected.items():
        found = [package for package in packages if package["name"] == name]
        if len(found) != 1 or found[0].get("source") != source:
            raise ValueError(f"{name} did not resolve uniquely to {source}")


def metadata(manifest: Path, features: list[str]) -> list[dict]:
    result = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--manifest-path", str(manifest), *features],
        cwd=manifest.parent, check=True, text=True, stdout=subprocess.PIPE, timeout=300,
    )
    return json.loads(result.stdout)["packages"]


def run_scenario(command: list[str], timeout: float = 300) -> None:
    """Run the harness in a private Unix process group; stop its node on interruption."""
    process = subprocess.Popen(command, start_new_session=True)
    try:
        returncode = process.wait(timeout=timeout)
    except (subprocess.TimeoutExpired, KeyboardInterrupt):
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
        finally:
            # The harness can exit before its node. Kill any remaining group members.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
        raise
    if returncode:
        raise subprocess.CalledProcessError(returncode, command)


def run_demo(node: Path) -> None:
    harness = Path(__file__).resolve().parent
    expected = expected_sources(node)
    node_features = ["--no-default-features", "--features", "internal-miner,experimental-pq-transparent"]
    node_packages = metadata(node / "Cargo.toml", node_features)
    verify_sources(node_packages, expected)
    harness_packages = metadata(harness / "Cargo.toml", [])
    harness_names = {package["name"] for package in harness_packages}
    verify_sources(harness_packages, {name: source for name, source in expected.items() if name in harness_names})
    for name in ("zcash_script", "zakura-transparent", "zakura-primitives"):
        if name not in harness_names:
            raise ValueError(f"the harness is missing {name}")
    target = node / "target"
    env = dict(os.environ, CARGO_PROFILE_DEV_DEBUG="0", CARGO_TARGET_DIR=str(target))
    subprocess.run(
        ["cargo", "build", "--locked", "-p", "zakura", *node_features],
        cwd=node, env=env, check=True, timeout=1800,
    )
    subprocess.run(
        ["cargo", "build", "--locked", "--manifest-path", str(harness / "Cargo.toml")],
        cwd=node, env=env, check=True, timeout=600,
    )
    run_scenario([str(target / "debug/zakura-wots-regtest"), str(target / "debug/zakurad")])


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args(argv)
    try:
        run_demo(Path(__file__).resolve().parents[2])
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"WOTS regtest demo failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
