#!/usr/bin/env python3
"""Check that a public-Testnet deploy keeps the NU7 dashboard reporting contract.

The NU7 activation collector reads `getnetworkparameters` at the tip and the next
height from every public-Testnet validator, and accepts a validator only when its
`getinfo` build names the revision pinned in the public manifest. A deploy that
drops the RPC, or that is not followed by a manifest update, freezes the public
dashboard while the nodes themselves stay healthy.

`source` runs before a build and fails if the checkout does not serve the RPC.
`nodes` runs after a deploy: it fails if a deployed node does not export rules
or does not run the deployed revision, and reports, without failing, a stale
dashboard or a manifest pin that differs from the deployed revision.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
import tomllib
import urllib.request
from pathlib import Path

RPC_METHOD = "getnetworkparameters"
METHODS_FILE = Path("crates/zakura-rpc/src/methods.rs")
PUBLIC_TESTNET_MAGIC = "fa1af9bf"
MAX_JSON_BYTES = 1_048_576
TIMEOUT_SECONDS = 10


def read_json(url, body=None):
    request = urllib.request.Request(
        url, data=body, headers={"Content-Type": "application/json"} if body else {}
    )
    with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        raw = response.read(MAX_JSON_BYTES + 1)
    if len(raw) > MAX_JSON_BYTES:
        raise ValueError("JSON response exceeds limit")
    return json.loads(raw)


def rpc(url, method, params=None):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []})
    reply = read_json(url, body.encode())
    if reply.get("error") is not None:
        raise ValueError(f"{method} failed: {reply['error'].get('message', 'error')}")
    return reply["result"]


def source_problems(repo: Path) -> list[str]:
    """Return why the checkout would not serve the collector's RPC, if it would not."""
    try:
        text = (repo / METHODS_FILE).read_text()
    except OSError as error:
        return [f"cannot read {METHODS_FILE}: {error.strerror}"]
    problems = []
    if f'#[method(name = "{RPC_METHOD}")]' not in text:
        problems.append(f"{METHODS_FILE} does not register {RPC_METHOD}")
    if f'("{RPC_METHOD}", RpcAccess::Unauthenticated)' not in text:
        problems.append(f"{RPC_METHOD} is not on the unauthenticated (restricted) surface")
    return problems


def node_rpc_urls(config_path: Path, node: str | None) -> dict[str, str]:
    """Map deployer node names to their RPC URLs, using the deployer config."""
    config = tomllib.loads(config_path.read_text())
    defaults = config.get("defaults", {})
    urls = {}
    for entry in config.get("nodes", []):
        if node and entry["name"] != node:
            continue
        host = entry["ssh_string"].rsplit("@", 1)[-1]
        port = (entry.get("rpc_listen_addr") or defaults["rpc_listen_addr"]).rsplit(":", 1)[-1]
        urls[entry["name"]] = f"http://{host}:{port}/"
    if node and not urls:
        raise ValueError(f"node {node} is not in {config_path}")
    return urls


def node_problems(name: str, url: str, revision: str) -> list[str]:
    """Check one node serves height-specific rules from the deployed revision."""
    try:
        build = rpc(url, "getinfo").get("build")
        problems = []
        if not isinstance(build, str) or revision[:9] not in build.lower():
            problems.append(f"{name} runs build {build!r}, not revision {revision[:9]}")
        tip = rpc(url, "getblockchaininfo")["blocks"]
        for height in (tip, tip + 1):
            exported = rpc(url, RPC_METHOD, [height])
            if exported.get("effectiveHeight") != height:
                problems.append(f"{name} {RPC_METHOD}({height}) returned another height")
            elif exported.get("networkMagic") != PUBLIC_TESTNET_MAGIC:
                problems.append(f"{name} {RPC_METHOD} is not for public Testnet")
        return problems
    except (OSError, ValueError, KeyError, TypeError) as error:
        return [f"{name}: {error}"]


def dashboard_warnings(url: str, revision: str, max_age: float, now: float) -> list[str]:
    """Report a stale or degraded public dashboard and a stale revision pin."""
    try:
        dashboard = read_json(url)
        warnings = []
        if dashboard.get("selectedProfile") != "public-testnet":
            return warnings
        pinned = dashboard["network"]["nodeRevision"]
        if pinned != revision:
            warnings.append(
                f"public NU7 manifest pins nodeRevision {pinned}, but this deploy runs {revision}; "
                "the collector rejects these nodes until public.manifest.nodeRevision is updated "
                "after all three validators run the same revision"
            )
        age = now - dashboard["generatedAt"]
        if age > max_age:
            warnings.append(f"public NU7 dashboard generation is {age:.0f}s old")
        status = dashboard["status"].get("status")
        if status != "live":
            warnings.append(f"public NU7 dashboard status is {status!r}")
        return warnings
    except (OSError, ValueError, KeyError, TypeError) as error:
        return [f"public NU7 dashboard unavailable: {error}"]


def report(kind: str, messages: list[str]) -> None:
    for message in messages:
        # GitHub Actions turns these lines into run annotations.
        print(f"::{kind}::{message}")


def main(argv=None, now=time.time, sleep=time.sleep) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    source = commands.add_parser("source", help="check the checkout before building")
    source.add_argument("--repo", type=Path, default=Path("."))
    nodes = commands.add_parser("nodes", help="check deployed nodes and the public dashboard")
    nodes.add_argument("--deployer-config", type=Path, required=True)
    nodes.add_argument("--node", default=None, help="only check this deployer node")
    nodes.add_argument("--revision", required=True, help="full deployed commit SHA")
    nodes.add_argument("--dashboard-url", default="https://api.nu7.valargroup.dev/v1/dashboard")
    nodes.add_argument("--max-age", type=float, default=120)
    nodes.add_argument("--attempts", type=int, default=30)
    nodes.add_argument("--retry-seconds", type=float, default=10)
    args = parser.parse_args(argv)

    if args.command == "source":
        problems = source_problems(args.repo)
        report("error", problems)
        if not problems:
            print(f"checkout serves {RPC_METHOD} on the restricted surface")
        return 1 if problems else 0

    if not re.fullmatch(r"[0-9a-f]{40}", args.revision):
        report("error", ["--revision must be a full lowercase commit SHA"])
        return 1
    urls = node_rpc_urls(args.deployer_config, args.node)
    # Restarted nodes open RPC after state initialization, so retry for a bounded time.
    for attempt in range(1, args.attempts + 1):
        problems = [p for name, url in urls.items() for p in node_problems(name, url, args.revision)]
        if not problems or attempt == args.attempts:
            break
        sleep(args.retry_seconds)
    report("error", problems)
    if not problems:
        print(f"{', '.join(urls)} export {RPC_METHOD} from {args.revision}")
    report("warning", dashboard_warnings(args.dashboard_url, args.revision, args.max_age, now()))
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
