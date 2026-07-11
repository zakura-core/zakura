#!/usr/bin/env python3
"""Emit one node's Zakura sync status as JSON for the Slack alert monitor."""

from __future__ import annotations

import argparse
import json
import socket
import subprocess
import tomllib
import urllib.request
from pathlib import Path
from typing import Any


def load_config(path: Path) -> dict[str, Any]:
    with path.open("rb") as config_file:
        return tomllib.load(config_file)


def service_active(service: str) -> bool:
    try:
        result = subprocess.run(
            ["systemctl", "is-active", "--quiet", service],
            timeout=4,
        )
        return result.returncode == 0
    except Exception:
        return False


def metric_height(text: str) -> int | None:
    # Prefer finalized/verified block progress over header-only metrics.
    priority = [
        "state_memory_best_committed_block_height",
        "state_memory_committed_block_height",
        "state_finalized_block_height",
        "state_checkpoint_finalized_block_height",
        "zcash_chain_verified_block_height",
        "sync_block_verified_tip_height",
        "checkpoint_verified_height",
        "checkpoint_processing_next_height",
    ]
    values = {name: [] for name in priority}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        base = parts[0].split("{", 1)[0]
        dotted_base = base.replace(".", "_")
        if dotted_base not in values:
            continue
        try:
            values[dotted_base].append(int(float(parts[1])))
        except ValueError:
            continue
    for name in priority:
        if values[name]:
            return max(values[name])
    return None


def node_info(config: dict[str, Any], hostname: str) -> dict[str, Any]:
    for node in config.get("nodes", []):
        if node.get("hostname") == hostname or node.get("name") == hostname:
            return node
    return {
        "hostname": hostname,
        "public_ip": "unknown",
        "mode_label": "unknown",
        "alias": hostname,
        "ssh_string": f"ssh {hostname}",
    }


def controller_state(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def status(config: dict[str, Any]) -> dict[str, Any]:
    defaults = config.get("defaults", {})
    hostname = socket.gethostname().split(".", 1)[0]
    node = node_info(config, hostname)
    metrics_url = str(defaults.get("metrics_url", "http://127.0.0.1:9999/metrics"))
    service = str(defaults.get("service_name", "zakura-zebrad.service"))
    controller_state_path = Path(
        str(
            defaults.get(
                "controller_state_path",
                "/var/lib/zakura-continuous-sync/state.json",
            )
        )
    )

    metrics_status = "unavailable"
    height = None
    try:
        with urllib.request.urlopen(metrics_url, timeout=3) as response:
            metrics = response.read(2_000_000).decode("utf-8", "replace")
        metrics_status = "ok"
        height = metric_height(metrics)
    except Exception as exc:
        metrics_status = f"unavailable: {type(exc).__name__}"

    return {
        "hostname": hostname,
        "public_ip": node.get("public_ip", "unknown"),
        "mode": node.get("mode_label", "unknown"),
        "service": service,
        "service_active": service_active(service),
        "metrics_status": metrics_status,
        "height": height,
        "controller_state": controller_state(controller_state_path),
        "connection": node.get("ssh_string", f"root@{node.get('public_ip', 'unknown')}"),
        "alias_connection": f"ssh {node.get('alias', hostname)}",
        "log_path": defaults.get("log_file", "/var/log/zakura/zebrad.log"),
        "trace_path": defaults.get("trace_link", "/var/log/zakura/traces"),
        "monitor_log_path": defaults.get("monitor_log", "/var/log/zakura/monitor.log"),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("/etc/zakura-continuous-sync/alert-monitor.toml"),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    print(json.dumps(status(load_config(args.config)), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
