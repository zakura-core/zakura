#!/usr/bin/env python3
"""Cluster Slack alerter for the permanent Zakura sync nodes."""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
import tomllib
import urllib.request
from pathlib import Path
from typing import Any


def now() -> int:
    return int(time.time())


def load_config(path: Path) -> dict[str, Any]:
    with path.open("rb") as config_file:
        return tomllib.load(config_file)


def load_env(path: Path) -> None:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def load_state(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"nodes": {}, "alerts": {}}


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def log(config: dict[str, Any], message: str) -> None:
    monitor_log = Path(str(config.get("defaults", {}).get("monitor_log", "/var/log/zakura/monitor.log")))
    monitor_log.parent.mkdir(parents=True, exist_ok=True)
    with monitor_log.open("a", encoding="utf-8") as file:
        file.write(time.strftime("%Y-%m-%dT%H:%M:%S%z ") + message + "\n")


def run_json(cmd: list[str], timeout: int = 8) -> tuple[dict[str, Any] | None, str | None]:
    try:
        result = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip().splitlines()
        return None, detail[-1] if detail else f"exit {result.returncode}"
    try:
        return json.loads(result.stdout), None
    except Exception as exc:
        return None, f"bad-json: {exc}"


def query_node(config: dict[str, Any], node: dict[str, Any]) -> dict[str, Any]:
    defaults = config.get("defaults", {})
    local = socket.gethostname().split(".", 1)[0]
    status_cmd = str(defaults.get("alert_status_command", "/usr/local/sbin/zakura-monitor-status.py"))
    if node["hostname"] == local:
        cmd = [status_cmd]
    else:
        cmd = [
            "ssh",
            "-i",
            str(defaults.get("alert_ssh_key", "/root/.ssh/zakura_monitor_ed25519")),
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=5",
            "-o",
            "StrictHostKeyChecking=accept-new",
            "-o",
            "UserKnownHostsFile=/root/.ssh/known_hosts",
            str(node["ssh_string"]),
            status_cmd,
        ]
    data, error = run_json(cmd)
    if data is not None:
        return data
    return {
        "hostname": node["hostname"],
        "public_ip": node.get("public_ip", "unknown"),
        "mode": node.get("mode_label", "unknown"),
        "service": defaults.get("service_name", "zakura-zebrad.service"),
        "service_active": None,
        "metrics_status": "unknown",
        "height": None,
        "connection": node.get("ssh_string", ""),
        "alias_connection": f"ssh {node.get('alias', node['hostname'])}",
        "log_path": defaults.get("log_file", "/var/log/zakura/zebrad.log"),
        "trace_path": defaults.get("trace_link", "/var/log/zakura/traces"),
        "monitor_log_path": defaults.get("monitor_log", "/var/log/zakura/monitor.log"),
        "query_error": error,
    }


def node_healthy(status: dict[str, Any]) -> bool:
    return (
        status.get("service_active") is True
        and status.get("metrics_status") == "ok"
        and status.get("height") is not None
    )


def controller_phase(status: dict[str, Any]) -> str:
    controller_state = status.get("controller_state")
    if not isinstance(controller_state, dict):
        return "unknown"
    return str(controller_state.get("phase") or "unknown")


def controller_failed(status: dict[str, Any]) -> bool:
    controller_state = status.get("controller_state")
    return isinstance(controller_state, dict) and bool(controller_state.get("failed"))


def expects_node_service(status: dict[str, Any]) -> bool:
    # During build/install/cleanup/cooldown phases the controller intentionally
    # keeps zakurad stopped. Treat the node process as required only while the
    # controller is actively syncing, or when no controller state is available.
    phase = controller_phase(status)
    return phase in ("syncing", "unknown")


def elected_leader(statuses: list[dict[str, Any]]) -> str | None:
    healthy = sorted(status["hostname"] for status in statuses if node_healthy(status))
    return healthy[0] if healthy else None


def height_text(status: dict[str, Any]) -> str:
    return "unknown" if status.get("height") is None else str(status["height"])


def alert_mode(status: dict[str, Any]) -> str:
    raw = str(status.get("mode") or status.get("p2p_stack") or "").lower()
    if "zakura" in raw or "v2" in raw:
        return "v2p2p"
    if "zebra" in raw or "legacy" in raw:
        return "legacy"
    return "dual" if "dual" in raw else raw or "unknown"


def ssh_target(status: dict[str, Any]) -> str:
    public_ip = str(status.get("public_ip") or "").strip()
    if public_ip and public_ip != "unknown":
        return f"root@{public_ip}"
    connection = str(status.get("connection") or "").strip()
    return connection.removeprefix("ssh ").strip() or "unknown"


def short_reason(reason: str, limit: int = 96) -> str:
    reason = " ".join((reason or "unknown").split())
    return reason if len(reason) <= limit else reason[: limit - 3] + "..."


def main_alert_text(kind: str, status: dict[str, Any], reason: str) -> str:
    label = kind.lower().replace(" recovered", " recovered")
    if label == "test alert":
        label = "continuous sync alert"
    elif label == "test recovered":
        label = "continuous sync recovered"
    icon = ":white_check_mark:" if "recovered" in label else ":rotating_light:"
    return f"{icon} Zakura {label}: {status['hostname']} | {alert_mode(status)} | {ssh_target(status)}"


def status_line(status: dict[str, Any]) -> str:
    extra = f", query_error={status.get('query_error')}" if status.get("query_error") else ""
    return (
        f"{status['hostname']} ({status.get('mode', 'unknown')}): "
        f"service_active={status.get('service_active')}, "
        f"metrics={status.get('metrics_status')}, height={height_text(status)}, "
        f"connect={status.get('alias_connection')} / {status.get('connection')}{extra}"
    )


def details_text(kind: str, status: dict[str, Any], statuses: list[dict[str, Any]], reason: str) -> str:
    cluster = "\n".join(status_line(item) for item in statuses)
    return (
        f"*Zakura alert details: {status['hostname']} {kind}*\n"
        f"*Full reason:* {reason}\n"
        f"*Service:* {status.get('service')}\n"
        f"*Service active:* {status.get('service_active')}\n"
        f"*Metrics:* {status.get('metrics_status')}\n"
        f"*Current height:* {height_text(status)}\n"
        f"*P2P mode:* {status.get('mode', 'unknown')}\n"
        f"*Connection:* {status.get('alias_connection')} | {status.get('connection')}\n"
        f"*Node log:* `{status.get('log_path')}`\n"
        f"*Traces:* `{status.get('trace_path')}`\n"
        f"*Monitor log:* `{status.get('monitor_log_path')}`\n"
        f"*Cluster status:*\n```{cluster}```"
    )


def slack_webhook_url() -> str:
    return (
        os.environ.get("SLACK_WEB_HOOK", "")
        or os.environ.get("SLACK_WEBHOOK_URL", "")
        or os.environ.get("SLACK_WEBHOOK", "")
    )


def post_slack(config: dict[str, Any], text: str) -> bool:
    if config.get("_dry_run"):
        print(f"DRY RUN Slack message:\n{text}\n")
        return True
    webhook = slack_webhook_url()
    if not webhook:
        log(config, "slack-webhook-missing")
        return False
    request = urllib.request.Request(
        webhook,
        data=json.dumps({"text": text}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=8) as response:
            body = response.read().decode("utf-8", "replace").strip()
    except Exception as exc:
        log(config, f"slack-send-failed err={type(exc).__name__}")
        return False
    if response.status < 200 or response.status >= 300 or body != "ok":
        log(config, f"slack-send-failed status={response.status} body={body}")
        return False
    return True


def post_alert(
    config: dict[str, Any],
    kind: str,
    status: dict[str, Any],
    statuses: list[dict[str, Any]],
    reason: str,
) -> bool:
    return post_slack(config, main_alert_text(kind, status, reason))


def update_progress_state(state: dict[str, Any], statuses: list[dict[str, Any]], ts: int) -> None:
    nodes = state.setdefault("nodes", {})
    for status in statuses:
        height = status.get("height")
        if height is None:
            continue
        record = nodes.setdefault(status["hostname"], {})
        if record.get("height") != height:
            record["height"] = height
            record["last_progress"] = ts
        else:
            record.setdefault("last_progress", ts)


def maybe_alert(
    config: dict[str, Any],
    state: dict[str, Any],
    key: str,
    active: bool,
    kind: str,
    status: dict[str, Any],
    statuses: list[dict[str, Any]],
    reason: str,
    ts: int,
) -> None:
    defaults = config.get("defaults", {})
    throttle = int(defaults.get("alert_throttle_seconds", 1800))
    alerts = state.setdefault("alerts", {})
    record = alerts.setdefault(key, {"active": False, "last_sent": 0})
    if active:
        if not record.get("active") or ts - int(record.get("last_sent", 0)) >= throttle:
            if post_alert(config, kind, status, statuses, reason):
                record["last_sent"] = ts
                log(config, f"alert-sent key={key} host={status['hostname']} kind={kind}")
        else:
            log(config, f"alert-throttled key={key} host={status['hostname']}")
        record["active"] = True
    elif record.get("active"):
        if post_alert(config, f"{kind} RECOVERED", status, statuses, "condition cleared"):
            record["last_sent"] = ts
            log(config, f"recovery-sent key={key} host={status['hostname']}")
        record["active"] = False


def run_once(config: dict[str, Any]) -> int:
    defaults = config.get("defaults", {})
    state_file = Path(str(defaults.get("alert_state_file", "/var/lib/zakura-monitor/cluster-state.json")))
    statuses = [query_node(config, node) for node in config.get("nodes", [])]
    ts = now()
    leader = elected_leader(statuses)
    local_host = socket.gethostname().split(".", 1)[0]
    state = load_state(state_file)
    update_progress_state(state, statuses, ts)
    log(
        config,
        "status leader=%s local=%s %s"
        % (leader, local_host, " | ".join(status_line(status) for status in statuses)),
    )

    service_name = str(defaults.get("service_name", "zakura-zebrad.service"))
    for status in statuses:
        name = status["hostname"]
        query_failed = bool(status.get("query_error"))
        down = False
        reasons = []
        if query_failed and name != local_host:
            log(config, f"peer-query-failed host={name} err={status.get('query_error')}")
        else:
            if controller_failed(status):
                down = True
                controller_state = status.get("controller_state") or {}
                reasons.append(f"controller halted: {controller_state.get('failure', 'unknown')}")
            if expects_node_service(status) and status.get("service_active") is not True:
                down = True
                reasons.append(f"{service_name} is not active")
            if expects_node_service(status) and status.get("metrics_status") != "ok":
                down = True
                reasons.append(f"metrics endpoint is {status.get('metrics_status')}")
        should_process = name == local_host or (leader == local_host and not query_failed)
        if should_process:
            maybe_alert(
                config,
                state,
                f"node-down:{name}",
                down,
                "DOWN",
                status,
                statuses,
                "; ".join(reasons) or "healthy",
                ts,
            )

    if leader == local_host:
        stall_seconds = int(defaults.get("cluster_stall_seconds", 600))
        for status in statuses:
            if status.get("query_error"):
                continue
            name = status["hostname"]
            record = state.get("nodes", {}).get(name, {})
            height = status.get("height")
            last_progress = int(record.get("last_progress", ts))
            age = ts - last_progress
            peer_evidence = []
            if node_healthy(status) and height is not None:
                for peer in statuses:
                    if peer["hostname"] == name or not node_healthy(peer):
                        continue
                    peer_height = peer.get("height")
                    peer_record = state.get("nodes", {}).get(peer["hostname"], {})
                    if (
                        peer_height is not None
                        and peer_height > height
                        and int(peer_record.get("last_progress", 0)) >= last_progress
                    ):
                        peer_evidence.append(
                            f"{peer['hostname']} height={peer_height} "
                            f"last_progress={peer_record.get('last_progress')}"
                        )
            stalled = node_healthy(status) and height is not None and age >= stall_seconds and bool(peer_evidence)
            reason = (
                f"height {height} has not progressed for {age}s "
                f"(threshold {stall_seconds}s); peer evidence: {', '.join(peer_evidence)}"
            )
            maybe_alert(
                config,
                state,
                f"cluster-stall:{name}",
                stalled,
                "STALLED",
                status,
                statuses,
                reason,
                ts,
            )

    save_state(state_file, state)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("/etc/zakura-continuous-sync/alert-monitor.toml"),
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = load_config(args.config)
    config["_dry_run"] = args.dry_run
    load_env(Path(str(config.get("defaults", {}).get("alert_env_file", "/etc/zakura-alerts.env"))))
    return run_once(config)


if __name__ == "__main__":
    raise SystemExit(main())
