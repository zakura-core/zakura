#!/usr/bin/env python3
"""Deploy and audit the permanent Zakura continuous genesis sync fleet."""

from __future__ import annotations

import argparse
import calendar
import concurrent.futures
import json
import os
import shlex
import subprocess
import sys
import time
import tomllib
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NamedTuple

SCRIPT_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = SCRIPT_DIR / "templates"

# This branch-only probe prints bounded, read-only evidence into the Actions log.
# It preserves the stopped service and its run artifacts.
STALL_DIAGNOSTIC_TARGETS = {
    "us-west-0": "root@143.244.184.176",
    "canada-0": "root@159.203.38.10",
    "europe-west-0": "root@64.227.44.93",
    "asia-south-0": "root@139.59.64.115",
    "asia-pacific-0": "root@168.144.173.250",
    "start-temp-zakura-sync-test-5": "root@142.93.27.189",
}

STALL_DIAGNOSTIC_START_TARGETS = {"start-temp-zakura-sync-test-5"}

PR800_VALIDATION_TARGET = "validate-pr800-failed-nodes"
PR800_STATUS_TARGET = "status-pr800-failed-nodes"
PR800_VALIDATION_SHA = "d3722adda63cdee8aac6b148788798f812a63285"
PR800_VALIDATION_NODES = {
    "temp-zakura-sync-test-1": "root@138.68.43.212",
    "temp-zakura-sync-test-2": "root@138.197.218.91",
}

PR800_VALIDATION_SCRIPT = rf"""
set -euo pipefail

sha={PR800_VALIDATION_SHA}
repo=/root/zakura
config=/etc/zakura-continuous-sync/controller.toml
binary=/var/lib/zakura-continuous-sync/build-cache/zakurad-${{sha}}
unit=zakura-pr800-validation.service

if systemctl is-active --quiet zakura-continuous-sync.service; then
    printf 'refusing validation: controller is active\n' >&2
    exit 1
fi
if systemctl is-active --quiet zakura.service; then
    printf 'refusing validation: managed node service is active\n' >&2
    exit 1
fi

git -C "${{repo}}" fetch origin fix/vct-root-repair-livelock
resolved="$(git -C "${{repo}}" rev-parse origin/fix/vct-root-repair-livelock^{{commit}})"
if [ "${{resolved}}" != "${{sha}}" ]; then
    printf 'PR 800 moved: expected %s, resolved %s\n' "${{sha}}" "${{resolved}}" >&2
    exit 1
fi

python3 - "${{config}}" "${{sha}}" <<'PY'
import importlib.util
import os
import pathlib
import sys

controller_path = pathlib.Path("/usr/local/sbin/zakura-continuous-sync.py")
spec = importlib.util.spec_from_file_location("continuous_sync", controller_path)
controller = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules[spec.name] = controller
spec.loader.exec_module(controller)
os.environ["PATH"] = "/root/.cargo/bin:" + os.environ.get("PATH", "")
config = controller.load_config(pathlib.Path(sys.argv[1]))
binary = controller.build_binary(config, sys.argv[2])
print(f"built={{binary}}")
PY

if [ ! -x "${{binary}}" ]; then
    printf 'validation binary missing: %s\n' "${{binary}}" >&2
    exit 1
fi

systemctl stop "${{unit}}" 2>/dev/null || true
systemctl reset-failed "${{unit}}" 2>/dev/null || true
systemd-run \
    --unit="${{unit%.service}}" \
    --description='Zakura PR 800 preserved-state validation' \
    --property=Type=simple \
    --property=Restart=no \
    --property=WorkingDirectory=/root/zakura \
    "${{binary}}" --config /etc/zakura/zebrad.toml start

sleep 15
systemctl show "${{unit}}" \
    --property=ActiveState,SubState,Result,ExecMainStatus,MainPID,ActiveEnterTimestamp \
    --no-pager
curl -fsS --max-time 20 http://127.0.0.1:9999/metrics 2>/dev/null |
    grep -E 'checkpoint_(verified_height|processing_next_height)|state_finalized_block_height|sync_(header|block|estimated|downloads)|vct|repair' |
    tail -n 500 || true
"""

PR800_STATUS_SCRIPT = r"""
set -u

printf '===== time =====\n'
date --iso-8601=seconds

printf '\n===== service =====\n'
systemctl show zakura-pr800-validation.service \
    --property=ActiveState,SubState,Result,ExecMainStatus,MainPID,ActiveEnterTimestamp \
    --no-pager

printf '\n===== metrics =====\n'
curl -fsS --max-time 20 http://127.0.0.1:9999/metrics 2>/dev/null |
    grep -E '^(zcash_chain_verified_block_height|state_finalized_block_height|checkpoint_(verified_height|processing_next_height)|sync_(estimated_network_tip_height|estimated_distance_to_tip)|sync_header_vct_repair[^ ]*|sync_header[^ ]*repair[^ ]*)' |
    tail -n 1000 || true

printf '\n===== repair-trace =====\n'
find -L /var/log/zakura/traces -maxdepth 2 -type f -name '*.jsonl' -print0 2>/dev/null |
    while IFS= read -r -d '' file; do
        tail -n 100000 "${file}" |
            grep -E -i 'vct.*repair|repair.*vct|no_supplier|replacement|required|missingroot' |
            tail -n 300 || true
    done

printf '\n===== journal =====\n'
journalctl -u zakura-pr800-validation.service --since '30 minutes ago' --no-pager -n 3000 2>&1 |
    grep -E -i 'info|warn|error|fail|stall|repair|MissingRoot|height' |
    tail -n 1000 || true
"""

STALL_DIAGNOSTIC_SCRIPT = r"""
set -u

section() {
    printf '\n===== %s =====\n' "$1"
}

section identity
date --iso-8601=seconds
hostname
uname -a
uptime

section resources
df -h
df -i
free -h
ps -eo pid,ppid,stat,etimes,%cpu,%mem,rss,cmd --sort=-rss | head -n 50

section services
for unit in zakura.service zakurad.service zakura-continuous-sync.service; do
    printf '\n--- %s ---\n' "${unit}"
    systemctl show "${unit}" \
        --property=ActiveState,SubState,Result,ExecMainStatus,NRestarts,ActiveEnterTimestamp \
        --no-pager 2>&1 || true
done

section process-stacks
node_pid="$(systemctl show zakura.service --property=MainPID --value 2>/dev/null || true)"
if [ -n "${node_pid}" ] && [ "${node_pid}" != "0" ]; then
    ps -L -p "${node_pid}" -o pid,tid,stat,etimes,pcpu,pmem,wchan:32,comm | head -n 200
    if command -v gdb >/dev/null 2>&1; then
        timeout 30 gdb --batch --quiet -p "${node_pid}" \
            -ex 'set pagination off' \
            -ex 'thread apply all bt 20' 2>&1 | tail -n 5000 || true
    else
        printf 'gdb is not installed\n'
    fi
fi

section controller-state
for file in \
    /var/lib/zakura-continuous-sync/state.json \
    /var/lib/zakura-monitor/cluster-state.json; do
    if [ -f "${file}" ]; then
        printf '\n--- %s ---\n' "${file}"
        cat "${file}"
    fi
done

section artifact-inventory
ls -la /var/log/zakura /var/log/zakura/runs /var/log/zakura/traces /root/logs 2>&1 || true
find -L /var/log/zakura /root/logs \
    -maxdepth 5 -type f \
    -printf '%TY-%Tm-%TdT%TH:%TM:%TS %s %p\n' 2>/dev/null | sort || true

section run-metadata
find -L /var/log/zakura/runs -maxdepth 2 \
    -type f -name run.json -print0 2>/dev/null |
    while IFS= read -r -d '' file; do
        printf '\n--- %s ---\n' "${file}"
        cat "${file}"
    done

section recent-samples
find -L /var/log/zakura/runs -maxdepth 2 \
    -type f -name samples.jsonl -print0 2>/dev/null |
    while IFS= read -r -d '' file; do
        printf '\n--- %s ---\n' "${file}"
        tail -n 300 "${file}"
    done

section node-log-errors
find -L /var/log/zakura/runs /var/log/zakura /root/logs \
    -maxdepth 3 -type f \
    \( -name zebrad.log -o -name zakura-tracing.log \) \
    -print0 2>/dev/null |
    while IFS= read -r -d '' file; do
        printf '\n--- %s errors ---\n' "${file}"
        tail -n 100000 "${file}" |
            grep -E -i 'warn|error|fail|stall|panic|MissingRoot|ConflictingReplay|repair' |
            tail -n 1000 || true
        printf '\n--- %s tail ---\n' "${file}"
        tail -n 200 "${file}"
    done

section trace-excerpts
find -L /var/log/zakura/runs /root/logs \
    -maxdepth 4 -type f -name '*.jsonl' -print0 2>/dev/null |
    while IFS= read -r -d '' file; do
        printf '\n--- %s ---\n' "${file}"
        tail -n 50000 "${file}" |
            grep -E -i \
                'MissingRoot|ConflictingReplay|repair|supplier|stall|error|fail|waiting|verifier|transition|replay|round_finish|pipeline_reset|block_finish|notfound' |
            tail -n 500 || true
    done

section legacy-sync-frontier
python3 - <<'PY'
import collections
import json
import os

path = "/var/log/zakura/traces/legacy_sync.jsonl"
max_bytes = 1024 * 1024 * 1024
if not os.path.isfile(path):
    print(f"missing trace: {path}")
    raise SystemExit

states = {}
snapshots = collections.deque(maxlen=8)
round_events = collections.deque(maxlen=20)
process_trace_id = None

def missing_height_ranges(state_tip, heights):
    if state_tip is None or not heights:
        return []
    height_set = set(heights)
    ranges = []
    for height in range(state_tip + 1, heights[-1] + 1):
        if height in height_set:
            continue
        if ranges and height == ranges[-1][1] + 1:
            ranges[-1][1] = height
        else:
            ranges.append([height, height])
    return ranges

size = os.path.getsize(path)
with open(path, "rb") as trace:
    start = max(0, size - max_bytes)
    trace.seek(start)
    if start:
        trace.readline()
    for raw_line in trace:
        try:
            event = json.loads(raw_line)
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        event_process = event.get("process_trace_id")
        if event_process != process_trace_id:
            process_trace_id = event_process
            states.clear()
            snapshots.clear()
            round_events.clear()
        event_name = event.get("event")
        if event_name in {
            "round_start",
            "round_finish",
            "round_error_snapshot",
            "round_stalled",
            "pipeline_reset",
        }:
            round_events.append({
                key: event[key]
                for key in (
                    "ts",
                    "event",
                    "reason",
                    "state_tip",
                    "error",
                    "in_flight",
                    "reserve",
                    "registry_retries",
                )
                if key in event
            })
        if event_name == "block_phase":
            block_hash = event.get("hash")
            if block_hash:
                previous = states.get(block_hash, {})
                states[block_hash] = {
                    "height": event.get("height", previous.get("height")),
                    "phase": event.get("phase"),
                }
        elif event_name == "block_finish":
            states.pop(event.get("hash"), None)
        elif event_name in {"pipeline_reset", "round_error_snapshot", "round_stalled"}:
            tasks = event.get("tasks", [])
            heights = sorted(
                task["height"] for task in tasks if task.get("height") is not None
            )
            ranges = missing_height_ranges(event.get("state_tip"), heights)
            snapshots.append({
                "event": event_name,
                "state_tip": event.get("state_tip"),
                "in_flight": event.get("in_flight"),
                "reserve": event.get("reserve"),
                "registry_retries": event.get("registry_retries"),
                "known_height_min": heights[0] if heights else None,
                "known_height_max": heights[-1] if heights else None,
                "known_height_count": len(heights),
                "missing_height_count": sum(end - start + 1 for start, end in ranges),
                "missing_height_ranges": ranges[:50],
                "missing_ranges_truncated": len(ranges) > 50,
            })
            if event_name == "pipeline_reset":
                states.clear()

phase_counts = collections.Counter(state.get("phase") for state in states.values())
heights = sorted(
    state["height"] for state in states.values() if state.get("height") is not None
)
state_tip = next(
    (snapshot["state_tip"] for snapshot in reversed(snapshots) if snapshot["state_tip"] is not None),
    None,
)
missing_ranges = missing_height_ranges(state_tip, heights)

print(json.dumps({
    "path": path,
    "file_bytes": size,
    "scanned_bytes": min(size, max_bytes),
    "process_trace_id": process_trace_id,
    "active_task_count": len(states),
    "phase_counts": dict(sorted(phase_counts.items())),
    "known_height_count": len(heights),
    "known_height_min": heights[0] if heights else None,
    "known_height_max": heights[-1] if heights else None,
    "state_tip": state_tip,
    "missing_height_count": sum(end - start + 1 for start, end in missing_ranges),
    "missing_height_ranges": missing_ranges[:200],
    "missing_ranges_truncated": len(missing_ranges) > 200,
    "recent_round_events": list(round_events),
    "recent_snapshots": list(snapshots),
}, indent=2, sort_keys=True))
PY

section journals
for unit in zakura.service zakurad.service zakura-continuous-sync.service; do
    printf '\n--- %s ---\n' "${unit}"
    journalctl -u "${unit}" --since '7 days ago' --no-pager -n 5000 2>&1 |
        grep -E -i 'warn|error|fail|stall|panic|MissingRoot|ConflictingReplay|repair' |
        tail -n 500 || true
done

section metrics
curl -fsS --max-time 20 http://127.0.0.1:9999/metrics 2>/dev/null |
    grep -E -i 'height|tip|sync|checkpoint|verif|queue|repair|replay|peer|error|fail' |
    tail -n 3000 || true
"""

SSH_COMMON_OPTS = [
    "-o",
    "BatchMode=yes",
    "-o",
    "ConnectTimeout=15",
    "-o",
    "StrictHostKeyChecking=accept-new",
    "-o",
    "ServerAliveInterval=30",
]


class DeployError(Exception):
    """Operator-facing deploy failure."""


@dataclass(frozen=True)
class Node:
    raw: dict[str, Any]

    @property
    def name(self) -> str:
        return str(self.raw["name"])

    @property
    def ssh_string(self) -> str:
        return str(self.raw["ssh_string"])

    def ssh_cmd(self, *remote: str) -> list[str]:
        return ["ssh", *SSH_COMMON_OPTS, self.ssh_string, *remote]

    def scp_to(self, local: Path, remote: str) -> list[str]:
        return ["scp", *SSH_COMMON_OPTS, str(local), f"{self.ssh_string}:{remote}"]


def run(
    cmd: list[str],
    *,
    capture: bool = False,
    check: bool = True,
    input_text: str | None = None,
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            cmd,
            check=check,
            capture_output=capture,
            text=True,
            input=input_text,
        )
    except subprocess.CalledProcessError as error:
        detail = ""
        if capture:
            detail = (error.stderr or error.stdout or "").strip()
        raise DeployError(
            f"command failed ({error.returncode}): {' '.join(shlex.quote(c) for c in cmd)}\n{detail}"
        ) from error


def load_nodes(path: Path, selected: list[str] | None) -> list[Node]:
    with path.open("rb") as config_file:
        data = tomllib.load(config_file)
    defaults = data.get("defaults", {})
    nodes = []
    seen = set()
    for raw_node in data.get("nodes", []):
        merged = dict(defaults)
        merged.update(raw_node)
        for required in ("name", "ssh_string", "hostname", "mode_label", "p2p_stack"):
            if required not in merged:
                raise DeployError(f"node missing required field {required!r}: {raw_node}")
        if merged["name"] in seen:
            raise DeployError(f"duplicate node name: {merged['name']}")
        seen.add(merged["name"])
        nodes.append(Node(merged))
    if not nodes:
        raise DeployError(f"no [[nodes]] defined in {path}")
    if selected:
        wanted = set(selected)
        unknown = wanted - {node.name for node in nodes}
        if unknown:
            raise DeployError(f"unknown --node name(s): {', '.join(sorted(unknown))}")
        nodes = [node for node in nodes if node.name in wanted]
    return nodes


def toml_string_list(values: list[str]) -> str:
    return ", ".join(json.dumps(str(value)) for value in values)


def render_template(name: str, substitutions: dict[str, str]) -> str:
    text = (TEMPLATES_DIR / name).read_text(encoding="utf-8")
    for key, value in substitutions.items():
        text = text.replace("{{" + key + "}}", value)
    return text


def subst_for(node: Node) -> dict[str, str]:
    raw = node.raw
    bootstrap_peers = [str(peer) for peer in raw.get("bootstrap_peers", [])]
    return {
        "REPO_DIR": str(raw["repo_dir"]),
        "STATE_DIR": str(raw["state_dir"]),
        "RUNS_DIR": str(raw["runs_dir"]),
        "CHAIN_STATE_DIR": str(raw["chain_state_dir"]),
        "WIPE_SENTINEL": str(raw["wipe_sentinel"]),
        "BUILD_CACHE_DIR": str(raw["build_cache_dir"]),
        "CONFIG_TEMPLATE_PATH": str(raw["config_template_path"]),
        "CONFIG_PATH": str(raw["config_path"]),
        "BIN_PATH": str(raw["bin_path"]),
        "LOG_FILE": str(raw["log_file"]),
        "MONITOR_LOG": str(raw["monitor_log"]),
        "TRACE_LINK": str(raw["trace_link"]),
        "ALERT_CONFIG_PATH": str(raw["alert_config_path"]),
        "ALERT_STATE_FILE": str(raw["alert_state_file"]),
        "ALERT_ENV_FILE": str(raw["alert_env_file"]),
        "ALERT_SSH_KEY": str(raw["alert_ssh_key"]),
        "ALERT_STATUS_COMMAND": str(raw["alert_status_command"]),
        "ALERT_THROTTLE_SECONDS": str(raw["alert_throttle_seconds"]),
        "DOWN_CONFIRMATION_SAMPLES": str(raw["down_confirmation_samples"]),
        "CLUSTER_STALL_SECONDS": str(raw["cluster_stall_seconds"]),
        "BRANCH": str(raw["branch"]),
        "REMOTE": str(raw["remote"]),
        "SERVICE_NAME": str(raw["service_name"]),
        "CONTROLLER_SERVICE_NAME": str(raw["controller_service_name"]),
        "CONTROLLER_CONFIG_PATH": str(raw["controller_config_path"]),
        "MODE_LABEL": str(raw["mode_label"]),
        "P2P_STACK": str(raw["p2p_stack"]),
        "PUBLIC_IP": str(raw.get("public_ip", "")),
        "HOSTNAME": str(raw["hostname"]),
        "ALIAS": str(raw.get("alias", raw["hostname"])),
        "SSH_STRING": str(raw["ssh_string"]),
        "METRICS_URL": str(raw["metrics_url"]),
        "READY_URL": str(raw["ready_url"]),
        "HEALTHY_URL": str(raw["healthy_url"]),
        "POLL_INTERVAL_SECONDS": str(raw["poll_interval_seconds"]),
        "STARTUP_TIMEOUT_SECONDS": str(raw["startup_timeout_seconds"]),
        "STALL_SECONDS": str(raw["stall_seconds"]),
        "MAX_RUN_SECONDS": str(raw["max_run_seconds"]),
        "READY_SAMPLES": str(raw["ready_samples"]),
        "READY_SAMPLE_INTERVAL_SECONDS": str(raw["ready_sample_interval_seconds"]),
        "MIN_FREE_BYTES": str(raw["min_free_bytes"]),
        "RETENTION_RUNS": str(raw["retention_runs"]),
        "COOLDOWN_SECONDS": str(raw["cooldown_seconds"]),
        "WIPE_ENTRIES": toml_string_list(raw["wipe_entries"]),
        "PRESERVE_ENTRIES": toml_string_list(raw["preserve_entries"]),
        "TRACING_FILTER": str(raw["tracing_filter"]).replace('"', '\\"'),
        "BOOTSTRAP_PEERS": "\n".join(f"    {json.dumps(peer)}," for peer in bootstrap_peers),
        "ALERT_NODES": render_alert_nodes(raw),
    }


def render_alert_nodes(raw: dict[str, Any]) -> str:
    # `raw` contains the merged defaults for the selected node. Reload the source
    # config so every host gets the full cluster inventory, not only itself.
    with (SCRIPT_DIR / "nodes.toml").open("rb") as config_file:
        data = tomllib.load(config_file)
    defaults = data.get("defaults", {})
    rendered = []
    for node in data.get("nodes", []):
        merged = dict(defaults)
        merged.update(node)
        rendered.append(
            "\n".join(
                [
                    "[[nodes]]",
                    f"name = {json.dumps(str(merged['name']))}",
                    f"hostname = {json.dumps(str(merged['hostname']))}",
                    f"ssh_string = {json.dumps(str(merged['ssh_string']))}",
                    f"alias = {json.dumps(str(merged.get('alias', merged['hostname'])))}",
                    f"public_ip = {json.dumps(str(merged.get('public_ip', 'unknown')))}",
                    f"mode_label = {json.dumps(str(merged['mode_label']))}",
                    f"p2p_stack = {json.dumps(str(merged['p2p_stack']))}",
                ]
            )
        )
    return "\n\n".join(rendered)


def render_files(node: Node) -> dict[str, str]:
    subst = subst_for(node)
    return {
        "controller.toml": render_template("controller.toml", subst),
        "alert-monitor.toml": render_template("alert-monitor.toml", subst),
        "zakurad.toml.template": render_template("zakurad.toml.template", subst),
        "zakura.service": render_template("zakura.service", subst),
        "zakura-continuous-sync.service": render_template("zakura-continuous-sync.service", subst),
        "zakura-monitor.service": render_template("zakura-monitor.service", subst),
        "zakura-monitor.timer": render_template("zakura-monitor.timer", subst),
        "logrotate": render_template("logrotate", subst),
        "tmpfiles.conf": render_template("tmpfiles.conf", subst),
    }


INSTALL_SCRIPT = r"""
set -euo pipefail

controller_config={controller_config}
alert_config={alert_config}
config_template={config_template}
config_path={config_path}
chain_state_dir={chain_state_dir}
wipe_sentinel={wipe_sentinel}
state_dir={state_dir}
runs_dir={runs_dir}
log_file={log_file}
monitor_log={monitor_log}
controller_service={controller_service}
node_service={node_service}
start_controller={start_controller}

install -d -m 755 /usr/local/sbin
install -d -m 755 "$(dirname "$controller_config")" "$(dirname "$alert_config")" \
  "$(dirname "$config_template")" "$(dirname "$config_path")" "$chain_state_dir" \
  "$state_dir" "$runs_dir" "$(dirname "$log_file")" "$(dirname "$monitor_log")" \
  /var/lib/zakura-monitor

install -m 755 /tmp/zakura-continuous-sync.py /usr/local/sbin/zakura-continuous-sync.py
install -m 755 /tmp/zakura-monitor.py /usr/local/sbin/zakura-monitor.py
install -m 755 /tmp/zakura-monitor-status.py /usr/local/sbin/zakura-monitor-status.py
install -m 755 /tmp/zakura-monitor-status.sh /usr/local/sbin/zakura-monitor-status.sh
install -m 644 /tmp/zakura-continuous-controller.toml "$controller_config"
install -m 644 /tmp/zakura-alert-monitor.toml "$alert_config"
install -m 644 /tmp/zakura-continuous-zakurad.toml.template "$config_template"
install -m 644 /tmp/zakura.service "/etc/systemd/system/${{node_service}}"
install -m 644 /tmp/zakura-continuous-controller.service "/etc/systemd/system/${{controller_service}}"
install -m 644 /tmp/zakura-monitor.service /etc/systemd/system/zakura-monitor.service
install -m 644 /tmp/zakura-monitor.timer /etc/systemd/system/zakura-monitor.timer
install -m 644 /tmp/zakura-continuous-logrotate /etc/logrotate.d/zakura-continuous-sync
install -m 644 /tmp/zakura-continuous-tmpfiles.conf /etc/tmpfiles.d/zakura-continuous-sync.conf

touch "$wipe_sentinel"
chmod 644 "$wipe_sentinel"
touch "$log_file" "$monitor_log"

rm -f /tmp/zakura-continuous-sync.py \
  /tmp/zakura-monitor.py \
  /tmp/zakura-monitor-status.py \
  /tmp/zakura-monitor-status.sh \
  /tmp/zakura-continuous-controller.toml \
  /tmp/zakura-alert-monitor.toml \
  /tmp/zakura-continuous-zakurad.toml.template \
  /tmp/zakura.service \
  /tmp/zakura-continuous-controller.service \
  /tmp/zakura-monitor.service \
  /tmp/zakura-monitor.timer \
  /tmp/zakura-continuous-logrotate \
  /tmp/zakura-continuous-tmpfiles.conf

systemd-tmpfiles --create /etc/tmpfiles.d/zakura-continuous-sync.conf || true
systemctl daemon-reload
systemctl enable "$node_service" >/dev/null
systemctl enable "$controller_service" >/dev/null
systemctl enable --now zakura-monitor.timer >/dev/null

if [ "$start_controller" = "1" ]; then
  systemctl restart "$controller_service"
fi

systemctl --no-pager --full status "$controller_service" || true
"""


def ssh_with_script(node: Node, script: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(node.ssh_cmd("bash", "-s"), text=True, input=script)


def deploy_node(node: Node, args: argparse.Namespace) -> tuple[str, bool, str]:
    rendered = render_files(node)
    tmp_dir = Path(os.environ.get("RUNNER_TEMP", "/tmp")) / f"zakura-continuous-{node.name}"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    staged = {
        "continuous-sync.py": SCRIPT_DIR / "continuous-sync.py",
        "alert-monitor.py": SCRIPT_DIR / "alert-monitor.py",
        "alert-status.py": SCRIPT_DIR / "alert-status.py",
        "monitor-status-wrapper.sh": SCRIPT_DIR / "monitor-status-wrapper.sh",
        "controller.toml": tmp_dir / "controller.toml",
        "alert-monitor.toml": tmp_dir / "alert-monitor.toml",
        "zakurad.toml.template": tmp_dir / "zakurad.toml.template",
        "zakura.service": tmp_dir / "zakura.service",
        "zakura-continuous-sync.service": tmp_dir / "zakura-continuous-sync.service",
        "zakura-monitor.service": tmp_dir / "zakura-monitor.service",
        "zakura-monitor.timer": tmp_dir / "zakura-monitor.timer",
        "logrotate": tmp_dir / "logrotate",
        "tmpfiles.conf": tmp_dir / "tmpfiles.conf",
    }
    for name, content in rendered.items():
        staged[name].write_text(content, encoding="utf-8")

    uploads = [
        (staged["continuous-sync.py"], "/tmp/zakura-continuous-sync.py"),
        (staged["alert-monitor.py"], "/tmp/zakura-monitor.py"),
        (staged["alert-status.py"], "/tmp/zakura-monitor-status.py"),
        (staged["monitor-status-wrapper.sh"], "/tmp/zakura-monitor-status.sh"),
        (staged["controller.toml"], "/tmp/zakura-continuous-controller.toml"),
        (staged["alert-monitor.toml"], "/tmp/zakura-alert-monitor.toml"),
        (staged["zakurad.toml.template"], "/tmp/zakura-continuous-zakurad.toml.template"),
        (staged["zakura.service"], "/tmp/zakura.service"),
        (staged["zakura-continuous-sync.service"], "/tmp/zakura-continuous-controller.service"),
        (staged["zakura-monitor.service"], "/tmp/zakura-monitor.service"),
        (staged["zakura-monitor.timer"], "/tmp/zakura-monitor.timer"),
        (staged["logrotate"], "/tmp/zakura-continuous-logrotate"),
        (staged["tmpfiles.conf"], "/tmp/zakura-continuous-tmpfiles.conf"),
    ]
    try:
        if args.dry_run:
            return (node.name, True, "rendered")
        for local, remote in uploads:
            run(node.scp_to(local, remote), capture=True)
        raw = node.raw
        script = INSTALL_SCRIPT.format(
            controller_config=shlex.quote(str(raw["controller_config_path"])),
            alert_config=shlex.quote(str(raw["alert_config_path"])),
            config_template=shlex.quote(str(raw["config_template_path"])),
            config_path=shlex.quote(str(raw["config_path"])),
            chain_state_dir=shlex.quote(str(raw["chain_state_dir"])),
            wipe_sentinel=shlex.quote(str(raw["wipe_sentinel"])),
            state_dir=shlex.quote(str(raw["state_dir"])),
            runs_dir=shlex.quote(str(raw["runs_dir"])),
            log_file=shlex.quote(str(raw["log_file"])),
            monitor_log=shlex.quote(str(raw["monitor_log"])),
            controller_service=shlex.quote(str(raw["controller_service_name"])),
            node_service=shlex.quote(str(raw["service_name"])),
            start_controller="0" if args.no_start else "1",
        )
        proc = ssh_with_script(node, script)
        if proc.returncode != 0:
            return (node.name, False, f"install failed rc={proc.returncode}")
        return (node.name, True, "deployed")
    except DeployError as error:
        return (node.name, False, str(error))


def remote_json(node: Node, command: str) -> tuple[bool, dict[str, Any] | str]:
    proc = subprocess.run(node.ssh_cmd(command), text=True, capture_output=True)
    if proc.returncode != 0:
        return False, (proc.stderr or proc.stdout or f"exit {proc.returncode}").strip()
    try:
        return True, json.loads(proc.stdout)
    except json.JSONDecodeError as error:
        return False, f"invalid JSON from {node.name}: {error}: {proc.stdout[:500]}"


def cmd_deploy(args: argparse.Namespace) -> int:
    nodes = load_nodes(args.config, args.node)
    return summarize_parallel(nodes, lambda node: deploy_node(node, args))


def cmd_status(args: argparse.Namespace) -> int:
    requested = set(args.node or [])
    if requested == {PR800_STATUS_TARGET}:
        nodes = [
            Node({"name": name, "ssh_string": ssh_string})
            for name, ssh_string in PR800_VALIDATION_NODES.items()
        ]

        def validation_status(node: Node) -> tuple[str, bool, str]:
            cmd = node.ssh_cmd("bash", "-s")
            actions_key = Path.home() / ".ssh" / "zakura-continuous-sync"
            if actions_key.exists():
                cmd[1:1] = ["-i", str(actions_key), "-o", "IdentitiesOnly=yes"]
            proc = subprocess.run(
                cmd,
                text=True,
                input=PR800_STATUS_SCRIPT,
                capture_output=True,
                timeout=240,
            )
            print(f"\n######## PR 800 status: {node.name} ########")
            print(proc.stdout)
            if proc.returncode != 0:
                return node.name, False, (proc.stderr or f"status exit {proc.returncode}").strip()
            return node.name, True, "PR 800 validation status fetched"

        return summarize_parallel(nodes, validation_status)

    if requested == {PR800_VALIDATION_TARGET}:
        nodes = [
            Node({"name": name, "ssh_string": ssh_string})
            for name, ssh_string in PR800_VALIDATION_NODES.items()
        ]

        def validate(node: Node) -> tuple[str, bool, str]:
            cmd = node.ssh_cmd("bash", "-s")
            actions_key = Path.home() / ".ssh" / "zakura-continuous-sync"
            if actions_key.exists():
                cmd[1:1] = ["-i", str(actions_key), "-o", "IdentitiesOnly=yes"]
            proc = subprocess.run(
                cmd,
                text=True,
                input=PR800_VALIDATION_SCRIPT,
                capture_output=True,
                timeout=1500,
            )
            print(f"\n######## PR 800 validation: {node.name} ########")
            print(proc.stdout)
            if proc.returncode != 0:
                return node.name, False, (proc.stderr or f"validation exit {proc.returncode}").strip()
            return node.name, True, "PR 800 validation service started on preserved state"

        return summarize_parallel(nodes, validate)

    diagnostic_targets = requested.intersection(STALL_DIAGNOSTIC_TARGETS)
    if diagnostic_targets:
        unknown = requested - diagnostic_targets
        if unknown:
            raise DeployError(
                "cannot mix diagnostic targets with continuous-sync nodes: "
                + ", ".join(sorted(unknown))
            )
        nodes = [
            Node({"name": name, "ssh_string": STALL_DIAGNOSTIC_TARGETS[name]})
            for name in sorted(diagnostic_targets)
        ]
    else:
        nodes = load_nodes(args.config, args.node)

    def work(node: Node) -> tuple[str, bool, str]:
        if node.name not in STALL_DIAGNOSTIC_TARGETS:
            ok, data = remote_json(node, "/usr/local/sbin/zakura-continuous-sync.py status")
            if not ok:
                return node.name, False, str(data)
            print(json.dumps(data, indent=2, sort_keys=True))

        if node.name in STALL_DIAGNOSTIC_START_TARGETS:
            start = subprocess.run(
                node.ssh_cmd("systemctl", "start", "zakura.service"),
                text=True,
                capture_output=True,
            )
            if start.returncode != 0:
                return node.name, False, (start.stderr or start.stdout).strip()

        cmd = node.ssh_cmd("bash", "-s")
        actions_key = Path.home() / ".ssh" / "zakura-continuous-sync"
        if actions_key.exists():
            cmd[1:1] = ["-i", str(actions_key), "-o", "IdentitiesOnly=yes"]
        proc = subprocess.run(
            cmd,
            text=True,
            input=STALL_DIAGNOSTIC_SCRIPT,
            capture_output=True,
            timeout=240,
        )
        print(f"\n######## diagnostics: {node.name} ########")
        print(proc.stdout)
        if proc.returncode != 0:
            return node.name, False, (proc.stderr or f"diagnostic exit {proc.returncode}").strip()
        return node.name, True, "status and diagnostics fetched"

    return summarize_parallel(nodes, work)


def cmd_resume(args: argparse.Namespace) -> int:
    nodes = load_nodes(args.config, args.node)

    def work(node: Node) -> tuple[str, bool, str]:
        proc = subprocess.run(
            node.ssh_cmd("/usr/local/sbin/zakura-continuous-sync.py resume"),
            text=True,
            capture_output=True,
        )
        if proc.returncode != 0:
            return node.name, False, (proc.stderr or proc.stdout).strip()
        return node.name, True, proc.stdout.strip() or "resumed"

    return summarize_parallel(nodes, work)


class Problem(NamedTuple):
    """One node's audit failure.

    `kind` is the throttle identity: it must stay byte-identical for as long as
    the same underlying failure persists, or every cycle looks like a brand-new
    problem and pages again. `detail` is the line posted to Slack and may embed
    volatile values -- free bytes, SSH stderr, an exception message -- that must
    therefore stay out of `kind`.
    """

    kind: str
    detail: str


def audit_problem(data: dict[str, Any], max_completion_age: int) -> Problem | None:
    state = data.get("controller_state") or {}
    sample = data.get("sample") or {}
    if state.get("failed"):
        # The halt reason is latched by the controller, so it is stable while the
        # halt lasts and a genuinely different halt should page again.
        failure = state.get("failure")
        return Problem(f"controller-halted:{failure}", f"controller halted: {failure}")
    if not data.get("service_active") and state.get("phase") == "syncing":
        return Problem(
            "service-inactive", "node service inactive while controller says syncing"
        )
    if sample.get("metrics_status") != "ok" and state.get("phase") == "syncing":
        # The status embeds the scrape exception, which varies between samples.
        return Problem(
            "metrics-unavailable", f"metrics unavailable: {sample.get('metrics_status')}"
        )
    if int(data.get("disk_free_bytes") or 0) < 10 * 1024 * 1024 * 1024:
        # Free bytes move on every sample, so they cannot be part of the identity.
        return Problem("low-disk", f"low disk: {data.get('disk_free_bytes')} bytes free")
    last_success = state.get("last_success_at")
    if last_success and max_completion_age > 0:
        try:
            parsed = int(time_from_stamp(str(last_success)))
            if now() - parsed > max_completion_age:
                return Problem(
                    "stale-success",
                    f"last successful run is older than {max_completion_age}s",
                )
        except ValueError:
            return Problem(
                "invalid-last-success", f"invalid last_success_at: {last_success}"
            )
    return None


def time_from_stamp(stamp: str) -> float:
    # The controller writes UTC stamps, so interpret them as UTC. `time.mktime`
    # would read the struct as local time and skew the age by the runner offset.
    return calendar.timegm(time.strptime(stamp, "%Y%m%dT%H%M%SZ"))


def now() -> int:
    return int(time.time())


def format_duration(seconds: int) -> str:
    seconds = max(0, int(seconds))
    hours, remainder = divmod(seconds, 3600)
    if hours >= 24:
        return f"{hours // 24}d{hours % 24}h"
    return f"{hours}h{remainder // 60}m"


def post_slack(text: str) -> bool:
    webhook = (
        os.environ.get("SLACK_WEB_HOOK", "")
        or os.environ.get("SLACK_WEBHOOK_URL", "")
        or os.environ.get("SLACK_WEBHOOK", "")
    )
    if not webhook:
        print(f"SLACK_WEB_HOOK missing; would post:\n{text}", file=sys.stderr)
        return False
    payload = json.dumps({"text": text}).encode("utf-8")
    request = urllib.request.Request(
        webhook,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            body = response.read().decode("utf-8", errors="replace").strip()
    except (OSError, urllib.error.URLError) as error:
        print(f"Slack post failed: {error}", file=sys.stderr)
        return False
    return 200 <= response.status < 300 and body == "ok"


# Bump whenever a stored record's shape changes, so incompatible state is
# discarded wholesale instead of being half-read. v2 replaced the single
# `problem` string with the `kind`/`detail` pair.
AUDIT_STATE_VERSION = 2


def load_audit_state(path: Path | None) -> dict[str, Any]:
    fresh: dict[str, Any] = {"version": AUDIT_STATE_VERSION, "problems": {}}
    if path is None or not path.exists():
        return fresh
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        print(f"audit state unreadable ({error}); starting fresh", file=sys.stderr)
        return fresh
    if not isinstance(data, dict) or data.get("version") != AUDIT_STATE_VERSION:
        return fresh
    problems = data.get("problems")
    if not isinstance(problems, dict):
        return fresh
    return {"version": AUDIT_STATE_VERSION, "problems": problems}


def save_audit_state(path: Path | None, state: dict[str, Any]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True))
    tmp.replace(path)


def audit_transitions(
    problems: dict[str, Problem],
    previous: dict[str, Any],
    reminder_interval: int,
    timestamp: int,
) -> tuple[list[str], list[str], list[str], dict[str, Any]]:
    """Split current problems into new/reminder/recovered lines.

    A problem alerts immediately the first time it is seen, and again only once
    `reminder_interval` has elapsed, so a node that stays broken reminds on a slow
    cadence instead of re-paging every audit cycle. Continuity is judged on
    `Problem.kind`, never on the rendered detail, which changes between samples.
    """
    prior = previous.get("problems", {})
    new_lines: list[str] = []
    reminder_lines: list[str] = []
    current: dict[str, Any] = {}

    for name in sorted(problems):
        problem = problems[name]
        record = prior.get(name)
        if not isinstance(record, dict) or record.get("kind") != problem.kind:
            # First sighting, or the failure changed to a different one.
            new_lines.append(f"{name}: {problem.detail}")
            current[name] = {
                "kind": problem.kind,
                "detail": problem.detail,
                "first_seen": timestamp,
                "last_sent": timestamp,
            }
            continue
        first_seen = int(record.get("first_seen", timestamp))
        last_sent = int(record.get("last_sent", timestamp))
        if timestamp - last_sent >= reminder_interval:
            reminder_lines.append(
                f"{name}: {problem.detail} "
                f"(unresolved for {format_duration(timestamp - first_seen)})"
            )
            last_sent = timestamp
        current[name] = {
            "kind": problem.kind,
            "detail": problem.detail,
            "first_seen": first_seen,
            "last_sent": last_sent,
        }

    recovered_lines = [
        f"{name}: was {prior[name].get('detail')}"
        for name in sorted(prior)
        if name not in problems and isinstance(prior.get(name), dict)
    ]
    return new_lines, reminder_lines, recovered_lines, {
        "version": AUDIT_STATE_VERSION,
        "problems": current,
    }


def audit_message(
    new_lines: list[str], reminder_lines: list[str], recovered_lines: list[str]
) -> str:
    sections = []
    if new_lines:
        sections.append(":rotating_light: Zakura continuous sync audit failed\n" + "\n".join(new_lines))
    if reminder_lines:
        sections.append(":alarm_clock: Zakura continuous sync still failing\n" + "\n".join(reminder_lines))
    if recovered_lines:
        sections.append(":white_check_mark: Zakura continuous sync recovered\n" + "\n".join(recovered_lines))
    return "\n\n".join(sections)


def cmd_audit(args: argparse.Namespace) -> int:
    nodes = load_nodes(args.config, args.node)
    problems: dict[str, Problem] = {}
    for node in nodes:
        ok, data = remote_json(node, "/usr/local/sbin/zakura-continuous-sync.py status")
        if not ok:
            # `data` is raw ssh stderr, which differs between attempts at the same
            # outage, so only the category identifies the problem.
            problems[node.name] = Problem(
                "unreachable", f"unreachable or invalid status: {data}"
            )
            continue
        assert isinstance(data, dict)
        problem = audit_problem(data, args.max_completion_age)
        if problem:
            problems[node.name] = problem

    state_file = Path(args.state_file) if args.state_file else None
    previous = load_audit_state(state_file)
    timestamp = now()
    new_lines, reminder_lines, recovered_lines, state = audit_transitions(
        problems, previous, args.reminder_interval, timestamp
    )
    text = audit_message(new_lines, reminder_lines, recovered_lines)

    posted = True
    if text:
        if not args.dry_run:
            posted = post_slack(text)
        print(text)
    elif problems:
        print(
            f"audit failing on {len(problems)} node(s); alert throttled "
            f"(reminder every {format_duration(args.reminder_interval)})"
        )
    else:
        print(f"audit ok: {len(nodes)} node(s)")

    if args.dry_run:
        pass
    elif posted:
        save_audit_state(state_file, state)
    else:
        # Advancing `last_sent` here would record an undelivered page as sent and
        # stay silent until the reminder interval elapsed. Leave the old state so
        # the next audit re-derives these same lines and retries.
        print(
            "slack post failed; leaving alert state unchanged so the next audit retries",
            file=sys.stderr,
        )

    return 1 if problems else 0


def summarize_parallel(nodes: list[Node], fn) -> int:
    results: list[tuple[str, bool, str]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(nodes))) as pool:
        for result in pool.map(fn, nodes):
            results.append(result)
    failed = 0
    for name, ok, message in results:
        if not ok:
            failed += 1
        status = "OK  " if ok else "FAIL"
        print(f"[{status}] {name}: {message}")
    return 1 if failed else 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=SCRIPT_DIR / "nodes.toml",
        help="fleet inventory TOML",
    )
    parser.add_argument("--node", action="append", help="limit to a single node name")
    sub = parser.add_subparsers(dest="command", required=True)
    deploy = sub.add_parser("deploy", help="install controller/config/systemd files")
    deploy.add_argument("--no-start", action="store_true", help="install but do not start controller")
    deploy.add_argument("--dry-run", action="store_true", help="render local files only")
    sub.add_parser("status", help="fetch controller status JSON")
    sub.add_parser("resume", help="clear durable failure marker and restart controller")
    audit = sub.add_parser("audit", help="scheduled external audit for CI")
    audit.add_argument("--dry-run", action="store_true")
    audit.add_argument(
        "--max-completion-age",
        type=int,
        default=0,
        help="alert if last successful cycle is older than this many seconds; 0 disables",
    )
    audit.add_argument(
        "--state-file",
        type=Path,
        default=None,
        help="persist alert state here so an unchanged failure is not re-sent every cycle",
    )
    audit.add_argument(
        "--reminder-interval",
        type=int,
        default=21600,
        help="re-send an unresolved failure at most this often, in seconds (default 6h)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.command == "deploy":
            return cmd_deploy(args)
        if args.command == "status":
            return cmd_status(args)
        if args.command == "resume":
            return cmd_resume(args)
        if args.command == "audit":
            return cmd_audit(args)
    except DeployError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    raise AssertionError(args.command)


if __name__ == "__main__":
    raise SystemExit(main())
