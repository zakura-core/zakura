#!/usr/bin/env python3
"""Deploy and audit the permanent Zakura continuous genesis sync fleet."""

from __future__ import annotations

import argparse
import calendar
import concurrent.futures
import hashlib
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
        for required in (
            "name",
            "ssh_string",
            "hostname",
            "mode_label",
            "p2p_stack",
            "public_ip",
        ):
            if required not in merged:
                raise DeployError(f"node missing required field {required!r}: {raw_node}")
        if not str(merged["public_ip"]).strip():
            raise DeployError(
                f"node {merged['name']!r} has empty required field 'public_ip'"
            )
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
        "HEALTH_MIN_CONNECTED_PEERS": str(raw["health_min_connected_peers"]),
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
    nodes = load_nodes(args.config, args.node)

    def work(node: Node) -> tuple[str, bool, str]:
        ok, data = remote_json(node, "/usr/local/sbin/zakura-continuous-sync.py status")
        if not ok:
            return node.name, False, str(data)
        print(json.dumps(data, indent=2, sort_keys=True))
        return node.name, True, "status fetched"

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

    `kind` is the failure category: it must stay byte-identical for as long as
    the same underlying failure persists, or every cycle looks like a brand-new
    problem and pages again. `detail` is the line posted to Slack and may embed
    volatile values -- free bytes, SSH stderr, an exception message -- that must
    therefore stay out of `kind`. `incident_id` distinguishes controller runs;
    `delivered_at` is set only for a verified controller delivery receipt.
    """

    kind: str
    detail: str
    incident_id: str = ""
    delivered_at: int | None = None


def audit_problem(
    data: dict[str, Any], max_completion_age: int, destination: str | None = None
) -> Problem | None:
    """Classify a node, accepting only a matching delivery receipt to this destination."""
    state = data.get("controller_state") or {}
    sample = data.get("sample") or {}
    if state.get("failed"):
        # The halt reason is latched by the controller, so it is stable while the
        # halt lasts and a genuinely different halt should page again.
        failure = state.get("failure")
        run_id, failed_at = state.get("last_failed_run"), state.get("failed_at")
        incident_id = json.dumps([run_id, failed_at]) if run_id and failed_at else ""
        receipt = state.get("failure_notification")
        delivered_at = None
        if (
            incident_id
            and destination
            and isinstance(receipt, dict)
            and receipt.get("run_id") == run_id
            and receipt.get("failed_at") == failed_at
            and receipt.get("reason") == failure
            and receipt.get("destination") == destination
            and type(receipt.get("sent_at")) is int
            and 0 < receipt["sent_at"] <= now()
        ):
            delivered_at = receipt["sent_at"]
        detail = f"controller halted: {failure}"
        if run_id:
            detail += f" (run {run_id})"
        return Problem(
            f"controller-halted:{failure}", detail,
            incident_id, delivered_at,
        )
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


def slack_webhook_url() -> str:
    return (
        os.environ.get("SLACK_WEB_HOOK", "")
        or os.environ.get("SLACK_WEBHOOK_URL", "")
        or os.environ.get("SLACK_WEBHOOK", "")
    )


def post_slack(text: str) -> bool:
    webhook = slack_webhook_url()
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
    if type(data.get("last_digest_at")) is not int or not 0 <= data["last_digest_at"] <= now():
        data.pop("last_digest_at", None)
    completions = data.get("completions", {})
    if not isinstance(completions, dict) or any(
        not isinstance(record, dict)
        or any(type(record.get(key)) is not int or record[key] < 0 for key in ("total", "pending"))
        or any(key not in record for key in ("run_id", "sha", "duration"))
        for record in completions.values()
    ):
        data.pop("completions", None)
    return {**data, "version": AUDIT_STATE_VERSION, "problems": problems}


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
    *,
    reminders_due: bool = True,
) -> tuple[list[str], list[str], list[str], dict[str, Any]]:
    """Split current problems into new/reminder/recovered lines.

    New problems alert unless a matching controller receipt already accounts for
    that first delivery. Changed categories or runs still alert. Unchanged
    problems remind only when due; volatile message detail does not define the
    incident. The caller persists the returned state only after successful delivery.
    """
    prior = previous.get("problems", {})
    new_lines: list[str] = []
    reminder_lines: list[str] = []
    current: dict[str, Any] = {}

    for name in sorted(problems):
        problem = problems[name]
        record = prior.get(name)
        had_prior_problem = isinstance(record, dict)
        if (
            not isinstance(record, dict)
            or record.get("kind") != problem.kind
            or record.get("incident_id", "") != problem.incident_id
        ):
            # First sighting, or the failure changed to a different one.
            record = {
                "kind": problem.kind,
                "detail": problem.detail,
                "incident_id": problem.incident_id,
                "first_seen": problem.delivered_at or timestamp,
                "last_sent": problem.delivered_at or timestamp,
            }
            if problem.delivered_at is None or had_prior_problem:
                record["first_seen"] = timestamp
                record["last_sent"] = timestamp
                new_lines.append(f"{name}: {problem.detail}")
                current[name] = record
                continue
        first_seen = int(record.get("first_seen", timestamp))
        last_sent = int(record.get("last_sent", timestamp))
        if reminders_due and timestamp - last_sent >= reminder_interval:
            reminder_lines.append(
                f"{name}: {problem.detail} "
                f"(unresolved for {format_duration(timestamp - first_seen)})"
            )
            last_sent = timestamp
        current[name] = {
            "kind": problem.kind,
            "detail": problem.detail,
            "incident_id": problem.incident_id,
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
        sections.append(":memo: Zakura continuous sync digest — unresolved\n" + "\n".join(reminder_lines))
    if recovered_lines:
        sections.append(":white_check_mark: Zakura continuous sync recovered\n" + "\n".join(recovered_lines))
    return "\n\n".join(sections)


def completion_updates(
    statuses: dict[str, dict[str, Any]], previous: dict[str, Any], digest_due: bool
) -> tuple[list[str], dict[str, Any]]:
    """Accumulate completed runs until a digest is successfully delivered.

    A new cache counts from when the controller enabled digests. Missing hosts
    retain pending counts; a counter reset counts the new success once.
    """
    records = dict(previous.get("completions", {}))
    for name, data in statuses.items():
        controller = data.get("controller_state") or {}
        run_id = controller.get("last_success_run")
        total = controller.get("runs")
        if not controller.get("completion_digest") or not run_id or type(total) is not int:
            continue
        old = records.get(name, {})
        if old.get("run_id") == run_id:
            continue
        baseline = old.get("total", controller.get("completion_digest_start_runs", total - 1))
        delta = max(1, total - baseline) if type(baseline) is int else 1
        records[name] = {
            "run_id": run_id, "total": total,
            "pending": old.get("pending", 0) + delta,
            "sha": controller.get("last_success_sha", "unknown"),
            "duration": controller.get("last_success_duration_seconds"),
        }
    lines = []
    if digest_due:
        for name, record in sorted(records.items()):
            if record.get("pending", 0):
                lines.append(
                    f"{name}: {record['pending']} completed run(s); "
                    f"latest={record['run_id']} | sha={record['sha']} | "
                    f"sync time={record['duration']}s"
                )
                records[name] = {**record, "pending": 0}
    return lines, records


def cmd_audit(args: argparse.Namespace) -> int:
    nodes = load_nodes(args.config, args.node)
    problems: dict[str, Problem] = {}
    statuses: dict[str, dict[str, Any]] = {}
    webhook = slack_webhook_url()
    destination = hashlib.sha256(webhook.encode()).hexdigest() if webhook else None
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
        statuses[node.name] = data
        problem = audit_problem(data, args.max_completion_age, destination)
        if problem:
            problems[node.name] = problem

    state_file = Path(args.state_file) if args.state_file else None
    previous = load_audit_state(state_file)
    timestamp = now()
    last_digest = previous.get("last_digest_at", timestamp)
    digest_due = not args.node and timestamp - last_digest >= args.reminder_interval
    # A targeted audit cannot recover nodes it did not inspect.
    selected = {node.name for node in nodes}
    prior_problems = previous.get("problems", {})
    scoped_previous = {"problems": {k: v for k, v in prior_problems.items() if k in selected}}
    new_lines, reminder_lines, recovered_lines, state = audit_transitions(
        problems, scoped_previous, 0 if digest_due else args.reminder_interval, timestamp,
        reminders_due=digest_due,
    )
    state["problems"].update({k: v for k, v in prior_problems.items() if k not in selected})
    completion_lines, state["completions"] = completion_updates(statuses, previous, digest_due)
    state["last_digest_at"] = timestamp if digest_due else last_digest
    text = audit_message(new_lines, reminder_lines, recovered_lines)
    if completion_lines:
        text += ("\n\n" if text else "") + (
            ":memo: Zakura continuous sync digest — completions\n" + "\n".join(completion_lines)
        )

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
        default=86400,
        help="digest interval for unresolved failures and completions (default 24h)",
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
