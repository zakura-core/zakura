#!/usr/bin/env python3
"""Host-local controller for the permanent Zakura genesis sync fleet.

The controller repeatedly builds the configured ref, starts `zakurad` from an
empty allowlisted state directory, waits until the node is stably ready at tip,
posts a Slack completion, and starts over. Any failure writes a durable halt
marker and exits non-zero; operators must explicitly run `resume`.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
import tomllib
import urllib.error
import urllib.request
import calendar
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

STATE_VERSION = 1


class ControllerError(Exception):
    """Operator-facing failure that should halt the sync loop."""


@dataclass(frozen=True)
class Paths:
    repo_dir: Path
    state_dir: Path
    runs_dir: Path
    chain_state_dir: Path
    wipe_sentinel: Path
    build_cache_dir: Path
    config_template: Path
    zakurad_config: Path
    bin_path: Path
    log_file: Path
    monitor_log: Path
    trace_link: Path


@dataclass(frozen=True)
class Policy:
    branch: str = "main"
    remote: str = "origin"
    service_name: str = "zakura.service"
    mode_label: str = "unknown"
    p2p_stack: str = "dual"
    public_ip: str = ""
    hostname: str = ""
    alias: str = ""
    ssh_string: str = ""
    metrics_url: str = "http://127.0.0.1:9999/metrics"
    ready_url: str = "http://127.0.0.1:8080/ready"
    healthy_url: str = "http://127.0.0.1:8080/healthy"
    poll_interval_seconds: int = 30
    startup_timeout_seconds: int = 600
    stall_seconds: int = 600
    max_run_seconds: int = 172800
    ready_samples: int = 6
    ready_sample_interval_seconds: int = 30
    min_free_bytes: int = 10 * 1024 * 1024 * 1024
    retention_days: int = 5
    cooldown_seconds: int = 60
    wipe_entries: tuple[str, ...] = ("state", "non_finalized_state")
    preserve_entries: tuple[str, ...] = ("network",)
    bootstrap_peers: tuple[str, ...] = ()
    tracing_filter: str = "info"


@dataclass
class Config:
    paths: Paths
    policy: Policy = field(default_factory=Policy)


def now() -> int:
    return int(time.time())


def utc_stamp(ts: int | None = None) -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(ts or now()))


def load_config(path: Path) -> Config:
    with path.open("rb") as config_file:
        raw = tomllib.load(config_file)

    paths_raw = raw.get("paths", {})
    required_paths = {
        "repo_dir",
        "state_dir",
        "runs_dir",
        "chain_state_dir",
        "wipe_sentinel",
        "build_cache_dir",
        "config_template",
        "zakurad_config",
        "bin_path",
        "log_file",
        "monitor_log",
        "trace_link",
    }
    missing = sorted(required_paths - set(paths_raw))
    if missing:
        raise ControllerError(f"missing [paths] fields: {', '.join(missing)}")

    paths = Paths(**{key: Path(paths_raw[key]) for key in required_paths})
    policy_raw = raw.get("policy", {})
    bootstrap_peers = tuple(policy_raw.pop("bootstrap_peers", []))
    wipe_entries = tuple(policy_raw.pop("wipe_entries", Policy.wipe_entries))
    preserve_entries = tuple(policy_raw.pop("preserve_entries", Policy.preserve_entries))
    policy = Policy(
        **policy_raw,
        bootstrap_peers=bootstrap_peers,
        wipe_entries=wipe_entries,
        preserve_entries=preserve_entries,
    )
    return Config(paths=paths, policy=policy)


def run(
    cmd: list[str],
    *,
    cwd: Path | None = None,
    capture: bool = False,
    check: bool = True,
    timeout: int | None = None,
) -> subprocess.CompletedProcess[str]:
    printable = " ".join(cmd)
    try:
        return subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=capture,
            check=check,
            text=True,
            timeout=timeout,
        )
    except subprocess.CalledProcessError as error:
        detail = ""
        if capture:
            detail = (error.stderr or error.stdout or "").strip()
        raise ControllerError(f"command failed ({error.returncode}): {printable}\n{detail}") from error
    except subprocess.TimeoutExpired as error:
        raise ControllerError(f"command timed out after {timeout}s: {printable}") from error


def load_state(path: Path) -> dict[str, Any]:
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"version": STATE_VERSION, "runs": 0}
    except json.JSONDecodeError as error:
        raise ControllerError(f"invalid state file {path}: {error}") from error
    if state.get("version") != STATE_VERSION:
        return {"version": STATE_VERSION, "runs": 0}
    return state


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    state["version"] = STATE_VERSION
    tmp = path.with_suffix(f"{path.suffix}.tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def write_run_json(run_dir: Path, data: dict[str, Any]) -> None:
    save_state(run_dir / "run.json", data)


def log(config: Config, message: str) -> None:
    config.paths.monitor_log.parent.mkdir(parents=True, exist_ok=True)
    with config.paths.monitor_log.open("a", encoding="utf-8") as log_file:
        log_file.write(time.strftime("%Y-%m-%dT%H:%M:%S%z ") + message + "\n")


def slack_webhook_url() -> str:
    return (
        os.environ.get("SLACK_WEB_HOOK", "")
        or os.environ.get("SLACK_WEBHOOK_URL", "")
        or os.environ.get("SLACK_WEBHOOK", "")
    )


def post_slack(config: Config, text: str) -> bool:
    webhook = slack_webhook_url()
    if not webhook:
        log(config, "slack-webhook-missing")
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
        log(config, f"slack-post-failed error={error}")
        return False
    if response.status < 200 or response.status >= 300 or body != "ok":
        log(config, f"slack-post-failed status={response.status} body={body}")
        return False
    return True


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def resolve_sha(config: Config) -> str:
    policy = config.policy
    run(["git", "fetch", "--prune", policy.remote, policy.branch], cwd=config.paths.repo_dir)
    result = run(
        ["git", "rev-parse", "--verify", f"{policy.remote}/{policy.branch}^{{commit}}"],
        cwd=config.paths.repo_dir,
        capture=True,
    )
    return result.stdout.strip()


def cached_binary(config: Config, sha: str) -> Path:
    return config.paths.build_cache_dir / f"zakurad-{sha}"


def binary_runnable(path: Path) -> bool:
    if not path.is_file():
        return False
    result = subprocess.run([str(path), "--version"], text=True, capture_output=True)
    return result.returncode == 0


def build_binary(config: Config, sha: str) -> Path:
    config.paths.build_cache_dir.mkdir(parents=True, exist_ok=True)
    target = cached_binary(config, sha)
    meta = target.with_suffix(".json")
    if binary_runnable(target) and meta.exists():
        log(config, f"build-cache-hit sha={sha}")
        return target

    worktree = config.paths.build_cache_dir / f"worktree-{sha[:12]}"
    if worktree.exists():
        run(["git", "worktree", "remove", "--force", str(worktree)], cwd=config.paths.repo_dir, check=False)
        shutil.rmtree(worktree, ignore_errors=True)
    run(["git", "worktree", "add", "--detach", str(worktree), sha], cwd=config.paths.repo_dir)
    try:
        run(["cargo", "build", "--release", "--locked", "-p", "zakura"], cwd=worktree)
        built = worktree / "target" / "release" / "zakurad"
        if not built.is_file():
            raise ControllerError(f"build completed but binary is missing: {built}")
        tmp = target.with_suffix(".tmp")
        shutil.copy2(built, tmp)
        os.chmod(tmp, 0o755)
        tmp.replace(target)
        meta_tmp = meta.with_suffix(".tmp")
        meta_tmp.write_text(
            json.dumps(
                {
                    "sha": sha,
                    "binary_sha256": sha256_file(target),
                    "built_at": utc_stamp(),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        meta_tmp.replace(meta)
    finally:
        run(["git", "worktree", "remove", "--force", str(worktree)], cwd=config.paths.repo_dir, check=False)
        shutil.rmtree(worktree, ignore_errors=True)
    return target


def install_binary(config: Config, binary: Path) -> None:
    target = config.paths.bin_path
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        shutil.copy2(target, target.with_suffix(target.suffix + ".bak"))
    tmp = target.with_suffix(target.suffix + ".new")
    shutil.copy2(binary, tmp)
    os.chmod(tmp, 0o755)
    tmp.replace(target)


def check_free_space(config: Config) -> None:
    usage = shutil.disk_usage(config.paths.chain_state_dir)
    if usage.free < config.policy.min_free_bytes:
        raise ControllerError(
            f"free disk {usage.free} bytes below minimum {config.policy.min_free_bytes}"
        )


def preflight(config: Config) -> None:
    for command in ("cargo", "git", "systemctl"):
        if shutil.which(command) is None:
            raise ControllerError(f"required command is unavailable: {command}")
    for path, description in (
        (config.paths.repo_dir, "repository"),
        (config.paths.config_template, "config template"),
        (config.paths.wipe_sentinel, "wipe sentinel"),
    ):
        if not path.exists():
            raise ControllerError(f"{description} is missing: {path}")
    check_free_space(config)


def safe_wipe_state(config: Config) -> None:
    root = config.paths.chain_state_dir.resolve()
    testing = os.environ.get("ZAKURA_CONTINUOUS_SYNC_TESTING") == "1"
    allowed_root = Path("/var/lib/zakura")
    if not testing and root != allowed_root:
        raise ControllerError(f"refusing to wipe unexpected state root: {root}")
    if not config.paths.wipe_sentinel.exists():
        raise ControllerError(f"wipe sentinel is missing: {config.paths.wipe_sentinel}")
    root.mkdir(parents=True, exist_ok=True)

    preserve = set(config.policy.preserve_entries)
    for entry_name in config.policy.wipe_entries:
        if entry_name in preserve or "/" in entry_name or entry_name in ("", ".", ".."):
            raise ControllerError(f"unsafe wipe entry: {entry_name!r}")
        entry = (root / entry_name).resolve()
        if entry.parent != root:
            raise ControllerError(f"wipe entry escaped state root: {entry}")
        if entry.is_dir():
            shutil.rmtree(entry)
        elif entry.exists():
            entry.unlink()


def render_config(config: Config, run_dir: Path) -> None:
    template = config.paths.config_template.read_text(encoding="utf-8")
    trace_dir = run_dir / "traces"
    trace_dir.mkdir(parents=True, exist_ok=True)
    substitutions = {
        "TRACE_DIR": str(trace_dir),
        "LOG_FILE": str(config.paths.log_file),
        "STATE_CACHE_DIR": str(config.paths.chain_state_dir),
        "P2P_STACK": config.policy.p2p_stack,
        "TRACING_FILTER": config.policy.tracing_filter,
        "BOOTSTRAP_PEERS": "\n".join(f'    "{peer}",' for peer in config.policy.bootstrap_peers),
    }
    rendered = template
    for key, value in substitutions.items():
        rendered = rendered.replace("{{" + key + "}}", value)
    tmp = config.paths.zakurad_config.with_suffix(".tmp")
    tmp.write_text(rendered, encoding="utf-8")
    tmp.replace(config.paths.zakurad_config)
    relink(config.paths.trace_link, trace_dir)


def relink(link: Path, target: Path) -> None:
    link.parent.mkdir(parents=True, exist_ok=True)
    tmp = link.with_name(f".{link.name}.tmp")
    if tmp.is_symlink() or tmp.is_file():
        tmp.unlink()
    elif tmp.is_dir():
        backup = tmp.with_name(f"{tmp.name}.migrated-{utc_stamp()}-{time.time_ns()}")
        tmp.rename(backup)
    tmp.symlink_to(target)
    if link.is_symlink():
        pass
    elif link.is_dir():
        try:
            link.rmdir()
        except OSError:
            backup = link.with_name(f"{link.name}.migrated-{utc_stamp()}-{time.time_ns()}")
            link.rename(backup)
    elif link.exists():
        link.unlink()
    tmp.replace(link)


def start_service(config: Config) -> None:
    run(["systemctl", "reset-failed", config.policy.service_name], check=False)
    run(["systemctl", "stop", config.policy.service_name], check=False)
    run(["systemctl", "start", config.policy.service_name])


def stop_service(config: Config) -> None:
    run(["systemctl", "stop", config.policy.service_name], check=False)


def service_active(config: Config) -> bool:
    return subprocess.run(
        ["systemctl", "is-active", "--quiet", config.policy.service_name]
    ).returncode == 0


def fetch_text(url: str, timeout: int = 10) -> str:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace")


def fetch_ready(config: Config) -> tuple[bool, str]:
    try:
        body = fetch_text(config.policy.ready_url, timeout=10).strip()
        return True, body or "ready"
    except urllib.error.HTTPError as error:
        body = error.read().decode("utf-8", errors="replace").strip()
        return False, body or f"HTTP {error.code}"
    except Exception as error:
        return False, f"{type(error).__name__}: {error}"


def metric_value(metrics: str, name: str) -> float | None:
    prometheus_name = re.escape(name.replace(".", "_"))
    dotted_name = re.escape(name)
    pattern = re.compile(
        rf"^(?:{dotted_name}|{prometheus_name})\s+(-?\d+(?:\.\d+)?)$",
        re.MULTILINE,
    )
    match = pattern.search(metrics)
    return float(match.group(1)) if match else None


def sample_status(config: Config) -> dict[str, Any]:
    status: dict[str, Any] = {"service_active": service_active(config)}
    try:
        metrics = fetch_text(config.policy.metrics_url)
        status["metrics_status"] = "ok"
        for key in (
            "state.memory.best.committed.block.height",
            "state.memory.committed.block.height",
            "state_finalized_block_height",
            "state_checkpoint_finalized_block_height",
            "zcash_chain_verified_block_height",
            "sync_block_verified_tip_height",
            "checkpoint_verified_height",
            "checkpoint_processing_next_height",
            "sync.estimated_network_tip_height",
            "sync.estimated_distance_to_tip",
        ):
            value = metric_value(metrics, key)
            if value is not None:
                status[key] = int(value)
        status["height"] = None
        for key in (
            "state.memory.best.committed.block.height",
            "state.memory.committed.block.height",
            "state_finalized_block_height",
            "state_checkpoint_finalized_block_height",
            "zcash_chain_verified_block_height",
            "sync_block_verified_tip_height",
            "checkpoint_verified_height",
            "checkpoint_processing_next_height",
        ):
            if status.get(key) is not None:
                status["height"] = status[key]
                status["height_source"] = key
                break
        if status["height"] is None:
            tip = status.get("sync.estimated_network_tip_height")
            distance = status.get("sync.estimated_distance_to_tip")
            if isinstance(tip, int) and isinstance(distance, int) and 0 <= distance <= tip:
                status["height"] = tip - distance
                status["height_source"] = "estimated_tip_minus_distance"
    except Exception as error:
        status["metrics_status"] = f"{type(error).__name__}: {error}"
        status["height"] = None
    ready, ready_detail = fetch_ready(config)
    status["ready"] = ready
    status["ready_detail"] = ready_detail
    return status


def wait_for_completion(config: Config, run_dir: Path, run_state: dict[str, Any]) -> None:
    started = now()
    last_height: int | None = None
    last_progress = started
    ready_samples = 0
    samples_path = run_dir / "samples.jsonl"

    while True:
        ts = now()
        if ts - started > config.policy.max_run_seconds:
            raise ControllerError(f"run exceeded max duration {config.policy.max_run_seconds}s")
        if not service_active(config):
            raise ControllerError(f"{config.policy.service_name} exited before sync completion")
        check_free_space(config)

        sample = sample_status(config)
        sample["time"] = utc_stamp(ts)
        with samples_path.open("a", encoding="utf-8") as samples:
            samples.write(json.dumps(sample, sort_keys=True) + "\n")

        height = sample.get("height")
        if isinstance(height, int) and height != last_height:
            last_height = height
            last_progress = ts
            run_state["height"] = height
            run_state["last_progress_at"] = utc_stamp(ts)
            write_run_json(run_dir, run_state)

        if last_height is None and ts - started >= config.policy.startup_timeout_seconds:
            raise ControllerError(
                f"no height observed within startup timeout "
                f"{config.policy.startup_timeout_seconds}s; metrics={sample.get('metrics_status')}, "
                f"ready={sample.get('ready_detail')}"
            )

        if ts - last_progress >= config.policy.stall_seconds:
            raise ControllerError(
                f"height {last_height} has not progressed for {ts - last_progress}s "
                f"(threshold {config.policy.stall_seconds}s)"
            )

        if sample.get("ready") is True:
            ready_samples += 1
            if ready_samples >= config.policy.ready_samples:
                return
            time.sleep(config.policy.ready_sample_interval_seconds)
        else:
            ready_samples = 0
            time.sleep(config.policy.poll_interval_seconds)


def compress_previous_log(config: Config, run_dir: Path) -> None:
    if not config.paths.log_file.exists():
        return
    dest = run_dir / "zebrad.log"
    shutil.copy2(config.paths.log_file, dest)
    config.paths.log_file.write_text("", encoding="utf-8")


def cleanup_retention(config: Config) -> None:
    cutoff = now() - config.policy.retention_days * 86400
    for child in config.paths.runs_dir.iterdir() if config.paths.runs_dir.exists() else []:
        if not child.is_dir():
            continue
        run_json = child / "run.json"
        if not run_json.exists():
            continue
        try:
            data = json.loads(run_json.read_text(encoding="utf-8"))
        except Exception:
            continue
        completed_at = data.get("completed_at") or data.get("failed_at")
        if not completed_at:
            continue
        try:
            parsed = int(calendar.timegm(time.strptime(completed_at, "%Y%m%dT%H%M%SZ")))
        except ValueError:
            continue
        if parsed < cutoff:
            shutil.rmtree(child, ignore_errors=True)


def completion_text(config: Config, run_state: dict[str, Any]) -> str:
    p = config.policy
    return f":white_check_mark: Zakura sync complete: {p.hostname} | {policy_mode(p)} | {ssh_target(p)}"


def failure_text(config: Config, run_state: dict[str, Any], reason: str) -> str:
    p = config.policy
    return f":rotating_light: Zakura failed: {p.hostname} | {policy_mode(p)} | {ssh_target(p)}"


def policy_mode(policy: Policy) -> str:
    if policy.p2p_stack == "zakura":
        return "v2p2p"
    if policy.p2p_stack in ("zebra", "legacy"):
        return "legacy"
    return "dual" if policy.p2p_stack == "dual" else policy.p2p_stack


def ssh_target(policy: Policy) -> str:
    if policy.public_ip:
        return f"root@{policy.public_ip}"
    return policy.ssh_string.removeprefix("ssh ").strip()


def short_reason(reason: str, limit: int = 96) -> str:
    reason = " ".join((reason or "unknown").split())
    return reason if len(reason) <= limit else reason[: limit - 3] + "..."


def one_cycle(config: Config, state_path: Path, state: dict[str, Any]) -> dict[str, Any]:
    preflight(config)
    sha = resolve_sha(config)
    run_id = f"{utc_stamp()}-{sha[:12]}"
    run_dir = config.paths.runs_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    run_state: dict[str, Any] = {
        "version": STATE_VERSION,
        "run_id": run_id,
        "sha": sha,
        "phase": "building",
        "started_at": utc_stamp(),
        "mode": config.policy.mode_label,
        "p2p_stack": config.policy.p2p_stack,
        "run_dir": str(run_dir),
    }
    write_run_json(run_dir, run_state)
    state.update({"failed": False, "current_run": run_id, "phase": "building", "running_sha": sha})
    save_state(state_path, state)

    binary = build_binary(config, sha)
    run_state.update({"phase": "installing", "binary_sha256": sha256_file(binary)})
    write_run_json(run_dir, run_state)
    install_binary(config, binary)

    run_state["phase"] = "preparing-empty-state"
    write_run_json(run_dir, run_state)
    stop_service(config)
    safe_wipe_state(config)
    render_config(config, run_dir)
    compress_previous_log(config, run_dir)

    run_state["phase"] = "syncing"
    write_run_json(run_dir, run_state)
    state.update({"phase": "syncing", "running_sha": sha, "current_run": run_id})
    save_state(state_path, state)
    start_service(config)
    try:
        wait_for_completion(config, run_dir, run_state)
    finally:
        stop_service(config)

    completed_at = utc_stamp()
    run_state.update({"phase": "complete", "completed_at": completed_at})
    write_run_json(run_dir, run_state)
    state.update(
        {
            "failed": False,
            "last_success_sha": sha,
            "last_success_at": completed_at,
            "last_success_run": run_id,
            "phase": "complete",
            "runs": int(state.get("runs", 0)) + 1,
        }
    )
    save_state(state_path, state)
    post_slack(config, completion_text(config, run_state))
    cleanup_retention(config)
    return state


def halt(config: Config, state_path: Path, state: dict[str, Any], run_state: dict[str, Any], reason: str) -> None:
    failed_at = utc_stamp()
    run_state.update({"phase": "failed", "failed_at": failed_at, "failure": reason})
    run_dir = Path(str(run_state.get("run_dir") or config.paths.runs_dir / "unknown"))
    if run_dir.exists():
        write_run_json(run_dir, run_state)
    state.update(
        {
            "failed": True,
            "failure": reason,
            "failed_at": failed_at,
            "phase": "failed",
            "last_failed_sha": run_state.get("sha"),
            "last_failed_run": run_state.get("run_id"),
        }
    )
    save_state(state_path, state)
    post_slack(config, failure_text(config, run_state, reason))
    log(config, f"halted reason={reason}")


def run_loop(config: Config, config_path: Path) -> int:
    state_path = config.paths.state_dir / "state.json"
    config.paths.state_dir.mkdir(parents=True, exist_ok=True)
    config.paths.runs_dir.mkdir(parents=True, exist_ok=True)
    state = load_state(state_path)
    if state.get("failed"):
        print(f"controller halted: {state.get('failure')}", file=sys.stderr)
        return 2

    while True:
        run_state: dict[str, Any] = {}
        try:
            state = one_cycle(config, state_path, state)
        except Exception as error:
            reason = f"{type(error).__name__}: {error}"
            current_run = state.get("current_run")
            if current_run:
                run_json = config.paths.runs_dir / str(current_run) / "run.json"
                if run_json.exists():
                    try:
                        run_state = json.loads(run_json.read_text(encoding="utf-8"))
                    except json.JSONDecodeError:
                        run_state = {}
            halt(config, state_path, state, run_state or state, reason)
            return 1
        time.sleep(config.policy.cooldown_seconds)


def status(config: Config) -> int:
    state_path = config.paths.state_dir / "state.json"
    state = load_state(state_path)
    payload = {
        "hostname": config.policy.hostname,
        "mode": config.policy.mode_label,
        "p2p_stack": config.policy.p2p_stack,
        "service_active": service_active(config),
        "controller_state": state,
        "sample": sample_status(config),
        "disk_free_bytes": shutil.disk_usage(config.paths.chain_state_dir).free,
        "log_path": str(config.paths.log_file),
        "trace_path": str(config.paths.trace_link),
        "monitor_log_path": str(config.paths.monitor_log),
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def resume(config: Config) -> int:
    state_path = config.paths.state_dir / "state.json"
    state = load_state(state_path)
    state.pop("failed", None)
    state.pop("failure", None)
    state["phase"] = "resumed"
    state["resumed_at"] = utc_stamp()
    save_state(state_path, state)
    run(["systemctl", "reset-failed", "zakura-continuous-sync.service"], check=False)
    run(["systemctl", "start", "zakura-continuous-sync.service"], check=False)
    print("resumed")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("/etc/zakura-continuous-sync/controller.toml"),
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("run")
    sub.add_parser("status")
    sub.add_parser("resume")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = load_config(args.config)
    if args.command == "run":
        return run_loop(config, args.config)
    if args.command == "status":
        return status(config)
    if args.command == "resume":
        return resume(config)
    raise AssertionError(args.command)


if __name__ == "__main__":
    raise SystemExit(main())
