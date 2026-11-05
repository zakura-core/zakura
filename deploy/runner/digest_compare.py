#!/usr/bin/env python3
"""Collect live comparison rows on the existing deploy runner and write one 30-minute report.

The control file records phase transitions made by the operator; this collector never deploys.
Raw observations rotate independently per node. status.json drives the desktop wake monitor.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import signal
import statistics
import subprocess
import sys
import time

HOSTS = {"us-west-0": "root@143.244.184.176", "canada-0": "root@159.203.38.10"}
ROOT = Path(__file__).resolve().parent


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


class RemoteNodes:
    """Use the registered runner's existing SSH identity and the standard bounded check helper."""

    def __init__(self, helper):
        self.helper = helper
        self.source = (ROOT / "digest_probe.py").read_text()

    def invoke(self, node, operation, value):
        if node not in HOSTS:
            raise ValueError("node is outside the approved comparison pair")
        duration = 120 if operation == "profile" else 30
        command = [sys.executable, str(self.helper), "--command", "python3", "--check-timeout", str(duration), "--limit", str(duration + 10), HOSTS[node], "--", "python3", "-c", self.source, operation, value]
        result = subprocess.run(command, capture_output=True, text=True, timeout=duration + 15)
        if result.returncode:
            # The helper's final JSON contains a classified, bounded diagnostic.
            detail = result.stderr.strip().splitlines()[-1:]
            raise RuntimeError(f"{node}: SSH operation failed ({result.returncode}): {detail}")
        return json.loads(result.stdout)


def phase_at(at, transitions):
    phase = None
    for transition in sorted(transitions, key=lambda transition: transition["at"]):
        if transition["at"] <= at:
            phase = transition["phase"]
    return phase


def complete_summary(payload):
    required = {"initial_id_calls", "initial_combined_calls", "transactions_auth_calls", "commitment_root_reuse_calls", "commitment_root_recompute_calls", "semantic_ns", "total_ns", "handoff_wall_ns", "commit_wall_ns"}
    return (required <= payload.keys() and payload.get("schema") == 1
            and payload.get("route") == "ordinary" and payload.get("outcome") == "accepted"
            and payload.get("active", 1) == 0 and payload.get("ambiguous", 1) == 0
            and bool(payload.get("semantic_ns")) and bool(payload.get("handoff_wall_calls"))
            and bool(payload.get("commit_wall_calls")))


def trace_coverage(previous, rows, lost_segments, now):
    """Retain coverage failures and allow concurrent producers one minute to fill sequence gaps."""
    state = dict(previous)
    state["failed"] = bool(state.get("failed") or lost_segments)
    processes = state.setdefault("processes", {})
    for row in rows:
        process = processes.setdefault(str(row["pid"]), {"last": None, "missing": {}})
        if row.get("trace_drops"):
            state["failed"] = True
        sequence = row.get("sequence")
        if sequence is None:
            state["failed"] = True
            continue
        last = process["last"]
        if last is not None and sequence > last + 1:
            if sequence - last > 4096:
                state["failed"] = True
            else:
                for missing in range(last + 1, sequence):
                    process["missing"].setdefault(str(missing), now)
        process["missing"].pop(str(sequence), None)
        process["last"] = max(sequence, last if last is not None else sequence)
    for process in processes.values():
        if any(now - since > 60 for since in process["missing"].values()):
            state["failed"] = True
            process["missing"] = {}
    state["processes"] = dict(list(processes.items())[-64:])
    return state


def eligible(row, control, profile_windows):
    payload = row["payload"]
    if row["event"] != "finish" or not complete_summary(payload):
        return False
    end = row["at_ns"] / 1e9
    start = end - payload["total_ns"] / 1e9
    if control.get("observation_finished_at") and end > control["observation_finished_at"]:
        return False
    if phase_at(start, control["transitions"]) not in {"phase1", "phase2"}:
        return False
    if phase_at(start, control["transitions"]) != phase_at(end, control["transitions"]):
        return False
    return not any(window["node"] == row["node"] and start <= window["end"] + 5 and end >= window["start"] - 5 for window in profile_windows)


def observations(directory):
    for node in HOSTS:
        paths = list(directory.glob(f"{node}.jsonl*"))
        for path in paths:
            with path.open() as source:
                for line in source:
                    try:
                        yield json.loads(line)
                    except ValueError:
                        continue


def summarize(rows, control, revisions, profiles):
    """Pair identical accepted block hashes within phases; retain late work separately."""
    rows = list(rows)
    hashes = {value["sha256"]: role for role, value in revisions.items()}
    sequences = {}
    drops = {}
    lost_nodes = set()
    attempts = {}
    for row in rows:
        if row.get("event") == "resource":
            if row.get("lost_segments") or row.get("coverage_failed"):
                # An evicted cursor can span process restarts; do not guess which PID lost rows.
                lost_nodes.add(row["node"])
            continue
        key = (row["node"], row.get("pid"))
        attempt = (*key, row.get("payload", {}).get("hash"))
        if row.get("event") in {"start", "finish"}:
            counts = attempts.setdefault(attempt, {"start": 0, "finish": 0})
            counts[row["event"]] += 1
        if "sequence" in row:
            sequences.setdefault(key, set()).add(row["sequence"])
        drops[key] = max(drops.get(key, 0), row.get("trace_drops", 0))
    gaps = {key: max(values) - min(values) + 1 - len(values) for key, values in sequences.items() if values}
    incomplete = {key for key in drops if drops[key] or gaps.get(key, 0)}
    paired = {}
    excluded = {}
    late = {}
    trace_drops = {}
    total = 0
    resources = {node: [] for node in HOSTS}
    started = {}
    finished = {}
    for row in rows:
        total += 1
        if row.get("event") == "resource":
            resources[row["node"]].append(row)
            continue
        key = (row["node"], row.get("pid"))
        attempt_key = (*key, row.get("payload", {}).get("hash"))
        if row.get("event") == "start":
            started[attempt_key] = started.get(attempt_key, 0) + 1
        elif row.get("event") == "finish":
            finished[attempt_key] = finished.get(attempt_key, 0) + 1
        trace_drops[str(key)] = max(trace_drops.get(str(key), 0), row.get("trace_drops", 0))
        payload = row.get("payload", {})
        role = hashes.get(row.get("binary_sha256"))
        if row.get("event") == "phase" and role:
            # Late snapshots are cumulative for a triggering block; keep the latest snapshot.
            late_key = (row["node"], row.get("pid"), payload.get("hash"), payload.get("phase"))
            if payload.get("wall_calls", 0) >= late.get(late_key, {}).get("wall_calls", 0):
                late[late_key] = {**payload, "role": role}
            continue
        complete_attempt = attempts.get(attempt_key) == {"start": 1, "finish": 1}
        if not role or key in incomplete or row["node"] in lost_nodes or not complete_attempt or not eligible(row, control, profiles):
            reason = payload.get("route", row.get("event", "unknown"))
            if row.get("event") in {"finish", "checkpoint"}:
                excluded[reason] = excluded.get(reason, 0) + 1
            continue
        phase = phase_at(row["at_ns"] / 1e9, control["transitions"])
        pair = paired.setdefault((phase, payload["hash"]), {})
        if role in pair:
            # Multiple successful attempts are not a clean matched observation.
            pair[role] = None
        else:
            pair[role] = {**payload, "_node": row["node"], "_pid": row.get("pid")}
    # Only late work linked to an eligible matched primary block enters paired timing.
    for pair in paired.values():
        if not (pair.get("baseline") and pair.get("candidate")):
            continue
        for role, payload in pair.items():
            for phase in ("writer", "finalized"):
                snapshot = late.get((payload["_node"], payload["_pid"], payload["hash"], phase))
                if snapshot:
                    for name, value in snapshot.items():
                        if name.endswith(("_calls", "_ns")):
                            payload[f"late_{phase}_{name}"] = value
    fields = {"late_writer_wall_ns", "late_finalized_wall_ns", "semantic_ns", "total_ns", "handoff_wall_ns", "commit_wall_ns", "commitment_wall_ns", "initial_wall_ns", "transactions_wall_ns", "parent_wall_ns"}
    accepted_pairs = [pair for pair in paired.values() if pair.get("baseline") and pair.get("candidate")]
    digest_fields = sorted({field for pair in accepted_pairs for payload in pair.values() for field in payload if field.endswith(("_calls", "_ns")) and any(token in field for token in ("_id_", "_auth_", "_combined_", "_root", "_sighash_"))})
    timing = {}
    for phase in ("phase1", "phase2", "all"):
        selected = [pair for (pair_phase, _), pair in paired.items() if (phase == "all" or phase == pair_phase) and pair.get("baseline") and pair.get("candidate")]
        timing[phase] = {"pairs": len(selected), "metrics": {}}
        for field in fields | set(digest_fields):
            if not selected:
                continue
            field_pairs = [pair for pair in selected if not field.startswith("late_") or all(field in pair[role] for role in ("baseline", "candidate"))]
            if not field_pairs:
                continue
            values = {role: [pair[role].get(field, 0) for pair in field_pairs] for role in ("baseline", "candidate")}
            stats = {role: {"median": statistics.median(items), "minimum": min(items), "maximum": max(items)} for role, items in values.items()}
            stats["paired_delta_median"] = statistics.median(pair["candidate"].get(field, 0) - pair["baseline"].get(field, 0) for pair in field_pairs)
            stats["matched_samples"] = len(field_pairs)
            timing[phase]["metrics"][field] = stats
    mix = {role: {name: sum(pair[role].get(name, 0) for pair in accepted_pairs) for name in ("tx", "pre_v5", "v5", "v6", "sprout", "sapling", "orchard", "ironwood")} for role in ("baseline", "candidate")}
    return {"total_records": total, "timing": timing, "transaction_mix": mix, "excluded": excluded, "unmatched": sum(not (pair.get("baseline") and pair.get("candidate")) for pair in paired.values()), "trace_drops_by_process": trace_drops, "sequence_gaps_by_process": {str(key): count for key, count in gaps.items()}, "late_phases": list(late.values()), "profiles": profiles, "missing_finish": sum(max(0, count - finished.get(key, 0)) for key, count in started.items()), "resource_read_gaps": {node: sum(row.get("lost_segments", 0) for row in samples) for node, samples in resources.items()}, "resource_samples": {node: len(samples) for node, samples in resources.items()}, "interpretation": "Live observational comparison; hashing work counts are direct observations. Timings include instrumentation overhead and do not establish an isolated speedup."}


def markdown(report):
    lines = ["Live Mainnet digest comparison", "", report["interpretation"], "", "| Phase | Matched blocks |", "|---|---:|"]
    for phase, data in report["timing"].items():
        lines.append(f"| {phase} | {data['pairs']} |")
    lines += ["", "| Layer (milliseconds) | Baseline median [min, max] | Candidate median [min, max] | Paired median delta |", "|---|---:|---:|---:|"]
    for field, value in sorted(report["timing"]["all"]["metrics"].items()):
        if field.endswith("_ns") and field in {"semantic_ns", "total_ns", "handoff_wall_ns", "commit_wall_ns", "commitment_wall_ns", "initial_wall_ns", "transactions_wall_ns", "parent_wall_ns", "late_writer_wall_ns", "late_finalized_wall_ns"}:
            cells = [f"{value[role]['median']/1e6:.4f} [{value[role]['minimum']/1e6:.4f}, {value[role]['maximum']/1e6:.4f}]" for role in ("baseline", "candidate")]
            labels = {"semantic_ns": "Semantic block verification", "total_ns": "Verification through commit response", "handoff_wall_ns": "State handoff through response", "commit_wall_ns": "Contextual validation", "commitment_wall_ns": "Header commitment check", "initial_wall_ns": "Initial transaction hashing", "transactions_wall_ns": "Transaction verification", "parent_wall_ns": "Parent and state waits", "late_writer_wall_ns": "Writer including post-response work", "late_finalized_wall_ns": "Finalized storage triggered by block"}
            lines.append(f"| {labels[field]} | {cells[0]} | {cells[1]} | {value['paired_delta_median']/1e6:.4f} |")
    lines += ["", "| Executed digest operations per block | Baseline median | Candidate median |", "|---|---:|---:|"]
    for field, value in sorted(report["timing"]["all"]["metrics"].items()):
        if field.endswith("_calls"):
            lines.append(f"| {field} | {value['baseline']['median']} | {value['candidate']['median']} |")
    lines += ["", "Overlapping phase durations must not be added. Finalization after the response is retained in the JSON report's late_phases array.", "", f"Transaction mix: `{json.dumps(report['transaction_mix'], sort_keys=True)}`", f"Unmatched blocks: {report['unmatched']}; excluded records: `{json.dumps(report['excluded'], sort_keys=True)}`.", f"Trace drops by process: `{json.dumps(report['trace_drops_by_process'], sort_keys=True)}`.", "", "Tail percentiles are omitted: this short live window is intended for medians, spread, and work-count evidence."]
    lines += ["", "CPU samples (self CPU percentages, not wall-time shares):", ""]
    import re
    for profile in report["profiles"]:
        lines.append(f"{profile['node']} / {profile['phase']}: capture exit {profile.get('exit', 'unavailable')}, report exit {profile.get('report_exit', 'unavailable')}")
        matches = []
        for line in profile.get("report", "").splitlines():
            match = re.match(r"^\s*(\d+(?:\.\d+)?)%\s+.*?\[([.k])\]\s+(.*)$", line)
            if match:
                matches.append((float(match[1]), match[3].replace("|", " ")))
        for share, symbol in sorted(matches, reverse=True)[:3]:
            lines.append(f"- {share:.2f}%: `{symbol}`")
        if not matches:
            lines.append("- Insufficient symbolized CPU samples; inspect profile coverage before drawing conclusions.")
    lines += ["", f"Missing finish records: {report['missing_finish']}; sequence gaps: `{json.dumps(report['sequence_gaps_by_process'], sort_keys=True)}`; trace-read gaps: `{json.dumps(report['resource_read_gaps'], sort_keys=True)}`."]
    return "\n".join(lines) + "\n"


def collect(manifest_path, output, helper):
    manifest = json.loads(manifest_path.read_text())
    if set(manifest["nodes"]) != set(HOSTS):
        raise ValueError("manifest must name exactly the approved pair")
    output.mkdir(parents=True, exist_ok=True)
    remote = RemoteNodes(helper)
    cursors_path = output / "cursors.json"
    cursors = json.loads(cursors_path.read_text()) if cursors_path.exists() else {}
    pid_maps = {node: {str(key): value for key, value in cursors.get(node, {}).get("pid_map", {}).items()} for node in HOSTS}
    loggers = {}
    for node in HOSTS:
        logger = logging.getLogger(node)
        logger.setLevel(logging.INFO)
        logger.propagate = False
        logger.addHandler(RotatingFileHandler(output / f"{node}.jsonl", maxBytes=64 * 1024 * 1024, backupCount=3))
        loggers[node] = logger
    stopped = False
    def stop(_signal, _frame):
        nonlocal stopped
        stopped = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    recent = {node: [] for node in HOSTS}
    status = {"state": "collecting", "started_at": time.time(), "first_report_ready": False, "nodes": {}}
    profile_windows = json.loads((output / "profiles.json").read_text()) if (output / "profiles.json").exists() else []
    profile_futures = {}
    progress = {}
    trace_progress = {}
    with ThreadPoolExecutor(max_workers=2) as pool:
        while not stopped:
            control = json.loads((output / "control.json").read_text())
            try:
                for node in HOSTS:
                    request_started = time.time()
                    result = remote.invoke(node, "sample", json.dumps(cursors.get(node, {})))
                    result["clock_offset_estimate_s"] = result["at"] - (request_started + time.time()) / 2
                    result["round_trip_s"] = time.time() - request_started
                    pid_maps[node][str(result["pid"])] = result["binary_sha256"]
                    rows = result.pop("rows")
                    coverage = trace_coverage(cursors.get(node, {}).get("coverage", {}), rows, result["lost_segments"], time.time())
                    result["coverage_failed"] = coverage["failed"]
                    for row in rows:
                        row.update(node=node, binary_sha256=pid_maps[node].get(str(row.get("pid"))))
                        loggers[node].info(json.dumps(row, separators=(",", ":")))
                        if row.get("event") == "finish" and row["payload"].get("outcome") == "accepted" and row["payload"].get("route") == "ordinary":
                            recent[node] = (recent[node] + [row])[-4:]
                    loggers[node].info(json.dumps({**result, "event": "resource", "node": node}, separators=(",", ":")))
                    cursors[node] = {**result["cursor"], "pid_map": dict(list(pid_maps[node].items())[-64:]), "coverage": coverage}
                    trace_state = trace_progress.get(node)
                    position = result["cursor"].get("position")
                    if trace_state is None or trace_state["position"] != position:
                        trace_state = {"position": position, "changed_at": time.time()}
                    trace_progress[node] = trace_state
                    previous = progress.get(node)
                    height = result.get("rpc", {}).get("blocks")
                    if previous is None or previous["pid"] != result["pid"]:
                        previous = {"pid": result["pid"], "height": height, "changed_at": time.time(), "advanced": False}
                    elif isinstance(height, int) and (previous["height"] is None or height > previous["height"]):
                        previous.update(height=height, changed_at=time.time(), advanced=True)
                    progress[node] = previous
                    status["nodes"][node] = {**result, "recent_accepted": recent[node], "progress": previous, "trace_progress": trace_state, "coverage_failed": coverage["failed"]}
                write_json(cursors_path, cursors)
                phase = phase_at(time.time(), control["transitions"])
                for node in HOSTS:
                    key = (node, phase)
                    if phase in {"phase1", "phase2"} and key not in profile_futures and not any(window.get("node") == node and window.get("phase") == phase for window in profile_windows):
                        profile_windows.append({"node": node, "phase": phase, "start": time.time(), "end": time.time() + 130, "pending": True})
                        write_json(output / "profiles.json", profile_windows)
                        profile_futures[key] = pool.submit(remote.invoke, node, "profile", phase)
                for key, future in list(profile_futures.items()):
                    if future.done():
                        try:
                            index = next(index for index, window in enumerate(profile_windows) if window["node"] == key[0] and window["phase"] == key[1])
                            profile_windows[index] = {**future.result(), "node": key[0]}
                        except Exception as error:
                            index = next(index for index, window in enumerate(profile_windows) if window["node"] == key[0] and window["phase"] == key[1])
                            profile_windows[index].update(error=str(error), pending=False, end=time.time() + 90)
                        del profile_futures[key]
                        write_json(output / "profiles.json", profile_windows)
                start = control.get("observation_started_at")
                status["observation_started_at"] = start
                status["crossover_due"] = bool(start and time.time() >= start + 900 and not any(transition["phase"] == "phase2" for transition in control["transitions"]))
                report_path = output / "first-report.json"
                if start and time.time() >= start + 1800 and not report_path.exists():
                    report_control = {**control, "observation_finished_at": start + 1800}
                    report = summarize(observations(output), report_control, manifest["revisions"], profile_windows)
                    report.update(control=control, node_health=status["nodes"], generated_at=time.time())
                    write_json(report_path, report)
                    (output / "first-report.md").write_text(markdown(report))
                status["first_report_ready"] = report_path.exists()
                issues = []
                for node, health in status["nodes"].items():
                    if health.get("coverage_failed"):
                        issues.append(f"{node}: trace coverage lost; primary comparison is not qualified")
                    if phase != "swapping" and (health["service"] != "active" or health["rpc"].get("error")):
                        issues.append(f"{node}: node or RPC unhealthy")
                    if phase in {"phase1", "phase2"} and time.time() - health["progress"]["changed_at"] > 600:
                        issues.append(f"{node}: chain height has not advanced for ten minutes")
                    trace = health["trace_progress"]
                    if phase in {"phase1", "phase2"} and time.time() - trace["changed_at"] > 120 and health["progress"]["changed_at"] > trace["changed_at"] + 20:
                        issues.append(f"{node}: trace sink stopped advancing while chain height advanced")
                    if abs(health["clock_offset_estimate_s"]) > max(2, health["round_trip_s"] / 2 + 1):
                        issues.append(f"{node}: node clock differs from controller beyond timing uncertainty")
                    if min(health["disk_free"], health.get("trace_disk_free", health["disk_free"])) < 5 * 1024**3:
                        issues.append(f"{node}: disk headroom below five GiB")
                for profile in profile_windows:
                    if profile.get("error") or (not profile.get("pending") and (profile.get("exit") != 0 or profile.get("report_exit") != 0)):
                        issues.append(f"{profile['node']}: {profile['phase']} CPU profile unavailable")
                if issues and issues != status.get("issues"):
                    status["attention_at"] = time.time()
                status["issues"] = issues
                status["state"] = "collecting"
                status["at"] = time.time()
                write_json(output / "status.json", status)
            except Exception as error:
                status.update(state="failed", error=str(error), at=time.time())
                write_json(output / "status.json", status)
                raise
            for _ in range(10):
                if stopped:
                    break
                time.sleep(1)
    status.update(state="stopped", at=time.time())
    write_json(output / "status.json", status)


def activate(manifest_path, output, phase):
    manifest = json.loads(manifest_path.read_text())
    if not manifest.get("approved"):
        raise ValueError("exact instrumented revisions require the planned Mainnet confirmation")
    control_path = output / "control.json"
    control = json.loads(control_path.read_text())
    previous = phase_at(time.time(), control["transitions"])
    allowed = {None: "phase1", "phase1": "swapping", "swapping": "phase2"}
    if allowed.get(previous) != phase:
        raise ValueError(f"invalid transition from {previous} to {phase}")
    if phase != "swapping":
        status = json.loads((output / "status.json").read_text())
        if time.time() - status["at"] > 30 or status["state"] != "collecting" or status.get("issues"):
            raise ValueError("collector is not fresh and healthy")
        roles = {"us-west-0": "baseline", "canada-0": "candidate"}
        if phase == "phase2":
            roles = {node: ("candidate" if role == "baseline" else "baseline") for node, role in roles.items()}
        hashes = []
        for node, role in roles.items():
            state = status["nodes"][node]
            expected = manifest["revisions"][role]["sha256"]
            if state["service"] != "active" or state["binary_sha256"] != expected or state["rpc"].get("chain") not in {"main", "mainnet", "Mainnet"}:
                raise ValueError(f"{node} has not qualified the expected {role} binary")
            if not state.get("progress", {}).get("advanced"):
                raise ValueError(f"{node} has not advanced its RPC height since restart")
            if min(state["disk_free"], state.get("trace_disk_free", state["disk_free"])) < 5 * 1024**3:
                raise ValueError(f"{node} has insufficient disk headroom")
            accepted = {row["payload"]["hash"] for row in state["recent_accepted"] if row["pid"] == state["pid"] and row["binary_sha256"] == expected and complete_summary(row["payload"])}
            hashes.append(accepted)
        if not (hashes[0] & hashes[1]):
            raise ValueError("both nodes must produce a complete accepted observation for the same block")
        tips = {state["rpc"].get("bestblockhash") for state in status["nodes"].values()}
        if len(tips) != 1 or not next(iter(tips)):
            raise ValueError("both nodes must agree on their current RPC tip")
    now = time.time()
    control["transitions"].append({"phase": phase, "at": now})
    if phase == "phase1":
        control["observation_started_at"] = now
    write_json(control_path, control)
    print(json.dumps(control))


def watch(helper, thread, state_path):
    """Wake the owning desktop chat on crossover, first report, failures, or a stale collector."""
    events = set(json.loads(state_path.read_text())) if state_path.exists() else set()
    transport_failures = 0
    while True:
        try:
            command = [sys.executable, str(helper), "--path", "/var/lib/zakura-digest-compare/pr2022/status.json", "root@159.65.183.89", "--", "cat", "/var/lib/zakura-digest-compare/pr2022/status.json"]
            result = subprocess.run(command, capture_output=True, text=True, timeout=100)
            if result.returncode:
                # Preserve classification without exporting remote output or potentially sensitive errors.
                diagnostic = {}
                for line in result.stderr.splitlines():
                    if line.startswith("ssh-check: {"):
                        try:
                            record = json.loads(line.removeprefix("ssh-check: "))
                            diagnostic = {key: record[key] for key in ("result", "cause", "step", "execution", "status", "precondition") if key in record}
                        except ValueError:
                            pass
                if diagnostic.get("result") == "unreachable" and diagnostic.get("cause") == "network" and diagnostic.get("execution") == "not_started":
                    transport_failures += 1
                    if transport_failures <= 3:
                        print(json.dumps({"event": "transport_retry", "attempt": transport_failures, "classification": diagnostic}), flush=True)
                        time.sleep(min(15 * transport_failures, 30))
                        continue
                raise RuntimeError(f"controller status check failed: exit {result.returncode}; classification={json.dumps(diagnostic, sort_keys=True)}")
            transport_failures = 0
            status = json.loads(result.stdout)
            event = None
            if status["state"] in {"failed", "stopped"} or time.time() - status["at"] > 60:
                event = "collector_failed_or_stalled"
            elif status.get("issues"):
                event = "collector_attention_" + str(status.get("attention_at"))
            elif status.get("first_report_ready"):
                event = "first_report_ready"
            elif status.get("crossover_due"):
                event = "crossover_due"
            if event and event not in events:
                message = f"Mainnet digest comparison monitor: {event}. Inspect the owned collector at /var/lib/zakura-digest-compare/pr2022 on us-east-0 and continue the authorized comparison plan. This is a status event, not a new authorization."
                queued = subprocess.run(["codex", "queue", "--thread", thread, "--message", message, "--resume"], capture_output=True, text=True, timeout=30)
                active_writer = "already has an active writer" in queued.stderr and "Queued message" in queued.stdout
                if queued.returncode or ("warning" in queued.stderr.lower() and not active_writer):
                    raise RuntimeError("native chat wake failed: " + queued.stderr[-1000:])
                events.add(event)
                state_path.write_text(json.dumps(sorted(events)))
                print(json.dumps({"event": event, "automatic_wake": True}), flush=True)
            if event == "collector_failed_or_stalled":
                return
        except Exception as error:
            message = f"Mainnet digest comparison monitor stopped: {error}. Inspect the collector directly over SSH; automatic follow-up is unavailable until repaired."
            subprocess.run(["codex", "queue", "--thread", thread, "--message", message, "--resume"], timeout=30)
            raise
        time.sleep(15)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["collect", "report", "activate", "watch"])
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--ssh-helper", type=Path)
    parser.add_argument("--phase", choices=["phase1", "swapping", "phase2"])
    parser.add_argument("--thread")
    args = parser.parse_args(argv)
    if args.operation == "collect":
        if args.ssh_helper is None:
            parser.error("collect requires --ssh-helper")
        collect(args.manifest, args.output, args.ssh_helper)
    elif args.operation == "activate":
        if not args.phase:
            parser.error("activate requires --phase")
        activate(args.manifest, args.output, args.phase)
    elif args.operation == "watch":
        if args.ssh_helper is None or not args.thread:
            parser.error("watch requires --ssh-helper and --thread")
        watch(args.ssh_helper, args.thread, args.output / "wake-events.json")
    else:
        manifest = json.loads(args.manifest.read_text())
        control = json.loads((args.output / "control.json").read_text())
        profiles = json.loads((args.output / "profiles.json").read_text()) if (args.output / "profiles.json").exists() else []
        report = summarize(observations(args.output), control, manifest["revisions"], profiles)
        print(markdown(report))


if __name__ == "__main__":
    main()
