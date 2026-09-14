"""Compact, versioned sync telemetry independent of rotating diagnostic traces.

Only allowlisted numeric metrics and public benchmark settings enter a report.
The collector and read-only status helper use only the Python standard library.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import time
import tomllib

VERSION = 1
MAX_REPORT_BYTES = 32 * 1024**2
MAX_REPORTS = 256
RETENTION_SECONDS = 30 * 86400
RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,95}\Z")

# Counter names accept the Prometheus exporter's optional _total suffix.
METRICS = {
    "height": "zcash_chain_verified_block_height",
    "download_zakura": "sync_block_payload_received_bytes",
    "commit_zakura": "sync_block_payload_committed_bytes",
    "download_legacy": "sync_legacy_payload_received_bytes",
    "commit_legacy": "sync_legacy_payload_committed_bytes",
    "apply_ready": "sync_block_applying_unsubmitted",
    "apply_submitted": "sync_block_applying_submitted",
    "reorder": "sync_block_reorder_blocks",
    "apply_bytes": "sync_block_applying_buffered_bytes",
    "reorder_bytes": "sync_block_reorder_buffered_bytes",
    "request_bytes": "sync_block_budget_reserved_bytes",
    "request_available": "sync_block_budget_available_bytes",
    "request_floor_bytes": "sync_block_bbr_min_cwnd_bytes",
    "peers": "sync_block_peers_with_status",
    "gap_height": "sync_block_floor_gap_height",
    "gap_request_age": "sync_block_floor_gap_oldest_request_seconds",
    "legacy_waiting_network": "sync_downloads_waiting_network",
    "legacy_downloading": "sync_downloads_downloading",
    "legacy_waiting_verifier": "sync_downloads_waiting_verifier",
    "legacy_verifying": "sync_downloads_verifying",
    "vct_fast": "state_vct_fast_block_count",
    "vct_legacy": "state_vct_legacy_block_count",
    "sapling_height": "sync_report_sapling_height",
    "ironwood_height": "sync_report_ironwood_height",
    "checkpoint_height": "sync_report_checkpoint_height",
    "tcp_received": "zcash_net_in_bytes_total",
    "rss": "process_resident_memory_bytes",
}
COLUMNS = ["t", *METRICS, "cpu_total", "cpu_idle", "cpu_iowait", "ready"]
INDEX = {name: index for index, name in enumerate(COLUMNS)}
METRIC_KEYS = {metric: key for key, metric in METRICS.items()}
METRIC_KEYS.update({metric + "_total": key for key, metric in METRICS.items()})


def finite(value) -> bool:
    return type(value) in (int, float) and math.isfinite(value)


def sample_metrics(text: str) -> dict:
    """Ignore labelled/per-peer series and non-finite or malformed observations."""
    result = {}
    for line in text.splitlines():
        fields = line.split()
        if len(fields) < 2:
            continue
        key = METRIC_KEYS.get(fields[0].replace(".", "_"))
        if key is None:
            continue
        try:
            value = float(fields[1])
        except ValueError:
            continue
        if finite(value) and value >= 0:
            result[key] = value
    return result


def public_settings(path: Path) -> dict:
    """Do not retain configuration text, peer identities, paths, or credentials."""
    try:
        raw = tomllib.loads(path.read_text())
    except (OSError, ValueError):
        return {"available": False}
    sections = {
        "network": ("network", "p2p_stack", "peerset_initial_target_size"),
        "consensus": ("checkpoint_sync", "vct_fast_sync"),
        "state": ("storage_mode",),
    }
    result = {section: {key: raw.get(section, {}).get(key) for key in keys}
              for section, keys in sections.items()}
    for section in result.values():
        for key, value in section.items():
            if not (value is None or type(value) in (bool, int)
                    or isinstance(value, str) and value in {"Mainnet", "Testnet", "dual", "zakura", "legacy", "full", "pruned"}):
                section[key] = None
    block_sync = raw.get("network", {}).get("zakura", {}).get("block_sync", {})
    knobs = {
        "replace_legacy_syncer", "max_blocks_per_response", "max_inflight_requests",
        "initial_inflight_requests", "max_response_bytes", "max_inflight_block_bytes",
        "max_reorder_lookahead_bytes", "max_submitted_block_applies",
        "initial_block_probe_requests", "max_requests_without_block_progress",
        "size_deviation_tolerance", "bbr_cwnd_gain_percent", "bbr_probe_bw_gain_percent",
        "bbr_startup_growth_percent", "bbr_min_cwnd", "bbr_min_cwnd_bytes",
        "bbr_delay_gradient_percent", "bbr_reliability_weight_percent", "floor_bypass_slots",
    }
    result["block_sync"] = {
        key: value for key, value in block_sync.items()
        if key in knobs and (type(value) is bool or finite(value))
    }
    for key in ("request_timeout", "floor_rescue_timeout", "floor_peer_avoid_cooldown",
                "no_progress_peer_cooldown", "status_refresh_interval", "bbr_probe_rtt_interval",
                "bbr_probe_rtt_duration", "bbr_rtprop_window", "bbr_delivery_rate_window"):
        value = block_sync.get(key)
        if isinstance(value, str) and re.fullmatch(r"(?:[0-9.]+(?:ns|us|ms|s|m|h|d) ?)+", value):
            result["block_sync"][key] = value
    if block_sync.get("bbr_cwnd_unit") in ("bytes", "blocks"):
        result["block_sync"]["bbr_cwnd_unit"] = block_sync["bbr_cwnd_unit"]
    result["tuning_complete"] = block_sync.keys() <= result["block_sync"].keys()
    result["available"] = True
    return result


def atomic_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w") as file:
        json.dump(data, file, separators=(",", ":"), allow_nan=False)
        file.flush()
        os.fsync(file.fileno())
    temporary.replace(path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def report_paths(directory: Path, run_id: str) -> tuple[Path, Path]:
    if not RUN_ID.fullmatch(run_id):
        raise ValueError("invalid report run ID")
    return directory / f"{run_id}.json", directory / f"{run_id}.jsonl"


class Recorder:
    """Append full-run samples without making chart failures stop a sync."""

    def __init__(self, directory: Path, run: dict, config: Path, interval: int):
        self.directory = directory
        self.run_id = run["run_id"]
        self.meta_path, self.samples_path = report_paths(directory, self.run_id)
        self.started = time.monotonic()
        self.last_sample = -1.0
        self.error = None
        self.metadata = {
            "version": VERSION, "run_id": self.run_id, "sha": run.get("sha"),
            "mode": run.get("p2p_stack"), "started_at": run.get("sync_started_at_epoch"),
            "phase": "syncing", "interval": interval, "columns": COLUMNS,
            "settings": public_settings(config),
            "host": {"cpus": os.cpu_count(), "architecture": platform.machine(),
                     "kernel": platform.release()},
        }
        self._try(lambda: atomic_json(self.meta_path, self.metadata))

    def _try(self, operation):
        try:
            return operation()
        except (OSError, ValueError, TypeError) as error:
            # Store a bounded class name, never exception text containing config data.
            self.error = type(error).__name__
            print(f"sync report unavailable: {self.error}", flush=True)
            return None

    def record(self, status: dict) -> None:
        elapsed = round(time.monotonic() - self.started, 3)
        if elapsed <= self.last_sample:
            return
        self.last_sample = elapsed
        values = {"t": elapsed,
                  **status.get("report", {}), "ready": status.get("ready") is True}
        try:
            fields = Path("/proc/stat").read_text().splitlines()[0].split()[1:9]
            ticks = [int(value) for value in fields]
            if len(ticks) == 8:
                values.update(cpu_total=sum(ticks), cpu_idle=ticks[3], cpu_iowait=ticks[4])
        except (OSError, ValueError, IndexError):
            pass
        row = [values.get(column) for column in COLUMNS]

        def append():
            line = json.dumps(row, separators=(",", ":"), allow_nan=False) + "\n"
            size = self.samples_path.stat().st_size if self.samples_path.exists() else 0
            if size + len(line.encode()) > MAX_REPORT_BYTES:
                raise ValueError("report sample limit reached")
            with self.samples_path.open("a") as file:
                file.write(line)
        self._try(append)

    def finish(self, phase: str, ready_since: float | None = None) -> None:
        self.metadata.update(phase=phase, duration=round(time.monotonic() - self.started, 3),
                             finished_at=int(time.time()), ready_since=ready_since,
                             collection_error=self.error)

        def finish():
            if self.samples_path.is_file():
                compressed = self.samples_path.with_suffix(".jsonl.gz")
                temporary = compressed.with_suffix(".tmp")
                with self.samples_path.open("rb") as source, temporary.open("wb") as target:
                    with gzip.GzipFile(fileobj=target, mode="wb", mtime=0) as archive:
                        while chunk := source.read(65536):
                            archive.write(chunk)
                    target.flush()
                    os.fsync(target.fileno())
                temporary.replace(compressed)
                atomic_json(self.meta_path, self.metadata)
                self.samples_path.unlink()
            else:
                atomic_json(self.meta_path, self.metadata)
            cleanup_reports(self.directory, self.run_id)
        self._try(finish)


def cleanup_reports(directory: Path, active: str) -> None:
    """Keep compact reports for 30 days, capped at 256 runs, separately from logs."""
    records = sorted(directory.glob("*.json"), key=lambda path: path.stat().st_mtime, reverse=True)
    for index, path in enumerate(records):
        if path.stem == active or path.is_symlink() or not RUN_ID.fullmatch(path.stem):
            continue
        if index < MAX_REPORTS and time.time() - path.stat().st_mtime < RETENTION_SECONDS:
            continue
        for suffix in (".json", ".jsonl", ".jsonl.gz"):
            (directory / (path.stem + suffix)).unlink(missing_ok=True)


def read_report(directory: Path, run_id: str) -> dict:
    """Read one bounded report. Missing/old runs stay explicitly unavailable."""
    meta_path, samples_path = report_paths(directory, run_id)
    if not meta_path.exists():
        return {"run_id": run_id, "unavailable": "no retained report"}
    compressed = samples_path.with_suffix(".jsonl.gz")
    source = compressed if compressed.exists() else samples_path
    if meta_path.is_symlink() or source.is_symlink():
        raise ValueError("report symlinks are not supported")
    if meta_path.stat().st_size > 65536:
        raise ValueError("report metadata limit exceeded")
    metadata = json.loads(meta_path.read_text())
    if metadata.get("version") != VERSION or metadata.get("run_id") != run_id or metadata.get("columns") != COLUMNS:
        raise ValueError("unsupported report schema")
    if not source.exists():
        return {"run_id": run_id, "unavailable": "no retained samples"}
    opener = gzip.open if source == compressed else open
    with opener(source, "rb") as file:
        payload = file.read(MAX_REPORT_BYTES + 1)
    if len(payload) > MAX_REPORT_BYTES:
        raise ValueError("report sample limit exceeded")
    # An active writer may be midway through its final line.
    lines = payload.splitlines(keepends=True)
    rows = [json.loads(line) for line in lines if line.endswith(b"\n")]
    validate_report({"metadata": metadata, "samples": rows})
    return {"metadata": metadata, "samples": rows}


def validate_report(report: dict) -> None:
    """Validate remote samples before storing or rendering them."""
    metadata = report.get("metadata", {})
    if (metadata.get("version") != VERSION or metadata.get("columns") != COLUMNS
            or not RUN_ID.fullmatch(metadata.get("run_id", ""))
            or not finite(metadata.get("interval")) or not 0 < metadata["interval"] <= 86400
            or not isinstance(metadata.get("host"), dict)
            or not isinstance(metadata.get("settings"), dict)):
        raise ValueError("unsupported report schema")
    previous = -1.0
    rows = report.get("samples", [])
    if not isinstance(rows, list) or len(rows) > MAX_REPORT_BYTES // len(COLUMNS):
        raise ValueError("invalid report samples")
    for row in rows:
        if (not isinstance(row, list) or len(row) != len(COLUMNS)
                or not finite(row[0]) or row[0] <= previous
                or row[0] < 0 or type(row[-1]) is not bool
                or any(value is not None and (not finite(value) or value < 0) for value in row[1:-1])):
            raise ValueError("invalid report sample")
        previous = row[0]
    duration = metadata.get("duration")
    if duration is not None and (not finite(duration) or duration < max(0, previous)):
        raise ValueError("invalid report duration")
    ready = metadata.get("ready_since")
    if ready is not None and (not finite(ready) or ready < 0 or not finite(duration) or ready > duration):
        raise ValueError("invalid readiness time")


def unpack(report: dict) -> list[dict]:
    if report.get("metadata", {}).get("columns") != COLUMNS:
        return []
    return [dict(zip(COLUMNS, row)) for row in report.get("samples", [])]


def rates(report: dict) -> list[dict]:
    """Counter resets and scrape gaps are gaps, not zero rates or interpolated work."""
    samples = unpack(report)
    maximum_gap = max(90, 3 * report.get("metadata", {}).get("interval", 10))
    result = []
    for previous, current in zip(samples, samples[1:]):
        elapsed = current["t"] - previous["t"]
        point = {**current, "download": None, "commit": None, "vct_share": None}
        if 0 < elapsed <= maximum_gap:
            for kind in ("download", "commit"):
                keys = [kind + "_zakura", kind + "_legacy"]
                if all(finite(previous[key]) and finite(current[key])
                       and current[key] >= previous[key] for key in keys):
                    point[kind] = sum(current[key] - previous[key] for key in keys) / elapsed / 1_000_000
            keys = ("vct_fast", "vct_legacy")
            if all(finite(previous[key]) and finite(current[key]) and current[key] >= previous[key] for key in keys):
                fast, legacy = (current[key] - previous[key] for key in keys)
                point["vct_share"] = fast / (fast + legacy) if fast + legacy > 0 else None
        result.append(point)
    return result


def boundaries(report: dict, settings: dict) -> list[tuple[int, str]]:
    samples = unpack(report)
    def observed(key):
        return next((int(row[key]) for row in samples if finite(row[key])), None)
    sapling = observed("sapling_height")
    if sapling is None:
        return []
    result = [(0, "Sprout"), (sapling, "Sapling")]
    start, end = settings["sandblast_start"], settings["sandblast_end"]
    if type(start) is not int or type(end) is not int or not sapling < start < end:
        raise ValueError("invalid Sandblast report window")
    result.extend([(start, "Sandblast"), (end + 1, "Post-Sandblast")])
    ironwood = observed("ironwood_height")
    if ironwood is not None and ironwood > end:
        result.append((ironwood, "Ironwood"))
    return result


def region_durations(report: dict, settings: dict) -> dict[str, float]:
    """Allocate monotonic elapsed time by committed height, keeping unknown time."""
    samples = unpack(report)
    regions = boundaries(report, settings)
    duration = report.get("metadata", {}).get("duration")
    if not samples or not finite(duration):
        return {}
    maximum_gap = max(90, 3 * report["metadata"].get("interval", 10))
    ready_since = report["metadata"].get("ready_since")
    end = min(duration, ready_since) if finite(ready_since) else duration
    totals = {"Startup / unobserved": min(end, samples[0]["t"])}
    for left, right in zip(samples, samples[1:]):
        start_t, end_t = left["t"], min(end, right["t"])
        seconds = max(0, end_t - start_t)
        if seconds == 0:
            continue
        h0, h1 = left["height"], right["height"]
        if (not regions or not finite(h0) or not finite(h1) or h1 < h0
                or right["t"] - left["t"] > maximum_gap):
            totals["Unobserved"] = totals.get("Unobserved", 0) + seconds
            continue
        # Crossings are interpolated only inside a valid sampled interval.
        cuts = [(start_t, next(name for height, name in reversed(regions) if h0 >= height))]
        if h1 > h0:
            cuts.extend((start_t + (right["t"] - start_t) * (height - h0) / (h1 - h0), name)
                        for height, name in regions if h0 < height <= h1)
        for index, (cut, name) in enumerate(cuts):
            next_cut = cuts[index + 1][0] if index + 1 < len(cuts) else end_t
            totals[name] = totals.get(name, 0) + max(0, min(end_t, next_cut) - cut)
    totals["Unobserved"] = totals.get("Unobserved", 0) + max(0, end - samples[-1]["t"])
    totals["Readiness / stop"] = max(0, duration - end)
    return {name: seconds for name, seconds in totals.items() if seconds > 0}


def comparison_key(report: dict) -> str | None:
    """Keep tuning, hardware, and network differences out of a shared baseline."""
    metadata = report.get("metadata", {})
    settings = metadata.get("settings", {})
    if not settings.get("available") or not settings.get("tuning_complete", True):
        return None
    floors = sorted({row["request_floor_bytes"] for row in unpack(report) if finite(row["request_floor_bytes"])})
    if metadata.get("mode") != "legacy" and len(floors) != 1:
        return None
    identity = {key: metadata.get(key) for key in ("mode", "settings", "host")}
    identity["request_floor_bytes"] = floors
    return hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
