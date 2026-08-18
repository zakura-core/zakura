#!/usr/bin/env python3
"""Correlate one block across Zakura JSONL trace directories.

The report joins native and legacy observations by block hash. Its origin can be
the mining node's `mined_block_broadcast_started` event or the earliest discovery
across the observed nodes. All timestamps are producer wall-clock values, so
operators must synchronize node clocks before interpreting cross-node offsets.
"""

import argparse
import hashlib
import json
import sys
from collections import defaultdict
from pathlib import Path


TRACE_FILES = {
    "block_propagation.jsonl",
    "header_sync.jsonl",
    "block_sync.jsonl",
    "commit_state.jsonl",
    "legacy_sync.jsonl",
}

PHASE_ORDER = {
    "broadcast_start": 0,
    "announcement": 1,
    "body_received": 2,
    "commit": 3,
    "broadcast_finish": 4,
    "error": 5,
}

ORIGIN_MODES = ("broadcast", "first-discovery")


def parse_named_value(raw):
    """Parse NAME=VALUE command-line values."""
    name, separator, value = raw.partition("=")
    if not separator or not name or not value:
        raise argparse.ArgumentTypeError("expected NAME=VALUE")
    return name, value


def normalize_hash(value):
    """Normalize a displayed block hash for case-insensitive matching."""
    return str(value or "").removeprefix("0x").lower()


def validate_block_hash(value):
    """Return a normalized 32-byte hash or raise a user-facing error."""
    normalized = normalize_hash(value)
    if len(normalized) != 64:
        raise ValueError("block hash must contain exactly 32 bytes of hexadecimal data")
    try:
        bytes.fromhex(normalized)
    except ValueError as error:
        raise ValueError("block hash must contain exactly 32 bytes of hexadecimal data") from error
    return normalized


def native_peer_label(node_id):
    """Return the privacy-preserving trace label for a native Zakura node ID."""
    try:
        raw = bytes.fromhex(node_id)
    except ValueError as error:
        raise ValueError(f"invalid native node ID {node_id!r}") from error
    if len(raw) != 32:
        raise ValueError(f"native node ID must contain exactly 32 bytes: {node_id!r}")
    digest = hashlib.blake2b(
        raw, digest_size=8, person=b"zakura-peer-lbl"
    ).hexdigest()
    return f"peer:{digest}"


def load_rows(trace_dir):
    """Load relevant JSONL rows, retaining malformed-row diagnostics."""
    rows = []
    warnings = []
    path = Path(trace_dir)
    if not path.exists():
        return rows, [f"trace directory does not exist: {path}"]

    for trace_file in sorted(
        candidate for candidate in path.rglob("*.jsonl") if candidate.name in TRACE_FILES
    ):
        with trace_file.open(encoding="utf-8") as source:
            for line_number, line in enumerate(source, 1):
                try:
                    row = json.loads(line)
                except json.JSONDecodeError as error:
                    warnings.append(f"{trace_file}:{line_number}: invalid JSON: {error}")
                    continue
                if not isinstance(row, dict):
                    warnings.append(
                        f"{trace_file}:{line_number}: JSON row must be an object"
                    )
                    continue
                row["_trace_file"] = trace_file.name
                rows.append(row)
    return rows, warnings


def event_for_row(node, row, block_hash):
    """Project one raw trace row into a propagation event, if relevant."""
    if normalize_hash(row.get("hash")) != block_hash:
        if not (
            row.get("event") == "header_status_received"
            and normalize_hash(row.get("selected_tip_hash")) == block_hash
        ):
            return None

    wall_ts = row.get("wall_ts_unix_us")
    if not isinstance(wall_ts, int):
        return {"warning": f"{node}:{row.get('event', 'unknown')} lacks wall_ts_unix_us"}

    event = row.get("event")
    projected = {
        "node": node,
        "wall_ts_unix_us": wall_ts,
        "process_trace_id": row.get("process_trace_id"),
        "raw_event": event,
        "height": row.get("height"),
        "source": row.get("source") or row.get("peer"),
        "result": row.get("result"),
        "reason": row.get("reason") or row.get("error"),
        "disposition": row.get("disposition"),
        "trace_file": row["_trace_file"],
    }

    if event == "mined_block_broadcast_started":
        projected.update(phase="broadcast_start", transport="local")
    elif event == "mined_block_broadcast_finished":
        projected.update(
            phase="broadcast_finish" if row.get("result") == "ok" else "error",
            transport="local",
        )
    elif event == "block_announced":
        projected.update(phase="announcement", transport=row.get("transport", "unknown"))
    elif event == "legacy_block_downloaded":
        projected.update(phase="body_received", transport="legacy")
    elif event == "legacy_block_finished":
        committed = row.get("result") == "committed" or row.get("reason") == "already present"
        projected.update(
            phase="commit" if committed else "error",
            transport="legacy",
        )
    elif event == "header_status_received":
        projected.update(
            phase="announcement",
            transport="native",
            height=row.get("selected_tip_height"),
            source=row.get("peer"),
        )
    elif event == "block_body_received":
        projected.update(phase="body_received", transport="native")
    elif event == "commit_finish":
        projected.update(
            phase="commit"
            if row.get("result") in {"committed", "duplicate"}
            else "error",
            transport="native",
        )
    elif event == "block_downloaded":
        projected.update(
            phase="body_received", transport="legacy_sync", source=row.get("peer")
        )
    elif event == "block_finish":
        projected.update(
            phase="commit" if row.get("result") == "verified" else "error",
            transport="legacy_sync",
        )
    else:
        return None

    return projected


def source_node(source, native_nodes, legacy_nodes):
    """Resolve a trace peer label to a managed node name when possible."""
    if not source:
        return None
    normalized = str(source)
    if normalized.startswith("native:"):
        normalized = normalized.removeprefix("native:")
    if normalized in native_nodes:
        return native_nodes[normalized]

    if normalized in legacy_nodes:
        return legacy_nodes[normalized]
    legacy_source = normalized.removeprefix("legacy:")
    for address, node in legacy_nodes.items():
        if legacy_source == address or legacy_source.startswith(f"{address}:"):
            return node
    return None


def build_report(
    trace_dirs,
    block_hash,
    native_node_ids=None,
    legacy_node_addresses=None,
    clock_uncertainty_us=100_000,
    origin="broadcast",
):
    """Load, correlate, and summarize propagation events for `block_hash`."""
    if origin not in ORIGIN_MODES:
        raise ValueError(f"origin must be one of: {', '.join(ORIGIN_MODES)}")

    block_hash = validate_block_hash(block_hash)
    native_nodes = {
        native_peer_label(node_id): name
        for name, node_id in (native_node_ids or {}).items()
    }
    legacy_nodes = {
        address: name for name, address in (legacy_node_addresses or {}).items()
    }

    events = []
    warnings = []
    process_ids = defaultdict(set)
    for node, trace_dir in trace_dirs.items():
        rows, load_warnings = load_rows(trace_dir)
        warnings.extend(load_warnings)
        for row in rows:
            embedded_node = row.get("node")
            if embedded_node and embedded_node != node:
                warnings.append(
                    f"{node}: skipped row labeled for different node {embedded_node!r}"
                )
                continue
            event = event_for_row(node, row, block_hash)
            if event and "warning" in event:
                if row.get("process_trace_id"):
                    process_ids[node].add(row["process_trace_id"])
                warnings.append(event["warning"])
            elif event:
                if row.get("process_trace_id"):
                    process_ids[node].add(row["process_trace_id"])
                event["source_node"] = source_node(
                    event.get("source"), native_nodes, legacy_nodes
                )
                events.append(event)
                if event.get("disposition") in {"queue_full", "source_queue_full"}:
                    warnings.append(
                        f"{node}: block announcement dropped with "
                        f"{event['disposition']}"
                    )

    native_events = {"header_status_received", "block_body_received", "commit_finish"}
    canonical_native_events = {
        (event["node"], event["raw_event"])
        for event in events
        if event["trace_file"] == "block_propagation.jsonl"
        and event["raw_event"] in native_events
    }
    events = [
        event
        for event in events
        if event["trace_file"] == "block_propagation.jsonl"
        or (event["node"], event["raw_event"]) not in canonical_native_events
    ]

    events.sort(
        key=lambda event: (
            event["wall_ts_unix_us"],
            PHASE_ORDER[event["phase"]],
            event["node"],
        )
    )
    discovery_phases = {"announcement", "body_received", "commit"}
    if origin == "broadcast":
        origins = [event for event in events if event["phase"] == "broadcast_start"]
        selected_origin = origins[0] if origins else None
        if not selected_origin:
            warnings.append("mining-node broadcast origin is missing")
        if len(origins) > 1:
            warnings.append(
                "multiple mining-node broadcast origins were found; using the earliest"
            )
    else:
        selected_origin = next(
            (event for event in events if event["phase"] in discovery_phases),
            None,
        )
        if not selected_origin:
            warnings.append("first-discovery origin is missing")

    origin_ts = (
        selected_origin["wall_ts_unix_us"] if selected_origin is not None else None
    )

    for node, ids in sorted(process_ids.items()):
        if len(ids) > 1:
            warnings.append(f"{node}: trace rows span {len(ids)} process instances")

    for event in events:
        event["offset_us"] = (
            event["wall_ts_unix_us"] - origin_ts if origin_ts is not None else None
        )
        warn_for_negative_offset = (
            origin == "broadcast" or event["phase"] in discovery_phases
        )
        if (
            warn_for_negative_offset
            and event["offset_us"] is not None
            and event["offset_us"] < -clock_uncertainty_us
        ):
            warnings.append(
                f"{event['node']}:{event['raw_event']} precedes t0 by "
                f"{-event['offset_us']}us"
            )

    per_node = {}
    duplicates = []
    for node in trace_dirs:
        node_events = [event for event in events if event["node"] == node]
        node_summary = {}
        for phase in ("announcement", "body_received", "commit"):
            phase_events = [event for event in node_events if event["phase"] == phase]
            if phase_events:
                node_summary[phase] = phase_events[0]
            if len(phase_events) > 1:
                duplicates.append(
                    {
                        "node": node,
                        "phase": phase,
                        "count": len(phase_events),
                        "paths": sorted(
                            {
                                f"{event['transport']}:{event.get('source') or 'unknown'}"
                                for event in phase_events
                            }
                        ),
                    }
                )
        node_summary["missing"] = [
            phase
            for phase in ("announcement", "body_received", "commit")
            if phase not in node_summary
        ]
        node_summary["errors"] = [
            event for event in node_events if event["phase"] == "error"
        ]
        discovery = next(
            (
                event
                for event in node_events
                if event["phase"] in {"announcement", "body_received", "commit"}
            ),
            None,
        )
        node_summary["discovery"] = discovery
        body_received = node_summary.get("body_received")
        commit = node_summary.get("commit")
        node_summary["discovery_to_body_us"] = (
            body_received["wall_ts_unix_us"] - discovery["wall_ts_unix_us"]
            if discovery is not None and body_received is not None
            else None
        )
        node_summary["discovery_to_commit_us"] = (
            commit["wall_ts_unix_us"] - discovery["wall_ts_unix_us"]
            if discovery is not None and commit is not None
            else None
        )
        for error_event in node_summary["errors"]:
            warnings.append(
                f"{node}:{error_event['raw_event']} reported "
                f"{error_event.get('result') or 'error'}"
            )
        per_node[node] = node_summary

    edges = []
    seen_edges = set()
    for event in events:
        source = event.get("source_node")
        edge = (source, event["node"], event["transport"])
        if source and source != event["node"] and edge not in seen_edges:
            seen_edges.add(edge)
            edges.append(
                {
                    "source": source,
                    "destination": event["node"],
                    "transport": event["transport"],
                    "first_observed_us": event["offset_us"],
                }
            )

    def spread(phase):
        timestamps = [
            summary[phase]["wall_ts_unix_us"]
            for summary in per_node.values()
            if phase in summary
        ]
        return max(timestamps) - min(timestamps) if len(timestamps) >= 2 else None

    return {
        "block_hash": block_hash,
        "clock_uncertainty_us": clock_uncertainty_us,
        "origin_kind": origin.replace("-", "_"),
        "origin": selected_origin,
        "events": events,
        "nodes": per_node,
        "edges": edges,
        "duplicates": duplicates,
        "announcement_spread_us": spread("announcement"),
        "body_receive_spread_us": spread("body_received"),
        "commit_spread_us": spread("commit"),
        "warnings": sorted(set(warnings)),
    }


def format_offset(event):
    """Format an event's t0-relative offset for a compact report."""
    if not event or event.get("offset_us") is None:
        return "missing"
    offset_us = event["offset_us"]
    return f"{offset_us / 1000:.3f} ms"


def format_duration(duration_us):
    """Format an optional duration in microseconds."""
    if duration_us is None:
        return "missing"
    return f"{duration_us / 1000:.3f} ms"


def render_markdown(report):
    """Render a concise human-readable propagation report."""
    lines = [
        "# Block propagation report",
        "",
        f"- Block: `{report['block_hash']}`",
        f"- Clock uncertainty assumption: ±{report['clock_uncertainty_us'] / 1000:.3f} ms",
    ]
    origin = report["origin"]
    if origin:
        if report["origin_kind"] == "first_discovery":
            lines.append(
                f"- Origin: first discovery on `{origin['node']}` via "
                f"`{origin['transport']}` at `{origin['wall_ts_unix_us']}` Unix µs"
            )
        else:
            lines.append(
                f"- Origin: mining broadcast on `{origin['node']}` at "
                f"`{origin['wall_ts_unix_us']}` Unix µs"
            )
    else:
        lines.append("- Origin: missing")

    lines.extend(
        [
            "",
            "| Node | First announcement | First body | Commit | "
            "Local discovery → body | Local discovery → commit | "
            "First transport/source |",
            "| --- | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for node, summary in report["nodes"].items():
        first = summary.get("announcement") or summary.get("body_received")
        path = (
            f"{first['transport']} / {first.get('source') or 'unknown'}"
            if first
            else "missing"
        )
        lines.append(
            f"| {node} | {format_offset(summary.get('announcement'))} | "
            f"{format_offset(summary.get('body_received'))} | "
            f"{format_offset(summary.get('commit'))} | "
            f"{format_duration(summary['discovery_to_body_us'])} | "
            f"{format_duration(summary['discovery_to_commit_us'])} | {path} |"
        )

    lines.extend(["", "## Spread"])
    for label, field in (
        ("Announcement", "announcement_spread_us"),
        ("Body receive", "body_receive_spread_us"),
        ("Commit", "commit_spread_us"),
    ):
        value = report[field]
        lines.append(f"- {label}: {value / 1000:.3f} ms" if value is not None else f"- {label}: unavailable")

    if report["edges"]:
        lines.extend(["", "## Inferred managed-node edges"])
        for edge in report["edges"]:
            observed = edge["first_observed_us"]
            observed_label = (
                f"{observed / 1000:.3f} ms" if observed is not None else "unknown offset"
            )
            lines.append(
                f"- `{edge['source']}` → `{edge['destination']}` via "
                f"`{edge['transport']}` at {observed_label}"
            )

    error_events = [
        error
        for summary in report["nodes"].values()
        for error in summary.get("errors", [])
    ]
    if error_events:
        lines.extend(["", "## Propagation errors"])
        for error in error_events:
            reason = error.get("reason") or error.get("result") or "unknown"
            lines.append(
                f"- `{error['node']}` `{error['raw_event']}`: {reason}"
            )

    if report["duplicates"]:
        lines.extend(["", "## Duplicate paths"])
        for duplicate in report["duplicates"]:
            lines.append(
                f"- `{duplicate['node']}` {duplicate['phase']}: "
                f"{duplicate['count']} events ({', '.join(duplicate['paths'])})"
            )

    if report["warnings"]:
        lines.extend(["", "## Warnings"])
        lines.extend(f"- {warning}" for warning in report["warnings"])

    return "\n".join(lines) + "\n"


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hash", required=True, dest="block_hash")
    parser.add_argument(
        "--trace-dir",
        action="append",
        required=True,
        type=parse_named_value,
        metavar="NODE=PATH",
    )
    parser.add_argument(
        "--native-node",
        action="append",
        default=[],
        type=parse_named_value,
        metavar="NODE=HEX_ID",
    )
    parser.add_argument(
        "--legacy-node",
        action="append",
        default=[],
        type=parse_named_value,
        metavar="NODE=IP",
    )
    parser.add_argument("--clock-uncertainty-ms", type=float, default=100.0)
    parser.add_argument("--origin", choices=ORIGIN_MODES, default="broadcast")
    parser.add_argument("--json-out", type=Path)
    parser.add_argument("--markdown-out", type=Path)
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        report = build_report(
            dict(args.trace_dir),
            args.block_hash,
            dict(args.native_node),
            dict(args.legacy_node),
            max(0, round(args.clock_uncertainty_ms * 1000)),
            args.origin,
        )
    except ValueError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    markdown = render_markdown(report)
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(report, indent=2) + "\n")
    if args.markdown_out:
        args.markdown_out.parent.mkdir(parents=True, exist_ok=True)
        args.markdown_out.write_text(markdown)
    print(markdown, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
