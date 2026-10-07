"""Sanitized transaction lifecycle events and measured, per-attempt durations."""

from collections import Counter
import json
import re

PHASES = {"queued", "received", "verification_started", "verified", "admission_started", "admitted",
          "relay_started", "relay_succeeded", "relay_failed", "mined", "expired", "evicted", "rejected"}
REASONS = {"already_mined", "state_unavailable", "download_failed", "cancelled", "policy",
           "verification", "timeout", "duplicate", "queue_full", "disabled", "capacity",
           "fee_below_capacity_floor", "conflicting_effects", "missing_output", "too_many_ancestors",
           "package_limit", "expiry_height", "ancestor_removed"}


def parse_transaction_event(data):
    if len(data) > 8192:
        return None
    try:
        row = json.loads(data)
        if not isinstance(row, dict) or type(row.get("version")) is not int or row["version"] != 1:
            return None
        if not isinstance(row.get("process"), str) or not re.fullmatch(r"[0-9]{1,20}-[0-9]{1,30}", row["process"]):
            return None
        if not all(type(row.get(key)) is int and 0 <= row[key] < 2**64 for key in ("sequence", "monotonic_ns", "unix_ms")):
            return None
        event = row.get("event")
        if not isinstance(event, dict) or event.get("event") != "transaction_lifecycle":
            return None
        token = event.get("transaction")
        if not isinstance(token, str) or not re.fullmatch(r"[0-9a-f]{32}", token):
            return None
        if event.get("phase") not in PHASES or (event.get("reason") is not None and event["reason"] not in REASONS):
            return None
        if event["phase"] in ("rejected", "expired", "evicted") and event.get("reason") is None:
            return None
        attempt = event.get("attempt")
        if attempt is not None and (type(attempt) is not int or not 0 < attempt < 2**64):
            return None
        return {"attempt": attempt, **{key: row[key] for key in ("process", "sequence", "monotonic_ns", "unix_ms")},
                **{key: event.get(key) for key in ("transaction", "phase", "reason")}}
    except (TypeError, ValueError, RecursionError):
        return None


def summarize_transactions(events, start, end):
    """Only join observed boundaries in the same run, exact token, and queue cycle."""
    counts, reasons = Counter(), Counter()
    pending, samples = {}, []
    spans = {"received": ("queued", "body_wait_ms"),
             "verified": ("verification_started", "verification_ms"),
             "relay_succeeded": ("relay_started", "relay_ms"),
             "relay_failed": ("relay_started", "relay_ms"),
             "admitted": ("admission_started", "admission_ms"),
             "mined": ("admitted", "residence_ms"),
             "expired": ("admitted", "residence_ms"),
             "evicted": ("admitted", "residence_ms")}
    seen = set()
    for event in sorted(events, key=lambda row: (row["process"], row["monotonic_ns"], row["sequence"])):
        identity = (event["process"], event["sequence"])
        if identity in seen:
            continue
        seen.add(identity)
        key = (event["process"], event["transaction"], event.get("attempt"))
        phase = event["phase"]
        if phase == "queued":
            pending[key] = {}
        cycle = pending.setdefault(key, {})
        selected = start <= event["at"] <= end
        if selected:
            counts[phase] += 1
            if event["reason"]:
                reasons[event["reason"]] += 1
        if phase in spans and event.get("attempt") is not None:
            first_phase, metric = spans[phase]
            first = cycle.pop(first_phase, None)
            if selected and first is not None and first["monotonic_ns"] <= event["monotonic_ns"]:
                samples.append({"t": event["at"], "metric": metric,
                                "value": (event["monotonic_ns"] - first["monotonic_ns"]) / 1e6,
                                "outcome": phase})
        if phase == "relay_started":
            cycle[phase] = event
        else:
            cycle.setdefault(phase, event)
        if phase in ("rejected", "expired", "evicted", "mined"):
            pending.pop(key, None)
    return {"counts": dict(counts), "reasons": dict(reasons), "timings": sorted(samples, key=lambda row: row["t"])}
