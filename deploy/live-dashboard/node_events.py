"""Bounded, validated local node events. Never expose raw datagrams to browsers."""

from collections import OrderedDict
import json
import re

MAX_EVENT_BYTES = 8192
MAX_ATTEMPTS = 4096
BLOCK_EVENTS = {"block_submit_queued", "commit_start", "commit_finish"}
RESULTS = {"committed", "duplicate", "rejected", "unavailable", "timed_out"}


def integer(value, maximum=2**64 - 1):
    return type(value) is int and 0 <= value <= maximum


def parse_block_event(data):
    """Allowlist public fields and require a complete process/attempt identity."""
    if len(data) > MAX_EVENT_BYTES:
        return None
    try:
        row = json.loads(data)
        if not isinstance(row, dict) or type(row.get("version")) is not int or row.get("version") != 1:
            return None
        process = row.get("process")
        if not isinstance(process, str) or not re.fullmatch(r"[0-9]{1,20}-[0-9]{1,30}", process):
            return None
        if not all(integer(row.get(key)) for key in ("sequence", "monotonic_ns", "unix_ms")):
            return None
        event = row.get("event")
        if not isinstance(event, dict) or event.get("event") not in BLOCK_EVENTS:
            return None
        block_hash = event.get("hash")
        if not isinstance(block_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", block_hash):
            return None
        if not integer(event.get("apply_token")) or not integer(event.get("height"), 2**32 - 1):
            return None
        kind = event["event"]
        result = event.get("result")
        if kind == "commit_finish" and result not in RESULTS:
            return None
        return {"process": process, "sequence": row["sequence"],
                "monotonic_ns": row["monotonic_ns"], "unix_ms": row["unix_ms"],
                "kind": kind, "hash": block_hash, "height": event["height"],
                "attempt": event["apply_token"], "result": result if kind == "commit_finish" else None}
    except (ValueError, TypeError, RecursionError):
        return None


class BlockAttempts:
    """Join only boundaries from the same process, hash, and apply token.

    Records may arrive out of order. Missing or backwards boundaries never turn
    into zero durations. Retention is bounded by capacity and receipt age.
    """
    def __init__(self, capacity=MAX_ATTEMPTS):
        if not 1 <= capacity <= MAX_ATTEMPTS:
            raise ValueError("invalid attempt capacity")
        self.capacity = capacity
        self.attempts = OrderedDict()

    def ingest(self, event, received_at):
        self.expire(received_at)
        key = (event["process"], event["hash"], event["attempt"])
        record = self.attempts.setdefault(key, {"hash": event["hash"], "height": event["height"],
                                               "received_at": received_at, "events": {}})
        # Retransmitted/duplicate records cannot change an already observed boundary.
        record["events"].setdefault(event["kind"], event)
        while len(self.attempts) > self.capacity:
            self.attempts.popitem(last=False)

    def expire(self, now):
        for key in list(self.attempts):
            if now - self.attempts[key]["received_at"] > 86400:
                del self.attempts[key]

    def block(self, block_hash, now):
        self.expire(now)
        result = []
        for record in self.attempts.values():
            if record["hash"] != block_hash:
                continue
            events = record["events"]
            def span(start, end):
                first, last = events.get(start), events.get(end)
                if first is None or last is None or last["monotonic_ns"] < first["monotonic_ns"]:
                    return None
                return (last["monotonic_ns"] - first["monotonic_ns"]) / 1_000_000
            finished = events.get("commit_finish")
            result.append({"height": record["height"], "observed_at": record["received_at"],
                           "queue_ms": span("block_submit_queued", "commit_start"),
                           "verify_and_commit_ms": span("commit_start", "commit_finish"),
                           "result": finished["result"] if finished else "incomplete",
                           "boundaries": sorted(events)})
        return result
