"""How well peer scores predict downloads of announced blocks.

A node can use a peer's expected answer time to choose where to request an announced
block. It can also ask a second announcer while the first request is outstanding.
This module summarises the outcomes recorded in two trace tables:

- `legacy_peer_request` `block_request_finish` rows of first (`advertised`) and second
  (`hedge`) block requests: how far each download was from its peer's score, by block size,
  and how often second requests were sent, delivered and won;
- `delivery_score` `delivery_sample` rows, one per sample that changed a peer's score, which show
  what the scores are made of;
- `delivery_score` `block_missed` rows, one per announced block a peer did not deliver,
  matched with the next observed block from the same connection.

Everything is kept in memory for at most `MAX_HOURS`; a restarted profiler starts from the
last part of each table that it reads back.
"""

import collections
import threading
import time

from blocks import process_started

MAX_HOURS = 24
MAX_REQUESTS = 50_000
MAX_SAMPLES = 300_000
# Block sizes compared separately: the score comes mostly from pings, so larger blocks show how
# much transfer time it leaves out.
SIZE_BUCKETS = ((50_000, "under 50 KB"), (256 * 1024, "50-256 KB"), (None, "over 256 KB"))
UNKNOWN_SIZE = "size not traced"
SAMPLE_KINDS = ("ping", "transaction", "block", "carried_over")
MISSED_REQUESTS = ("first", "second")


def quantile(values, share):
    """The value `share` of the way through `values`, or `None` when there are none."""
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(share * len(ordered)))]


def spread(values):
    """The count and the 10th, 50th and 90th percentiles of `values`."""
    return {
        "n": len(values),
        "p10": quantile(values, 0.1),
        "p50": quantile(values, 0.5),
        "p90": quantile(values, 0.9),
    }


def size_bucket(block_bytes):
    if not isinstance(block_bytes, int):
        return UNKNOWN_SIZE
    for limit, label in SIZE_BUCKETS:
        if limit is None or block_bytes < limit:
            return label
    return UNKNOWN_SIZE


class BlockSources:
    """Remembers recent block requests and score samples, and summarises them on request."""

    def __init__(self, clock=time.time):
        self._clock = clock
        self._lock = threading.Lock()
        self._requests = collections.deque(maxlen=MAX_REQUESTS)
        self._samples = collections.deque(maxlen=MAX_SAMPLES)
        self._misses = collections.deque(maxlen=MAX_REQUESTS)

    def _when(self, row):
        """The unix time of a row, from its run's start and its `ts`, or now if unknown."""
        started = process_started(row.get("process_trace_id"))
        when = row.get("ts")
        if started is None or type(when) is not int:
            return self._clock()
        return started + when / 1e6

    def feed(self, row):
        """Take one parsed trace row; rows this module does not use are ignored."""
        event = row.get("event")
        if event == "block_request_finish" and row.get("route") in ("advertised", "hedge"):
            if type(row.get("elapsed_ms")) is not int or not isinstance(row.get("requested_hash"), str):
                return
            request = {
                "when": self._when(row),
                "block": (row.get("process_trace_id"), row["requested_hash"]),
                "second": row["route"] == "hedge",
                "delivered": row.get("result") == "available",
                "elapsed_ms": row["elapsed_ms"],
                "score_ms": row.get("score_ms"),
                "bytes": row.get("block_bytes"),
                "reason": row.get("hedge_reason"),
                "connection": (row.get("process_trace_id"), row.get("connection_id")),
            }
            with self._lock:
                self._requests.append(request)
        elif event == "delivery_sample":
            if row.get("kind") not in SAMPLE_KINDS or type(row.get("score_ms")) is not int:
                return
            sample = (
                self._when(row),
                (row.get("process_trace_id"), row.get("connection_id")),
                row["kind"],
                row.get("weight"),
            )
            with self._lock:
                self._samples.append(sample)
        elif event == "block_missed" and row.get("request") in MISSED_REQUESTS:
            miss = (self._when(row), (row.get("process_trace_id"), row.get("connection_id")), row["request"])
            with self._lock:
                self._misses.append(miss)

    def summary(self, hours):
        """What the last `hours` (at most `MAX_HOURS`) of requests and samples show."""
        hours = max(1, min(int(hours), MAX_HOURS))
        now = self._clock()
        since = now - hours * 3600
        with self._lock:
            requests = [r for r in self._requests if r["when"] >= since]
            samples = list(self._samples)
            misses = [m for m in self._misses if m[0] >= since]
            all_requests = list(self._requests)
        return {
            "hours": hours,
            "since": since,
            "requests": request_counts(requests),
            "prediction": prediction_errors(requests),
            "second_requests": second_request_outcomes(requests),
            "samples": sample_mix([s for s in samples if s[0] >= since]),
            "missed_blocks": missed_blocks(misses, all_requests, samples),
        }


def request_counts(requests):
    firsts = [r for r in requests if not r["second"]]
    return {
        "first": len(firsts),
        "first_unscored": sum(1 for r in firsts if r["score_ms"] is None),
        "first_undelivered": sum(1 for r in firsts if not r["delivered"]),
        "second": len(requests) - len(firsts),
    }


def prediction_errors(requests):
    """Actual minus expected download time, by first or second request and block size."""
    groups = collections.defaultdict(list)
    for r in requests:
        if r["delivered"] and type(r["score_ms"]) is int:
            groups[("second" if r["second"] else "first", size_bucket(r["bytes"]))].append(
                r["elapsed_ms"] - r["score_ms"]
            )
    order = [label for _, label in SIZE_BUCKETS] + [UNKNOWN_SIZE]
    return [
        {"request": request, "size": size, **spread(groups[(request, size)])}
        for request in ("first", "second")
        for size in order
        if groups[(request, size)]
    ]


def second_request_outcomes(requests):
    """Second requests by reason: how many were sent, delivered and arrived first."""
    firsts = {}
    by_reason = collections.defaultdict(lambda: {"sent": 0, "delivered": 0, "won": 0, "saved_ms": []})
    # Trace rows arrive in completion order. Walk estimated start times to pair a second
    # request with the latest preceding first request, including when a block is retried.
    for r in sorted(requests, key=lambda r: (r["when"] - r["elapsed_ms"] / 1000, r["second"])):
        if not r["second"]:
            firsts[r["block"]] = r
            continue
        first = firsts.pop(r["block"], None)
        outcome = by_reason[r["reason"] or "fixed delay"]
        outcome["sent"] += 1
        if not r["delivered"]:
            continue
        outcome["delivered"] += 1
        if first is None:
            continue
        if not first["delivered"]:
            outcome["won"] += 1
        elif r["when"] < first["when"]:
            outcome["won"] += 1
            outcome["saved_ms"].append(round((first["when"] - r["when"]) * 1000))
    return [
        {
            "reason": reason,
            "sent": o["sent"],
            "delivered": o["delivered"],
            "won": o["won"],
            "saved_p50_ms": quantile(o["saved_ms"], 0.5),
            "saved_p90_ms": quantile(o["saved_ms"], 0.9),
        }
        for reason, o in sorted(by_reason.items())
    ]


def sample_mix(samples):
    """How many samples of each kind changed scores, and how many samples a score rests on."""
    counts = collections.Counter(kind for _, _, kind, _ in samples)
    weights = [
        weight for _, _, kind, weight in samples
        if kind != "carried_over" and isinstance(weight, (int, float))
    ]
    return {
        "counts": {kind: counts.get(kind, 0) for kind in SAMPLE_KINDS},
        "weight": spread(weights),
    }


def missed_blocks(misses, requests, samples):
    """Count missed blocks and time to the next observed block on the same connection."""
    delivered = collections.defaultdict(list)
    for r in requests:
        if r["delivered"]:
            delivered[r["connection"]].append(r["when"])
    for when, connection, kind, *_ in samples:
        if kind == "block":
            delivered[connection].append(when)
    counts = collections.Counter(kind for _, _, kind in misses)
    delays = collections.defaultdict(list)
    for when, connection, kind in misses:
        again = min((t for t in delivered.get(connection, ()) if t > when), default=None)
        if again is not None:
            delays[kind].append(again - when)
    return {
        kind: {
            "count": counts[kind],
            "delivered_again": len(delays[kind]),
            "delivered_again_s": spread(delays[kind]),
        }
        for kind in MISSED_REQUESTS
    }
