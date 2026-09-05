"""Observe a drained process before closing and reconciling its trace files."""
import time

from getblocks_capture import IncompleteCapture, MESSAGE_KINDS, read_capture_metrics
from getblocks_lifetimes import FAMILIES

COUNTER_FAMILIES = set(FAMILIES.values()) | {
    "process_info", "sessions_started", "sessions_finished", "decoded_messages",
}


PHASES = {
    "serving_events": {"input_retained", "admission_reserved", "input_consumed", "committed", "request_bound"},
    "serving_query_events": {"read_started", "read_finished", "delivery_timeout", "delivery_cancelled", "claim_rejected"},
    "frame_events": {"queued", "write_started", "write_returned", "release_started", "release_finished"},
    "settlement_events": {"release_started", "release_finished"},
    "ownership_events": {"release_started", "release_finished"},
    "wait_events": {"started", "ready", "closed", "cancelled"},
}


def validate_counter_labels(samples):
    for family, label_pairs in samples:
        labels = dict(label_pairs)
        if family in PHASES:
            expected = {"phase"}
            stages = None
            if family == "ownership_events":
                stages = {"pending", "provisional"}
            elif family == "wait_events":
                stages = {"pending_input", "admission", "reactor_queue"}
            if stages:
                expected.add("stage")
            if (set(labels) != expected or labels.get("phase") not in PHASES[family]
                    or stages and labels.get("stage") not in stages):
                raise IncompleteCapture("unsupported lifecycle counter labels")
        elif family == "process_info":
            if set(labels) != {"process_trace_id"}:
                raise IncompleteCapture("unsupported process identity labels")
        elif family == "decoded_messages":
            if set(labels) != {"kind"} or labels["kind"] not in MESSAGE_KINDS:
                raise IncompleteCapture("unsupported decoded message labels")
        elif labels:
            raise IncompleteCapture("unexpected labels on session counter")


def unsettled(samples):
    """Return unfinished application owners; no instantaneous balance is inferred."""
    def count(name, **labels):
        return samples.get((name, tuple(sorted(labels.items()))), 0)

    validate_counter_labels(samples)
    failures = []
    identities = [(labels, value) for (name, labels), value in samples.items() if name == "process_info"]
    if (len(identities) != 1 or identities[0][1] != 1 or len(identities[0][0]) != 1
            or identities[0][0][0][0] != "process_trace_id"):
        failures.append("one process identity is required")
    if count("decoded_messages", kind="get_blocks") == 0:
        failures.append("no decoded GetBlocks requests")

    def equal(label, left, right):
        if left != right:
            failures.append(f"{label}: {left} != {right}")

    equal("sessions", count("sessions_started"), count("sessions_finished"))
    for stage in ["pending", "provisional"]:
        equal(stage + " release", count("ownership_events", stage=stage, phase="release_started"),
              count("ownership_events", stage=stage, phase="release_finished"))
    equal("retained inputs", count("serving_events", phase="input_retained"),
          count("ownership_events", stage="pending", phase="release_finished"))
    committed = count("serving_events", phase="committed")
    equal("provisional admissions", count("serving_events", phase="admission_reserved"),
          committed + count("ownership_events", stage="provisional", phase="release_finished"))
    # Conservatively reject commits without IDs, including request-ID exhaustion.
    equal("request binding", committed, count("serving_events", phase="request_bound"))
    equal("committed owners", committed, count("settlement_events", phase="release_finished"))
    equal("settlement interval", count("settlement_events", phase="release_started"),
          count("settlement_events", phase="release_finished"))
    equal("queued frames", count("frame_events", phase="queued"), count("frame_events", phase="release_finished"))
    equal("frame interval", count("frame_events", phase="release_started"), count("frame_events", phase="release_finished"))
    equal("state reads", count("serving_query_events", phase="read_started"), count("serving_query_events", phase="read_finished"))
    for stage in ["pending_input", "admission", "reactor_queue"]:
        equal(stage + " waits", count("wait_events", stage=stage, phase="started"),
              sum(count("wait_events", stage=stage, phase=end) for end in ["ready", "closed", "cancelled"]))
    return failures


def await_quiescence(scrape, timeout, *, clock=time.monotonic, sleep=time.sleep):
    """Require two equal drained samples, with at least two seconds between them.

    The caller must keep clients disconnected until the server stops. Equal
    counters cannot establish that operational boundary or replace file import.
    """
    if not 0 < timeout <= 600:
        raise ValueError("timeout must be positive and no greater than 600 seconds")
    deadline = clock() + timeout
    previous = None
    previous_finished = None
    failures = ["not sampled"]
    while clock() < deadline:
        started = clock()
        raw = scrape()
        finished = clock()
        current = read_capture_metrics(raw, COUNTER_FAMILIES)
        failures = unsettled(current)
        if (not failures and current == previous and previous_finished is not None
                and started - previous_finished >= 2 and finished < deadline):
            return raw, {
                "sample_start_monotonic_ns": int(started * 1_000_000_000),
                "sample_end_monotonic_ns": int(finished * 1_000_000_000),
                "stable_samples": 2,
                "minimum_sample_separation_seconds": 2,
                "quiescent_counters_verified": True,
                "capture_loss_verified": False,
            }
        previous = current if not failures else None
        previous_finished = finished
        sleep(min(2, max(0, deadline - clock())))
    raise IncompleteCapture("capture did not drain: " + "; ".join(failures or ["counters kept changing"]))
