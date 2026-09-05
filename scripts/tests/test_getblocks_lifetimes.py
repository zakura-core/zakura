"""Strict import rejects a whole episode when even one owner is incomplete."""
from collections import Counter
import hashlib
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).parents[1]))
from getblocks_capture import FINISH, MESSAGE, START, IncompleteCapture, import_arrivals
from getblocks_lifetimes import FAMILIES, import_completed_lifetimes


def row(event, timestamp, **fields):
    return dict(event=event, ts=timestamp, capture_version=1,
                process_trace_id="test-process", peer="peer:test", session_id=1,
                message_sequence=1, **fields)


def episode():
    blocks = [row(START, 0), row(MESSAGE, 1, kind="get_blocks", range_start=100, range_count=1)]
    blocks[0]["message_sequence"] = 0
    for phase, timestamp in [("input_retained", 2), ("admission_reserved", 3), ("input_consumed", 4)]:
        blocks.append(row("get_blocks_serving", timestamp, phase=phase))
    for phase in ["release_started", "release_finished"]:
        blocks.append(row("get_blocks_ownership", 4, phase=phase, stage="pending"))
    for phase in ["committed", "request_bound"]:
        blocks.append(row("get_blocks_serving", 5, phase=phase, request_id=1))
    queries = [row("get_blocks_query", timestamp, phase=phase, request_id=1)
               for phase, timestamp in [("read_started", 6), ("read_finished", 7)]]
    # Transport retains the frame beyond the request's final release.
    for phase, timestamp, state in [("queued", 8, "queued"), ("write_started", 9, "writing"),
                                    ("write_returned", 13, "returned"), ("release_started", 14, "returned"),
                                    ("release_finished", 15, "returned")]:
        blocks.append(row("get_blocks_frame", timestamp, phase=phase, request_id=1,
                          frame_sequence=0, payload_bytes=10, message_type=1, write_state=state))
    for phase, timestamp in [("release_started", 10), ("release_finished", 11)]:
        blocks.append(row("get_blocks_settlement", timestamp, phase=phase, request_id=1,
                          request_overhead=2, response_cap=20, transferred=10, unused_response_capacity=10))
    for phase in ["started", "ready"]:
        blocks.append(row("get_blocks_wait", 3, phase=phase, wait_sequence=0,
                          stage="reactor_queue", initial_bound="reactor_queue"))
    blocks.append(row(FINISH, 20))
    return blocks, queries


def encode(rows):
    return [(json.dumps(item) + "\n").encode() for item in rows]


def metrics_for(blocks, queries):
    counts = Counter()
    for item in blocks + queries:
        event = item["event"]
        labels = {}
        if event in FAMILIES:
            family = FAMILIES[event]
            labels["phase"] = item["phase"]
            if event in {"get_blocks_ownership", "get_blocks_wait"}:
                labels["stage"] = item["stage"]
        elif event in {START, FINISH}:
            family = "sessions_started" if event == START else "sessions_finished"
        elif event == MESSAGE:
            family, labels = "decoded_messages", {"kind": item["kind"]}
        else:
            continue
        counts[(family, tuple(sorted(labels.items())))] += 1
    counts[("process_info", (("process_trace_id", "test-process"),))] = 1
    return "".join("sync_block_capture_" + family +
                   ("{" + ",".join(f'{key}="{value}"' for key, value in labels) + "}" if labels else "") +
                   f" {count}\n" for (family, labels), count in counts.items()).encode()


def load(blocks, queries, metrics=None, boundary_change=None, clients_change=None):
    metrics = metrics if metrics is not None else metrics_for(blocks, queries)
    clients = dict(schema_version=1, no_new_clients=True, clients=[dict(MainPID=0, ActiveState="inactive")])
    if clients_change:
        clients.update(clients_change)
    clients_bytes = json.dumps(clients).encode()
    boundary = dict(schema_version=1, quiescent_counters_verified=True,
                    metrics_sha256=hashlib.sha256(metrics).hexdigest(),
                    clients_stopped_sha256=hashlib.sha256(clients_bytes).hexdigest())
    if boundary_change:
        boundary.update(boundary_change)
    lines = encode(blocks)
    arrivals = import_arrivals(lines, metrics)
    return import_completed_lifetimes(lines, encode(queries), arrivals, metrics,
                                      json.dumps(boundary).encode(), clients_bytes)


class LifetimeTests(unittest.TestCase):
    def test_frame_outlives_request_and_equal_timestamps_remain_explicit(self):
        result = load(*episode())
        request = result["requests"][0]
        self.assertEqual(request["settlement_us"], [10, 11])
        self.assertEqual(request["frames"][0]["release_us"], [14, 15])
        self.assertEqual(request["pending_release_us"], [4, 4])
        self.assertFalse(result["instantaneous_global_balances_reconstructed"])
        self.assertEqual(result["write_return_semantics"], "success_or_error_not_peer_receipt")
        self.assertNotIn("peer:test", json.dumps(result))

    def test_losing_any_lifecycle_row_rejects_episode(self):
        original, queries = episode()
        metrics = metrics_for(original, queries)
        for index, item in enumerate(original):
            if item["event"] not in FAMILIES:
                continue
            blocks = original[:index] + original[index + 1:]
            with self.subTest(index=index), self.assertRaises(IncompleteCapture):
                load(blocks, queries, metrics)
            # Even a matching counter scrape cannot make a partial profile valid.
            with self.subTest(index=index, recount=True), self.assertRaises(IncompleteCapture):
                load(blocks, queries)
        with self.assertRaises(IncompleteCapture):
            load(original, [])

    def test_unsupported_outcome_rejects_even_with_matching_counters(self):
        blocks, queries = episode()
        queries[-1]["phase"] = "delivery_cancelled"
        with self.assertRaises(IncompleteCapture):
            load(blocks, queries)

    def test_identity_timing_and_accounting_corruption_rejected(self):
        for event, phase, key, value in [
            ("get_blocks_query", "read_finished", "request_id", 2),
            ("get_blocks_query", "read_started", "ts", 4),
            ("get_blocks_query", "read_started", "process_trace_id", "other"),
            ("get_blocks_settlement", "release_finished", "transferred", 9),
            ("get_blocks_frame", "queued", "payload_bytes", True),
            ("get_blocks_frame", "write_returned", "write_state", "writing"),
            ("get_blocks_frame", "release_finished", "ts", 13),
            ("get_blocks_wait", "ready", "initial_bound", "unknown"),
        ]:
            blocks, queries = episode()
            target = next(item for item in blocks + queries if item["event"] == event and item.get("phase") == phase)
            target[key] = value
            with self.subTest(event=event, key=key), self.assertRaises(IncompleteCapture):
                load(blocks, queries)

    def test_incomplete_second_request_is_not_filtered_out(self):
        blocks, queries = episode()
        second_blocks, second_queries = episode()
        for item in second_blocks + second_queries:
            item["session_id"] = 2
            item["ts"] += 100
            if "request_id" in item:
                item["request_id"] = 2
        complete_blocks, complete_queries = blocks + second_blocks, queries + second_queries
        self.assertEqual(len(load(complete_blocks, complete_queries)["requests"]), 2)
        partial = blocks + [item for item in second_blocks if item["event"] not in FAMILIES]
        with self.assertRaises(IncompleteCapture):
            load(partial, queries)
        # Distinct decode sessions must not share a reactor request ID.
        for item in second_blocks + second_queries:
            if "request_id" in item:
                item["request_id"] = 1
        with self.assertRaises(IncompleteCapture):
            load(blocks + second_blocks, queries + second_queries)

    def test_boundary_proof_is_required(self):
        for change in [dict(schema_version=True), dict(quiescent_counters_verified=False),
                       dict(metrics_sha256="wrong"), dict(clients_stopped_sha256="wrong")]:
            with self.subTest(change=change), self.assertRaises(IncompleteCapture):
                load(*episode(), boundary_change=change)
        for change in [dict(schema_version=True), dict(no_new_clients=False), dict(clients=[]),
                       dict(clients=[dict(MainPID=42, ActiveState="active")])]:
            with self.subTest(change=change), self.assertRaises(IncompleteCapture):
                load(*episode(), clients_change=change)


if __name__ == "__main__":
    unittest.main()
