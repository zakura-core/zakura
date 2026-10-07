import importlib.util
import json
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("node_events", Path(__file__).resolve().parents[1] / "node_events.py")
n = importlib.util.module_from_spec(spec)
spec.loader.exec_module(n)


def event(kind, timestamp, process="1-123", attempt=1):
    raw = {"version": 1, "process": process, "sequence": timestamp, "monotonic_ns": timestamp,
           "unix_ms": 123, "event": {"event": kind, "hash": "a" * 64, "height": 123,
           "apply_token": attempt, "result": "committed", "peer": "private", "error": "private"}}
    return n.parse_block_event(json.dumps(raw).encode())


class EventTests(unittest.TestCase):
    def test_public_allowlist_and_bad_inputs(self):
        good = event("commit_start", 1)
        self.assertNotIn("private", json.dumps(good))
        for value in (b"null", b"[]", b"{}", b"not json", b"x" * 8193):
            self.assertIsNone(n.parse_block_event(value))

    def test_out_of_order_events_join_with_monotonic_durations(self):
        store = n.BlockAttempts()
        store.ingest(event("commit_finish", 9_000_000), 100)
        store.ingest(event("block_submit_queued", 1_000_000), 101)
        store.ingest(event("commit_start", 3_000_000), 102)
        record = store.block("a" * 64, 103)[0]
        self.assertEqual(record["queue_ms"], 2)
        self.assertEqual(record["verify_and_commit_ms"], 6)
        self.assertEqual(record["result"], "committed")

    def test_missing_restart_and_retry_boundaries_do_not_join(self):
        store = n.BlockAttempts()
        store.ingest(event("commit_start", 1), 100)
        store.ingest(event("commit_finish", 9, process="2-124"), 100)
        store.ingest(event("commit_finish", 9, attempt=2), 100)
        self.assertEqual(len(store.block("a" * 64, 100)), 3)
        self.assertTrue(all(r["verify_and_commit_ms"] is None for r in store.block("a" * 64, 100)))

    def test_duplicates_backwards_time_and_retention(self):
        store = n.BlockAttempts(capacity=2)
        store.ingest(event("commit_start", 10), 100)
        store.ingest(event("commit_start", 1), 100)
        store.ingest(event("commit_finish", 5), 100)
        self.assertIsNone(store.block("a" * 64, 100)[0]["verify_and_commit_ms"])
        store.ingest(event("commit_start", 10, attempt=2), 100)
        store.ingest(event("commit_start", 10, attempt=3), 100)
        self.assertEqual(len(store.attempts), 2)
        self.assertEqual(store.block("a" * 64, 86601), [])
