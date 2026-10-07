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


class FeedTests(unittest.TestCase):
    def test_persists_sanitized_events_across_restart(self):
        import tempfile
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "events.sqlite3")
            raw = {"version": 1, "process": "1-123", "sequence": 1, "monotonic_ns": 10,
                   "unix_ms": 123, "event": {"event": "commit_start", "hash": "a" * 64,
                   "height": 123, "apply_token": 1, "peer": "secret"}}
            feed = n.EventFeed(path)
            self.assertEqual(feed.ingest([json.dumps(raw).encode(), b"bad"], 100), 1)
            self.assertEqual(feed.status()["rejected"], 1)
            feed.close()
            feed = n.EventFeed(path)
            try:
                detail = feed.block("a" * 64, 101)
                self.assertEqual(detail["attempts"][0]["result"], "incomplete")
                self.assertNotIn("secret", json.dumps(detail))
                self.assertNotIn("secret", feed.db.execute("SELECT body FROM node_block_events").fetchone()[0])
                self.assertEqual(feed.block("a" * 64, 86501)["attempts"], [])
            finally:
                feed.close()

    def test_socket_does_not_replace_existing_file(self):
        import tempfile
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "socket"
            path.write_text("keep")
            feed = n.EventFeed(":memory:", str(path))
            try:
                feed.start()
                self.assertEqual(path.read_text(), "keep")
                self.assertFalse(feed.status()["listening"])
            finally:
                feed.close()

    def test_real_datagram_receipt_and_shutdown(self):
        import socket
        import tempfile
        import time
        with tempfile.TemporaryDirectory() as directory:
            feed = n.EventFeed(":memory:", str(Path(directory) / "socket"))
            sender = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            try:
                feed.start()
                sender.sendto(b"{}", feed.socket_path)
                deadline = time.monotonic() + 2
                while feed.status()["rejected"] == 0 and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertEqual(feed.status()["rejected"], 1)
                self.assertTrue(feed.status()["listening"])
            finally:
                sender.close()
                feed.close()
            self.assertFalse(Path(feed.socket_path).exists())


class ArrivalTests(unittest.TestCase):
    def test_transport_and_local_durations_with_duplicates(self):
        def observation(kind, timestamp, **fields):
            return n.parse_block_event(json.dumps({"version": 1, "process": "1-123",
                "sequence": timestamp, "monotonic_ns": timestamp * 1_000_000, "unix_ms": timestamp,
                "event": {"event": kind, "hash": "a" * 64, **fields}}).encode())
        records = [observation("block_inventory_received", 1, transport="legacy"),
                   observation("block_body_received", 8, transport="zakura", first=True),
                   observation("block_body_received", 9, transport="legacy", first=False),
                   observation("commit_finish", 20, apply_token=1, height=123, result="committed"),
                   observation("block_relay_finished", 21, relay_attempt=1, succeeded=True)]
        self.assertTrue(all(records))
        result = n.arrival_summary(list(reversed(records)))[0]
        self.assertEqual(result["body_transport"], "zakura")
        self.assertEqual(result["inventory_to_body_ms"], 7)
        self.assertEqual(result["body_to_commit_ms"], 12)
        self.assertEqual(result["duplicate_bodies"], 1)
        self.assertEqual(result["relay_successes"], 1)
        records[3]["process"] = "2-124"
        self.assertIsNone(n.arrival_summary(records)[0]["body_to_commit_ms"])

    def test_relay_does_not_accept_non_boolean_success(self):
        raw = {"version": 1, "process": "1-123", "sequence": 1,
               "monotonic_ns": 1, "unix_ms": 1, "event": {"event": "block_relay_finished",
               "hash": "a" * 64, "relay_attempt": 1, "succeeded": "true"}}
        self.assertIsNone(n.parse_block_event(json.dumps(raw).encode()))


class NativeTests(unittest.TestCase):
    def test_rates_require_same_connection_and_fresh_ordered_samples(self):
        def packet(ns, rx, process="1-123", closed=False):
            return json.dumps({"version": 1, "process": process, "monotonic_ns": ns,
                "event": {"event": "native_connection", "connection": 1, "closed": closed,
                "rx_bytes": rx, "tx_bytes": rx * 2, "lost_packets": 0,
                "lost_bytes": 0, "rtt_ms": 10, "peer": "secret"}}).encode()
        feed = n.EventFeed(":memory:")
        try:
            feed.ingest([packet(1_000_000_000, 100)], 100)
            self.assertIsNone(feed.native(100)["connections"][0]["rx_bytes_ps"])
            feed.ingest([packet(6_000_000_000, 200)], 105)
            row = feed.native(105)["connections"][0]
            self.assertEqual(row["rx_bytes_ps"], 20)
            self.assertEqual(row["tx_bytes_ps"], 40)
            self.assertNotIn("secret", json.dumps(row))
            feed.ingest([packet(2_000_000_000, 150)], 106)
            self.assertEqual(feed.native(106)["connections"][0]["rx_bytes_ps"], 20)
            self.assertEqual(feed.native(121)["connections"], [])
            feed.ingest([packet(11_000_000_000, 300, closed=True)], 110)
            self.assertEqual(feed.native(110)["connections"], [])
            feed.ingest([packet(1_000_000_000, 500, process="2-124")], 111)
            self.assertIsNone(feed.native(111)["connections"][0]["rx_bytes_ps"])
        finally:
            feed.close()


class CryptoTests(unittest.TestCase):
    @staticmethod
    def packet(sequence=1, process="1-123", **fields):
        return json.dumps({"version": 1, "process": process, "sequence": sequence,
                           "monotonic_ns": sequence * 1000, "unix_ms": 123000,
                           "event": {"event": "crypto_batch", "verifier": "halo2",
                                     "unit": "actions", "mode": "batch", "success": True,
                                     "items": 3, "work_units": 12, "in_batch_wait_ms": 4,
                                     "scheduling_ms": 2, "execution_ms": 8,
                                     "private": "secret", **fields}}).encode()

    def test_schema_rejects_bad_units_counts_and_nonfinite_durations(self):
        parsed = n.parse_crypto_event(self.packet())
        self.assertEqual((parsed["items"], parsed["work_units"]), (3, 12))
        self.assertNotIn("secret", json.dumps(parsed))
        for fields in ({"unit": "proofs"}, {"items": 0}, {"items": True},
                       {"work_units": -1}, {"success": 1}, {"mode": "unknown"},
                       {"verifier": []}, {"execution_ms": float("nan")},
                       {"scheduling_ms": float("inf")}, {"in_batch_wait_ms": -1}):
            self.assertIsNone(n.parse_crypto_event(self.packet(**fields)), fields)

    def test_completion_deduplication_windows_failures_and_restart(self):
        import tempfile
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "crypto.sqlite3")
            feed = n.EventFeed(path)
            feed.ingest([self.packet(), self.packet()], 100)
            # Equal measured times are legitimate for distinct batches.
            feed.ingest([self.packet(2), self.packet(process="2-456", mode="fallback", success=False)], 110)
            self.assertEqual(len(feed.crypto(90, 120)["samples"]), 3)
            feed.close()
            feed = n.EventFeed(path)
            try:
                selected = feed.crypto(105, 110)
                self.assertEqual(len(selected["samples"]), 2)
                self.assertFalse(selected["limited"])
                self.assertFalse(selected["samples"][-1]["success"])
                self.assertNotIn("secret", json.dumps(selected))
                self.assertEqual(feed.crypto(111, 120)["samples"], [])
                feed.ingest([self.packet(3)], 86600)
                self.assertEqual(len(feed.crypto(0, 86600)["samples"]), 1)
            finally:
                feed.close()


class RequestTimingTests(unittest.TestCase):
    def test_duration_belongs_to_first_recorded_body_only(self):
        def body(sequence, duration):
            return n.parse_block_event(json.dumps({
                "version": 1, "process": "1-123", "sequence": sequence,
                "monotonic_ns": sequence * 1_000_000, "unix_ms": 1000,
                "event": {"event": "block_body_received", "hash": "a" * 64,
                          "first": sequence == 1, "transport": "zakura",
                          "request_queue_to_body_ms": duration}}).encode())
        first, duplicate = body(1, 12.5), body(2, 99)
        self.assertEqual(n.arrival_summary([duplicate, first])[0]["request_queue_to_body_ms"], 12.5)
        self.assertIsNone(n.arrival_summary([body(1, None), duplicate])[0]["request_queue_to_body_ms"])
        for bad in (-1, True, float("nan"), float("inf")):
            self.assertIsNone(body(1, bad))
