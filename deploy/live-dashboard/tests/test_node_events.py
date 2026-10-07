import importlib.util
import json
from pathlib import Path
import unittest
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

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


class StageTests(unittest.TestCase):
    @staticmethod
    def stage(kind, timestamp, token=1, process="1-123", name="initial_checks"):
        return n.parse_block_event(json.dumps({
            "version": 1, "process": process, "sequence": timestamp,
            "monotonic_ns": timestamp * 1_000_000, "unix_ms": timestamp,
            "event": {"event": kind, "hash": "a" * 64, "stage": name,
                      "stage_token": token, "success": False}}).encode())

    def test_out_of_order_duplicates_and_retries(self):
        start = self.stage("block_stage_started", 2)
        finish = self.stage("block_stage_finished", 5)
        rows = n.stage_summary([finish, start, finish,
                                self.stage("block_stage_started", 7, token=2)])
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["duration_ms"], 3)
        self.assertFalse(rows[0]["success"])
        self.assertFalse(rows[1]["complete"])
        self.assertIsNone(rows[1]["duration_ms"])

    def test_legacy_body_uses_only_successful_commit_stage_in_same_run(self):
        body = {"kind": "block_body_received", "process": "1-123", "sequence": 1,
                "monotonic_ns": 1_000_000, "unix_ms": 1, "transport": "legacy", "first": True}
        finish = self.stage("block_stage_finished", 5, name="verification_and_commit")
        self.assertIsNone(n.arrival_summary([body, finish])[0]["body_to_commit_ms"])
        finish["success"] = True
        self.assertEqual(n.arrival_summary([body, finish])[0]["body_to_commit_ms"], 4)
        finish["process"] = "2-123"
        self.assertIsNone(n.arrival_summary([body, finish])[0]["body_to_commit_ms"])
        finish["process"] = "1-123"
        for name in ("contextual_validation", "writer_queue", "finalized_write"):
            finish = self.stage("block_stage_finished", 5, name=name)
            self.assertIsNotNone(finish)
            finish["success"] = True
            self.assertIsNone(n.arrival_summary([body, finish])[0]["body_to_commit_ms"])

    def test_restart_and_wrong_stage_never_join(self):
        rows = n.stage_summary([self.stage("block_stage_started", 2),
                                self.stage("block_stage_finished", 5, process="2-123"),
                                self.stage("block_stage_finished", 5, name="shielded_anchors")])
        self.assertEqual(len(rows), 3)
        self.assertTrue(all(not row["complete"] for row in rows))
        self.assertIsNone(self.stage("block_stage_started", 2, name="unknown"))
        self.assertIsNone(self.stage("block_stage_started", 2, token=True))


class WaterfallTests(unittest.TestCase):
    def test_shared_axis_preserves_overlap_and_separate_id_namespaces(self):
        rows = [event("block_submit_queued", 1_000_000),
                event("commit_start", 3_000_000), event("commit_finish", 10_000_000),
                StageTests.stage("block_stage_started", 4),
                StageTests.stage("block_stage_finished", 8)]
        for kind, timestamp in (("block_relay_started", 9_000_000), ("block_relay_finished", 12_000_000)):
            rows.append({"process": "1-123", "hash": "a" * 64, "kind": kind,
                         "monotonic_ns": timestamp, "unix_ms": 123, "sequence": timestamp,
                         "relay_attempt": 1, "succeeded": True})
        run = n.block_timeline(list(reversed(rows)))[0]
        spans = run["spans"]
        self.assertEqual(run["extent_ms"], 11)
        self.assertEqual([(s["start_ms"], s["end_ms"], s["duration_ms"]) for s in spans],
                         [(0, 2, 2), (2, 9, 7), (3, 7, 4), (8, 11, 3)])
        self.assertEqual(spans[2]["outcome"], "failed")
        self.assertEqual(spans[3]["outcome"], "succeeded")

    def test_repeated_announcements_do_not_stretch_processing_axis(self):
        rows = [event("commit_start", 10_000_000), event("commit_finish", 40_000_000)]
        for timestamp, transport in ((1_000_000, "legacy"), (2_000_000, "zakura"), (9_000_000_000, "legacy")):
            rows.append({"process": "1-123", "hash": "a" * 64,
                         "kind": "block_inventory_received", "transport": transport,
                         "monotonic_ns": timestamp, "unix_ms": 123, "sequence": timestamp})
        run = n.block_timeline(rows)[0]
        self.assertEqual(run["extent_ms"], 39)
        self.assertEqual([p["count"] for p in run["points"]], [2, 1])
        self.assertEqual([p["offset_ms"] for p in run["points"]], [0, 1])

    def test_missing_backwards_and_restart_boundaries_stay_incomplete(self):
        rows = [event("commit_start", 9), event("commit_finish", 3),
                event("commit_finish", 20, process="2-456")]
        runs = n.block_timeline(rows)
        self.assertEqual(len(runs), 2)
        self.assertTrue(all(not span["complete"] for run in runs for span in run["spans"]))
        self.assertTrue(all(span["duration_ms"] is None for run in runs for span in run["spans"]))


class SocketActivationTests(unittest.TestCase):
    def test_socket_retains_datagrams_between_receivers(self):
        import os
        import socket
        import tempfile
        import time
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "events.sock")
            keeper = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            sender = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            keeper.bind(path)
            inherited = n.inherited_event_socket
            def activate(expected):
                with patch.dict(os.environ, {"LISTEN_PID": str(os.getpid()), "LISTEN_FDS": "1"}):
                    return inherited(expected, os.dup(keeper.fileno()))
            try:
                with patch.object(n, "inherited_event_socket", side_effect=activate):
                    first = n.EventFeed(":memory:", path)
                    first.start()
                    first.close()
                    self.assertTrue(Path(path).exists())
                    sender.sendto(CryptoTests.packet(1), path)
                    second = n.EventFeed(":memory:", path)
                    try:
                        second.start()
                        deadline = time.monotonic() + 2
                        while second.status()["received"] < 1 and time.monotonic() < deadline:
                            time.sleep(.01)
                        self.assertEqual(second.status()["received"], 1)
                        self.assertEqual(second.status()["rejected"], 0)
                    finally:
                        second.close()
                    self.assertTrue(Path(path).exists())
            finally:
                keeper.close()
                sender.close()


class DeliveryCoverageTests(unittest.TestCase):
    def test_out_of_order_sequences_do_not_invent_loss(self):
        feed = n.EventFeed(":memory:")
        try:
            def packet(sequence, failures=None, process="1-123"):
                row = json.loads(CryptoTests.packet(sequence, process))
                if failures is not None:
                    row["send_failures"] = failures
                return json.dumps(row).encode()
            feed.ingest([packet(100), packet(1)], 100)
            self.assertIsNone(feed.status()["reported_send_failures"])
            feed.ingest([packet(102, 3), packet(101, 2), packet(103, 3)], 101)
            self.assertEqual(feed.status()["reported_send_failures"], 3)
            feed.ingest([packet(1, 1, "2-456")], 102)
            self.assertEqual(feed.status()["reported_send_failures"], 4)
            self.assertEqual(feed.status()["reporting_runs"], 2)
            self.assertEqual(feed.status()["latest_run_send_failures"], 1)
            feed.ingest([packet(104, 4)], 103)
            self.assertEqual(feed.status()["latest_run_send_failures"], 1)
            feed.ingest([packet(1, None, "3-789")], 104)
            self.assertIsNone(feed.status()["latest_run_send_failures"])
            feed.ingest([packet(2, 0, "3-789")], 105)
            self.assertEqual(feed.status()["latest_run_send_failures"], 0)
        finally:
            feed.close()


class QueuePressureTests(unittest.TestCase):
    def test_bounded_private_queue_observations_and_missing_old_fields(self):
        row = {"version": 1, "process": "1-123", "monotonic_ns": 1,
               "event": {"event": "native_connection", "connection": 1, "closed": False,
                         "rx_bytes": 0, "tx_bytes": 0, "lost_packets": 0, "lost_bytes": 0,
                         "rtt_ms": None, "queues_limited": {"private": "secret"}}}
        self.assertIsNone(n.parse_network_event(json.dumps(row).encode())["queues_limited"])
        queue = {"stream": 1, "kind": 2, "kind_name": "gossip", "direction": "inbound",
                 "occupied_slots": 1, "capacity": 4, "private": "secret"}
        row["event"].update(queues=[queue], queues_limited=False)
        parsed = n.parse_network_event(json.dumps(row).encode())
        self.assertEqual(parsed["queues"][0]["occupied_slots"], 1)
        self.assertNotIn("secret", json.dumps(parsed))
        for field, value in (("occupied_slots", 5), ("capacity", 0), ("direction", "unknown"), ("kind_name", "private")):
            invalid = json.loads(json.dumps(row))
            invalid["event"]["queues"][0][field] = value
            self.assertIsNone(n.parse_network_event(json.dumps(invalid).encode()))
        row["event"]["queues"] = [queue] * 33
        self.assertIsNone(n.parse_network_event(json.dumps(row).encode()))
