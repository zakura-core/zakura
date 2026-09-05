import importlib.util
import json
from pathlib import Path
import unittest


SCRIPT = Path(__file__).parents[1] / "import-getblocks-arrivals.py"
SPEC = importlib.util.spec_from_file_location("getblocks_arrivals", SCRIPT)
arrivals = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(arrivals)


def row(event, sequence, timestamp, **fields):
    return {
        "event": event,
        "process_trace_id": "one-process",
        "peer": "peer:example",
        "session_id": 1,
        "capture_version": 1,
        "message_sequence": sequence,
        "ts": timestamp,
        **fields,
    }


def episode():
    return [
        row(arrivals.START, 0, 10),
        row(arrivals.MESSAGE, 1, 11, kind="status"),
        row(arrivals.MESSAGE, 2, 12, kind="get_blocks", range_start=100, range_count=1),
        row(arrivals.MESSAGE, 3, 12, kind="get_blocks", range_start=100, range_count=1),
        row(arrivals.FINISH, 3, 20),
    ]


def load(rows, metrics=None):
    return arrivals.import_arrivals(((json.dumps(item) + "\n").encode() for item in rows), metrics)


METRICS = b'''# TYPE sync_block_capture_sessions_started counter
sync_block_capture_process_info{process_trace_id="one-process"} 1
sync_block_capture_sessions_started 1
sync_block_capture_sessions_finished 1
sync_block_capture_decoded_messages{kind="status"} 1
sync_block_capture_decoded_messages{kind="get_blocks"} 2
'''


class ImportTests(unittest.TestCase):
    def test_duplicate_ranges_remain_distinct_and_reconnect_keeps_identity(self):
        first = episode()
        second = [dict(item, session_id=2, ts=item["ts"] + 100) for item in episode()]
        result = load(first + second)
        self.assertEqual(result["peers"], 1)
        self.assertEqual([item["session"] for item in result["requests"]], [0, 0, 1, 1])
        self.assertEqual([item["message_sequence"] for item in result["requests"]], [2, 3, 2, 3])
        self.assertEqual(result["requests"][0]["decoded_us"], result["requests"][1]["decoded_us"])
        self.assertNotIn("peer:example", json.dumps(result))
        self.assertFalse(result["service_lifecycles_complete"])
        self.assertFalse(result["capture_loss_verified"])

    def test_losing_any_decode_or_boundary_invalidates_the_episode(self):
        for missing in range(len(episode())):
            with self.subTest(missing=missing):
                rows = episode()
                rows.pop(missing)
                with self.assertRaises(arrivals.IncompleteCapture):
                    load(rows)

    def test_duplicate_message_and_mixed_processes_are_rejected(self):
        rows = episode()
        rows.insert(3, rows[2])
        with self.assertRaises(arrivals.IncompleteCapture):
            load(rows)
        rows = episode()
        rows[2]["process_trace_id"] = "restart"
        with self.assertRaises(arrivals.IncompleteCapture):
            load(rows)

    def test_invalid_versions_fields_and_clock_are_rejected(self):
        for index, key, value in [
            (0, "capture_version", 2),
            (0, "message_sequence", 1),
            (2, "session_id", 9),
            (2, "range_count", True),
            (2, "range_start", -1),
            (2, "ts", 9),
            (4, "message_sequence", 4),
        ]:
            with self.subTest(key=key, value=value):
                rows = episode()
                rows[index][key] = value
                with self.assertRaises(arrivals.IncompleteCapture):
                    load(rows)

    def test_legacy_kind_only_trace_is_not_a_valid_empty_capture(self):
        with self.assertRaises(arrivals.IncompleteCapture):
            load([{"event": arrivals.MESSAGE, "kind": "get_blocks"}])

    def test_reconciliation_checks_totals_without_claiming_service_coverage(self):
        result = load(episode(), METRICS)
        self.assertTrue(result["decode_totals_reconciled"])
        self.assertFalse(result["capture_loss_verified"])
        self.assertFalse(result["service_lifecycles_complete"])
        self.assertEqual(result["final_metrics"]["messages_by_kind"], {"status": 1, "get_blocks": 2})

    def test_wholly_missing_session_is_detected_by_independent_totals(self):
        second = [dict(item, session_id=2, ts=item["ts"] + 100) for item in episode()]
        totals = METRICS.replace(b"started 1", b"started 2").replace(b"finished 1", b"finished 2")
        totals = totals.replace(b'kind="status"} 1', b'kind="status"} 2')
        totals = totals.replace(b'kind="get_blocks"} 2', b'kind="get_blocks"} 4')
        self.assertTrue(load(episode() + second, totals)["decode_totals_reconciled"])
        with self.assertRaises(arrivals.IncompleteCapture):
            load(episode(), totals)

    def test_invalid_or_mismatched_scrapes_are_rejected(self):
        for metrics in [
            b"",
            METRICS.replace(b"one-process", b"different-process"),
            METRICS.replace(b"finished 1", b"finished 0"),
            METRICS.replace(b'kind="get_blocks"} 2', b'kind="get_blocks"} 1'),
            METRICS + b"sync_block_capture_sessions_started 1\n",
            METRICS.replace(b"started 1", b"started NaN"),
            METRICS.replace(b"started 1", b"started 1.5"),
            METRICS.replace(b"started 1", b"started 9007199254740992"),
        ]:
            with self.subTest(metrics=metrics):
                with self.assertRaises(arrivals.IncompleteCapture):
                    load(episode(), metrics)

    def test_exporter_zero_counters_and_exact_exponent_numbers_are_supported(self):
        metrics = METRICS.replace(b"started 1", b"started 1e0")
        metrics += b'sync_block_capture_decoded_messages{kind="block"} 0\n'
        self.assertTrue(load(episode(), metrics)["decode_totals_reconciled"])


if __name__ == "__main__":
    unittest.main()
