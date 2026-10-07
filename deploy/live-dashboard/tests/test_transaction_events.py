import json
from pathlib import Path
import sys
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from transaction_events import parse_transaction_event, summarize_transactions
from node_events import EventFeed


def packet(phase, sequence, process="1-123", reason=None):
    return json.dumps({"version": 1, "process": process, "sequence": sequence,
                       "monotonic_ns": sequence * 1_000_000, "unix_ms": sequence,
                       "event": {"event": "transaction_lifecycle", "transaction": "a" * 32,
                                 "phase": phase, "reason": reason, "attempt": 1, "peer": "secret"}}).encode()


def event(phase, sequence, **kw):
    return {**parse_transaction_event(packet(phase, sequence, **kw)), "at": sequence}


class TransactionTests(unittest.TestCase):
    def test_schema_and_privacy(self):
        self.assertNotIn("secret", json.dumps(parse_transaction_event(packet("queued", 1))))
        for raw in (packet("unknown", 1), packet("queued", True), packet("rejected", 1),
                    packet("rejected", 1, reason="raw error text"), b"[]", b"x" * 8193):
            self.assertIsNone(parse_transaction_event(raw))

    def test_attempts_restarts_duplicates_and_selected_period(self):
        events = [event("queued", 1), event("received", 3), event("verification_started", 4),
                  event("verified", 9), event("admitted", 10), event("mined", 20),
                  event("queued", 21), event("verified", 25),
                  event("admitted", 28, process="2-456")]
        summary = summarize_transactions(list(reversed(events)) + [events[3]], 8, 30)
        self.assertEqual(summary["counts"]["verified"], 2)
        self.assertNotIn("received", summary["counts"])
        self.assertEqual([(r["metric"], r["value"]) for r in summary["timings"]],
                         [("verification_ms", 5), ("residence_ms", 10)])

    def test_persistent_feed_and_rejection_reasons(self):
        feed = EventFeed(":memory:")
        try:
            feed.ingest([packet("queued", 1), packet("queued", 1)], 100)
            feed.ingest([packet("rejected", 2, reason="download_failed")], 101)
            summary = feed.transactions(99, 102)
            self.assertEqual(summary["counts"], {"queued": 1, "rejected": 1})
            self.assertEqual(summary["reasons"], {"download_failed": 1})
            self.assertEqual(summary["timings"], [])
            self.assertEqual(feed.transactions(102, 103)["counts"], {})
            self.assertNotIn("secret", json.dumps(summary))
        finally:
            feed.close()


    def test_different_attempt_cannot_complete_an_old_boundary(self):
        first = event("verification_started", 1)
        other = event("verified", 8)
        other["attempt"] = 2
        self.assertEqual(summarize_transactions([first, other], 0, 10)["timings"], [])
        other["attempt"] = None
        self.assertEqual(summarize_transactions([first, other], 0, 10)["timings"], [])

    def test_relay_completion_requires_its_own_attempt_and_consumes_start(self):
        start = event("relay_started", 1)
        wrong = event("relay_succeeded", 3)
        wrong["attempt"] = 2
        failed = event("relay_failed", 5)
        duplicate_finish = event("relay_succeeded", 6)
        result = summarize_transactions([duplicate_finish, failed, wrong, start], 0, 10)
        self.assertEqual([(x["metric"], x["value"], x["outcome"]) for x in result["timings"]],
                         [("relay_ms", 4, "relay_failed")])
        start["attempt"] = None
        failed["attempt"] = None
        self.assertEqual(summarize_transactions([start, failed], 0, 10)["timings"], [])

    def test_readmission_cannot_finish_previous_residence(self):
        first = event("admitted", 1)
        second_start = event("admission_started", 4)
        second = event("admitted", 6)
        mined = event("mined", 20)
        for row in (second_start, second, mined):
            row["attempt"] = 2
        result = summarize_transactions([first, second_start, second, mined], 5, 21)
        self.assertEqual([(x["metric"], x["value"]) for x in result["timings"]],
                         [("admission_ms", 2), ("residence_ms", 14)])
        mined["attempt"] = 3
        result = summarize_transactions([first, mined], 0, 21)
        self.assertEqual(result["timings"], [])
        mined["attempt"] = None
        first["attempt"] = None
        self.assertEqual(summarize_transactions([first, mined], 0, 21)["timings"], [])
