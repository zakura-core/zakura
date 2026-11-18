"""Tests for the block source summary (`block_sources.py`)."""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import block_sources  # noqa: E402

# A node run that started at unix time 1,000,000.
RUN = "4242-1000000000000000"
STARTED = 1_000_000.0


def at(seconds):
    """`ts` for `seconds` after the run started."""
    return int(seconds * 1e6)


def request(seconds, block, *, second=False, delivered=True, elapsed=100, score=80, size=10_000, **extra):
    row = {
        "event": "block_request_finish",
        "process_trace_id": RUN,
        "ts": at(seconds),
        "requested_hash": block,
        "route": "hedge" if second else "advertised",
        "result": "available" if delivered else "notfound_response",
        "elapsed_ms": elapsed,
        "score_ms": score,
        **extra,
    }
    if delivered and size is not None:
        row["block_bytes"] = size
    return row


def sample(seconds, connection, kind, before, score, weight=1.0, sample_ms=10):
    row = {
        "event": "delivery_sample",
        "process_trace_id": RUN,
        "ts": at(seconds),
        "connection_id": connection,
        "kind": kind,
        "sample_ms": sample_ms,
        "score_ms": score,
        "weight": weight,
    }
    if before is not None:
        row["score_before_ms"] = before
    return row


def sources_at(seconds, rows):
    sources = block_sources.BlockSources(clock=lambda: STARTED + seconds)
    for row in rows:
        sources.feed(row)
    return sources


class PredictionTest(unittest.TestCase):
    def test_errors_are_grouped_by_request_and_block_size(self):
        sources = sources_at(100, [
            request(10, "a", elapsed=230, score=230, size=10_000),
            request(11, "b", elapsed=600, score=240, size=1_500_000),
            request(11.2, "b", second=True, elapsed=33, score=3, size=1_500_000,
                    hedge_reason="predicted", first_request_waited_ms=250),
            request(12, "c", elapsed=50, score=None),
        ])  # fmt: skip
        summary = sources.summary(1)
        groups = {(g["request"], g["size"]): g for g in summary["prediction"]}
        self.assertEqual(groups[("first", "under 50 KB")]["p50"], 0)
        self.assertEqual(groups[("first", "over 256 KB")]["p50"], 360)
        self.assertEqual(groups[("second", "over 256 KB")]["p50"], 30)
        self.assertEqual(summary["requests"]["first"], 3)
        self.assertEqual(summary["requests"]["first_unscored"], 1)
        self.assertEqual(summary["requests"]["second"], 1)

    def test_rows_from_before_the_window_are_left_out(self):
        sources = sources_at(3 * 3600, [request(10, "old"), request(3 * 3600 - 60, "new")])
        self.assertEqual(sources.summary(1)["requests"]["first"], 1)
        self.assertEqual(sources.summary(6)["requests"]["first"], 2)

    def test_a_row_without_a_block_size_is_counted_apart(self):
        sources = sources_at(100, [request(10, "a", size=None)])
        self.assertEqual(sources.summary(1)["prediction"][0]["size"], block_sources.UNKNOWN_SIZE)


class SecondRequestTest(unittest.TestCase):
    def test_a_second_request_wins_when_it_arrives_before_the_first(self):
        sources = sources_at(100, [
            request(10, "a", elapsed=400),
            request(10.3, "a", second=True, elapsed=30, hedge_reason="predicted", first_request_waited_ms=250),
            request(20, "b", elapsed=100),
            request(20.2, "b", second=True, elapsed=90, hedge_reason="predicted", first_request_waited_ms=60),
            request(30, "c", delivered=False, elapsed=2000),
            request(31, "c", second=True, elapsed=900, hedge_reason="stalled", first_request_waited_ms=1000),
        ])  # fmt: skip
        outcomes = {o["reason"]: o for o in sources.summary(1)["second_requests"]}
        self.assertEqual(outcomes["predicted"]["sent"], 2)
        self.assertEqual(outcomes["predicted"]["won"], 1, "250 + 30 beats 400; 60 + 90 does not beat 100")
        self.assertEqual(outcomes["predicted"]["saved_p50_ms"], 120)
        self.assertEqual(outcomes["stalled"]["won"], 1, "the first request never delivered")


class PenaltyTest(unittest.TestCase):
    def test_a_penalty_reports_its_jump_and_when_the_score_came_back(self):
        sources = sources_at(2000, [
            sample(10, 1, "ping", None, 5),
            sample(20, 1, "missed_second_block", 5, 1000, weight=2.0, sample_ms=2000),
            sample(80, 1, "transaction", 1000, 400),
            sample(620, 1, "ping", 400, 6),
            sample(30, 2, "missed_block", 240, 600, weight=5.0, sample_ms=2000),
            sample(40, 2, "ping", 600, 520),
            sample(50, 3, "missed_block", 250, 700, sample_ms=2000),
        ])  # fmt: skip
        summary = sources.summary(1)
        second = summary["penalties"]["missed_second_block"]
        self.assertEqual(second["count"], 1)
        self.assertEqual(second["jump_ms"]["p50"], 995)
        self.assertEqual(second["recovered"], 1)
        self.assertEqual(second["recovery_s"]["p50"], 600)
        first = summary["penalties"]["missed_block"]
        self.assertEqual(first["count"], 2)
        self.assertEqual(first["still_raised"], 1)
        self.assertEqual(first["closed"], 1)
        self.assertEqual(summary["samples"]["counts"]["ping"], 3)


class TraceFollowerTest(unittest.TestCase):
    def test_delivery_rows_reach_the_summary_but_not_the_stage_timeline(self):
        import json
        import tempfile

        import blocks

        with tempfile.TemporaryDirectory() as trace_dir:
            with open(os.path.join(trace_dir, "delivery_score.jsonl"), "w") as table:
                table.write(json.dumps(sample(10, 1, "ping", None, 5), separators=(",", ":")) + "\n")
            sources = block_sources.BlockSources(clock=lambda: STARTED + 100)
            follower = blocks.TraceFollower(trace_dir, 0, also_feed=(sources.feed,))
            self.assertEqual(follower.poll(), [])
            self.assertEqual(sources.summary(1)["samples"]["counts"]["ping"], 1)


if __name__ == "__main__":
    unittest.main()
