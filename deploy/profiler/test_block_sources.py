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
            request(9.88, "a", second=True, elapsed=30, hedge_reason="predicted"),
            request(20, "b", elapsed=100),
            request(20.05, "b", second=True, elapsed=90, hedge_reason="predicted"),
            request(30, "c", delivered=False, elapsed=2000),
            request(29.9, "c", second=True, elapsed=900, hedge_reason="stalled"),
        ])  # fmt: skip
        outcomes = {o["reason"]: o for o in sources.summary(1)["second_requests"]}
        self.assertEqual(outcomes["predicted"]["sent"], 2)
        self.assertEqual(outcomes["predicted"]["won"], 1)
        self.assertEqual(outcomes["predicted"]["saved_p50_ms"], 120)
        self.assertEqual(outcomes["stalled"]["won"], 1, "the first request never delivered")

    def test_fixed_delay_request_that_finishes_later_does_not_win(self):
        sources = sources_at(100, [
            request(10, "a", elapsed=400),
            request(10.05, "a", second=True, elapsed=200),
        ])
        outcome, = sources.summary(1)["second_requests"]
        self.assertEqual(outcome["reason"], "fixed delay")
        self.assertEqual(outcome["sent"], 1)
        self.assertEqual(outcome["delivered"], 1)
        self.assertEqual(outcome["won"], 0)
        self.assertIsNone(outcome["saved_p50_ms"])

    def test_second_request_is_paired_with_the_retry_it_belongs_to(self):
        sources = sources_at(100, [
            request(10, "a", elapsed=400, delivered=False),
            request(20.1, "a", second=True, elapsed=100),
            request(20.08, "a", elapsed=200),
        ])
        outcome, = sources.summary(1)["second_requests"]
        self.assertEqual(outcome["won"], 0)


def missed(seconds, connection, request):
    return {
        "event": "block_missed",
        "process_trace_id": RUN,
        "ts": at(seconds),
        "connection_id": connection,
        "request": request,
    }


class MissedBlockTest(unittest.TestCase):
    def test_restoring_a_score_is_counted_without_another_measurement(self):
        restored = sample(30, 2, "carried_over", None, 1000, weight=2.0)
        del restored["sample_ms"]
        summary = sources_at(100, [sample(20, 1, "ping", None, 5), restored]).summary(1)
        self.assertEqual(summary["samples"]["counts"]["carried_over"], 1)
        self.assertEqual(summary["samples"]["weight"]["p50"], 1.0)

    def test_large_delivery_clears_a_miss_without_adding_a_round_trip_sample(self):
        for score in (None, 50):
            with self.subTest(score=score):
                large = sample(20, 1, "large_block", score, score, weight=0.0)
                if score is None:
                    del large["score_ms"]
                summary = sources_at(100, [missed(10, 1, "first"), large]).summary(1)
                recovered = summary["missed_blocks"]["first"]
                self.assertEqual(recovered["delivered_again"], 1)
                self.assertEqual(recovered["delivered_again_s"]["p50"], 10)
                self.assertEqual(sum(summary["samples"]["counts"].values()), 0)
                self.assertEqual(summary["samples"]["weight"]["n"], 0)

    def test_a_miss_reports_when_the_peer_delivered_a_block_again(self):
        sources = sources_at(2000, [
            missed(20, 1, "second"),
            sample(80, 1, "ping", 5, 5),
            sample(620, 1, "block", 5, 6),
            missed(30, 2, "first"),
            dict(request(130, "x", elapsed=50), connection_id=2),
            missed(50, 3, "first"),
            sample(60, 3, "ping", 200, 200),
            missed(70, 4, "first"),
        ])  # fmt: skip
        summary = sources.summary(1)["missed_blocks"]
        self.assertEqual(summary["second"]["count"], 1)
        self.assertEqual(summary["second"]["delivered_again"], 1)
        self.assertEqual(summary["second"]["delivered_again_s"]["p50"], 600, "a block sample counts")
        first = summary["first"]
        self.assertEqual(first["count"], 3)
        self.assertEqual(first["delivered_again"], 1, "a delivered request counts")
        self.assertEqual(first["delivered_again_s"]["p50"], 100)


class TraceFollowerTest(unittest.TestCase):
    def test_delivery_rows_reach_the_summary_but_not_the_stage_timeline(self):
        import json
        import tempfile

        import blocks

        with tempfile.TemporaryDirectory() as trace_dir:
            with open(os.path.join(trace_dir, "delivery_score.jsonl"), "w") as table:
                for row in (missed(10, 1, "first"), sample(20, 1, "block", None, 5)):
                    table.write(json.dumps(row, separators=(",", ":")) + "\n")
            sources = block_sources.BlockSources(clock=lambda: STARTED + 100)
            follower = blocks.TraceFollower(trace_dir, 0, also_feed=(sources.feed,))
            self.assertEqual(follower.poll(), [])
            summary = sources.summary(1)
            self.assertEqual(summary["samples"]["counts"]["block"], 1)
            self.assertEqual(summary["missed_blocks"]["first"]["count"], 1)
            self.assertEqual(summary["missed_blocks"]["first"]["delivered_again_s"]["p50"], 10)


if __name__ == "__main__":
    unittest.main()
