"""Boundary checks must not mistake a stable unfinished owner for a drained run."""
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).parents[1]))
from getblocks_boundary import COUNTER_FAMILIES, await_quiescence, unsettled
from getblocks_capture import IncompleteCapture, read_capture_metrics
from test_getblocks_lifetimes import episode, metrics_for


class Clock:
    def __init__(self):
        self.now = 0

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class BoundaryTests(unittest.TestCase):
    def test_two_stable_drained_samples_are_required(self):
        raw = metrics_for(*episode())
        clock, calls = Clock(), []
        def scrape():
            calls.append(clock.now)
            return raw
        result, evidence = await_quiescence(scrape, 10, clock=clock.time, sleep=clock.sleep)
        self.assertEqual(result, raw)
        self.assertEqual(calls, [0, 2])
        self.assertTrue(evidence["quiescent_counters_verified"])
        self.assertFalse(evidence["capture_loss_verified"])

    def test_each_unfinished_owner_prevents_finalization(self):
        raw = metrics_for(*episode())
        samples = read_capture_metrics(raw, COUNTER_FAMILIES)
        for family, labels in [
            ("sessions_finished", ()),
            ("ownership_events", (("phase", "release_finished"), ("stage", "pending"))),
            ("settlement_events", (("phase", "release_finished"),)),
            ("frame_events", (("phase", "release_finished"),)),
            ("serving_query_events", (("phase", "read_finished"),)),
            ("wait_events", (("phase", "ready"), ("stage", "reactor_queue"))),
        ]:
            altered = dict(samples)
            altered[(family, labels)] -= 1
            with self.subTest(family=family):
                self.assertTrue(unsettled(altered))

    def test_stable_but_unfinished_capture_times_out(self):
        raw = metrics_for(*episode()).replace(b"sync_block_capture_sessions_finished 1", b"sync_block_capture_sessions_finished 0")
        clock = Clock()
        with self.assertRaisesRegex(IncompleteCapture, "sessions"):
            await_quiescence(lambda: raw, 5, clock=clock.time, sleep=clock.sleep)
        self.assertEqual(clock.now, 5)

    def test_a_draining_sample_does_not_count_as_stable(self):
        raw = metrics_for(*episode())
        unfinished = raw.replace(b"sync_block_capture_sessions_finished 1", b"sync_block_capture_sessions_finished 0")
        samples, clock = iter([unfinished, raw, raw]), Clock()
        await_quiescence(lambda: next(samples), 10, clock=clock.time, sleep=clock.sleep)
        self.assertEqual(clock.now, 4)

    def test_short_sleep_cannot_forge_two_second_separation(self):
        raw, clock = metrics_for(*episode()), Clock()
        with self.assertRaises(IncompleteCapture):
            await_quiescence(lambda: raw, 5, clock=clock.time, sleep=lambda _: clock.sleep(1))

    def test_unknown_counter_phase_is_rejected(self):
        raw = metrics_for(*episode()) + b'sync_block_capture_frame_events{phase="unknown"} 1\n'
        with self.assertRaises(IncompleteCapture):
            unsettled(read_capture_metrics(raw, COUNTER_FAMILIES))


if __name__ == "__main__":
    unittest.main()
