import unittest
from dataclasses import replace

from observations import comparisons, parity_bounds
from graph import closure, header_tree_repair
from sim import Config, Controller, Link, Packet, Simulation, State, WIRE


class Tests(unittest.TestCase):
    def test_subscription_cycle(self):
        suppliers = [[set(), set(), set()], [{2}, {2}, {2}], [{1}, {1}, {1}]]
        self.assertEqual(closure(suppliers, 0, 2, 3, True)[0], 1)
        suppliers[1][0].add(0)
        suppliers[1][1].add(0)
        self.assertEqual(closure(suppliers, 0, 2, 3, True)[0], 3)

    def test_header_tree_repair(self):
        suppliers = [[set(), set(), set()], [{2}, {2}, {2}], [{1}, {1}, {1}]]
        peers = [{1}, {0, 2}, {1}]
        self.assertEqual(header_tree_repair(suppliers, peers, 0, 2, 3)[0], 3)

    def test_replay(self):
        cfg = Config(blocks=12)
        self.assertEqual(Simulation(cfg, "budgeted", 1).run(), Simulation(cfg, "budgeted", 1).run())

    def test_coverage(self):
        cfg = Config(blocks=4)
        sim = Simulation(cfg, "equal", 1)
        sim.run()
        for state in sim.states.values():
            for failed in range(4):
                self.assertGreaterEqual(sum(bool(p - {failed}) for p in state.assignments.values()), state.k)

    def test_shared_load(self):
        cfg = Config(blocks=16, rates=(5, 5, 5, 5), deadline_ms=1000)
        one = Simulation(cfg, "equal", 1).run()
        burst = Simulation(replace(cfg, burst=4), "equal", 1).run()
        self.assertGreater(burst["peak_assigned_bytes"], one["peak_assigned_bytes"])
        self.assertGreater(burst["p95_ms"], one["p95_ms"])

    def test_no_idle_budget_growth(self):
        c = Controller(Config(), "budgeted", __import__("random").Random(0))
        before = c.energy, list(c.w)
        self.assertEqual(before, (c.energy, c.w))
        # Time can expire trials, but cannot replenish funds or create observations.
        c.start_trial(0, 40, 1e9)
        self.assertEqual(c.energy, before[0])
        self.assertEqual(c.w, before[1])

    def test_proposer_isolation(self):
        c = Controller(Config(), "budgeted", __import__("random").Random(0))
        original = c.row(1).copy()
        c.row(0)[0] = 3
        self.assertEqual(c.row(1), original)
        self.assertEqual(c.row(0)[0], 3)

    def test_exploration_and_queue_bounds(self):
        cfg = Config(blocks=24, burst=4, interval_ms=100, down_mib=2, queue_parts=12)
        report = Simulation(cfg, "budgeted", 2).run()
        self.assertLessEqual(report["exploration_charged"], report["exploration_bound"])
        self.assertLessEqual(report["peak_link_queue_bytes"], 12 * WIRE)
        self.assertGreater(report["queue_drops"], 0)

    def test_cancel_censors_failures(self):
        cfg = Config(blocks=4, rates=(100, 100, 100, 1))
        sim = Simulation(cfg, "budgeted", 0)
        sim.run()
        for s in sim.states.values():
            if s.done - s.start <= cfg.deadline_ms:
                for failures in sim.controllers[s.receiver].failures:
                    self.assertNotIn(s.block, failures)

    def test_fallback_is_not_decode(self):
        result = Simulation(Config(blocks=4, source_stall_ms=2000), "equal", 0).run()
        self.assertEqual(result["completed"], 0)
        self.assertEqual(result["fallback"], 1)
        self.assertIsNone(result["p95_ms"])

    def test_counterexample(self):
        self.assertTrue(comparisons(0, 64, "fixed_skew")["passive_wrong"])
        self.assertFalse(comparisons(0, 64, "fixed_skew")["paired_wrong"])

    def test_parity_threshold(self):
        rows = parity_bounds()
        row = next(r for r in rows if r["k"] == 32 and r["n"] == 40 and
                   r["subscribed"] == 40 and r["peers"] == 4)
        self.assertFalse(row["tolerate_one_without_duplicates"])
        row = next(r for r in rows if r["k"] == 32 and r["n"] == 40 and
                   r["subscribed"] == 40 and r["peers"] == 5)
        self.assertTrue(row["tolerate_one_without_duplicates"])

    def test_link_service_and_fairness(self):
        sim = Simulation(Config(blocks=1), "equal", 0)
        got = []
        link = Link(sim, lambda: 1, lambda p: got.append((p.block, sim.now)), False)
        for b in (0, 0, 0, 1, 1):
            sim.states[0, b] = State(b, 0, 0, 0, 1, 1, [0], {0: {0}}, (0, 1))
            link.put(Packet(b, 0, 0, 0))
        while sim.events:
            import heapq
            sim.now, _, fn, args = heapq.heappop(sim.events)
            fn(*args)
        self.assertEqual([b for b, _ in got], [0, 0, 1, 0, 1])
        self.assertAlmostEqual(got[-1][1], 5 * WIRE / 1048576 * 1000)


if __name__ == "__main__":
    unittest.main()
