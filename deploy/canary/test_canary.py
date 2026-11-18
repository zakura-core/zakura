"""Tests for canary.py's reads and grades."""

import json
import os
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import canary  # noqa: E402


def fake_rpc(replies):
    """Returns an `rpc` stand-in that answers each method from `replies` in order."""
    queues = {method: list(values) for method, values in replies.items()}

    def call(url, method, params=None, timeout=20.0):
        return queues[method].pop(0)

    return call


def row(round_index, node, tip, txids, t=None, template=None):
    """A `sample` row with a stable tip."""
    out = {"round": round_index, "node": node, "t": t if t is not None else float(round_index),
           "tip": tip, "txids": sorted(txids)}
    if template is not None:
        out["template"] = template
    return out


class ReadPoolTests(unittest.TestCase):
    def test_a_moved_tip_discards_the_pool(self):
        call = fake_rpc({"getbestblockhash": ["a", "b"], "getrawmempool": [["x"]]})

        self.assertEqual(canary.read_pool("u", call)[:2], (None, None))

    def test_a_stable_tip_keeps_the_pool(self):
        call = fake_rpc({"getbestblockhash": ["a", "a"], "getrawmempool": [["y", "x"]]})

        ticks = iter([10.0, 12.0])

        tip, txids, at, err = canary.read_pool("u", call, clock=lambda: next(ticks))

        self.assertEqual((tip, txids, at, err), ("a", ["x", "y"], 11.0, 1.0))

    def test_slow_reads_do_not_pair_past_the_skew(self):
        rounds = canary.by_round([
            {**row(0, "x", "h", {"a"}, t=10.0), "t_err": 2.0},
            {**row(0, "y", "h", {"a"}, t=10.0), "t_err": 2.0},
        ])

        self.assertEqual(canary.pair_overlap(rounds, "x", "y", 3.0)["pairs"], 0)


class OverlapTests(unittest.TestCase):
    def rows(self, canary_pool):
        rows = []
        for r in range(4):
            rows += [row(r, "n1", "h", {"a", "b"}), row(r, "n2", "h", {"a", "b"}),
                     row(r, "can", "h", canary_pool)]
        return rows

    def test_canary_matching_its_neighbours_passes(self):
        report = canary.grade_overlap(self.rows({"a", "b"}), "can", 3.0, 4, 0.0)

        self.assertEqual(report["verdict"], "pass")

    def test_canary_below_its_neighbours_fails(self):
        report = canary.grade_overlap(self.rows({"a"}), "can", 3.0, 4, 0.0)

        self.assertEqual(report["verdict"], "fail")

    def test_a_canary_that_stops_answering_fails(self):
        rows = self.rows({"a", "b"})
        for r in range(4, 8):
            rows += [row(r, "n1", "h", {"a", "b"}), row(r, "n2", "h", {"a", "b"}),
                     {"round": r, "node": "can", "t": float(r), "error": "timed out"}]

        report = canary.grade_overlap(rows, "can", 3.0, 4, 0.0)

        self.assertEqual(report["answered"], {"can": 0.5, "n1": 1.0, "n2": 1.0})
        self.assertEqual(report["verdict"], "fail")

    def test_too_few_pairs_is_inconclusive(self):
        report = canary.grade_overlap(self.rows({"a", "b"}), "can", 3.0, 5, 0.0)

        self.assertEqual(report["verdict"], "inconclusive")

    def test_different_tips_skew_and_empty_pools_do_not_pair(self):
        rounds = canary.by_round([
            row(0, "x", "h1", {"a"}), row(0, "y", "h2", {"a"}),
            row(1, "x", "h", {"a"}, t=0.0), row(1, "y", "h", {"a"}, t=9.0),
            row(2, "x", "h", set()), row(2, "y", "h", set()),
            {"round": 3, "node": "x", "t": 3.0, "tip_moved": True}, row(3, "y", "h", {"a"}),
        ])

        result = canary.pair_overlap(rounds, "x", "y", 3.0)

        self.assertEqual((result["pairs"], result["empty_pairs"], result["skipped"]), (0, 1, 3))


class SampleRoundTests(unittest.TestCase):
    def test_a_failed_template_keeps_the_pool_reads_time(self):
        def call(url, method, params=None, timeout=20.0):
            if method == "getblocktemplate":
                raise RuntimeError("no miner address")
            return {"getbestblockhash": "h", "getrawmempool": ["a"]}[method]

        rows = canary.sample_round({"x": "u1", "y": "u2"}, "r", 0, True, call)

        for result in rows:
            self.assertEqual(result["txids"], ["a"])
            self.assertIn("t_err", result)
            self.assertIn("template_error", result)
            self.assertNotIn("error", result)


class AppendedRunTests(unittest.TestCase):
    def test_rounds_from_two_runs_do_not_overwrite_each_other(self):
        rows = []
        for run, pool in (("r1", {"x"}), ("r2", {"a"})):
            for r in range(2):
                for node in ("n1", "n2", "can"):
                    rows.append({**row(r, node, "h", {"a"} if node != "can" else pool),
                                 "run": run})

        report = canary.grade_overlap(rows, "can", 3.0, 4, 0.0)

        self.assertEqual(report["pairs"]["can~n1"]["pairs"], 4)
        self.assertEqual(report["verdict"], "fail")


class TemplateTests(unittest.TestCase):
    def rows(self, canary_fees, canary_prev="p"):
        rows = []
        for r in range(3):
            rows += [
                row(r, "n1", "h", set(), template={"prev": "p", "txids": ["a"], "fees": 100}),
                row(r, "can", "h", set(),
                    template={"prev": canary_prev, "txids": ["a"], "fees": canary_fees}),
            ]
        return rows

    def test_same_fees_passes(self):
        self.assertEqual(canary.grade_templates(self.rows(100), "can", 3, 0.01)["verdict"], "pass")

    def test_lower_fees_fails(self):
        self.assertEqual(canary.grade_templates(self.rows(90), "can", 3, 0.01)["verdict"], "fail")

    def test_fees_compare_median_totals_not_ratios(self):
        rows = []
        pairs = [(10, 10), (20, 20), (30, 30), (1, 1000), (2, 1000)]
        for r, (fees, neighbour_fees) in enumerate(pairs):
            rows += [
                row(r, "n1", "h", set(),
                    template={"prev": "p", "txids": ["a"], "fees": neighbour_fees}),
                row(r, "can", "h", set(), template={"prev": "p", "txids": ["a"], "fees": fees}),
            ]

        report = canary.grade_templates(rows, "can", 3, 0.01)

        # The per-round ratios' median is 1.0, but the medians are 10 and 30.
        self.assertEqual(report["neighbours"]["n1"]["median_fee_ratio"], round(10 / 30, 4))
        self.assertEqual(report["verdict"], "fail")

    def test_a_canary_missing_template_reads_fails(self):
        rows = self.rows(100)
        for r in range(3, 6):
            rows += [row(r, "n1", "h", set(), template={"prev": "p", "txids": ["a"], "fees": 100}),
                     {**row(r, "can", "h", set()), "template_error": "timed out"}]

        report = canary.grade_templates(rows, "can", 3, 0.01)

        self.assertEqual(report["answered"], {"can": 0.5, "n1": 1.0})
        self.assertEqual(report["verdict"], "fail")

    def test_empty_templates_are_counted_apart(self):
        rows = []
        for r in range(3):
            for node in ("n1", "can"):
                rows.append(row(r, node, "h", set(),
                                template={"prev": "p", "txids": [], "fees": 0}))

        report = canary.grade_templates(rows, "can", 1, 0.01)

        self.assertEqual(report["neighbours"]["n1"]["empty_pairs"], 3)
        self.assertEqual(report["verdict"], "inconclusive")

    def test_templates_on_other_tips_are_inconclusive(self):
        report = canary.grade_templates(self.rows(100, canary_prev="q"), "can", 1, 0.01)

        self.assertEqual(report["verdict"], "inconclusive")


class TipLagTests(unittest.TestCase):
    def mirror(self, tmp, rows):
        path = os.path.join(tmp, "mirror.sqlite")
        db = sqlite3.connect(path)
        db.execute("CREATE TABLE trace_rows (node, tbl, file_id, offset, event, ts, "
                   "process_trace_id, fetched_unix, row)")
        for i, (node, height, block_hash, wall_us) in enumerate(rows):
            # Start the process at 1 second, so ts is the wall time minus that.
            body = {"event": "tip_published", "tip_height": height, "tip_hash": block_hash}
            db.execute("INSERT INTO trace_rows VALUES (?, 'chain_tip', '', ?, 'tip_published', "
                       "?, '7-1000000000', 0, ?)",
                       (node, i, wall_us - 1_000_000, json.dumps(body)))
        db.commit()
        db.close()
        return path

    def test_wall_time_adds_the_process_start(self):
        self.assertEqual(canary.wall_micros("42-5000000000", 250), 5_000_250)

    def test_every_block_published_everywhere_is_graded(self):
        blocks = [(h, f"b{h}") for h in range(10, 13)]
        with tempfile.TemporaryDirectory() as tmp:
            path = self.mirror(tmp, [
                (node, h, f"b{h}", 2_000_000 + h * 1_000_000 + (lag if node == "can" else 0))
                for h in range(10, 13) for node, lag in (("ref", 0), ("can", 40_000))
            ])
            times = canary.tip_times(path, {"ref", "can"}, 10, 12)

        report = canary.grade_tip_lag(blocks, times, ["ref", "can"], "ref", 50.0, 5.0, 3)

        self.assertEqual(report["nodes"]["can"]["p90_ms"], 40.0)
        self.assertEqual(
            canary.grade_tip_lag(blocks, times, ["ref", "can"], "ref", None, 5.0, 3)["verdict"],
            "inconclusive")
        self.assertEqual(report["verdict"], "pass")
        report = canary.grade_tip_lag(blocks, times, ["ref", "can"], "ref", 30.0, 5.0, 3)
        self.assertEqual(report["verdict"], "fail")

    def test_a_missing_row_makes_the_window_inconclusive(self):
        blocks = [(10, "b10"), (11, "b11")]
        times = {("ref", "b10"): 1, ("ref", "b11"): 2, ("can", "b10"): 1}

        report = canary.grade_tip_lag(blocks, times, ["ref", "can"], "ref", None, 0.0, 1)

        self.assertEqual(report["verdict"], "inconclusive")
        self.assertEqual(report["missing"], [{"height": 11, "hash": "b11", "node": "can"}])

    def test_lag_inside_the_clock_bound_counts_as_none(self):
        blocks = [(10, "b10")]
        times = {("ref", "b10"): 0, ("can", "b10"): 3_000}

        report = canary.grade_tip_lag(blocks, times, ["ref", "can"], "ref", 1.0, 5.0, 1)

        self.assertEqual(report["nodes"]["can"]["p90_ms"], 0.0)
        self.assertEqual(report["verdict"], "pass")


class QuantileTests(unittest.TestCase):
    def test_nearest_rank(self):
        values = [20.0] * 9 + [500.0]

        self.assertEqual(canary.quantile(values, 0.9), 20.0)
        self.assertEqual(canary.quantile(values, 0.95), 500.0)
        self.assertEqual(canary.quantile(values, 0.0), 20.0)
        self.assertEqual(canary.quantile([1.0, 2.0], 0.5), 1.0)


class MainTests(unittest.TestCase):
    def test_verdicts_map_to_exit_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            samples = os.path.join(tmp, "s.jsonl")
            with open(samples, "w") as out:
                for r in range(2):
                    for node in ("n1", "n2", "can"):
                        out.write(json.dumps(row(r, node, "h", {"a"})) + "\n")
                out.write('{"torn')
            args = ["overlap", "--samples", samples, "--canary", "can", "--min-pairs"]

            self.assertEqual(canary.main(args + ["2"]), canary.EXIT_PASS)
            self.assertEqual(canary.main(args + ["3"]), canary.EXIT_INCONCLUSIVE)

    def test_a_single_node_is_a_usage_error(self):
        self.assertEqual(canary.main(["sample", "--node", "a=http://x", "--out", "/dev/null",
                                      "--duration", "0"]), canary.EXIT_ERROR)


if __name__ == "__main__":
    unittest.main()
