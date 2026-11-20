#!/usr/bin/env python3
"""Tests for how the self-send report grades a round."""

from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

import round_robin_selfsend
import spam_report


def submitted(
    txid: str, first_seen: dict, seen_in_block: dict, nodes: list[str], unobserved=()
) -> dict:
    """A `submitted` event as the driver logs it."""
    unobserved = list(unobserved)
    missing = [
        name
        for name in nodes
        if name not in first_seen and name not in seen_in_block and name not in unobserved
    ]
    return {
        "event": "submitted",
        "txid": txid,
        "submit": nodes[0],
        "impl": "zakura",
        "first_seen": first_seen,
        "seen_in_block": seen_in_block,
        "unobserved": unobserved,
        "missing": missing,
        "seen_all": not missing and not unobserved,
        "inconclusive": bool(unobserved) and not missing,
    }


class GradingTests(unittest.TestCase):
    def test_an_unsettled_node_is_inconclusive_and_a_block_sighting_is_seen(self):
        nodes = ["a", "b"]
        events = [
            {"event": "start", "nodes": nodes},
            submitted("pass", {"a": 0.1, "b": 0.4}, {}, nodes),
            submitted("late", {"a": 0.1}, {"b": 2.0}, nodes),
            submitted("unsettled", {"a": 0.1}, {}, nodes, unobserved=["b"]),
            submitted("miss", {"a": 0.1}, {}, nodes),
        ]
        for txid in ("pass", "late", "unsettled", "miss"):
            events.append({"event": "drain_result", "txid": txid, "confirmations": 1})

        summary = spam_report.build_report(events)["summary"]

        self.assertEqual(summary["seen_all"], 2)
        self.assertEqual(summary["mempool_all"], 1)
        self.assertEqual(summary["inconclusive"], 1)
        self.assertEqual(summary["missed"], 1)
        self.assertEqual(summary["missed_by_node"], {"b": 1})
        self.assertEqual(summary["block_first_by_node"], {"b": 1})

    def test_an_old_log_without_the_split_proves_neither_a_pass_nor_a_miss(self):
        nodes = ["a", "b"]
        events = [
            {"event": "start", "nodes": nodes},
            {"event": "submitted", "txid": "miss", "first_seen": {"a": 0.1}, "seen_all": False},
            {
                "event": "submitted",
                "txid": "old",
                "first_seen": {"a": 0.1, "b": 0.2},
                "seen_all": True,
            },
        ]

        summary = spam_report.build_report(events)["summary"]

        # The old log's miss may have been an unreachable node.
        self.assertEqual(summary["missed"], 0)
        self.assertEqual(summary["seen_all"], 0)
        self.assertEqual(summary["inconclusive"], 2)


class RunStatusTests(unittest.TestCase):
    def test_a_run_with_a_pass_and_an_inconclusive_round_does_not_pass(self):
        nodes = ["a", "b"]
        events = [
            {"event": "start", "nodes": nodes},
            submitted("pass", {"a": 0.1, "b": 0.4}, {}, nodes),
            submitted("unsettled", {"a": 0.1}, {}, nodes, unobserved=["b"]),
        ]
        for txid in ("pass", "unsettled"):
            events.append({"event": "drain_result", "txid": txid, "confirmations": 1})
        summary = spam_report.build_report(events)["summary"]

        status = round_robin_selfsend.run_status(
            summary, require_all_seen=True, require_mined=True, errored=False
        )

        self.assertEqual(status, round_robin_selfsend.EXIT_INCONCLUSIVE)

    def test_a_miss_fails_whatever_else_happened(self):
        summary = {"submitted": 2, "seen_all": 0, "missed": 1, "inconclusive": 1, "unconfirmed": 0}
        status = round_robin_selfsend.run_status(
            summary, require_all_seen=True, require_mined=True, errored=False
        )
        self.assertEqual(status, 2)

    def test_every_round_seen_in_every_pool_passes(self):
        summary = {"submitted": 2, "seen_all": 2, "missed": 0, "inconclusive": 0, "unconfirmed": 0}
        status = round_robin_selfsend.run_status(
            summary, require_all_seen=True, require_mined=True, errored=False
        )
        self.assertEqual(status, 0)

    def test_a_round_not_seen_everywhere_never_passes(self):
        # Neither a miss nor marked inconclusive, as a hidden node would be.
        summary = {"submitted": 2, "seen_all": 1, "missed": 0, "inconclusive": 0, "unconfirmed": 0}
        status = round_robin_selfsend.run_status(
            summary, require_all_seen=True, require_mined=True, errored=False
        )
        self.assertEqual(status, round_robin_selfsend.EXIT_INCONCLUSIVE)


class ConfigTests(unittest.TestCase):
    def test_duplicate_node_names_are_refused(self):
        config = {
            "network": "test",
            "nodes": [
                {"name": "same", "impl": "zakura", "rpc_url": "http://a"},
                {"name": "same", "impl": "zakura", "rpc_url": "http://b"},
            ],
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(json.dumps(config))
            args = round_robin_selfsend.parse_args(["--config", str(path)])
            with self.assertRaises(SystemExit):
                round_robin_selfsend.load_config(args)


class SendTests(unittest.TestCase):
    def test_a_send_is_kept_whatever_the_inventory_after_it_does(self):
        for failure in (round_robin_selfsend.RunDurationElapsed("deadline"), TimeoutError()):
            with self.subTest(failure=failure):
                self.assert_send_kept(failure)

    def assert_send_kept(self, failure: Exception):
        inventories = iter([{"orchard": {"notes": 1}}])

        def inventory_pools(deadline=None):
            try:
                return next(inventories)
            except StopIteration:
                raise failure from None

        def zecd_rpc(method, params=None, timeout=300):
            return "txid" if method == "sendtoaddress" else "address"

        with mock.patch.multiple(
            round_robin_selfsend,
            inventory_pools=inventory_pools,
            zecd_rpc=zecd_rpc,
            tip_height=lambda *_args, **_kwargs: 7,
        ):
            txid, height, _, after = round_robin_selfsend.send_with_retry(
                {"name": "a", "rpc_url": "http://a"}, 0.1
            )

        self.assertEqual((txid, height, after), ("txid", 7, {}))

    def test_a_round_with_no_time_left_is_inconclusive(self):
        nodes = [{"name": "a"}]
        with mock.patch.object(
            round_robin_selfsend, "transaction_state", lambda *_a, **_k: round_robin_selfsend.IN_POOL
        ):
            in_pool, _, unobserved = round_robin_selfsend.first_seen_matrix(
                nodes, "t", run_deadline=0.0
            )
        self.assertEqual((in_pool, unobserved), ({}, ["a"]))


class ArgumentTests(unittest.TestCase):
    def test_grading_is_strict_unless_turned_off(self):
        args = round_robin_selfsend.parse_args([])
        self.assertTrue(args.require_all_seen)
        self.assertTrue(args.require_mined)

        args = round_robin_selfsend.parse_args(["--no-require-all-seen", "--no-require-mined"])
        self.assertFalse(args.require_all_seen)
        self.assertFalse(args.require_mined)


class TransactionStateTests(unittest.TestCase):
    """Where a node's answers place a transaction."""

    def state(self, mempool: list, tx) -> str | None:
        def rpc_call(_url, method, params=None, timeout=30, auth=None):
            if method == "getrawmempool":
                return mempool
            if isinstance(tx, Exception):
                raise tx
            return tx

        with mock.patch.object(round_robin_selfsend, "rpc_call", rpc_call):
            return round_robin_selfsend.transaction_state({"rpc_url": "http://node"}, "t")

    def test_no_lookup_starts_after_the_window(self):
        asked = []

        def rpc_call(_url, method, params=None, timeout=30, auth=None):
            asked.append(method)
            if method == "getrawmempool":
                # The pool listing uses up the rest of the window.
                time.sleep(0.05)
                return []
            return {"txid": "t"}

        with mock.patch.object(round_robin_selfsend, "rpc_call", rpc_call):
            with self.assertRaises(round_robin_selfsend.RunDurationElapsed):
                round_robin_selfsend.transaction_state(
                    {"rpc_url": "http://node"}, "t", deadline=time.monotonic() + 0.02
                )

        self.assertEqual(asked, ["getrawmempool"])

    def test_a_pooled_transaction_is_in_the_pool(self):
        self.assertEqual(self.state(["t"], None), round_robin_selfsend.IN_POOL)
        self.assertEqual(self.state([], {"txid": "t"}), round_robin_selfsend.IN_POOL)

    def test_an_answer_about_no_transaction_settles_nothing(self):
        self.assertIsNone(self.state([], {}))
        self.assertIsNone(self.state([], {"txid": "other"}))

    def test_a_mined_transaction_is_in_the_chain(self):
        tx = {"txid": "t", "confirmations": 2, "height": 10, "blockhash": "b"}
        self.assertEqual(self.state([], tx), round_robin_selfsend.IN_CHAIN)

    def test_a_side_chain_transaction_is_in_a_block(self):
        tx = {"txid": "t", "confirmations": 0, "height": -1, "blockhash": "b"}
        self.assertEqual(self.state([], tx), round_robin_selfsend.IN_CHAIN)

    def test_only_a_not_found_answer_is_absent(self):
        not_found = round_robin_selfsend.RpcError("not found", round_robin_selfsend.RPC_NOT_FOUND)
        self.assertEqual(self.state([], not_found), round_robin_selfsend.ABSENT)
        # An empty pool and a failed lookup, as when the lookup times out.
        self.assertIsNone(self.state([], TimeoutError("timed out")))
        self.assertIsNone(self.state([], round_robin_selfsend.RpcError("busy", -32603)))

    def test_a_node_that_answers_nothing_is_unreachable(self):
        def rpc_call(*_args, **_kwargs):
            raise OSError("connection refused")

        with mock.patch.object(round_robin_selfsend, "rpc_call", rpc_call):
            self.assertIsNone(
                round_robin_selfsend.transaction_state({"rpc_url": "http://node"}, "t")
            )


class MatrixTests(unittest.TestCase):
    def test_a_slow_pass_still_resamples_every_node_in_the_window(self):
        nodes = [{"name": n} for n in "abcde"]
        samples = {n: 0 for n in "abcde"}
        lock = threading.Lock()

        def state(node, _txid, deadline=None):
            with lock:
                samples[node["name"]] += 1
                count = samples[node["name"]]
            time.sleep(0.03)
            # Node a gets the transaction after its first sample.
            if node["name"] == "a" and count > 1:
                return round_robin_selfsend.IN_POOL
            return round_robin_selfsend.ABSENT

        with mock.patch.object(round_robin_selfsend, "transaction_state", state):
            in_pool, _, _ = round_robin_selfsend.first_seen_matrix(
                nodes, "t", duration=0.1, interval=0.0
            )

        # One after another, five 30 ms samples outlast the 100 ms window
        # before a is sampled again.
        self.assertIn("a", in_pool)

    def test_a_slow_node_does_not_hold_back_another(self):
        nodes = [{"name": "a"}, {"name": "b"}]
        samples = {"a": 0}
        lock = threading.Lock()

        def state(node, _txid, deadline=None):
            if node["name"] == "b":
                # b's one sample outlasts the window.
                time.sleep(0.15)
                return round_robin_selfsend.ABSENT
            with lock:
                samples["a"] += 1
                count = samples["a"]
            # a gets the transaction after its first sample.
            return round_robin_selfsend.IN_POOL if count > 1 else round_robin_selfsend.ABSENT

        with mock.patch.object(round_robin_selfsend, "transaction_state", state):
            in_pool, _, _ = round_robin_selfsend.first_seen_matrix(
                nodes, "t", duration=0.1, interval=0.01
            )

        self.assertIn("a", in_pool)

    def test_an_absence_in_a_window_the_run_cut_short_is_unobserved(self):
        nodes = [{"name": "a"}]
        with mock.patch.object(
            round_robin_selfsend,
            "transaction_state",
            lambda *_a, **_k: round_robin_selfsend.ABSENT,
        ):
            _, _, unobserved = round_robin_selfsend.first_seen_matrix(
                nodes, "t", duration=30, interval=0.01, run_deadline=time.monotonic() + 0.05
            )
        self.assertEqual(unobserved, ["a"])

    def test_an_interrupted_sample_after_an_absent_one_is_unobserved(self):
        nodes = [{"name": "a"}]
        answers = iter([round_robin_selfsend.ABSENT])

        def state(_node, _txid, deadline=None):
            answer = next(answers, None)
            if answer is None:
                raise round_robin_selfsend.RunDurationElapsed("deadline")
            return answer

        with mock.patch.object(round_robin_selfsend, "transaction_state", state):
            _, _, unobserved = round_robin_selfsend.first_seen_matrix(
                nodes, "t", duration=5, interval=0.01
            )

        self.assertEqual(unobserved, ["a"])

    def test_a_definitely_absent_node_is_a_miss(self):
        nodes = [{"name": "a"}, {"name": "b"}]
        answers = {"a": round_robin_selfsend.IN_POOL, "b": round_robin_selfsend.ABSENT}

        def state(node, _txid, deadline=None):
            return answers[node["name"]]

        with mock.patch.object(round_robin_selfsend, "transaction_state", state):
            in_pool, after_mined, unobserved = round_robin_selfsend.first_seen_matrix(
                nodes, "t", duration=0.05, interval=0.01
            )

        self.assertEqual(list(in_pool), ["a"])
        self.assertEqual((after_mined, unobserved), ({}, []))

    def test_an_unsettled_node_beside_a_mined_one_is_not_a_miss(self):
        nodes = [{"name": "a"}, {"name": "b"}]
        answers = {"a": round_robin_selfsend.IN_CHAIN, "b": None}

        def state(node, _txid, deadline=None):
            return answers[node["name"]]

        with mock.patch.object(round_robin_selfsend, "transaction_state", state):
            in_pool, after_mined, unobserved = round_robin_selfsend.first_seen_matrix(
                nodes, "t", duration=0.05, interval=0.01
            )

        self.assertEqual(in_pool, {})
        self.assertEqual(list(after_mined), ["a"])
        self.assertEqual(unobserved, ["b"])
        event = submitted("t", in_pool, after_mined, ["a", "b"], unobserved)
        summary = spam_report.build_report([{"event": "start", "nodes": ["a", "b"]}, event])[
            "summary"
        ]
        self.assertEqual(summary["missed"], 0)
        self.assertEqual(summary["inconclusive"], 1)


if __name__ == "__main__":
    unittest.main()
