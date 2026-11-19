"""Tests for the gossip harness's first-seen polling and its report."""

import time
import unittest
from unittest import mock

import round_robin_selfsend as harness
from spam_report import build_report

TXID = "ab" * 32


def fake_rpc(nodes: dict):
    """An `rpc_call` stand-in. `nodes` maps an RPC URL to where it holds TXID:
    "mempool", "block" (best chain), "side" (only a side-chain block), or
    "dead" (never answers, so every call waits out its timeout)."""

    def call(url, method, params=None, timeout=30, auth=None):
        behaviour = nodes[url]
        if behaviour == "dead":
            time.sleep(timeout)
            raise TimeoutError("timed out")
        if method == "getrawmempool":
            return [TXID] if behaviour == "mempool" else []
        if behaviour == "block":
            return {"txid": TXID, "confirmations": 1, "blockhash": "11" * 32}
        if behaviour == "side":
            return {"txid": TXID, "confirmations": 0, "height": -1, "blockhash": "22" * 32}
        raise RuntimeError("No such mempool or main chain transaction")

    return call


def node(name: str) -> dict:
    """An observation node whose RPC URL is `http://<name>`."""
    return {"name": name, "impl": "zakura", "rpc_url": f"http://{name}"}


class FirstSeenMatrix(unittest.TestCase):
    def test_a_dead_node_does_not_delay_the_others(self):
        # Polled in order, the dead node used to hold up every node after it
        # for two RPC timeouts, so "seen after 21 s" meant "polled after 21 s".
        nodes = [node("a"), node("dead"), node("b")]
        rpc = fake_rpc({"http://a": "mempool", "http://dead": "dead", "http://b": "mempool"})
        with mock.patch.object(harness, "rpc_call", rpc), \
                mock.patch.object(harness, "POLL_RPC_TIMEOUT", 0.5):
            in_mempool, in_block = harness.first_seen_matrix(
                nodes, TXID, duration=1.5, interval=0.05
            )

        self.assertEqual(set(in_mempool), {"a", "b"})
        self.assertLess(in_mempool["b"], 0.25)
        self.assertEqual(in_block, {})

    def test_a_node_that_first_finds_it_mined_is_kept_apart(self):
        # A side-chain block also counts: it isn't in the mempool either.
        nodes = [node("a"), node("late"), node("side")]
        rpc = fake_rpc({"http://a": "mempool", "http://late": "block", "http://side": "side"})
        with mock.patch.object(harness, "rpc_call", rpc):
            in_mempool, in_block = harness.first_seen_matrix(
                nodes, TXID, duration=1.0, interval=0.05
            )

        self.assertEqual(set(in_mempool), {"a"})
        self.assertEqual(set(in_block), {"late", "side"})


class Report(unittest.TestCase):
    def test_block_sightings_count_as_seen_but_stay_out_of_the_spread(self):
        events = [
            {"event": "start", "nodes": ["a", "b", "late"]},
            {
                "event": "submitted",
                "txid": TXID,
                "submit": "a",
                "impl": "zakura",
                "first_seen": {"a": 0.0, "b": 1.5},
                "seen_in_block": {"late": 21.0},
                "missing": [],
                "seen_all": True,
            },
        ]

        summary = build_report(events)["summary"]

        self.assertEqual(summary["missed"], 0)
        self.assertEqual(summary["mempool_all"], 0)
        self.assertEqual(summary["block_first_by_node"], {"late": 1})
        self.assertEqual(summary["spread_p50_secs"], 1.5)


if __name__ == "__main__":
    unittest.main()
