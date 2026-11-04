#!/usr/bin/env python3
"""Tests for the NU7 mainnet dashboard collector (stdlib unittest)."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import collector as collector_module  # noqa: E402
import nu7_chain as chain  # noqa: E402
from collector import BoundedHTTPServer, Collector, Handler  # noqa: E402
from nu7_store import Store  # noqa: E402

ACTIVATION = chain.NU7_ACTIVATION_HEIGHT
# The fake node activates NU7 at a small height so tests build few blocks.
FAKE_ACTIVATION = 500

EXPOSITION = """# HELP ignored
zcash_chain_tip_height 497
zcash_chain_block_height 497
zcash_chain_block_fees_zat 1000
zcash_chain_block_fees_zat_total 50000
zcash_chain_block_fees_nsm_zat 0
zcash_chain_block_fees_nsm_zat_total 0
zcash_chain_block_fees_miner_zat 1000
zcash_chain_block_fees_miner_zat_total 50000
zcash_chain_block_orchard_actions 12
zcash_chain_block_orchard_actions_total 400
zcash_net_peers 42
zcash_mempool_size_transactions 7
zakura_build_info{version="1.8.2"} 1
sync_block_verify_duration_seconds{quantile="0.5"} 0.2
sync_block_verify_duration_seconds{quantile="0.99"} 0.9
zcash_chain_block_tx_fee_zat{quantile="0.5"} 10000
zakura_p2p_something{peer="x"} NaN
"""


class ChainArithmeticTests(unittest.TestCase):
    def test_parse_keeps_wanted_lines_and_quantiles(self):
        metrics = chain.parse_exposition(EXPOSITION)
        self.assertEqual(chain.bare(metrics, "zcash_chain_tip_height"), 497)
        self.assertEqual(chain.quantile(metrics, "sync_block_verify_duration_seconds", "0.99"), 0.9)
        self.assertEqual(chain.label_value(metrics, "zakura_build_info", "version"), "1.8.2")
        self.assertNotIn("zakura_p2p_something", metrics)

    def test_interval_stats_and_expected_median(self):
        self.assertEqual(chain.interval_stats([], 10), {"median": None, "mean": None, "count": 0})
        stats = chain.interval_stats([10, 20, 30, 40], 3)
        self.assertEqual((stats["median"], stats["mean"], stats["count"]), (30, 30, 3))
        self.assertAlmostEqual(chain.expected_median_interval(25), 17.33, places=2)

    def test_difficulty_change(self):
        self.assertIsNone(chain.difficulty_change_pct(None, 10))
        self.assertAlmostEqual(chain.difficulty_change_pct(100, 116), 16.0)

    def test_activation_eta_extrapolates_recent_intervals(self):
        remaining, eta, basis = chain.activation_eta(ACTIVATION - 10, 1000, [50, 100])
        self.assertEqual((remaining, basis), (10, 75))
        self.assertEqual(eta, 1000 + 10 * 75)
        self.assertEqual(chain.activation_eta(ACTIVATION, 1000, [25]), (0, None, None))

    def test_fee_split_rounds_the_block_aggregate_down(self):
        self.assertEqual(chain.nsm_fee_share(1001), 600)
        self.assertTrue(chain.fee_split_ok(1001, 600))
        self.assertFalse(chain.fee_split_ok(1001, 601))
        self.assertIsNone(chain.fee_split_ok(None, 600))

    def test_attribute_one_new_block_is_exact(self):
        previous = {"height": 10, "totals": {"fees_zat": 100}, "latest": {"fees_zat": 40}}
        current = {"height": 11, "totals": {"fees_zat": 150}, "latest": {"fees_zat": 50}}
        self.assertEqual(chain.attribute_counters(previous, current), [(11, {"fees_zat": 50}, None)])

    def test_attribute_several_new_blocks_merges_the_remainder(self):
        previous = {"height": 10, "totals": {"fees_zat": 100}, "latest": {"fees_zat": 40}}
        current = {"height": 13, "totals": {"fees_zat": 190}, "latest": {"fees_zat": 30}}
        rows = chain.attribute_counters(previous, current)
        self.assertEqual(rows, [(12, {"fees_zat": 60}, 11), (13, {"fees_zat": 30}, None)])

    def test_attribute_detects_a_counter_reset(self):
        previous = {"height": 10, "totals": {"fees_zat": 100}, "latest": {}}
        current = {"height": 11, "totals": {"fees_zat": 5}, "latest": {}}
        self.assertEqual(chain.attribute_counters(previous, current), "reset")
        self.assertEqual(chain.attribute_counters(None, current), [])

    def test_mempool_fee_quantiles(self):
        raw = {f"tx{i}": {"fee": i * 0.00001, "size": 1000} for i in range(1, 11)}
        quantiles = chain.mempool_fee_quantiles(raw)
        self.assertEqual(quantiles["count"], 10)
        self.assertEqual(quantiles["p50"], 5000)
        self.assertAlmostEqual(quantiles["feeRateP50"], 5.5)
        self.assertEqual(chain.mempool_fee_quantiles({})["count"], 0)

    def test_rules_switch_at_activation(self):
        before = chain.rules_at(ACTIVATION - 1, "c8e71055", None)
        after = chain.rules_at(ACTIVATION, "77190ad9", 26041666)
        self.assertEqual((before["targetSpacingSeconds"], before["nsmPercent"]), (75, 0))
        self.assertEqual((after["targetSpacingSeconds"], after["nsmPercent"], after["averagingWindowBlocks"]), (25, 60, 102))
        self.assertEqual(after["actionLimits"]["orchardActionsPerPool"], 330)


class FakeNode:
    """A node whose chain the test extends block by block."""

    def __init__(self, tip=FAKE_ACTIVATION - 3, start_time=1_762_000_000):
        self.blocks = {}
        self.exposition = EXPOSITION
        self.nsm_balance = 55_768_414_957
        for height in range(tip - 20, tip + 1):
            self.blocks[height] = self.make_block(height, start_time + height * 75)
        self.calls = []

    def make_block(self, height, time, salt=""):
        return {"hash": f"{height:08x}{salt}".ljust(64, "0"), "time": time, "bits": "1c123456",
                "difficulty": 1000.0 + height, "size": 2000, "nTx": 3,
                "valuePools": [{"id": "lockbox", "chainValueZat": 123}, {"id": "orchard", "chainValueZat": 456}],
                "chainSupply": {"chainValueZat": 999}}

    def tip(self):
        return max(self.blocks)

    def extend(self, interval=25, salt=""):
        height = self.tip() + 1
        self.blocks[height] = self.make_block(height, self.blocks[height - 1]["time"] + interval, salt)
        return height

    def reorg(self, depth):
        for height in range(self.tip() - depth + 1, self.tip() + 1):
            self.blocks[height] = self.make_block(height, self.blocks[height]["time"] + 1, "ff")

    def rpc(self, method, params=None):
        self.calls.append(method)
        params = params or []
        if method == "getblockchaininfo":
            return {"blocks": self.tip(), "bestblockhash": self.blocks[self.tip()]["hash"],
                    "nsmValueBalanceZat": self.nsm_balance,
                    "consensus": {"chaintip": "c8e71055", "nextblock": "77190ad9"},
                    "upgrades": {"77190ad9": {"status": "pending", "activationheight": FAKE_ACTIVATION}}}
        if method == "getblockhash":
            return self.blocks[params[0]]["hash"]
        if method == "getblock":
            return self.blocks[int(params[0])]
        if method == "getblocksubsidy":
            return {"miner": 0.2, "fundingstreamstotal": 0.05, "lockboxtotal": 0.01, "totalblocksubsidy": 0.26}
        if method == "getnetworksolps":
            return 12345
        if method == "getmempoolinfo":
            return {"size": 2, "bytes": 500}
        if method == "getrawmempool":
            return {"a": {"fee": 0.0001, "size": 200}, "b": {"fee": 0.0002, "size": 300}}
        if method == "getchaintips":
            return [{"height": self.tip()}]
        raise RuntimeError(f"unexpected rpc {method}")

    def metrics_text(self):
        return self.exposition

    def service_status(self):
        return {"active_state": "active", "pid": 0, "restarts": 0}

    def rss_bytes(self, pid):
        return None


def new_collector(node, tmp):
    db = os.path.join(tmp, "test.sqlite")
    page = Path(tmp) / "page.html"
    page.write_text("<html>test</html>")
    return Collector(node, db, activation_height=FAKE_ACTIVATION, backfill=5, page_path=page,
                     clock=lambda: 1_770_000_000.0), Store(db)


class CollectorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.node = FakeNode()
        self.collector, self.store = new_collector(self.node, self.tmp.name)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def test_backfill_then_live_blocks_with_intervals(self):
        self.assertEqual(self.collector.ingest_once(self.store), 6)
        tip = self.store.tip()
        self.assertEqual((tip["height"], tip["source"], tip["interval_s"]), (FAKE_ACTIVATION - 3, "backfill", 75))
        self.assertEqual(tip["nsm_balance_zat"], self.node.nsm_balance)
        self.assertEqual(tip["solps"], 12345)
        for _ in range(3):
            self.node.extend(interval=25)
        self.assertEqual(self.collector.ingest_once(self.store), 3)
        live = self.store.tip()
        self.assertEqual((live["height"], live["source"], live["interval_s"], live["subsidy_total_zat"]),
                         (FAKE_ACTIVATION, "live", 25, 26_000_000))
        self.assertEqual(self.store.blocks_observed_since(0), 3)
        self.assertIn("activation", [e["kind"] for e in self.store.events_since(0)])

    def test_reorg_replaces_rows_above_the_fork(self):
        self.collector.ingest_once(self.store)
        before = self.store.tip()
        self.node.reorg(2)
        self.node.extend()
        self.collector.ingest_once(self.store)
        events = [e for e in self.store.events_since(0) if e["kind"] == "reorg"]
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["depth"], 2)
        self.assertNotEqual(self.store.block(before["height"])["hash"], before["hash"])
        self.assertEqual(self.store.tip()["height"], before["height"] + 1)

    def test_counter_attribution_reaches_the_block_row(self):
        self.collector.ingest_once(self.store)
        self.collector.scrape_once(self.store)
        height = self.node.extend()
        self.node.exposition = EXPOSITION.replace("zcash_chain_block_height 497", f"zcash_chain_block_height {height}") \
            .replace("zcash_chain_block_fees_zat_total 50000", "zcash_chain_block_fees_zat_total 52001") \
            .replace("zcash_chain_block_fees_nsm_zat_total 0", "zcash_chain_block_fees_nsm_zat_total 1200") \
            .replace("zcash_chain_block_orchard_actions_total 400", "zcash_chain_block_orchard_actions_total 433")
        self.collector.scrape_once(self.store)
        self.assertIn(height, self.collector.pending_attribution)
        self.collector.ingest_once(self.store)
        row = self.store.block(height)
        self.assertEqual((row["fees_zat"], row["nsm_fee_zat"], row["orchard_actions"]), (2001, 1200, 33))
        self.assertEqual(row["fee_split_ok"], 1)

    def test_counter_reset_is_an_event(self):
        self.collector.ingest_once(self.store)
        self.collector.scrape_once(self.store)
        self.node.exposition = EXPOSITION.replace("zcash_chain_block_fees_zat_total 50000", "zcash_chain_block_fees_zat_total 10") \
            .replace("zcash_chain_block_height 497", f"zcash_chain_block_height {FAKE_ACTIVATION}")
        self.collector.scrape_once(self.store)
        self.assertEqual(self.store.count_events("counter_reset", 0), 1)

    def test_payload_before_and_after_activation(self):
        self.collector.ingest_once(self.store)
        self.collector.scrape_once(self.store)
        self.collector.sample_once(self.store)
        payload = self.collector.payload(self.store, "6h")
        json.dumps(payload, allow_nan=False)
        self.assertEqual(payload["activation"]["state"], "pending")
        self.assertEqual(payload["activation"]["blocksRemaining"], 3)
        self.assertEqual(payload["chain"]["targetSpacingSeconds"], 75)
        self.assertEqual(payload["rules"]["nextBlock"]["nsmPercent"], 0)
        self.assertEqual(payload["mempool"]["feeZat"]["p50"], 10000)
        self.assertEqual(payload["node"]["peers"], 42)
        self.assertEqual(payload["status"], "live")
        for _ in range(4):
            self.node.extend()
        self.collector.ingest_once(self.store)
        payload = self.collector.payload(self.store, "1h")
        self.assertEqual(payload["activation"]["state"], "active")
        self.assertEqual(payload["activation"]["blocksSinceActivation"], 1)
        self.assertEqual(payload["chain"]["targetSpacingSeconds"], 25)
        self.assertEqual(payload["chain"]["intervals"]["postNu7"]["count"], 2)
        self.assertEqual(payload["rules"]["atTip"]["nsmPercent"], 60)
        self.assertEqual(len(payload["recentBlocks"]), 8)


class HttpTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.node = FakeNode()
        self.collector, self.store = new_collector(self.node, self.tmp.name)
        self.collector.clock = lambda: 1_770_000_000.0
        self.collector.ingest_once(self.store)
        self.collector.scrape_once(self.store)
        self.server = BoundedHTTPServer(("127.0.0.1", 0), Handler, self.collector)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.store.close()
        self.tmp.cleanup()

    def get(self, path):
        try:
            with urllib.request.urlopen(self.base + path, timeout=5) as response:
                return response.status, response.read(), dict(response.headers)
        except urllib.error.HTTPError as error:
            return error.code, error.read(), dict(error.headers)

    def test_routes(self):
        status, body, _ = self.get("/healthz")
        self.assertEqual(status, 200)
        self.collector.clock = lambda: 1_770_000_000.0 + 2 * collector_module.FRESHNESS_SECONDS
        self.assertEqual(self.get("/healthz")[0], 503, "a scrape older than the freshness window is stale")
        self.collector.clock = lambda: 1_770_000_000.0
        status, body, headers = self.get("/v1/dashboard?window=1h")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["series"]["window"], "1h")
        self.assertIn("max-age=5", headers["Cache-Control"])
        status, body, _ = self.get("/v1/dashboard?window=forever")
        self.assertEqual(status, 400)
        status, body, _ = self.get("/")
        self.assertEqual((status, body), (200, b"<html>test</html>"))
        status, _, _ = self.get("/nope")
        self.assertEqual(status, 404)

    def test_rate_limit(self):
        self.server.limiter.limit = 2
        self.assertEqual(self.get("/healthz")[0], 200)
        self.assertEqual(self.get("/healthz")[0], 200)
        self.assertEqual(self.get("/healthz")[0], 429)


if __name__ == "__main__":
    unittest.main()
