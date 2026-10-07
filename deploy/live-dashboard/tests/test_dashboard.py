"""Exercise boundaries where telemetry could mislead or leak private data."""
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
import threading
import time
import unittest
from unittest.mock import patch
import urllib.error
import urllib.request

spec = importlib.util.spec_from_file_location("dashboard", Path(__file__).resolve().parents[1] / "dashboard.py")
d = importlib.util.module_from_spec(spec)
spec.loader.exec_module(d)


def block(height, branch="a", previous=None):
    return {"hash": f"{branch}{height:063x}", "height": height, "time": time.time(),
            "tx": ["one", "two"], "size": 200, "previousblockhash": previous,
            "trees": {"sapling": {"size": 10}}, "solution": "not public"}


class CollectorTests(unittest.TestCase):
    def setUp(self):
        self.c = d.Collector(SimpleNamespace(history=":memory:", node="test", build="test"))

    def tearDown(self):
        self.c.pool.shutdown(wait=True)
        self.c.store.db.close()

    def test_rates_require_two_observations_and_survive_reset(self):
        self.assertIsNone(d.rate(None, 10, 15))
        self.assertIsNone(d.rate(100, 10, 15))
        self.assertIsNone(d.rate(100, 200, 121))
        self.assertEqual(d.rate(100, 130, 15), 2)
        self.assertEqual(d.rate(100, 100, 15), 0)

    def test_metrics_filter_private_series_and_do_not_add_quantiles(self):
        metrics = d.metrics_parse('''zcash_net_peers_cache{remote_ip="secret"} 1
rpc_request_duration_seconds{method="a",quantile="0.95"} 0.2
rpc_request_duration_seconds{method="b",quantile="0.95"} 0.3
rpc_request_duration_seconds{method="c",quantile="0.95"} NaN
sync_block_applying 0
''')
        self.assertNotIn("secret", json.dumps(metrics))
        self.assertIsNone(d.quantile(metrics, "rpc_request_duration_seconds"))
        self.assertEqual(d.quantile(metrics, "rpc_request_duration_seconds", method="a"), 200)
        self.assertEqual(d.metric(metrics, "sync_block_applying"), 0)
        self.assertIsNone(d.metric(metrics, "missing_metric"))

    def test_missing_and_failed_sources_make_gaps_not_zeroes(self):
        now = time.time()
        with self.c.lock:
            self.c.state["chain"] = {"height": 123, "lag": 0}
            self.c.state["metrics"] = {"applying": 5}
            self.c.source("chain", True, now - 30)
            self.c.source("metrics", True, now)
        sample = self.c.sample(now)
        self.assertIsNone(sample["height"])
        self.assertEqual(sample["applying"], 5)
        with self.c.lock:
            self.c.source("metrics", False)
        self.assertIsNone(self.c.sample(now)["applying"])

    def test_empty_summary_is_not_zero_latency(self):
        metrics = d.metrics_parse('''state_block_writer_queue_duration_seconds{quantile="0.95"} 0
state_block_writer_queue_duration_seconds{quantile="1"} 0
''')
        self.assertIsNone(d.quantile(metrics, "state_block_writer_queue_duration_seconds"))

    def test_support_countdown_uses_current_height(self):
        self.c.state["chain"] = {"height": 123}
        self.c.state["metrics"] = {"support_height": 150, "support_blocks": 40}
        self.c.source("chain", True, time.time())
        self.assertEqual(self.c.snapshot()["metrics"]["support_blocks"], 27)
        self.c.source("chain", False)
        self.assertIsNone(self.c.snapshot()["metrics"]["support_blocks"])

    def test_fleet_and_peers_only_publish_allowlisted_fields(self):
        now = time.time()
        fleet = {"network": "mainnet", "majority_height": 100, "majority_hash": "a" * 64,
                 "config": {"path": "secret"}, "node": {"last_seen_at": now, "ssh": "secret",
                 "logs": ["secret"], "host": {"disk_path": "secret", "rss_bytes": 123}},
                 "reorgs": [{"demo": True, "from_height": 999}]}
        self.c.update_fleet(fleet, now)
        self.c.update_peers([{"addr": "secret", "subver": "Zakura", "inbound": True}], now)
        public = self.c.snapshot()
        self.assertNotIn("secret", json.dumps(public))
        self.assertEqual(public["host"]["rss_bytes"], 123)
        self.assertEqual(public["peer_summary"], {"inbound": 1, "outbound": 0, "total": 1})
        self.assertEqual(public["reorgs"], [])
        fleet["node"]["last_seen_at"] = now - 121
        with self.assertRaises(ValueError):
            self.c.update_fleet(fleet, now)

    def test_wrong_network_is_rejected(self):
        with self.assertRaises(ValueError):
            d.chain_public({"chain": "test", "bestblockhash": "a" * 64, "blocks": 1, "headers": 1})

    def test_stopped_local_node_keeps_host_observations_and_hides_private_fields(self):
        files = {"/proc/meminfo": "MemTotal: 8000 kB\nMemAvailable: 3000 kB\n",
                 "/proc/uptime": "456.75 123.00\n"}
        disk = SimpleNamespace(f_blocks=100, f_bavail=30, f_frsize=4096)
        service = SimpleNamespace(stdout="LoadState=loaded\nActiveState=inactive\nMainPID=0\nNRestarts=2\nPrivate=secret\n")
        with patch.object(d.Path, "read_text", lambda path: files[str(path)]), \
                patch.object(d.os, "statvfs", return_value=disk), \
                patch.object(d.os, "getloadavg", return_value=(0.1, 0.2, 0.3)), \
                patch.object(d.subprocess, "run", return_value=service) as run:
            observation = d.local_host("/", "zakura-dashboard-node.service")
        self.assertEqual(observation["service"], "inactive")
        self.assertIsNone(observation["host"]["rss_bytes"])
        self.assertEqual(observation["host"]["restart_count"], 2)
        self.assertEqual(observation["host"]["mem_available_bytes"], 3000 * 1024)
        self.assertEqual(observation["host"]["disk_free_bytes"], 30 * 4096)
        self.assertIsNone(observation["host"]["oom_kills_24h"])
        self.assertNotIn("secret", json.dumps(observation))
        self.assertEqual(run.call_args.kwargs["timeout"], 3)

    def test_local_process_exit_during_collection_does_not_fail_host_health(self):
        def read(path):
            if str(path) == "/proc/meminfo":
                return "MemTotal: 8000 kB\nMemAvailable: 3000 kB\n"
            if str(path) == "/proc/uptime":
                return "456.75 123.00\n"
            raise FileNotFoundError()
        disk = SimpleNamespace(f_blocks=100, f_bavail=30, f_frsize=4096)
        service = SimpleNamespace(stdout="LoadState=loaded\nActiveState=active\nMainPID=123\nNRestarts=0\n")
        with patch.object(d.Path, "read_text", read), \
                patch.object(d.os, "statvfs", return_value=disk), \
                patch.object(d.os, "getloadavg", return_value=(0.1, 0.2, 0.3)), \
                patch.object(d.subprocess, "run", return_value=service):
            observation = d.local_host("/", "zakura-dashboard-node.service")
        self.assertEqual(observation["service"], "active")
        self.assertIsNone(observation["host"]["rss_bytes"])
        self.assertEqual(observation["host"]["uptime_seconds"], 456.75)

    def test_reorg_and_return_to_known_tip_recompute_membership(self):
        a1 = block(1)
        a2 = block(2, previous=a1["hash"])
        b2 = block(2, "b", a1["hash"])
        blocks = {b["hash"]: b for b in (a1, a2, b2)}
        self.c.rpc = lambda method, params: blocks[params[0]]
        for tip, orphan in ((a2, None), (b2, a2), (a2, b2)):
            self.c.update_blocks({"hash": tip["hash"], "height": 2}, time.time())
            snapshot = {b["hash"]: b for b in self.c.snapshot()["blocks"]}
            self.assertTrue(snapshot[tip["hash"]]["canonical"])
            self.assertTrue(snapshot[a1["hash"]]["canonical"])
            if orphan:
                self.assertFalse(snapshot[orphan["hash"]]["canonical"])
        self.assertNotIn("solution", json.dumps(snapshot))

    def test_backfill_is_bounded_and_unlinked_history_is_unknown(self):
        blocks = [block(i, previous=f"a{i - 1:063x}" if i > 1 else None) for i in range(1, 20)]
        self.c.state["blocks"] = [d.block_public(blocks[0])]
        calls = []
        def rpc(method, params):
            calls.append(params[0])
            return next(b for b in blocks if b["hash"] == params[0])
        self.c.rpc = rpc
        self.c.update_blocks({"hash": blocks[-1]["hash"], "height": 19}, time.time())
        self.assertEqual(len(calls), 4)
        old = next(b for b in self.c.snapshot()["blocks"] if b["height"] == 1)
        self.assertIsNone(old["canonical"])

    def test_history_retention_and_sample_gaps(self):
        now = time.time()
        for t, v in ((now - 90000, 1), (now - 30, 2), (now, None)):
            self.c.store.save({"t": t, "height": v}, [])
        self.assertEqual([s["height"] for s in self.c.store.history(86400)], [2, None])

    def test_http_serves_only_cached_reads(self):
        self.c.rpc = lambda *_: self.fail("browser must not trigger RPC")
        server = d.Server(("127.0.0.1", 0), self.c)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        url = f"http://127.0.0.1:{server.server_port}"
        try:
            with urllib.request.urlopen(url + "/api/overview") as response:
                self.assertIn("sources", json.load(response))
                self.assertIn("frame-ancestors 'none'", response.headers["Content-Security-Policy"])
            for path, code in (("/api/history?window=forever", 400), ("/../../dashboard.py", 404), ("/healthz", 503)):
                with self.assertRaises(urllib.error.HTTPError) as error:
                    urllib.request.urlopen(url + path)
                self.assertEqual(error.exception.code, code)
                error.exception.close()
            with urllib.request.urlopen(url + "/") as response:
                self.assertIn(b"Block pipeline", response.read())
        finally:
            server.shutdown()
            server.server_close()
            worker.join()


if __name__ == "__main__":
    unittest.main()
