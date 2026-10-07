"""Exercise boundaries where telemetry could mislead or leak private data."""
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
import threading
import time
import tempfile
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

    def test_processing_readings_survive_idle_without_filling_live_samples(self):
        now = time.time()
        active = d.metrics_parse('''state_contextual_total_duration_seconds{quantile="0.5"} 0.005
state_contextual_total_duration_seconds{quantile="0.95"} 0.009
state_contextual_total_duration_seconds{quantile="1"} 0.010
zakura_consensus_batch_duration_seconds{verifier="halo2",result="success",quantile="0.5"} 0.012
zakura_consensus_batch_duration_seconds{verifier="halo2",result="success",quantile="0.95"} 0.015
zakura_consensus_batch_duration_seconds{verifier="halo2",result="success",quantile="1"} 0.020
''')
        idle = d.metrics_parse('''state_contextual_total_duration_seconds{quantile="0.5"} 0
state_contextual_total_duration_seconds{quantile="0.95"} 0
state_contextual_total_duration_seconds{quantile="1"} 0
zakura_consensus_batch_duration_seconds{verifier="halo2",result="success",quantile="0.5"} 0
zakura_consensus_batch_duration_seconds{verifier="halo2",result="success",quantile="0.95"} 0
zakura_consensus_batch_duration_seconds{verifier="halo2",result="success",quantile="1"} 0
sync_block_applying 0
''')
        self.c.update_metrics(active, now - 60)
        self.c.update_metrics(idle, now)
        saved = self.c.snapshot()["last_processing"]
        self.assertEqual(saved["stages"], [{"name": "Contextual validation", "p50_ms": 5,
                                          "p95_ms": 9, "observed_at": now - 60}])
        self.assertEqual(saved["verifiers"][0]["p95_ms"], 15)
        self.assertEqual(saved["verifiers"][0]["observed_at"], now - 60)
        self.assertIsNone(self.c.state["metrics"]["contextual_ms"])
        self.assertIsNone(self.c.sample(now)["contextual_ms"])
        self.assertEqual(self.c.sample(now)["applying"], 0)
        self.c.source("metrics", False)
        self.assertEqual(self.c.snapshot()["last_processing"], saved)
        self.assertFalse(self.c.snapshot()["sources"]["metrics"]["fresh"])
        self.c.update_metrics(active, now)
        self.assertEqual(self.c.snapshot()["last_processing"]["stages"][0]["observed_at"], now)

    def test_processing_readings_persist_across_dashboard_restarts(self):
        now = time.time()
        metrics = d.metrics_parse('''state_contextual_total_duration_seconds{quantile="0.5"} 0.01
state_contextual_total_duration_seconds{quantile="0.95"} 0.02
state_contextual_total_duration_seconds{quantile="1"} 0.03
''')
        with tempfile.TemporaryDirectory() as directory:
            args = SimpleNamespace(history=str(Path(directory) / "dashboard.sqlite3"), node="test", build="test")
            first = d.Collector(args)
            try:
                first.update_metrics(metrics, now)
                expected = first.snapshot()["last_processing"]
            finally:
                first.pool.shutdown(wait=True)
                first.store.db.close()
            second = d.Collector(args)
            try:
                self.assertEqual(second.snapshot()["last_processing"], expected)
                self.assertEqual(second.state["metrics"], {})
                self.assertEqual(second.snapshot()["sources"], {})
            finally:
                second.pool.shutdown(wait=True)
                second.store.db.close()

    def test_processing_retention_expires_and_never_initializes_from_empty_summaries(self):
        now = time.time()
        self.c.update_metrics({}, now)
        self.assertEqual(self.c.snapshot()["last_processing"], {"stages": [], "verifiers": []})
        self.c.state["last_processing"]["stages"] = [
            {"name": "Expired", "p50_ms": 1, "p95_ms": 2, "observed_at": now - 86401},
            {"name": "Future", "p50_ms": 1, "p95_ms": 2, "observed_at": now + 100}]
        self.assertEqual(self.c.snapshot()["last_processing"]["stages"], [])
        self.c.update_metrics({}, now)
        self.assertEqual(self.c.store.processing(), {"stages": [], "verifiers": []})

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
                 "/proc/uptime": "456.75 123.00\n",
                 "/proc/stat": "cpu 1 2 3 4 5 6 7 8 9 10\n",
                 "/proc/net/dev": "header\nheader\nlo: 1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0\neth0: 50 0 1 2 0 0 0 0 70 0 3 4 0 0 0 0\n"}
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
        self.assertEqual(observation["counters"]["cpu"]["total"], 36)
        self.assertNotIn("lo", observation["counters"]["interfaces"])
        self.assertEqual(observation["counters"]["interfaces"]["eth0"]["drops"], 6)

    def test_chain_tps_excludes_boundary_block_and_coinbase(self):
        blocks = [d.block_public(block(i, previous=f"a{i-1:063x}")) for i in range(1, 40)]
        for b in blocks:
            b.update(time=b["height"] * 75, canonical=True, transactions=4)
        activity = d.chain_activity(blocks, blocks[-1]["hash"])
        self.assertEqual(activity["blocks"], 30)
        self.assertEqual(activity["transactions"], 120)
        self.assertEqual(activity["seconds"], 2250)
        self.assertEqual(activity["tps"], 120 / 2250)
        self.assertEqual(activity["user_tps"], 90 / 2250)
        self.assertEqual(activity["from_height"], 9)

    def test_chain_tps_rejects_unlinked_or_nonpositive_intervals(self):
        blocks = [d.block_public(block(i, previous=f"a{i-1:063x}")) for i in range(1, 4)]
        for b in blocks:
            b.update(time=100, canonical=True)
        self.assertIsNone(d.chain_activity(blocks, blocks[-1]["hash"]))
        blocks[-1]["time"] = 200
        blocks[1]["canonical"] = False
        self.assertIsNone(d.chain_activity(blocks, blocks[-1]["hash"]))
        blocks[1]["canonical"] = True
        self.assertEqual(d.chain_activity(blocks, blocks[-1]["hash"])["blocks"], 2)
        self.assertIsNone(d.chain_activity(blocks, "b" * 64))

    def test_peer_latency_omits_missing_negative_and_private_values(self):
        peers = [{"addr": "secret", "inbound": True, "pingtime": v, "version": 170160}
                 for v in (0.1, 0.2, 0.3, None, -1)]
        self.c.update_peers(peers, time.time())
        state = self.c.snapshot()
        self.assertEqual(state["peer_latency"], {"measured": 3, "unknown": 2, "p50_ms": 200, "p95_ms": 300})
        self.assertEqual(state["peer_details"][0]["ping_ms"], 300)
        self.assertNotIn("secret", json.dumps(state))
        self.assertEqual(len(state["peer_details"]), 5)

    def test_new_network_series_preserve_labels_and_hide_error_reasons(self):
        before = d.metrics_parse('''zcash_net_in_messages{command="ping"} 10
zcash_net_in_messages{command="block"} 20
zakura_p2p_queue_depth{stream_kind="block_sync"} 2
mempool_failed_verify_tasks_total{reason="private-secret"} 3
''')
        after = d.metrics_parse('''zcash_net_in_messages{command="ping"} 40
zcash_net_in_messages{command="block"} 35
zakura_p2p_queue_depth{stream_kind="block_sync"} 4
mempool_failed_verify_tasks_total{reason="private-secret"} 6
''')
        self.c.update_metrics(before, 100)
        self.assertIsNone(self.c.state["messages"][0]["in_ps"])
        self.c.update_metrics(after, 115)
        messages = {m["name"]: m for m in self.c.state["messages"]}
        self.assertEqual(messages["ping"]["in_ps"], 2)
        self.assertEqual(messages["block"]["in_ps"], 1)
        self.assertIsNone(messages["ping"]["out_ps"])
        self.assertEqual(self.c.state["streams"][1]["last_depth"], 4)
        self.assertEqual(self.c.state["transaction_flow"][5]["rate"], 0.2)
        self.assertIsNone(self.c.state["transaction_flow"][2]["total"])
        self.assertNotIn("private-secret", json.dumps(self.c.snapshot()))

    def test_host_rates_need_two_samples_and_handle_interface_resets(self):
        def update(total, idle, rx, when):
            self.c.update_host({"host": {}, "service": "active", "counters": {
                "cpu": {"total": total, "idle": idle, "iowait": 10}, "cores": 8,
                "interfaces": {"eth0": {"rx": rx, "tx": rx, "errors": 0, "drops": 0}}}}, when)
        update(1000, 500, 100, 100)
        self.assertIsNone(self.c.state["host"]["cpu_percent"])
        self.assertIsNone(self.c.state["host"]["host_rx_bps"])
        update(1200, 650, 400, 130)
        self.assertEqual(self.c.state["host"]["cpu_percent"], 25)
        self.assertEqual(self.c.state["host"]["host_rx_bps"], 10)
        self.assertEqual(self.c.state["host"]["host_errors_ps"], 0)
        update(1500, 800, 20, 160)
        self.assertIsNone(self.c.state["host"]["host_rx_bps"])
        update(1900, 1000, 400, 400)
        self.assertIsNone(self.c.state["host"]["cpu_percent"])
        self.assertIsNone(self.c.state["host"]["host_rx_bps"])

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
