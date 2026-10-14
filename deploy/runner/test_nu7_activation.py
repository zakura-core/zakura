"""Activation boundary and durable selection tests with deterministic RPCs."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from nu7_activation import ActivationFeed, atomic_json


def manifest(magic="fa1af9bf"):
    config = '[network]\nnetwork = "Testnet"\n'
    return {"schemaVersion": 1, "nodeRevision": "a" * 40, "config": config,
            "configSha256": hashlib.sha256(config.encode()).hexdigest(),
            "network": {"name": "Testnet", "magic": magic,
                        "activationHeight": 100, "branchId": "77190ad9"}}


class FakeRPC:
    def __init__(self):
        self.height = 102
        self.bad = set()
        self.offline = set()
        self.calls = []

    def __call__(self, url, method, params=None):
        self.calls.append((url, method, params))
        url = url.removeprefix("http://")
        if url in self.offline:
            raise OSError("offline")
        if method == "getblockchaininfo":
            return {"chain": "test", "blocks": self.height, "bestblockhash": f"{self.height:064x}",
                    "nsmValueBalanceZat": 700, "upgrades": {
                        "77190ad9": {"activationheight": 100, "status": "active" if self.height >= 100 else "pending"}}}
        if method == "getblockhash":
            if url in self.bad and params[0] >= 100:
                return "f" * 64
            return f"{params[0]:064x}"
        if method == "getblockheader":
            return {"height": int(params[0], 16), "hash": params[0], "time": 990 - (102 - int(params[0], 16)) * 25, "difficulty": 3}
        if method == "getnetworkparameters":
            raise AssertionError("the service must use existing RPCs")
        if method == "getinfo":
            return {"build": "test-" + "a" * 12}
        raise AssertionError(method)


class Staging:
    def response(self):
        return 200, {"status": "live", "observedAt": 1000, "chain": {"height": 102}}


class ActivationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.now = 1000
        self.rpc = FakeRPC()
        self.config = {"armed": True, "stateFile": str(self.root / "state.json"),
                       "staging": {"manifest": {**manifest(), "network": {**manifest()["network"], "activationHeight": 80}}, "rpcUrl": "http://staging"},
                       "public": {"manifest": manifest(), "identityCheckpoint": {"height": 90, "hash": f"{90:064x}"},
                                  "nodes": [{"name": str(i), "rpcUrl": "http://" + str(i)} for i in range(3)],
                                  "reference": {"name": "reference", "rpcUrl": "http://reference"}}}
        self.config_path = self.root / "config.json"
        self.config_path.write_text(json.dumps(self.config))
        self.feed = ActivationFeed(self.config_path, Staging(), lambda: self.now, self.rpc)

    def tearDown(self):
        self.feed.owner.close()
        self.temporary.cleanup()

    def poll(self):
        self.feed.poll()
        self.now += 10

    def test_public_reorg_observation_is_persisted_and_retained_during_outage(self):
        for _ in range(3):
            self.poll()
        self.assertIsNone(self.feed.response()[1]["status"]["observation"]["reorgs24h"])
        self.poll()
        observation = self.feed.response()[1]["status"]["observation"]
        self.assertEqual(observation["reorgs24h"], 0)
        self.assertEqual(observation["since"], 1020)
        self.feed.owner.close()
        self.feed = ActivationFeed(self.config_path, Staging(), lambda: self.now, self.rpc)
        self.poll()
        self.assertEqual(self.feed.response()[1]["status"]["observation"]["since"], 1020)
        recorded = copy.deepcopy(self.feed.state["publicReorgObservation"])
        self.rpc.offline = {"0", "1", "2", "reference"}
        self.poll()
        self.assertEqual(self.feed.response()[1]["status"]["status"], "unavailable")
        self.assertEqual(self.feed.state["publicReorgObservation"], recorded)

    def test_existing_rpcs_publish_after_a_release_upgrade(self):
        original = self.rpc
        def released_node(url, method, params=None):
            if method == "getnetworkparameters":
                raise ValueError("Method not found")
            if method == "getinfo":
                return {"build": "release-" + "b" * 12}
            return original(url, method, params)
        self.feed.rpc = released_node
        for _ in range(3):
            self.poll()
        payload = self.feed.response()[1]
        self.assertEqual(payload["selectedProfile"], "public-testnet")
        self.assertEqual(payload["status"]["status"], "live")
        self.assertIsNone(payload["rules"])
        first = payload["generatedAt"]
        self.rpc.height += 1
        self.poll()
        payload = self.feed.response()[1]
        self.assertGreater(payload["generatedAt"], first)
        self.assertEqual(payload["status"]["chain"]["height"], 103)
        self.assertFalse(any(method == "getnetworkparameters" for _, method, _ in self.rpc.calls))

    def test_three_consecutive_passes_select_once(self):
        self.poll()
        self.poll()
        self.assertEqual(self.feed.response()[1]["selectedProfile"], "staging")
        self.poll()
        payload = self.feed.response()[1]
        self.assertEqual(payload["selectedProfile"], "public-testnet")
        self.assertEqual(payload["status"]["nsm"]["balanceZat"], 700)
        self.assertIsNone(payload["rules"])
        self.assertEqual(payload["network"]["configSha256"], manifest()["configSha256"])

    def test_no_selection_before_confirmation_height(self):
        for height in (99, 100, 101):
            self.rpc.height = height
            for _ in range(3):
                self.poll()
            self.assertEqual(self.feed.state["selectedProfile"], "staging")

    def test_reference_disagreement_resets_consecutive_passes(self):
        self.poll()
        self.rpc.bad.add("reference")
        self.poll()
        self.assertEqual(self.feed.state["consecutivePasses"], 0)
        self.rpc.bad.clear()
        self.poll()
        self.poll()
        self.assertEqual(self.feed.state["selectedProfile"], "staging")
        self.poll()
        self.assertEqual(self.feed.state["selectedProfile"], "public-testnet")

    def test_two_nodes_suffice_but_reference_is_mandatory(self):
        self.rpc.offline.add("2")
        for _ in range(3):
            self.poll()
        self.assertEqual(self.feed.state["selectedProfile"], "public-testnet")
        self.rpc.offline.add("reference")
        self.poll()
        self.assertEqual(self.feed.response()[1]["selectedProfile"], "public-testnet")
        self.assertEqual(self.feed.response()[1]["status"]["status"], "degraded")

    def test_restart_and_outage_never_restore_staging(self):
        for _ in range(3):
            self.poll()
        self.feed.owner.close()
        self.feed = ActivationFeed(self.config_path, Staging(), lambda: self.now, self.rpc)
        self.rpc.offline.update({"0", "1", "2", "reference"})
        self.poll()
        payload = self.feed.response()[1]
        self.assertEqual(payload["selectedProfile"], "public-testnet")
        self.assertEqual(payload["status"]["status"], "unavailable")
        self.assertEqual(payload["generatedAt"], 1020)

    def test_fresh_observations_can_have_long_minimum_difficulty_gaps(self):
        self.poll()
        self.now += 451
        self.poll()
        self.poll()
        self.assertEqual(self.feed.state["selectedProfile"], "public-testnet")
        source = self.feed.observe(self.config["public"]["nodes"][0], self.now)
        self.assertTrue(source["fresh"])
        self.assertGreater(source["progressAgeSeconds"], 450)

    def test_old_observation_does_not_count_as_fresh(self):
        nodes = [self.feed.observe(n, self.now) for n in self.config["public"]["nodes"]]
        reference = self.feed.observe(self.config["public"]["reference"], self.now, False)
        self.assertIsNone(self.feed.agreement(nodes, reference, self.now + 61))

    def test_slow_hash_reads_cannot_extend_freshness(self):
        nodes = [self.feed.observe(n, self.now) for n in self.config["public"]["nodes"]]
        reference = self.feed.observe(self.config["public"]["reference"], self.now, False)
        self.feed.clock = lambda: self.now + 61
        self.assertIsNone(self.feed.agreement(nodes, reference, self.now))

    def test_release_revision_does_not_disable_chain_observation(self):
        original = self.rpc
        def wrong_binary(url, method, params=None):
            return {"build": "test-" + "b" * 12} if method == "getinfo" else original(url, method, params)
        self.feed.rpc = wrong_binary
        observed = self.feed.observe(self.config["public"]["nodes"][0], self.now)
        self.assertTrue(observed["fresh"])
        self.assertEqual(observed["buildVersion"], "test-" + "b" * 12)
        self.assertNotIn("sourceRevision", observed)

    def test_forward_tip_change_retains_the_sampled_chain_observation(self):
        original = self.rpc
        count = 0
        def moved(url, method, params=None):
            nonlocal count
            if method == "getblockchaininfo":
                count += 1
                result = original(url, method, params)
                if count > 1:
                    result["blocks"] += 1
                return result
            return original(url, method, params)
        self.feed.rpc = moved
        result = self.feed.observe(self.config["public"]["nodes"][0], self.now)
        self.assertTrue(result["fresh"])
        self.assertEqual(result["height"], self.rpc.height)
        self.assertNotIn("rules", result)

    def test_replaced_sampled_block_is_rejected(self):
        original = self.rpc
        replaced = False
        def reorg(url, method, params=None):
            nonlocal replaced
            value = original(url, method, params)
            if method == "getinfo":
                replaced = True
            if replaced and method == "getblockhash" and params[0] == self.rpc.height:
                return "f" * 64
            return value
        self.feed.rpc = reorg
        observed = self.feed.observe(self.config["public"]["nodes"][0], self.now)
        self.assertFalse(observed["fresh"])

    def test_nsm_balance_belongs_to_the_sampled_height(self):
        nodes = [self.feed.observe(n, self.now) for n in self.config["public"]["nodes"]]
        reference = self.feed.observe(self.config["public"]["reference"], self.now, False)
        agreement = self.feed.agreement(nodes, reference, self.now)
        self.rpc.height += 1
        original = self.rpc
        def advanced(url, method, params=None):
            value = original(url, method, params)
            if method == "getblockchaininfo":
                value["nsmValueBalanceZat"] = 900
            return value
        self.feed.rpc = advanced
        status, _ = self.feed.public_status(nodes, agreement, self.now)
        self.assertEqual(status["chain"]["height"], 102)
        self.assertEqual(status["nsm"]["balanceZat"], 700)

    def test_process_lock_and_corrupt_state_fail_closed(self):
        with self.assertRaises(BlockingIOError):
            ActivationFeed(self.config_path, Staging(), lambda: self.now, self.rpc)
        self.feed.owner.close()
        self.root.joinpath("state.json").write_text("bad")
        with self.assertRaises(json.JSONDecodeError):
            ActivationFeed(self.config_path, Staging(), lambda: self.now, self.rpc)

    def test_missing_selected_state_fails_closed(self):
        for _ in range(3):
            self.poll()
        self.feed.owner.close()
        self.feed.path.unlink()
        with self.assertRaisesRegex(ValueError, "operator repair"):
            ActivationFeed(self.config_path, Staging(), lambda: self.now, self.rpc)
        self.assertFalse(self.feed.path.exists())

    def test_intact_legacy_selection_is_adopted_without_reset(self):
        for _ in range(3):
            self.poll()
        self.feed.owner.close()
        marker = self.feed.path.with_suffix(".initialized")
        marker.unlink()
        self.feed = ActivationFeed(self.config_path, Staging(), lambda: self.now, self.rpc)
        self.assertEqual(self.feed.state["selectedProfile"], "public-testnet")
        self.assertTrue(marker.exists())

    def test_missing_legacy_state_with_existing_lock_fails_closed(self):
        self.feed.owner.close()
        self.feed.path.unlink()
        self.feed.path.with_suffix(".initialized").unlink()
        with self.assertRaisesRegex(ValueError, "operator repair"):
            ActivationFeed(self.config_path, Staging(), lambda: self.now, self.rpc)

    def test_wrong_identity_and_manifest_checksum_rejected(self):
        self.config["public"]["manifest"]["configSha256"] = "bad"
        self.config_path.write_text(json.dumps(self.config))
        self.feed.owner.close()
        with self.assertRaisesRegex(ValueError, "checksum"):
            ActivationFeed(self.config_path, Staging(), lambda: self.now, self.rpc)

    def test_public_selection_persists_even_when_chain_collection_fails(self):
        self.feed.public_status = mock.Mock(side_effect=ValueError("failed"))
        for _ in range(3):
            self.poll()
        self.assertEqual(json.loads(self.feed.path.read_text())["selectedProfile"], "public-testnet")
        self.assertEqual(self.feed.response()[1]["status"]["status"], "unavailable")

    def test_dispatch_failure_retries_then_stops_after_success(self):
        self.feed.config["dispatch"] = True
        self.feed.state["selectionId"] = "test"
        self.feed.state["lastPublicEnvelope"] = {"test": True}
        with mock.patch.dict("os.environ", {"NU7_DISPATCH_TOKEN": "test-only"}), \
                mock.patch("urllib.request.urlopen", side_effect=OSError):
            self.feed.dispatch()
        self.assertNotIn("publicationDispatched", self.feed.state)
        response = mock.MagicMock()
        response.__enter__.return_value.status = 204
        with mock.patch.dict("os.environ", {"NU7_DISPATCH_TOKEN": "test-only"}), \
                mock.patch("urllib.request.urlopen", return_value=response) as request:
            self.feed.dispatch()
            self.feed.dispatch()
        self.assertEqual(request.call_count, 1)
        self.assertTrue(self.feed.state["publicationDispatched"])

    def test_node_on_a_different_chain_is_excluded(self):
        self.rpc.bad.add("0")
        for _ in range(3):
            self.poll()
        payload = self.feed.response()[1]
        self.assertEqual(payload["status"]["status"], "live")
        self.assertIsNone(payload["rules"])
        self.assertEqual(self.feed.state["selectionEvidence"]["nodes"], ["1", "2"])

    def test_activation_hash_disagreement_does_not_select(self):
        self.rpc.offline.add("2")
        self.rpc.bad.add("0")
        for _ in range(3):
            self.poll()
        self.assertEqual(self.feed.state["selectedProfile"], "staging")

    def test_higher_tip_requires_corroboration_at_that_height(self):
        original = self.rpc
        def higher(url, method, params=None):
            value = original(url, method, params)
            if url == "http://0" and method == "getblockchaininfo":
                value.update(blocks=104, bestblockhash=f"{104:064x}")
            return value
        self.feed.rpc = higher
        for _ in range(3):
            self.poll()
        payload = self.feed.response()[1]
        self.assertEqual(payload["selectedProfile"], "public-testnet")
        self.assertEqual(payload["status"]["chain"]["height"], 102)
        self.assertIsNone(payload["rules"])

    def test_duplicate_normalized_rpc_endpoints_are_rejected(self):
        self.feed.owner.close()
        for duplicate in ("http://0", "HTTP://0:80/", "http://user:password@0/?token=different"):
            with self.subTest(endpoint=duplicate):
                self.config["public"]["nodes"][1]["rpcUrl"] = duplicate
                self.config_path.write_text(json.dumps(self.config))
                with self.assertRaisesRegex(ValueError, "distinct RPC"):
                    ActivationFeed(self.config_path, Staging(), lambda: self.now, self.rpc)

    def test_reference_endpoint_and_name_collisions_are_rejected(self):
        self.feed.owner.close()
        self.config["public"]["reference"]["rpcUrl"] = "http://0:80/"
        self.config_path.write_text(json.dumps(self.config))
        with self.assertRaisesRegex(ValueError, "independent"):
            ActivationFeed(self.config_path, Staging(), lambda: self.now, self.rpc)
        self.config["public"]["reference"]["rpcUrl"] = "http://reference"
        self.config["public"]["reference"]["name"] = "0"
        self.config_path.write_text(json.dumps(self.config))
        with self.assertRaisesRegex(ValueError, "distinct source name"):
            ActivationFeed(self.config_path, Staging(), lambda: self.now, self.rpc)

class ReferenceAdapterTests(unittest.TestCase):
    def test_compact_hash_byte_order_and_height_are_verified(self):
        import base64
        feed = object.__new__(ActivationFeed)
        expected = "0011c54eccb61d4c1ec70f3f03bb8a534e7e8cb034b54384de389996a9342f38"
        feed.lightwallet = mock.Mock(return_value={"height": "4453700", "hash":
            base64.b64encode(bytes.fromhex(expected)[::-1]).decode()})
        self.assertEqual(feed.lightwallet_hash({"hashByteOrder": "little"}, 4453700), expected)
        with self.assertRaisesRegex(ValueError, "wrong height"):
            feed.lightwallet_hash({"hashByteOrder": "little"}, 4453701)
        with self.assertRaisesRegex(ValueError, "not verified"):
            feed.lightwallet_hash({"hashByteOrder": "guess"}, 4453700)

    def test_reference_uses_identity_and_current_branch(self):
        feed = object.__new__(ActivationFeed)
        feed.config = {"public": {"identityCheckpoint": {"height": 1, "hash": "trusted"},
                                  "manifest": {"network": {"branchId": "77190ad9"}}}}
        feed.progress = {}
        feed.lightwallet = mock.Mock(return_value={"chainName": "test", "blockHeight": "102",
                                                   "consensusBranchId": "77190ad9"})
        feed.lightwallet_hash = mock.Mock(side_effect=["trusted", "tip"])
        observed = feed.observe_lightwallet({"name": "ref"}, 1000)
        self.assertTrue(observed["active"])
        self.assertTrue(observed["fresh"])
        feed.lightwallet_hash = mock.Mock(return_value="wrong")
        with self.assertRaisesRegex(ValueError, "identity mismatch"):
            feed.observe_lightwallet({"name": "ref"}, 1000)

    def test_reference_deadline_kills_and_reaps_process(self):
        import subprocess
        feed = object.__new__(ActivationFeed)
        process = mock.Mock()
        process.wait.side_effect = [subprocess.TimeoutExpired("grpcurl", 15), 0]
        with mock.patch("subprocess.Popen", return_value=process):
            with self.assertRaisesRegex(OSError, "deadline"):
                feed.lightwallet({"grpcurlPath": "/usr/local/bin/grpcurl", "endpoint": "example:443"}, "GetLightdInfo", {})
        process.kill.assert_called_once()
        self.assertEqual(process.wait.call_count, 2)


class PublicReorgObservationTests(unittest.TestCase):
    def setUp(self):
        self.feed = object.__new__(ActivationFeed)
        self.feed.state = {}
        self.ancestor = "a" * 64
        self.ancestry_failure = None
        self.tip_override = None
        def rpc(url, method, params=None):
            if method == "getblockchaininfo":
                return self.tip_override or self.tip
            if self.ancestry_failure:
                raise self.ancestry_failure
            return self.ancestor
        self.feed.rpc = mock.Mock(side_effect=rpc)

    def observe(self, height=102, block_hash="a" * 64, now=1000, url="primary"):
        self.tip = {"blocks": height, "bestblockhash": block_hash}
        return self.feed.record_public_reorgs(height, block_hash, url, now)

    def test_first_tip_is_unknown_then_unchanged_tip_is_measured_zero(self):
        self.assertEqual(self.observe(), {"reorgs24h": None, "since": 1000})
        self.assertEqual(self.observe(now=1010), {"reorgs24h": 0, "since": 1000})
        self.assertFalse(any(call.args[1] == "getblockhash" for call in self.feed.rpc.call_args_list))

    def test_extension_and_primary_switch_are_not_reorgs(self):
        self.observe()
        self.assertEqual(self.observe(103, "b" * 64, 1010, "other")["reorgs24h"], 0)
        self.feed.rpc.assert_any_call("other", "getblockhash", [102])

    def test_same_height_and_taller_fork_are_each_one_event(self):
        self.observe()
        self.ancestor = "b" * 64
        self.assertEqual(self.observe(block_hash="b" * 64, now=1010)["reorgs24h"], 1)
        self.ancestor = "c" * 64
        self.assertEqual(self.observe(105, "d" * 64, 1020)["reorgs24h"], 2)
        self.assertEqual(self.observe(105, "d" * 64, 1030)["reorgs24h"], 2)

    def test_height_drop_is_one_event_without_reading_nonexistent_height(self):
        self.observe()
        self.assertEqual(self.observe(100, "b" * 64, 1010)["reorgs24h"], 1)
        self.assertFalse(any(call.args[1] == "getblockhash" for call in self.feed.rpc.call_args_list))

    def test_unavailable_ancestry_preserves_last_verified_observation(self):
        self.observe()
        before = copy.deepcopy(self.feed.state)
        for failure in (OSError("offline"), "not a hash"):
            self.ancestry_failure = failure if isinstance(failure, Exception) else None
            self.ancestor = failure
            with self.assertRaises((OSError, ValueError)):
                self.observe(103, "b" * 64, 1010)
            self.assertEqual(self.feed.state, before)

    def test_tip_moving_during_ancestry_check_cannot_count_or_advance(self):
        self.observe()
        before = copy.deepcopy(self.feed.state)
        self.ancestor = "d" * 64
        self.tip_override = {"blocks": 104, "bestblockhash": "c" * 64}
        with self.assertRaisesRegex(ValueError, "tip moved"):
            self.observe(103, "b" * 64, 1010)
        self.assertEqual(self.feed.state, before)

    def test_restart_retains_events_and_compares_the_persisted_tip(self):
        self.observe()
        self.observe(100, "b" * 64, 1010)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            atomic_json(path, self.feed.state)
            self.feed.state = json.loads(path.read_text())
        self.assertEqual(self.observe(100, "b" * 64, 1020), {"reorgs24h": 1, "since": 1000})

    def test_rolling_window_expires_events_without_resetting_provenance(self):
        self.observe()
        self.observe(100, "b" * 64, 1010)
        self.assertEqual(self.observe(100, "b" * 64, 87410), {"reorgs24h": 1, "since": 1010})
        self.assertEqual(self.observe(100, "b" * 64, 87411), {"reorgs24h": 0, "since": 1011})

    def test_corrupt_history_and_clock_rollback_fail_without_zero(self):
        self.observe()
        for mutate in (lambda s:s.update(events=[999]), lambda s:s.update(events=[1100]),
                       lambda s:s.update(height=-1), lambda s:s.update(hash="bad")):
            before = copy.deepcopy(self.feed.state)
            mutate(self.feed.state["publicReorgObservation"])
            with self.assertRaises(ValueError):
                self.observe(now=1010)
            self.feed.state = before
        with self.assertRaises(ValueError):
            self.observe(now=990)


if __name__ == "__main__":
    unittest.main()
