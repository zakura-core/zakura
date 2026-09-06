"""Synthetic metadata fixtures exercise reporting, never native node execution."""
import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).parents[1]))
from native_sync_series import digest, report_series


def write(path, value):
    path.write_text(json.dumps(value))


class SeriesTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.plan = {"schema": 1, "first_pair": {}, "pairs": []}
        self.specs = {}
        for index in (1, 2):
            pair = {"index": index, "order": ["candidate", "baseline"], "runs": {}}
            for policy in ("baseline", "candidate"):
                run = f"{policy}-{index:02}"
                config = f'[network]\ncache_dir = "/runs/{run}/network"\n[network.zakura]\ntrace_dir = "/runs/{run}/traces"\n[state]\ncache_dir = "/runs/{run}/state"\n'
                spec = {"schema": 1, "run": run, "client_count": 1, "server_revision": policy,
                        "capture_application_lifetimes": policy == "candidate", "hosts": {}}
                for host, role in (("server", "server"), ("client", "downloader")):
                    spec["hosts"][host] = {"role": role, "binary": policy if host == "server" else "fixed-client",
                                           "binary_sha256": policy if host == "server" else "fixed-client-hash",
                                           "config": config, "config_sha256": hashlib.sha256(config.encode()).hexdigest()}
                self.specs[run] = spec
                witness = self.save_spec(run)
                if index == 1:
                    self.plan["first_pair"][policy] = witness
                else:
                    pair["runs"][policy] = witness
                self.complete(run, 100 if policy == "baseline" else 90 if index == 1 else 105)
            if index == 2:
                self.plan["pairs"].append(pair)
        self.path = self.root / "series.json"
        write(self.path, self.plan)

    def save_spec(self, run):
        path = self.root / (run + ".json")
        write(path, self.specs[run])
        return {"file": path.name, "sha256": digest(path)}

    def update_planned_spec(self, run):
        witness = self.save_spec(run)
        policy = run.split("-", 1)[0]
        if run.endswith("01"):
            self.plan["first_pair"][policy] = witness
        else:
            self.plan["pairs"][0]["runs"][policy] = witness
        write(self.path, self.plan)

    def complete(self, run, seconds):
        spec = self.specs[run]
        resources = {"run": run, "hosts": {host: {"recording_complete": True,
                                                    "completion_seconds": seconds if host == "client" else None}
                                             for host in spec["hosts"]}}
        resources_path = self.root / (run + "-resources.json")
        write(resources_path, resources)
        audit = {"run": run, "resources_sha256": digest(resources_path), "hosts": {}}
        for host, config in spec["hosts"].items():
            directory = self.root / "evidence" / run / host
            extracted = directory / "extracted"
            extracted.mkdir(parents=True)
            write(extracted / "run-spec.json", spec)
            outcome = {"run": run, "host": host, "native_completed": True, "all_clients_reached_target": True}
            write(extracted / "run-outcome.json", outcome)
            archive = directory / (run + ".tar.zst")
            archive.write_bytes(b"metadata-only test fixture, not a node archive")
            audit["hosts"][host] = {"role": config["role"], "resources": resources["hosts"][host],
                                    "run_outcome": outcome, "archive_sha256": digest(archive)}
        write(self.root / (run + "-outcome-audit.json"), audit)
        write(self.root / (run + "-controller.json"),
              {"run": run, "host_environments": {"server": "fixed environment"},
               "remote_tool_hashes": {"server": "fixed helper hashes"}})

    def test_complete_pairs_keep_individual_changes_and_spread(self):
        result = report_series(self.path)
        self.assertTrue(result["all_pairs_complete"])
        self.assertEqual(result["pair_counts"], {"complete": 2, "failed": 0, "unavailable": 0})
        self.assertIsNone(result["pairs"][0]["planned_order"])
        client = result["clients"]["client"]
        self.assertAlmostEqual(client["minimum_change_percent"], -10)
        self.assertAlmostEqual(client["maximum_change_percent"], 5)
        self.assertAlmostEqual(client["median_change_percent"], -2.5)

    def test_missing_trial_stays_in_the_planned_denominator(self):
        (self.root / "candidate-02-outcome-audit.json").unlink()
        result = report_series(self.path)
        self.assertFalse(result["all_pairs_complete"])
        self.assertEqual(result["pair_counts"], {"complete": 1, "failed": 0, "unavailable": 1})
        self.assertEqual(result["clients"]["client"]["completed_pairs"], 1)

    def test_successful_native_pairs_preserve_failed_and_unavailable_captures(self):
        write(self.root / "candidate-01-capture-import.json", {
            "run": "candidate-01", "capture_import_ok": False, "returncode": 1,
            "stderr": "missing decoded messages"})
        result = report_series(self.path)
        self.assertTrue(result["all_pairs_complete"])
        self.assertFalse(result["all_required_captures_imported"])
        self.assertEqual(result["capture_counts"],
                         {"complete": 0, "failed": 1, "unavailable": 1, "not_requested": 2})
        failed = result["pairs"][0]["captures"]["candidate"]
        self.assertEqual(failed["import_record"]["stderr"], "missing decoded messages")

    def test_successful_capture_requires_unchanged_workload_bytes(self):
        for run in ("candidate-01", "candidate-02"):
            profile = self.root / (run + "-workload.json")
            write(profile, {"fixture": run})
            write(self.root / (run + "-capture-import.json"), {
                "run": run, "capture_import_ok": True, "returncode": 0,
                "profile_sha256": digest(profile)})
        self.assertTrue(report_series(self.path)["all_required_captures_imported"])
        profile.write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "workload differs"):
            report_series(self.path)

    def test_capture_claim_cannot_override_its_run_or_exit_status(self):
        for record in (
            {"run": "another-run", "capture_import_ok": False, "returncode": 1},
            {"run": "candidate-01", "capture_import_ok": True, "returncode": 1},
            {"run": "candidate-01", "capture_import_ok": False, "returncode": 0},
            {"run": "candidate-01", "capture_import_ok": "true", "returncode": 0},
        ):
            write(self.root / "candidate-01-capture-import.json", record)
            with self.subTest(record=record), self.assertRaises(ValueError):
                report_series(self.path)

    def test_failed_trial_is_not_a_fast_success(self):
        path = self.root / "candidate-02-outcome-audit.json"
        audit = json.loads(path.read_text())
        outcome = audit["hosts"]["client"]["run_outcome"]
        outcome.update(native_completed=False, all_clients_reached_target=False)
        write(self.root / "evidence/candidate-02/client/extracted/run-outcome.json", outcome)
        write(path, audit)
        result = report_series(self.path)
        self.assertEqual(result["pair_counts"], {"complete": 1, "failed": 1, "unavailable": 0})
        self.assertEqual(result["clients"]["client"]["completed_pairs"], 1)

    def test_preparation_failure_remains_visible_after_verified_resumption(self):
        run = "candidate-02"
        audit_path = self.root / (run + "-outcome-audit.json")
        saved_audit = audit_path.read_bytes()
        audit_path.unlink()
        failure_path = self.root / (run + "-supervision-failure.json")
        write(failure_path, {"run": run, "step": "prepare-native-run.py", "cleanup": {
            host: {"all_run_units_stopped": True, "stops": [], "recorder_forced_stop": False}
            for host in self.specs[run]["hosts"]}})
        report = report_series(self.path)
        self.assertEqual(report["pair_counts"]["failed"], 1)
        self.assertEqual(report["runs_with_supervision_failures"], 1)
        audit_path.write_bytes(saved_audit)
        with self.assertRaises(FileNotFoundError):
            report_series(self.path)
        resumed = {"run": run, "failure_sha256": digest(failure_path),
                   "original_controller_absent": True, "prepared_hosts": {
                       host: {"copy_complete": True, "native_start_absent": True}
                       for host in self.specs[run]["hosts"]}}
        resumed_path = self.root / (run + "-resumption.json")
        write(resumed_path, resumed)
        report = report_series(self.path)
        self.assertTrue(report["all_pairs_complete"])
        self.assertEqual(report["runs_with_supervision_failures"], 1)
        self.assertIn("supervision_failure", report["pairs"][1]["observations"]["candidate"])
        resumed["prepared_hosts"]["client"]["native_start_absent"] = False
        write(resumed_path, resumed)
        with self.assertRaisesRegex(ValueError, "resumption before native startup"):
            report_series(self.path)

    def test_changed_companion_or_unknown_condition_is_rejected(self):
        original = copy.deepcopy(self.specs["candidate-02"])
        for change in ("companion", "network_profile"):
            self.specs["candidate-02"] = copy.deepcopy(original)
            if change == "companion":
                self.specs["candidate-02"]["hosts"]["client"]["binary_sha256"] = "different"
            else:
                self.specs["candidate-02"]["network_profile"] = {"rtt_ms": 80}
            self.update_planned_spec("candidate-02")
            with self.assertRaisesRegex(ValueError, "paired conditions differ"):
                report_series(self.path)

    def test_server_rate_tuning_cannot_mix_repetitions(self):
        host = self.specs["candidate-02"]["hosts"]["server"]
        host["config"] += "\n[network.zakura.block_sync.get_blocks_regulation]\npeer_rate_bytes_per_second = 7\n"
        host["config_sha256"] = hashlib.sha256(host["config"].encode()).hexdigest()
        self.update_planned_spec("candidate-02")
        with self.assertRaisesRegex(ValueError, "repetition conditions changed"):
            report_series(self.path)

    def test_mutated_archive_and_spec_are_rejected(self):
        path = self.root / "candidate-02.json"
        path.write_text(path.read_text() + " ")
        with self.assertRaisesRegex(ValueError, "planned specification changed"):
            report_series(self.path)
        self.update_planned_spec("candidate-02")
        (self.root / "evidence/candidate-02/client/candidate-02.tar.zst").write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "archived bytes differ"):
            report_series(self.path)

    def test_runtime_change_cannot_mix_trials(self):
        path = self.root / "candidate-02-controller.json"
        original = json.loads(path.read_text())
        for field in ("host_environments", "remote_tool_hashes"):
            with self.subTest(field=field):
                controller = copy.deepcopy(original)
                controller[field]["server"] = "changed"
                write(path, controller)
                with self.assertRaisesRegex(ValueError, "runtime helpers or host environment changed"):
                    report_series(self.path)

    def test_recovery_run_is_not_an_ordinary_timing_trial(self):
        self.specs["candidate-02"]["finite_client_pause"] = {"seconds": 30}
        self.update_planned_spec("candidate-02")
        with self.assertRaisesRegex(ValueError, "recovery trials"):
            report_series(self.path)


if __name__ == "__main__":
    unittest.main()
