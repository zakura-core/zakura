"""Pressure evidence must preserve missing data, bounded memory use and failures."""
import argparse
import copy
import gc
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import weakref

SCRIPTS = Path(__file__).parents[1]
sys.path.insert(0, str(SCRIPTS))
from native_sync_pressure import MemoryPressure, pressure_total


def load(name):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def sample(start, total):
    pressure = f"some avg10=0 total={total}\nfull avg10=0 total=0\n"
    return {"schema": 1, "clock": "monotonic_ns", "unit_name": "test-node.service",
            "utc_ns": start, "sample_start_ns": start, "sample_end_ns": start + 100_000_000,
            "unit": {"MainPID": "123", "ControlGroup": "/test", "ActiveState": "active",
                     "Result": "success", "ExecMainStatus": "0"},
            "host": {"pressure/memory": pressure,
                     "meminfo": "MemTotal: 1024 kB\nMemAvailable: 128 kB\nCached: 512 kB\n"},
            "cgroup": {"memory.pressure": pressure}}


def summary(rows):
    accumulator = MemoryPressure()
    for row in rows:
        accumulator.add(row)
    return accumulator.report()


class PressureTests(unittest.TestCase):
    def test_peak_composition_does_not_combine_different_samples(self):
        rows = [sample(0, 0), sample(2_000_000_000, 1)]
        rows[0]["cgroup"].update({"memory.current": "1000", "memory.stat": "anon 100\nfile 900\ninactive_file 800\n"})
        rows[1]["cgroup"].update({"memory.current": "700", "memory.stat": "anon 200\nfile 500\ninactive_file 400\n"})
        result = summary(rows)["cgroup_memory"]
        self.assertEqual(result["peak_usage_sample"]["memory_current_bytes"], 1000)
        self.assertEqual(result["peak_usage_sample"]["memory_stat"]["anon"], 100)
        self.assertEqual(result["peak_anon_sample"]["memory_current_bytes"], 700)
        self.assertEqual(result["peak_anon_sample"]["memory_stat"]["anon"], 200)

    def test_limit_events_report_changes_not_initial_counts(self):
        rows = [sample(i * 2_000_000_000, 0) for i in range(3)]
        for row, count in zip(rows, [50, 54, 61]):
            row["cgroup"]["memory.events"] = f"max {count}\noom 0\n"
        result = summary(rows)["cgroup_memory"]["event_changes"]
        self.assertEqual(result["max"]["observed_change"], 11)
        self.assertEqual(result["max"]["valid_intervals"], 2)
        self.assertEqual(result["oom"]["observed_change"], 0)
        self.assertIsNone(result["high"]["observed_change"])
        self.assertIsNone(summary(rows[:1])["cgroup_memory"]["event_changes"]["max"]["observed_change"])

    def test_missing_and_reset_memory_counters_remain_explicit(self):
        rows = [sample(i * 2_000_000_000, 0) for i in range(4)]
        for row, count in zip(rows, [50, None, 52, 0]):
            row["cgroup"]["memory.events"] = {"error": "unavailable"} if count is None else f"max {count}\n"
        result = summary(rows)["cgroup_memory"]
        self.assertIsNone(result["peak_usage_sample"])
        self.assertEqual(result["missing_stat_samples"], 4)
        self.assertIsNone(result["event_changes"]["max"]["observed_change"])
        self.assertEqual(result["event_changes"]["max"]["excluded_intervals"],
                         {"missing_counter": 2, "counter_reset": 1})
        rows[1]["unit"]["MainPID"] = "456"
        self.assertEqual(summary(rows)["cgroup_memory"]["event_changes"]["max"]["excluded_intervals"],
                         {"unit_changed": 2, "counter_reset": 1})

    def test_malformed_memory_counter_cannot_be_a_zero(self):
        for value in ("anon -1\n", "anon 1\nanon 2\n", "anon unavailable\n"):
            row = sample(0, 0)
            row["cgroup"]["memory.stat"] = value
            with self.assertRaises(ValueError):
                summary([row])

    def test_limit_event_changes_exclude_boot_changes_and_sampling_gaps(self):
        for after, reason in [(sample(20_000_000_000, 0), "clock_or_sampling_gap"),
                              (sample(2_000_000_000, 0), "host_changed")]:
            before = sample(0, 0)
            if reason == "host_changed":
                after["boot_id"] = "new-boot"
            before["cgroup"]["memory.events"] = "max 1\n"
            after["cgroup"]["memory.events"] = "max 2\n"
            result = summary([before, after])["cgroup_memory"]["event_changes"]["max"]
            self.assertIsNone(result["observed_change"])
            self.assertEqual(result["excluded_intervals"], {reason: 1})

    def test_microsecond_counter_and_clock_uncertainty(self):
        report = summary([sample(0, 0), sample(2_000_000_000, 100_000)])
        value = report["memory_pressure"]["host"]["some"]
        self.assertEqual(value["observed_stall_us"], 100_000)
        self.assertAlmostEqual(value["maximum_interval_percent_upper"], 100 / 19)
        self.assertAlmostEqual(value["minimum_observed_seconds"], 1.9)
        self.assertAlmostEqual(value["maximum_observed_seconds"], 2.1)
        self.assertEqual(report["host_minimum_available_percent"], 12.5)
        self.assertEqual(report["host_peak_cached_bytes"], 512 * 1024)

    def test_missing_is_unknown_and_observed_zero_is_zero(self):
        rows = [sample(0, 0), sample(2_000_000_000, 0)]
        self.assertEqual(summary(rows)["memory_pressure"]["host"]["some"]["observed_stall_us"], 0)
        rows[1]["host"]["pressure/memory"] = {"error": "unavailable"}
        result = summary(rows)["memory_pressure"]["host"]["some"]
        self.assertIsNone(result["observed_stall_us"])
        self.assertEqual(result["excluded_intervals"], {"missing_counter": 1})
        self.assertIsNone(summary([])["host_minimum_available_percent"])
        self.assertIsNone(pressure_total("some total=1 total=2", "some"))

    def test_restarts_resets_and_long_gaps_do_not_bridge_observations(self):
        original = [sample(0, 100), sample(2_000_000_000, 200)]
        rows = copy.deepcopy(original)
        rows[1]["unit"]["MainPID"] = "456"
        self.assertEqual(summary(rows)["memory_pressure"]["cgroup"]["some"]["excluded_intervals"], {"unit_changed": 1})
        self.assertEqual(summary(rows)["memory_pressure"]["host"]["some"]["intervals"], 1)
        rows = copy.deepcopy(original)
        rows[1]["boot_id"] = "different-host"
        self.assertEqual(summary(rows)["memory_pressure"]["host"]["some"]["excluded_intervals"], {"host_changed": 1})
        for after, reason in [(sample(2_000_000_000, 99), "counter_reset"),
                              (sample(20_000_000_000, 101), "clock_or_sampling_gap")]:
            result = summary([original[0], after])["memory_pressure"]["host"]["some"]
            self.assertEqual(result["excluded_intervals"], {reason: 1})

    def test_large_unrelated_sample_fields_are_not_retained(self):
        class Payload:
            pass
        row = sample(0, 0)
        row["metrics"] = Payload()
        observed = weakref.ref(row["metrics"])
        accumulator = MemoryPressure()
        accumulator.add(row)
        del row
        gc.collect()
        self.assertIsNone(observed())
        for index in range(1, 10_000):
            accumulator.add(sample(index * 2_000_000_000, index))
        self.assertEqual(accumulator.report()["memory_pressure"]["host"]["some"]["intervals"], 9999)


class RecordingTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / "samples.jsonl.gz"
        self.reporter = load("report-native-sync-pressure")

    def write(self, rows):
        with gzip.open(self.path, "wt") as output:
            for row in rows:
                output.write(json.dumps(row) + "\n")

    def test_complete_recording_hash_and_selected_interval(self):
        rows = [sample(i * 2_000_000_000, i * 100) for i in range(4)]
        rows[-1]["unit"].update(MainPID="0", ActiveState="inactive")
        self.write(rows)
        report = self.reporter.report_recording(self.path, 0, 2_000_000_000)
        self.assertEqual(report["samples"], 2)
        self.assertEqual(report["recorded_rows"], 4)
        self.assertEqual(report["samples_sha256"], hashlib.sha256(self.path.read_bytes()).hexdigest())
        self.assertTrue(report["recording_complete"])
        self.assertTrue(report["unit_success_observed"])

    def test_failed_or_unknown_unit_result_is_not_success(self):
        row = sample(0, 0)
        row["unit"].update(ActiveState="failed", Result="oom-kill", ExecMainStatus="9")
        self.write([row])
        report = self.reporter.report_recording(self.path, 0, 0)
        self.assertTrue(report["recording_complete"])
        self.assertFalse(report["unit_success_observed"])
        row["unit"].update(ActiveState="inactive", ExecMainStatus="0")
        row["unit"].pop("Result")
        self.write([row])
        self.assertIsNone(self.reporter.report_recording(self.path, 0, 0)["unit_success_observed"])

    def test_deadline_or_truncated_gzip_cannot_be_a_complete_recording(self):
        self.write([sample(0, 0)])
        with self.assertRaises(ValueError):
            self.reporter.report_recording(self.path, 0, 0)
        result = self.reporter.report_recording(self.path, 0, 0, allow_incomplete=True)
        self.assertFalse(result["recording_complete"])
        self.path.write_bytes(self.path.read_bytes()[:-4])
        with self.assertRaises(EOFError):
            self.reporter.report_recording(self.path, 0, 0, allow_incomplete=True)

    def test_corruption_outside_selected_phase_is_not_ignored(self):
        rows = [sample(0, 0), sample(2_000_000_000, 1)]
        rows[1]["clock"] = "unrelated-clock"
        self.write(rows)
        with self.assertRaises(ValueError):
            self.reporter.report_recording(self.path, 0, 0, allow_incomplete=True)

    def test_observer_uses_only_explicit_local_endpoints(self):
        recorder = load("record-native-sync")
        self.assertEqual(recorder.local_url("http://[::1]:19999/metrics"), "http://[::1]:19999/metrics")
        for url in ["https://127.0.0.1", "http://example.com", "http://name:secret@127.0.0.1"]:
            with self.assertRaises(argparse.ArgumentTypeError):
                recorder.local_url(url)

    def test_missing_service_is_not_a_successfully_stopped_node(self):
        recorder = load("record-native-sync")
        properties = "MainPID=0\nControlGroup=\nActiveState=inactive\nExecMainStatus=0\nResult=success\nLoadState=not-found\n"
        with patch.object(recorder.subprocess, "check_output", return_value=properties):
            with self.assertRaises(ValueError):
                recorder.observe("missing.service", "http://127.0.0.1", "http://127.0.0.1")


if __name__ == "__main__":
    unittest.main()
