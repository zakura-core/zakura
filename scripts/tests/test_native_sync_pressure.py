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
