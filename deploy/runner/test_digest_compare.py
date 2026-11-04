"""Contracts for paired observations, late phases, and the bounded remote cursor."""

import json
from pathlib import Path
import tempfile
import unittest

import digest_compare as comparison
import digest_probe as probe


class ComparisonTests(unittest.TestCase):
    def setUp(self):
        self.control = {"transitions": [{"phase": "phase1", "at": 0}]}
        self.revisions = {"baseline": {"sha256": "a" * 64}, "candidate": {"sha256": "b" * 64}}

    def row(self, role, block_hash="1" * 64, at=10):
        baseline = role == "baseline"
        return {
            "event": "finish", "node": "us-west-0" if baseline else "canada-0",
            "pid": 1 if baseline else 2, "at_ns": int(at * 1e9),
            "binary_sha256": self.revisions[role]["sha256"], "trace_drops": 0,
            "payload": {
                "schema": 1, "hash": block_hash, "route": "ordinary", "outcome": "accepted",
                "active": 0, "ambiguous": 0, "total_ns": 1000 if baseline else 700,
                "semantic_ns": 700 if baseline else 500, "handoff_wall_calls": 1,
                "handoff_wall_ns": 300 if baseline else 200,
                "commit_wall_calls": 1, "commit_wall_ns": 200 if baseline else 100,
                "initial_id_calls": 2 if baseline else 0,
                "initial_combined_calls": 0 if baseline else 2,
                "transactions_auth_calls": 2 if baseline else 0,
                "commitment_root_reuse_calls": 0 if baseline else 1,
                "commitment_root_recompute_calls": 1 if baseline else 0,
                "tx": 2, "v5": 2,
            },
        }

    def report(self, rows, profiles=None):
        starts = [{**row, "event": "start"} for row in rows if row.get("event") == "finish"]
        rows = starts + rows
        return comparison.summarize(rows, self.control, self.revisions, profiles or [])

    def test_missing_start_and_lost_cursor_cannot_supply_primary_pairs(self):
        rows = [self.row("baseline"), self.row("candidate")]
        report = comparison.summarize(rows, self.control, self.revisions, [])
        self.assertEqual(report["timing"]["all"]["pairs"], 0)
        rows.append({"event": "resource", "node": "canada-0", "pid": 2, "lost_segments": 1})
        self.assertEqual(self.report(rows)["timing"]["all"]["pairs"], 0)

    def test_identical_blocks_expose_removed_calls_and_paired_durations(self):
        report = self.report([self.row("baseline"), self.row("candidate")])
        self.assertEqual(report["timing"]["all"]["pairs"], 1)
        metrics = report["timing"]["all"]["metrics"]
        self.assertEqual(metrics["transactions_auth_calls"]["candidate"]["median"], 0)
        self.assertEqual(metrics["transactions_auth_calls"]["baseline"]["median"], 2)
        self.assertEqual(metrics["total_ns"]["paired_delta_median"], -300)

    def test_different_hashes_or_phases_do_not_form_a_pair(self):
        report = self.report([self.row("baseline"), self.row("candidate", "2" * 64)])
        self.assertEqual(report["timing"]["all"]["pairs"], 0)
        self.control["transitions"].append({"phase": "phase2", "at": 20})
        report = self.report([self.row("baseline"), self.row("candidate", at=30)])
        self.assertEqual(report["timing"]["all"]["pairs"], 0)

    def test_profile_windows_duplicates_and_incomplete_work_are_excluded(self):
        baseline, candidate = self.row("baseline"), self.row("candidate")
        candidate["payload"]["active"] = 1
        self.assertEqual(self.report([baseline, candidate])["timing"]["all"]["pairs"], 0)
        candidate["payload"]["active"] = 0
        profiles = [{"node": "canada-0", "start": 9, "end": 11}]
        self.assertEqual(self.report([baseline, candidate], profiles)["timing"]["all"]["pairs"], 0)
        self.assertEqual(self.report([baseline, candidate, candidate])["timing"]["all"]["pairs"], 0)

    def test_late_work_is_joined_without_adding_it_to_response_time(self):
        rows = [self.row("baseline"), self.row("candidate")]
        for role in ("baseline", "candidate"):
            row = self.row(role)
            row.update(event="phase", at_ns=int(11e9), payload={"hash": "1" * 64, "phase": "finalized", "wall_calls": 1, "wall_ns": 400, "auth_calls": 2, "root_calls": 1})
            rows.append(row)
        report = self.report(rows)
        metrics = report["timing"]["all"]["metrics"]
        self.assertEqual(metrics["late_finalized_wall_ns"]["baseline"]["median"], 400)
        self.assertEqual(metrics["total_ns"]["baseline"]["median"], 1000)
        self.assertEqual(metrics["late_finalized_auth_calls"]["candidate"]["median"], 2)

    def test_older_unknown_binaries_cannot_supply_comparison_samples(self):
        candidate = self.row("candidate")
        candidate["binary_sha256"] = "c" * 64
        self.assertEqual(self.report([self.row("baseline"), candidate])["timing"]["all"]["pairs"], 0)

    def test_trace_drops_and_sequence_gaps_make_the_process_ineligible(self):
        baseline, candidate = self.row("baseline"), self.row("candidate")
        candidate["trace_drops"] = 1
        self.assertEqual(self.report([baseline, candidate])["timing"]["all"]["pairs"], 0)
        candidate.update(trace_drops=0, sequence=3)
        earlier = {**candidate, "event": "start", "sequence": 1}
        report = self.report([baseline, earlier, candidate])
        self.assertEqual(report["timing"]["all"]["pairs"], 0)
        self.assertEqual(report["sequence_gaps_by_process"][str(("canada-0", 2))], 1)

    def test_cumulative_late_snapshots_use_the_last_value(self):
        rows = [self.row("baseline"), self.row("candidate")]
        for role in ("baseline", "candidate"):
            for count in (1, 2):
                row = self.row(role)
                row.update(event="phase", payload={"hash": "1" * 64, "phase": "finalized", "wall_calls": count, "wall_ns": 400 * count, "auth_calls": 2 * count, "root_calls": count})
                rows.append(row)
        report = self.report(rows)
        self.assertEqual(report["timing"]["all"]["metrics"]["late_finalized_auth_calls"]["candidate"]["median"], 4)

    def test_missing_schema_fields_cannot_be_reported_as_zero_work(self):
        candidate = self.row("candidate")
        del candidate["payload"]["transactions_auth_calls"]
        self.assertEqual(self.report([self.row("baseline"), candidate])["timing"]["all"]["pairs"], 0)

    def test_blocks_crossing_a_swap_or_report_deadline_are_excluded(self):
        self.control["transitions"].append({"phase": "swapping", "at": 9.9999995})
        self.assertEqual(self.report([self.row("baseline"), self.row("candidate")])["timing"]["all"]["pairs"], 0)
        self.control = {"transitions": [{"phase": "phase1", "at": 0}], "observation_finished_at": 9}
        self.assertEqual(self.report([self.row("baseline"), self.row("candidate")])["timing"]["all"]["pairs"], 0)

    def test_an_unapproved_manifest_cannot_start_the_observation_clock(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            manifest = directory / "manifest.json"
            manifest.write_text(json.dumps({"approved": False}))
            with self.assertRaisesRegex(ValueError, "confirmation"):
                comparison.activate(manifest, directory, "phase1")

    def test_live_coverage_retains_losses_and_allows_delayed_sequence_rows(self):
        def row(sequence):
            return {"pid": 7, "sequence": sequence, "trace_drops": 0}
        state = comparison.trace_coverage({}, [row(1), row(3)], 0, 0)
        self.assertFalse(state["failed"])
        state = comparison.trace_coverage(state, [row(2)], 0, 30)
        self.assertFalse(comparison.trace_coverage(state, [], 0, 90)["failed"])
        state = comparison.trace_coverage({}, [row(1), row(3)], 0, 0)
        self.assertTrue(comparison.trace_coverage(state, [], 0, 90)["failed"])
        state = comparison.trace_coverage({}, [], 1, 0)
        self.assertTrue(comparison.trace_coverage(state, [], 0, 90)["failed"])


class CursorTests(unittest.TestCase):
    def test_rotation_and_partial_rows_preserve_the_read_position(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            active = directory / "events.jsonl"
            row = json.dumps({"event": "start", "payload": {"hash": "1" * 64}}).encode() + b"\n"
            active.write_bytes(row)
            rows, cursor, lost = probe.read_rows(directory, None)
            self.assertEqual(rows, [])
            active.rename(directory / "events.jsonl.1")
            active.write_bytes(row + b'{"event":')
            rows, cursor, lost = probe.read_rows(directory, cursor)
            self.assertEqual(len(rows), 1)
            self.assertEqual(lost, 0)
            with active.open("ab") as stream:
                stream.write(b'"phase","payload":{}}\n')
            rows, _, lost = probe.read_rows(directory, cursor)
            self.assertEqual(rows[0]["event"], "phase")
            self.assertEqual(lost, 0)

    def test_evicted_cursor_and_malformed_rows_are_reported_as_gaps(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            (directory / "events.jsonl").write_text("invalid\n")
            rows, _, lost = probe.read_rows(directory, {"inode": -1, "offset": 0})
            self.assertEqual(rows, [])
            self.assertEqual(lost, 2)


if __name__ == "__main__":
    unittest.main()
