import csv
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import zakura_trace as trace

SPEC = importlib.util.spec_from_file_location("trace_oracle", ROOT / "docker/zakura-regtest-e2e/trace_oracle.py")
oracle = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = oracle
SPEC.loader.exec_module(oracle)


def row(event, index, process="p", **fields):
    return oracle.TraceRow("node1", "commit_state", index, dict(
        event=event, ts=index, process_trace_id=process, source="block_sync_driver", apply_token=1, **fields,
    ))


class TraceTests(unittest.TestCase):
    def test_shared_csv_contract(self):
        cases = json.loads((ROOT / "crates/zakura-jsonl-trace/tests/fixtures/csv.json").read_text())
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "commit_state.csv"
            for case in cases:
                with self.subTest(case=case["name"]):
                    path.write_text(case["csv"])
                    if case["valid"]:
                        rows = list(trace.read_table(path))
                        self.assertEqual(rows[0]["node"], "=1+1")
                        self.assertEqual(rows[0]["hash"], "000123")
                    else:
                        with self.assertRaises(trace.TraceInputError):
                            list(trace.read_table(path))

    def test_commit_matching_is_shared_and_process_scoped(self):
        start, finish = oracle.COMMIT_START, oracle.COMMIT_FINISH
        for rows in (
            [row(start, 1, "old"), row(finish, 2, "new")],
            [row(start, 1), row(start, 2), row(finish, 3)],
            [row(finish, 1)],
            [row(start, 1), row(finish, 2, apply_class="full", result="committed", elapsed_ms=1)],
        ):
            matches = oracle.match_commits(rows)
            node = oracle.NodeTrace("node1", {"commit_state": rows})
            self.assertEqual(bool(oracle.check_commit_pairs(node, oracle.OracleOptions())), bool(matches.errors))
            self.assertEqual(oracle.commits_balanced(rows), not matches.errors)

    def test_current_process_cannot_supply_old_process_activity(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            oracle.write_csv(root / "node1/block_sync.csv", [
                dict(event=oracle.BLOCK_GET_BLOCKS_SENT, ts=100, process_trace_id="old", range_start=1, range_count=1),
                dict(event=oracle.BLOCK_SYNC_STATE, ts=100, process_trace_id="new", best_header_tip=10, verified_block_tip=1),
            ])
            failures = oracle.run_oracle(root)
            self.assertIn("lagging_body_sync_has_real_activity", [failure.invariant for failure in failures])

    def test_complete_capture_requires_matching_writer_status(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            node = root / "node1"
            oracle.write_csv(node / "commit_state.csv", [dict(event=oracle.COMMIT_START, apply_token=1),
                                                       dict(event=oracle.COMMIT_FINISH, apply_token=1)])
            options = oracle.OracleOptions(capture_run_id="run", required_commit_nodes=("node1",))
            self.assertTrue(oracle.run_oracle(root, options))
            path = node / "capture-fixture.json"
            valid = dict(version=2, run_id="run", process_trace_id="fixture-process", accepted=2,
                         dropped=0, tables={"commit_state": 2}, failed_tables=[], rotation_segments=0, sealed=True)
            path.write_text(json.dumps(valid))
            self.assertFalse(oracle.run_oracle(root, options))
            for change in (dict(run_id="old"), dict(dropped=1), dict(accepted=3), dict(sealed=False),
                           dict(tables={"commit_state": 1}, accepted=1),
                           dict(failed_tables=["block_sync"]), dict(rotation_segments=2)):
                path.write_text(json.dumps(valid | change))
                self.assertTrue(oracle.run_oracle(root, options), change)

    def test_required_sync_evidence_is_not_vacuous(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            oracle.write_csv(root / "node1/commit_state.csv", [dict(event=oracle.COMMIT_START, apply_token=1),
                                                            dict(event=oracle.COMMIT_FINISH, apply_token=1)])
            oracle.write_csv(root / "node1/block_sync.csv", [])
            oracle.write_csv(root / "node1/header_sync.csv", [])
            failures = oracle.run_oracle(root, oracle.OracleOptions(required_sync_nodes=("node1",)))
            self.assertEqual(sum(f.invariant == "required_sync_evidence_exists" for f in failures), 2)

    def test_missing_event_fields_fail_before_invariants(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "block_sync.csv"
            oracle.write_csv(path, [dict(event=oracle.BLOCK_SYNC_STATE)])
            with path.open(newline="") as handle:
                reader = csv.DictReader(handle)
                header, records = reader.fieldnames, list(reader)
            records[0]["applying"] = ""
            with path.open("w", newline="") as handle:
                writer = csv.DictWriter(handle, header)
                writer.writeheader()
                writer.writerows(records)
            with self.assertRaises(trace.TraceInputError):
                list(trace.read_table(path))

    def test_special_files_and_symlinks_fail_without_opening_targets(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "commit_state.csv"
            outside = root / "outside"
            outside.write_text("private fixture")
            path.symlink_to(outside)
            with self.assertRaises(trace.TraceInputError):
                list(trace.read_table(path))
            path.unlink()
            for target in (path, root / "commit_state.csv.1"):
                os.mkfifo(target)
                with self.assertRaises(trace.TraceInputError):
                    list(trace.read_table(path))
                target.unlink()

    def test_limits_cover_the_entire_capture(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "commit_state.csv"
            oracle.write_csv(path, [dict(event="sample"), dict(event="sample")])
            for budget in (trace.Budget(bytes_left=1), trace.Budget(rows_left=1), trace.Budget(files_left=0)):
                with self.assertRaises(trace.TraceInputError):
                    list(trace.read_table(path, budget))

    def test_diagnostics_are_cached_and_failure_count_is_bounded(self):
        node = oracle.NodeTrace("node1", {"commit_state": [
            oracle.TraceRow("node1", "commit_state", index, dict(event=oracle.COMMIT_START, apply_token=index))
            for index in range(1000)
        ]})
        failures = oracle.check_commit_pairs(node, oracle.OracleOptions())
        self.assertEqual(len(failures), 100)
        self.assertIs(failures[0].detail["diagnostics"], failures[-1].detail["diagnostics"])

    def test_row_count_cursor_rejects_rotated_capture(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "block_sync.csv"
            oracle.write_csv(path, [dict(event="sample")])
            path.rename(path.with_name("block_sync.csv.1"))
            oracle.write_csv(path, [dict(event="new")])
            with self.assertRaises(trace.TraceInputError):
                oracle.main(["--dump-csv", str(path), "--after", "1"])


if __name__ == "__main__":
    unittest.main()
