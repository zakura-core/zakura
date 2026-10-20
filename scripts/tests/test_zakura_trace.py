import csv
import importlib.util
import json
import os
import re
import subprocess
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
    def test_reorg_wait_reads_new_csv_reset_events(self):
        harness = ROOT / "docker/zakura-regtest-e2e/run.sh"
        source = harness.read_text()
        functions = "\n".join(
            re.search(rf"^{name}\(\) \{{\n.*?^\}}", source, re.MULTILINE | re.DOTALL).group()
            for name in ("trace_line_count", "trace_rows_after", "wait_for_block_sync_reset")
        )
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "node1/block_sync.csv"
            oracle.write_csv(path, [
                dict(event="block_chain_tip_reset", verified_block_tip=161),
                dict(event="block_chain_tip_reset", verified_block_tip=162),
            ])
            result = subprocess.run(
                ["bash", "-c", functions + "\n" + r'''
set -euo pipefail
fail() { printf '%s\n' "$*" >&2; return 1; }
[[ $(trace_line_count "$ZAKURA_E2E_TRACE_DIR/node1/block_sync.csv") == 2 ]]
wait_for_block_sync_reset node1 162 1 1
'''],
                env={**os.environ, "SCRIPT_DIR": str(harness.parent),
                     "ZAKURA_E2E_TRACE_DIR": temporary},
                capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

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
            for budget in (trace.Budget(bytes_left=1), trace.Budget(rows_left=1), trace.Budget(files_left=0),
                           trace.Budget(values_left=3)):
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

    def test_compressed_segments_read_oldest_first(self):
        import gzip
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "block_sync.csv"
            for name, event in (("block_sync.csv.2", "oldest"), ("block_sync.csv.1", "older")):
                oracle.write_csv(path, [dict(event=event)])
                path.with_name(f"{name}.gz").write_bytes(gzip.compress(path.read_bytes()))
            oracle.write_csv(path, [dict(event="current")])
            self.assertEqual([item["event"] for item in trace.read_table(path)],
                             ["oldest", "older", "current"])

            budget = trace.Budget(bytes_left=path.with_name("block_sync.csv.2.gz").stat().st_size + 1)
            with self.assertRaises(trace.TraceInputError):
                list(trace.read_table(path, budget))

            path.with_name("block_sync.csv.1.gz").write_bytes(b"not gzip")
            with self.assertRaises(trace.TraceInputError):
                list(trace.read_table(path))

    def test_snapshot_releases_the_writer_lock_before_parsing(self):
        import fcntl
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "block_sync.csv"
            oracle.write_csv(path, [dict(event="before")])
            (Path(temporary) / ".trace.lock").touch()
            with trace.locked_directory(temporary):
                # The node's writer can take its exclusive lock and append.
                with (Path(temporary) / ".trace.lock").open("a") as writer_lock:
                    fcntl.flock(writer_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    with path.open("a") as appended:
                        appended.write("partial,row")
                self.assertEqual([row["event"] for row in trace.read_table(path)], ["before"])

    def test_symlinked_ancestor_directories_are_readable(self):
        with tempfile.TemporaryDirectory() as temporary:
            real = Path(temporary) / "real"
            oracle.write_csv(real / "traces" / "block_sync.csv", [dict(event="linked")])
            (Path(temporary) / "link").symlink_to(real)
            path = Path(temporary) / "link" / "traces" / "block_sync.csv"
            with trace.locked_directory(path.parent):
                self.assertEqual([row["event"] for row in trace.read_table(path)], ["linked"])
            (real / "traces" / "commit_state.csv").symlink_to(path)
            with self.assertRaises(trace.TraceInputError):
                list(trace.read_table(real / "traces" / "commit_state.csv"))

    def test_row_count_cursor_rejects_rotated_capture(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "block_sync.csv"
            oracle.write_csv(path, [dict(event="sample")])
            path.rename(path.with_name("block_sync.csv.1"))
            oracle.write_csv(path, [dict(event="new")])
            with self.assertRaises(trace.TraceInputError):
                oracle.main(["--dump-csv", str(path), "--after", "1"])


    def test_full_finish_search_matches_a_linear_scan(self):
        import random
        rng = random.Random(7)
        rows = [
            oracle.TraceRow("node1", "commit_state", index, dict(height=rng.randrange(50)))
            for index in range(200)
        ]
        for index, value in enumerate(rows):
            value.row["ts"] = rng.randrange(400)
        search = oracle.FullFinishSearch(rows)
        for _ in range(2000):
            after_ts, height = rng.randrange(-1, 401), rng.randrange(-1, 51)
            later = [value for value in search.rows if value.ts > after_ts and value.row["height"] > height]
            self.assertIs(search.first_above(after_ts, height), later[0] if later else None)

    def test_imported_trace_checks_do_not_scale_quadratically(self):
        import time
        count = 20_000
        options = oracle.OracleOptions()
        header_rows = [
            oracle.TraceRow("node1", "header_sync", index, dict(
                event=oracle.HEADER_RANGE_REJECTED, ts=index, validation_stage="link", range_start=10**9,
            ))
            for index in range(count)
        ] + [
            oracle.TraceRow("node1", "header_sync", count + index, dict(
                event=oracle.HEADER_FRONTIER_ADVANCED, ts=index, height=10**9,
            ))
            for index in range(count)
        ]
        handoffs = [
            oracle.TraceRow("node1", "legacy_sync", index, dict(
                event=oracle.LEGACY_ROUND_FINISH, ts=index, reason=oracle.LEGACY_CHECKPOINT_HANDOFF,
                checkpoint_height=10**9, state_tip=10**9, process_trace_id="p",
            ))
            for index in range(count)
        ]
        fulls = [
            oracle.TraceRow("node1", "commit_state", index, dict(
                event=oracle.COMMIT_FINISH, ts=index, apply_class=oracle.APPLY_CLASS_FULL, result="committed",
                height=index, process_trace_id="p",
            ))
            for index in range(count)
        ]
        node = oracle.NodeTrace("node1", {"header_sync": header_rows, "legacy_sync": handoffs, "commit_state": fulls})
        started = time.monotonic()
        # Every reject recovers, so the check searches a window for each one.
        self.assertEqual(oracle.check_header_recovery(node, options), [])
        failure, _ = oracle.checkpoint_to_full_handoff(node, options)
        self.assertEqual(failure.invariant, "checkpoint_to_full_handoff_observed")
        # The quadratic scans took minutes at this size.
        self.assertLess(time.monotonic() - started, 10)


if __name__ == "__main__":
    unittest.main()
