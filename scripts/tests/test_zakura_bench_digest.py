import importlib.util
import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


SCRIPT = Path(__file__).parents[1] / "zakura-bench-digest.py"
SPEC = importlib.util.spec_from_file_location("zakura_bench_digest", SCRIPT)
assert SPEC and SPEC.loader
digest = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = digest
SPEC.loader.exec_module(digest)


PERF_SCRIPT = """\
zakurad  4021/4055  [002]  60.100: 1010101 cycles:u:
\t    55a1b2c3 zakura_state::service::finalized_state::FinalizedState::commit_finalized_direct+0x1a (/opt/zakura-bench/bins/abc/zakurad)
\t    55a1b2c0 _ZN4core3ops::hdeadbeefdeadbeef (/opt/zakura-bench/bins/abc/zakurad)
\t    55a1b200 std::sys::unix::thread::Thread::new (/opt/zakura-bench/bins/abc/zakurad)

zakurad  4021/4055  [002]  60.110: 1010101 cycles:u:
\t    55a1b2c3 zakura_state::service::finalized_state::FinalizedState::commit_finalized_direct (/opt/zakura-bench/bins/abc/zakurad)
\t    55a1b2c0 _ZN4core3ops::hdeadbeefdeadbeef (/opt/zakura-bench/bins/abc/zakurad)
\t    55a1b200 std::sys::unix::thread::Thread::new (/opt/zakura-bench/bins/abc/zakurad)

rayon 3  4021/4099  [007]  60.120: 1010101 cycles:u:
\t    7f00aa11 <halo2_proofs::plonk::verifier::verify_proof (with (parens))> (/opt/zakura-bench/bins/abc/zakurad)
\t    7f00aa10 rayon_core::registry::WorkerThread::wait_until_cold (/opt/zakura-bench/bins/abc/zakurad)

commit-compute-1  4021/4100  [001]  60.130: 1010101 cycles:u:
\t    7f00bb22 [unknown] ([unknown])
"""


def run_command(handler, args, stdin_text=None):
    """Run one digest subcommand, returning its captured stdout."""
    out = io.StringIO()
    with redirect_stdout(out):
        if stdin_text is None:
            handler(args)
        else:
            with mock.patch.object(digest.sys, "stdin", io.StringIO(stdin_text)):
                handler(args)
    return out.getvalue()


class CollapseTests(unittest.TestCase):
    def collapse(self, text):
        return run_command(digest.cmd_collapse, SimpleNamespace(), stdin_text=text)

    def test_folds_and_aggregates_identical_stacks(self):
        folded = self.collapse(PERF_SCRIPT)
        self.assertIn(
            "zakurad;std::sys::unix::thread::Thread::new;_ZN4core3ops;"
            "zakura_state::service::finalized_state::FinalizedState"
            "::commit_finalized_direct 2",
            folded.splitlines(),
        )

    def test_offset_and_rust_hash_suffixes_are_stripped(self):
        folded = self.collapse(PERF_SCRIPT)
        self.assertNotIn("+0x", folded)
        self.assertNotIn("::hdeadbeef", folded)

    def test_llvm_internalization_suffix_is_stripped(self):
        text = (
            "zakurad  1/1  [000]  1.0: 1 cycles:u:\n"
            "\t    aa pasta_curves::fields::fp::Fp::square"
            "::h1405046f7da51426.llvm.2791652607651103975 (/bin/z)\n"
        )
        folded = self.collapse(text)
        self.assertIn("zakurad;pasta_curves::fields::fp::Fp::square 1", folded)

    def test_comm_with_spaces_and_symbol_parens_survive(self):
        folded = self.collapse(PERF_SCRIPT)
        rayon_lines = [line for line in folded.splitlines() if line.startswith("rayon 3;")]
        self.assertEqual(len(rayon_lines), 1)
        self.assertIn("verify_proof (with (parens))", rayon_lines[0])

    def test_unknown_frames_kept_as_placeholder(self):
        folded = self.collapse(PERF_SCRIPT)
        self.assertIn("commit-compute-1;[unknown] 1", folded.splitlines())

    def test_missing_blank_line_between_samples_still_flushes(self):
        text = (
            "zakurad  1/1  [000]  1.0: 1 cycles:u:\n"
            "\t    aa fn_one (/bin/z)\n"
            "zakurad  1/1  [000]  2.0: 1 cycles:u:\n"
            "\t    bb fn_two (/bin/z)\n"
        )
        folded = self.collapse(text)
        self.assertIn("zakurad;fn_one 1", folded)
        self.assertIn("zakurad;fn_two 1", folded)


class TopTests(unittest.TestCase):
    def write_folded(self, tmp, text):
        path = Path(tmp) / "primary.folded"
        path.write_text(text)
        return path

    def test_thread_groups_and_hottest_functions(self):
        folded_text = (
            "rayon 0;a;hot_fn 6\n"
            "rayon 1;a;hot_fn 2\n"
            "commit-compute-0;b;tree_fn 2\n"
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = self.write_folded(tmp, folded_text)
            args = SimpleNamespace(folded=str(path), title="t", note="", limit=15)
            output = run_command(digest.cmd_top, args)
        # per-thread suffixes collapse into one group
        self.assertIn("| `rayon` | 80.0% |", output)
        self.assertIn("| `commit-compute` | 20.0% |", output)
        # hot_fn: 8/10 self and total; helper `a` appears with total 80%
        self.assertIn("| 1 | 80.0% | 80.0% | `hot_fn` |", output)
        self.assertIn("| 20.0% | 20.0% | `tree_fn` |", output)

    def test_missing_and_empty_files_degrade_to_notes(self):
        args = SimpleNamespace(folded="/nonexistent/x.folded", title="t", note="", limit=5)
        self.assertIn("_(no CPU profile:", run_command(digest.cmd_top, args))
        with tempfile.TemporaryDirectory() as tmp:
            path = self.write_folded(tmp, "")
            args = SimpleNamespace(folded=str(path), title="t", note="", limit=5)
            self.assertIn("file is empty", run_command(digest.cmd_top, args))


class DiffTests(unittest.TestCase):
    def test_reports_signed_self_share_deltas(self):
        with tempfile.TemporaryDirectory() as tmp:
            baseline = Path(tmp) / "baseline.folded"
            primary = Path(tmp) / "primary.folded"
            baseline.write_text("zakurad;fast_fn 8\nzakurad;slow_fn 2\n")
            primary.write_text("zakurad;fast_fn 4\nzakurad;slow_fn 6\n")
            args = SimpleNamespace(
                baseline=str(baseline), primary=str(primary), title="d", limit=5
            )
            output = run_command(digest.cmd_diff, args)
        self.assertIn("| +40.00pp | 20.0% | 60.0% | `slow_fn` |", output)
        self.assertIn("| -40.00pp | 80.0% | 40.0% | `fast_fn` |", output)


METRICS_SNAPSHOT = """\
# TYPE zakura_state_write_update_trees_duration_seconds summary
zakura_state_write_update_trees_duration_seconds{quantile="0.5"} 0.002
zakura_state_write_update_trees_duration_seconds{quantile="0.9"} 0.004
zakura_state_write_update_trees_duration_seconds{quantile="0.99"} 0.010
zakura_state_write_update_trees_duration_seconds_sum 12.5
zakura_state_write_update_trees_duration_seconds_count 5000
zakura_consensus_batch_duration_seconds{verifier="halo2",result="ok",quantile="0.5"} 0.030
zakura_consensus_batch_duration_seconds_sum{verifier="halo2",result="ok"} 90.0
zakura_consensus_batch_duration_seconds_count{verifier="halo2",result="ok"} 3000
zakura_state_rocksdb_batch_commit_duration_seconds_sum 25.0
zakura_state_rocksdb_batch_commit_duration_seconds_count 5000
state_finalized_block_count 22790
"""


def commit_row(**overrides):
    row = {
        "ts": 1000,
        "node": "bench",
        "event": "commit_finish",
        "height": 1707211,
        "apply_class": "checkpoint",
        "result": "committed",
        "elapsed_ms": 10,
    }
    row.update(overrides)
    return json.dumps(row)


def block_lifecycle(height, queued_ts, start_ts, finish_ts, elapsed_ms, **finish_overrides):
    """The three driver rows (queued/start/finish) for one committed block."""
    return [
        json.dumps({"event": "block_submit_queued", "height": height, "ts": queued_ts}),
        json.dumps({"event": "commit_start", "height": height, "ts": start_ts}),
        commit_row(height=height, ts=finish_ts, elapsed_ms=elapsed_ms, **finish_overrides),
    ]


def header_range_row(elapsed_ms, range_count):
    return json.dumps(
        {
            "event": "commit_finish",
            "source": "header_sync_driver",
            "action": "commit_header_range",
            "result": "committed",
            "elapsed_ms": elapsed_ms,
            "range_count": range_count,
            "range_start": 1707211,
            "ts": 5000,
        }
    )


class LatencyTests(unittest.TestCase):
    def run_latency(self, traces="", metrics="", json_out=""):
        args = SimpleNamespace(
            traces=traces, metrics=metrics, json_out=json_out, title="t"
        )
        return run_command(digest.cmd_latency, args)

    def write_trace(self, tmp, rows):
        (Path(tmp) / "commit_state.jsonl").write_text("\n".join(rows) + "\n")

    def test_checkpoint_residence_with_takeaway_and_header_ranges(self):
        rows = []
        # 3 blocks finishing 1s apart (steady 1 blk/s), negligible queue wait
        for i, elapsed in enumerate([1000, 2000, 3000]):
            rows += block_lifecycle(
                1707211 + i,
                queued_ts=1_000_000 * i + 100,
                start_ts=1_000_000 * i + 300,  # 0.2ms queue wait
                finish_ts=1_000_000 * (i + 1),
                elapsed_ms=elapsed,
            )
        rows += [
            header_range_row(10, 60),
            header_range_row(30, 72),
            commit_row(height=1707216, result="rejected"),
            json.dumps(
                {
                    "event": "commit_stalled",
                    "height": 1707217,
                    "commit_stall_reason": "contiguous_head",
                }
            ),
            "not json at all",
        ]
        with tempfile.TemporaryDirectory() as tmp:
            self.write_trace(tmp, rows)
            json_out = Path(tmp) / "latency.json"
            output = self.run_latency(traces=tmp, json_out=str(json_out))
            report = json.loads(json_out.read_text())

        self.assertIn("**Takeaway:** committed 3 blocks at **1 blocks/s** steady", output)
        self.assertIn("(**1000.0 ms/block** marginal cost)", output)
        self.assertIn("Median pipeline residence is **2,000 ms**", output)
        # collapsed: exactly one residence row, queue demoted to a note
        self.assertIn("| residence (submitted→committed) | 3 | 2,000 | 2,000 |", output)
        self.assertNotIn("| driver queue (queued→submitted) |", output)
        self.assertIn("driver queue wait: negligible (p50 0.2 ms)", output)
        self.assertIn("slowest: 1707213 (3,000 ms), 1707212 (2,000 ms)", output)
        self.assertIn("not applicable: checkpoint mode skips per-block verification", output)
        self.assertIn(
            "**Header-range commits** (parallel header pipeline): 2 ranges × ~66 headers,"
            " mean 20.0 ms, p99 30.0 ms.",
            output,
        )
        self.assertIn("commit stalls (>30s): contiguous_head: 1", output)
        self.assertIn("non-committed results: rejected: 1", output)

        self.assertEqual(report["checkpoint_residence"]["stats"]["count"], 3)
        self.assertEqual(report["checkpoint_residence"]["queue_wait"]["count"], 3)
        self.assertEqual(report["header_ranges"]["ranges"], 2)
        self.assertEqual(report["stalls"], {"contiguous_head": 1})

    def test_significant_queue_wait_gets_its_own_row(self):
        rows = []
        for i in range(2):
            rows += block_lifecycle(
                1707211 + i,
                queued_ts=1_000_000 * i,
                start_ts=1_000_000 * i + 500_000,  # 500ms queue wait
                finish_ts=1_000_000 * (i + 1),
                elapsed_ms=1000,
            )
        with tempfile.TemporaryDirectory() as tmp:
            self.write_trace(tmp, rows)
            output = self.run_latency(traces=tmp)
        self.assertIn("| driver queue (queued→submitted) | 2 | 500 | 500 |", output)
        self.assertNotIn("negligible", output)

    def test_full_class_renders_per_block_latency(self):
        rows = []
        for i, elapsed in enumerate([10, 20, 500]):
            rows += block_lifecycle(
                1707211 + i,
                queued_ts=1_000_000 * i,
                start_ts=1_000_000 * i + 100,
                finish_ts=1_000_000 * (i + 1),
                elapsed_ms=elapsed,
                apply_class="full",
            )
        with tempfile.TemporaryDirectory() as tmp:
            self.write_trace(tmp, rows)
            json_out = Path(tmp) / "latency.json"
            output = self.run_latency(traces=tmp, json_out=str(json_out))
            report = json.loads(json_out.read_text())
        self.assertIn("**Per-block verify+commit latency** (full class)", output)
        self.assertIn("true verification+commit cost (no range batching)", output)
        self.assertIn("| 3 | 177 | 20.0 | 500 | 500 | 500 |", output)
        self.assertIn("slowest: 1707213 (500 ms)", output)
        self.assertNotIn("not applicable", output)
        self.assertEqual(report["full_per_block"]["stats"]["count"], 3)

    def test_unclassified_block_rows_are_surfaced(self):
        rows = [commit_row(height=1707211, apply_class=None, elapsed_ms=5)]
        with tempfile.TemporaryDirectory() as tmp:
            self.write_trace(tmp, rows)
            output = self.run_latency(traces=tmp)
        self.assertIn("_(unclassified block commit rows: unknown: 1)_", output)

    def test_stage_timings_from_metrics_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            metrics = Path(tmp) / "metrics.prom"
            metrics.write_text(METRICS_SNAPSHOT)
            output = self.run_latency(metrics=str(metrics))
        # mean = sum/count * 1000; the exporter's rolling-window summary
        # quantile lines are ignored (validated against run 30025780553, where
        # they decayed to 0 while _sum/_count stayed correct)
        self.assertIn("| commit: update note trees | 5,000 | 2.5 |", output)
        self.assertIn(
            "| verify: batch (result=ok,verifier=halo2) | 3,000 | 30.0 |", output
        )
        self.assertIn("| commit: rocksdb batch write | 5,000 | 5.0 |", output)
        self.assertNotIn("| p50 |", output.split("Pipeline stage timings")[1])

    def test_missing_inputs_degrade_to_notes(self):
        output = self.run_latency()
        self.assertIn("no per-block trace", output)
        self.assertIn("no final /metrics snapshot", output)


if __name__ == "__main__":
    unittest.main()
