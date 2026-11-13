#!/usr/bin/env python3
"""Tests for the continuous profiler: stack parsing, trees, views, storage and the API."""

import argparse
import json
import os
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock
import urllib.error
import urllib.request
from unittest.mock import patch
from http.server import ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import blocks  # noqa: E402
import stacks  # noqa: E402
import zakura_profiler as profiler_module  # noqa: E402

# Three samples in the shape `perf script -F comm,tid,time,ip,sym,dso` prints them:
# an async worker inside a syscall, a verification-pool thread whose name has a space,
# and a stack the unwinder could not follow to the thread's start.
PERF_SCRIPT = """\
tokio-rt-worker 1981158 1794528036.264668:
\tffffffffb9434d88 udp_sendmsg ([kernel.kallsyms])
\tffffffffb964d8ef do_syscall_64 ([kernel.kallsyms])
\t    758ba332c2cb sendmsg (/usr/lib/x86_64-linux-gnu/libc.so.6)
\t    5dbed5a602ff _RNvNtCs1_14zakura_network4peer4send (/usr/local/bin/zakurad)
\t    5dbed5a30863 _RNvNtCs2_5tokio7runtime3run (/usr/local/bin/zakurad)
\t    758ba329cb83 [unknown] (/usr/lib/x86_64-linux-gnu/libc.so.6)

rayon 2 1981200 1794528036.300000:
\t    5dbed5a11111 _RNvNtCs3_12pasta_curves6fields3mul.llvm.123456 (/usr/local/bin/zakurad)
\t    5dbed5a22222 _RNvNtCs4_16zakura_consensus5block6verify (/usr/local/bin/zakurad)
\t    5dbed5a33333 _RNvNtCs5_10rayon_core3job7execute (/usr/local/bin/zakurad)

tokio-rt-worker 1981158 1794528036.900000:
\t    5dbed5a22222 _RNvNtCs4_16zakura_consensus5block6verify (/usr/local/bin/zakurad)
\tffffffffffffffff [unknown] ([unknown])
"""

DEMANGLED = {
    "_RNvNtCs1_14zakura_network4peer4send": "zakura_network[0123456789abcdef]::peer::send",
    "_RNvNtCs2_5tokio7runtime3run": "tokio[0123456789abcdef]::runtime::run",
    "_RNvNtCs3_12pasta_curves6fields3mul": "pasta_curves[0123456789abcdef]::fields::mul",
    "_RNvNtCs4_16zakura_consensus5block6verify": "zakura_consensus[0123456789abcdef]::block::verify",
    "_RNvNtCs5_10rayon_core3job7execute": "rayon_core[0123456789abcdef]::job::execute",
}


def fake_demangle(mangled):
    return [DEMANGLED[name] for name in mangled]


def samples():
    return list(stacks.parse_perf_script(PERF_SCRIPT.splitlines()))


def nested(node):
    """A tree as plain nested values, for comparing two trees."""
    return (node.self_us, node.block_us, {name: nested(child) for name, child in node.children.items()})


def window_tree(block_times=()):
    return profiler_module.build_window(
        samples(), stacks.SymbolNames(fake_demangle), stacks.Categorizer(), 100, list(block_times)
    )


class ParsingTests(unittest.TestCase):
    def test_reads_thread_name_time_and_frames_innermost_first(self):
        parsed = samples()
        self.assertEqual([s[0] for s in parsed], ["tokio-rt-worker", "rayon 2", "tokio-rt-worker"])
        self.assertAlmostEqual(parsed[0][1], 1794528036.264668)
        self.assertEqual(parsed[0][2][0], (0xFFFFFFFFB9434D88, "udp_sendmsg"))
        self.assertEqual(len(parsed[0][2]), 6)

    def test_a_frame_perf_could_not_name_is_reported_as_its_library(self):
        self.assertEqual(samples()[0][2][-1][1], "[libc.so.6]")
        self.assertEqual(stacks.frame_symbol("[unknown] ([unknown])"), "[unknown]")
        self.assertEqual(stacks.frame_symbol("epoll_wait"), "epoll_wait")

    def test_symbol_names_drop_build_specific_parts(self):
        self.assertEqual(
            stacks.clean_symbol("zakura_state[0123456789abcdef]::write::run::<tokio[fedcba9876543210]::X>"),
            "zakura_state::write::run::<tokio::X>",
        )
        self.assertEqual(stacks.clean_symbol("core::fmt::write::h0123456789abcdef"), "core::fmt::write")

    def test_names_mark_kernel_frames_and_truncated_stacks(self):
        names = stacks.SymbolNames(fake_demangle)
        names.resolve(DEMANGLED)
        self.assertEqual(names.frame_name(0xFFFFFFFFB9434D88, "udp_sendmsg"), "[k] udp_sendmsg")
        self.assertEqual(names.frame_name(stacks.TRUNCATED_ADDRESS, "[unknown]"), "[stack truncated]")
        self.assertEqual(
            names.frame_name(0x5DBED5A602FF, "_RNvNtCs1_14zakura_network4peer4send"),
            "zakura_network::peer::send",
        )

    def test_a_missing_demangler_leaves_names_raw_instead_of_failing(self):
        def broken(_):
            raise OSError("c++filt is not installed")

        names = stacks.SymbolNames(broken)
        names.resolve(["_RNvNtCs2_5tokio7runtime3run", "sendmsg"])
        self.assertEqual(names.frame_name(1, "_RNvNtCs2_5tokio7runtime3run"), "_RNvNtCs2_5tokio7runtime3run")
        self.assertEqual(names.frame_name(1, "sendmsg"), "sendmsg")

    def test_threads_are_grouped_without_their_index(self):
        self.assertEqual(stacks.thread_group("rayon 3"), stacks.thread_group("rayon 0"))
        self.assertIn("rayon", stacks.thread_group("rayon 3"))
        self.assertEqual(stacks.thread_group("commit-compute-"), "commit-compute (tree updates)")
        self.assertEqual(stacks.thread_group("some-new-thread"), "some-new-thread")


class CategoryTests(unittest.TestCase):
    def test_a_frame_belongs_to_the_subsystem_named_first_in_its_symbol(self):
        wrapper = "<zakurad::components::inbound::downloads::Downloads<zakura_consensus::Request>>::poll"
        self.assertEqual(stacks.frame_category(wrapper), "arrival")
        self.assertEqual(
            stacks.frame_category("<rayon_core::job::HeapJob<zakura_consensus::primitives::spawn_fifo<X>>>::execute"),
            "verification",
        )

    def test_narrower_patterns_win_over_the_crate_they_live_in(self):
        self.assertEqual(stacks.frame_category("zakura_network::peer::block_gossip::broadcast_block"), "propagation")
        self.assertEqual(stacks.frame_category("zakura_network::zakura::block_sync::reactor::run"), "arrival")
        self.assertEqual(stacks.frame_category("zakura_network::peer::connection::Connection::run"), "network")
        self.assertEqual(stacks.frame_category("zakurad::components::sync::gossip::gossip_best_tip"), "propagation")

    def test_unmatched_frames_are_kernel_or_other(self):
        self.assertEqual(stacks.frame_category("[k] do_syscall_64"), "kernel")
        self.assertEqual(stacks.frame_category("tokio::runtime::run"), "other")

    def test_a_sample_counts_toward_its_innermost_subsystem_frame(self):
        categorizer = stacks.Categorizer()
        self.assertEqual(
            categorizer.stack(["tokio::run", "zakura_network::peer::send", "sendmsg", "[k] udp_sendmsg"]),
            "network",
        )
        self.assertEqual(categorizer.stack(["tokio::run", "epoll_wait", "[k] ep_poll"]), "kernel")
        self.assertEqual(categorizer.stack(["tokio::run"]), "other")

    def test_every_view_and_category_is_listed_for_the_page(self):
        self.assertEqual(stacks.view_ids()[0], "all")
        self.assertEqual(set(stacks.view_ids()[1:]), {c["id"] for c in stacks.CATEGORIES})
        self.assertEqual(set(stacks.category_ids()), set(stacks.view_ids()[1:]) | {"kernel", "other"})


class TreeTests(unittest.TestCase):
    def test_merging_adds_time_function_by_function(self):
        first, second = stacks.new_tree(), stacks.new_tree()
        stacks.add_stack(first, ["a", "b"], 10, 4)
        stacks.add_stack(second, ["a", "b"], 5)
        stacks.add_stack(second, ["a", "c"], 7, 7)
        stacks.merge_into(first, second)
        b = first.children["a"].children["b"]
        self.assertEqual((b.self_us, b.block_us), (15, 4))
        self.assertEqual(stacks.total_us(first), 22)
        self.assertEqual(stacks.total_us(first, block_only=True), 11)

    def test_a_stored_tree_reads_back_identical(self):
        tree, _ = window_tree([1794528036.3])
        restored = stacks.decode_tree(json.loads(json.dumps(stacks.encode_tree(tree))))
        self.assertEqual(nested(restored), nested(tree))

    def test_a_view_merges_one_subsystem_across_threads_and_callers(self):
        tree, _ = window_tree()
        view = stacks.view_tree(tree, "verification", stacks.Categorizer())
        # Verification ran on a rayon thread and, cut short, on a tokio worker.
        self.assertEqual(list(view.children), ["zakura_consensus::block::verify"])
        self.assertEqual(stacks.total_us(view), 20_000)
        self.assertEqual(list(view.children["zakura_consensus::block::verify"].children), ["pasta_curves::fields::mul"])

    def test_the_whole_process_view_keeps_thread_groups(self):
        tree, _ = window_tree()
        self.assertIs(stacks.view_tree(tree, "all", stacks.Categorizer()), tree)
        self.assertTrue(all(name.startswith("thread: ") for name in tree.children))

    def test_focusing_on_a_function_keeps_only_what_ran_under_it(self):
        tree, _ = window_tree()
        focused = stacks.function_tree(tree, "tokio::runtime::run")
        self.assertEqual(stacks.total_us(focused), 10_000)

    def test_flame_document_folds_small_frames_and_keeps_every_total(self):
        tree = stacks.new_tree()
        stacks.add_stack(tree, ["main", "big"], 9_990)
        for index in range(10):
            stacks.add_stack(tree, ["main", f"tiny{index}"], 1)
        document = stacks.flame_document(tree, min_share=0.001)
        names = [document["names"][i] for i in document["n"]]
        self.assertEqual(sorted(names), ["(small frames)", "big", "main"])
        self.assertEqual(document["total_us"], 10_000)
        self.assertEqual(document["v"][names.index("main")], 10_000)
        self.assertEqual(document["v"][names.index("(small frames)")], 10)
        self.assertEqual(document["top"][0], ["big", 9_990])
        # Folded functions still appear in the list of functions by own time.
        self.assertIn(["tiny0", 1], document["top"])

    def test_flame_document_can_show_only_time_near_blocks(self):
        tree, totals = window_tree([1794528036.3])
        document = stacks.flame_document(tree, block_only=True)
        self.assertEqual(document["total_us"], totals["block_us"])
        self.assertEqual(totals["block_us"], 20_000)


class WindowTests(unittest.TestCase):
    def test_samples_are_weighted_by_the_sampling_rate_and_split_by_category(self):
        tree, totals = window_tree()
        self.assertEqual(totals["samples"], 3)
        self.assertEqual(totals["cpu_us"], 30_000)
        self.assertEqual(totals["by_category"], {"network": 10_000, "verification": 20_000})
        self.assertEqual(stacks.total_us(tree), 30_000)

    def test_a_truncated_stack_hangs_under_a_marker_in_its_thread_group(self):
        tree, _ = window_tree()
        tokio = tree.children["thread: " + stacks.thread_group("tokio-rt-worker")]
        self.assertIn("[stack truncated]", tokio.children)

    def test_block_window_covers_just_before_to_just_after_the_announcement(self):
        block = 1000.0
        inside = profiler_module.in_block_window
        self.assertTrue(inside(block - 0.24, [block]))
        self.assertTrue(inside(block + 0.09, [block]))
        self.assertFalse(inside(block - 0.26, [block]))
        self.assertFalse(inside(block + 0.11, [block]))
        self.assertFalse(inside(block, []))

    def test_rate_falls_when_a_window_is_over_budget_and_recovers_when_quiet(self):
        next_rate = profiler_module.next_rate
        self.assertEqual(next_rate(199, 500, budget=3000), 199)
        self.assertEqual(next_rate(199, 12_000, budget=3000), 49)
        self.assertEqual(next_rate(199, 10**6, budget=3000), 19)
        self.assertEqual(next_rate(19, 100, budget=3000), 199)


def summary(start, cpu_us=1000, build="1.0"):
    return {
        "start": start, "end": start + 60, "rate": 199, "pid": 1, "build": build, "lost": False,
        "overhead_cpu_s": 0.6, "samples": 2, "cpu_us": cpu_us, "block_us": 0,
        "by_category": {"network": cpu_us},
    }  # fmt: skip


def tree_of(name, cpu_us):
    tree = stacks.new_tree()
    stacks.add_stack(tree, ["thread: tokio", name], cpu_us)
    return tree


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.store = profiler_module.Store(self.directory.name)
        # A whole number of hours, recent enough to be inside every retention period.
        self.hour = int(time.time()) // 3600 * 3600 - 7200

    def test_a_range_merges_the_windows_that_started_inside_it(self):
        for offset, name in ((0, "a"), (60, "b"), (120, "c")):
            self.store.add_window(summary(self.hour + offset), tree_of(name, 1000))
        merged = self.store.merged_tree(self.hour + 60, self.hour + 180)
        self.assertEqual(sorted(merged.children["thread: tokio"].children), ["b", "c"])

    def test_hourly_files_replace_the_windows_they_were_merged_from(self):
        for offset in (0, 1800, 3600):
            self.store.add_window(summary(self.hour + offset), tree_of("f", 1000))
        self.store.roll_up_hours(self.hour + 2 * 3600 + 200)
        self.assertTrue((self.store.hours / f"{self.hour}.json.gz").exists())
        # With the first hour's per-window files gone, the range still adds up.
        for path in self.store.minutes.glob("*.json.gz"):
            if int(path.name.split(".")[0]) < self.hour + 3600:
                path.unlink()
        merged = self.store.merged_tree(self.hour, self.hour + 2 * 3600)
        self.assertEqual(stacks.total_us(merged), 3000)

    def test_a_window_stored_after_its_hour_was_merged_still_counts(self):
        for offset in range(0, 3540, 60):
            self.store.add_window(summary(self.hour + offset), tree_of("f", 1000))
        self.store.roll_up_hours(self.hour + 3600 + 130)
        # Processing was behind: the hour's last window is stored after the roll-up.
        self.store.add_window(summary(self.hour + 3590), tree_of("late", 1_000_000))
        merged = self.store.merged_tree(self.hour, self.hour + 3600 + 300)
        self.assertEqual(stacks.total_us(merged), 59 * 1000 + 1_000_000)

    def test_an_unfinished_hour_is_not_rolled_up(self):
        self.store.add_window(summary(self.hour), tree_of("f", 1000))
        self.store.roll_up_hours(self.hour + 1800)
        self.assertEqual(list(self.store.hours.glob("*")), [])

    def test_pruning_keeps_windows_whose_hour_has_not_been_merged(self):
        old = self.hour - profiler_module.MINUTE_RETENTION_SECONDS - 3600
        self.store.add_window(summary(old), tree_of("f", 1000))
        self.store.prune(time.time())
        self.assertEqual(len(list(self.store.minutes.glob("*.json.gz"))), 1)
        self.store.roll_up_hours(time.time())
        self.store.prune(time.time())
        self.assertEqual(list(self.store.minutes.glob("*.json.gz")), [])
        self.assertEqual(stacks.total_us(self.store.merged_tree(old - old % 3600, old - old % 3600 + 3600)), 1000)

    def test_summaries_and_blocks_survive_a_restart(self):
        self.store.add_window(summary(self.hour), tree_of("f", 1000))
        self.store.add_blocks([(self.hour + 5.5, 100, "ab" * 32)])
        self.store.add_blocks([(self.hour + 5.5, 100, "ab" * 32)])
        reopened = profiler_module.Store(self.directory.name)
        self.assertEqual([w["start"] for w in reopened.windows], [self.hour])
        self.assertEqual(reopened.block_times(self.hour, self.hour + 60), [self.hour + 5.5])

    def test_block_stage_records_survive_a_restart_with_their_announcement_time(self):
        self.store.add_blocks([(self.hour + 5.5, 10, BLOCK_HASH)])
        self.store.add_block_stages([stage_record()])
        (record,) = profiler_module.Store(self.directory.name).recent_block_stages(5)
        self.assertEqual((record["height"], record["t"], record["ms"]["verify"]), (10, self.hour + 5.5, 2.0))


# Stands in for `perf script`: prints the sample text, optional warnings, then sleeps.
FAKE_PERF = """\
#!/usr/bin/env python3
import os, sys, time
sys.stderr.write("w" * int(os.environ.get("FAKE_PERF_WARNING_BYTES", "0")))
sys.stderr.write(os.environ.get("FAKE_PERF_ERRORS", ""))
sys.stderr.flush()
sys.stdout.write(open(os.environ["FAKE_PERF_SAMPLES"]).read())
sys.stdout.flush()
time.sleep(float(os.environ.get("FAKE_PERF_SLEEP", "0")))
"""


class PerfScriptTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        perf = self.directory / "perf"
        perf.write_text(FAKE_PERF)
        perf.chmod(0o755)
        (self.directory / "samples.txt").write_text(PERF_SCRIPT)
        args = argparse.Namespace(
            node_name="n", service="zakurad", window=60, log_file="", perf=str(perf),
            scratch_dir=str(self.directory), trace_dir="", trace_table_cap_mb=200,
        )  # fmt: skip
        self.profiler = profiler_module.Profiler(args, profiler_module.Store(self.directory / "store"))
        self.profiler.symbols = stacks.SymbolNames(fake_demangle)
        self.job = {"path": self.directory / "window.data", "rate": 100}
        self.environment = {"FAKE_PERF_SAMPLES": str(self.directory / "samples.txt")}

    def run_script(self, **environment):
        saved = {key: os.environ.get(key) for key in {**self.environment, **environment}}
        os.environ.update(self.environment, **environment)
        try:
            return self.profiler.run_perf_script(self.job, [])
        finally:
            for key, value in saved.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_many_warnings_do_not_stall_reading_the_samples(self):
        _, totals, errors = self.run_script(
            FAKE_PERF_WARNING_BYTES="300000", FAKE_PERF_ERRORS="lost 3 chunks"
        )
        self.assertEqual(totals["samples"], 3)
        self.assertTrue(errors.endswith("lost 3 chunks"))

    def test_a_hung_run_is_stopped(self):
        original = profiler_module.PERF_SCRIPT_TIMEOUT_SECONDS
        profiler_module.PERF_SCRIPT_TIMEOUT_SECONDS = 1
        self.addCleanup(setattr, profiler_module, "PERF_SCRIPT_TIMEOUT_SECONDS", original)
        started = time.monotonic()
        with self.assertRaises(TimeoutError):
            self.run_script(FAKE_PERF_SLEEP="30")
        self.assertLess(time.monotonic() - started, 10)

    def test_a_failure_shows_only_its_summary_on_the_page(self):
        self.profiler.fail("perf record could not start", "Permission denied: /root/secret")
        status = self.profiler.status()
        self.assertEqual((status["state"], status["error"]), ("error", "perf record could not start"))


class TraceRetentionTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        args = argparse.Namespace(
            node_name="n", service="zakurad", window=60, log_file="", perf="perf",
            scratch_dir=str(self.directory / "scratch"), trace_dir=str(self.directory / "traces"),
            trace_table_cap_mb=1,
        )
        Path(args.trace_dir).mkdir()
        self.profiler = profiler_module.Profiler(args, profiler_module.Store(self.directory / "store"))
        self.table = Path(args.trace_dir) / "discovery.jsonl"
        self.table.write_bytes(b"x" * (1024 * 1024 + 1))

    def test_retention_keeps_running_when_perf_cannot_start(self):
        failed = threading.Event()
        followed = threading.Event()
        real_fail = self.profiler.fail
        real_add = self.profiler.store.add_block_stages

        def report_failure(*arguments):
            real_fail(*arguments)
            failed.set()

        def remember_stages(records):
            real_add(records)
            followed.set()

        with (
            patch.object(self.profiler, "main_pid", return_value=123),
            patch.object(self.profiler, "node_build", return_value="test"),
            patch.object(profiler_module.subprocess, "Popen", side_effect=OSError("perf is unavailable")),
            patch.object(self.profiler, "fail", side_effect=report_failure),
            patch.object(self.profiler.store, "add_block_stages", side_effect=remember_stages),
        ):
            recorder = threading.Thread(target=self.profiler.record_loop)
            follower = threading.Thread(target=self.profiler.trace_loop)
            recorder.start()
            try:
                self.assertTrue(failed.wait(2), "CPU sampling must fail before retention starts")
                self.assertFalse(self.profiler.status()["trace_retention"]["ready"])
                follower.start()
                self.assertTrue(followed.wait(2), "trace following must run without a CPU window")
            finally:
                self.profiler.stop()
                recorder.join(timeout=2)
                if follower.ident is not None:
                    follower.join(timeout=2)
        self.assertFalse(recorder.is_alive())
        self.assertFalse(follower.is_alive())
        self.assertFalse(self.table.exists())
        self.assertGreater(self.table.with_suffix(".jsonl.1").stat().st_size, 1024 * 1024)
        self.assertEqual(len(self.profiler._jobs), 0)
        self.assertTrue(self.profiler.status()["trace_retention"]["ready"])

    def test_a_stale_retention_worker_is_not_ready(self):
        self.profiler._trace_checked_at = time.monotonic() - 31
        self.assertFalse(self.profiler.status()["trace_retention"]["ready"])

    def test_a_failed_stage_write_is_reported_after_the_trace_file_is_rotated(self):
        failed = threading.Event()
        with (
            patch.object(self.profiler.store, "add_block_stages", side_effect=OSError("disk write failed")),
            patch.object(self.profiler, "fail", side_effect=lambda *_: failed.set()),
        ):
            follower = threading.Thread(target=self.profiler.trace_loop)
            follower.start()
            try:
                self.assertTrue(failed.wait(2))
            finally:
                self.profiler.stop()
                follower.join(timeout=2)
        self.assertFalse(follower.is_alive())
        self.assertFalse(self.table.exists())
        self.assertGreater(self.table.with_suffix(".jsonl.1").stat().st_size, 1024 * 1024)
        self.assertFalse(self.profiler.status()["trace_retention"]["ready"])

    def test_a_table_that_cannot_be_rotated_is_reported_and_not_ready(self):
        failed = threading.Event()
        real_fail = self.profiler.fail

        def report_failure(*arguments):
            real_fail(*arguments)
            failed.set()

        size = self.table.stat().st_size
        with (
            patch.object(blocks.os, "replace", side_effect=PermissionError("the table is read-only")),
            patch.object(self.profiler, "fail", side_effect=report_failure),
        ):
            follower = threading.Thread(target=self.profiler.trace_loop)
            follower.start()
            try:
                self.assertTrue(failed.wait(2), "a table left over the cap must be reported")
            finally:
                self.profiler.stop()
                follower.join(timeout=2)
        self.assertFalse(follower.is_alive())
        self.assertEqual(self.table.stat().st_size, size)
        status = self.profiler.status()
        self.assertFalse(status["trace_retention"]["ready"])
        self.assertEqual(status["error"], "trimming trace files failed (PermissionError)")

    def test_an_unconfigured_trace_reader_exits_without_claiming_readiness(self):
        self.profiler.traces = None
        self.profiler.trace_loop()
        self.assertIsNone(self.profiler.status()["trace_retention"])


class LogFollowingTests(unittest.TestCase):
    LINE = (
        b'2026-11-12T23:00:06.158820Z  INFO zakurad::components::sync::gossip: sending block announcement '
        b'height=Height(3566895) hash=block::Hash("00000000005ca2fb98ca2410db5304c132cf91b28d1175ddcac294ae45b1ab3b") '
        b"size=None is_block_submission=false"
    )

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "node.log"

    def test_announcement_line_gives_time_height_and_hash(self):
        when, height, block_hash = blocks.parse_announcement(self.LINE)
        self.assertAlmostEqual(when, 1794524406.15882, places=5)
        self.assertEqual(height, 3566895)
        self.assertEqual(block_hash, "00000000005ca2fb98ca2410db5304c132cf91b28d1175ddcac294ae45b1ab3b")
        self.assertIsNone(blocks.parse_announcement(b"2026-11-12T23:00:06Z INFO something else"))

    def test_only_complete_new_lines_are_returned(self):
        self.path.write_bytes(b"first\nsecond\npart")
        follower = blocks.FileFollower(self.path)
        self.assertEqual(follower.read_new_lines(), [b"first", b"second"])
        self.assertEqual(follower.read_new_lines(), [])
        with open(self.path, "ab") as handle:
            handle.write(b"ial\nthird\n")
        self.assertEqual(follower.read_new_lines(), [b"partial", b"third"])

    def test_a_long_log_is_picked_up_near_its_end_on_a_line_boundary(self):
        self.path.write_bytes(b"x" * 100 + b"\nrecent one\nrecent two\n")
        follower = blocks.FileFollower(self.path, lookback_bytes=30)
        self.assertEqual(follower.read_new_lines(), [b"recent one", b"recent two"])

    def test_a_truncated_or_missing_log_is_read_again_from_the_start(self):
        follower = blocks.FileFollower(self.path)
        self.assertEqual(follower.read_new_lines(), [])
        self.path.write_bytes(b"one\ntwo\n")
        self.assertEqual(follower.read_new_lines(), [b"one", b"two"])
        self.path.write_bytes(b"new\n")
        self.assertEqual(follower.read_new_lines(), [b"new"])

    def test_a_renamed_file_is_read_to_its_end_before_the_new_one(self):
        moved = self.path.with_name("node.log.1")
        follower = blocks.FileFollower(self.path, moved_path=moved)
        self.path.write_bytes(b"one\n")
        self.assertEqual(follower.read_new_lines(), [b"one"])
        os.replace(self.path, moved)
        # The writer has not started a new file yet and still appends to the renamed one.
        with open(moved, "ab") as handle:
            handle.write(b"two\n")
        self.assertEqual(follower.read_new_lines(), [b"two"])
        with open(moved, "ab") as handle:
            handle.write(b"three\n")
        self.path.write_bytes(b"four\n")
        self.assertEqual(follower.read_new_lines(), [b"three", b"four"])
        self.assertEqual(follower.read_new_lines(), [])

    def test_a_follower_that_starts_with_only_the_renamed_file_follows_it(self):
        moved = self.path.with_name("node.log.1")
        moved.write_bytes(b"x" * 100 + b"\nrecent\n")
        follower = blocks.FileFollower(self.path, lookback_bytes=20, moved_path=moved)
        self.assertEqual(follower.read_new_lines(), [b"recent"])
        with open(moved, "ab") as handle:
            handle.write(b"later\n")
        self.assertEqual(follower.read_new_lines(), [b"later"])
        with open(moved, "ab") as handle:
            handle.write(b"last\n")
        self.path.write_bytes(b"new\n")
        self.assertEqual(follower.read_new_lines(), [b"last", b"new"])

    def test_a_renamed_tail_is_drained_across_bounded_reads_before_the_new_file(self):
        for tail in (b"a" * 15 + b"\n", b"old0001\nold0002\n" + b"split" * 5 + b"\nlast\n"):
            with self.subTest(tail=tail), mock.patch.object(blocks, "MAX_READ_BYTES", 16):
                moved = self.path.with_name("node.log.1")
                self.path.write_bytes(b"initial\n")
                follower = blocks.FileFollower(self.path, moved_path=moved)
                self.assertEqual(follower.read_new_lines(), [b"initial"])
                with open(self.path, "ab") as handle:
                    handle.write(tail)
                os.replace(self.path, moved)
                self.path.write_bytes(b"new\n")

                returned = []
                for _ in range(10):
                    returned.extend(follower.read_new_lines())
                self.assertEqual(returned, tail.splitlines() + [b"new"])
                self.assertEqual(follower.read_new_lines(), [])

    def test_a_missing_renamed_file_does_not_block_switching_to_the_new_file(self):
        with mock.patch.object(blocks, "MAX_READ_BYTES", 8):
            moved = self.path.with_name("node.log.1")
            self.path.write_bytes(b"initial\n")
            follower = blocks.FileFollower(self.path, moved_path=moved)
            self.assertEqual(follower.read_new_lines(), [b"initial"])
            with open(self.path, "ab") as handle:
                handle.write(b"old0001\nold0002\n")
            os.replace(self.path, moved)
            self.path.write_bytes(b"new\n")
            self.assertEqual(follower.read_new_lines(), [b"old0001"])
            moved.unlink()
            self.assertEqual(follower.read_new_lines(), [b"new"])

    def test_a_renamed_file_that_is_not_the_one_being_followed_is_skipped(self):
        moved = self.path.with_name("node.log.1")
        follower = blocks.FileFollower(self.path, moved_path=moved)
        self.path.write_bytes(b"one\n")
        self.assertEqual(follower.read_new_lines(), [b"one"])
        # The followed file went somewhere else, and an unrelated file has the renamed name.
        os.replace(self.path, self.path.with_name("node.log.elsewhere"))
        moved.write_bytes(b"unrelated\n")
        self.assertEqual(follower.read_new_lines(), [])
        self.path.write_bytes(b"two\n")
        self.assertEqual(follower.read_new_lines(), [b"two"])


NODE_RUN = "100-1794528928000000000"
BLOCK_HASH = "ab" * 32


def trace_row(event, ts, process=NODE_RUN, **fields):
    """One row of a node trace table; `ts` is microseconds since the node started."""
    return {"ts": ts, "event": event, "process_trace_id": process, **fields}


def tip_row(ts, height=10, block_hash=BLOCK_HASH, process=NODE_RUN, queue_us=500, commit_us=4000):
    return trace_row(
        "tip_published", ts, process, tip_height=height, tip_hash=block_hash, kind="extend",
        queue_us=queue_us, commit_us=commit_us,
    )  # fmt: skip


def stage_record(height=10, block_hash=BLOCK_HASH, verify=2.0):
    stages = dict.fromkeys(stage for stage, _, _ in blocks.STAGES)
    stages.update(body_download=12, verify=verify, writer_wait=0.5, writer_commit=4.0, status_fanout=0.6)
    return {
        "height": height, "hash": block_hash, "process": NODE_RUN, "tip_us": 1_010_000, "reorg": False,
        "path": "legacy", "size": None, "status_peers": 3, "status_slowest_ms": 0.8, "ms": stages,
    }  # fmt: skip


class BlockStageTests(unittest.TestCase):
    def finished_after(self, rows, settle_at=10_000_000, process=NODE_RUN):
        stages = blocks.BlockStages()
        for row in rows:
            stages.feed(row)
        # Any later row moves the node's clock past the block's settling time.
        stages.feed(trace_row("block_status_sent", settle_at, process, height=999))
        return stages.finished()

    def test_a_block_from_the_legacy_network_starts_when_its_download_finishes(self):
        (record,) = self.finished_after(
            [
                trace_row(
                    "block_request_finish", 1_000_000, result="available", elapsed_ms=12,
                    returned_hash=BLOCK_HASH, returned_height=10,
                ),
                tip_row(1_010_000),
                trace_row("block_status_sent", 1_010_400, height=10),
                trace_row("block_status_sent", 1_010_600, height=10),
                trace_row("block_status_sent", 1_010_800, height=10),
                # A peer that connected two seconds later is not part of the fan-out.
                trace_row("block_status_sent", 3_010_000, height=10),
            ]
        )  # fmt: skip
        self.assertEqual((record["height"], record["hash"], record["path"]), (10, BLOCK_HASH, "legacy"))
        self.assertEqual((record["status_peers"], record["status_slowest_ms"]), (3, 0.8))
        self.assertEqual(
            {stage: ms for stage, ms in record["ms"].items() if ms is not None},
            # Verify ends where the writer's queue begins: tip - wait - commit.
            {"body_download": 12, "verify": 5.5, "writer_wait": 0.5, "writer_commit": 4.0, "status_fanout": 0.6},
        )

    def test_a_block_from_zakura_block_sync_has_every_stage(self):
        (record,) = self.finished_after(
            [
                trace_row("header_request_sent", 0, branch_target=BLOCK_HASH),
                trace_row("header_response_received", 80_000, branch_target=BLOCK_HASH),
                trace_row("header_target_admitted", 86_000, branch_target=BLOCK_HASH),
                trace_row("block_get_blocks_sent", 90_000, request_owner=f"tip({BLOCK_HASH})"),
                trace_row("block_body_received", 92_500, height=10, serialized_bytes=1500),
                trace_row("commit_start", 93_000, height=10, hash=BLOCK_HASH),
                tip_row(100_000, queue_us=1000, commit_us=5000),
            ]
        )
        self.assertEqual((record["path"], record["size"], record["status_peers"]), ("zakura", 1500, 0))
        self.assertEqual(
            record["ms"],
            {
                "header_fetch": 80.0, "header_check": 6.0, "body_request": 4.0, "body_download": 2.5,
                "submit": 0.5, "verify": 1.0, "writer_wait": 1.0, "writer_commit": 5.0, "status_fanout": None,
            },
        )  # fmt: skip

    def test_a_block_is_held_until_its_fan_out_had_time_to_be_recorded(self):
        stages = blocks.BlockStages()
        stages.feed(tip_row(1_010_000))
        stages.feed(trace_row("block_status_sent", 1_500_000, height=10))
        self.assertEqual(stages.finished(), [])
        stages.feed(trace_row("block_status_sent", 4_100_000, height=11))
        self.assertEqual([record["height"] for record in stages.finished()], [10])
        self.assertEqual(stages.finished(), [])

    def test_rows_from_before_a_node_restart_are_not_joined(self):
        earlier_run = "99-1794520000000000000"
        (record,) = self.finished_after(
            [
                trace_row(
                    "block_request_finish", 9_000_000, earlier_run, result="available", elapsed_ms=12,
                    returned_hash=BLOCK_HASH, returned_height=10,
                ),
                tip_row(2_000_000),
            ]
        )  # fmt: skip
        self.assertEqual(record["path"], "unknown")
        self.assertIsNone(record["ms"]["verify"])
        self.assertEqual(record["ms"]["writer_commit"], 4.0)

    def test_a_block_seen_again_after_a_restart_keeps_its_earlier_timings(self):
        later_run = "101-1794529000000000000"
        stages = blocks.BlockStages()
        stages.feed(tip_row(1_010_000))
        # The restarted node publishes the same block as its tip again.
        stages.feed(tip_row(50_000, process=later_run, queue_us=0, commit_us=0))
        stages.feed(trace_row("block_status_sent", 9_000_000, later_run, height=999))
        self.assertEqual(
            [(record["process"], record["ms"]["writer_commit"]) for record in stages.finished()],
            [(NODE_RUN, 4.0), (later_run, 0.0)],
        )

    def test_a_block_request_that_returned_nothing_is_not_a_download(self):
        (record,) = self.finished_after(
            [
                trace_row(
                    "block_request_finish", 1_000_000, result="missing", elapsed_ms=900,
                    returned_hash=BLOCK_HASH, returned_height=10,
                ),
                tip_row(1_010_000),
            ]
        )  # fmt: skip
        self.assertEqual(record["path"], "unknown")
        self.assertIsNone(record["ms"]["body_download"])


class TraceFollowingTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)

    def append(self, table, *rows, renamed=False):
        """Append rows to a table, or to the file it was renamed to when it passed the cap."""
        name = f"{table}.jsonl.1" if renamed else f"{table}.jsonl"
        with open(self.directory / name, "a") as handle:
            for row in rows:
                handle.write(json.dumps(row, separators=(",", ":")) + "\n")

    def size(self, name):
        return (self.directory / name).stat().st_size

    def test_blocks_are_joined_across_tables_and_other_rows_are_skipped(self):
        follower = blocks.TraceFollower(str(self.directory), table_cap_bytes=1 << 20)
        self.append(
            "legacy_peer_request",
            trace_row(
                "block_request_finish", 1_000_000, result="available", elapsed_ms=12,
                returned_hash=BLOCK_HASH, returned_height=10,
            ),
        )  # fmt: skip
        self.append("block_sync", trace_row("block_peer_wake", 1_005_000, height=10))
        self.append("chain_tip", tip_row(1_010_000))
        self.assertEqual(follower.poll(), [])
        self.append("block_sync", trace_row("block_status_sent", 4_100_000, height=11))
        (record,) = follower.poll()
        self.assertEqual((record["height"], record["path"], record["ms"]["verify"]), (10, "legacy", 5.5))

    def twelve_tips(self):
        """Enough `chain_tip` rows, for heights 0 to 11, to pass a 2000-byte cap."""
        return [tip_row(1_000_000 + index, height=index, block_hash=f"{index:064x}") for index in range(12)]

    def heights(self, records):
        return [record["height"] for record in records]

    def test_a_table_over_the_cap_is_renamed_and_no_row_is_lost(self):
        follower = blocks.TraceFollower(str(self.directory), table_cap_bytes=2000)
        # A table the profiler does not read is kept under the cap too.
        self.append("discovery", *[trace_row("peer_seen", index, peer="p" * 40) for index in range(20)])
        self.append("chain_tip", *self.twelve_tips())
        written = {name: self.size(name) for name in ("discovery.jsonl", "chain_tip.jsonl")}
        follower.poll()
        for name, size in written.items():
            self.assertFalse((self.directory / name).exists())
            self.assertEqual(self.size(name + ".1"), size)
        # The node appends to the renamed file until its next sync, then starts a new one.
        self.append("chain_tip", tip_row(1_100_000, height=50), renamed=True)
        self.append("chain_tip", tip_row(9_000_000, height=51, block_hash="cd" * 32))
        self.assertEqual(self.heights(follower.poll()), list(range(12)) + [50])

    def test_a_renamed_table_is_followed_until_the_node_starts_a_new_one(self):
        follower = blocks.TraceFollower(str(self.directory), table_cap_bytes=2000)
        self.append("chain_tip", *self.twelve_tips())
        follower.poll()
        self.append(
            "chain_tip", tip_row(1_100_000, height=50), tip_row(9_000_000, height=51, block_hash="cd" * 32),
            renamed=True,
        )  # fmt: skip
        self.assertEqual(self.heights(follower.poll()), list(range(12)) + [50])
        self.assertFalse((self.directory / "chain_tip.jsonl").exists())
        self.append("chain_tip", tip_row(20_000_000, height=52, block_hash="ef" * 32))
        self.assertEqual(self.heights(follower.poll()), [51])

    def test_a_restarted_profiler_follows_a_table_the_node_still_writes_under_its_renamed_name(self):
        self.append("chain_tip", *self.twelve_tips())
        blocks.TraceFollower(str(self.directory), table_cap_bytes=2000).poll()
        self.assertFalse((self.directory / "chain_tip.jsonl").exists())
        # A node build that never starts a new file, and a profiler started after the rename.
        restarted = blocks.TraceFollower(str(self.directory), table_cap_bytes=2000)
        self.append(
            "chain_tip", tip_row(1_100_000, height=50), tip_row(9_000_000, height=51, block_hash="cd" * 32),
            renamed=True,
        )  # fmt: skip
        self.assertEqual(self.heights(restarted.poll()), list(range(12)) + [50])

    def test_rotation_keeps_both_files_until_the_renamed_backlog_is_read(self):
        follower = blocks.TraceFollower(str(self.directory), table_cap_bytes=20)
        current = self.directory / "block_sync.jsonl"
        moved = self.directory / "block_sync.jsonl.1"
        current.write_bytes(b"initial\n")
        follower.poll()
        with open(current, "ab") as handle:
            handle.write(b"old\n" * 30)
        with mock.patch.object(blocks, "MAX_READ_BYTES", 16):
            follower.poll()
            original = moved.read_bytes()
            self.assertGreater(len(original), 2 * follower.table_cap_bytes)
            current.write_bytes(b"new\n" * 10)
            follower.poll()
            # Neither the twice-cap fallback nor the next rotation may destroy the tail.
            self.assertEqual(moved.read_bytes(), original)
            self.assertEqual(current.read_bytes(), b"new\n" * 10)
            for _ in range(20):
                follower.poll()
            self.assertEqual(moved.read_bytes(), b"new\n" * 10)
            self.assertFalse(current.exists())

    def test_rotation_keeps_the_unread_tail_while_the_writer_has_not_reopened(self):
        follower = blocks.TraceFollower(str(self.directory), table_cap_bytes=20)
        current = self.directory / "block_sync.jsonl"
        moved = self.directory / "block_sync.jsonl.1"
        current.write_bytes(b"initial\n")
        follower.poll()
        with open(current, "ab") as handle:
            handle.write(b"old\n" * 30)
        with mock.patch.object(blocks, "MAX_READ_BYTES", 16):
            follower.poll()
            original = moved.read_bytes()
            self.assertGreater(len(original), 2 * follower.table_cap_bytes)
            # An updated writer can be delayed before its next sync creates the replacement.
            follower.poll()
            self.assertFalse(current.exists())
            self.assertEqual(moved.read_bytes(), original)
            current.write_bytes(b"new\n" * 10)
            for _ in range(20):
                follower.poll()
            self.assertEqual(moved.read_bytes(), b"new\n" * 10)
            self.assertFalse(current.exists())

    def test_a_renamed_table_that_keeps_growing_is_emptied_at_twice_the_cap(self):
        follower = blocks.TraceFollower(str(self.directory), table_cap_bytes=2000)
        self.append("chain_tip", *self.twelve_tips())
        follower.poll()
        self.assertLess(self.size("chain_tip.jsonl.1"), 4000)
        # A node build that never starts a new file keeps appending to the renamed one.
        self.append("chain_tip", *self.twelve_tips(), renamed=True)
        self.assertGreater(self.size("chain_tip.jsonl.1"), 4000)
        follower.poll()
        self.assertEqual(self.size("chain_tip.jsonl.1"), 0)
        self.append(
            "chain_tip", tip_row(30_000_000, height=60), tip_row(40_000_000, height=61, block_hash="cd" * 32),
            renamed=True,
        )  # fmt: skip
        self.assertIn(60, self.heights(follower.poll()))

    def test_a_cap_of_zero_never_empties_a_table(self):
        follower = blocks.TraceFollower(str(self.directory), table_cap_bytes=0)
        self.append("discovery", *[trace_row("peer_seen", index, peer="p" * 40) for index in range(40)])
        size = (self.directory / "discovery.jsonl").stat().st_size
        follower.poll()
        self.assertEqual((self.directory / "discovery.jsonl").stat().st_size, size)

    def test_a_missing_trace_directory_yields_nothing(self):
        follower = blocks.TraceFollower(str(self.directory / "absent"), table_cap_bytes=2000)
        self.assertEqual(follower.poll(), [])
        self.assertIsNone(follower.trim_failure)

    def test_a_table_that_cannot_be_rotated_is_a_failure_until_it_is_rotated(self):
        follower = blocks.TraceFollower(str(self.directory), table_cap_bytes=2000)
        rows = [trace_row("peer_seen", index, peer="p" * 40) for index in range(40)]
        self.append("discovery", *rows)
        self.append("block_sync", *rows)
        stuck = str(self.directory / "discovery.jsonl")
        real_replace = os.replace

        def refuse_one_table(path, destination):
            if path == stuck:
                raise PermissionError("the table is read-only")
            real_replace(path, destination)

        with patch.object(blocks.os, "replace", side_effect=refuse_one_table):
            follower.poll()
        self.assertIsInstance(follower.trim_failure, PermissionError)
        self.assertGreater(os.path.getsize(stuck), 2000)
        # The other table was rotated despite the failure.
        self.assertFalse((self.directory / "block_sync.jsonl").exists())
        self.assertGreater((self.directory / "block_sync.jsonl.1").stat().st_size, 2000)

        follower.poll()
        self.assertIsNone(follower.trim_failure)
        self.assertFalse(os.path.exists(stuck))
        self.assertGreater(os.path.getsize(stuck + ".1"), 2000)


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        store = profiler_module.Store(self.directory.name)
        self.end = int(time.time()) - 30
        for index in range(3):
            start = self.end - 60 * (3 - index)
            build = "1.0" if index < 2 else "1.1"
            store.add_window(summary(start, cpu_us=6_000_000, build=build), self.sample_tree())
        store.add_blocks([(self.end - 100.0, 7, "cd" * 32)])
        store.add_block_stages(
            [stage_record(6, "ef" * 32, verify=1.0), stage_record(7, "cd" * 32, verify=3.0), stage_record(8, verify=9.0)]
        )
        args = argparse.Namespace(
            node_name="test-node", service="zakurad", window=60, log_file="", perf="perf",
            scratch_dir=self.directory.name, trace_dir="", trace_table_cap_mb=200,
        )  # fmt: skip
        profiler = profiler_module.Profiler(args, store)
        profiler.state = "ok"
        self.queries = profiler_module.Queries(store, profiler)
        page = Path(__file__).resolve().with_name("page.html")
        self.server = ThreadingHTTPServer(
            ("127.0.0.1", 0), profiler_module.make_handler(self.queries, profiler, page)
        )
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

    @staticmethod
    def sample_tree():
        tree = stacks.new_tree()
        stacks.add_stack(tree, ["thread: tokio", "tokio::run", "zakura_consensus::block::verify"], 2_000_000, 500_000)
        stacks.add_stack(tree, ["thread: tokio", "tokio::run", "zakura_network::peer::send"], 4_000_000)
        return tree

    def get(self, path):
        url = f"http://127.0.0.1:{self.server.server_address[1]}{path}"
        try:
            with urllib.request.urlopen(url, timeout=10) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            with error:
                return error.code, error.read()

    def test_status_reports_the_profilers_own_cost_and_the_page_vocabulary(self):
        code, body = self.get("/api/status")
        status = json.loads(body)
        self.assertEqual(code, 200)
        self.assertEqual(status["node"], "test-node")
        self.assertEqual(status["state"], "ok")
        self.assertEqual(status["overhead_pct_last_hour"], 1.0)
        self.assertEqual(status["node_cpu_cores_last_hour"], 0.1)
        self.assertEqual(status["views"][0], "all")
        self.assertIn({"id": "kernel", "label": "Kernel"}, status["categories"])

    def test_flame_for_a_view_reports_its_share_of_all_cpu(self):
        code, body = self.get("/api/flame?range=15m&view=verification")
        flame = json.loads(body)
        self.assertEqual(code, 200)
        self.assertEqual(flame["total_us"], 6_000_000)
        self.assertEqual(flame["whole_us"], 18_000_000)
        self.assertEqual(flame["names"], ["zakura_consensus::block::verify"])
        self.assertEqual(flame["categories"], ["verification"])
        self.assertEqual((flame["samples"], flame["blocks"], flame["covered_seconds"]), (6, 1, 180.0))

    def test_flame_can_be_limited_to_block_windows_and_to_one_function(self):
        _, body = self.get("/api/flame?range=15m&lens=block")
        self.assertEqual(json.loads(body)["total_us"], 1_500_000)
        _, body = self.get("/api/flame?range=15m&focus=zakura_network%3A%3Apeer")
        self.assertEqual(json.loads(body)["total_us"], 12_000_000)

    def test_flame_for_an_explicit_range_covers_only_that_range(self):
        _, body = self.get(f"/api/flame?from={self.end - 125}&to={self.end}")
        self.assertEqual(json.loads(body)["whole_us"], 12_000_000)

    def test_timeline_has_cpu_by_category_blocks_and_build_changes(self):
        code, body = self.get("/api/timeline?range=15m")
        timeline = json.loads(body)
        self.assertEqual(code, 200)
        self.assertEqual(len(timeline["buckets"]), 3)
        self.assertEqual(timeline["buckets"][0]["us"], {"network": 6_000_000})
        self.assertEqual(timeline["blocks"], [self.end - 100.0])
        self.assertEqual([build for _, build in timeline["builds"]], ["1.0", "1.1"])

    def test_blocks_report_each_stage_with_medians_and_announcement_times(self):
        code, body = self.get("/api/blocks?limit=50")
        answer = json.loads(body)
        self.assertEqual(code, 200)
        self.assertEqual([stage["id"] for stage in answer["stages"]], [stage for stage, _, _ in blocks.STAGES])
        self.assertEqual(list(answer["paths"]), ["legacy"])
        legacy = answer["paths"]["legacy"]
        self.assertEqual((legacy["blocks"], legacy["median_ms"]["verify"], legacy["p90_ms"]["verify"]), (3, 3.0, 9.0))
        self.assertNotIn("header_fetch", legacy["median_ms"])
        # Only the block whose announcement was seen in the log has a wall-clock time.
        self.assertEqual([block["t"] for block in answer["blocks"]], [None, self.end - 100.0, None])
        self.assertEqual(answer["blocks"][1]["ms"]["writer_commit"], 4.0)
        self.assertEqual(len(json.loads(self.get("/api/blocks?limit=1")[1])["blocks"]), 1)

    def test_uncached_flame_graphs_are_refused_once_their_cpu_budget_is_spent(self):
        self.assertEqual(self.get("/api/flame?range=15m")[0], 200)
        original = profiler_module.QUERY_CPU_BUDGET_SECONDS
        profiler_module.QUERY_CPU_BUDGET_SECONDS = 0
        self.addCleanup(setattr, profiler_module, "QUERY_CPU_BUDGET_SECONDS", original)
        # A graph already computed is still served; a new one waits for the budget.
        self.assertEqual(self.get("/api/flame?range=15m")[0], 200)
        code, body = self.get("/api/flame?range=1h")
        self.assertEqual((code, json.loads(body)), (503, {"error": "busy, try again"}))

    def test_bad_requests_are_refused(self):
        for path in (
            "/api/flame?view=everything",
            "/api/flame?lens=sideways",
            "/api/flame?range=1y",
            "/api/flame?from=10&to=5",
            "/api/flame?focus=" + "x" * 400,
            "/api/timeline?from=abc&to=5",
            "/api/blocks?limit=many",
        ):
            self.assertEqual(self.get(path)[0], 400, path)
        self.assertEqual(self.get("/api/nothing")[0], 404)

    def test_page_and_health_are_served(self):
        code, body = self.get("/")
        self.assertEqual(code, 200)
        self.assertIn(b"continuous CPU profile", body)
        self.assertEqual(self.get("/healthz"), (200, b"ok\n"))


if __name__ == "__main__":
    unittest.main()
