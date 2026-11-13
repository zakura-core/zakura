#!/usr/bin/env python3
"""Tests for the continuous profiler: stack parsing, trees, views, storage and the API."""

import argparse
import json
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
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
        self.assertEqual(first[2]["a"][2]["b"][:2], [15, 4])
        self.assertEqual(stacks.total_us(first), 22)
        self.assertEqual(stacks.total_us(first, block_only=True), 11)

    def test_a_stored_tree_reads_back_identical(self):
        tree, _ = window_tree([1794528036.3])
        restored = stacks.decode_tree(json.loads(json.dumps(stacks.encode_tree(tree))))
        self.assertEqual(restored, tree)

    def test_a_view_merges_one_subsystem_across_threads_and_callers(self):
        tree, _ = window_tree()
        view = stacks.view_tree(tree, "verification", stacks.Categorizer())
        # Verification ran on a rayon thread and, cut short, on a tokio worker.
        self.assertEqual(list(view[2]), ["zakura_consensus::block::verify"])
        self.assertEqual(stacks.total_us(view), 20_000)
        self.assertEqual(list(view[2]["zakura_consensus::block::verify"][2]), ["pasta_curves::fields::mul"])

    def test_the_whole_process_view_keeps_thread_groups(self):
        tree, _ = window_tree()
        self.assertIs(stacks.view_tree(tree, "all", stacks.Categorizer()), tree)
        self.assertTrue(all(name.startswith("thread: ") for name in tree[2]))

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
        tokio = tree[2]["thread: " + stacks.thread_group("tokio-rt-worker")]
        self.assertIn("[stack truncated]", tokio[2])

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
        self.assertEqual(sorted(merged[2]["thread: tokio"][2]), ["b", "c"])

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
        args = argparse.Namespace(
            node_name="test-node", service="zakurad", window=60, log_file="", perf="perf",
            scratch_dir=self.directory.name,
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

    def test_bad_requests_are_refused(self):
        for path in (
            "/api/flame?view=everything",
            "/api/flame?lens=sideways",
            "/api/flame?range=1y",
            "/api/flame?from=10&to=5",
            "/api/flame?focus=" + "x" * 400,
            "/api/timeline?from=abc&to=5",
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
