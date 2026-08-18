import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "zakura-block-propagation-report.py"
SPEC = importlib.util.spec_from_file_location("zakura_block_propagation_report", SCRIPT)
assert SPEC and SPEC.loader
reporter = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = reporter
SPEC.loader.exec_module(reporter)


BLOCK_HASH = "2a" * 32


def write_rows(directory, file_name, rows):
    path = Path(directory) / file_name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))


class PropagationReportTests(unittest.TestCase):
    def test_correlates_legacy_and_native_paths_from_broadcast_origin(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            miner = root / "miner"
            node_a = root / "node-a"
            node_b = root / "node-b"
            native_a_id = "11" * 32
            native_a_peer = reporter.native_peer_label(native_a_id)

            write_rows(
                miner,
                "block_propagation.jsonl",
                [
                    {
                        "wall_ts_unix_us": 1_000_000,
                        "process_trace_id": "miner-1",
                        "event": "mined_block_broadcast_started",
                        "hash": BLOCK_HASH,
                        "height": 42,
                    },
                    {
                        "wall_ts_unix_us": 1_080_000,
                        "process_trace_id": "miner-1",
                        "event": "mined_block_broadcast_finished",
                        "hash": BLOCK_HASH,
                        "height": 42,
                        "result": "ok",
                    },
                ],
            )
            write_rows(
                node_a,
                "block_propagation.jsonl",
                [
                    {
                        "wall_ts_unix_us": 1_010_000,
                        "process_trace_id": "a-1",
                        "event": "block_announced",
                        "hash": BLOCK_HASH,
                        "transport": "legacy",
                        "source": "legacy:198.51.100.7:18233",
                        "disposition": "queued",
                    },
                    {
                        "wall_ts_unix_us": 1_020_000,
                        "process_trace_id": "a-1",
                        "event": "legacy_block_downloaded",
                        "hash": BLOCK_HASH,
                        "height": 42,
                        "source": "legacy:198.51.100.7:18233",
                    },
                    {
                        "wall_ts_unix_us": 1_040_000,
                        "process_trace_id": "a-1",
                        "event": "legacy_block_finished",
                        "hash": BLOCK_HASH,
                        "height": 42,
                        "result": "committed",
                    },
                ],
            )
            write_rows(
                node_b,
                "header_sync.jsonl",
                [
                    {
                        "wall_ts_unix_us": 1_015_000,
                        "process_trace_id": "b-1",
                        "event": "header_status_received",
                        "selected_tip_hash": BLOCK_HASH,
                        "selected_tip_height": 42,
                        "peer": native_a_peer,
                    }
                ],
            )
            write_rows(
                node_b,
                "block_sync.jsonl",
                [
                    {
                        "wall_ts_unix_us": 1_025_000,
                        "process_trace_id": "b-1",
                        "event": "block_body_received",
                        "hash": BLOCK_HASH,
                        "height": 42,
                        "peer": native_a_peer,
                    }
                ],
            )
            write_rows(
                node_b,
                "commit_state.jsonl",
                [
                    {
                        "wall_ts_unix_us": 1_050_000,
                        "process_trace_id": "b-1",
                        "event": "commit_finish",
                        "hash": BLOCK_HASH,
                        "height": 42,
                        "result": "committed",
                    }
                ],
            )

            report = reporter.build_report(
                {"miner": miner, "node-a": node_a, "node-b": node_b},
                BLOCK_HASH,
                native_node_ids={"node-a": native_a_id},
                legacy_node_addresses={"miner": "198.51.100.7"},
            )

            self.assertEqual(report["origin"]["node"], "miner")
            self.assertEqual(report["origin_kind"], "broadcast")
            self.assertEqual(report["nodes"]["node-a"]["commit"]["offset_us"], 40_000)
            self.assertEqual(report["nodes"]["node-a"]["discovery_to_body_us"], 10_000)
            self.assertEqual(report["nodes"]["node-a"]["discovery_to_commit_us"], 30_000)
            self.assertEqual(report["nodes"]["node-b"]["announcement"]["offset_us"], 15_000)
            self.assertEqual(report["announcement_spread_us"], 5_000)
            self.assertEqual(report["commit_spread_us"], 10_000)
            self.assertIn(
                {
                    "source": "node-a",
                    "destination": "node-b",
                    "transport": "native",
                    "first_observed_us": 15_000,
                },
                report["edges"],
            )
            self.assertIn(
                {
                    "source": "miner",
                    "destination": "node-a",
                    "transport": "legacy",
                    "first_observed_us": 10_000,
                },
                report["edges"],
            )

    def test_first_discovery_origin_uses_earliest_observer_announcement(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            miner = root / "miner"
            node_a = root / "node-a"
            node_b = root / "node-b"
            write_rows(
                miner,
                "block_propagation.jsonl",
                [
                    {
                        "wall_ts_unix_us": 1_500_000,
                        "event": "mined_block_broadcast_started",
                        "hash": BLOCK_HASH,
                        "height": 42,
                    }
                ],
            )
            write_rows(
                node_a,
                "block_propagation.jsonl",
                [
                    {
                        "wall_ts_unix_us": 2_000_000,
                        "event": "block_announced",
                        "hash": BLOCK_HASH,
                        "transport": "legacy",
                    },
                    {
                        "wall_ts_unix_us": 2_020_000,
                        "event": "legacy_block_downloaded",
                        "hash": BLOCK_HASH,
                        "height": 42,
                    },
                    {
                        "wall_ts_unix_us": 2_035_000,
                        "event": "legacy_block_finished",
                        "hash": BLOCK_HASH,
                        "height": 42,
                        "result": "committed",
                    },
                ],
            )
            write_rows(
                node_b,
                "block_sync.jsonl",
                [
                    {
                        "wall_ts_unix_us": 2_010_000,
                        "event": "block_body_received",
                        "hash": BLOCK_HASH,
                        "height": 42,
                        "peer": "peer:source",
                    }
                ],
            )

            report = reporter.build_report(
                {"miner": miner, "node-a": node_a, "node-b": node_b},
                BLOCK_HASH,
                origin="first-discovery",
            )
            markdown = reporter.render_markdown(report)

            self.assertEqual(report["origin_kind"], "first_discovery")
            self.assertEqual(report["origin"]["node"], "node-a")
            self.assertEqual(report["origin"]["phase"], "announcement")
            self.assertEqual(report["nodes"]["node-a"]["announcement"]["offset_us"], 0)
            self.assertEqual(report["nodes"]["node-b"]["body_received"]["offset_us"], 10_000)
            self.assertEqual(report["nodes"]["node-a"]["discovery_to_body_us"], 20_000)
            self.assertEqual(report["nodes"]["node-a"]["discovery_to_commit_us"], 35_000)
            self.assertNotIn("mining-node broadcast origin is missing", report["warnings"])
            self.assertFalse(any("precedes t0" in warning for warning in report["warnings"]))
            self.assertIn("first discovery on `node-a` via `legacy`", markdown)
            self.assertIn("# Block propagation report", markdown)

    def test_first_discovery_origin_uses_body_before_later_announcement(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            body_node = root / "body-node"
            announcement_node = root / "announcement-node"
            write_rows(
                body_node,
                "block_sync.jsonl",
                [
                    {
                        "wall_ts_unix_us": 3_000_000,
                        "event": "block_body_received",
                        "hash": BLOCK_HASH,
                        "height": 42,
                    }
                ],
            )
            write_rows(
                announcement_node,
                "block_propagation.jsonl",
                [
                    {
                        "wall_ts_unix_us": 3_005_000,
                        "event": "block_announced",
                        "hash": BLOCK_HASH,
                        "transport": "legacy",
                    }
                ],
            )

            report = reporter.build_report(
                {
                    "body-node": body_node,
                    "announcement-node": announcement_node,
                },
                BLOCK_HASH,
                origin="first-discovery",
            )

            self.assertEqual(report["origin"]["node"], "body-node")
            self.assertEqual(report["origin"]["phase"], "body_received")
            self.assertEqual(report["origin"]["offset_us"], 0)
            self.assertEqual(
                report["nodes"]["announcement-node"]["announcement"]["offset_us"],
                5_000,
            )
            self.assertEqual(report["nodes"]["body-node"]["discovery_to_body_us"], 0)

    def test_dedicated_native_rows_replace_broad_trace_duplicates(self):
        with tempfile.TemporaryDirectory() as temp:
            node = Path(temp) / "observer"
            canonical = {
                "wall_ts_unix_us": 3_000_000,
                "event": "block_body_received",
                "hash": BLOCK_HASH,
                "height": 42,
                "peer": "peer:source",
            }
            write_rows(node, "block_propagation.jsonl", [canonical])
            write_rows(
                node,
                "block_sync.jsonl",
                [{**canonical, "wall_ts_unix_us": 3_000_001}],
            )

            report = reporter.build_report(
                {"observer": node},
                BLOCK_HASH,
                origin="first-discovery",
            )

            body_events = [
                event
                for event in report["events"]
                if event["raw_event"] == "block_body_received"
            ]
            self.assertEqual(len(body_events), 1)
            self.assertEqual(
                body_events[0]["trace_file"],
                "block_propagation.jsonl",
            )
            self.assertEqual(report["duplicates"], [])

    def test_reports_missing_origin_timestamp_restarts_and_duplicate_paths(self):
        with tempfile.TemporaryDirectory() as temp:
            node = Path(temp) / "observer"
            write_rows(
                node,
                "block_propagation.jsonl",
                [
                    {
                        "process_trace_id": "old",
                        "event": "block_announced",
                        "hash": BLOCK_HASH,
                    },
                    {
                        "wall_ts_unix_us": 2_000_000,
                        "process_trace_id": "old",
                        "event": "block_announced",
                        "hash": BLOCK_HASH,
                        "transport": "legacy",
                    },
                    {
                        "wall_ts_unix_us": 2_001_000,
                        "process_trace_id": "new",
                        "event": "block_announced",
                        "hash": BLOCK_HASH,
                        "transport": "native_compat",
                    },
                ],
            )

            report = reporter.build_report({"observer": node}, BLOCK_HASH)

            self.assertIsNone(report["origin"])
            self.assertEqual(report["duplicates"][0]["count"], 2)
            self.assertIn("mining-node broadcast origin is missing", report["warnings"])
            self.assertTrue(
                any("lacks wall_ts_unix_us" in warning for warning in report["warnings"])
            )
            self.assertTrue(
                any("2 process instances" in warning for warning in report["warnings"])
            )

    def test_missing_origin_managed_edge_renders_without_crashing(self):
        with tempfile.TemporaryDirectory() as temp:
            node = Path(temp) / "observer"
            source_id = "22" * 32
            write_rows(
                node,
                "header_sync.jsonl",
                [
                    {
                        "wall_ts_unix_us": 2_000_000,
                        "event": "header_status_received",
                        "selected_tip_hash": BLOCK_HASH,
                        "peer": reporter.native_peer_label(source_id),
                    }
                ],
            )

            report = reporter.build_report(
                {"observer": node},
                BLOCK_HASH,
                native_node_ids={"source": source_id},
            )
            markdown = reporter.render_markdown(report)

            self.assertIn("unknown offset", markdown)

    def test_rejects_invalid_hash_and_non_object_rows(self):
        with tempfile.TemporaryDirectory() as temp:
            trace_dir = Path(temp)
            (trace_dir / "block_propagation.jsonl").write_text("null\n")

            with self.assertRaisesRegex(ValueError, "exactly 32 bytes"):
                reporter.build_report({"node": trace_dir}, "0x")

            rows, warnings = reporter.load_rows(trace_dir)
            self.assertEqual(rows, [])
            self.assertTrue(any("must be an object" in warning for warning in warnings))

    def test_skips_mislabeled_rows_and_surfaces_failures(self):
        with tempfile.TemporaryDirectory() as temp:
            trace_dir = Path(temp)
            write_rows(
                trace_dir,
                "block_propagation.jsonl",
                [
                    {
                        "node": "other-node",
                        "wall_ts_unix_us": 1_000_000,
                        "event": "block_announced",
                        "hash": BLOCK_HASH,
                    },
                    {
                        "node": "observer",
                        "wall_ts_unix_us": 1_001_000,
                        "event": "legacy_block_finished",
                        "hash": BLOCK_HASH,
                        "result": "error",
                        "reason": "verification failed",
                    },
                ],
            )

            report = reporter.build_report({"observer": trace_dir}, BLOCK_HASH)
            markdown = reporter.render_markdown(report)

            self.assertEqual(len(report["events"]), 1)
            self.assertTrue(any("different node" in warning for warning in report["warnings"]))
            self.assertTrue(any("reported error" in warning for warning in report["warnings"]))
            self.assertIn("verification failed", markdown)

    def test_exposed_legacy_labels_resolve_to_nodes(self):
        source = "legacy:203.0.113.7:18233"
        resolved = reporter.source_node(
            source,
            {},
            {
                "203.0.113.7": "source",
            },
        )

        self.assertEqual(resolved, "source")

    def test_classifies_broadcast_drops_and_expected_duplicates(self):
        with tempfile.TemporaryDirectory() as temp:
            trace_dir = Path(temp)
            write_rows(
                trace_dir,
                "block_propagation.jsonl",
                [
                    {
                        "wall_ts_unix_us": 1_000_000,
                        "event": "mined_block_broadcast_finished",
                        "hash": BLOCK_HASH,
                        "result": "error",
                    },
                    {
                        "wall_ts_unix_us": 1_001_000,
                        "event": "block_announced",
                        "hash": BLOCK_HASH,
                        "transport": "legacy",
                        "disposition": "queue_full",
                    },
                ],
            )
            write_rows(
                trace_dir,
                "commit_state.jsonl",
                [
                    {
                        "wall_ts_unix_us": 1_002_000,
                        "event": "commit_finish",
                        "hash": BLOCK_HASH,
                        "result": "duplicate",
                    }
                ],
            )

            report = reporter.build_report({"observer": trace_dir}, BLOCK_HASH)

            self.assertEqual(report["nodes"]["observer"]["commit"]["result"], "duplicate")
            self.assertTrue(any("broadcast" in warning for warning in report["warnings"]))
            self.assertTrue(any("queue_full" in warning for warning in report["warnings"]))
            self.assertFalse(any("commit_finish" in warning for warning in report["warnings"]))

    def test_rejects_non_32_byte_native_node_id(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaisesRegex(ValueError, "exactly 32 bytes"):
                reporter.build_report(
                    {"observer": Path(temp)},
                    BLOCK_HASH,
                    native_node_ids={"observer": "11"},
                )

    def test_main_writes_json_and_markdown_outputs(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            trace_dir = root / "observer"
            write_rows(
                trace_dir,
                "block_propagation.jsonl",
                [
                    {
                        "wall_ts_unix_us": 1_000_000,
                        "event": "block_announced",
                        "hash": BLOCK_HASH,
                        "height": 42,
                        "transport": "legacy",
                    }
                ],
            )
            json_out = root / "report.json"
            markdown_out = root / "report.md"

            result = reporter.main(
                [
                    "--hash",
                    BLOCK_HASH,
                    "--origin",
                    "first-discovery",
                    "--trace-dir",
                    f"observer={trace_dir}",
                    "--json-out",
                    str(json_out),
                    "--markdown-out",
                    str(markdown_out),
                ]
            )

            self.assertEqual(result, 0)
            output = json.loads(json_out.read_text())
            self.assertEqual(output["block_hash"], BLOCK_HASH)
            self.assertEqual(output["origin_kind"], "first_discovery")
            self.assertIn("Block propagation report", markdown_out.read_text())


if __name__ == "__main__":
    unittest.main()
