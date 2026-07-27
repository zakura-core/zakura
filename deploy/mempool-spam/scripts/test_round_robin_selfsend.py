#!/usr/bin/env python3

import importlib.util
import sys
import unittest
from pathlib import Path
from unittest import mock

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
SPEC = importlib.util.spec_from_file_location(
    "round_robin_selfsend", SCRIPT_DIR / "round_robin_selfsend.py"
)
ROUND_ROBIN = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ROUND_ROBIN)


class DurationDeadlineTests(unittest.TestCase):
    def test_remaining_timeout_is_capped_by_run_deadline(self):
        with mock.patch.object(ROUND_ROBIN.time, "monotonic", return_value=100):
            self.assertEqual(ROUND_ROBIN.remaining_timeout(105, 10), 5)
            with self.assertRaises(ROUND_ROBIN.RunDurationElapsed):
                ROUND_ROBIN.remaining_timeout(100, 10)

    def test_expired_matrix_deadline_skips_rpc_queries(self):
        nodes = [{"name": "node", "rpc_url": "http://node"}]
        with (
            mock.patch.object(ROUND_ROBIN.time, "monotonic", return_value=100),
            mock.patch.object(ROUND_ROBIN, "transaction_state") as transaction_state,
        ):
            seen = ROUND_ROBIN.first_seen_matrix(
                nodes, "txid", duration=30, run_deadline=100
            )

        self.assertEqual(seen, {})
        transaction_state.assert_not_called()

    def test_send_rpc_timeout_uses_remaining_run_duration(self):
        node = {"name": "node", "rpc_url": "http://node"}
        with (
            mock.patch.object(ROUND_ROBIN.time, "monotonic", return_value=100),
            mock.patch.object(
                ROUND_ROBIN, "inventory_pools", side_effect=[{"orchard": {}}, {}]
            ),
            mock.patch.object(ROUND_ROBIN, "tip_height", return_value=1),
            mock.patch.object(
                ROUND_ROBIN, "zecd_rpc", side_effect=["address", "txid"]
            ) as zecd_rpc,
        ):
            result = ROUND_ROBIN.send_with_retry(
                node, 0.0002, run_deadline=105
            )

        self.assertEqual(result[0], "txid")
        self.assertEqual(zecd_rpc.call_args_list[1].kwargs["timeout"], 5)


if __name__ == "__main__":
    unittest.main()
