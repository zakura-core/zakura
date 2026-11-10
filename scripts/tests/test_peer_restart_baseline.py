"""Exercise restart boundaries without running nodes or waiting for wall time."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from scripts import peer_restart_baseline as baseline


class BaselineTests(unittest.TestCase):
    def test_reference_stays_running_and_measurements_span_restart(self):
        clock = [0.0]

        def sample():
            height = 100 if clock[0] == 0 else 101
            return {"getblockchaininfo": {"blocks": height, "headers": height,
                    "estimatedheight": height, "bestblockhash": str(height)}}

        nodes = [Mock(), Mock()]
        for node in nodes:
            node.process.poll.return_value = None
            node.sample.side_effect = sample
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch.object(baseline.time, "monotonic", side_effect=lambda: clock[0]), \
                 patch.object(baseline.time, "sleep", side_effect=lambda delta: clock.__setitem__(0, clock[0] + delta)), \
                 patch.object(baseline, "progress"):
                baseline.measure(nodes, root, root, 10, 10, 100)
            nodes[0].stop.assert_called_once()
            nodes[0].start.assert_called_once()
            nodes[1].stop.assert_not_called()
            nodes[1].start.assert_not_called()
            phases = {json.loads(line)["phase"] for line in (root / "samples.jsonl").read_text().splitlines()}
            self.assertEqual(phases, {"catchup", "before_restart", "after_restart"})
            self.assertTrue((root / "completed.json").is_file())

    def test_matching_stale_nodes_do_not_count_as_tip(self):
        info = {"blocks": 100, "headers": 100, "estimatedheight": 200, "bestblockhash": "same"}
        self.assertFalse(baseline.near_tip([{"getblockchaininfo": info}] * 2))
        self.assertFalse(baseline.near_tip([{}, {}]))


if __name__ == "__main__":
    unittest.main()
