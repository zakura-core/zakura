#!/usr/bin/env python3
"""Tests for the cluster view: peer labels, one node's report, and the join across nodes."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import propagation  # noqa: E402

# us-west-0's mainnet node id, and the label us-east-0's trace rows carry for it.
US_WEST_NODE_ID = "bd3dc5d2a3d44c6bf90e364bf446231dbf9737e38a562ccf9e91ea631ea59b22"
US_WEST_LABEL = "peer:9aa6fbe8ccba212e"
NODE_IDS = {"us-east-0": "", "us-west-0": US_WEST_NODE_ID, "us-0": ""}
BLOCK = "ab" * 32
START = 1_794_000_000.0


def record(block_hash=BLOCK, height=10, logged=None, **parts):
    """One block as a node's profiler reports it; `wall` times are seconds after START."""
    wall = {point: START + when for point, when in parts.pop("wall").items()}
    peers = {
        peer: {step: START + when for step, when in steps.items()}
        for peer, steps in parts.pop("peers", {}).items()
    }
    return {
        "hash": block_hash, "height": height, "path": "legacy", "reorg": False, "wall": wall, "peers": peers,
        "t": wall["tip"] + 0.0004 if logged is None else START + logged, **parts,
    }  # fmt: skip


def report(*records, clock=None):
    return {"node": "ignored", "clock": clock, "blocks": list(records)}


class LabelTests(unittest.TestCase):
    def test_a_node_id_gives_the_label_the_node_writes_for_that_peer(self):
        self.assertEqual(propagation.peer_label(US_WEST_NODE_ID), US_WEST_LABEL)

    def test_fleet_nodes_are_known_by_name_and_by_label(self):
        fleet = propagation.fleet_peers(NODE_IDS)
        self.assertEqual(fleet["name:us-0"], "us-0")
        self.assertEqual(fleet[US_WEST_LABEL], "us-west-0")
        # A node without a node id has a name only.
        self.assertEqual(sorted(name for name in fleet if name.startswith("peer:")), [US_WEST_LABEL])


class JoinTests(unittest.TestCase):
    def setUp(self):
        # us-0 hears of the block from outside, then tells us-east-0, which asks for it.
        self.reports = {
            "us-0": report(
                record(
                    wall={"heard": 0.0, "asked": 0.002, "body": 0.040, "queued": 0.042, "tip": 0.045, "told": 0.046},
                    heard_from=None,
                    peers={"name:us-east-0": {"told": 0.046, "asked": 0.060, "sent": 0.090}},
                ),
                clock={"offset_ms": 0.02, "rms_ms": 0.1},
            ),
            "us-east-0": report(
                record(
                    wall={"heard": 0.058, "asked": 0.059, "body": 0.095, "queued": 0.097, "tip": 0.1},
                    heard_from="name:us-0",
                    peers={"name:us-0": {"heard": 0.058}, "peer:ffffffffffffffff": {"told": 0.101}},
                )
            ),
            "us-west-0": None,
        }

    def join(self, **changes):
        return propagation.join({**self.reports, **changes}, NODE_IDS, now=START + 60)

    def test_a_block_is_placed_on_one_axis_from_its_first_sighting(self):
        (block,) = self.join()["blocks"]
        self.assertEqual((block["hash"], block["height"], block["origin"]), (BLOCK, 10, START))
        self.assertEqual((block["first_node"], block["first_from"]), ("us-0", "outside"))
        self.assertEqual(block["spread_ms"], 100.0)
        self.assertEqual(
            block["nodes"]["us-east-0"],
            {
                "path": "legacy", "clock": "trace", "from": "us-0",
                "at": {"heard": 58.0, "asked": 59.0, "body": 95.0, "queued": 97.0, "tip": 100.0},
            },
        )  # fmt: skip

    def test_a_hop_carries_the_sender_s_steps_and_the_receiver_s_hearing(self):
        (block,) = self.join()["blocks"]
        self.assertEqual(
            block["hops"],
            [{"from": "us-0", "to": "us-east-0", "told": 46.0, "asked": 60.0, "sent": 90.0, "heard": 58.0}],
        )

    def test_a_peer_outside_the_fleet_is_not_a_hop(self):
        (block,) = self.join()["blocks"]
        self.assertEqual({hop["to"] for hop in block["hops"]}, {"us-east-0"})

    def test_a_zakura_peer_is_resolved_through_its_label(self):
        told = record(
            wall={"heard": 0.058, "tip": 0.1}, heard_from=US_WEST_LABEL, peers={US_WEST_LABEL: {"told": 0.102}}
        )
        (block,) = self.join(**{"us-east-0": report(told)})["blocks"]
        self.assertEqual(block["nodes"]["us-east-0"]["from"], "us-west-0")
        self.assertIn({"from": "us-east-0", "to": "us-west-0", "told": 102.0}, block["hops"])

    def test_a_node_whose_trace_clock_is_off_is_placed_by_its_log(self):
        # The trace says the tip was 3.277 s later than the node log does.
        late = record(wall={"heard": 3.335, "tip": 3.377}, logged=0.1004, heard_from="name:us-0")
        (block,) = self.join(**{"us-east-0": report(late)})["blocks"]
        view = block["nodes"]["us-east-0"]
        self.assertEqual(view["clock"], "log")
        self.assertEqual(view["at"], {"heard": 58.4, "tip": 100.4})
        self.assertEqual(block["nodes"]["us-0"]["clock"], "trace")

    def test_a_node_that_did_not_answer_is_listed_and_the_rest_still_join(self):
        joined = self.join()
        self.assertEqual(joined["unreachable"], ["us-west-0"])
        self.assertEqual(joined["nodes"], ["us-east-0", "us-west-0", "us-0"])
        self.assertEqual(joined["clocks"]["us-0"], {"offset_ms": 0.02, "rms_ms": 0.1})

    def test_blocks_come_newest_first_and_stop_at_the_limit(self):
        older = record(block_hash="cd" * 32, height=9, wall={"heard": -75.0, "tip": -74.9})
        reports = {"us-0": report(older, *self.reports["us-0"]["blocks"])}
        joined = propagation.join(reports, NODE_IDS, now=START + 60)
        self.assertEqual([block["height"] for block in joined["blocks"]], [10, 9])
        self.assertEqual(len(propagation.join(reports, NODE_IDS, limit=1)["blocks"]), 1)

    def test_the_summary_gives_each_node_s_intervals_and_each_pair_s_hop_time(self):
        summary = self.join()["summary"]
        east = summary["nodes"]["us-east-0"]
        self.assertEqual((east["blocks"], east["first"], east["tip_median_ms"]), (1, 0, 100.0))
        self.assertEqual(east["intervals"]["download"], {"median_ms": 36.0, "p90_ms": 36.0})
        self.assertEqual(summary["nodes"]["us-0"]["first"], 1)
        self.assertNotIn("us-west-0", summary["nodes"])
        self.assertEqual(summary["pairs"], [{"from": "us-0", "to": "us-east-0", "hops": 1, "median_ms": 12.0}])


if __name__ == "__main__":
    unittest.main()
