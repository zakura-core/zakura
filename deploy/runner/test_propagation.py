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
                "path": "legacy", "clock": "trace", "from": "us-0", "downloads": [],
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


# An address that must never be shown, and the forms a node may report it in.
PRIVATE_IP = "198.51.100.77"
FLEET_ADDRESSES = {"192.0.2.10": "us-0", "192.0.2.11": "us-east-0"}


def download(addr, ms=40, agent="/Zakura:1.8.7/", result="available", **fields):
    return {"stack": "legacy", "addr": addr, "agent": agent, "ms": ms, "result": result, "route": "maybe", **fields}


class DownloadTests(unittest.TestCase):
    def join(self, *downloads_per_block):
        """us-east-0 reports one block per list of downloads, newest block first."""
        records = [
            record(block_hash=f"{index:064x}", height=100 - index, wall={"heard": 0.0, "tip": 0.1}, downloads=downloads)
            for index, downloads in enumerate(downloads_per_block)
        ]
        return propagation.join(
            {"us-east-0": report(*records)}, NODE_IDS, now=START + 60, fleet_addresses=FLEET_ADDRESSES,
            is_private=lambda ip: ip == PRIVATE_IP,
        )  # fmt: skip

    def test_a_download_names_the_outside_peer_by_address_and_software(self):
        joined = self.join([download("203.0.113.5", ms=120)])
        (served,) = joined["blocks"][0]["nodes"]["us-east-0"]["downloads"]
        self.assertEqual(
            served,
            {"kind": "outside", "ip": "203.0.113.5", "agent": "/Zakura:1.8.7/", "stack": "legacy", "ms": 120,
             "bytes": None, "result": "available", "route": "maybe"},
        )  # fmt: skip

    def test_a_fleet_node_is_named_by_address_or_by_zakura_label(self):
        by_address = download("192.0.2.10")
        by_label = {**download(None), "stack": "zakura", "peer": US_WEST_LABEL}
        downloads = self.join([by_address, by_label])["blocks"][0]["nodes"]["us-east-0"]["downloads"]
        self.assertEqual([(d["kind"], d["name"]) for d in downloads], [("fleet", "us-0"), ("fleet", "us-west-0")])
        self.assertEqual(self.join([by_address, by_label])["outside_peers"], [])

    def test_a_peer_whose_address_the_node_hid_has_no_address(self):
        (served,) = self.join([download(None, agent="/MagicBean:6.2.0/")])["blocks"][0]["nodes"]["us-east-0"]["downloads"]
        self.assertEqual((served["kind"], served["agent"]), ("hidden", "/MagicBean:6.2.0/"))
        self.assertNotIn("ip", served)
        # Text that is not an address is never shown as one.
        (odd,) = self.join([download("not an address")])["blocks"][0]["nodes"]["us-east-0"]["downloads"]
        self.assertEqual(odd["kind"], "hidden")

    def test_a_private_address_is_never_returned_in_any_form(self):
        forms = [
            download(PRIVATE_IP, ms=900, agent="/Zakura:9.9.9-private/"),
            download("::ffff:" + PRIVATE_IP, ms=800, agent="/Zakura:9.9.9-private/"),
            download(PRIVATE_IP + ":8233", ms=750, agent="/Zakura:9.9.9-private/"),
            download("[::ffff:" + PRIVATE_IP + "]:8233", ms=740, agent="/Zakura:9.9.9-private/"),
            {**download(PRIVATE_IP, ms=700, agent="/Zakura:9.9.9-private/"), "stack": "zakura", "peer": "peer:feedfeedfeedfeed"},
        ]
        joined = self.join(forms)
        downloads = joined["blocks"][0]["nodes"]["us-east-0"]["downloads"]
        self.assertEqual([d["kind"] for d in downloads], ["private"] * 5)
        self.assertEqual(joined["outside_peers"], [])
        # Its downloads still count as slow downloads, as a private peer.
        self.assertEqual([d["ms"] for d in joined["slowest_downloads"]], [900, 800, 750, 740, 700])
        served = repr(joined)
        for secret in (PRIVATE_IP, "9.9.9-private", "feedfeed"):
            self.assertNotIn(secret, served)

    def test_the_first_outside_announcer_is_filtered_like_a_download(self):
        def first_heard_from(addr):
            peer = {"addr": addr, "agent": "/MagicBean:6.2.0/"}
            joined = propagation.join(
                {"us-east-0": report(record(wall={"heard": 0.0, "tip": 0.1}, heard_from_peer=peer))}, NODE_IDS,
                now=START + 60, fleet_addresses=FLEET_ADDRESSES, is_private=lambda ip: ip == PRIVATE_IP,
            )  # fmt: skip
            return joined["blocks"][0]["nodes"]["us-east-0"]["from_peer"], joined

        outside, _ = first_heard_from("203.0.113.5")
        self.assertEqual(outside, {"kind": "outside", "ip": "203.0.113.5", "agent": "/MagicBean:6.2.0/"})
        private, joined = first_heard_from(PRIVATE_IP)
        self.assertEqual(private, {"kind": "private"})
        self.assertNotIn(PRIVATE_IP, repr(joined))
        self.assertNotIn("6.2.0", repr(joined))

    def test_a_private_address_wins_over_a_fleet_name(self):
        joined = propagation.join(
            {"us-east-0": report(record(wall={"tip": 0.1}, downloads=[download(PRIVATE_IP)]))}, NODE_IDS,
            now=START + 60, fleet_addresses={PRIVATE_IP: "us-0"}, is_private=lambda ip: ip == PRIVATE_IP,
        )  # fmt: skip
        self.assertEqual(joined["blocks"][0]["nodes"]["us-east-0"]["downloads"][0], {
            "kind": "private", "stack": "legacy", "ms": 40, "bytes": None, "result": "available", "route": "maybe",
        })  # fmt: skip

    def test_outside_peers_are_listed_by_address_slowest_first(self):
        slow, fast = "203.0.113.5", "203.0.113.6"
        joined = self.join(
            [download(slow, ms=300, agent="/Zakura:1.8.7/"), download(fast, ms=20)],
            [download(slow, ms=100, agent="/Zakura:1.8.6/"), download(slow, ms=5000, result="error")],
            [download("203.0.113.7", ms=30, result="notfound_response", agent=None)],
        )
        peers = joined["outside_peers"]
        self.assertEqual([peer["ip"] for peer in peers], [slow, fast, "203.0.113.7"])
        self.assertEqual(
            peers[0],
            {"ip": slow, "agent": "/Zakura:1.8.7/", "nodes": ["us-east-0"], "downloads": 3, "failed": 1,
             "median_ms": 300, "p90_ms": 300, "max_ms": 300, "last": START},
        )  # fmt: skip
        # A peer that never delivered a block has attempts but no time.
        self.assertEqual((peers[2]["downloads"], peers[2]["failed"], peers[2]["median_ms"]), (1, 1, None))

    def test_the_slowest_downloads_are_single_attempts_with_their_block(self):
        joined = self.join([download("203.0.113.5", ms=300)], [download("192.0.2.10", ms=900), download(None, ms=None)])
        slowest = joined["slowest_downloads"]
        # `node` is the one that downloaded; a fleet peer that served it is under `name`.
        self.assertEqual([(d["ms"], d["height"], d["node"], d["kind"]) for d in slowest],
                         [(900, 99, "us-east-0", "fleet"), (300, 100, "us-east-0", "outside")])  # fmt: skip
        self.assertEqual(slowest[0]["name"], "us-0")
        self.assertEqual(slowest[1]["hash"], f"{0:064x}")

    def test_a_slow_download_says_how_large_the_block_was_when_any_node_recorded_it(self):
        reports = {
            # The node that downloaded did not record the size; another node did.
            "us-east-0": report(record(wall={"tip": 0.2}, downloads=[download("203.0.113.5", ms=300)])),
            "us-0": report(record(wall={"tip": 0.1}, size=1_500_000)),
        }
        joined = propagation.join(reports, NODE_IDS, now=START + 60)
        self.assertEqual(joined["blocks"][0]["size"], 1_500_000)
        self.assertEqual(joined["slowest_downloads"][0]["size"], 1_500_000)
        unknown = propagation.join({"us-east-0": reports["us-east-0"]}, NODE_IDS, now=START + 60)
        self.assertIsNone(unknown["slowest_downloads"][0]["size"])


if __name__ == "__main__":
    unittest.main()
