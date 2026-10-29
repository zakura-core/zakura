"""Embedded state bytes remain reviewable without executing repository code."""

import hashlib
import struct
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from release_review.assets import FRONTIER_PATH, SUBTREES_PATH, asset_text
from release_review.contracts import ReviewError
from release_review.engine import review
from test_release_review import (
    GitFixture,
    ScriptedProvider,
    candidate,
    completed,
    settings,
)


def frontier(height=12, trees=(b"sapling\0", b"orchard\0", b"sprout\0")):
    return struct.pack("<I", height) + b"".join(struct.pack("<I", len(tree)) + tree for tree in trees)


def subtrees(*, version=1, network=1, counts=(1, 1, 0), payload=None):
    if payload is None:
        payload = struct.pack("<HI", 0, 10) + bytes(range(32))
        payload += struct.pack("<HI", 0, 11) + bytes(reversed(range(32)))
    header = b"ZKVCTST1" + struct.pack("<HBIIII", version, network, 12, *counts)
    return header + hashlib.sha256(header + payload).digest() + payload


class AssetFormatTests(unittest.TestCase):
    def test_frontier_representation_preserves_each_tree_byte_and_length(self):
        for count in (3, 4):
            trees = tuple(bytes(range(63 + i)) for i in range(count))
            text = asset_text(FRONTIER_PATH, frontier(trees=trees))
            self.assertIn("height_u32_le=12", text)
            for pool, original in zip(("sapling", "orchard", "sprout", "ironwood"), trees):
                rows = [line.split("=", 1)[1] for line in text.splitlines() if line.startswith(pool + ".bytes[")]
                self.assertEqual(bytes.fromhex("".join(rows)), original)
                self.assertIn(f"{pool}.length_u32_le={len(original)}", text)
            self.assertEqual("ironwood=absent" in text, count == 3)

    def test_subtree_representation_preserves_every_record(self):
        text = asset_text(SUBTREES_PATH, subtrees())
        self.assertIn("network_u8=1", text)
        self.assertIn("last_checkpoint_u32_le=12", text)
        self.assertIn("sapling.count_u32_le=1", text)
        self.assertIn("orchard.count_u32_le=1", text)
        self.assertIn("sapling.record[0] index_u16_le=0 end_height_u32_le=10 root=" + bytes(range(32)).hex(), text)
        self.assertIn("orchard.record[0] index_u16_le=0 end_height_u32_le=11 root=" + bytes(reversed(range(32))).hex(), text)
        self.assertIn("not proven", text)

    def test_malformed_frontiers_fail_closed(self):
        for blob in (b"", frontier()[:-1], frontier() + b"extra", struct.pack("<II", 12, 0xffffffff)):
            with self.subTest(blob=blob), self.assertRaises(ReviewError):
                asset_text(FRONTIER_PATH, blob)

    def test_malformed_subtree_frames_fail_closed(self):
        corrupt = bytearray(subtrees())
        corrupt[-1] ^= 1
        for blob in (b"", subtrees()[:-1], subtrees() + b"extra", bytes(corrupt),
                     subtrees(version=2), subtrees(network=0), subtrees(counts=(65537, 0, 0)),
                     subtrees(counts=(0, 0, 0))):
            with self.subTest(length=len(blob)), self.assertRaises(ReviewError):
                asset_text(SUBTREES_PATH, blob)

    def test_unknown_binary_formats_are_not_assumed_reviewable(self):
        with self.assertRaises(ReviewError):
            asset_text("unknown.bin", subtrees())


class AssetRepositoryTests(GitFixture):
    def test_binary_changes_are_review_units_with_read_only_context(self):
        for path, blob in ((FRONTIER_PATH, frontier()), (SUBTREES_PATH, subtrees())):
            file = self.root / path
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_bytes(blob)
        self.commit("embedded state assets")
        self.repository.pin(self.git("rev-parse", "HEAD").strip(), "")
        units, coverage = self.repository.units(1000)
        self.assertTrue(all(entry["status"] == "pending" for entry in coverage))
        asset_units = [unit for unit in units if unit.path == SUBTREES_PATH]
        self.assertIn("end_height_u32_le=11", "".join(unit.patch for unit in asset_units))
        result = self.repository.read("head", SUBTREES_PATH, 1, 15)
        self.assertEqual(result["representation"], "structured binary")
        self.assertIn("sapling.record[0]", result["text"])
        report = self.metadata()
        provider = ScriptedProvider([completed()] * 10)
        review(self.repository, provider, settings(), report, lambda _: None)
        self.assertEqual(report["status"], "complete")
        self.assertTrue(all(entry["status"] == "reviewed" for entry in report["coverage"]))

    def test_malformed_asset_is_a_coverage_gap_not_a_clean_review(self):
        file = self.root / SUBTREES_PATH
        file.parent.mkdir(parents=True)
        file.write_bytes(subtrees()[:-1])
        self.commit("truncated embedded state")
        self.repository.pin(self.git("rev-parse", "HEAD").strip(), "")
        report = self.metadata()
        review(self.repository, ScriptedProvider([completed()] * 4), settings(), report, lambda _: None)
        self.assertEqual(report["status"], "incomplete")
        gap = next(entry for entry in report["coverage"] if entry["path"] == SUBTREES_PATH)
        self.assertEqual(gap["status"], "excluded")

    def test_asset_findings_cite_consuming_source_not_synthetic_binary_lines(self):
        file = self.root / FRONTIER_PATH
        file.parent.mkdir(parents=True)
        file.write_bytes(frontier())
        self.commit("embedded frontier")
        self.repository.pin(self.git("rev-parse", "HEAD").strip(), "")
        finding = candidate()
        finding["change_path"] = FRONTIER_PATH
        self.repository.validate_finding(finding, {FRONTIER_PATH})
        finding["reference"]["path"] = FRONTIER_PATH
        with self.assertRaisesRegex(ReviewError, "binary"):
            self.repository.validate_finding(finding, {FRONTIER_PATH})


if __name__ == "__main__":
    unittest.main()
