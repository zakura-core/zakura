"""Reject false readiness evidence and discovery-corpus contamination."""

import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

import check_mutation_pilot as pilot


class PilotEvidenceTests(unittest.TestCase):
    def setUp(self):
        source = Path(__file__).resolve().parents[1]
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.catalogue_path = self.root / "qa/mutation-pilot/catalogue.json"
        self.catalogue = json.loads((source / "qa/mutation-pilot/catalogue.json").read_text())
        shutil.copytree(source / "qa/mutation-pilot", self.root / "qa/mutation-pilot")
        paths = {"Cargo.lock", "qa/fuzz/sim/Cargo.toml"}
        for case in self.catalogue["cases"]:
            paths.update(case["production_files"])
            paths.update(case["oracle_and_generator_files"])
            paths.update([case["patch"], case["witness"], str(Path(case["witness"]).with_suffix(".toml"))])
            paths.add(str(Path(case["patch"]).with_suffix(".toml")))
        for path in paths:
            destination = self.root / path
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source / path, destination)
        # These tests exercise artifact validation; the fixture has no Git history.
        compared = patch.object(pilot.subprocess, "run")
        self.git_result = compared.start().return_value
        self.git_result.returncode = 0
        self.git_result.stdout = ""
        self.addCleanup(compared.stop)

    def save_catalogue(self):
        self.catalogue_path.write_text(json.dumps(self.catalogue))

    def test_intact_evidence_passes(self):
        self.assertEqual(pilot.validate(self.root), [])

    def test_zero_applicability_fails_even_with_matching_hashes(self):
        case = self.catalogue["cases"][0]
        path = self.root / case["baseline"]["directory"] / "trace.jsonl"
        trace = [json.loads(line) for line in path.read_text().splitlines()]
        trace[-1]["end"]["properties"][0]["outcome"]["held"]["applicable"] = 0
        path.write_text("\n".join(json.dumps(row) for row in trace) + "\n")
        case["baseline"]["trace_sha256"] = pilot.digest(path)
        case["baseline"]["applicable"] = 0
        self.save_catalogue()
        self.assertTrue(any("intended property did not apply" in error for error in pilot.validate(self.root)))

    def test_wrong_witness_id_fails_even_with_matching_hash(self):
        case = self.catalogue["cases"][0]
        path = self.root / case["baseline"]["directory"] / "witness.json"
        witness = json.loads(path.read_text())
        witness["property"] = "wrong/property"
        path.write_text(json.dumps(witness))
        case["baseline"]["witness_result_sha256"] = pilot.digest(path)
        self.save_catalogue()
        self.assertTrue(any("witness did not pass" in error for error in pilot.validate(self.root)))

    def test_another_property_failure_invalidates_baseline(self):
        case = self.catalogue["cases"][0]
        path = self.root / case["baseline"]["directory"] / "trace.jsonl"
        trace = [json.loads(line) for line in path.read_text().splitlines()]
        trace[-1]["end"]["properties"].append({"property": "other/property", "outcome": {"violated": {"witness": [1]}}})
        path.write_text("\n".join(json.dumps(row) for row in trace) + "\n")
        case["baseline"]["trace_sha256"] = pilot.digest(path)
        self.save_catalogue()
        self.assertTrue(any("another property failure" in error for error in pilot.validate(self.root)))

    def test_discovery_rejects_a_copied_sensitivity_input(self):
        case = self.catalogue["cases"][0]
        corpus = self.root / self.catalogue["discovery"]["corpus"]
        shutil.copyfile(self.root / case["witness"], corpus / "leaked.bin")
        self.assertIn("discovery corpus contains seed files", pilot.validate(self.root))

    def test_missing_discovery_directory_fails(self):
        self.catalogue["discovery"]["corpus"] = "qa/nonexistent-directory"
        self.save_catalogue()
        self.assertIn("discovery corpus directory is missing", pilot.validate(self.root))

    def test_untracked_build_inputs_fail(self):
        self.git_result.stdout = "crates/zakura-sim/src/untracked.rs\n"
        self.assertIn("untracked build inputs invalidate baseline source identity", pilot.validate(self.root))

    def test_oracle_drift_fails(self):
        path = next(iter(self.catalogue["cases"][0]["oracle_and_generator_files"]))
        with (self.root / path).open("a") as stream:
            stream.write("\n// changed oracle\n")
        self.assertIn(f"hash differs: {path}", pilot.validate(self.root))


if __name__ == "__main__":
    unittest.main()
