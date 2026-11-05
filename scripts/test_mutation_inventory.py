"""Regression checks for false readiness claims in the static inventory."""

import json
from pathlib import Path
import subprocess
import tempfile
import unittest

import mutation_inventory as inventory


class InventoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.write("Cargo.toml", '[package]\nname = "fixture"\n')
        self.write("src/lib.rs", "#[test]\nfn selected() {}\n")
        self.write("qa/native-mutations/example.patch", "--- a/src/lib.rs\n+++ b/src/lib.rs\n@@ -1,2 +1,2 @@\n #[test]\n-fn selected() {}\n+fn selected() { panic!(); }\n")
        self.case = {
            "patch": "example.patch", "package": "fixture", "test": "selected",
            "source": "src/lib.rs", "required_failure": "intended assertion",
        }
        self.write_cases([self.case])
        for command in [
            ["git", "init", "-q"], ["git", "add", "."],
            ["git", "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "fixture"],
        ]:
            subprocess.run(command, cwd=self.root, check=True, capture_output=True)

    def write(self, path, text):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)

    def write_cases(self, cases):
        self.write("qa/native-mutations/cases.json", json.dumps(cases))

    def test_source_symbol_never_claims_executable_selection(self):
        row = inventory.inventory(self.root)["cases"][0]
        self.assertEqual(row["errors"], [])
        self.assertEqual(row["apply"], "passed")
        self.assertEqual(row["compilation"], "not-run")
        self.assertEqual(row["sensitivity"], "not-run")
        self.assertEqual(row["executable_selection"], "not-run; source symbol check only")

    def test_missing_mapping_keeps_the_patch_and_reports_failure(self):
        self.write_cases([])
        report = inventory.inventory(self.root)
        self.assertEqual(len(report["cases"]), 1)
        self.assertTrue(report["cases"][0]["errors"])

    def test_wrong_function_name_fails(self):
        self.case["test"] = "absent"
        self.write_cases([self.case])
        self.assertIn("test function not found", inventory.inventory(self.root)["cases"][0]["errors"][0])

    def test_stale_patch_and_dirty_worktree_are_reported(self):
        self.write("src/lib.rs", "#[test]\nfn replaced() {}\n")
        report = inventory.inventory(self.root)
        self.assertEqual(report["cases"][0]["apply"], "failed")
        self.assertTrue(report["cases"][0]["apply_error"])
        self.assertTrue(report["worktree_status"])

    def test_duplicate_and_orphan_mappings_fail(self):
        orphan = dict(self.case, patch="absent.patch")
        self.write_cases([self.case, self.case, orphan])
        errors = inventory.inventory(self.root)["errors"]
        self.assertEqual(len(errors), 2)

    def test_wrong_property_witness_fails(self):
        self.write("qa/planted/right/example.patch", "unused")
        self.write("qa/planted/right/example.toml", 'input = "input.bin"\n')
        self.write("input.bin", "input")
        digest = inventory.sha256(self.root / "input.bin")
        self.write("input.toml", f'harness = "harness"\ngenerator = 1\nsha256 = "{digest}"\n[[witness]]\nproperty = "wrong"\nexpect = "holds"\n')
        self.write("qa/fuzz/sim/Cargo.toml", '[package.metadata.zakura-sim.targets]\n[[bin]]\nname = "unrelated"\npath = "unrelated.rs"\n')
        row = inventory.planted_binding(self.root, self.root / "qa/planted/right/example.patch")
        self.assertEqual(row["errors"], ["sidecar has no holds witness for the intended property"])
        self.assertEqual(row["discovery_target"], "missing")


if __name__ == "__main__":
    unittest.main()
