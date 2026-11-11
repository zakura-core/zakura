"""Regression checks for false readiness claims in the static inventory."""

import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "mutation_inventory.py"
SPEC = importlib.util.spec_from_file_location("mutation_inventory", SCRIPT)
inventory = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(inventory)


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

    def test_intact_mapping_passes(self):
        row = inventory.inventory(self.root)["cases"][0]
        self.assertEqual(row["errors"], [])
        self.assertEqual(row["apply"], "passed")

    def test_selector_outside_the_source_module_fails(self):
        # `--exact` selects zero tests when the module path is wrong.
        self.write("src/service/tests.rs", "#[test]\nfn selected() {}\n")
        self.case.update(source="src/service/tests.rs", test="other::tests::selected")
        self.write_cases([self.case])
        errors = inventory.inventory(self.root)["cases"][0]["errors"]
        self.assertEqual(errors, ["selector other::tests::selected lies outside module service::tests"])

    def test_integration_selector_needs_its_target_file(self):
        self.write("tests/props.rs", "#[test]\nfn selected() {}\n")
        self.case.update(source="tests/props.rs", test_target="other")
        self.write_cases([self.case])
        errors = inventory.inventory(self.root)["cases"][0]["errors"]
        self.assertEqual(errors, ["tests/props.rs is not the integration test file for other"])

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

    def test_witness_inventory_selector_outside_its_module_fails(self):
        self.write("src/service/tests.rs", "#[test]\nfn selected() {}\n")
        rows = [{"id": "own-1.example", "tests": [
            {"source": "src/service/tests.rs", "selector": "service::tests::selected"},
            {"source": "src/service/tests.rs", "selector": "service::selected"},
        ]}]
        self.write(inventory.WITNESSES, json.dumps({"rows": rows}))
        self.assertEqual(inventory.inventory(self.root)["errors"], [
            f"{inventory.WITNESSES} own-1.example: selector service::selected lies outside module service::tests",
        ])

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

    def test_typed_case_checks_its_files_and_has_no_target(self):
        self.write("qa/planted/right/example.patch", "unused")
        self.write("qa/planted/right/example.toml", '[typed]\nharness = "harness"\nscenario = "s.json"\nplan = "p.bin"\n')
        self.write("s.json", "{}")
        row = inventory.planted_binding(self.root, self.root / "qa/planted/right/example.patch")
        self.assertEqual(row["errors"], ["missing typed plan: p.bin"])
        self.assertEqual(row["harness"], "harness")
        self.assertEqual(row["fuzz_targets"], [])
        self.assertEqual(row["scenario_sha256"], inventory.sha256(self.root / "s.json"))
        self.write("p.bin", "plan")
        row = inventory.planted_binding(self.root, self.root / "qa/planted/right/example.patch")
        self.assertEqual(row["errors"], [])

    def test_case_with_both_forms_is_refused(self):
        self.write("qa/planted/right/example.patch", "unused")
        self.write("qa/planted/right/example.toml", 'input = "input.bin"\n[typed]\nharness = "h"\nscenario = "s"\nplan = "p"\n')
        with self.assertRaises(ValueError):
            inventory.planted_binding(self.root, self.root / "qa/planted/right/example.patch")


WORK_LIST = """schema = 1

[[entry]]
id = "bounds"
kind = "shared-definition"
claim = "c"
check = []
shared = []

[[entry.instance]]
property = "fixture/bound"
code = []
witness = { planted = "qa/planted/fixture/bound/case.toml" }

[[entry]]
id = "wire"
kind = "shared-wire-path"
claim = "c"
check = []
shared = []
tables = [
    { table = "one/s1", witness = { test = "selected", test_target = "props" } },
    { table = "two/s2", witness = { test = "selected", test_target = "props" } },
]
"""

PATCH = "--- a/crates/prod/src/lib.rs\n+++ b/crates/prod/src/lib.rs\n@@ -1 +1 @@\n-pub fn limit() -> u32 { 1 }\n+pub fn limit() -> u32 { 2 }\n"


class LoopTests(unittest.TestCase):
    """The mutation loop's generated cases (qa/native-mutations/PROMPT.md)."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.write("crates/prod/Cargo.toml", '[package]\nname = "prod"\n')
        self.write("crates/prod/src/lib.rs", "pub fn limit() -> u32 { 1 }\n")
        self.write("crates/zakura-sim/Cargo.toml", '[package]\nname = "zakura-sim"\n')
        self.write("crates/zakura-sim/src/lib.rs", "pub fn check() {}\n")
        self.write("crates/zakura-sim/tests/props.rs", "#[test]\nfn selected() {}\n")
        self.write("qa/planted/fixture/bound/case.toml", 'input = "input.bin"\n')
        self.write(inventory.WORK_LIST, WORK_LIST)
        self.write("qa/native-mutations/bounds/raise/raise.patch", PATCH)
        self.readme("Required failure: the run ends at `fixture/bound` with\nits bound passed.\n")
        self.case = {
            "patch": "bounds/raise/raise.patch", "entry": "bounds", "property": "fixture/bound",
            "witness": {"planted": "qa/planted/fixture/bound/case.toml"},
            "fault_class": "boundary-accounting",
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

    def readme(self, text):
        self.write("qa/native-mutations/bounds/raise/README.md", "# Raise\n\n" + text)

    def write_cases(self, cases):
        self.write("qa/native-mutations/cases.json", json.dumps(cases))

    def errors(self):
        report = inventory.inventory(self.root, loop_only=True)
        self.assertEqual(report["errors"], [])
        self.assertEqual(len(report["cases"]), 1)
        self.assertEqual(report["cases"][0]["apply"], "passed")
        return report["cases"][0]["errors"]

    def test_intact_case_passes(self):
        self.assertEqual(self.errors(), [])

    def test_a_simulator_or_test_file_is_refused(self):
        self.write("qa/native-mutations/bounds/raise/raise.patch", PATCH.replace("crates/prod/src/lib.rs", "crates/zakura-sim/src/lib.rs").replace("pub fn limit() -> u32 { 1 }", "pub fn check() {}", 1).replace("pub fn limit() -> u32 { 2 }", "pub fn check() { }"))
        self.assertEqual(self.errors(), ["changes a non-production file: crates/zakura-sim/src/lib.rs"])

    def test_readme_and_case_must_name_the_same_id(self):
        self.readme("Required failure: the run ends at `fixture/other`.\n")
        self.assertEqual(self.errors(), [
            "cases.json names ['fixture/bound'], the README ['fixture/other']",
        ])

    def test_a_readme_without_a_required_failure_fails(self):
        self.readme("It breaks the bound.\n")
        self.assertIn("the README names no required failure", self.errors())

    def test_a_case_names_exactly_one_witness(self):
        self.case["witness"]["input"] = "input.bin"
        self.write_cases([self.case])
        self.assertEqual(self.errors(), [
            "fixture/bound: a witness names exactly one of planted, input or test, not ['planted', 'input']",
        ])

    def test_a_test_witness_needs_its_function(self):
        self.case["witness"] = {"test": "absent", "test_target": "props"}
        self.write_cases([self.case])
        self.assertEqual(self.errors(), [
            "fixture/bound: test function not found in crates/zakura-sim/tests/props.rs",
        ])

    def test_the_entry_must_cover_the_property(self):
        self.readme("Required failure: the run ends at `fixture/elsewhere`.\n")
        self.case["property"] = "fixture/elsewhere"
        self.write_cases([self.case])
        self.assertEqual(self.errors(), ["fixture/elsewhere: entry bounds doesn't cover it"])

    def test_a_shared_path_patch_names_one_id_per_table(self):
        self.readme(
            "Required failure for `one/s1`: the run ends at `one/s1/x-closes`.\n"
            "Required failure for `two/s2`: the run ends at\n`two/s2/x-closes`.\n"
        )
        witness = {"test": "selected", "test_target": "props"}
        self.write_cases([
            {"patch": "bounds/raise/raise.patch", "entry": "wire", "table": "one/s1",
             "property": "one/s1/x-closes", "witness": witness},
            {"patch": "bounds/raise/raise.patch", "entry": "wire", "table": "two/s2",
             "property": "two/s2/x-closes", "witness": witness},
        ])
        self.assertEqual(self.errors(), [])

    def test_a_generated_directory_outside_the_tree_is_checked(self):
        generated = tempfile.TemporaryDirectory()
        self.addCleanup(generated.cleanup)
        out = Path(generated.name)
        (out / "bounds/raise").mkdir(parents=True)
        (out / "bounds/raise/raise.patch").write_text(PATCH)
        (out / "bounds/raise/README.md").write_text("Required failure: the run ends at `fixture/bound`.\n")
        (out / "cases.json").write_text(json.dumps([self.case]))
        report = inventory.inventory(self.root, cases_dir=out)
        self.assertEqual(report["errors"], [])
        self.assertEqual([row["errors"] for row in report["cases"]], [[]])
        self.assertEqual(report["cases"][0]["apply"], "passed")

    def generated(self, cases):
        generated = tempfile.TemporaryDirectory()
        self.addCleanup(generated.cleanup)
        out = Path(generated.name)
        (out / "bounds/raise").mkdir(parents=True)
        (out / "bounds/raise/raise.patch").write_text(PATCH)
        (out / "bounds/raise/README.md").write_text("Required failure: the run ends at `fixture/bound`.\n")
        (out / "cases.json").write_text(json.dumps(cases))
        return out

    def test_a_generated_patch_without_a_loop_mapping_fails(self):
        out = self.generated([])
        report = inventory.inventory(self.root, cases_dir=out)
        self.assertEqual(report["errors"], [f"patch has no loop mapping: {(out / 'bounds/raise/raise.patch').as_posix()}"])
        unmapped = {key: value for key, value in self.case.items() if key != "property"}
        out = self.generated([unmapped])
        report = inventory.inventory(self.root, cases_dir=out)
        patch = (out / "bounds/raise/raise.patch").as_posix()
        self.assertEqual(report["errors"], [
            f"patch has no loop mapping: {patch}", f"mapping names no property: {patch}",
        ])

    def test_a_simulator_only_module_of_a_production_crate_is_refused(self):
        self.write("crates/prod/src/lib.rs", "pub fn limit() -> u32 { 1 }\n\n#[cfg(zakura_sim)]\nmod sim_reader;\n#[cfg(not(zakura_sim))]\nmod real;\n")
        self.write("crates/prod/src/sim_reader.rs", "pub fn limit() -> u32 { 1 }\n")
        self.write("crates/prod/src/real.rs", "pub fn limit() -> u32 { 1 }\n")
        self.assertEqual(inventory.production_errors(self.root, [
            "crates/prod/src/lib.rs", "crates/prod/src/real.rs", "crates/prod/src/sim_reader.rs",
        ]), ["changes a non-production file: crates/prod/src/sim_reader.rs"])

    def test_release_cfg_evaluates_a_release_build(self):
        for predicate, holds in [
            ("test", False), ("zakura_sim", False), ("unix", True), ("not(zakura_sim)", True),
            ('any(test, feature = "proptest-impl")', False), ('feature = "indexer"', True),
            ('all(test, unix)', False), ('any(test, zakura_sim, feature = "zakura-testkit")', False),
            ('not(feature = "test-support")', True),
        ]:
            self.assertEqual(inventory.release_cfg(predicate), holds, predicate)


if __name__ == "__main__":
    unittest.main()
