"""Tests for scripts/check-fuzz-campaign.py over a small generated checkout."""

import importlib.util
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "check-fuzz-campaign.py"
SPEC = importlib.util.spec_from_file_location("check_fuzz_campaign", SCRIPT)
CAMPAIGN = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CAMPAIGN)

CARGO = """
[package]
name = "p2p-fuzz"

[package.metadata.zakura-machine.targets]
alpha = { decoder = 2, properties = ["a/one"] }

[[bin]]
name = "alpha"
path = "fuzz_targets/alpha.rs"
"""
SUITE = """
[[target]]
package = "p2p"
name = "alpha"
args = ["-max_len=512"]
"""
WORKFLOW = """
on:
  schedule:
    - cron: "0 0 * * *"
  workflow_dispatch:
jobs:
  fuzz:
    strategy:
      matrix:
        include:
          - target: alpha
            decoder: 2
            max_len: 512

    steps: []
"""


class CampaignCheck(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.write("qa/suite.toml", SUITE)
        self.write("qa/fuzz/p2p/Cargo.toml", CARGO)
        self.write("qa/fuzz/p2p/fuzz_targets/alpha.rs", "fn main() {}\n")
        self.write(".github/workflows/p2p-fuzz.yml", WORKFLOW)
        self.write("crates/demo/src/lib.rs", 'const PROPERTY: &str = "a/one";\n')

    def write(self, path, text):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")

    def assert_problem(self, text):
        problems = CAMPAIGN.problems_at(self.root)
        self.assertTrue(any(text in problem for problem in problems), problems)

    def test_a_complete_checkout_has_no_problems(self):
        self.assertEqual(CAMPAIGN.problems_at(self.root), [])

    def test_a_target_without_a_suite_row_fails(self):
        self.write("qa/suite.toml", "")
        self.assert_problem("qa/suite.toml neither runs nor excludes `alpha`")

    def test_a_target_without_a_registration_fails(self):
        self.write("qa/fuzz/p2p/Cargo.toml", CARGO.replace("alpha = {", "beta = {"))
        self.assert_problem("`alpha` has no machine registration")

    def test_an_unknown_property_fails(self):
        self.write("crates/demo/src/lib.rs", "")
        self.assert_problem("names the unknown property `a/one`")

    def test_an_unscheduled_target_fails(self):
        self.write(".github/workflows/p2p-fuzz.yml", WORKFLOW.replace("alpha", "beta"))
        self.assert_problem("doesn't schedule `alpha`")
        self.assert_problem("schedules `beta`, which qa/suite.toml doesn't run")

    def test_a_campaign_on_pull_requests_fails(self):
        self.write(
            ".github/workflows/p2p-fuzz.yml",
            WORKFLOW.replace("  workflow_dispatch:", "  workflow_dispatch:\n  pull_request:"),
        )
        self.assert_problem("must not run on pull_request")

    def test_a_stale_matrix_field_fails(self):
        self.write(".github/workflows/p2p-fuzz.yml", WORKFLOW.replace("max_len: 512", "max_len: 64"))
        self.assert_problem("has max_len 64")
        self.write(".github/workflows/p2p-fuzz.yml", WORKFLOW.replace("decoder: 2", "decoder: 1"))
        self.assert_problem("has decoder 1")

    def test_a_coverage_row_without_a_declaration_fails(self):
        self.write(
            "crates/zakura-sim/src/harness/coverage.rs",
            'Coverage {\n    property: "b/two",\n}\n',
        )
        self.assert_problem("the row `b/two` has no property declaration")

    def test_a_pending_row_with_coverage_fails(self):
        self.write(
            "crates/zakura-sim/src/harness/coverage.rs",
            'Coverage {\n    property: "a/one",\n}\n',
        )
        self.write(
            "qa/suite.toml",
            SUITE + '\n[[pending]]\nid = "a/one"\nkind = "property"\n',
        )
        self.assert_problem("`a/one` is pending and has a coverage row")


if __name__ == "__main__":
    unittest.main()
