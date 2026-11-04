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
[campaign.p2p]
seconds = 900
weekly-seconds = 3600
rss-mb = 2048

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
    uses: ./.github/workflows/fuzz-campaign.yml
    with:
      package: p2p
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

    def test_a_suite_target_without_a_binary_fails(self):
        self.write("qa/suite.toml", SUITE + '\n[[target]]\npackage = "p2p"\nname = "gone"\n')
        self.assert_problem("qa/fuzz/p2p has no target `gone`")
        self.write(
            "qa/suite.toml",
            SUITE + '\n[[excluded-target]]\npackage = "p2p"\nname = "gone"\nreason = "x"\n',
        )
        self.assert_problem("qa/fuzz/p2p has no target `gone`")

    def test_a_suite_target_of_a_missing_package_fails(self):
        self.write("qa/suite.toml", SUITE + '\n[[target]]\npackage = "removed"\nname = "beta"\n')
        self.assert_problem("`beta` names the missing package qa/fuzz/removed")

    def test_a_target_without_a_registration_fails(self):
        self.write("qa/fuzz/p2p/Cargo.toml", CARGO.replace("alpha = {", "beta = {"))
        self.assert_problem("`alpha` has no machine registration")

    def test_an_unknown_property_fails(self):
        self.write("crates/demo/src/lib.rs", "")
        self.assert_problem("names the unknown property `a/one`")

    def test_a_workflow_that_skips_the_shared_campaign_fails(self):
        self.write(".github/workflows/p2p-fuzz.yml", WORKFLOW.replace("uses: ./", "uses: ./other/"))
        self.assert_problem("doesn't run fuzz-campaign.yml")
        self.write(".github/workflows/p2p-fuzz.yml", WORKFLOW.replace("package: p2p", "package: sim"))
        self.assert_problem("doesn't run the package `p2p`")

    def test_a_package_without_a_campaign_table_fails(self):
        self.write("qa/suite.toml", SUITE.replace("[campaign.p2p]", "[campaign.sim]"))
        self.assert_problem("qa/fuzz/p2p has no [campaign] table")

    def test_a_campaign_on_pull_requests_fails(self):
        self.write(
            ".github/workflows/p2p-fuzz.yml",
            WORKFLOW.replace("  workflow_dispatch:", "  workflow_dispatch:\n  pull_request:"),
        )
        self.assert_problem("must not run on pull_request")

    def test_the_matrix_joins_the_suite_and_the_decoder(self):
        self.assertEqual(
            CAMPAIGN.campaign_matrix(self.root, "p2p"),
            {
                "include": [
                    {
                        "target": "alpha",
                        "decoder": "d2",
                        "harness": "",
                        "seconds": 900,
                        "weekly_seconds": 3600,
                        "rss_mb": 2048,
                        "max_len": 512,
                        "detect_leaks": 1,
                    }
                ]
            },
        )

    def test_a_target_overrides_its_package_budgets_and_limits(self):
        self.write(
            "qa/suite.toml",
            SUITE.replace(
                'args = ["-max_len=512"]',
                'campaign-seconds = 1800\ncampaign-weekly-seconds = 7200\n'
                'args = ["-rss_limit_mb=4096", "-max_len=256", "-detect_leaks=0"]',
            ),
        )
        row = CAMPAIGN.campaign_matrix(self.root, "p2p")["include"][0]
        self.assertEqual(
            (row["seconds"], row["weekly_seconds"], row["rss_mb"], row["max_len"], row["detect_leaks"]),
            (1800, 7200, 4096, 256, 0),
        )

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
