import hashlib
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "fuzz-campaign.py"
SPEC = importlib.util.spec_from_file_location("fuzz_campaign", SCRIPT)
assert SPEC and SPEC.loader
fuzz_campaign = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = fuzz_campaign
SPEC.loader.exec_module(fuzz_campaign)

HEAD = "a" * 40
SUITE_TEXT = b"""
[[target]]
package = "sim"
name = "discovery_wait_complete"
smoke-secs = 60
timeout-secs = 1800

[[target]]
package = "header-chain"
name = "recovery_rows"
smoke-secs = 60
args = ["-max_len=256"]
env = ["ASAN_OPTIONS=detect_leaks=0"]
timeout-secs = 2700
"""


def campaign():
    """A campaign of the two targets above, in two shards."""
    return {
        "schema": 2,
        "name": "test",
        "cpus": 4,
        "integration": {"base": "b" * 40},
        "toolchain": {"fuzz": "nightly-2026-07-15"},
        "formats": {"trace": 21, "contract": 1},
        "segment": [{"name": "pilot", "budget-secs": 3600}],
        "target": [
            {"id": "target/sim/discovery_wait_complete", "shard": "sim", "workers": 1,
             "input-timeout-secs": 60},
            {"id": "target/header-chain/recovery_rows", "shard": "header-chain", "workers": 2,
             "input-timeout-secs": 300},
        ],
    }


def manifest():
    """`zakura-sim suite`'s output for `SUITE_TEXT` at `HEAD`, trimmed."""
    return {
        "build": {"git": HEAD, "dirty": False},
        "suite_sha256": hashlib.sha256(SUITE_TEXT).hexdigest(),
        "nightly": "nightly-2026-07-15",
        "format": 21,
        "contract": 1,
        "targets": [
            {"id": "target/sim/discovery_wait_complete", "harness": "discovery/wait-complete"},
            {"id": "target/header-chain/recovery_rows", "harness": None},
        ],
    }


def problems(campaign, manifest, ancestor=True):
    return fuzz_campaign.check(campaign, SUITE_TEXT, manifest, HEAD, lambda _: ancestor)


class CheckTest(unittest.TestCase):
    def test_a_matching_campaign_has_no_problem(self):
        self.assertEqual(problems(campaign(), manifest()), [])

    def test_a_target_the_suite_maps_must_run(self):
        bad = campaign()
        bad["target"].pop()
        self.assertEqual(
            problems(bad, manifest()),
            ["the suite manifest maps `target/header-chain/recovery_rows`, and manifest.toml doesn't run it"],
        )

    def test_a_target_the_suite_doesnt_map_is_refused(self):
        bad = campaign()
        bad["target"].append(
            {"id": "target/sim/other", "shard": "sim", "workers": 1, "input-timeout-secs": 60}
        )
        self.assertIn(
            "manifest.toml runs `target/sim/other`, which the suite manifest doesn't map",
            problems(bad, manifest()),
        )

    def test_every_segment_needs_a_budget_above_the_input_timeout(self):
        bad = campaign()
        bad["segment"].append({"name": "calibration", "budget-secs": 0})
        bad["segment"].append({"name": "short", "budget-secs": 120})
        self.assertEqual(
            problems(bad, manifest()),
            [
                "the segment `calibration` has no search budget",
                "`target/header-chain/recovery_rows`'s per-input timeout isn't below the `short` budget",
            ],
        )

    def test_the_suite_manifest_must_describe_the_checkout(self):
        stale = manifest()
        stale["build"] = {"git": "c" * 40, "dirty": True}
        stale["suite_sha256"] = "0" * 64
        self.assertEqual(
            problems(campaign(), stale, ancestor=False),
            [
                "the suite manifest comes from a dirty build; write it with `suite --frozen`",
                f"the suite manifest describes {'c' * 40}, not the checkout's {HEAD}",
                "the suite manifest read another qa/suite.toml",
                f"integration.base, {'b' * 40}, isn't an ancestor of {HEAD}",
            ],
        )

    def test_a_shard_fits_the_host(self):
        bad = campaign()
        bad["target"][1]["workers"] = 5
        self.assertEqual(problems(bad, manifest()), ["the shard `header-chain` has 5 workers on 4 vCPUs"])


class ScriptTest(unittest.TestCase):
    def script(self, shard, attempt=1):
        rows = fuzz_campaign.suite_targets(fuzz_campaign.tomllib.loads(SUITE_TEXT.decode()), manifest())
        with tempfile.TemporaryDirectory() as dir:
            suite = Path(dir) / "suite.json"
            suite.write_text(json.dumps(manifest()))
            return fuzz_campaign.segment_script(
                campaign(), rows, suite, "pilot", shard, attempt, set(), Path("/c"), HEAD, Path("/r")
            )

    def test_each_run_has_a_total_budget_and_a_per_input_timeout(self):
        script = self.script("header-chain")
        self.assertIn(
            "env ASAN_OPTIONS=detect_leaks=0 timeout --kill-after=300 4200 cargo +nightly-2026-07-15 fuzz run "
            "--fuzz-dir /r/qa/fuzz/header-chain recovery_rows /c/corpus/header-chain/recovery_rows -- "
            "-max_total_time=3600 -timeout=300 -jobs=2 -workers=2 -max_len=256 "
            "-artifact_prefix=/c/segments/pilot/header-chain/header-chain/recovery_rows/artifacts/",
            script,
        )
        self.assertIn("mkdir /c/segments/pilot/header-chain\n", script)
        self.assertNotIn("discovery_wait_complete", script)

    def test_a_sim_target_starts_from_its_seeds(self):
        script = self.script("sim")
        self.assertIn(
            "test -d /c/corpus/sim/discovery_wait_complete || { mkdir -p /c/corpus/sim/discovery_wait_complete "
            "&& cargo xtask sim-seeds --sim discovery/wait-complete --out /c/corpus/sim/discovery_wait_complete; }",
            script,
        )
        self.assertIn("'RUSTFLAGS=--cfg zakura_sim' "
                      "ZAKURA_SIM_ARTIFACTS=/c/segments/pilot/sim/sim/discovery_wait_complete/sim", script)

    def test_a_resume_needs_its_parent_and_writes_its_own_directory(self):
        script = self.script("sim", attempt=2)
        self.assertIn("test -d /c/segments/pilot/sim ||", script)
        self.assertIn("mkdir /c/segments/pilot.2/sim\n", script)
        self.assertIn('"parent": "pilot"', script)


if __name__ == "__main__":
    unittest.main()
