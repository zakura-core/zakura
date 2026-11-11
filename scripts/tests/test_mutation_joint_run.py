"""The joint run's report diff: new survivors, new baseline failures and new property findings."""

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "mutation_joint_run.py"
SPEC = importlib.util.spec_from_file_location("mutation_joint_run", SCRIPT)
joint = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(joint)


def case(patch, witness, generator=(), baseline=True):
    return {
        "patch": patch,
        "property": "block-sync/retained-byte-bound",
        "result": {"class": witness},
        "baseline": {"passed": baseline, "detail": None if baseline else "a run violates it"},
        "generator": [
            {"harness": "block-sync/storage", "seed": seed, "result": {"class": kind}}
            for seed, kind in generator
        ],
    }


def finding(seed, prop):
    return {"harness": "discovery/wire", "seed": seed, "mode": "inproc", "property": prop, "detail": "stopped"}


class DiffTests(unittest.TestCase):
    def test_only_changes_toward_survival_or_failure_are_listed(self):
        previous = {
            "pinned": "a" * 40,
            "cases": [
                case("one.patch", "killed-by-named-rule", [(11, "killed-by-named-rule")]),
                case("two.patch", "survived", [(11, "survived")]),
            ],
            "property_findings": [finding(11, "discovery/x")],
        }
        new = {
            "pinned": "b" * 40,
            "cases": [
                case("one.patch", "survived", [(11, "killed-by-named-rule"), (23, "survived")]),
                case("two.patch", "survived", [(11, "survived")], baseline=False),
                case("three.patch", "killed-by-named-rule"),
            ],
            "property_findings": [finding(11, "discovery/x"), finding(23, "discovery/y")],
        }
        changes = joint.diff(previous, new)
        self.assertEqual(
            [entry["lane"][3:] for entry in changes["newly_surviving"]],
            [["generator", "block-sync/storage", 23], ["witness"]],
        )
        self.assertEqual(changes["newly_surviving"][1]["was"], "killed-by-named-rule")
        self.assertEqual([entry["case"][0] for entry in changes["newly_failing_baselines"]], ["two.patch"])
        self.assertEqual([entry["seed"] for entry in changes["new_property_findings"]], [23])
        self.assertTrue(joint.has_news(changes))

    def test_a_schema_1_report_diffs_and_an_unchanged_run_has_no_news(self):
        previous = {"pinned": "a" * 40, "cases": [case("one.patch", "survived")]}
        self.assertFalse(joint.has_news(joint.diff(previous, previous)))
        with tempfile.TemporaryDirectory() as temp:
            old = Path(temp) / "old.json"
            new_dir = Path(temp) / "new"
            new_dir.mkdir()
            old.write_text(json.dumps(previous))
            (new_dir / "report.json").write_text(json.dumps({**previous, "pinned": "c" * 40}))
            self.assertEqual(joint.main(["diff", str(old), str(new_dir / "report.json")]), 0)
            written = json.loads((new_dir / joint.DIFF_FILE).read_text())
            self.assertEqual(written["previous_pinned"], "a" * 40)

    def test_the_loop_runs_on_what_changed_since_the_previous_pin(self):
        command = joint.loop_command({"pinned": "a" * 40}, Path("out"), 500, ["--jobs", "8"])
        self.assertEqual(
            command,
            ["cargo", "xtask", "planted", "loop", "--out", "out", "--generate-runs", "500",
             "--changed-since", "a" * 40, "--jobs", "8"],
        )


if __name__ == "__main__":
    unittest.main()
