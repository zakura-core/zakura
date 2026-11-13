"""The joint run's report diff: new survivors, new baseline failures and new property findings."""

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

SCRIPT = Path(__file__).resolve().parents[1] / "mutation_joint_run.py"
SPEC = importlib.util.spec_from_file_location("mutation_joint_run", SCRIPT)
joint = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(joint)


def case(patch, witness, generator=(), baseline=True, test="t", mode="inproc"):
    return {
        "patch": patch,
        "property": "block-sync/retained-byte-bound",
        "witness": {"test": test, "test_target": "block_table"},
        "result": {"class": witness},
        "baseline": {"passed": baseline, "detail": None if baseline else "a run violates it"},
        "generator": [
            {"harness": "block-sync/storage", "seed": seed, "mode": mode, "result": {"class": kind}}
            for seed, kind in generator
        ],
    }


def finding(seed, prop, detail="stopped", mode="inproc"):
    return {"harness": "discovery/wire", "seed": seed, "mode": mode, "property": prop, "detail": detail}


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
            [entry["lane"][4:] for entry in changes["newly_surviving"]],
            [["generator", "block-sync/storage", 23, "inproc"], ["witness"]],
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

    def test_lanes_differ_by_mode_and_cases_by_witness(self):
        previous = {"cases": [case("one.patch", "killed-by-named-rule", [(11, "survived")], test="a")]}
        new = {"cases": [
            case("one.patch", "killed-by-named-rule", [(11, "survived")], test="a"),
            case("one.patch", "killed-by-named-rule", [(11, "survived")], test="a", mode="libfuzzer"),
            case("one.patch", "survived", test="b"),
        ]}
        surviving = [entry["lane"] for entry in joint.diff(previous, new)["newly_surviving"]]
        self.assertEqual(len(surviving), 2, surviving)
        self.assertIn("libfuzzer", surviving[0] + surviving[1])
        self.assertTrue(any('"b"' in entry[3] and entry[4] == "witness" for entry in surviving))

    def test_findings_are_keyed_by_property_not_by_detail(self):
        previous = {"cases": [], "property_findings": [
            finding(11, "discovery/x", "run 7 at /old/path"),
            finding(23, None, "run 3 at /old/path"),
        ]}
        new = {"cases": [], "property_findings": [
            finding(11, "discovery/x", "run 9 at /new/path"),
            finding(23, None, "run 4 at /new/path"),
            finding(11, "discovery/x", mode="libfuzzer"),
        ]}
        found = joint.diff(previous, new)["new_property_findings"]
        self.assertEqual([entry["mode"] for entry in found], ["libfuzzer"])

    def test_the_loop_runs_on_what_changed_since_the_previous_pin(self):
        command = joint.loop_command({"pinned": "a" * 40}, Path("out"), 500, ["--jobs", "8"])
        self.assertEqual(
            command,
            ["cargo", "xtask", "planted", "loop", "--out", "out", "--generate-runs", "500",
             "--changed-since", "a" * 40, "--jobs", "8"],
        )
        self.assertNotIn("--changed-since", joint.loop_command(None, Path("out"), 500, []))

    def test_untriaged_lists_survivors_and_other_rule_kills_without_a_record(self):
        triaged = case("one.patch", "survived", [(11, "killed-by-other-rule"), (23, "killed-by-named-rule")])
        triaged["triage"] = {"stage": "observation", "reason": "why"}
        new = {"cases": [triaged, case("two.patch", "killed-by-named-rule", [(11, "survived")])]}
        found = joint.diff({}, new)["untriaged"]
        self.assertEqual(
            [(entry["lane"][0], entry["lane"][4:], entry["class"]) for entry in found],
            [
                ("one.patch", ["generator", "block-sync/storage", 11, "inproc"], "killed-by-other-rule"),
                ("two.patch", ["generator", "block-sync/storage", 11, "inproc"], "survived"),
            ],
        )
        self.assertFalse(joint.has_news({**joint.diff(new, new), "untriaged": found}))


UNIT = Path(__file__).resolve().parents[2] / "deploy" / "mutation-joint"


class RoutineTests(unittest.TestCase):
    def dry_run(self, state, *flags):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            status = joint.main(["routine", "--checkout", "/c", "--state", str(state), "--dry-run", *flags])
        self.assertEqual(status, 0)
        return json.loads(out.getvalue())

    def test_a_first_run_covers_every_case_and_skips_libfuzzer(self):
        with tempfile.TemporaryDirectory() as state:
            plan = self.dry_run(state, "--libfuzzer-seconds", "60", "--harness-runs", "block-sync/storage=20")
        self.assertIsNone(plan["latest"])
        self.assertEqual(
            [step["name"] for step in plan["steps"]], ["suite-manifest", "suite", "loop", "completeness"]
        )
        manifest, suite, loop, _ = plan["steps"]
        self.assertEqual(manifest["argv"][:3], ["cargo", "xtask", "sim-suite"])
        self.assertIn("--frozen", manifest["argv"])
        self.assertEqual(suite["env"], {"RUSTFLAGS": "--cfg zakura_sim"})
        self.assertIn("planted", suite["argv"])
        self.assertNotIn("--changed-since", loop["argv"])
        self.assertEqual(loop["argv"][loop["argv"].index("--generate-runs") + 1], "20000")
        self.assertEqual(
            loop["argv"][loop["argv"].index("--generate-harness-runs") + 1], "block-sync/storage=20"
        )

    def test_a_later_run_covers_what_changed_since_the_last_pin(self):
        with tempfile.TemporaryDirectory() as state:
            (Path(state) / joint.LATEST_FILE).write_text(json.dumps({"pinned": "a" * 40}))
            plan = self.dry_run(state, "--libfuzzer-seconds", "60", "--", "--jobs", "4")
        names = [step["name"] for step in plan["steps"]]
        self.assertEqual(names, ["suite-manifest", "suite", "loop", "libfuzzer", "completeness"])
        for step in plan["steps"][2:4]:
            self.assertEqual(step["argv"][step["argv"].index("--changed-since") + 1], "a" * 40)
            self.assertEqual(step["argv"][-2:], ["--jobs", "4"])
            self.assertEqual(step["cwd"], "/c")
        self.assertIn("libfuzzer", plan["steps"][3]["argv"])

    def test_a_fuzz_budget_adds_the_suite_fuzz_and_its_archive_before_completeness(self):
        with tempfile.TemporaryDirectory() as state:
            plan = self.dry_run(state, "--fuzz-seconds", "600", "--fuzz-jobs", "2")
            suite_only = self.dry_run(state, "--suite-fuzz")
        names = [step["name"] for step in plan["steps"]]
        self.assertEqual(names, ["suite-manifest", "suite", "loop", "suite-fuzz", "archive", "completeness"])
        steps = {step["name"]: step["argv"] for step in plan["steps"]}
        manifest = steps["suite-manifest"][steps["suite-manifest"].index("--out") + 1]
        fuzz, archive, completeness = steps["suite-fuzz"], steps["archive"], steps["completeness"]
        self.assertEqual(fuzz[2], "run")
        self.assertEqual(fuzz[fuzz.index("--manifest") + 1], manifest)
        self.assertEqual(fuzz[fuzz.index("--state") + 1], str(Path(state).resolve()))
        self.assertEqual(fuzz[fuzz.index("--seconds") + 1], "600")
        self.assertEqual(fuzz[fuzz.index("--jobs") + 1], "2")
        out = fuzz[fuzz.index("--out") + 1]
        self.assertEqual(archive[archive.index("--suite-fuzz") + 1], str(Path(out) / "suite-fuzz.json"))
        self.assertEqual(archive[archive.index("--archive") + 1], str(Path(state).resolve() / "archive"))
        self.assertEqual(completeness[completeness.index("--manifest") + 1], manifest)
        self.assertEqual(completeness[completeness.index("--suite-fuzz") + 1], str(Path(out) / "suite-fuzz.json"))
        fuzz = next(step["argv"] for step in suite_only["steps"] if step["name"] == "suite-fuzz")
        self.assertNotIn("--seconds", fuzz, "--suite-fuzz alone keeps each target's suite budget")

    def test_the_units_command_and_example_budget_parse(self):
        service = (UNIT / "zakura-mutation-joint.service").read_text()
        start = next(line for line in service.splitlines() if line.startswith("ExecStart="))
        env = dict(
            line.split("=", 1)
            for line in (UNIT / "zakura-mutation-joint.env.example").read_text().splitlines()
            if line and not line.startswith("#")
        )
        argv = start.removeprefix("ExecStart=").split()
        self.assertEqual(argv[1], "${ZAKURA_MUTATION_CHECKOUT}/scripts/mutation_joint_run.py")
        self.assertEqual(argv[2], "routine")
        self.assertEqual(argv[-1], "$ZAKURA_MUTATION_ARGS")
        with tempfile.TemporaryDirectory() as state:
            plan = self.dry_run(state, *env["ZAKURA_MUTATION_ARGS"].split())
        loop = next(step for step in plan["steps"] if step["name"] == "loop")["argv"]
        self.assertEqual(loop.count("--generate-harness-runs"), 4)
        self.assertEqual(loop[loop.index("--cases") + 1], "qa/native-mutations")
        self.assertEqual(loop[-2:], ["--jobs", "4"])


class CumulativeTests(unittest.TestCase):
    def routine(self, state, reports, libfuzzer_seconds="0"):
        """Run the routine once with each step writing `reports[name]` as its report, if it has one."""
        def run_step(step, run_dir):
            if step["name"] in reports:
                out = Path(run_dir) / step["name"]
                out.mkdir()
                (out / joint.REPORT_FILE).write_text(json.dumps(reports[step["name"]]))
            return 0

        # Each run pins a new commit, so its run directory is new.
        self.pins = getattr(self, "pins", 0) + 1
        with mock.patch.object(joint, "pin", return_value=f"{self.pins:x}" * 40), \
                mock.patch.object(joint, "run_step", side_effect=run_step), \
                contextlib.redirect_stdout(io.StringIO()):
            return joint.main(["routine", "--checkout", "/c", "--state", str(state),
                               "--libfuzzer-seconds", libfuzzer_seconds])

    def test_a_partial_report_is_diffed_against_every_case_so_far(self):
        one = case("one.patch", "survived")
        two = case("two.patch", "survived")
        with tempfile.TemporaryDirectory() as state:
            self.assertEqual(self.routine(state, {"loop": {"cases": [one, two]}}), 1, "first survivors are news")
            # A `--changed-since` report reruns only two.patch: one.patch's
            # survival is no news, and two.patch's isn't either.
            self.assertEqual(self.routine(state, {"loop": {"cases": [two]}}), 0)
            merged = json.loads((Path(state) / "cumulative.json").read_text())
            self.assertEqual([entry["patch"] for entry in merged["cases"]], ["one.patch", "two.patch"])
            latest = json.loads((Path(state) / joint.LATEST_FILE).read_text())
            self.assertEqual(latest["pinned"], "2" * 40)

    def test_a_libfuzzer_report_has_its_own_diff(self):
        killed = {"cases": [case("one.patch", "killed-by-named-rule")]}
        with tempfile.TemporaryDirectory() as state:
            self.assertEqual(self.routine(state, {"loop": killed}), 0)
            news = {"cases": [case("one.patch", "killed-by-named-rule", [(11, "survived")], mode="libfuzzer")]}
            self.assertEqual(self.routine(state, {"loop": killed, "libfuzzer": news}, "60"), 1)
            self.assertEqual(self.routine(state, {"loop": killed, "libfuzzer": news}, "60"), 0)
            self.assertTrue((Path(state) / "cumulative-libfuzzer.json").exists())

    def test_completeness_runs_last_and_reads_the_steps_and_findings_written_before_it(self):
        seen = {}

        def run_step(step, run_dir):
            if step["name"] == "loop":
                (Path(run_dir) / "loop").mkdir()
                (Path(run_dir) / "loop" / joint.REPORT_FILE).write_text(
                    json.dumps({"cases": [case("one.patch", "survived")]})
                )
            if step["name"] == "completeness":
                seen["routine"] = json.loads((Path(run_dir) / joint.ROUTINE_FILE).read_text())
                seen["findings"] = json.loads((Path(run_dir) / joint.FINDINGS_FILE).read_text())
            seen.setdefault("order", []).append(step["name"])
            return 0

        with tempfile.TemporaryDirectory() as state, \
                mock.patch.object(joint, "pin", return_value="a" * 40), \
                mock.patch.object(joint, "run_step", side_effect=run_step), \
                contextlib.redirect_stdout(io.StringIO()):
            status = joint.main(["routine", "--checkout", "/c", "--state", state, "--suite-fuzz"])
            run_dir = next((Path(state) / "runs").iterdir())
            record = json.loads((run_dir / joint.ROUTINE_FILE).read_text())
        self.assertEqual(status, 1, "the first survivor is news")
        self.assertEqual(seen["order"], ["suite-manifest", "suite", "loop", "suite-fuzz", "archive", "completeness"])
        self.assertNotIn("completeness", seen["routine"]["steps"])
        self.assertEqual(list(seen["routine"]["steps"]), seen["order"][:-1])
        self.assertEqual(len(seen["findings"]["loop"]["newly_surviving"]), 1)
        self.assertEqual(record["steps"]["completeness"], 0)

    def test_merge_keeps_every_property_finding(self):
        merged = joint.merge(
            {"cases": [], "property_findings": [finding(11, "a")]},
            {"pinned": "c" * 40, "cases": [], "property_findings": [finding(23, "b"), finding(11, "a")]},
        )
        self.assertEqual([entry["seed"] for entry in merged["property_findings"]], [11, 23])
        self.assertEqual(merged["pinned"], "c" * 40)


if __name__ == "__main__":
    unittest.main()
