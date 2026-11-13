"""The joint run's report diff: new survivors, new baseline failures and new property findings."""

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sys
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


def baseline_failure(seed, reason="timeout", mode="inproc"):
    return {
        "harness": "block-sync/storage", "seed": seed, "mode": mode,
        "result": {"class": "harness-failure", "reason": reason}, "log": f"baseline/{seed}",
    }


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

    def test_a_new_generator_baseline_failure_is_news(self):
        previous = {"cases": [], "generator_baseline_failures": [baseline_failure(11)]}
        new = {"cases": [], "generator_baseline_failures": [
            baseline_failure(11, "oom"), baseline_failure(23), baseline_failure(11, mode="libfuzzer"),
        ]}
        changes = joint.diff(previous, new)
        self.assertEqual(
            [(entry["seed"], entry["mode"]) for entry in changes["new_generator_baseline_failures"]],
            [(23, "inproc"), (11, "libfuzzer")],
            "seed 11's inproc search was already blocked",
        )
        self.assertTrue(joint.has_news(changes))
        self.assertFalse(joint.has_news(joint.diff(previous, previous)))

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
        self.assertEqual([step["name"] for step in plan["steps"]], ["suite", "loop"])
        suite, loop = plan["steps"]
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
        self.assertEqual(names, ["suite", "loop", "libfuzzer"])
        for step in plan["steps"][1:]:
            self.assertEqual(step["argv"][step["argv"].index("--changed-since") + 1], "a" * 40)
            self.assertEqual(step["argv"][-2:], ["--jobs", "4"])
            self.assertEqual(step["cwd"], "/c")
        self.assertIn("libfuzzer", plan["steps"][2]["argv"])
        self.assertEqual(plan["cases"]["argv"], ["cargo", "xtask", "planted", "loop", "--list", "--jobs", "4"])

    def test_the_libfuzzer_lane_runs_on_what_changed_since_its_own_pin(self):
        with tempfile.TemporaryDirectory() as state:
            (Path(state) / joint.LATEST_FILE).write_text(json.dumps({"pinned": "a" * 40}))
            (Path(state) / joint.LIBFUZZER_LATEST_FILE).write_text(json.dumps({"pinned": "b" * 40}))
            plan = self.dry_run(state, "--libfuzzer-seconds", "60")
        since = {step["name"]: step["argv"][step["argv"].index("--changed-since") + 1] for step in plan["steps"][1:]}
        self.assertEqual(since, {"loop": "a" * 40, "libfuzzer": "b" * 40})

    def test_the_case_list_is_keyed_as_the_report_is(self):
        listed = [{"patch": "one.patch", "property": "p", "witness": {"test_target": "t", "test": "x"}}]
        step = {"name": "cases", "argv": [sys.executable, "-c", f"print({json.dumps(json.dumps(listed))})"],
                "env": {}, "cwd": "."}
        failing = {**step, "argv": [sys.executable, "-c", "raise SystemExit(2)"]}
        with tempfile.TemporaryDirectory() as run_dir:
            self.assertEqual(joint.current_cases(step, run_dir), {joint.case_key(listed[0])})
            self.assertIsNone(joint.current_cases(failing, run_dir))
        reported = {**listed[0], "witness": {"test": "x", "test_target": "t"}, "result": {"class": "survived"}}
        self.assertEqual(joint.case_key(reported), joint.case_key(listed[0]))

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
        loop = plan["steps"][1]["argv"]
        self.assertEqual(loop.count("--generate-harness-runs"), 4)
        self.assertEqual(loop[loop.index("--cases") + 1], "qa/native-mutations")
        self.assertEqual(loop[-2:], ["--jobs", "4"])


class CumulativeTests(unittest.TestCase):
    def routine(self, state, reports, libfuzzer_seconds="0", triage=None, failing=()):
        """Run the routine once with each step writing `reports[name]` as its report, if it has one.

        A `triage-<lane>` step calls `triage(argv)` if given, each step in
        `failing` exits 1, and the steps that ran are in `self.ran`.
        """
        self.ran = []

        def run_step(step, run_dir):
            self.ran.append(step)
            if step["name"] in reports:
                out = Path(run_dir) / step["name"]
                out.mkdir()
                (out / joint.REPORT_FILE).write_text(json.dumps(reports[step["name"]]))
            if step["name"].startswith("triage-") and triage:
                triage(step["argv"])
            return int(step["name"] in failing)

        # The checkout lists every case a report so far named, unless the
        # test sets `self.current`.
        self.seen = getattr(self, "seen", set())
        self.seen |= {joint.case_key(entry) for report in reports.values() for entry in report["cases"]}

        def current_cases(step, run_dir):
            self.listed = step
            return getattr(self, "current", None) or set(self.seen)

        # Each run pins a new commit, so its run directory is new.
        self.pins = getattr(self, "pins", 0) + 1
        with mock.patch.object(joint, "pin", return_value=f"{self.pins:x}" * 40), \
                mock.patch.object(joint, "run_step", side_effect=run_step), \
                mock.patch.object(joint, "current_cases", side_effect=current_cases), \
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

    def test_the_cumulative_report_takes_the_checkouts_triage_every_run(self):
        survivor = case("one.patch", "survived")
        with tempfile.TemporaryDirectory() as state:
            cumulative = Path(state) / "cumulative.json"
            self.routine(state, {"loop": {"cases": [survivor]}})
            triage = [step for step in self.ran if step["name"] == "triage-loop"]
            self.assertEqual(triage[0]["argv"], [
                "cargo", "xtask", "planted", "triage", "--report", str(cumulative),
            ])
            self.assertEqual(triage[0]["cwd"], str(Path("/c").resolve()))
            first = sorted(Path(state, "runs").iterdir())[0]
            self.assertEqual(len(json.loads((first / "loop" / joint.DIFF_FILE).read_text())["untriaged"]), 1)

            # A triage-only commit selects no case, so the loop writes no
            # report, and the routine still applies the new record to the
            # merged report before it lists the untriaged lanes.
            def record(argv):
                path = Path(argv[argv.index("--report") + 1])
                merged = json.loads(path.read_text())
                self.assertEqual([entry["patch"] for entry in merged["cases"]], ["one.patch"])
                merged["cases"][0]["triage"] = {"stage": "observation", "reason": "why"}
                path.write_text(json.dumps(merged))

            self.assertEqual(self.routine(state, {}, triage=record), 0)
            self.assertEqual([step["name"] for step in self.ran], ["suite", "loop", "triage-loop"])
            second = sorted(Path(state, "runs").iterdir())[1]
            changes = json.loads((second / "loop" / joint.DIFF_FILE).read_text())
            self.assertEqual(changes["untriaged"], [])
            self.assertEqual(json.loads(cumulative.read_text())["cases"][0]["triage"]["reason"], "why")

    def test_the_cumulative_report_drops_cases_the_checkout_no_longer_lists(self):
        one, two = case("one.patch", "survived"), case("two.patch", "survived")
        with tempfile.TemporaryDirectory() as state:
            self.routine(state, {"loop": {"cases": [one, two]}})
            self.assertEqual(self.listed["argv"][:5], ["cargo", "xtask", "planted", "loop", "--list"])
            # A witness change re-keys one.patch, and two.patch is gone.
            moved = case("one.patch", "survived", test="b")
            self.current = {joint.case_key(moved)}
            self.assertEqual(self.routine(state, {"loop": {"cases": [moved]}}), 1, "the re-keyed lane is new")
            merged = json.loads((Path(state) / "cumulative.json").read_text())
            self.assertEqual([joint.case_key(entry) for entry in merged["cases"]], [joint.case_key(moved)])
            second = sorted(Path(state, "runs").iterdir())[1]
            changes = json.loads((second / "loop" / joint.DIFF_FILE).read_text())
            self.assertEqual(
                changes["removed_cases"],
                [list(joint.case_key(one)), list(joint.case_key(two))],
            )

    def test_a_failed_case_list_keeps_every_case_and_fails_the_unit(self):
        with tempfile.TemporaryDirectory() as state:
            self.routine(state, {"loop": {"cases": [case("one.patch", "killed-by-named-rule")]}})
            with mock.patch.object(joint, "current_cases", return_value=None):
                with mock.patch.object(joint, "run_step", return_value=0), \
                        mock.patch.object(joint, "pin", return_value="f" * 40), \
                        contextlib.redirect_stdout(io.StringIO()):
                    status = joint.main(["routine", "--checkout", "/c", "--state", state])
            self.assertEqual(status, 1)
            merged = json.loads((Path(state) / "cumulative.json").read_text())
            self.assertEqual(len(merged["cases"]), 1)

    def test_a_blocked_generator_search_fails_the_unit_until_it_clears(self):
        blocked = {"cases": [case("one.patch", "killed-by-named-rule", [(11, "baseline-fails")])],
                   "generator_baseline_failures": [baseline_failure(11)]}
        cleared = {"cases": [case("one.patch", "killed-by-named-rule", [(11, "killed-by-named-rule")])]}
        other = {"cases": [case("two.patch", "killed-by-named-rule")]}
        with tempfile.TemporaryDirectory() as state:
            cumulative = Path(state) / "cumulative.json"
            self.assertEqual(self.routine(state, {"loop": blocked}), 1)
            # A run that didn't search seed 11 keeps its failure, so it's no
            # news again.
            self.assertEqual(self.routine(state, {"loop": other}), 0)
            self.assertEqual(len(json.loads(cumulative.read_text())["generator_baseline_failures"]), 1)
            # A run whose search of seed 11 ended clean clears it, so a new
            # failure there is news again.
            self.assertEqual(self.routine(state, {"loop": cleared}), 0)
            self.assertEqual(json.loads(cumulative.read_text())["generator_baseline_failures"], [])
            self.assertEqual(self.routine(state, {"loop": blocked}), 1)

    def test_the_libfuzzer_lane_keeps_its_own_pin_until_it_succeeds(self):
        killed = {"cases": [case("one.patch", "killed-by-named-rule")]}

        def since(step):
            return step["argv"][step["argv"].index("--changed-since") + 1]

        def steps():
            return {step["name"]: step for step in self.ran}

        def pin_file():
            return json.loads((Path(state) / joint.LIBFUZZER_LATEST_FILE).read_text())["pinned"]

        with tempfile.TemporaryDirectory() as state:
            self.routine(state, {"loop": killed}, "60")
            self.assertNotIn("libfuzzer", steps(), "a first run has no pin to bound it")
            self.assertFalse((Path(state) / joint.LIBFUZZER_LATEST_FILE).exists())
            # Before its own pin, the lane runs on what changed since the
            # loop's. A failure leaves that pin for the next run.
            self.assertEqual(self.routine(state, {"loop": killed}, "60", failing={"libfuzzer"}), 1)
            self.assertEqual(since(steps()["libfuzzer"]), "1" * 40)
            self.assertEqual(pin_file(), "1" * 40)
            self.assertEqual(self.routine(state, {"loop": killed}, "60", failing={"libfuzzer"}), 1)
            self.assertEqual(since(steps()["loop"]), "2" * 40)
            self.assertEqual(since(steps()["libfuzzer"]), "1" * 40, "the failed run's changes run again")
            self.assertEqual(pin_file(), "1" * 40)
            # A success advances it.
            self.assertEqual(self.routine(state, {"loop": killed}, "60"), 0)
            self.assertEqual(since(steps()["libfuzzer"]), "1" * 40)
            self.assertEqual(pin_file(), "4" * 40)
            self.routine(state, {"loop": killed}, "60")
            self.assertEqual(since(steps()["libfuzzer"]), "4" * 40)

    def test_merge_keeps_every_property_finding(self):
        merged = joint.merge(
            {"cases": [], "property_findings": [finding(11, "a")]},
            {"pinned": "c" * 40, "cases": [], "property_findings": [finding(23, "b"), finding(11, "a")]},
        )
        self.assertEqual([entry["seed"] for entry in merged["property_findings"]], [11, 23])
        self.assertEqual(merged["pinned"], "c" * 40)


if __name__ == "__main__":
    unittest.main()
