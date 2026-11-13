"""The suite fuzz run, its archive and its completeness report, against a fake campaign tool."""

import contextlib
import hashlib
import importlib.util
import io
import itertools
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "suite_fuzz_run.py"
SPEC = importlib.util.spec_from_file_location("suite_fuzz_run", SCRIPT)
suite = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(suite)

VERSIONS = {"harness": "sync/wire", "generator": "3", "scenario_bytes": "2", "format": "7", "contract": "4"}
IDENTITY = {
    "package": "qa/fuzz/sim", "target": "sync_wire", "harness": "sync/wire", "git": "a" * 40, "dirty": False,
    "source": "s" * 64, "lockfile": "l" * 64, "fuzz_lockfile": "f" * 64, "rustc": "rustc 1.99.0-nightly",
    "cargo_fuzz": "0.12.0", "libfuzzer_sys": "0.4.10", "rustflags": "--cfg zakura_sim",
    "cargo_args": ["--sanitizer", "address"],
}


def target_row(package, name, harness=None):
    return {"id": f"target/{package}/{name}", "expect": "pass", "package": package, "name": name, "harness": harness}


def suite_manifest(versions=VERSIONS):
    """A frozen manifest of two sim targets, a p2p one and a header-chain one, over the checkout's suite file."""
    return {
        "build": {"git": "a" * 40},
        "suite_sha256": hashlib.sha256((REPO / "qa" / "suite.toml").read_bytes()).hexdigest(),
        "harnesses": [
            {"harness": "sync/wire", "versions": versions},
            {"harness": "fixtures/all", "versions": dict(versions, harness="fixtures/all")},
        ],
        "targets": [
            target_row("sim", "sync_wire", "sync/wire"),
            target_row("sim", "race_fixtures", "fixtures/all"),
            target_row("p2p", "cadence"),
            target_row("header-chain", "fork_transitions"),
        ],
    }


def flag(argv, name):
    return argv[argv.index(name) + 1]


class FakeCampaignTool:
    """Stands in for `run_logged`: answers each `cargo xtask` command and writes the store records it would."""

    def __init__(self, identity=IDENTITY, versions=VERSIONS):
        self.identity = identity
        self.versions = versions
        self.calls = []
        self.counter = itertools.count()

    def digest(self, value):
        return hashlib.sha256(json.dumps([value, next(self.counter)]).encode()).hexdigest()

    def __call__(self, argv, cwd, log, env=None):
        self.calls.append(argv)
        if argv[2] != "sim-campaign":
            return suite.Ran(0, "")
        store = Path(flag(argv, "--store"))
        action = argv[3]
        if action == "build":
            digest = self.digest(self.identity)
            (store / "builds" / digest).mkdir(parents=True)
            record = {"binary": digest, "identity": self.identity}
            (store / "builds" / digest / "build.json").write_text(json.dumps(record))
            return suite.Ran(0, f"saved fuzz target {self.identity['target']} (address) as build {digest}\n")
        if action in ("start", "resume"):
            return self.run(store, argv, action)
        if action == "evidence":
            Path(flag(argv, "--out")).parent.mkdir(parents=True, exist_ok=True)
            Path(flag(argv, "--out")).write_text(json.dumps({"status": "complete", "unapplied": [], "incomplete": []}))
        return suite.Ran(0, "")

    def run(self, store, argv, action):
        build = flag(argv, "--build")
        identity = json.loads((store / "builds" / build / "build.json").read_text())["identity"]
        leaves = suite.store_manifests(store)
        if action == "resume":
            parent = suite.plan_campaign(store, build, self.versions)["parent"]
            links = {"resume_parent": parent, "previous_segment": None}
        else:
            parent = flag(argv, "--parent") if "--parent" in argv else None
            links = {"resume_parent": None, "previous_segment": parent}
        assert parent is None or parent in leaves
        manifest = {"build": {"binary": build, "identity": identity},
                    "harness": {"name": identity["harness"], "versions": self.versions}, **links}
        digest = self.digest(manifest)
        (store / "manifests").mkdir(exist_ok=True)
        (store / "manifests" / f"{digest}.json").write_text(json.dumps(manifest))
        (store / "runs" / digest).mkdir(parents=True)
        (store / "runs" / digest / "end.json").write_text(json.dumps({"status": "complete", "failures": []}))
        return suite.Ran(0, f"manifest {digest} in {store}/manifests/{digest}.json\nrun {digest} ended complete\n")

    def commands(self, subcommand, action=None):
        return [argv for argv in self.calls if argv[2] == subcommand and (action is None or argv[3] == action)]


class RunTests(unittest.TestCase):
    def run_suite(self, temp, tool, *flags, manifest=None):
        """Run `run` once over `manifest` in `temp`, and return its status and suite-fuzz.json."""
        path = Path(temp) / "manifest.json"
        path.write_text(json.dumps(manifest or suite_manifest()))
        out = Path(temp) / f"out-{len(tool.calls)}"
        with mock.patch.object(suite, "run_logged", side_effect=tool), contextlib.redirect_stderr(io.StringIO()):
            status = suite.main(["run", "--manifest", str(path), "--checkout", str(REPO),
                                 "--state", str(Path(temp) / "state"), "--out", str(out), *flags])
        return status, json.loads((out / suite.SUITE_FUZZ_FILE).read_text())

    def test_a_new_store_starts_and_a_store_with_a_run_resumes(self):
        tool = FakeCampaignTool()
        with tempfile.TemporaryDirectory() as temp:
            status, first = self.run_suite(temp, tool, "--target", "sync_wire")
            self.assertEqual(status, 0)
            [start] = tool.commands("sim-campaign", "start")
            self.assertEqual(flag(start, "--sim"), "sync/wire")
            self.assertEqual(flag(start, "--suite"), str(Path(temp) / "manifest.json"))
            self.assertNotIn("--parent", start)
            self.assertEqual(first["targets"][0]["plan"]["action"], "start")
            # Today's build has yesterday's identity, so the run resumes.
            status, second = self.run_suite(temp, tool, "--target", "sync_wire")
            self.assertEqual(status, 0)
            [resume] = tool.commands("sim-campaign", "resume")
            self.assertEqual(flag(resume, "--build"), second["targets"][0]["build"])
            self.assertEqual(second["targets"][0]["plan"]["parent"], first["targets"][0]["run"])
            for record in (first, second):
                target = record["targets"][0]
                self.assertEqual(set(target["steps"]), {"smoke", "build", "campaign", "verify", "evidence"})
                self.assertEqual(target["end_status"], "complete")
        smoke = tool.commands("sim-targets")[0]
        self.assertEqual(flag(smoke, "--target"), "sync_wire")
        evidence = tool.commands("sim-campaign", "evidence")[0]
        self.assertEqual(flag(evidence, "--run"), first["targets"][0]["run"])

    def test_a_new_commit_starts_a_segment_that_carries_the_corpus(self):
        tool = FakeCampaignTool()
        with tempfile.TemporaryDirectory() as temp:
            _, first = self.run_suite(temp, tool, "--target", "sync_wire")
            tool.identity = dict(IDENTITY, git="b" * 40, source="t" * 64)
            _, second = self.run_suite(temp, tool, "--target", "sync_wire")
        [_, segment] = tool.commands("sim-campaign", "start")
        self.assertEqual(flag(segment, "--parent"), first["targets"][0]["run"])
        self.assertEqual(flag(segment, "--corpus"), "carry")
        # Both builds are clean, so a new binary alone doesn't count.
        self.assertEqual(second["targets"][0]["plan"]["differences"], ["build git", "build source"])
        self.assertEqual(tool.commands("sim-campaign", "resume"), [])

    def test_a_new_generator_starts_a_segment_from_the_seeds(self):
        tool = FakeCampaignTool()
        newer = dict(VERSIONS, generator="4")
        with tempfile.TemporaryDirectory() as temp:
            self.run_suite(temp, tool, "--target", "sync_wire")
            tool.versions = newer
            _, second = self.run_suite(temp, tool, "--target", "sync_wire", manifest=suite_manifest(newer))
        segment = tool.commands("sim-campaign", "start")[1]
        self.assertEqual(flag(segment, "--corpus"), "seeds")
        self.assertEqual(second["targets"][0]["plan"]["differences"], ["harness versions"])

    def test_a_dirty_build_starts_a_segment_from_the_seeds(self):
        tool = FakeCampaignTool(identity=dict(IDENTITY, dirty=True))
        with tempfile.TemporaryDirectory() as temp:
            self.run_suite(temp, tool, "--target", "sync_wire")
            self.run_suite(temp, tool, "--target", "sync_wire")
        segment = tool.commands("sim-campaign", "start")[1]
        self.assertEqual(flag(segment, "--corpus"), "seeds")

    def test_a_subset_marks_itself_and_lists_the_omitted_rows(self):
        tool = FakeCampaignTool()
        with tempfile.TemporaryDirectory() as temp:
            _, record = self.run_suite(temp, tool, "--target", "sync_wire", "--target", "cadence")
        self.assertTrue(record["subset"])
        self.assertEqual(record["omitted"], ["target/sim/race_fixtures", "target/header-chain/fork_transitions"])
        self.assertEqual([target["id"] for target in record["targets"]], ["target/sim/sync_wire"])
        self.assertEqual([entry["id"] for entry in record["not_run"]], ["target/p2p/cadence"])

    def test_rows_the_campaign_tool_cant_build_are_not_run_with_a_reason(self):
        tool = FakeCampaignTool()
        with tempfile.TemporaryDirectory() as temp:
            status, record = self.run_suite(temp, tool)
        self.assertEqual(status, 0)
        self.assertFalse(record["subset"])
        self.assertEqual(record["omitted"], [])
        self.assertEqual(record["pinned"], "a" * 40)
        self.assertEqual([target["target"] for target in record["targets"]], ["sync_wire", "race_fixtures"])
        reasons = {entry["id"]: entry["reason"] for entry in record["not_run"]}
        self.assertEqual(set(reasons), {"target/p2p/cadence", "target/header-chain/fork_transitions"})
        self.assertIn("qa/fuzz/p2p", reasons["target/p2p/cadence"])
        built = {flag(argv, "--target") for argv in tool.commands("sim-campaign", "build")}
        self.assertEqual(built, {"sync_wire", "race_fixtures"})

    def test_budgets_come_from_the_suite_unless_overridden(self):
        tool = FakeCampaignTool()
        with tempfile.TemporaryDirectory() as temp:
            self.run_suite(temp, tool, "--target", "sync_wire")
            self.run_suite(temp, tool, "--target", "race_fixtures", "--seconds", "900", "--workers", "2")
        suite_budget, override = tool.commands("sim-campaign", "start")
        # qa/suite.toml's [campaign.sim]: 120 s and 4096 MB, and no row override.
        self.assertEqual(flag(suite_budget, "--seconds"), "120")
        self.assertEqual(flag(suite_budget, "--rss-limit-mb"), "4096")
        self.assertEqual(flag(suite_budget, "--workers"), "1")
        self.assertEqual(flag(override, "--seconds"), "900")
        self.assertEqual(flag(override, "--workers"), "2")

    def test_a_suite_file_the_manifest_didnt_freeze_is_refused(self):
        manifest = dict(suite_manifest(), suite_sha256="0" * 64)
        tool = FakeCampaignTool()
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "manifest.json"
            path.write_text(json.dumps(manifest))
            with mock.patch.object(suite, "run_logged", side_effect=tool), \
                    contextlib.redirect_stderr(io.StringIO()):
                status = suite.main(["run", "--manifest", str(path), "--checkout", str(REPO),
                                     "--state", temp, "--out", temp])
        self.assertEqual(status, 2)
        self.assertEqual(tool.calls, [])

    def test_a_failed_build_stops_its_target_and_fails_the_run(self):
        tool = FakeCampaignTool()

        def broken(argv, cwd, log, env=None):
            if argv[2:4] == ["sim-campaign", "build"]:
                tool.calls.append(argv)
                return suite.Ran(1, "error: cargo fuzz build failed\n")
            return tool(argv, cwd, log, env)

        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "manifest.json"
            path.write_text(json.dumps(suite_manifest()))
            with mock.patch.object(suite, "run_logged", side_effect=broken), \
                    contextlib.redirect_stderr(io.StringIO()):
                status = suite.main(["run", "--manifest", str(path), "--checkout", str(REPO), "--state", temp,
                                     "--out", temp, "--target", "sync_wire"])
            record = json.loads((Path(temp) / suite.SUITE_FUZZ_FILE).read_text())
        self.assertEqual(status, 1)
        self.assertEqual(record["targets"][0]["error"], "the build failed")
        self.assertEqual(tool.commands("sim-campaign", "start"), [])


class ArchiveTests(unittest.TestCase):
    def test_each_runs_failure_inputs_are_triaged_once_and_each_bundle_gets_a_verdict(self):
        with tempfile.TemporaryDirectory() as temp:
            store = Path(temp) / "campaigns" / "sync_wire"
            run = "r" * 64
            (store / "runs" / run).mkdir(parents=True)
            failures = [
                {"path": f"runs/{run}/workers/0/artifacts/crash-1", "sha256": "a" * 64},
                {"path": f"runs/{run}/workers/0/sim/inputs/active/p/{'a' * 64}.bin", "sha256": "a" * 64},
                {"path": f"runs/{run}/workers/0/sim/traces/p/0123456789ab/trace.jsonl", "sha256": "c" * 64},
                {"path": f"runs/{run}/workers/1/artifacts/timeout-2", "sha256": "b" * 64},
            ]
            (store / "runs" / run / "end.json").write_text(json.dumps({"status": "complete", "failures": failures}))
            suite_fuzz = {"targets": [
                {"target": "sync_wire", "harness": "sync/wire", "store": str(store), "run": run},
                {"target": "sync_storage", "harness": "sync/storage", "store": str(store), "error": "the build failed"},
            ]}
            archive_dir = Path(temp) / "archive"
            crash = store / failures[0]["path"]
            timeout = store / failures[3]["path"]
            calls = []

            def tool(argv, cwd, log, env=None):
                calls.append(argv)
                if argv[2] == "sim-triage":
                    archive_dir.mkdir()
                    return suite.Ran(0, "\n".join([
                        "$ cargo run ...",
                        f"{crash}: confirmed in active: p; archived in {archive_dir}/one",
                        f"{crash}: minimized; archived in {archive_dir}/one.minimized",
                        f"{timeout}: not-reproduced; already archived in {archive_dir}/two",
                    ]))
                return suite.Ran(1, f"{archive_dir}/two: SHA256SUMS lists input.bin with another digest\n"
                                    "error: 1 of 3 bundles don't match their checksums\n")

            with mock.patch.object(suite, "run_logged", side_effect=tool):
                record = suite.archive(suite_fuzz, archive_dir, Path(temp), REPO, 64, 300)
        triage, verify = calls
        self.assertEqual(triage[:5], ["cargo", "xtask", "sim-triage", "--sim", "sync/wire"])
        self.assertEqual(flag(triage, "--out"), str(archive_dir))
        self.assertEqual(flag(triage, "--target"), "sync_wire")
        self.assertEqual(flag(triage, "--runs"), "64")
        self.assertEqual(flag(triage, "--wall-secs"), "300")
        self.assertEqual(triage[-2:], [str(crash), str(timeout)])
        self.assertEqual(verify, ["cargo", "xtask", "sim-verify-archive", str(archive_dir)])
        self.assertEqual(record["campaigns"], [{"target": "sync_wire", "run": run, "inputs": 2, "triage": 0}])
        self.assertEqual(record["verify"], 1)
        verdicts = [(Path(b["bundle"]).name, b["status"], b["verdict"]) for b in record["bundles"]]
        self.assertEqual(verdicts, [
            ("one", "confirmed", "match"),
            ("one.minimized", "minimized", "match"),
            ("two", "not-reproduced", "mismatch"),
        ])
        self.assertEqual(record["bundles"][2]["problems"], ["SHA256SUMS lists input.bin with another digest"])

    def test_minimization_off_drops_the_minimizer_flags(self):
        with tempfile.TemporaryDirectory() as temp:
            store = Path(temp)
            run = "r" * 64
            (store / "runs" / run).mkdir(parents=True)
            end = {"failures": [{"path": f"runs/{run}/workers/0/artifacts/crash-1", "sha256": "a" * 64}]}
            (store / "runs" / run / "end.json").write_text(json.dumps(end))
            calls = []

            def tool(argv, cwd, log, env=None):
                calls.append(argv)
                return suite.Ran(0, "")

            target = {"target": "sync_wire", "harness": "sync/wire", "store": str(store), "run": run}
            with mock.patch.object(suite, "run_logged", side_effect=tool):
                record = suite.archive({"targets": [target]}, store / "archive", store, REPO, 0, 300)
        self.assertNotIn("--target", calls[0])
        # Triage made no archive directory, so nothing is verified.
        self.assertEqual(len(calls), 1)
        self.assertIsNone(record["verify"])


def full_manifest():
    """A manifest with a row of each kind the completeness report reads."""
    manifest = suite_manifest()
    manifest.update({
        "tests": [
            {"id": "test/zakura-sim/lib", "kind": "sim-lib"},
            {"id": "test/zakura-sim/sim_discovery", "kind": "sim-test"},
            {"id": "test/zakura-rt/lib", "kind": "sim-lib"},
        ],
        "witnesses": [
            {"id": "witness/a/passing", "expect": "pass", "decided_by": "test/zakura-sim/sim_discovery"},
            {"id": "witness/b/passing", "expect": "pass", "decided_by": "test/zakura-rt/lib"},
            {"id": "witness/c/bypass", "expect": "unimplemented", "decided_by": None, "pending": "no planted case"},
        ],
        "planted": [{"id": "planted/a"}],
        "seed_sets": [{"id": "seeds/sync/wire"}],
        "seed_witnesses": [],
        "pending": [
            {"id": "missing-target/discovery/wire", "kind": "target", "owner": "fuzz-10", "detail": "no target yet"},
            {"id": "pending/feature/hold", "kind": "feature", "owner": "G2a/E2b", "detail": "https://plan"},
        ],
        "excluded": [{"id": "excluded/target/rpc/json_rpc_request", "reason": "not P2P"}],
        "properties": [{}, {}],
        "coverage": [{}],
    })
    return manifest


class CompletenessTests(unittest.TestCase):
    def test_rows_are_ran_not_run_inconclusive_or_unimplemented(self):
        with tempfile.TemporaryDirectory() as temp:
            evidence = Path(temp) / "evidence.json"
            evidence.write_text(json.dumps({"status": "complete", "unapplied": ["sync/x"], "incomplete": [{}]}))
            suite_fuzz = {
                "targets": [{"id": "target/sim/sync_wire", "run": "r" * 64, "end_status": "complete",
                             "steps": {"campaign": 0}, "evidence": str(evidence)}],
                "not_run": [{"id": "target/p2p/cadence", "reason": "p2p can't build"}],
                "omitted": ["target/sim/race_fixtures", "target/header-chain/fork_transitions"],
            }
            loop = {"cases": [{
                "patch": "one.patch", "property": "p", "result": {"class": "killed-by-named-rule"},
                "generator": [{"harness": "sync/wire", "seed": 11,
                               "result": {"class": "harness-failure", "reason": "timeout"}}],
            }]}
            report = suite.completeness(full_manifest(), {"suite": 0, "suite-fuzz": 0}, suite_fuzz, loop)
        ran = {entry["id"]: entry["by"] for entry in report["ran"]}
        self.assertEqual(ran, {
            "target/sim/sync_wire": "suite-fuzz",
            "test/zakura-sim/lib": "suite",
            "test/zakura-sim/sim_discovery": "suite",
            "witness/a/passing": "test/zakura-sim/sim_discovery",
        })
        not_run = {entry["id"]: entry["reason"] for entry in report["not_run"]}
        self.assertEqual(set(not_run), {
            "target/sim/race_fixtures", "target/header-chain/fork_transitions", "target/p2p/cadence",
            "test/zakura-rt/lib", "witness/b/passing", "planted/a", "seeds/sync/wire",
        })
        self.assertIn("--target", not_run["target/sim/race_fixtures"])
        self.assertEqual(not_run["target/p2p/cadence"], "p2p can't build")
        self.assertEqual(
            [entry["id"] for entry in report["unimplemented"]],
            ["witness/c/bypass", "missing-target/discovery/wire", "pending/feature/hold"],
        )
        [campaign, lane] = report["inconclusive"]
        self.assertEqual(campaign["id"], "target/sim/sync_wire")
        self.assertEqual(len(campaign["reasons"]), 2)
        self.assertEqual((lane["lane"], lane["class"], lane["reason"]),
                         ("generator sync/wire seed 11", "harness-failure", "timeout"))
        self.assertEqual(report["excluded"], [{"id": "excluded/target/rpc/json_rpc_request", "reason": "not P2P"}])
        self.assertEqual(report["derived"], {"harnesses": 2, "properties": 2, "coverage": 1})
        # Completeness keeps no correctness findings.
        self.assertNotIn("findings", report)

    def test_without_the_suite_fuzz_every_target_is_not_run(self):
        report = suite.completeness(full_manifest(), {"suite": 1}, None, None)
        targets = [entry for entry in report["not_run"] if entry["id"].startswith("target/")]
        self.assertEqual(len(targets), 4)
        self.assertTrue(all(entry["reason"] == "the routine's suite fuzz is off" for entry in targets))
        self.assertEqual(report["inconclusive"], [])
        self.assertIn({"id": "test/zakura-sim/lib", "by": "suite", "exit": 1}, report["ran"])

    def test_the_cli_reads_the_routines_steps_and_writes_the_report(self):
        with tempfile.TemporaryDirectory() as temp:
            temp = Path(temp)
            (temp / "manifest.json").write_text(json.dumps(full_manifest()))
            (temp / "routine.json").write_text(json.dumps({"steps": {"suite-manifest": 0, "suite": 0}}))
            status = suite.main(["completeness", "--manifest", str(temp / "manifest.json"),
                                 "--routine", str(temp / "routine.json"), "--out", str(temp)])
            report = json.loads((temp / suite.COMPLETENESS_FILE).read_text())
            with contextlib.redirect_stderr(io.StringIO()):
                missing = suite.main(["completeness", "--manifest", str(temp / "none.json"), "--out", str(temp)])
        self.assertEqual(status, 0)
        self.assertIn("test/zakura-sim/lib", {entry["id"] for entry in report["ran"]})
        self.assertEqual(missing, 2)


if __name__ == "__main__":
    unittest.main()
