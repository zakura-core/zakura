#!/usr/bin/env python3
"""Fuzz each target of the frozen suite manifest in its own campaign store, then archive and account for it.

The three subcommands read the manifest that `cargo xtask sim-suite --frozen
--out <file>` writes (mutation plan, run-3 and run-4).

`run` fuzzes each target row. For each target it runs, it runs
`cargo xtask sim-targets --target <name>` as a smoke, builds the target into
`<state>/campaigns/<target>` with `sim-campaign build`, and then runs one
campaign run there: `sim-campaign start` if the store holds no run, else
`sim-campaign resume`. It then runs `sim-campaign verify` and
`sim-campaign evidence`. `sim-campaign build` builds only qa/fuzz/sim's
targets, so `run` lists every other target as not run, with that reason.
Each target's budget is its `[campaign.sim]` budget in `qa/suite.toml`, as
`scripts/check-fuzz-campaign.py --matrix sim` joins it with the row's own;
`--seconds` overrides each budget's length. `--target` runs a subset, and
`suite-fuzz.json` then marks `subset` and lists the omitted rows.

The campaign tool decides when a run starts a new segment, and `run` follows
its rules. `resume` refuses a build with another identity, such as one from
a new commit, or a harness with other versions, such as a new generator. For
those, `run` starts a new segment after the store's last run instead:
`--corpus carry` if both builds are clean and the harness's versions didn't
change, else `--corpus seeds`.

`archive` triages each campaign run's failure inputs into one archive with
`cargo xtask sim-triage`, then checks the archive with
`cargo xtask sim-verify-archive`, and writes each bundle's path and verdict
to `archive.json`.

`completeness` diffs the manifest's rows against what the routine ran, and
writes `completeness.json`: the rows it ran, the rows it didn't run with the
reason, the ran rows whose evidence is inconclusive, and the unimplemented
rows. It reports coverage only. Correctness findings stay in the routine's
`findings.json`, and `completeness` exits 0 whatever it lists.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import NamedTuple

SUITE_FUZZ_FILE = "suite-fuzz.json"
ARCHIVE_FILE = "archive.json"
COMPLETENESS_FILE = "completeness.json"
EVIDENCE_FILE = "evidence.json"
SUITE_FILE = "qa/suite.toml"
# The fuzz package that `sim-campaign build` builds from.
CAMPAIGN_PACKAGE = "sim"
MATRIX_SCRIPT = Path(__file__).resolve().parent / "check-fuzz-campaign.py"
BUILT = re.compile(r" as build ([0-9a-f]{64})$", re.MULTILINE)
CHECKSUM_MISMATCH = re.compile(r"bundles don't match their checksums$", re.MULTILINE)
WROTE_MANIFEST = re.compile(r"^manifest ([0-9a-f]{64}) in ", re.MULTILINE)
# The identity fields whose `unknown` makes a build unclean, as
# `BuildIdentity::clean` says.
PROVENANCE_FIELDS = ("git", "source", "lockfile", "fuzz_lockfile")
UNKNOWN = "unknown"
# Loop lane classes that give no mutant result.
RUN_FAILURES = ("did-not-compile", "patch-does-not-apply", "baseline-fails", "harness-failure")
# qa/fuzz/sim/README.md's example minimization budgets.
MINIMIZE_RUNS = 64
MINIMIZE_WALL_SECS = 300


class Ran(NamedTuple):
    """A command's exit status and its combined output."""

    code: int
    output: str


def run_logged(argv, cwd, log, env=None):
    """Run `argv` in `cwd` with its output in `log`, and return its status and output."""
    log = Path(log)
    log.parent.mkdir(parents=True, exist_ok=True)
    with open(log, "w") as out:
        ended = subprocess.run(
            argv, cwd=cwd, env=env, stdout=out, stderr=subprocess.STDOUT, check=False,
        )
    return Ran(ended.returncode, log.read_text(errors="replace"))


def xtask(subcommand, *args):
    return ["cargo", "xtask", subcommand, *(str(arg) for arg in args)]


def read_json(path):
    """Return the JSON at `path`, or None if it's missing."""
    path = Path(path)
    return json.loads(path.read_text()) if path.exists() else None


def write_json(path, value):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(value, indent=2) + "\n")


def sha256_file(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def campaign_budgets(checkout, manifest):
    """Return each sim target's campaign budget by name, from the suite file the manifest froze.

    Refuses a checkout whose `qa/suite.toml` isn't the one the manifest
    digests, since its budgets could then belong to other rows.
    """
    suite = Path(checkout) / SUITE_FILE
    if sha256_file(suite) != manifest["suite_sha256"]:
        raise ValueError(f"{suite} isn't the suite file the manifest froze; rerun `cargo xtask sim-suite --frozen`")
    spec = importlib.util.spec_from_file_location("check_fuzz_campaign", MATRIX_SCRIPT)
    matrix = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(matrix)
    return {row["target"]: row for row in matrix.campaign_matrix(Path(checkout), CAMPAIGN_PACKAGE)["include"]}


def not_runnable(row, versions):
    """Return why `run` can't fuzz a target row, or None if it can."""
    if row["package"] != CAMPAIGN_PACKAGE:
        return f"`sim-campaign build` builds only qa/fuzz/{CAMPAIGN_PACKAGE}'s targets, not qa/fuzz/{row['package']}'s"
    if not row.get("harness"):
        return "the target maps to no harness, and `sim-campaign start` needs one"
    if row["harness"] not in versions:
        return f"the manifest has no harness row for {row['harness']}"
    return None


def harness_versions(manifest):
    """Return each registered harness's versions by name."""
    return {row["harness"]: row["versions"] for row in manifest.get("harnesses", [])}


def store_manifests(store):
    """Return each run manifest in a campaign store by its digest."""
    found = {}
    for path in sorted((Path(store) / "manifests").glob("*.json")):
        if not path.name.startswith("."):
            found[path.stem] = json.loads(path.read_text())
    return found


def is_clean(identity):
    """Return true if a build identity says what the binary was built from, as `BuildIdentity::clean` does."""
    return not identity["dirty"] and all(identity[field] != UNKNOWN for field in PROVENANCE_FIELDS)


def plan_campaign(store, build, versions):
    """Return how to run `build` in `store`: start, resume, or start a new segment after the last run.

    It applies `prepare_resume`'s refusals that the store's records show: a
    build of another identity, another binary unless both builds are clean,
    and other harness versions. Each of them starts a new segment, which
    carries the corpus only if both builds are clean and the versions match.
    """
    manifests = store_manifests(store)
    if not manifests:
        return {"action": "start", "parent": None, "corpus": None, "differences": []}
    continued = {
        manifest.get(field) for manifest in manifests.values() for field in ("resume_parent", "previous_segment")
    }
    leaves = [digest for digest in manifests if digest not in continued]
    if len(leaves) != 1:
        return {"action": None, "reason": f"the store holds {len(leaves)} runs that no run continues"}
    parent = manifests[leaves[0]]
    old = parent["build"]["identity"]
    new = json.loads((Path(store) / "builds" / build / "build.json").read_text())["identity"]
    differences = [f"build {field}" for field in sorted(set(old) | set(new)) if old.get(field) != new.get(field)]
    clean = is_clean(old) and is_clean(new)
    if build != parent["build"]["binary"] and not clean:
        differences.append("build binary, which a dirty or unknown build must keep")
    same_versions = parent["harness"]["versions"] == versions
    if not same_versions:
        differences.append("harness versions")
    if not differences:
        return {"action": "resume", "parent": leaves[0], "corpus": None, "differences": []}
    corpus = "carry" if clean and same_versions else "seeds"
    return {"action": "start", "parent": leaves[0], "corpus": corpus, "differences": differences}


def campaign_command(plan, store, target, harness, build, budget, suite):
    """Return the `sim-campaign start` or `resume` command line, and the environment it adds."""
    if plan["action"] == "resume":
        # `resume` keeps the parent's budget but its length, and its suite digest.
        argv = xtask("sim-campaign", "resume", "--store", store, "--build", build,
                     "--seconds", budget["seconds"], "--workers", budget["workers"])
        return argv, {}
    argv = xtask(
        "sim-campaign", "start", "--store", store, "--campaign", target, "--sim", harness,
        "--build", build, "--seconds", budget["seconds"], "--workers", budget["workers"],
        "--suite", suite, "--max-len", budget["max_len"], "--rss-limit-mb", budget["rss_mb"],
        "--libfuzzer-arg", f"-detect_leaks={budget['detect_leaks']}",
    )
    for arg in budget["extra_args"].split():
        argv += ["--libfuzzer-arg", arg]
    if plan["parent"]:
        argv += ["--parent", plan["parent"], "--corpus", plan["corpus"]]
    # The supervisor records the sanitizer variables it starts with.
    return argv, {"ASAN_OPTIONS": f"detect_leaks={budget['detect_leaks']}"}


def build_env():
    """Return the environment with the CXXFLAGS that the fuzz builds need, unless it sets its own."""
    return {**os.environ, "CXXFLAGS": os.environ.get("CXXFLAGS", "-include cstdint")}


class Fuzz:
    """One `run`'s paths: the checkout it runs from, the stores, the output and the manifest."""

    def __init__(self, checkout, state, out, manifest_path):
        self.checkout = Path(checkout)
        self.state = Path(state)
        self.out = Path(out)
        self.manifest_path = Path(manifest_path)

    def call(self, argv, log, env=None):
        """Run one command from the checkout with its log under `suite-fuzz/`, and return its status and output."""
        return run_logged(argv, self.checkout, self.out / "suite-fuzz" / log, {**build_env(), **(env or {})})


def prepare_target(fuzz, row, budget, versions):
    """Smoke and build one target, and plan its campaign run. Return its record."""
    name, harness = row["name"], row["harness"]
    store = fuzz.state / "campaigns" / name
    record = {"id": row["id"], "target": name, "harness": harness, "store": str(store), "budget": budget, "steps": {}}
    smoke = fuzz.call(xtask("sim-targets", "--out", fuzz.out / "suite-fuzz" / name / "smoke", "--target", name),
                      f"{name}/smoke.log")
    record["steps"]["smoke"] = smoke.code
    store.mkdir(parents=True, exist_ok=True)
    built = fuzz.call(xtask("sim-campaign", "build", "--store", store, "--target", name), f"{name}/build.log")
    record["steps"]["build"] = built.code
    builds = BUILT.findall(built.output)
    if built.code != 0 or not builds:
        record["error"] = "the build failed" if built.code else "the build log names no build"
        return record
    record["build"] = builds[-1]
    record["plan"] = plan_campaign(store, record["build"], versions[harness])
    if record["plan"]["action"] is None:
        record["error"] = record["plan"]["reason"]
    return record


def run_campaign(fuzz, record):
    """Run one prepared target's campaign, then verify its store and sum its run's evidence."""
    if "error" in record:
        return record
    store, name = record["store"], record["target"]
    argv, env = campaign_command(record["plan"], store, name, record["harness"], record["build"],
                                 record["budget"], fuzz.manifest_path)
    ran = fuzz.call(argv, f"{name}/campaign.log", env)
    record["steps"]["campaign"] = ran.code
    record["steps"]["verify"] = fuzz.call(xtask("sim-campaign", "verify", "--store", store), f"{name}/verify.log").code
    runs = WROTE_MANIFEST.findall(ran.output)
    if not runs:
        record["error"] = f"`sim-campaign {record['plan']['action']}` wrote no run manifest"
        return record
    record["run"] = runs[-1]
    end = read_json(Path(store) / "runs" / record["run"] / "end.json")
    record["end_status"] = end["status"] if end else None
    record["failures"] = len(end["failures"]) if end else 0
    evidence = fuzz.out / "suite-fuzz" / name / EVIDENCE_FILE
    record["steps"]["evidence"] = fuzz.call(
        xtask("sim-campaign", "evidence", "--store", store, "--run", record["run"], "--out", evidence),
        f"{name}/evidence.log",
    ).code
    record["evidence"] = str(evidence)
    return record


def fuzz_suite(fuzz, manifest, budgets, selected, seconds=None, workers=1, jobs=1):
    """Fuzz each selected target row, and return the `suite-fuzz.json` record.

    Every target builds first, since builds share the fuzz package's target
    directory, and then up to `jobs` campaigns run at once.
    """
    versions = harness_versions(manifest)
    rows = manifest["targets"]
    chosen = [row for row in rows if selected is None or row["name"] in selected]
    not_run, prepared = [], []
    for row in chosen:
        reason = not_runnable(row, versions)
        if reason:
            not_run.append({"id": row["id"], "reason": reason})
            continue
        budget = dict(budgets[row["name"]], workers=workers)
        if seconds:
            budget["seconds"] = seconds
        prepared.append(prepare_target(fuzz, row, budget, versions))
    with ThreadPoolExecutor(max_workers=jobs) as pool:
        targets = list(pool.map(lambda record: run_campaign(fuzz, record), prepared))
    return {
        "manifest": str(fuzz.manifest_path),
        "manifest_sha256": sha256_file(fuzz.manifest_path),
        "pinned": manifest["build"]["git"],
        "subset": selected is not None,
        "omitted": [row["id"] for row in rows if row not in chosen],
        "targets": targets,
        "not_run": not_run,
    }


def failure_inputs(end):
    """Return the store paths of a run's failure files that are fuzz inputs, once per digest.

    libFuzzer's artifact directories hold only inputs. A target's own
    directory holds its traces too, and its inputs under `inputs/`.
    """
    seen, found = set(), []
    for record in end.get("failures", []):
        parts = record["path"].split("/")
        libfuzzer = "artifacts" in parts or "compact-artifacts" in parts
        saved = any(a in ("sim", "compact-sim") and b == "inputs" for a, b in zip(parts, parts[1:]))
        if (libfuzzer or saved) and record["sha256"] not in seen:
            seen.add(record["sha256"])
            found.append(record["path"])
    return found


def triaged_bundles(output, inputs):
    """Return the bundles `sim-triage` printed for each input: its status, bundle and whether it's minimized."""
    bundles = []
    for line in output.splitlines():
        input_path = next((path for path in inputs if line.startswith(f"{path}: ")), None)
        if input_path is None or " archived in " not in line:
            continue
        rest, _, bundle = line[len(input_path) + 2:].rpartition(" archived in ")
        status = re.split(r"[;\s]", rest, maxsplit=1)[0]
        bundles.append({
            "input": input_path,
            "status": status,
            "minimized": status == "minimized",
            "bundle": bundle,
        })
    return bundles


def archive(suite_fuzz, archive_dir, out, checkout, minimize_runs, minimize_wall_secs):
    """Triage each campaign run's failure inputs into `archive_dir`, verify it, and return `archive.json`."""
    archive_dir = Path(archive_dir)
    campaigns, bundles = [], []
    for target in suite_fuzz["targets"]:
        if "run" not in target:
            continue
        store = Path(target["store"])
        end = read_json(store / "runs" / target["run"] / "end.json") or {}
        inputs = [str(store / path) for path in failure_inputs(end)]
        entry = {"target": target["target"], "run": target["run"], "inputs": len(inputs), "triage": None}
        campaigns.append(entry)
        if not inputs:
            continue
        minimize = []
        if minimize_runs:
            minimize = ["--target", target["target"], "--runs", minimize_runs, "--wall-secs", minimize_wall_secs]
        argv = xtask("sim-triage", "--sim", target["harness"], "--out", archive_dir, *minimize, *inputs)
        ran = run_logged(argv, checkout, Path(out) / "archive" / f"{target['target']}-triage.log", build_env())
        entry["triage"] = ran.code
        bundles += [dict(bundle, target=target["target"]) for bundle in triaged_bundles(ran.output, inputs)]
    verify = None
    if archive_dir.exists():
        verified = run_logged(xtask("sim-verify-archive", archive_dir), checkout, Path(out) / "archive" / "verify.log",
                              build_env())
        verify = verified.code
        # Exit 1 lists each bundle that doesn't match; any other failure checked none.
        checked = verify == 0 or (verify == 1 and CHECKSUM_MISMATCH.search(verified.output))
        problems = {}
        for line in verified.output.splitlines():
            bundle = next((b["bundle"] for b in bundles if line.startswith(f"{b['bundle']}: ")), None)
            if bundle:
                problems.setdefault(bundle, []).append(line[len(bundle) + 2:])
        for bundle in bundles:
            if bundle["bundle"] in problems:
                bundle["verdict"], bundle["problems"] = "mismatch", problems[bundle["bundle"]]
            else:
                bundle["verdict"] = "match" if checked else "unverified"
    return {"archive": str(archive_dir), "campaigns": campaigns, "bundles": bundles, "verify": verify}


def evidence_gaps(path):
    """Return why a campaign run's evidence is inconclusive, or an empty list."""
    evidence = read_json(path) if path else None
    if evidence is None:
        return ["the run has no evidence report"]
    reasons = []
    if evidence.get("status") != "complete":
        reasons.append(f"the run ended {evidence.get('status') or 'without an end record'}")
    if evidence.get("unapplied"):
        reasons.append("no complete process saw these apply: " + ", ".join(evidence["unapplied"]))
    if evidence.get("incomplete"):
        reasons.append(f"{len(evidence['incomplete'])} gaps in the counts")
    return reasons


# Why the routine runs no row of each command kind but targets and tests.
NOT_RUN_REASONS = {
    "planted": "the routine runs `planted loop` on mutation cases, not `cargo xtask planted`",
    "seed_sets": "the routine doesn't run `cargo xtask sim-seeds`",
    "seed_witnesses": "the routine doesn't run the seeds' witness tests",
}
# Rows that other rows decide, so nothing runs them.
DERIVED_KINDS = ("harnesses", "properties", "coverage")


def completeness(manifest, steps, suite_fuzz, loop_report):
    """Return each manifest row as ran, not run with a reason, inconclusive or unimplemented.

    `steps` maps each routine step to its exit status. The `suite` step runs
    every zakura-sim test row. `suite_fuzz` is `run`'s record, or None if it
    didn't run. A witness row counts as ran if the row that decides it ran.
    """
    ran, not_run, inconclusive, unimplemented = [], [], [], []
    fuzzed = {target["id"]: target for target in (suite_fuzz or {}).get("targets", [])}
    unfuzzed = {entry["id"]: entry["reason"] for entry in (suite_fuzz or {}).get("not_run", [])}
    omitted = set((suite_fuzz or {}).get("omitted", []))
    for row in manifest.get("targets", []):
        target = fuzzed.get(row["id"])
        if target and "run" in target:
            ran.append({"id": row["id"], "by": "suite-fuzz", "run": target["run"],
                        "end_status": target.get("end_status"), "exit": target["steps"].get("campaign")})
            gaps = evidence_gaps(target.get("evidence"))
            if gaps:
                inconclusive.append({"id": row["id"], "reasons": gaps})
        elif target:
            not_run.append({"id": row["id"], "reason": f"its campaign didn't run: {target['error']}"})
        elif row["id"] in unfuzzed:
            not_run.append({"id": row["id"], "reason": unfuzzed[row["id"]]})
        elif row["id"] in omitted:
            not_run.append({"id": row["id"], "reason": "`--target` left it out of the subset"})
        elif "suite-fuzz" in steps:
            not_run.append({"id": row["id"], "reason": "the suite-fuzz step wrote no record of it"})
        else:
            not_run.append({"id": row["id"], "reason": "the routine's suite fuzz is off"})
    ran_ids = {entry["id"] for entry in ran}
    for row in manifest.get("tests", []):
        if row["id"].startswith("test/zakura-sim/") and "suite" in steps:
            ran.append({"id": row["id"], "by": "suite", "exit": steps["suite"]})
            ran_ids.add(row["id"])
        else:
            not_run.append({"id": row["id"], "reason": "the routine's suite step runs only zakura-sim's tests"})
    for row in manifest.get("witnesses", []):
        if row["expect"] == "unimplemented":
            unimplemented.append({"id": row["id"], "kind": "witness", "detail": row.get("pending")})
        elif row.get("decided_by") in ran_ids:
            ran.append({"id": row["id"], "by": row["decided_by"]})
        else:
            decider = row.get("decided_by") or "its test"
            not_run.append({"id": row["id"], "reason": f"the routine doesn't run {decider}"})
    for kind, reason in NOT_RUN_REASONS.items():
        not_run += [{"id": row["id"], "reason": reason} for row in manifest.get(kind, [])]
    for row in manifest.get("pending", []):
        unimplemented.append({"id": row["id"], "kind": row["kind"], "owner": row["owner"], "detail": row["detail"]})
    for case in (loop_report or {}).get("cases", []):
        lanes = [("witness", case)] + [(f"generator {lane['harness']} seed {lane['seed']}", lane)
                                       for lane in case.get("generator", [])]
        for name, lane in lanes:
            if lane["result"]["class"] in RUN_FAILURES:
                inconclusive.append({"loop_case": case["patch"], "property": case["property"], "lane": name,
                                     "class": lane["result"]["class"], "reason": lane["result"].get("reason")})
    return {
        "pinned": manifest["build"]["git"],
        "ran": ran,
        "not_run": not_run,
        "inconclusive": inconclusive,
        "unimplemented": unimplemented,
        "excluded": [{"id": row["id"], "reason": row["reason"]} for row in manifest.get("excluded", [])],
        "derived": {kind: len(manifest.get(kind, [])) for kind in DERIVED_KINDS},
    }


def run_command(args):
    manifest = read_json(args.manifest)
    if manifest is None:
        print(f"{args.manifest} is missing: `cargo xtask sim-suite --frozen --out` writes it", file=sys.stderr)
        return 2
    names = {row["name"] for row in manifest["targets"]}
    unknown = sorted(set(args.target) - names)
    if unknown:
        print(f"the manifest has no target {', '.join(unknown)}", file=sys.stderr)
        return 2
    try:
        budgets = campaign_budgets(args.checkout, manifest)
    except ValueError as error:
        print(error, file=sys.stderr)
        return 2
    fuzz = Fuzz(Path(args.checkout).resolve(), Path(args.state).resolve(), Path(args.out).resolve(),
                Path(args.manifest).resolve())
    record = fuzz_suite(fuzz, manifest, budgets, set(args.target) if args.target else None,
                        args.seconds, args.workers, args.jobs)
    write_json(fuzz.out / SUITE_FUZZ_FILE, record)
    failed = [target["target"] for target in record["targets"]
              if "error" in target or any(code != 0 for code in target["steps"].values())]
    for name in failed:
        print(f"{name}: a step failed; see {fuzz.out / 'suite-fuzz' / name}", file=sys.stderr)
    return 1 if failed else 0


def archive_command(args):
    suite_fuzz = read_json(args.suite_fuzz)
    if suite_fuzz is None:
        print(f"{args.suite_fuzz} is missing: `run` writes it", file=sys.stderr)
        return 2
    record = archive(suite_fuzz, Path(args.archive).resolve(), Path(args.out).resolve(),
                     Path(args.checkout).resolve(), args.minimize_runs, args.minimize_wall_secs)
    write_json(Path(args.out) / ARCHIVE_FILE, record)
    triage_failed = any(entry["triage"] not in (None, 0) for entry in record["campaigns"])
    return 1 if triage_failed or record["verify"] not in (None, 0) else 0


def completeness_command(args):
    manifest = read_json(args.manifest)
    if manifest is None:
        print(f"{args.manifest} is missing: `cargo xtask sim-suite --frozen --out` writes it", file=sys.stderr)
        return 2
    routine = read_json(args.routine) if args.routine else None
    record = completeness(
        manifest,
        (routine or {}).get("steps", {}),
        read_json(args.suite_fuzz) if args.suite_fuzz else None,
        read_json(args.loop_report) if args.loop_report else None,
    )
    write_json(Path(args.out) / COMPLETENESS_FILE, record)
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="fuzz each target row in its own campaign store")
    run.add_argument("--manifest", required=True, help="the frozen manifest `sim-suite --frozen --out` wrote")
    run.add_argument("--checkout", required=True, help="the checkout at the manifest's commit")
    run.add_argument("--state", required=True, help="where `campaigns/<target>` stores live")
    run.add_argument("--out", required=True, help="where suite-fuzz.json and the logs go")
    run.add_argument("--target", action="append", default=[], help="run only this target, repeatable")
    run.add_argument("--seconds", type=int, help="each worker's budget, in place of the suite's")
    run.add_argument("--workers", type=int, default=1, help="each campaign run's workers")
    run.add_argument("--jobs", type=int, default=1, help="how many campaigns run at once")
    store = commands.add_parser("archive", help="triage each campaign run's failure inputs, and verify the archive")
    store.add_argument("--suite-fuzz", required=True, help="the suite-fuzz.json `run` wrote")
    store.add_argument("--checkout", required=True, help="the checkout at the manifest's commit")
    store.add_argument("--archive", required=True, help="the triage archive, shared across runs")
    store.add_argument("--out", required=True, help="where archive.json and the logs go")
    store.add_argument("--minimize-runs", type=int, default=MINIMIZE_RUNS,
                       help="the minimizer's runs per confirmed active failure; 0 turns minimization off")
    store.add_argument("--minimize-wall-secs", type=int, default=MINIMIZE_WALL_SECS, help="the minimizer's time limit")
    account = commands.add_parser("completeness", help="diff the manifest's rows against what the routine ran")
    account.add_argument("--manifest", required=True, help="the frozen manifest")
    account.add_argument("--routine", help="the routine.json whose steps ran")
    account.add_argument("--suite-fuzz", help="the suite-fuzz.json `run` wrote, if it ran")
    account.add_argument("--loop-report", help="the loop's report.json, if it wrote one")
    account.add_argument("--out", required=True, help="where completeness.json goes")
    args = parser.parse_args(argv)
    if args.command == "run":
        return run_command(args)
    if args.command == "archive":
        return archive_command(args)
    return completeness_command(args)


if __name__ == "__main__":
    sys.exit(main())
