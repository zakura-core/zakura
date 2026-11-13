#!/usr/bin/env python3
"""Run the mutation loop with its generator lanes on what changed, and diff the report.

`run` reads the previous `report.json`, runs `cargo xtask planted loop` with
`--generate-runs` and `--changed-since` set to the previous report's pinned
revision, and diffs the new report against the previous one. `diff` diffs
two reports without running anything. Both print the diff as JSON, write it
beside the new report as `joint-diff.json`, and exit 1 if it lists anything
in the first four lists:

- `newly_surviving`: lanes, witness or generator, that survived in the new
  report but not in the previous one, or that are new.
- `newly_failing_baselines`: cases whose witness baseline fails in the new
  report but not in the previous one, or that are new.
- `new_property_findings`: what a generator's search on the unpatched build
  stopped at, if the previous report didn't list it.
- `new_generator_baseline_failures`: generator searches on the unpatched
  build that didn't end clean, other than property findings, if the previous
  report didn't list them. Each blocks its harness and seed's lanes, as a
  failing witness baseline blocks its case.
- `untriaged`: every lane of the new report, or of the routine's cumulative
  report, that survived, or that another rule killed, and has no record in
  `qa/native-mutations/triage.toml`.
- `removed_cases`: the cases the routine dropped from its cumulative report,
  because the checkout no longer lists them.

The new report holds only the cases a change since the previous pin
affects, so a case missing from it is unchanged, not fixed. The loop runs
only on the remote host.

`routine` is the droplet timer's run (mutation plan, mut-4.6 and run-2 to
run-4). In order, it:

1. pins the checkout to `origin/main`;
2. `suite-manifest`: writes the frozen suite manifest with
   `cargo xtask sim-suite --frozen`;
3. `suite`: runs the full planted sim suite;
4. `loop`: runs the loop with generator lanes on what changed since the
   last pin, or on every case on its first run;
5. `libfuzzer`: with `--libfuzzer-seconds`, runs the libFuzzer lane on what
   changed since its own pin, `<state>/latest-libfuzzer.json`, which
   advances only when the step succeeds, so a failed step's changes run
   again next time;
6. `suite-fuzz` and `archive`: with `--suite-fuzz` or `--fuzz-seconds`,
   fuzzes each manifest target in its own campaign store under
   `<state>/campaigns/`, then triages the campaigns' failure inputs into
   `<state>/archive/`, with `scripts/suite_fuzz_run.py`;
7. `cases`: lists the loop's cases at the checkout with
   `cargo xtask planted loop --list`;
8. `triage-<lane>`: merges each new report into its cumulative one,
   `<state>/cumulative.json` (`cumulative-libfuzzer.json` for the libFuzzer
   lane), keeps only the listed cases, and applies the checkout's triage
   records to it afresh with `cargo xtask planted triage`. A
   `--changed-since` report holds only the cases it reran, so the
   cumulative report keeps every case's latest result, every property
   finding so far, and each generator search's latest baseline failure. A
   triage-only commit selects no case, so this step is what records its
   triage. The routine then diffs the new report against the previous
   cumulative one, lists the cumulative report's untriaged lanes, and
   writes every lane's diff to `findings.json`;
9. `completeness`: writes `completeness.json`, which lists the manifest rows
   the routine ran, the rows it didn't run and why, and the unimplemented
   rows. It reports coverage only, apart from `findings.json`.

Each run writes under `<state>/runs/<UTC time>-<commit>/`, with each step's
exit status in `routine.json`, and `<state>/latest.json` names the last
pin. `--dry-run` prints the steps without running any.
"""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys

DIFF_FILE = "joint-diff.json"
REPORT_FILE = "report.json"
LATEST_FILE = "latest.json"
LIBFUZZER_LATEST_FILE = "latest-libfuzzer.json"
CUMULATIVE_FILES = {"loop": "cumulative.json", "libfuzzer": "cumulative-libfuzzer.json"}
ROUTINE_FILE = "routine.json"
FINDINGS_FILE = "findings.json"
SUITE_MANIFEST_FILE = "suite-manifest.json"
SUITE_FUZZ_SCRIPT = Path(__file__).resolve().parent / "suite_fuzz_run.py"
NEEDS_TRIAGE = ("survived", "killed-by-other-rule")
SIM_RUSTFLAGS = "--cfg zakura_sim"


def case_key(case):
    """Return one case's identity across reports: its patch, property, protocol table and witness."""
    return (case["patch"], case["property"], case.get("table"), json.dumps(case.get("witness"), sort_keys=True))


def lanes(report):
    """Return every lane's class by its identity: the case's, then `witness` or the generator's harness, seed and mode."""
    found = {}
    for case in report.get("cases", []):
        key = case_key(case)
        found[(*key, "witness")] = case["result"]["class"]
        for lane in case.get("generator", []):
            found[(*key, "generator", lane["harness"], lane["seed"], lane.get("mode", "inproc"))] = lane["result"]["class"]
    return found


def untriaged(report):
    """Return each lane that needs triage and has none, by its identity and class."""
    found = []
    for case in report.get("cases", []):
        key = case_key(case)
        if case["result"]["class"] in NEEDS_TRIAGE and not case.get("triage"):
            found.append({"lane": [*key, "witness"], "class": case["result"]["class"]})
        for lane in case.get("generator", []):
            if lane["result"]["class"] in NEEDS_TRIAGE and not lane.get("triage"):
                identity = [*key, "generator", lane["harness"], lane["seed"], lane.get("mode", "inproc")]
                found.append({"lane": identity, "class": lane["result"]["class"]})
    return found


def finding_key(finding):
    """Return one property finding's identity: the ID it violated, and its harness, seed and mode."""
    return (finding.get("property"), finding["harness"], finding["seed"], finding.get("mode", "inproc"))


def baseline_failure_key(failure):
    """Return one generator baseline failure's identity: its harness, seed and mode."""
    return (failure["harness"], failure["seed"], failure.get("mode", "inproc"))


def searched_pairs(report):
    """Return the identity of every generator search whose lanes `report` holds: its harness, seed and mode."""
    return {
        (lane["harness"], lane["seed"], lane.get("mode", "inproc"))
        for case in report.get("cases", [])
        for lane in case.get("generator", [])
    }


def diff(previous, new, current=None, removed=()):
    """Return what `new` reports that `previous` doesn't, what in `current`, or `new` without it, still needs triage, and the `removed` case keys."""
    before = lanes(previous)
    surviving = [
        {"lane": list(key), "was": before.get(key)}
        for key, kind in sorted(lanes(new).items(), key=lambda item: repr(item[0]))
        if kind == "survived" and before.get(key) != "survived"
    ]
    passed = {case_key(case): case["baseline"]["passed"] for case in previous.get("cases", [])}
    failing = [
        {"case": list(case_key(case)), "detail": case["baseline"].get("detail")}
        for case in new.get("cases", [])
        if not case["baseline"]["passed"] and passed.get(case_key(case), True)
    ]
    known = {finding_key(finding) for finding in previous.get("property_findings", [])}
    findings = [finding for finding in new.get("property_findings", []) if finding_key(finding) not in known]
    blocked = {baseline_failure_key(failure) for failure in previous.get("generator_baseline_failures", [])}
    baseline_failures = [
        failure for failure in new.get("generator_baseline_failures", [])
        if baseline_failure_key(failure) not in blocked
    ]
    return {
        "previous_pinned": previous.get("pinned"),
        "pinned": new.get("pinned"),
        "newly_surviving": surviving,
        "newly_failing_baselines": failing,
        "new_property_findings": findings,
        "new_generator_baseline_failures": baseline_failures,
        "untriaged": untriaged(new if current is None else current),
        "removed_cases": [list(key) for key in removed],
    }


def has_news(changes):
    news = ("newly_surviving", "newly_failing_baselines", "new_property_findings", "new_generator_baseline_failures")
    return any(changes[key] for key in news)


def merge(cumulative, new):
    """Return `cumulative` with `new`'s cases and generator baselines in place of their earlier results, and every property finding of both.

    A `--changed-since` report holds only the cases it reran, so a case
    missing from `new` keeps its last result. Likewise a generator search
    `new` didn't run keeps its baseline failure, and one it ran keeps only
    `new`'s.
    """
    cases = {case_key(case): case for case in cumulative.get("cases", [])}
    cases.update((case_key(case), case) for case in new.get("cases", []))
    findings = {finding_key(finding): finding for finding in cumulative.get("property_findings", [])}
    findings.update((finding_key(finding), finding) for finding in new.get("property_findings", []))
    searched = searched_pairs(new)
    failures = {
        baseline_failure_key(failure): failure
        for failure in cumulative.get("generator_baseline_failures", [])
        if baseline_failure_key(failure) not in searched
    }
    failures.update(
        (baseline_failure_key(failure), failure) for failure in new.get("generator_baseline_failures", [])
    )
    return {
        "pinned": new.get("pinned"),
        "cases": [cases[key] for key in sorted(cases, key=repr)],
        "property_findings": [findings[key] for key in sorted(findings, key=repr)],
        "generator_baseline_failures": [failures[key] for key in sorted(failures, key=repr)],
    }


def prune(report, current):
    """Return `report` with only the cases whose key is in `current`, and the keys of the cases it dropped, sorted."""
    kept = [case for case in report.get("cases", []) if case_key(case) in current]
    removed = sorted((case_key(case) for case in report.get("cases", []) if case_key(case) not in current), key=repr)
    return {**report, "cases": kept}, removed


def read_json(path):
    """Return the JSON at `path`, or `{}` if it's missing."""
    path = Path(path)
    return json.loads(path.read_text()) if path.exists() else {}


def write_diff(changes, out_dir):
    """Print `changes`, and write them to `out_dir`'s diff file."""
    text = json.dumps(changes, indent=2) + "\n"
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    (Path(out_dir) / DIFF_FILE).write_text(text)
    sys.stdout.write(text)


def report_diff(previous_path, new_path):
    """Diff two reports, print the diff, write it beside the new one, and return the exit status."""
    changes = diff(json.loads(Path(previous_path).read_text()), json.loads(Path(new_path).read_text()))
    write_diff(changes, Path(new_path).parent)
    return 1 if has_news(changes) else 0


def loop_command(previous, out, generate_runs, extra):
    """Return the loop's command line: the cases a change since `previous`'s pin affects, or every case without one."""
    since = ["--changed-since", previous["pinned"]] if previous else []
    return [
        "cargo", "xtask", "planted", "loop",
        "--out", str(out),
        "--generate-runs", str(generate_runs),
        *since,
        *extra,
    ]


def read_latest(state, name=LATEST_FILE):
    """Return the last pin `<state>/<name>` records as `{"pinned"}`, or None before the first."""
    path = Path(state) / name
    return json.loads(path.read_text()) if path.exists() else None


def libfuzzer_since(latest, libfuzzer_latest):
    """Return the pin the libFuzzer lane runs on what changed since: its own, else the loop's, else None."""
    for pinned in (libfuzzer_latest, latest):
        if pinned:
            return pinned["pinned"]
    return None


def routine_steps(checkout, state, run_dir, latest, args, libfuzzer_latest=None):
    """Return the routine's steps after the checkout is pinned, as {name, argv, env, cwd} dicts.

    The loop runs on what changed since `latest`, and the libFuzzer lane on
    what changed since its own pin, `libfuzzer_latest`, or the loop's before
    it has one. `cases_step`, each lane's `triage_step` and
    `completeness_step` follow them.
    """
    checkout = str(checkout)
    previous = {"pinned": latest["pinned"]} if latest else None
    sim_env = {"RUSTFLAGS": SIM_RUSTFLAGS}
    manifest = Path(run_dir) / SUITE_MANIFEST_FILE
    steps = [{
        "name": "suite-manifest",
        "argv": ["cargo", "xtask", "sim-suite", "--out", str(manifest), "--frozen"],
        "env": {},
        "cwd": checkout,
    }, {
        "name": "suite",
        "argv": ["cargo", "test", "-p", "zakura-sim", "--profile", "planted", "--locked", "--no-fail-fast"],
        "env": sim_env,
        "cwd": checkout,
    }]
    budget = [flag for harness in args.harness_runs for flag in ("--generate-harness-runs", harness)]
    extra = [arg for arg in args.loop_args if arg != "--"]
    steps.append({
        "name": "loop",
        "argv": loop_command(previous, Path(run_dir) / "loop", args.generate_runs, budget + extra),
        "env": {},
        "cwd": checkout,
    })
    # The libFuzzer lane is opt-in, and runs only on what changed: a first
    # run has no previous pin to bound it.
    since = libfuzzer_since(latest, libfuzzer_latest)
    if args.libfuzzer_seconds and since:
        libfuzzer = ["--generate-mode", "libfuzzer", "--generate-seconds", str(args.libfuzzer_seconds)]
        steps.append({
            "name": "libfuzzer",
            "argv": loop_command({"pinned": since}, Path(run_dir) / "libfuzzer", args.generate_runs, libfuzzer + extra),
            "env": {},
            "cwd": checkout,
        })
    # The suite fuzz is opt-in. Its archive step reads what it ran.
    if args.suite_fuzz or args.fuzz_seconds:
        budget = ["--seconds", str(args.fuzz_seconds)] if args.fuzz_seconds else []
        steps.append({
            "name": "suite-fuzz",
            "argv": [sys.executable, str(SUITE_FUZZ_SCRIPT), "run", "--manifest", str(manifest),
                     "--checkout", checkout, "--state", str(state), "--out", str(run_dir), *budget,
                     "--workers", str(args.fuzz_workers), "--jobs", str(args.fuzz_jobs)],
            "env": {},
            "cwd": checkout,
        })
        steps.append({
            "name": "archive",
            "argv": [sys.executable, str(SUITE_FUZZ_SCRIPT), "archive",
                     "--suite-fuzz", str(Path(run_dir) / "suite-fuzz.json"), "--checkout", checkout,
                     "--archive", str(Path(state) / "archive"), "--out", str(run_dir)],
            "env": {},
            "cwd": checkout,
        })
    return steps


def completeness_step(checkout, run_dir, steps):
    """Return the step that writes `completeness.json` from what `steps` wrote and `routine.json` records."""
    run_dir = Path(run_dir)
    names = {step["name"] for step in steps}
    inputs = ["--routine", str(run_dir / ROUTINE_FILE)]
    if "suite-fuzz" in names:
        inputs += ["--suite-fuzz", str(run_dir / "suite-fuzz.json")]
    if "loop" in names:
        inputs += ["--loop-report", str(run_dir / "loop" / REPORT_FILE)]
    return {
        "name": "completeness",
        "argv": [sys.executable, str(SUITE_FUZZ_SCRIPT), "completeness",
                 "--manifest", str(run_dir / SUITE_MANIFEST_FILE), *inputs, "--out", str(run_dir)],
        "env": {},
        "cwd": str(checkout),
    }


def cases_step(checkout, args):
    """Return the step that prints the loop's current cases at the checkout, selected by the loop's own flags."""
    extra = [arg for arg in args.loop_args if arg != "--"]
    return {
        "name": "cases",
        "argv": ["cargo", "xtask", "planted", "loop", "--list", *extra],
        "env": {},
        "cwd": str(checkout),
    }


def current_cases(step, run_dir):
    """Run `step`, which prints cases as JSON, and return their keys, or None if it fails.

    Its standard error goes to `<run_dir>/<name>.log`.
    """
    with open(Path(run_dir) / f"{step['name']}.log", "w") as log:
        ended = subprocess.run(
            step["argv"], cwd=step["cwd"], env={**os.environ, **step["env"]},
            stdout=subprocess.PIPE, stderr=log, text=True, check=False,
        )
    if ended.returncode != 0:
        return None
    try:
        return {case_key(case) for case in json.loads(ended.stdout)}
    except (ValueError, KeyError, TypeError):
        return None


def triage_step(checkout, lane, cumulative_path):
    """Return the step that applies the checkout's triage records afresh to a lane's cumulative report, in place."""
    return {
        "name": f"triage-{lane}",
        "argv": ["cargo", "xtask", "planted", "triage", "--report", str(cumulative_path)],
        "env": {},
        "cwd": str(checkout),
    }


def pin(checkout):
    """Fetch `origin/main` into `checkout`, detach it there, and return the commit."""
    git = ["git", "-C", str(checkout)]
    subprocess.run([*git, "fetch", "--quiet", "origin", "main"], check=True)
    subprocess.run([*git, "checkout", "--quiet", "--detach", "origin/main"], check=True)
    return subprocess.run([*git, "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout.strip()


def run_step(step, run_dir):
    """Run one step with its output in `<run_dir>/<name>.log`, and return its exit status."""
    with open(Path(run_dir) / f"{step['name']}.log", "w") as log:
        ended = subprocess.run(
            step["argv"], cwd=step["cwd"], env={**os.environ, **step["env"]},
            stdout=log, stderr=subprocess.STDOUT, check=False,
        )
    return ended.returncode


def routine(args):
    """Run the droplet timer's routine once, and return its exit status."""
    state = Path(args.state).resolve()
    checkout = Path(args.checkout).resolve()
    latest = read_latest(state)
    libfuzzer_latest = read_latest(state, LIBFUZZER_LATEST_FILE)
    if args.dry_run:
        run_dir = state / "runs" / "<UTC time>-<commit>"
        steps = routine_steps(checkout, state, run_dir, latest, args, libfuzzer_latest)
        cases = cases_step(checkout, args)
        triage = [triage_step(checkout, lane, state / name) for lane, name in CUMULATIVE_FILES.items()]
        completeness = completeness_step(checkout, run_dir, steps)
        plan = {
            "latest": latest, "libfuzzer_latest": libfuzzer_latest, "pin": "origin/main", "steps": steps,
            "cases": cases, "triage": triage, "completeness": completeness,
            "order": [step["name"] for step in [*steps, cases, *triage, completeness]],
        }
        sys.stdout.write(json.dumps(plan, indent=2) + "\n")
        return 0
    pinned = pin(checkout)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = state / "runs" / f"{stamp}-{pinned[:12]}"
    run_dir.mkdir(parents=True)
    ended = {}
    steps = routine_steps(checkout, state, run_dir, latest, args, libfuzzer_latest)
    for step in steps:
        ended[step["name"]] = run_step(step, run_dir)
    record = {
        "pinned": pinned, "previous": latest, "previous_libfuzzer": libfuzzer_latest,
        "steps": ended, "news": {},
    }
    status = 0 if all(code == 0 for code in ended.values()) else 1
    # The cumulative reports keep only the checkout's current cases, so a
    # removed case, or one a witness change re-keyed, leaves them.
    current = current_cases(cases_step(checkout, args), run_dir)
    ended["cases"] = 0 if current is not None else 1
    if current is None:
        print("the loop's case list failed; the cumulative reports keep every case")
        status = 1
    findings = {}
    for lane, cumulative_file in CUMULATIVE_FILES.items():
        report = run_dir / lane / REPORT_FILE
        cumulative_path = state / cumulative_file
        if not report.exists() and not cumulative_path.exists():
            continue
        cumulative = read_json(cumulative_path)
        # A run that selected no case, such as one after a triage-only
        # commit, writes no report, and the cumulative report still takes
        # the checkout's triage.
        new = read_json(report) if report.exists() else {"pinned": cumulative.get("pinned"), "cases": []}
        merged, removed = merge(cumulative, new), []
        if current is not None:
            merged, removed = prune(merged, current)
        cumulative_path.write_text(json.dumps(merged, indent=2) + "\n")
        step = triage_step(checkout, lane, cumulative_path)
        ended[step["name"]] = run_step(step, run_dir)
        status = status or int(ended[step["name"]] != 0)
        changes = diff(cumulative, new, read_json(cumulative_path), removed)
        write_diff(changes, run_dir / lane)
        findings[lane] = changes
        record["news"][lane] = has_news(changes)
        status = status or int(record["news"][lane])
    (run_dir / FINDINGS_FILE).write_text(json.dumps(findings, indent=2) + "\n")
    if ended.get("loop") == 0 or (run_dir / "loop" / REPORT_FILE).exists():
        (state / LATEST_FILE).write_text(json.dumps({"pinned": pinned, "run": str(run_dir)}, indent=2) + "\n")
    else:
        print(f"the loop failed without a report; {LATEST_FILE} stays")
    # The libFuzzer lane keeps its own pin, which advances only when its
    # step succeeds, so the next run covers a failed run's changes again.
    if "libfuzzer" in ended:
        if ended["libfuzzer"] == 0:
            libfuzzer_pin = {"pinned": pinned, "run": str(run_dir)}
        else:
            print(f"the libFuzzer lane failed; its pin stays in {LIBFUZZER_LATEST_FILE}")
            libfuzzer_pin = libfuzzer_latest or {"pinned": libfuzzer_since(latest, None)}
        (state / LIBFUZZER_LATEST_FILE).write_text(json.dumps(libfuzzer_pin, indent=2) + "\n")
    # The completeness step reads the steps that ran from routine.json.
    (run_dir / ROUTINE_FILE).write_text(json.dumps(record, indent=2) + "\n")
    ended["completeness"] = run_step(completeness_step(checkout, run_dir, steps), run_dir)
    (run_dir / ROUTINE_FILE).write_text(json.dumps(record, indent=2) + "\n")
    return status or int(ended["completeness"] != 0)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="run the loop on what changed since the previous report, then diff")
    run.add_argument("--previous", required=True, help="the previous run's report.json")
    run.add_argument("--out", required=True, help="the new run's --out, which must be missing or empty")
    run.add_argument("--generate-runs", type=int, default=2000, help="each inproc search's runs")
    run.add_argument("loop_args", nargs=argparse.REMAINDER, help="more `planted loop` flags, after `--`")
    compare = commands.add_parser("diff", help="diff two reports")
    compare.add_argument("previous")
    compare.add_argument("new")
    timer = commands.add_parser("routine", help="the droplet timer's run: pin origin/main, suite, loop, diff")
    timer.add_argument("--checkout", required=True, help="the checkout the routine pins and builds")
    timer.add_argument("--state", required=True, help="where runs and latest.json live")
    timer.add_argument("--generate-runs", type=int, default=20000, help="each inproc search's runs")
    timer.add_argument("--harness-runs", action="append", default=[], metavar="HARNESS=RUNS",
                       help="a harness's own inproc runs, repeatable")
    timer.add_argument("--libfuzzer-seconds", type=int, default=0,
                       help="also run the libFuzzer lane on changed cases for this long per seed; 0 turns it off")
    timer.add_argument("--suite-fuzz", action="store_true",
                       help="also fuzz each suite manifest target for its suite budget, then archive its failures")
    timer.add_argument("--fuzz-seconds", type=int, default=0,
                       help="fuzz each target for this long per worker instead; nonzero also turns the suite fuzz on")
    timer.add_argument("--fuzz-workers", type=int, default=1, help="each suite-fuzz campaign run's workers")
    timer.add_argument("--fuzz-jobs", type=int, default=1, help="how many suite-fuzz campaigns run at once")
    timer.add_argument("--dry-run", action="store_true", help="print the steps without running any")
    timer.add_argument("loop_args", nargs=argparse.REMAINDER, help="more `planted loop` flags, after `--`")
    args = parser.parse_args(argv)
    if args.command == "diff":
        return report_diff(args.previous, args.new)
    if args.command == "routine":
        return routine(args)
    previous = json.loads(Path(args.previous).read_text())
    extra = [arg for arg in args.loop_args if arg != "--"]
    subprocess.run(loop_command(previous, args.out, args.generate_runs, extra), check=True)
    new = Path(args.out) / REPORT_FILE
    if not new.exists():
        print(f"no case changed since {previous['pinned']}; nothing to diff")
        return 0
    return report_diff(args.previous, new)


if __name__ == "__main__":
    sys.exit(main())
