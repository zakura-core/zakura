#!/usr/bin/env python3
"""Run the mutation loop with its generator lanes on what changed, and diff the report.

`run` reads the previous `report.json`, runs `cargo xtask planted loop` with
`--generate-runs` and `--changed-since` set to the previous report's pinned
revision, and diffs the new report against the previous one. `diff` diffs
two reports without running anything. Both print the diff as JSON, write it
beside the new report as `joint-diff.json`, and exit 1 if it lists anything
in the first three lists:

- `newly_surviving`: lanes, witness or generator, that survived in the new
  report but not in the previous one, or that are new.
- `newly_failing_baselines`: cases whose witness baseline fails in the new
  report but not in the previous one, or that are new.
- `new_property_findings`: what a generator's search on the unpatched build
  stopped at, if the previous report didn't list it.
- `untriaged`: every lane of the new report that survived, or that another
  rule killed, and has no record in `qa/native-mutations/triage.toml`.

The new report holds only the cases a change since the previous pin
affects, so a case missing from it is unchanged, not fixed. The loop runs
only on the remote host.

`routine` is the droplet timer's run (mutation plan, mut-4.6). It pins the
checkout to `origin/main`, runs the full planted sim suite, runs the loop
with generator lanes on what changed since the last pin (every case on its
first run), and optionally runs the libFuzzer lane on the same cases. A
`--changed-since` report holds only the cases it reran, so the routine
diffs each new report against a cumulative one, `<state>/cumulative.json`
(`cumulative-libfuzzer.json` for the libFuzzer lane): every case's latest
result and every property finding so far. It then merges the new report
into it. Each run writes under `<state>/runs/<UTC time>-<commit>/`, and
`<state>/latest.json` names the last pin. `--dry-run` prints the steps
without running any.
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
CUMULATIVE_FILES = {"loop": "cumulative.json", "libfuzzer": "cumulative-libfuzzer.json"}
ROUTINE_FILE = "routine.json"
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


def diff(previous, new):
    """Return what `new` reports that `previous` doesn't, and what in `new` still needs triage."""
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
    return {
        "previous_pinned": previous.get("pinned"),
        "pinned": new.get("pinned"),
        "newly_surviving": surviving,
        "newly_failing_baselines": failing,
        "new_property_findings": findings,
        "untriaged": untriaged(new),
    }


def has_news(changes):
    return any(changes[key] for key in ("newly_surviving", "newly_failing_baselines", "new_property_findings"))


def merge(cumulative, new):
    """Return `cumulative` with `new`'s cases in place of their earlier results, and every property finding of both.

    A `--changed-since` report holds only the cases it reran, so a case
    missing from `new` keeps its last result.
    """
    cases = {case_key(case): case for case in cumulative.get("cases", [])}
    cases.update((case_key(case), case) for case in new.get("cases", []))
    findings = {finding_key(finding): finding for finding in cumulative.get("property_findings", [])}
    findings.update((finding_key(finding), finding) for finding in new.get("property_findings", []))
    return {
        "pinned": new.get("pinned"),
        "cases": [cases[key] for key in sorted(cases, key=repr)],
        "property_findings": [findings[key] for key in sorted(findings, key=repr)],
    }


def read_json(path):
    """Return the JSON at `path`, or `{}` if it's missing."""
    path = Path(path)
    return json.loads(path.read_text()) if path.exists() else {}


def write_diff(previous, new_path):
    """Diff `previous` against the report at `new_path`, print the diff, write it beside the report, and return it."""
    changes = diff(previous, json.loads(Path(new_path).read_text()))
    text = json.dumps(changes, indent=2) + "\n"
    (Path(new_path).parent / DIFF_FILE).write_text(text)
    sys.stdout.write(text)
    return changes


def report_diff(previous_path, new_path):
    """Diff two reports, print the diff, write it beside the new one, and return the exit status."""
    changes = write_diff(json.loads(Path(previous_path).read_text()), new_path)
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


def read_latest(state):
    """Return the last routine's `{"pinned"}`, or None before the first report."""
    path = Path(state) / LATEST_FILE
    return json.loads(path.read_text()) if path.exists() else None


def routine_steps(checkout, run_dir, latest, args):
    """Return the routine's steps after the checkout is pinned, as {name, argv, env, cwd} dicts."""
    checkout = str(checkout)
    previous = {"pinned": latest["pinned"]} if latest else None
    sim_env = {"RUSTFLAGS": SIM_RUSTFLAGS}
    steps = [{
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
    if args.libfuzzer_seconds and previous:
        libfuzzer = ["--generate-mode", "libfuzzer", "--generate-seconds", str(args.libfuzzer_seconds)]
        steps.append({
            "name": "libfuzzer",
            "argv": loop_command(previous, Path(run_dir) / "libfuzzer", args.generate_runs, libfuzzer + extra),
            "env": {},
            "cwd": checkout,
        })
    return steps


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
    if args.dry_run:
        run_dir = state / "runs" / "<UTC time>-<commit>"
        steps = routine_steps(checkout, run_dir, latest, args)
        sys.stdout.write(json.dumps({"latest": latest, "pin": "origin/main", "steps": steps}, indent=2) + "\n")
        return 0
    pinned = pin(checkout)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = state / "runs" / f"{stamp}-{pinned[:12]}"
    run_dir.mkdir(parents=True)
    ended = {}
    for step in routine_steps(checkout, run_dir, latest, args):
        ended[step["name"]] = run_step(step, run_dir)
    record = {"pinned": pinned, "previous": latest, "steps": ended, "news": {}}
    status = 0 if all(code == 0 for code in ended.values()) else 1
    for lane, cumulative_file in CUMULATIVE_FILES.items():
        report = run_dir / lane / REPORT_FILE
        if not report.exists():
            continue
        cumulative_path = state / cumulative_file
        cumulative = read_json(cumulative_path)
        changes = write_diff(cumulative, report)
        record["news"][lane] = has_news(changes)
        status = status or int(record["news"][lane])
        merged = merge(cumulative, json.loads(report.read_text()))
        cumulative_path.write_text(json.dumps(merged, indent=2) + "\n")
    if ended.get("loop") == 0 or (run_dir / "loop" / REPORT_FILE).exists():
        (state / LATEST_FILE).write_text(json.dumps({"pinned": pinned, "run": str(run_dir)}, indent=2) + "\n")
    else:
        print(f"the loop failed without a report; {LATEST_FILE} stays")
    (run_dir / ROUTINE_FILE).write_text(json.dumps(record, indent=2) + "\n")
    return status


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
