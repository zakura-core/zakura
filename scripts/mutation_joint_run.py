#!/usr/bin/env python3
"""Run the mutation loop with its generator lanes on what changed, and diff the report.

`run` reads the previous `report.json`, runs `cargo xtask planted loop` with
`--generate-runs` and `--changed-since` set to the previous report's pinned
revision, and diffs the new report against the previous one. `diff` diffs
two reports without running anything. Both print the diff as JSON, write it
beside the new report as `joint-diff.json`, and exit 1 if it lists anything:

- `newly_surviving`: lanes, witness or generator, that survived in the new
  report but not in the previous one, or that are new.
- `newly_failing_baselines`: cases whose witness baseline fails in the new
  report but not in the previous one, or that are new.
- `new_property_findings`: what a generator's search on the unpatched build
  stopped at, if the previous report didn't list it.

The new report holds only the cases a change since the previous pin
affects, so a case missing from it is unchanged, not fixed. The loop runs
only on the remote host.
"""

import argparse
import json
from pathlib import Path
import subprocess
import sys

DIFF_FILE = "joint-diff.json"
REPORT_FILE = "report.json"


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


def finding_key(finding):
    """Return one property finding's identity: the ID it violated, and its harness, seed and mode."""
    return (finding.get("property"), finding["harness"], finding["seed"], finding.get("mode", "inproc"))


def diff(previous, new):
    """Return what `new` reports that `previous` doesn't: surviving lanes, failing baselines and property findings."""
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
    }


def has_news(changes):
    return any(changes[key] for key in ("newly_surviving", "newly_failing_baselines", "new_property_findings"))


def report_diff(previous_path, new_path):
    """Diff two reports, print the diff, write it beside the new one, and return the exit status."""
    changes = diff(json.loads(Path(previous_path).read_text()), json.loads(Path(new_path).read_text()))
    text = json.dumps(changes, indent=2) + "\n"
    (Path(new_path).parent / DIFF_FILE).write_text(text)
    sys.stdout.write(text)
    return 1 if has_news(changes) else 0


def loop_command(previous, out, generate_runs, extra):
    """Return the loop's command line for the cases a change since `previous`'s pin affects."""
    return [
        "cargo", "xtask", "planted", "loop",
        "--out", str(out),
        "--generate-runs", str(generate_runs),
        "--changed-since", previous["pinned"],
        *extra,
    ]


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
    args = parser.parse_args(argv)
    if args.command == "diff":
        return report_diff(args.previous, args.new)
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
