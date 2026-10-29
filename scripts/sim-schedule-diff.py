#!/usr/bin/env python3
"""Compare the simulator schedules of two revisions over saved fixtures.

It checks that a change adds no scheduling point or draw to a harness. It
builds `zakura-sim` with `RUSTFLAGS="--cfg zakura_sim"` in a detached base
and head worktree (see `lib/revision_worktrees.py`), records each fixture
on both with `zakura-sim record --sim <name> --input <fixture> --out <dir>`,
and compares the traces' recorded choices: the `init` draws, then each
step's pick, key, option count, `by`, offset and draws. Observation digests
(`obs`), the `meta` record and the `end` record aren't compared.

The fixture glob is matched in the head worktree, and both sides record
the same fixture files, so a fixture the change re-records still runs the
same input on both.

Usage:
  sim-schedule-diff.py --base <rev> --head <rev>
      [--sim header-sync/wire] [--fixtures <glob>]
      [--work-dir <dir>] [--cargo <cargo>]

Exit status: 0 when every trace's choices match; 1 when any differ, a run
ends in a harness failure, or a base fixture is missing from head; 2 when
the glob matches no fixture; 3 when a command fails.
"""

import argparse
import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "lib"))
import revision_worktrees as rw  # noqa: E402

DEFAULT_SIM = "header-sync/wire"
DEFAULT_FIXTURES = "crates/zakura-sim/tests/fixtures/header-wire-*.bin"
SIM_RUSTFLAGS = "--cfg zakura_sim"
EXIT_SAME, EXIT_DIFFERS, EXIT_NO_FIXTURES = 0, 1, 2


def choices(lines):
    """A trace's recorded choices, from its JSON lines.

    Returns the `init` draws followed by every step record without its
    `obs` digest, in order.
    """
    out = []
    for line in lines:
        if not line.strip():
            continue
        record = json.loads(line)
        if "init" in record:
            out.append({"init": record["init"].get("draws", [])})
        elif "step" in record:
            out.append({key: value for key, value in record.items() if key != "obs"})
    return out


def end_outcome(lines):
    """The outcome a trace's `end` record names, or None when it has none."""
    for line in reversed(lines):
        if line.strip():
            record = json.loads(line)
            return record["end"].get("outcome") if "end" in record else None
    return None


def compare_traces(base_lines, head_lines):
    """None when two traces' choices match, else a description of the first difference.

    A trace without an `end` record, or whose run ended in a harness
    failure, fails too, since its choices stop short of a whole run.
    """
    for side, lines in (("base", base_lines), ("head", head_lines)):
        outcome = end_outcome(lines)
        if outcome is None or outcome == "harness":
            return "%s run %s" % (side, "has no end record" if outcome is None else "ended in a harness failure")
    base, head = choices(base_lines), choices(head_lines)
    for index, (b, h) in enumerate(zip(base, head)):
        if b != h:
            what = "step %d" % b["step"] if "step" in b else "record %d" % (index + 1)
            return "%s differs:\n    base %s\n    head %s" % (
                what, json.dumps(b, sort_keys=True), json.dumps(h, sort_keys=True))
    if len(base) != len(head):
        return "base has %d choice records, head %d" % (len(base), len(head))
    return None


def find_fixtures(base_tree, head_tree, pattern):
    """The head's fixtures matching `pattern`, and the names of base fixtures head lacks.

    Removing a fixture would quietly shrink what the gate checks, so a
    fixture only base has is reported as missing.
    """
    head = sorted(head_tree.glob(pattern))
    names = {path.relative_to(head_tree) for path in head}
    missing = sorted(str(path.relative_to(base_tree)) for path in base_tree.glob(pattern)
                     if path.relative_to(base_tree) not in names)
    return head, missing


def build_sim(cargo, tree, target):
    """Builds the simulator `zakura-sim` binary in `tree`, and returns its path."""
    print("== %s: zakura-sim" % tree.name, flush=True)
    rw.run(
        [cargo, "build", "--locked", "-p", "zakura-sim", "--bin", "zakura-sim"],
        cwd=tree, env=rw.cargo_env(target, {"RUSTFLAGS": SIM_RUSTFLAGS}),
    )
    return target / "debug" / "zakura-sim"


def record(binary, tree, sim, fixture, out):
    """Records `fixture` on `sim` with `binary`, and returns the trace's lines."""
    shutil.rmtree(out, ignore_errors=True)
    out.parent.mkdir(parents=True, exist_ok=True)
    rw.run([binary, "record", "--sim", sim, "--input", fixture, "--out", out], cwd=tree, capture=True)
    return (out / "trace.jsonl").read_text().splitlines()


def parse_args(argv):
    """Parses the command line."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--base", required=True, help="the base revision")
    parser.add_argument("--head", required=True, help="the head revision")
    parser.add_argument("--sim", default=DEFAULT_SIM, help="the harness (default: %s)" % DEFAULT_SIM)
    parser.add_argument("--fixtures", default=DEFAULT_FIXTURES,
                        help="a fixture glob, relative to the repository root (default: %s)" % DEFAULT_FIXTURES)
    parser.add_argument("--work-dir", help="keep the worktrees' build caches here for reuse (default: a temporary directory)")
    parser.add_argument("--cargo", default="cargo", help="the cargo command (default: cargo)")
    parser.add_argument("--repo", default=".", help="the repository (default: the current one)")
    return parser.parse_args(argv)


def main(argv=None):
    """Runs the comparison, and returns the exit status."""
    args = parse_args(argv)
    status = EXIT_SAME
    try:
        repo = rw.repo_root(args.repo)
        base, head = rw.resolve(repo, args.base), rw.resolve(repo, args.head)
        print("base %s, head %s" % (base, head), flush=True)
        with rw.Worktrees(repo, base, head, args.work_dir) as trees:
            fixtures, missing = find_fixtures(trees.tree("base"), trees.tree("head"), args.fixtures)
            if not fixtures:
                print("error: %s matches no fixture" % args.fixtures, file=sys.stderr)
                return EXIT_NO_FIXTURES
            for name in missing:
                print("%s: only in base" % name)
                status = EXIT_DIFFERS
            binaries = {side: build_sim(args.cargo, trees.tree(side), trees.target(side))
                        for side in ("base", "head")}
            for fixture in fixtures:
                traces = {
                    side: record(binaries[side], trees.tree(side), args.sim, fixture,
                                 trees.work / "traces" / side / fixture.stem)
                    for side in ("base", "head")
                }
                difference = compare_traces(traces["base"], traces["head"])
                steps = sum(1 for line in traces["head"] if line.startswith('{"step"'))
                print("%s: %s" % (fixture.name, difference or "identical (%d steps)" % steps), flush=True)
                if difference:
                    status = EXIT_DIFFERS
    except rw.CommandFailed as error:
        print("error: %s" % error, file=sys.stderr)
        return rw.EXIT_COMMAND_FAILED
    except (OSError, ValueError) as error:
        print("error: %s" % error, file=sys.stderr)
        return rw.EXIT_COMMAND_FAILED
    print("%d fixtures: %s" % (len(fixtures), "all identical" if status == EXIT_SAME else "differences found"))
    return status


if __name__ == "__main__":
    sys.exit(main())
