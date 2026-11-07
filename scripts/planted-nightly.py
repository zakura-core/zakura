#!/usr/bin/env python3
"""Plans and reports the nightly `zakura_sim planted` shards in tests-unit.yml.

`base --run-id <id>` prints the commit that the nightly's change filter starts
from. That is the head of the newest scheduled tests-unit.yml run on main that
completed with success or failure, other than run `<id>`. If GitHub doesn't
name one, or the checkout doesn't hold it, the script prints main as of
FALLBACK_DAYS before now instead: the weekly rotation ran every case since
then.

`report --full <index> --run-id <id> --run-attempt <n>` keeps one failure issue
per shard. A shard job that failed or was cancelled opens its shard's issue, or
comments on it. Only a passing job that ran the whole shard, the `--full` one,
closes the issue, because only that job reran every case in the shard. A job
that ran only the shard's changed cases can't close it.
"""

import argparse
import datetime
import json
import re
import subprocess
import sys

COUNT = 7
WORKFLOW = "tests-unit.yml"
FALLBACK_DAYS = 7
JOB = re.compile(r"^zakura_sim planted shard (\d+)/(\d+)$")
FAILED = {"failure", "cancelled", "timed_out"}


def gh(*args):
    """Runs `gh` with `args` and returns its standard output."""
    return subprocess.run(
        ["gh", *args], check=True, capture_output=True, text=True
    ).stdout


def git(*args):
    """Runs `git` with `args`, and returns its exit code and standard output."""
    done = subprocess.run(["git", *args], capture_output=True, text=True)
    return done.returncode, done.stdout.strip()


def previous_nightly(runs, run_id):
    """The head commit of the newest run in `runs` other than `run_id` that
    ended in success or failure, or None. `runs` is newest first."""
    for run in runs:
        if run["databaseId"] != run_id and run["conclusion"] in ("success", "failure"):
            return run["headSha"]
    return None


def fallback(now):
    """Main's first-parent commit as of FALLBACK_DAYS before `now`, or the
    root commit if the history is shorter."""
    before = (now - datetime.timedelta(days=FALLBACK_DAYS)).isoformat()
    _, commit = git("rev-list", "-1", "--first-parent", f"--before={before}", "HEAD")
    if commit:
        return commit
    _, roots = git("rev-list", "--max-parents=0", "HEAD")
    return roots.splitlines()[0]


def base(run_id, run_gh=gh, now=None):
    """The commit the nightly's change filter starts from."""
    now = now or datetime.datetime.now(datetime.timezone.utc)
    try:
        runs = json.loads(
            run_gh(
                "run", "list", "--workflow", WORKFLOW, "--event", "schedule",
                "--branch", "main", "--status", "completed", "--limit", "20",
                "--json", "databaseId,headSha,conclusion",
            )
        )
        commit = previous_nightly(runs, run_id)
        if commit is None:
            print("no earlier nightly run completed", file=sys.stderr)
        elif git("merge-base", "--is-ancestor", commit, "HEAD")[0] != 0:
            print(f"the checkout's history doesn't hold {commit}", file=sys.stderr)
            commit = None
    except (subprocess.CalledProcessError, ValueError, KeyError) as error:
        print(f"can't list the earlier nightly runs: {error}", file=sys.stderr)
        commit = None
    if commit is None:
        commit = fallback(now)
        print(f"falling back to {commit}", file=sys.stderr)
    return commit


def outcomes(jobs):
    """Maps each shard to its job's conclusion, from the run's `jobs`."""
    shards = {}
    for job in jobs:
        match = JOB.match(job["name"])
        if match and int(match[2]) == COUNT:
            shards[int(match[1])] = job["conclusion"]
    return shards


def actions(shards, full):
    """What to do with each shard's issue: `failed` for a job that failed or
    was cancelled, `recovered` for the full shard's passing job. A changed
    cases job that passed, or a skipped job, leaves the issue as it is."""
    plan = []
    for shard, conclusion in sorted(shards.items()):
        if conclusion in FAILED:
            plan.append((shard, "failed"))
        elif conclusion == "success" and shard == full:
            plan.append((shard, "recovered"))
    return plan


def title(shard):
    """The failure issue's title for `shard`."""
    return f"[nightly] zakura_sim planted shard {shard}/{COUNT} failed"


def report(full, run_id, run_attempt, run_url, run_gh=gh):
    """Opens, comments on or closes each shard's failure issue."""
    pages = json.loads(
        run_gh(
            "api", "--paginate", "--slurp",
            f"repos/{{owner}}/{{repo}}/actions/runs/{run_id}/attempts/{run_attempt}/jobs",
        )
    )
    shards = outcomes(job for page in pages for job in page["jobs"])
    if full not in shards:
        raise SystemExit(f"this run has no job for the full shard {full}/{COUNT}")
    open_issues = json.loads(
        run_gh(
            "issue", "list", "--state", "open", "--limit", "100",
            "--search", '"zakura_sim planted shard" in:title',
            "--json", "number,title",
        )
    )
    numbers = {issue["title"]: str(issue["number"]) for issue in open_issues}
    for shard, action in actions(shards, full):
        number = numbers.get(title(shard))
        what = "the whole shard" if shard == full else "its changed cases"
        if action == "failed":
            message = f"Shard {shard}/{COUNT} failed on {what}: {run_url}"
            if number:
                run_gh("issue", "comment", number, "--body", message)
            else:
                body = (
                    f"{message}\n\nOnly a nightly run of the whole shard that "
                    "passes closes this issue."
                )
                run_gh("issue", "create", "--title", title(shard), "--body", body)
        elif number:
            run_gh(
                "issue", "comment", number, "--body",
                f"Shard {shard}/{COUNT} ran in full and passed: {run_url}",
            )
            run_gh("issue", "close", number, "--reason", "completed")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    base_command = commands.add_parser("base")
    base_command.add_argument("--run-id", type=int, required=True)
    report_command = commands.add_parser("report")
    report_command.add_argument("--full", type=int, required=True)
    report_command.add_argument("--run-id", type=int, required=True)
    report_command.add_argument("--run-attempt", type=int, required=True)
    report_command.add_argument("--run-url", required=True)
    args = parser.parse_args(argv)
    if args.command == "base":
        print(base(args.run_id))
    else:
        report(args.full, args.run_id, args.run_attempt, args.run_url)


if __name__ == "__main__":
    main()
