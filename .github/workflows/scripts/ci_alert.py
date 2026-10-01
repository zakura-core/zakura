#!/usr/bin/env python3
"""Post failures and recoveries of watched workflows on `main` to Slack.

`ci-alerts.yml` runs this after a watched workflow completes on `main`. A push
failure is posted only when the previous push run did not fail, so a broken
`main` is reported once rather than on every merge. Every scheduled failure is
posted, because nothing else surfaces those runs. A pass that follows a failure
is posted as a recovery. A run that a newer run of the same workflow and event
has already superseded is not reported.
"""

import argparse
import json
import os
import subprocess
import urllib.request


FAILED = {"failure", "startup_failure", "timed_out"}
# Conclusions that say nothing about the workflow's health.
UNDECIDED = {"cancelled", "skipped", "stale"}
MAX_LISTED_JOBS = 8
MAX_TITLE_CHARS = 100


def api(path):
    """Returns the decoded JSON of a GitHub REST API `GET` request, made through `gh`."""
    output = subprocess.run(
        ["gh", "api", path], check=True, capture_output=True, text=True
    ).stdout
    return json.loads(output)


def previous_conclusion(run, history, previous_attempt_conclusion):
    """Returns the conclusion `run` follows, or `None` when nothing came before it.

    A re-run follows its own earlier attempt. Otherwise the run follows the
    newest earlier run in `history` that reached a verdict.
    """
    if run["run_attempt"] > 1:
        return previous_attempt_conclusion
    earlier = [
        other
        for other in history
        if other["run_number"] < run["run_number"]
        and other["conclusion"] not in UNDECIDED
    ]
    if not earlier:
        return None
    return max(earlier, key=lambda other: other["run_number"])["conclusion"]


def superseded(run, history):
    """Returns whether a newer run in `history` has already reached a verdict."""
    return any(
        other["run_number"] > run["run_number"]
        and other["conclusion"] not in UNDECIDED
        for other in history
    )


def alert_kind(run, previous, is_superseded):
    """Returns `"failure"`, `"recovery"`, or `None` for a completed run."""
    if is_superseded or run["conclusion"] in UNDECIDED:
        return None
    if run["conclusion"] in FAILED:
        if run["event"] == "schedule" or previous not in FAILED:
            return "failure"
        return None
    if run["conclusion"] == "success" and previous in FAILED:
        return "recovery"
    return None


def failed_jobs(jobs):
    """Returns the names of the failed jobs, in the order GitHub lists them.

    A workflow's `... success` summary job fails whenever another job does, so it
    is left out unless it is the only failure.
    """
    failed = [job["name"] for job in jobs if job["conclusion"] in FAILED]
    return [name for name in failed if not name.endswith(" success")] or failed


def escape(text):
    """Escapes the characters that Slack message markup reserves."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def message(kind, run, jobs):
    """Formats the Slack text for a failure or recovery of `run`."""
    workflow = escape(run["name"])
    trigger = "scheduled run" if run["event"] == "schedule" else "push"
    link = f"<{run['html_url']}|run {run['run_number']}>"
    if kind == "recovery":
        return f":large_green_circle: *{workflow}* is passing again on `main` ({trigger}): {link}"

    lines = [f":red_circle: *{workflow}* failed on `main` ({trigger}): {link}"]
    if jobs:
        listed = ", ".join(f"`{escape(name)}`" for name in jobs[:MAX_LISTED_JOBS])
        hidden = len(jobs) - MAX_LISTED_JOBS
        lines.append(f"Failed jobs: {listed}" + (f" and {hidden} more" if hidden > 0 else ""))
    commit = run.get("head_commit")
    if run["event"] == "push" and commit:
        title = (commit.get("message") or "").splitlines()[:1] or [""]
        title = escape(title[0][:MAX_TITLE_CHARS])
        author = escape((commit.get("author") or {}).get("name") or "unknown author")
        url = f"{run['repository']['html_url']}/commit/{run['head_sha']}"
        lines.append(f"Commit: <{url}|{run['head_sha'][:9]}> {title} ({author})")
    return "\n".join(lines)


def alert_text(repository, run_id):
    """Returns the Slack text for a completed run, or `None` when it needs no alert."""
    base = f"repos/{repository}/actions"
    run = api(f"{base}/runs/{run_id}")
    history = api(
        f"{base}/workflows/{run['workflow_id']}/runs"
        f"?branch=main&event={run['event']}&status=completed&per_page=20"
    )["workflow_runs"]
    previous_attempt = None
    if run["run_attempt"] > 1:
        previous_attempt = api(f"{base}/runs/{run_id}/attempts/{run['run_attempt'] - 1}")[
            "conclusion"
        ]

    kind = alert_kind(
        run,
        previous_conclusion(run, history, previous_attempt),
        superseded(run, history),
    )
    if kind is None:
        return None
    jobs = []
    if kind == "failure":
        jobs = failed_jobs(
            api(f"{base}/runs/{run_id}/attempts/{run['run_attempt']}/jobs?per_page=100")["jobs"]
        )
    return message(kind, run, jobs)


def post(webhook, text):
    """Posts `text` to a Slack incoming webhook, and fails unless Slack accepts it."""
    request = urllib.request.Request(
        webhook,
        data=json.dumps({"text": text}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        if response.read().decode().strip() != "ok":
            raise SystemExit("Slack did not acknowledge the CI alert")


def main():
    """Posts the alert for one completed run, or a test message."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--repository", required=True, help="owner/name of the repository")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--run-id", type=int, help="completed workflow run to report")
    mode.add_argument("--test", metavar="RUN_URL", help="post a test message linking RUN_URL")
    parser.add_argument("--dry-run", action="store_true", help="print the alert instead of posting it")
    args = parser.parse_args()

    if args.test:
        text = f":large_blue_circle: CI alerts test message from <{args.test}|this run>"
    else:
        text = alert_text(args.repository, args.run_id)
        if text is None:
            print("No alert for this run.")
            return
    if args.dry_run:
        print(text)
        return

    webhook = os.environ.get("SLACK_WEB_HOOK")
    if not webhook:
        raise SystemExit("SLACK_WEB_HOOK is not set")
    post(webhook, text)
    print(text)


if __name__ == "__main__":
    main()
