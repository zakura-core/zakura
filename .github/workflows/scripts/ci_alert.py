#!/usr/bin/env python3
"""Post failures and recoveries of watched workflows on `main` to Slack.

`ci-alerts.yml` runs this after a watched workflow completes on `main`, once per
completed attempt. Each attempt is judged against the verdict before it: the
newest earlier attempt of the same run that reached a verdict, or else the
newest earlier run of the same workflow and event that has one. A run's verdict
is that of its newest attempt that reached one. Cancelled, skipped and stale
attempts never count as a verdict, so a cancelled re-run cannot erase one.

A push failure is posted only when the previous verdict was not a failure, so a
broken `main` is reported once rather than on every merge. Every scheduled
failure is posted, because nothing else surfaces those runs. A pass after a
failure is posted as a recovery. An attempt is not reported when a newer
attempt or run already reached the opposite verdict, because `main` has moved
on. A newer verdict that agrees does not suppress it, so a burst of failures
that complete before their alerts run is still reported once.
"""

import argparse
import json
import os
import subprocess
import urllib.request


FAILED = {"failure", "startup_failure", "timed_out"}
# Conclusions that say nothing about the workflow's health.
UNDECIDED = {"cancelled", "skipped", "stale"}
HISTORY_PAGE_SIZE = 100
HISTORY_MAX_PAGES = 5
MAX_LISTED_JOBS = 8
MAX_TITLE_CHARS = 100


def gh_api(path):
    """Returns the decoded JSON of a GitHub REST API `GET` request, made through `gh`."""
    output = subprocess.run(
        ["gh", "api", path], check=True, capture_output=True, text=True
    ).stdout
    return json.loads(output)


def is_decided(conclusion):
    """Returns whether `conclusion` is a verdict on the workflow's health."""
    return conclusion is not None and conclusion not in UNDECIDED


def is_failure(conclusion):
    """Returns whether `conclusion` is a failure."""
    return conclusion in FAILED


def newest_verdict(conclusions):
    """Returns the first decided conclusion in `conclusions` (newest first), or `None`."""
    return next((conclusion for conclusion in conclusions if is_decided(conclusion)), None)


def earlier_attempts(api, base, run_id, attempt):
    """Yields the conclusions of the run's attempts before `attempt`, newest first."""
    for earlier in range(attempt - 1, 0, -1):
        yield api(f"{base}/runs/{run_id}/attempts/{earlier}")["conclusion"]


def later_attempts(api, base, latest, attempt):
    """Yields the conclusions of the run's attempts after `attempt`, newest first.

    `latest` is the run as its newest attempt reports it.
    """
    if latest["run_attempt"] <= attempt:
        return
    yield latest["conclusion"] if latest["status"] == "completed" else None
    for later in range(latest["run_attempt"] - 1, attempt, -1):
        yield api(f"{base}/runs/{latest['id']}/attempts/{later}")["conclusion"]


def run_verdict(api, base, run):
    """Returns a run's verdict: its newest decided attempt's conclusion, or `None`.

    A cancelled or still-active retry does not erase the verdict of an attempt
    before it.
    """
    if run["status"] == "completed" and is_decided(run["conclusion"]):
        return run["conclusion"]
    return newest_verdict(earlier_attempts(api, base, run["id"], run["run_attempt"]))


def history(api, base, run):
    """Yields runs of `run`'s workflow and event on `main`, newest first.

    Active runs are included: a run being retried still has the verdict of its
    earlier attempts.
    """
    for page in range(1, HISTORY_MAX_PAGES + 1):
        runs = api(
            f"{base}/workflows/{run['workflow_id']}/runs?branch=main&event={run['event']}"
            f"&per_page={HISTORY_PAGE_SIZE}&page={page}"
        )["workflow_runs"]
        yield from runs
        if len(runs) < HISTORY_PAGE_SIZE:
            return


def verdicts(current, attempts_before, attempts_after, runs, resolve):
    """Returns `(previous, contradicted)` for the attempt `current`.

    `previous` is the verdict `current` follows: the newest decided conclusion in
    `attempts_before`, or else the verdict of the newest older run in `runs` that
    has one. It is `None` when nothing decided came first. `contradicted` says
    whether a later attempt of the same run, or a newer run, reached the opposite
    verdict. Attempts and runs are newest first, and `resolve` gives a run's
    verdict.
    """
    failed = is_failure(current["conclusion"])
    later = newest_verdict(attempts_after)
    contradicted = is_decided(later) and is_failure(later) != failed
    previous = newest_verdict(attempts_before)
    for other in runs:
        if other["run_number"] > current["run_number"]:
            verdict = resolve(other)
            if is_decided(verdict) and is_failure(verdict) != failed:
                contradicted = True
        elif other["run_number"] < current["run_number"]:
            if previous is None:
                previous = resolve(other)
            if previous is not None:
                break
    return previous, contradicted


def alert_kind(current, previous, contradicted):
    """Returns `"failure"`, `"recovery"`, or `None` for a completed attempt."""
    conclusion = current["conclusion"]
    if contradicted or not is_decided(conclusion):
        return None
    if is_failure(conclusion):
        if current["event"] == "schedule" or not is_failure(previous):
            return "failure"
        return None
    if conclusion == "success" and is_failure(previous):
        return "recovery"
    return None


def failed_jobs(jobs):
    """Returns the names of the failed jobs, in the order GitHub lists them.

    A workflow's `... success` summary job fails whenever another job does, so it
    is left out unless it is the only failure.
    """
    failed = [job["name"] for job in jobs if is_failure(job["conclusion"])]
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


def alert_text(repository, run_id, attempt, api=gh_api):
    """Returns the Slack text for one completed attempt, or `None` when it needs no alert."""
    base = f"repos/{repository}/actions"
    current = api(f"{base}/runs/{run_id}/attempts/{attempt}")
    latest = api(f"{base}/runs/{run_id}")
    previous, contradicted = verdicts(
        current,
        earlier_attempts(api, base, run_id, attempt),
        later_attempts(api, base, latest, attempt),
        history(api, base, current),
        lambda run: run_verdict(api, base, run),
    )
    kind = alert_kind(current, previous, contradicted)
    if kind is None:
        return None
    jobs = []
    if kind == "failure":
        jobs = failed_jobs(
            api(f"{base}/runs/{run_id}/attempts/{attempt}/jobs?per_page=100")["jobs"]
        )
    return message(kind, current, jobs)


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
    """Posts the alert for one completed attempt, or a test message."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--repository", required=True, help="owner/name of the repository")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--run-id", type=int, help="workflow run to report")
    mode.add_argument("--test", metavar="RUN_URL", help="post a test message linking RUN_URL")
    parser.add_argument("--attempt", type=int, help="completed attempt of --run-id to report")
    parser.add_argument("--dry-run", action="store_true", help="print the alert instead of posting it")
    args = parser.parse_args()

    if args.test:
        text = f":large_blue_circle: CI alerts test message from <{args.test}|this run>"
    else:
        if args.attempt is None:
            parser.error("--run-id needs --attempt")
        text = alert_text(args.repository, args.run_id, args.attempt)
        if text is None:
            print("No alert for this attempt.")
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
