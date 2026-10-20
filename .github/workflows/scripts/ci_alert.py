#!/usr/bin/env python3
"""Post failures of watched workflows on `main` to Slack.

`ci-alerts.yml` runs this when an attempt of a watched workflow fails on `main`.
Every failed attempt is posted, with its failed jobs and, for a push, its
commit. Passing runs post nothing, so nothing depends on GitHub's run history.
"""

import argparse
import json
import os
import subprocess
import urllib.request


FAILED = {"failure", "startup_failure", "timed_out"}
MAX_LISTED_JOBS = 8
MAX_TITLE_CHARS = 100


def gh_api(path):
    """Returns the decoded JSON of a GitHub REST API `GET` request, made through `gh`."""
    output = subprocess.run(
        ["gh", "api", path], check=True, capture_output=True, text=True
    ).stdout
    return json.loads(output)


def is_failure(conclusion):
    """Returns whether `conclusion` is a failure."""
    return conclusion in FAILED


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


def message(run, jobs):
    """Formats the Slack text for a failed attempt `run` and its failed `jobs`."""
    workflow = escape(run["name"])
    trigger = "scheduled run" if run["event"] == "schedule" else "push"
    link = f"<{run['html_url']}|run {run['run_number']}>"
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
    """Returns the Slack text for attempt `attempt` of run `run_id`, or `None` unless it failed."""
    base = f"repos/{repository}/actions/runs/{run_id}/attempts/{attempt}"
    run = api(base)
    if not is_failure(run["conclusion"]):
        return None
    return message(run, failed_jobs(api(f"{base}/jobs?per_page=100")["jobs"]))


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
    """Posts the alert for one failed attempt, or a test message."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--repository", required=True, help="owner/name of the repository")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--run-id", type=int, help="workflow run to report")
    mode.add_argument("--test", metavar="RUN_URL", help="post a test message linking RUN_URL")
    parser.add_argument("--attempt", type=int, help="attempt of --run-id to report")
    parser.add_argument("--dry-run", action="store_true", help="print the alert instead of posting it")
    args = parser.parse_args()

    if args.test:
        text = f":large_blue_circle: CI alerts test message from <{args.test}|this run>"
    else:
        if args.attempt is None:
            parser.error("--run-id needs --attempt")
        text = alert_text(args.repository, args.run_id, args.attempt)
        if text is None:
            print("No alert: the attempt did not fail.")
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
