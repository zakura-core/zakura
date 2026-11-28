#!/usr/bin/env python3
"""Report external links that have stayed broken for several days.

`external-links.yml` runs lychee once a day and passes its JSON report here.
The state file remembers when each currently failing URL first failed. A URL
that passes once is dropped, so only an unbroken run of daily failures counts.
The script exits 1 when any URL has been failing for at least `--days` days,
which fails the workflow and lets `ci-alerts.yml` post it.
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone


def failures(report):
    """Map each failing URL to its status text and the files that link to it."""
    found = {}
    for key in ("error_map", "timeout_map"):
        for source, entries in (report.get(key) or {}).items():
            for entry in entries:
                status = entry.get("status") or {}
                item = found.setdefault(
                    entry["url"], {"status": status.get("text", "failed"), "sources": []}
                )
                if source not in item["sources"]:
                    item["sources"].append(source)
    return found


def update(state, current, now):
    """Carry over each still-failing URL's first failure time and drop recovered URLs."""
    return {
        url: {
            "first_failed": state.get(url, {}).get("first_failed", now.isoformat()),
            "status": info["status"],
            "sources": sorted(info["sources"]),
        }
        for url, info in current.items()
    }


def persistent(state, now, days):
    """Return (url, entry, age) for URLs failing for at least `days` days, oldest first.

    Days are counted by calendar date, so a scheduled run that starts a few
    minutes earlier than the first failing run still counts as a full day.
    """
    rows = []
    for url, entry in state.items():
        first = datetime.fromisoformat(entry["first_failed"])
        if (now.date() - first.date()).days >= days:
            rows.append((url, entry, now - first))
    return sorted(rows, key=lambda row: row[2], reverse=True)


def summary(state, now, days):
    """Render every current failure and its age as Markdown for the job summary."""
    if not state:
        return "All external links passed.\n"
    lines = [
        f"Links fail this job once they have been broken for {days} days in a row.",
        "",
        "| Failing for | URL | Status | Linked from |",
        "| --- | --- | --- | --- |",
    ]
    for url, entry, age in persistent(state, now, 0):
        hours = int(age.total_seconds() // 3600)
        lines.append(
            f"| {hours // 24}d {hours % 24}h | {url} | {entry['status']} | "
            f"{', '.join(entry['sources'])} |"
        )
    return "\n".join(lines) + "\n"


def main(argv=None, now=None):
    """Update the state file from a lychee report and fail on long-broken links."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", required=True, help="lychee JSON report")
    parser.add_argument("--state", required=True, help="state file to read and rewrite")
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args(argv)
    now = now or datetime.now(timezone.utc)

    with open(args.report, encoding="utf-8") as file:
        report = json.load(file)
    try:
        with open(args.state, encoding="utf-8") as file:
            state = json.load(file)
    except FileNotFoundError:
        state = {}

    state = update(state, failures(report), now)
    os.makedirs(os.path.dirname(os.path.abspath(args.state)), exist_ok=True)
    with open(args.state, "w", encoding="utf-8") as file:
        json.dump(state, file, indent=2, sort_keys=True)

    text = summary(state, now, args.days)
    print(text, end="")
    if summary_path := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary_path, "a", encoding="utf-8") as file:
            file.write(text)

    broken = persistent(state, now, args.days)
    for url, entry, age in broken:
        print(f"::error::{url} has failed for {age.days} days: {entry['status']}")
    return 1 if broken else 0


if __name__ == "__main__":
    sys.exit(main())
