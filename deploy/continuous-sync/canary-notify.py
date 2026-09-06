#!/usr/bin/env python3
"""Deduplicate notifications for one VCT canary execution."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import time

from deploy import post_slack, save_audit_state, slack_webhook_url


def transition(event: dict[str, str], previous: dict, timestamp: int) -> tuple[str, dict]:
    """Deduplicate complete, identical failures only within one workflow run.

    New executions, changed diagnostics, and incomplete diagnostics always alert. A
    successful retry clears only a previously delivered failure for that run.
    """
    run_id = event["RUN_ID"]
    if previous.get("run_id") != run_id:
        previous = {}
    result = event["RESULT"]
    if result == "success":
        text = ""
        if previous.get("failed"):
            text = f":white_check_mark: Zakura VCT handoff canary recovered\n{event['RUN_URL']}"
        return text, {"run_id": run_id, "failed": False}
    if result != "failure":
        return "", previous

    fields = (
        "TARGET_SHA", "FAILURE_PHASE", "MAX_CHECKPOINT", "START_HEIGHT", "END_HEIGHT",
        "EXECUTION_ATTEMPT",
    )
    fingerprint = [event.get(key) or "unknown" for key in fields]
    complete = (
        re.fullmatch(r"[0-9a-fA-F]{40}", fingerprint[0]) is not None
        and fingerprint[1] != "unknown"
        and all(value.isdigit() for value in fingerprint[2:])
    )
    same_failure = complete and previous.get("failed") and previous.get("fingerprint") == fingerprint
    last_sent = previous.get("last_sent")
    if same_failure and type(last_sent) is int and 0 <= timestamp - last_sent < 86400:
        return "", {**previous, "last_attempt": event.get("RUN_ATTEMPT")}

    heading = "still failing on retry" if same_failure else "failed"
    sha, phase, checkpoint, start, end, execution = fingerprint
    text = (
        f":rotating_light: Zakura VCT handoff canary {heading}\n"
        f"phase={phase} | mainnet | ref={event.get('TARGET_REF', 'unknown')} | sha={sha} | "
        f"C={checkpoint} | start={start} | end={end} | execution={execution}\n"
        f"{event['RUN_URL']}"
    )
    return text, {
        "run_id": run_id, "failed": True, "fingerprint": fingerprint,
        "last_sent": timestamp, "last_attempt": event.get("RUN_ATTEMPT"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-file", type=Path, required=True)
    args = parser.parse_args()
    webhook = slack_webhook_url()
    if not webhook:
        raise SystemExit("SLACK_WEB_HOOK is not configured")
    destination = hashlib.sha256(webhook.encode()).hexdigest()
    try:
        previous = json.loads(args.state_file.read_text())
        if not isinstance(previous, dict) or previous.get("destination") != destination:
            previous = {}
    except (OSError, ValueError):
        previous = {}
    text, state = transition(dict(os.environ), previous, int(time.time()))
    if text:
        print(text)
        if not post_slack(text):
            # An undelivered failure or recovery must remain retryable.
            return 1
    else:
        print("No new canary notification; see this workflow attempt for its result.")
    save_audit_state(args.state_file, {**state, "destination": destination})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
