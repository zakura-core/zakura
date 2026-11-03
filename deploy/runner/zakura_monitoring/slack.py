"""Slack sanitization and incoming-webhook transport for #zakura-alerts."""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request


MAX_DASHBOARD_URL_CHARS = 2_048
MAX_SLACK_MESSAGE_CHARS = 35_000
SLACK_ESSENTIAL_PREFIX_LINES = 3
SLACK_TRUNCATION_MARKER = "[alert truncated]"
SLACK_PLAIN_TEXT_TRANSLATION = str.maketrans(
    {
        "&": "＆",
        "<": "‹",
        ">": "›",
        "*": "∗",
        "_": "＿",
        "~": "～",
        "`": "ˋ",
        "@": "＠",
    }
)


def bounded_text(value: object, limit: int) -> str:
    return " ".join(str(value or "").split())[:limit]


def slack_plain_text(value: object, limit: int) -> str:
    return bounded_text(value, limit).translate(SLACK_PLAIN_TEXT_TRANSLATION)


def slack_identity(value: object, limit: int, fallback: str) -> str:
    return slack_plain_text(value or fallback, limit) or fallback


def slack_dashboard_url(value: object) -> str:
    return bounded_text(value, MAX_DASHBOARD_URL_CHARS)


def bounded_slack_message(text: str) -> str:
    if len(text) <= MAX_SLACK_MESSAGE_CHARS:
        return text

    lines = text.splitlines()
    if len(lines) < 2:
        keep = MAX_SLACK_MESSAGE_CHARS - len(SLACK_TRUNCATION_MARKER)
        return text[:keep] + SLACK_TRUNCATION_MARKER

    prefix_count = min(SLACK_ESSENTIAL_PREFIX_LINES, len(lines) - 1)
    prefix = lines[:prefix_count]
    middle = lines[prefix_count:-1]
    suffix = lines[-1]
    protected = [*prefix, SLACK_TRUNCATION_MARKER, suffix]
    protected_length = sum(map(len, protected)) + len(protected) - 1
    if protected_length > MAX_SLACK_MESSAGE_CHARS:
        # Alert formatters bound protected fields before they reach this fallback.
        # Keep both ends for direct callers that do not use an alert formatter.
        available = MAX_SLACK_MESSAGE_CHARS - len(SLACK_TRUNCATION_MARKER) - 2
        prefix_budget = max(0, available // 2)
        suffix_budget = max(0, available - prefix_budget)
        return "\n".join(
            (
                "\n".join(prefix)[:prefix_budget],
                SLACK_TRUNCATION_MARKER,
                suffix[:suffix_budget],
            )
        )

    remaining = MAX_SLACK_MESSAGE_CHARS - protected_length
    kept_middle = []
    for line in middle:
        added = len(line) + 1
        if added > remaining:
            break
        kept_middle.append(line)
        remaining -= added

    return "\n".join(
        (*prefix, *kept_middle, SLACK_TRUNCATION_MARKER, suffix)
    )


def slack_webhook_url() -> str:
    """Return the configured incoming webhook URL for #zakura-alerts.

    Bot tokens are intentionally unsupported: a token without channel
    membership fails with `not_in_channel` and previously masked webhook
    misconfiguration.
    """
    return (
        os.environ.get("SLACK_WEB_HOOK", "")
        or os.environ.get("SLACK_WEBHOOK_URL", "")
        or os.environ.get("SLACK_WEBHOOK", "")
    )


def post_slack(text: str, args: argparse.Namespace) -> bool:
    text = bounded_slack_message(text)
    webhook = slack_webhook_url()
    if args.dry_run:
        print(f"dry-run Slack message:\n{text}\n")
        return True

    if not webhook:
        print(
            "SLACK_WEB_HOOK (or SLACK_WEBHOOK_URL / SLACK_WEBHOOK) is not set; "
            f"cannot post:\n{text}\n",
            file=sys.stderr,
        )
        return False

    return post_slack_webhook(webhook, text, args)


def post_slack_webhook(webhook: str, text: str, args: argparse.Namespace) -> bool:
    text = bounded_slack_message(text)
    payload = json.dumps({"text": text}).encode("utf-8")
    request = urllib.request.Request(
        webhook,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=args.slack_timeout) as response:
            body = response.read().decode("utf-8", errors="replace").strip()
    except (OSError, urllib.error.URLError) as error:
        print(f"Slack webhook post failed: {error}", file=sys.stderr)
        return False

    if response.status < 200 or response.status >= 300 or body != "ok":
        print(
            f"Slack webhook post failed: status={response.status} body={body}",
            file=sys.stderr,
        )
        return False

    return True
