"""Durable, batched Slack delivery shared by the watchdog alert lanes.

A lane plans its transitions into a prospective state while capturing the
messages they need. The messages and that prospective state are queued under
one key of a lane-owned queue namespace and checkpointed before each send. The
prospective state is committed only after every queued payload is accepted, so
a Slack outage delays, but never loses, an alert or its recovery.
"""

from __future__ import annotations

import datetime
from typing import Any, Callable

from . import slack


BATCH_SEPARATOR = "\n\n---\n\n"
FLEET_BATCH_TITLE = "Fleet status updates"


def batch_messages(
    messages: list[str], now: float, title: str = FLEET_BATCH_TITLE
) -> list[str]:
    """Pack transitions without dropping incident summaries or exceeding Slack's cap."""
    if not messages:
        return []
    observed_at = datetime.datetime.fromtimestamp(now, datetime.timezone.utc).isoformat()
    prefix = f"*{title}* — observed {observed_at}\n\n"
    # Keep detailed diagnostics for a single incident; a batch shows the essential
    # lines for every event and links to the dashboard for the full node details.
    events = []
    for message in messages:
        if len(messages) == 1:
            events.append(message)
            continue
        lines = message.splitlines()
        summary = lines[:slack.SLACK_ESSENTIAL_PREFIX_LINES]
        link = next(
            (line for line in reversed(lines) if line.startswith(("dashboard:", "endpoint:"))),
            "",
        )
        if link and link not in summary:
            summary.append(link)
        events.append("\n".join(summary))
    chunks: list[str] = []
    chunk = prefix
    for event in events:
        event = slack.bounded_slack_message(event)
        # An individual legacy message may already fill the limit. Keep it intact
        # as a separate payload rather than cutting off another incident.
        if len(prefix) + len(event) > slack.MAX_SLACK_MESSAGE_CHARS:
            if chunk != prefix:
                chunks.append(chunk)
                chunk = prefix
            chunks.append(event)
            continue
        separator = BATCH_SEPARATOR if chunk != prefix else ""
        if len(chunk) + len(separator) + len(event) > slack.MAX_SLACK_MESSAGE_CHARS:
            chunks.append(chunk)
            chunk = prefix
            separator = ""
        chunk += separator + event
    if chunk != prefix:
        chunks.append(chunk)
    return chunks


def queue_transitions(
    state: dict[str, Any],
    queue: str,
    key: str,
    messages: list[str],
    candidate: dict[str, Any],
    now: float,
    title: str = FLEET_BATCH_TITLE,
) -> None:
    """Append batched messages and replace the prospective state for one key."""
    pending = state.setdefault(queue, {})
    delivery = pending.setdefault(key, {"messages": [], "state": {}})
    delivery["messages"].extend(batch_messages(messages, now, title))
    delivery["state"] = candidate


def deliver_pending(
    state: dict[str, Any],
    queue: str,
    key: str,
    post: Callable[[str], bool],
    commit: Callable[[dict[str, Any]], None],
    checkpoint: Callable[[dict[str, Any]], None],
) -> bool:
    """Send at most one payload for one key; commit after all chunks succeed.

    Checkpoint the pending payload before sending, then checkpoint each
    acknowledgement. A crash after Slack accepts a message but
    before that checkpoint can still repeat it (webhooks have no receipt ID).
    Returns whether a payload was accepted.
    """
    pending = state[queue][key]
    checkpoint(state)
    if not post(pending["messages"][0]):
        return False
    pending["messages"].pop(0)
    if not pending["messages"]:
        commit(pending["state"])
        del state[queue][key]
    checkpoint(state)
    return True
