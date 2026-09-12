"""Report approval transitions in one Slack thread per PR.

Delivery receipts live in the App's GitHub review bodies. A checkpoint before
each POST prevents blind retries when Slack may have accepted a lost response.
"""

import http.client
import json
import re
import urllib.error
import urllib.request


MARKER = "<!-- zakura-codex-slack:v1 "
TIMESTAMP = re.compile(r"[0-9]{10,}\.[0-9]{6}")
STATES = {"APPROVED", "DISMISSED"}


class NotificationError(Exception):
    """Notification failed independently of the GitHub approval decision."""


class Rejected(NotificationError):
    """Slack definitively rejected a message, so a later run may retry it."""


def checkpoint(review):
    body = review["body"]
    if MARKER not in body:
        return None
    lines = [line for line in body.splitlines() if line.startswith(MARKER)]
    try:
        if body.count(MARKER) != 1 or len(lines) != 1 or not lines[0].endswith(" -->"):
            raise ValueError
        state = json.loads(lines[0][len(MARKER):-4])
        if (not isinstance(state, dict)
                or set(state) != {"channel", "thread_ts", "sent", "pending"}
                or not isinstance(state["channel"], str)
                or re.fullmatch(r"C[A-Z0-9]+", state["channel"]) is None
                or (state["thread_ts"] is not None
                    and (not isinstance(state["thread_ts"], str)
                         or TIMESTAMP.fullmatch(state["thread_ts"]) is None))
                or state["sent"] not in ([], ["APPROVED"], ["APPROVED", "DISMISSED"])
                or state["pending"] not in (None, "APPROVED", "DISMISSED")
                or state["pending"] in state["sent"]
                or (state["sent"] and state["thread_ts"] is None)
                or (state["pending"] == "DISMISSED" and state["sent"] != ["APPROVED"])):
            raise ValueError
        return state
    except (TypeError, ValueError, KeyError):
        raise NotificationError("Malformed Slack checkpoint in an App review") from None


def needs_notification(review):
    """Keep withdrawal delivery reachable after the PR author loses access."""
    try:
        state = checkpoint(review)
        return bool(state and (state["pending"] or (
            review["state"] == "DISMISSED" and state["sent"] == ["APPROVED"])))
    except NotificationError:
        return True


class Slack:
    def __init__(self, token):
        self.token = token

    def post(self, channel, text, thread_ts):
        payload = {"channel": channel, "text": text,
                   "unfurl_links": False, "unfurl_media": False}
        if thread_ts:
            payload["thread_ts"] = thread_ts
        request = urllib.request.Request(
            "https://slack.com/api/chat.postMessage", method="POST",
            data=json.dumps(payload).encode(),
            headers={"Authorization": f"Bearer {self.token}",
                     "Content-Type": "application/json; charset=utf-8"},
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                raw = response.read(1024 * 1024 + 1)
                if len(raw) > 1024 * 1024:
                    raise NotificationError("Slack response exceeded the size limit; delivery is uncertain")
                result = json.loads(raw)
        except urllib.error.HTTPError as exc:
            if exc.code == 429:
                raise Rejected("Slack rate limited the notification; a later run can retry") from None
            raise NotificationError(f"Slack HTTP {exc.code}; delivery is uncertain") from None
        except (OSError, http.client.HTTPException, ValueError):
            raise NotificationError("Slack response unavailable; delivery is uncertain") from None
        if isinstance(result, dict) and result.get("ok") is False:
            # Never log arbitrary response bodies, which may contain credentials.
            code = result.get("error", "unknown_error")
            safe_code = code if isinstance(code, str) and re.fullmatch(r"[a-z_]+", code) else "unknown_error"
            raise Rejected(f"Slack rejected the notification ({safe_code})")
        if (not isinstance(result, dict) or result.get("ok") is not True
                or result.get("channel") != channel
                or not isinstance(result.get("ts"), str)
                or TIMESTAMP.fullmatch(result["ts"]) is None):
            raise NotificationError("Unexpected Slack response; delivery is uncertain")
        return result["ts"]


def save(worker, review, state):
    body = "\n".join(line for line in review["body"].splitlines()
                     if not line.startswith(MARKER)).rstrip()
    body += "\n\n" + MARKER + json.dumps(state, sort_keys=True, separators=(",", ":")) + " -->"
    worker.writer.request(f"{worker.pull_path}/reviews/{int(review['id'])}", "PUT", {"body": body})
    review["body"] = body


def notify(worker, token, *, approved):
    """Reconcile Slack with verified, owned reviews without changing approval state."""
    channel = worker.policy.data["slack_channel"]
    reviews = sorted(worker.owned_reviews(), key=lambda review: review["id"])
    states = {review["id"]: checkpoint(review) for review in reviews}
    roots = {state["thread_ts"] for state in states.values() if state and state["thread_ts"]}
    if len(roots) > 1 or any(state and state["channel"] != channel for state in states.values()):
        raise NotificationError("Conflicting Slack thread checkpoints; inspect the App reviews")
    if any(state and state["pending"] for state in states.values()):
        raise NotificationError("Slack delivery is uncertain; inspect and repair the pending App review checkpoint")
    root = next(iter(roots), None)
    sent = 0
    slack = Slack(token)
    for review in reviews:
        status = review["state"]
        state = states[review["id"]]
        if (status not in STATES or (status == "APPROVED" and not approved)
                or (state and status in state["sent"])
                or (status == "DISMISSED" and not (state and "APPROVED" in state["sent"]))):
            continue
        if not token:
            raise NotificationError("SLACK_BOT_TOKEN is required to notify #gh-alerts")
        if re.fullmatch(r"[0-9a-f]{40}", review["commit_id"]) is None:
            raise NotificationError("Cannot notify an approval with an invalid commit ID")
        url = f"https://github.com/{worker.repo}/pull/{worker.number}"
        heading = "✅ Codex approval" if status == "APPROVED" else "↩️ Codex approval withdrawn"
        action = "Approved" if status == "APPROVED" else "Withdrew approval of"
        text = (f"{heading}: <{url}|Zakura #{worker.number}>\n"
                f"{action} commit <https://github.com/{worker.repo}/commit/{review['commit_id']}"
                f"|{review['commit_id'][:7]}>. "
                f"<{url}#pullrequestreview-{review['id']}|GitHub review>.")
        if status == "APPROVED":
            text += " Other merge requirements still apply."
        state = state or {"channel": channel, "thread_ts": root, "sent": [], "pending": None}
        state["pending"] = status
        save(worker, review, state)
        try:
            timestamp = slack.post(channel, text, root)
        except Rejected:
            state["pending"] = None
            save(worker, review, state)
            raise
        root = root or timestamp
        state.update(thread_ts=root, pending=None, sent=[*state["sent"], status])
        save(worker, review, state)
        sent += 1
    return sent
