"""Slack bot delivery with a durable parent and per-image progress journal."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import urllib.error
import urllib.parse
import urllib.request

from deploy import DeployError


class Rejected(DeployError):
    """Slack explicitly rejected a request before performing its write."""


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Client:
    def __init__(self):
        self.token = os.environ.get("SLACK_BOT_TOKEN", "")
        if not self.token:
            raise DeployError("SLACK_BOT_TOKEN is not configured for chart delivery")
        self.opener = urllib.request.build_opener(NoRedirect())

    def api(self, method: str, **payload) -> dict:
        url = "https://slack.com/api/" + method
        body = json.dumps(payload).encode()
        if method == "files.info":
            url += "?" + urllib.parse.urlencode(payload)
            body = None
        request = urllib.request.Request(
            url, data=body,
            headers={"Authorization": "Bearer " + self.token, "Content-Type": "application/json; charset=utf-8"},
        )
        try:
            with self.opener.open(request, timeout=30) as response:
                data = json.loads(response.read(2 * 1024**2))
        except urllib.error.HTTPError as error:
            if error.code == 429:
                raise Rejected("Slack rate limit; retry on the next timer tick") from None
            raise DeployError(f"Slack {method} response uncertain (HTTP {error.code})") from None
        except (OSError, ValueError) as error:
            raise DeployError(f"Slack {method} response uncertain ({type(error).__name__})") from None
        if not isinstance(data, dict) or data.get("ok") is not True:
            code = data.get("error", "unknown") if isinstance(data, dict) else "unknown"
            code = code if isinstance(code, str) and re.fullmatch(r"[a-z_]{1,80}", code) else "unknown"
            # Only known precondition failures can safely retry a write. Unknown
            # server failures can occur after part of an operation has succeeded.
            rejected = {"invalid_auth", "not_authed", "token_revoked", "token_expired", "missing_scope",
                        "channel_not_found", "not_in_channel", "is_archived", "no_text", "msg_too_long",
                        "invalid_arguments", "invalid_arg_name", "invalid_array_arg", "invalid_charset",
                        "invalid_form_data", "invalid_post_type", "missing_post_type", "invalid_thread_ts",
                        "restricted_action", "access_denied", "ratelimited", "file_type_not_allowed",
                        "file_uploads_disabled", "file_upload_size_restricted"}
            error_type = Rejected if code in rejected else DeployError
            raise error_type(f"Slack {method}: {code}")
        return data

    def destination(self, channel: str) -> str:
        if not re.fullmatch(r"[CG][A-Z0-9]{8,}", channel):
            raise DeployError("chart channel_id must be a Slack channel ID")
        identity = self.api("auth.test")
        if not identity.get("team_id") or not identity.get("bot_id"):
            raise DeployError("chart delivery requires a workspace bot token")
        return hashlib.sha256(f"{identity['team_id']}:{channel}".encode()).hexdigest()

    def upload(self, url: str, payload: bytes) -> None:
        parsed = urllib.parse.urlsplit(url)
        if (parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password
                or parsed.port not in (None, 443)
                or not (parsed.hostname == "slack.com" or parsed.hostname.endswith(".slack.com"))):
            raise DeployError("Slack returned an unsupported upload URL")
        request = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/octet-stream"})
        try:
            with self.opener.open(request, timeout=45) as response:
                if response.status != 200:
                    raise DeployError("Slack file transfer did not succeed")
        except OSError as error:
            raise DeployError(f"Slack file transfer failed ({type(error).__name__})") from None


def send_pending(client: Client, channel: str, pending: dict, directory: Path, persist) -> None:
    """Retry known failures, reconcile uncertain file shares, and never guess a parent."""
    if pending.get("parent_posting"):
        raise DeployError("parent delivery is uncertain; inspect Slack and use recover-parent before retrying")
    if not pending.get("parent_ts"):
        pending["parent_posting"] = True
        persist()
        try:
            result = client.api("chat.postMessage", channel=channel, text=pending["text"],
                                unfurl_links=False, unfurl_media=False)
        except Rejected:
            pending.pop("parent_posting")
            persist()
            raise
        if result.get("channel") != channel or not re.fullmatch(r"\d+\.\d+", result.get("ts", "")):
            raise DeployError("Slack parent response lacks the expected channel/timestamp; inspect before recovery")
        pending["parent_ts"] = result["ts"]
        pending.pop("parent_posting")
        persist()

    for file in pending["files"]:
        if file.get("done"):
            continue
        if file.get("completing"):
            result = client.api("files.info", file=file["file_id"])
            shares = result.get("file", {}).get("shares", {})
            if any(share.get("thread_ts") == pending["parent_ts"]
                   for visibility in ("public", "private")
                   for share in shares.get(visibility, {}).get(channel, [])):
                file["done"] = True
                persist()
                continue
            raise DeployError("chart share is uncertain; inspect Slack and use recover-image before retrying")
        name = file["name"]
        if Path(name).name != name:
            raise DeployError("invalid pending image filename")
        payload = (directory / name).read_bytes()
        # Unfinalized tickets cannot create messages. A failed transfer can get a new ticket.
        ticket = client.api("files.getUploadURLExternal", filename=name, length=len(payload), alt_txt=file["title"])
        client.upload(ticket["upload_url"], payload)
        file.update(file_id=ticket["file_id"], completing=True)
        persist()
        try:
            result = client.api("files.completeUploadExternal", files=[{"id": file["file_id"], "title": file["title"]}],
                                channel_id=channel, thread_ts=pending["parent_ts"], initial_comment=file["title"])
        except Rejected:
            file.pop("completing")
            persist()
            raise
        if not any(item.get("id") == file["file_id"] for item in result.get("files", [])):
            raise DeployError("Slack completion response lacks the expected file; inspect before recovery")
        file["done"] = True
        persist()
