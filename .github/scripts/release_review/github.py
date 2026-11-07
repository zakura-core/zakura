"""GitHub metadata resolution and idempotent, exact-head report publication."""

from __future__ import annotations

import base64
import json
import os
import re
import subprocess
from pathlib import Path
from urllib.parse import urlencode

from .contracts import ReviewError
from .repository import SHA

COMMENT_MARKER = "<!-- zakura-release-ai-review -->"
STATE_MARKER = re.compile(r"<!-- release-review-run: (.*?) -->")
RELEASE_BRANCH = re.compile(r"release/v[0-9]+\.[0-9]+\.[0-9]+(?:-rc[0-9]+)?")
REQUEST_WORKFLOW = ".github/workflows/release-ai-review-request.yml"


class GitHub:
    def __init__(self, repository: str):
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
            raise ReviewError("invalid GitHub repository")
        self.repository = repository

    def api(self, path: str, *, method: str = "GET", body: dict | None = None):
        args = ["gh", "api", "--method", method, f"repos/{self.repository}/{path}"]
        if body is not None:
            args.extend(["--input", "-"])
        try:
            response = subprocess.run(args, input=json.dumps(body) if body is not None else None,
                                      text=True, capture_output=True, timeout=45, check=False)
        except (OSError, subprocess.TimeoutExpired):
            raise ReviewError("GitHub request failed or timed out") from None
        if response.returncode:
            raise ReviewError("GitHub request was not accepted; check token permissions")
        try:
            return json.loads(response.stdout)
        except ValueError:
            raise ReviewError("GitHub returned invalid JSON") from None

    def comments(self, pr: int) -> list[dict]:
        result = []
        for page in range(1, 101):
            batch = self.api(f"issues/{pr}/comments?per_page=100&page={page}")
            result.extend(batch)
            if len(batch) < 100:
                return result
        raise ReviewError("PR comment enumeration exceeded its bound")


def eligible_pr(pr: dict, repository: str, *, allow_draft: bool) -> bool:
    return (pr["state"] == "open" and (allow_draft or not pr["draft"])
            and pr["base"]["ref"] == "main"
            and (pr["head"].get("repo") or {}).get("full_name") == repository
            and RELEASE_BRANCH.fullmatch(pr["head"]["ref"]) is not None)


def resolve_request(github: GitHub, event_name: str, event: dict) -> dict | None:
    """Revalidate triggering metadata using GitHub; never trust request artifacts."""
    inputs = event.get("inputs") or {}
    expected_head = None
    if event_name == "workflow_dispatch":
        if event.get("ref") != "refs/heads/main":
            raise ReviewError("manual reviews must run the workflow from main")
        raw_number = str(inputs.get("pr_number", ""))
        if not raw_number.isdigit() or int(raw_number) < 1:
            raise ReviewError("manual review requires a positive PR number")
        number = int(raw_number)
    elif event_name == "workflow_run":
        run = github.api(f"actions/runs/{event['workflow_run']['id']}")
        if (run.get("event") != "pull_request" or run.get("conclusion") != "success"
                or run.get("path") != REQUEST_WORKFLOW
                or (run.get("head_repository") or {}).get("full_name") != github.repository
                or not RELEASE_BRANCH.fullmatch(run.get("head_branch") or "")):
            return None
        expected_head = run["head_sha"]
        candidates = github.api("pulls?" + urlencode({
            "state": "open", "base": "main", "head": github.repository.split("/")[0] + ":" + run["head_branch"],
        }))
        if len(candidates) != 1:
            return None
        number = candidates[0]["number"]
    else:
        raise ReviewError("unsupported review event")
    pr = github.api(f"pulls/{number}")
    if not eligible_pr(pr, github.repository, allow_draft=event_name == "workflow_dispatch"):
        if event_name == "workflow_dispatch":
            raise ReviewError("target must be an open same-repository release/v* PR into main")
        return None
    if expected_head and pr["head"]["sha"] != expected_head:
        return None
    if not SHA.fullmatch(pr["head"]["sha"]):
        raise ReviewError("GitHub returned an invalid PR head")
    provider = inputs.get("provider", "openai")
    if provider not in {"openai", "abliterated"}:
        raise ReviewError("unknown review provider")
    return {"repository": github.repository, "pr_number": number,
            "head_sha": pr["head"]["sha"], "pr_body": pr.get("body") or "",
            "provider": provider, "profile": inputs.get("prompt_profile", "default"),
            "base_override": inputs.get("base_tag", "")}


def fetch_head(root: Path, context: dict) -> None:
    """Fetch objects only. Keep checkout credentials out of Git config and command text."""
    number = context["pr_number"]
    if type(number) is not int or number < 1:
        raise ReviewError("invalid PR number for fetch")
    token = os.environ.get("GH_TOKEN", "")
    if not token:
        raise ReviewError("GitHub read token is unavailable")
    env = {key: value for key, value in os.environ.items()
           if key in {"PATH", "HOME", "SYSTEMROOT", "TMPDIR"}}
    authorization = base64.b64encode(("x-access-token:" + token).encode()).decode()
    env.update(GIT_TERMINAL_PROMPT="0", GIT_CONFIG_COUNT="1",
               GIT_CONFIG_KEY_0="http.https://github.com/.extraheader",
               GIT_CONFIG_VALUE_0="AUTHORIZATION: basic " + authorization)
    remote = f"https://github.com/{context['repository']}.git"
    try:
        result = subprocess.run(
            ["git", "-c", "core.hooksPath=/dev/null", "fetch", "--no-tags", remote,
             f"refs/pull/{number}/head:refs/release-review/head"],
            cwd=root, env=env, capture_output=True, timeout=120, check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        raise ReviewError("release source fetch failed or timed out") from None
    if result.returncode:
        raise ReviewError("release source could not be fetched")


def publish(github: GitHub, report: dict, markdown: str) -> str:
    """Update only our bot comment, and only while this exact head is current."""
    number = report["pr_number"]
    if type(number) is not int or number < 1 or not SHA.fullmatch(report["head_sha"]):
        raise ReviewError("report has invalid target metadata")
    pr = github.api(f"pulls/{number}")
    if (not eligible_pr(pr, github.repository, allow_draft=True)
            or pr["head"]["sha"] != report["head_sha"]):
        return "stale: report retained as an artifact; PR comment not changed"
    existing = [comment for comment in github.comments(number)
                if comment["user"]["login"] == "github-actions[bot]"
                and comment.get("body", "").startswith(COMMENT_MARKER)]
    if len(existing) > 1:
        raise ReviewError("multiple review summary comments need manual reconciliation")
    state = {"run_number": report["run_number"], "attempt": report["run_attempt"],
             "head_sha": report["head_sha"]}
    if existing:
        marker = STATE_MARKER.search(existing[0]["body"])
        if marker is None:
            raise ReviewError("existing review comment has no valid run identity")
        try:
            previous = json.loads(marker[1])
            newer = (previous["run_number"], previous["attempt"]) > (state["run_number"], state["attempt"])
        except (ValueError, KeyError, TypeError):
            raise ReviewError("existing review comment has an invalid run identity") from None
        if newer:
            return "superseded: newer report already published"
    # Recheck after enumerating comments, immediately before the write.
    if github.api(f"pulls/{number}")["head"]["sha"] != report["head_sha"]:
        return "stale: head changed while preparing the comment"
    body = COMMENT_MARKER + "\n<!-- release-review-run: " + json.dumps(state) + " -->\n" + markdown
    if existing:
        written = github.api(f"issues/comments/{existing[0]['id']}", method="PATCH", body={"body": body})
    else:
        written = github.api(f"issues/{number}/comments", method="POST", body={"body": body})
    comment_id = written.get("id") if isinstance(written, dict) else None
    if type(comment_id) is not int or comment_id < 1:
        raise ReviewError("GitHub did not return a comment identity; publication is unconfirmed")
    confirmed = github.api(f"issues/comments/{comment_id}")
    url = f"https://github.com/{github.repository}/pull/{number}#issuecomment-{comment_id}"
    if (not isinstance(confirmed, dict) or confirmed.get("body") != body
            or confirmed.get("html_url") != url
            or confirmed.get("user", {}).get("login") != "github-actions[bot]"):
        raise ReviewError("PR summary read-back did not confirm the bot comment; publication is unconfirmed")
    report["comment_url"] = url
    return "published"
