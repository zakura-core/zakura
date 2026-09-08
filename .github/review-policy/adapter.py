#!/usr/bin/env python3
"""Translate an authenticated, current native Codex review into a PR approval.

The default is read-only. This program reads GitHub metadata, never PR code.
Approval policy and this executable must come from the trusted default branch.
"""

from __future__ import annotations

import argparse
import base64
import binascii
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


POLICY_PATH = Path(__file__).with_name("policy.json")
SUMMARY_MARKER = "<!-- codex-pull-request-review-summary -->"
RECEIPT_MARKER = "<!-- zakura-codex-approval:v1 "
MAX_PAGES = 30
MAX_RESPONSE = 16 * 1024 * 1024
ACTOR = "login ... on Bot { databaseId }"
ROW = re.compile(
    r'^\| 📝 \*\*Code Review\*\* \| '
    r'(?P<status>✅ \*\*Completed\*\*|🔄 \*\*Running\*\* since) '
    r'<relative-time datetime="(?P<time>[^"\n]+)">[^<\n]+</relative-time> '
    r'\| `(?P<sha>[0-9a-f]{7,40})` \| (?P<trigger>[^|\n]+) \|$'
)
COMMAND = re.compile(r"@codex\s+(?:security\s+)?review\b", re.IGNORECASE)


class Ineligible(Exception):
    """Evidence is absent, incomplete, or outside the approval policy."""


class APIError(Exception):
    """A GitHub API operation failed; its response body is not logged."""


def require(condition, reason):
    if not condition:
        raise Ineligible(reason)


def instant(value):
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        require(result.tzinfo is not None, "Timestamp has no timezone")
        return result
    except (AttributeError, TypeError, ValueError) as exc:
        raise Ineligible("Missing or malformed timestamp") from exc


@dataclass(frozen=True)
class Policy:
    data: dict

    @classmethod
    def load(cls):
        return cls(json.loads(POLICY_PATH.read_text()))

    @property
    def digest(self):
        return hashlib.sha256(json.dumps(self.data, sort_keys=True).encode()).hexdigest()

    def eligible_path(self, path):
        # Reject paths whose representation could disagree with GitHub matching.
        if (not isinstance(path, str) or not path or "\\" in path
                or any(ord(c) < 32 for c in path)
                or any(p in ("", ".", "..") for p in path.split("/"))):
            return False
        return any(path.startswith(root) for root in self.data["eligible_roots"]) and not any(
            path.startswith(p) if p.endswith("/") else path == p
            for p in self.data["human_only"]
        )

    def check_files(self, files, expected_count, pr_number=None):
        """Return the optional new PR-owned fragment; other changes must qualify."""
        require(0 < len(files) == expected_count < 3000, "Incomplete or empty changed-file list")
        require(len({f["filename"] for f in files}) == len(files), "Duplicate changed files")
        fragment = None
        own_fragment = (f"{self.data['changelog_fragment_root']}{pr_number}.md"
                        if isinstance(pr_number, int) and pr_number > 0 else None)
        for file in files:
            if file["filename"] == own_fragment:
                require(file.get("status") == "added" and not file.get("previous_filename"),
                        "Only a newly added changelog fragment for this PR qualifies")
                fragment = own_fragment
                continue
            require(file.get("status") in ("modified", "removed", "renamed"),
                    "New or unclassified files require human review")
            paths = [file["filename"]]
            if file["status"] == "renamed":
                require(bool(file.get("previous_filename")), "Rename is missing its source path")
                paths.append(file["previous_filename"])
            require(all(self.eligible_path(p) for p in paths),
                    "Changed files include a path requiring human review")
            require(file["status"] != "renamed", "Renamed files require human classification")
        require(len(files) > int(fragment is not None),
                "A changelog fragment must accompany an eligible CI or deployment change")
        return fragment

    def native(self, obj):
        return (obj.get("user", {}).get("id") == self.data["codex_user_id"]
                and obj.get("user", {}).get("type") == "Bot"
                and (obj.get("performed_via_github_app") or {}).get("id")
                == self.data["codex_app_id"])

    def native_actor(self, actor):
        # The inline Bot fragment supplies databaseId only for a real Bot.
        return (actor or {}).get("databaseId") == self.data["codex_user_id"]


class GitHub:
    def __init__(self, token):
        self.token = token

    def request(self, path, method="GET", payload=None):
        require(path.startswith("/") and not path.startswith("//"), "Invalid API path")
        body = None if payload is None else json.dumps(payload).encode()
        request = urllib.request.Request(
            "https://api.github.com" + path, data=body, method=method,
            headers={"Authorization": f"Bearer {self.token}",
                     "Accept": "application/vnd.github+json",
                     "X-GitHub-Api-Version": "2022-11-28",
                     "Content-Type": "application/json", "User-Agent": "zakura-review-adapter"},
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                raw = response.read(MAX_RESPONSE + 1)
                if len(raw) > MAX_RESPONSE:
                    raise APIError("GitHub response exceeded the size limit")
                return json.loads(raw) if raw else None
        except urllib.error.HTTPError as exc:
            raise APIError(f"GitHub {method} failed with HTTP {exc.code}") from None
        except (OSError, http.client.HTTPException, ValueError) as exc:
            raise APIError(f"GitHub {method} failed ({type(exc).__name__})") from None

    def pages(self, path):
        result = []
        separator = "&" if "?" in path else "?"
        for page in range(1, MAX_PAGES + 1):
            batch = self.request(f"{path}{separator}per_page=100&page={page}")
            require(isinstance(batch, list), "Unexpected paginated API response")
            result.extend(batch)
            if len(batch) < 100:
                return result
        raise Ineligible("GitHub pagination limit reached")

    def graphql(self, query, **variables):
        result = self.request("/graphql", "POST", {"query": query, "variables": variables})
        if result.get("errors") or not result.get("data"):
            raise APIError("GitHub GraphQL returned incomplete data")
        return result["data"]


def parse_summary(body):
    require(isinstance(body, str) and body.startswith(SUMMARY_MARKER), "Missing native summary")
    rows = [line for line in body.splitlines() if line.startswith("|")]
    require(len(rows) == 3 and rows[0] == "| Review | Status | Commit | Review trigger |"
            and rows[1] == "| --- | --- | --- | --- |", "Unknown native summary format")
    match = ROW.fullmatch(rows[2])
    require(match is not None, "Review is running, failed, or uses an unsupported format")
    row = match.groupdict()
    row["time"] = instant(row["time"])
    row["completed"] = row["status"] == "✅ **Completed**"
    return row


def check_evidence(policy, pull, comments, summary, reactions, reviews, threads, resolved_sha):
    """Return a durable receipt only for one authenticated, finished review episode."""
    candidates = [c for c in comments if policy.native(c)
                  and c.get("body", "").startswith(SUMMARY_MARKER)]
    require(len(candidates) == 1, "Expected exactly one native Codex summary")
    comment = candidates[0]
    require(summary["databaseId"] == comment["id"] and summary["body"] == comment["body"],
            "Summary changed while it was being read")
    require(policy.native_actor(summary["author"]) and policy.native_actor(summary["editor"]),
            "Summary author or last editor is not Codex")
    history = summary["userContentEdits"]
    require(not history["pageInfo"]["hasNextPage"], "Incomplete summary edit history")
    edits = history["nodes"]
    require(len(edits) >= 2 and all(policy.native_actor(e["editor"]) for e in edits),
            "Summary history is missing or has a non-Codex editor")
    require(edits[0]["diff"] == summary["body"]
            and instant(edits[0]["editedAt"]) == instant(summary["lastEditedAt"]),
            "Summary edit history does not match its current content")
    finished, started = parse_summary(summary["body"]), parse_summary(edits[1]["diff"])
    require(finished["completed"] and not started["completed"],
            "Missing the immediately preceding Running review")
    require(started["sha"] == finished["sha"] and started["trigger"] == finished["trigger"],
            "Review episode changed commit or trigger")
    require(started["time"] < finished["time"] <= datetime.now(timezone.utc) + timedelta(seconds=2),
            "Invalid review episode times")
    require(started["time"] - timedelta(seconds=1) <= instant(edits[1]["editedAt"])
            <= finished["time"], "Running edit is outside the review episode")
    require(finished["time"] - timedelta(seconds=1) <= instant(summary["lastEditedAt"])
            <= finished["time"] + timedelta(minutes=2), "Completion edit is outside the review episode")
    require(re.fullmatch(r"[0-9a-f]{40}", resolved_sha) is not None
            and resolved_sha == pull["head"]["sha"] and resolved_sha.startswith(finished["sha"]),
            "Codex reviewed a different or ambiguous commit")

    requests = [c for c in comments if c.get("user", {}).get("type") != "Bot"
                and COMMAND.search(c.get("body", ""))]
    for request in requests:
        require(instant(request["created_at"]) <= started["time"]
                and instant(request["updated_at"]) <= started["time"],
                "A newer or edited Codex request needs a fresh completed review")
    if finished["trigger"] == "Manual request":
        require(bool(requests), "Manual review request is missing")
        latest = max(requests, key=lambda c: instant(c["created_at"]))
        require(latest["body"].strip() == "@codex review", "Scoped review requests require human review")
        require(started["time"] - instant(latest["created_at"]) <= timedelta(minutes=30),
                "Cannot correlate this manual request to the review")
    else:
        require(finished["trigger"] in ("Draft marked ready", "New commits", "Pull request opened"),
                "Unrecognized automatic review trigger")

    # Completed means processing finished, including runs with findings. Only a
    # fresh PR-level thumbs-up is a clean-result signal; old reactions cannot pass.
    clean = [r for r in reactions if r.get("content") == "+1"
             and r.get("user", {}).get("id") == policy.data["codex_user_id"]
             and finished["time"] < instant(r["created_at"])
             <= finished["time"] + timedelta(minutes=2)]
    require(not any(r.get("content") == "eyes"
                    and r.get("user", {}).get("id") == policy.data["codex_user_id"]
                    for r in reactions), "Codex still has a running-review reaction")
    require(len(clean) == 1, "Waiting for a fresh Codex thumbs-up on the PR")
    # Review objects do not expose performed_via_github_app. The immutable Bot
    # user ID is the native App's identity on this endpoint.
    require(not any(r.get("user", {}).get("id") == policy.data["codex_user_id"]
                    and instant(r["submitted_at"]) >= started["time"]
                    for r in reviews), "Codex posted findings during or after this review")
    for thread in threads:
        require(not thread["comments"]["pageInfo"]["hasNextPage"], "Incomplete review thread")
        require(thread["isResolved"] or not any(policy.native_actor(c["author"])
                for c in thread["comments"]["nodes"]), "Unresolved Codex review thread")

    return {"head": resolved_sha, "summary": comment["id"],
            "completed": finished["time"].isoformat(), "reaction": clean[0]["id"],
            "policy": policy.digest}


def check_rules(api, policy):
    """Verify existing approval and CI requirements without changing review policy."""
    repo = policy.data["repository"]
    branch = urllib.parse.quote(policy.data["base_branch"], safe="")
    rules = api.request(f"/repos/{repo}/rules/branches/{branch}")
    require(any(r["type"] == "required_status_checks"
                and any(c["context"] == "test success"
                        for c in r["parameters"]["required_status_checks"]) for r in rules),
            "The existing test success requirement is missing")
    for rule in rules:
        if rule["type"] != "pull_request":
            continue
        parameters = rule["parameters"]
        if parameters.get("required_approving_review_count", 0) < 1:
            continue
        require(rule.get("ruleset_source_type") == "Repository",
                "Expected a repository ruleset")
        full = api.request(f"/repos/{repo}/rulesets/{int(rule['ruleset_id'])}")
        require(full.get("enforcement") == "active" and not full.get("bypass_actors"),
                "Review ruleset must be active without bypass actors")
        return
    raise Ineligible("The required approval rule is not active")


class Adapter:
    def __init__(self, api, policy, number, writer=None, app_id=0, bot_id=0,
                 trusted_sha=None):
        self.api, self.policy, self.number = api, policy, number
        self.writer = writer
        self.app_id, self.bot_id, self.trusted_sha = app_id, bot_id, trusted_sha
        self.repo = policy.data["repository"]
        self.prefix = f"/repos/{self.repo}"
        self.pull_path = f"{self.prefix}/pulls/{number}"

    def threads(self):
        owner, name = self.repo.split("/")
        query = f"""query($owner:String!,$name:String!,$number:Int!,$cursor:String) {{
          repository(owner:$owner,name:$name) {{ pullRequest(number:$number) {{
            reviewThreads(first:100,after:$cursor) {{
              nodes {{ isResolved comments(first:100) {{ nodes {{ author {{ {ACTOR} }} }}
                pageInfo {{ hasNextPage }} }} }}
              pageInfo {{ hasNextPage endCursor }}
            }}
          }} }}
        }}"""
        result, cursor = [], None
        for _ in range(MAX_PAGES):
            connection = self.api.graphql(query, owner=owner, name=name, number=self.number,
                                          cursor=cursor)["repository"]["pullRequest"]["reviewThreads"]
            result.extend(connection["nodes"])
            if not connection["pageInfo"]["hasNextPage"]:
                return result
            cursor = connection["pageInfo"]["endCursor"]
        raise Ineligible("Review thread pagination limit reached")

    def evidence(self, pull):
        comments = self.api.pages(f"{self.prefix}/issues/{self.number}/comments")
        summaries = [c for c in comments if self.policy.native(c)
                     and c.get("body", "").startswith(SUMMARY_MARKER)]
        require(len(summaries) == 1, "Expected exactly one native Codex summary")
        query = f"""query($id:ID!) {{ node(id:$id) {{ ... on IssueComment {{
          databaseId body lastEditedAt author {{ {ACTOR} }} editor {{ {ACTOR} }}
          userContentEdits(first:100) {{ nodes {{ editedAt diff editor {{ {ACTOR} }} }}
            pageInfo {{ hasNextPage }} }}
        }} }} }}"""
        summary = self.api.graphql(query, id=summaries[0]["node_id"])["node"]
        row = parse_summary(summary["body"])
        # Ask GitHub to resolve the abbreviation; prefix comparison alone is not
        # sufficient. Ambiguous abbreviations make this request fail closed.
        resolved = self.api.request(f"{self.prefix}/commits/{row['sha']}")["sha"]
        reactions = self.api.pages(f"{self.prefix}/issues/{self.number}/reactions")
        reviews = self.api.pages(self.pull_path + "/reviews")
        return check_evidence(self.policy, pull, comments, summary, reactions, reviews,
                              self.threads(), resolved)

    def check_fragment(self, path, head):
        """Read the regular fragment blob at the reviewed head without executing it."""
        tree_sha = head
        parts = path.split("/")
        for index, part in enumerate(parts):
            tree = self.api.request(f"{self.prefix}/git/trees/{tree_sha}")
            require(tree.get("truncated") is False, "Incomplete changelog tree")
            entries = [entry for entry in tree["tree"] if entry["path"] == part]
            require(len(entries) == 1, "Changelog fragment is missing from the current commit")
            entry = entries[0]
            if index < len(parts) - 1:
                require(entry["type"] == "tree" and entry["mode"] == "040000",
                        "Changelog parent must be a directory")
            else:
                require(entry["type"] == "blob" and entry["mode"] == "100644",
                        "Changelog fragment must be a regular non-executable file")
            require(re.fullmatch(r"[0-9a-f]{40}", entry["sha"]) is not None, "Invalid changelog object ID")
            tree_sha = entry["sha"]
        blob = self.api.request(f"{self.prefix}/git/blobs/{tree_sha}")
        require(blob["sha"] == tree_sha and blob["encoding"] == "base64"
                and 0 < blob["size"] <= 65536 and isinstance(blob["content"], str),
                "Unsupported changelog fragment blob")
        try:
            raw = base64.b64decode("".join(blob["content"].splitlines()), validate=True)
            content = raw.decode("utf-8")
        except (binascii.Error, UnicodeError, ValueError) as exc:
            raise Ineligible("Changelog fragment is not valid UTF-8 text") from exc
        require(len(raw) == blob["size"], "Incomplete changelog fragment blob")
        require("release-readiness" not in content.casefold(),
                "Changelog release-policy directives require human review")

    def evaluate(self, enforce_rules=True):
        pull = self.api.request(self.pull_path)
        require(pull["state"] == "open" and not pull["draft"], "PR is closed or a draft")
        require(pull["base"]["repo"]["full_name"] == self.repo
                and pull["base"]["ref"] == self.policy.data["base_branch"], "Unsupported base branch")
        files = self.api.pages(self.pull_path + "/files")
        fragment = self.policy.check_files(files, pull["changed_files"], self.number)
        if fragment:
            self.check_fragment(fragment, pull["head"]["sha"])
        if enforce_rules:
            check_rules(self.api, self.policy)
        receipt = self.evidence(pull)
        again = self.api.request(self.pull_path)
        require(pull["head"]["sha"] == again["head"]["sha"]
                and pull["base"]["sha"] == again["base"]["sha"]
                and again["state"] == "open" and not again["draft"]
                and again["base"]["ref"] == pull["base"]["ref"], "PR changed during evaluation")
        receipt["base"] = pull["base"]["sha"]
        return receipt

    def owned_reviews(self):
        return [r for r in self.api.pages(self.pull_path + "/reviews")
                if r.get("user", {}).get("id") == self.bot_id
                and r.get("user", {}).get("type") == "Bot"
                and r.get("body", "").startswith(RECEIPT_MARKER)]

    def dismiss(self, review_id):
        self.writer.request(f"{self.pull_path}/reviews/{int(review_id)}/dismissals", "PUT", {
            "message": "Native Codex approval evidence is no longer current. A fresh clean review or human approval is needed."
        })

    def check_trusted_revision(self):
        require(self.trusted_sha is not None and re.fullmatch(r"[0-9a-f]{40}", self.trusted_sha),
                "Missing trusted workflow checkout revision")
        branch = urllib.parse.quote(self.policy.data["base_branch"], safe="")
        require(self.api.request(f"{self.prefix}/commits/{branch}")["sha"] == self.trusted_sha,
                "Trusted branch advanced; use a new workflow run")

    def reconcile(self):
        """Re-evaluate before/after writes and only withdraw this adapter's reviews."""
        require(self.writer is not None and self.app_id > 0 and self.bot_id > 0,
                "Approval App identity is not configured")
        owned = self.owned_reviews()
        existing = [r for r in owned if r["state"] == "APPROVED"]
        try:
            self.check_trusted_revision()
            receipt = self.evaluate()
        except (Ineligible, APIError, KeyError, TypeError) as exc:
            for review in existing:
                self.dismiss(review["id"])
            return {"approved": False, "reason": str(exc), "dismissed": len(existing)}
        marker = RECEIPT_MARKER + json.dumps(receipt, sort_keys=True, separators=(",", ":")) + " -->"
        episode_keys = ("head", "summary", "completed", "reaction")
        dismissed = False
        for review in owned:
            if review["state"] != "DISMISSED":
                continue
            try:
                prior = json.loads(review["body"].splitlines()[0][len(RECEIPT_MARKER):-4])
                dismissed |= all(prior[k] == receipt[k] for k in episode_keys)
            except (ValueError, KeyError, TypeError):
                # Do not manufacture a new approval when our prior receipt can
                # no longer be interpreted. A human can review the PR instead.
                dismissed = True
        if dismissed:
            for review in existing:
                self.dismiss(review["id"])
            return {"approved": False, "reason": "This review was dismissed; request a fresh Codex review"}
        keep = [r for r in existing if r["commit_id"] == receipt["head"]
                and r["body"].splitlines()[0] == marker]
        for review in existing:
            if not keep or review["id"] != keep[0]["id"]:
                self.dismiss(review["id"])
        if keep:
            return {"approved": True, "reason": "Current adapter approval already exists"}

        # No PR-provided files, commands, artifacts, or strings are executed.
        # A second snapshot catches pushes/new review requests during API reads.
        try:
            require(self.evaluate() == receipt, "Review evidence changed before approval")
            self.check_trusted_revision()
        except (Ineligible, APIError, KeyError, TypeError) as exc:
            return {"approved": False, "reason": str(exc)}
        body = (marker + "\n\nNative Codex completed a clean review of `" + receipt["head"]
                + "`. Every changed path is eligible for Codex approval.\n\n"
                + f"[Native review summary](https://github.com/{self.repo}/pull/{self.number}"
                + f"#issuecomment-{receipt['summary']}). Existing merge requirements still apply.")
        created = None
        try:
            created = self.writer.request(self.pull_path + "/reviews", "POST", {
                "event": "APPROVE", "commit_id": receipt["head"], "body": body,
            })
            require(created.get("user", {}).get("id") == self.bot_id
                    and created.get("user", {}).get("type") == "Bot",
                    "Approval token does not belong to the configured App")
            require(self.evaluate() == receipt, "Review evidence changed while approving")
            self.check_trusted_revision()
        except (Ineligible, APIError, KeyError, TypeError):
            if created is not None:
                self.dismiss(created["id"])
            else:
                # A timed-out POST may have succeeded. Never retry it blindly.
                for review in self.owned_reviews():
                    if review["state"] == "APPROVED":
                        self.dismiss(review["id"])
            raise
        return {"approved": True, "reason": "Approved the current native Codex review",
                "review_id": created["id"]}


def target_numbers(api, policy, event):
    if event.get("pull_request"):
        return [int(event["pull_request"]["number"])]
    if event.get("issue", {}).get("pull_request"):
        return [int(event["issue"]["number"])]
    if event.get("inputs", {}).get("pr"):
        return [int(event["inputs"]["pr"])]
    # Periodic/default-branch reconciliation also catches missed reaction events,
    # revoked reactions, policy changes, and a previous interrupted runner.
    branch = urllib.parse.quote(policy.data["base_branch"], safe="")
    pulls = api.pages(f"/repos/{policy.data['repository']}/pulls?state=open&base={branch}")
    require(len(pulls) <= 100, "Too many PRs for one bounded reconciliation run")
    return [p["number"] for p in pulls]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pr", type=int)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--event", type=Path)
    parser.add_argument("--wait-seconds", type=int, default=0, choices=range(0, 61), metavar="0..60")
    args = parser.parse_args()
    policy = Policy.load()
    require(os.environ.get("GITHUB_REPOSITORY", policy.data["repository"]) == policy.data["repository"],
            "This policy is only for the canonical repository")
    api = GitHub(os.environ["GH_TOKEN"])
    writer = None
    if args.apply:
        require(os.environ.get("CODEX_APPROVAL_ENABLED") == "true", "Approval writes are disabled")
        # The pinned create-github-app-token action supplies the slug of the App
        # for which it minted the token; check the configured immutable IDs too.
        slug = os.environ["CODEX_APPROVAL_APP_SLUG"]
        require(re.fullmatch(r"[a-z0-9-]+", slug) is not None, "Invalid approval App slug")
        app = api.request(f"/apps/{slug}")
        bot = api.request(f"/users/{slug}%5Bbot%5D")
        require(app["id"] == int(os.environ["CODEX_APPROVAL_APP_ID"])
                and app["client_id"] == os.environ["CODEX_APPROVAL_APP_CLIENT_ID"]
                and bot["id"] == int(os.environ["CODEX_APPROVAL_BOT_ID"])
                and bot["type"] == "Bot", "Approval App configuration does not match its identity")
        writer = GitHub(os.environ["GH_APPROVAL_TOKEN"])
    event = json.loads(args.event.read_text()) if args.event else {}
    numbers = [args.pr] if args.pr is not None else target_numbers(api, policy, event)
    require(all(n > 0 for n in numbers), "PR number must be positive")
    results = []
    for number in numbers:
        adapter = Adapter(api, policy, number,
                          writer=writer, app_id=int(os.environ.get("CODEX_APPROVAL_APP_ID") or 0),
                          bot_id=int(os.environ.get("CODEX_APPROVAL_BOT_ID") or 0),
                          trusted_sha=os.environ.get("TRUSTED_SHA"))
        for attempt in range(2):
            try:
                if writer:
                    result = adapter.reconcile()
                else:
                    receipt = adapter.evaluate(enforce_rules=False)
                    result = {"approved": False, "eligible": True, "receipt": receipt,
                              "reason": "Read-only: evidence and paths qualify; activation rules not checked"}
            except Ineligible as exc:
                result = {"approved": False, "eligible": False, "reason": str(exc)}
            except (APIError, KeyError, TypeError) as exc:
                result = {"approved": False, "error": type(exc).__name__, "reason": "Could not verify GitHub state"}
            # Reactions have no webhook and usually follow the summary by a few
            # seconds. Wait once, only for a single-PR event, then use the schedule.
            if (attempt == 0 and len(numbers) == 1 and args.wait_seconds
                    and "Waiting for a fresh Codex thumbs-up" in result.get("reason", "")):
                time.sleep(args.wait_seconds)
            else:
                break
        results.append({"pr": number, **result})
        print(json.dumps(results[-1], sort_keys=True), flush=True)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as output:
            output.write("### Codex approval adapter\n\n```json\n"
                         + json.dumps(results, indent=2, sort_keys=True) + "\n```\n")
    return int(any("error" in r for r in results))


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (Ineligible, APIError, KeyError, ValueError) as error:
        print(f"Adapter stopped: {type(error).__name__}: {error}", file=sys.stderr)
        sys.exit(1)
