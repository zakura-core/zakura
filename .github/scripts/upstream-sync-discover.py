#!/usr/bin/env python3
"""Discover the next upstream Zebra PR missing from this fork.

The script intentionally emits one candidate at a time. It uses the first
missing upstream commit by compare order, maps that commit to its merged
upstream PR through GitHub's commit-to-PR API, and then records the whole PR as
the candidate.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any


TERMINAL_LEDGER_STATUSES = {"imported", "skipped", "superseded"}


def run(args: list[str], *, cwd: Path | None = None) -> str:
    return subprocess.check_output(args, cwd=cwd, text=True).strip()


def run_json(args: list[str], *, cwd: Path | None = None) -> Any:
    output = run(args, cwd=cwd)
    return json.loads(output) if output else None


def repo_url(repo: str) -> str:
    return f"https://github.com/{repo}.git"


def fetch_ref(repo: str, ref: str, local_ref: str) -> str:
    run(["git", "fetch", "--no-tags", "--force", repo_url(repo), f"{ref}:{local_ref}"])
    return run(["git", "rev-parse", local_ref])


def api_json(path: str) -> Any:
    return run_json(["gh", "api", "-H", "Accept: application/vnd.github+json", path])


def pr_from_commit(source_repo: str, sha: str) -> dict[str, Any] | None:
    pulls = api_json(f"repos/{source_repo}/commits/{sha}/pulls")
    merged = [pull for pull in pulls if pull.get("merged_at")]
    if not merged:
        return None
    merged.sort(key=lambda pull: pull["merged_at"])
    return merged[0]


def pr_metadata(source_repo: str, pr_number: int) -> dict[str, Any]:
    pull = api_json(f"repos/{source_repo}/pulls/{pr_number}")
    commits = api_json(f"repos/{source_repo}/pulls/{pr_number}/commits?per_page=100")
    files = api_json(f"repos/{source_repo}/pulls/{pr_number}/files?per_page=100")
    return {
        "number": pull["number"],
        "title": pull["title"],
        "state": pull["state"],
        "merged_at": pull.get("merged_at"),
        "merge_commit_sha": pull.get("merge_commit_sha"),
        "base_ref": pull["base"]["ref"],
        "head_ref": pull["head"]["ref"],
        "url": pull["html_url"],
        "labels": [label["name"] for label in pull.get("labels", [])],
        "commits": [commit["sha"] for commit in commits],
        "files": [file["filename"] for file in files],
        "additions": pull.get("additions"),
        "deletions": pull.get("deletions"),
        "changed_files": pull.get("changed_files"),
    }


def terminal_prs_from_ledger(path: Path) -> set[int]:
    if not path.exists():
        return set()

    terminal: set[int] = set()
    current_pr: int | None = None

    for line in path.read_text(encoding="utf-8").splitlines():
        pr_match = re.match(r"\s*-\s+upstream_pr:\s+([0-9]+)\s*$", line)
        if pr_match:
            current_pr = int(pr_match.group(1))
            continue

        status_match = re.match(r"\s*status:\s+([A-Za-z0-9_-]+)\s*$", line)
        if current_pr is not None and status_match:
            if status_match.group(1) in TERMINAL_LEDGER_STATUSES:
                terminal.add(current_pr)
            current_pr = None

    return terminal


def existing_marker(source_pr: int, branch: str, target_repo: str) -> dict[str, Any]:
    branch_exists = False
    try:
        branch_exists = bool(run(["git", "ls-remote", "--heads", "origin", branch]))
    except subprocess.CalledProcessError:
        branch_exists = False

    search_query = f'"Upstream-Zebra-PR: {source_pr}" in:body repo:{target_repo}'
    prs: list[dict[str, Any]] = []
    try:
        prs = run_json(
            [
                "gh",
                "pr",
                "list",
                "--repo",
                target_repo,
                "--state",
                "all",
                "--search",
                search_query,
                "--json",
                "number,state,url,headRefName",
            ]
        )
    except subprocess.CalledProcessError:
        prs = []

    return {"branch_exists": branch_exists, "pull_requests": prs}


def blocks_candidate(existing: dict[str, Any]) -> bool:
    """Return true when an existing PR means the upstream PR is already handled."""

    return any(
        pull.get("state") in {"OPEN", "MERGED"}
        for pull in existing.get("pull_requests", [])
    )


def write_source_diffs(source_repo: str, source_pr: int, output_dir: Path) -> None:
    patch_path = output_dir / "source.patch"
    diff_path = output_dir / "source.diff"
    with patch_path.open("w", encoding="utf-8") as patch_file:
        subprocess.check_call(
            ["gh", "pr", "diff", str(source_pr), "--repo", source_repo, "--patch"],
            stdout=patch_file,
        )
    with diff_path.open("w", encoding="utf-8") as diff_file:
        subprocess.check_call(
            ["gh", "pr", "diff", str(source_pr), "--repo", source_repo],
            stdout=diff_file,
        )


def candidate_from_pr(
    *,
    source_repo: str,
    target_repo: str,
    target_ref: str,
    source_ref: str,
    source_ref_sha: str,
    target_ref_sha: str,
    merge_base: str,
    ahead_count: int,
    behind_count: int,
    first_missing_sha: str | None,
    pr: dict[str, Any],
) -> dict[str, Any]:
    source_pr = int(pr["number"])
    branch = f"upstream-sync/pr-{source_pr}"
    return {
        "status": "candidate",
        "source_repo": source_repo,
        "source_ref": source_ref,
        "source_ref_sha": source_ref_sha,
        "target_repo": target_repo,
        "target_ref": target_ref,
        "target_ref_sha": target_ref_sha,
        "merge_base": merge_base,
        "ahead_count": ahead_count,
        "behind_count": behind_count,
        "first_missing_sha": first_missing_sha,
        "source_pr": source_pr,
        "source_pr_title": pr["title"],
        "source_pr_url": pr["url"],
        "source_pr_merged_at": pr["merged_at"],
        "source_merge_commit": pr.get("merge_commit_sha"),
        "source_commits": pr["commits"],
        "source_files": pr["files"],
        "source_labels": pr["labels"],
        "source_additions": pr["additions"],
        "source_deletions": pr["deletions"],
        "source_changed_files": pr["changed_files"],
        "branch_name": branch,
        "pr_title": pr["title"],
        "body_markers": {
            "upstream_pr": f"Upstream-Zebra-PR: {source_pr}",
            "upstream_merge": f"Upstream-Zebra-Merge: {pr.get('merge_commit_sha')}",
        },
    }


def discover_live(args: argparse.Namespace) -> dict[str, Any]:
    if args.limit != 1:
        raise SystemExit("upstream-sync v1 only supports --limit 1")

    source_local_ref = "refs/remotes/upstream-sync/source"
    target_local_ref = "refs/remotes/upstream-sync/target"
    source_sha = fetch_ref(args.source_repo, args.source_ref, source_local_ref)
    target_sha = fetch_ref(args.target_repo, args.target_ref, target_local_ref)
    merge_base = run(["git", "merge-base", target_local_ref, source_local_ref])
    counts = run(
        ["git", "rev-list", "--left-right", "--count", f"{target_local_ref}...{source_local_ref}"]
    )
    ahead_count, behind_count = [int(part) for part in counts.split()]

    ledger_terminal = terminal_prs_from_ledger(args.ledger)
    missing_commits = run(
        ["git", "rev-list", "--reverse", f"{target_local_ref}..{source_local_ref}"]
    ).splitlines()

    selected_pr: dict[str, Any] | None = None
    first_missing_sha: str | None = None

    if args.candidate_pr:
        selected_pr = pr_metadata(args.source_repo, int(args.candidate_pr))
        first_missing_sha = selected_pr["commits"][0] if selected_pr["commits"] else None
    else:
        for sha in missing_commits:
            pull = pr_from_commit(args.source_repo, sha)
            if not pull:
                continue
            pr_number = int(pull["number"])
            if pr_number in ledger_terminal:
                continue
            maybe_pr = pr_metadata(args.source_repo, pr_number)
            maybe_candidate = candidate_from_pr(
                source_repo=args.source_repo,
                target_repo=args.target_repo,
                target_ref=args.target_ref,
                source_ref=args.source_ref,
                source_ref_sha=source_sha,
                target_ref_sha=target_sha,
                merge_base=merge_base,
                ahead_count=ahead_count,
                behind_count=behind_count,
                first_missing_sha=sha,
                pr=maybe_pr,
            )
            existing = existing_marker(
                maybe_candidate["source_pr"],
                maybe_candidate["branch_name"],
                args.target_repo,
            )
            if blocks_candidate(existing):
                continue
            selected_pr = maybe_pr
            first_missing_sha = sha
            break

    if not selected_pr:
        return {
            "status": "no_candidate",
            "source_repo": args.source_repo,
            "source_ref": args.source_ref,
            "source_ref_sha": source_sha,
            "target_repo": args.target_repo,
            "target_ref": args.target_ref,
            "target_ref_sha": target_sha,
            "merge_base": merge_base,
            "ahead_count": ahead_count,
            "behind_count": behind_count,
            "message": "No untracked missing upstream PRs found.",
        }

    candidate = candidate_from_pr(
        source_repo=args.source_repo,
        target_repo=args.target_repo,
        target_ref=args.target_ref,
        source_ref=args.source_ref,
        source_ref_sha=source_sha,
        target_ref_sha=target_sha,
        merge_base=merge_base,
        ahead_count=ahead_count,
        behind_count=behind_count,
        first_missing_sha=first_missing_sha,
        pr=selected_pr,
    )
    candidate["existing"] = existing_marker(candidate["source_pr"], candidate["branch_name"], args.target_repo)
    return candidate


def discover_fixture(args: argparse.Namespace) -> dict[str, Any]:
    fixture = json.loads(args.fixture.read_text(encoding="utf-8"))
    if args.limit != 1:
        raise SystemExit("upstream-sync v1 only supports --limit 1")

    missing = fixture["missing_commits"]
    selected = None
    first_missing_sha = None

    if args.candidate_pr:
        selected = fixture["pulls"][str(args.candidate_pr)]
        first_missing_sha = selected["commits"][0]
    else:
        first = missing[0]
        first_missing_sha = first["sha"]
        selected = fixture["pulls"][str(first["source_pr"])]

    return candidate_from_pr(
        source_repo=fixture["source_repo"],
        target_repo=fixture["target_repo"],
        target_ref=fixture["target_ref"],
        source_ref=fixture["source_ref"],
        source_ref_sha=fixture["source_ref_sha"],
        target_ref_sha=fixture["target_ref_sha"],
        merge_base=fixture["merge_base"],
        ahead_count=fixture["ahead_count"],
        behind_count=fixture["behind_count"],
        first_missing_sha=first_missing_sha,
        pr=selected,
    )


def write_outputs(candidate: dict[str, Any], output_dir: Path, github_output: str | None) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "candidate.json").write_text(
        json.dumps(candidate, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    summary = [
        "## Upstream sync discovery",
        "",
        f"- Status: `{candidate['status']}`",
        f"- Source: `{candidate['source_repo']}:{candidate['source_ref']}`",
        f"- Target: `{candidate['target_repo']}:{candidate['target_ref']}`",
        f"- Compare: `{candidate.get('ahead_count', 0)} ahead / {candidate.get('behind_count', 0)} behind`",
    ]
    if candidate["status"] == "candidate":
        summary.extend(
            [
                f"- Candidate: upstream PR {candidate['source_pr']}",
                f"- Title: `{candidate['source_pr_title']}`",
                f"- Branch: `{candidate['branch_name']}`",
                f"- Merge commit: `{candidate.get('source_merge_commit')}`",
            ]
        )
    else:
        summary.append(f"- Message: {candidate.get('message', '')}")

    (output_dir / "summary.md").write_text("\n".join(summary) + "\n", encoding="utf-8")

    if github_output:
        with open(github_output, "a", encoding="utf-8") as handle:
            handle.write(f"status={candidate['status']}\n")
            handle.write(f"has_candidate={'true' if candidate['status'] == 'candidate' else 'false'}\n")
            handle.write(f"source_pr={candidate.get('source_pr', '')}\n")
            handle.write(f"branch_name={candidate.get('branch_name', '')}\n")
            handle.write(f"pr_title={candidate.get('pr_title', '')}\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-repo", default="ZcashFoundation/zebra")
    parser.add_argument("--source-ref", default="main")
    parser.add_argument("--target-repo", default=os.environ.get("GITHUB_REPOSITORY", "valargroup/zebra"))
    parser.add_argument("--target-ref", default="ironwood-main")
    parser.add_argument("--candidate-pr", default="")
    parser.add_argument("--limit", type=int, default=1)
    parser.add_argument("--output-dir", type=Path, default=Path(".github/upstream-sync/work"))
    parser.add_argument("--ledger", type=Path, default=Path("docs/upstream-sync/ledger.yml"))
    parser.add_argument("--fixture", type=Path)
    parser.add_argument("--write-diffs", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    candidate = discover_fixture(args) if args.fixture else discover_live(args)
    write_outputs(candidate, args.output_dir, os.environ.get("GITHUB_OUTPUT"))
    if args.write_diffs and candidate["status"] == "candidate" and not args.fixture:
        write_source_diffs(args.source_repo, int(candidate["source_pr"]), args.output_dir)
    print(json.dumps(candidate, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
