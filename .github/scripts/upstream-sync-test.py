#!/usr/bin/env python3
"""Offline fixture checks for the upstream sync discovery script."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def load_discover_module():
    module_path = ROOT / ".github" / "scripts" / "upstream-sync-discover.py"
    spec = importlib.util.spec_from_file_location("upstream_sync_discover", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def result_for(
    candidate: dict[str, object],
    body: str,
    *,
    branch_name: str | None = None,
    status: str = "applied",
    validation: list[dict[str, object]] | None = None,
    recommendation: str = "Open a draft PR for human review.",
) -> dict[str, object]:
    return {
        "status": status,
        "source_pr": candidate["source_pr"],
        "confidence_percent": 90,
        "recommendation": recommendation,
        "branch_name": branch_name or candidate["branch_name"],
        "pr_title": "fix(state): adapt upstream test fixes",
        "pr_body": body,
        "files_changed": ["docs/upstream-sync/ledger.yml"],
        "validation": validation if validation is not None else [
            {
                "command": "cargo fmt --all -- --check",
                "status": "passed",
                "output": "fixture",
            },
            {
                "command": "git diff --check",
                "status": "passed",
                "output": "fixture",
            }
        ],
        "risks": [],
        "follow_up": [],
    }


def run_validator(output_dir: Path, candidate_path: Path, result: dict[str, object]) -> subprocess.CompletedProcess[str]:
    result_path = output_dir / "result.json"
    result_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    env = os.environ.copy()
    env["UPSTREAM_SYNC_SKIP_PROTECTED_CHECK"] = "true"
    return subprocess.run(
        [
            str(ROOT / ".github" / "scripts" / "upstream-sync-validate.sh"),
            str(result_path),
            str(candidate_path),
        ],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def run_write_pr_body(output_dir: Path, result: dict[str, object]) -> tuple[subprocess.CompletedProcess[str], Path]:
    result_path = output_dir / "result.json"
    pr_body_path = output_dir / "pr-body.md"
    github_output_path = output_dir / "github-output.txt"
    result_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    env = os.environ.copy()
    env["UPSTREAM_SYNC_RESULT_JSON"] = str(result_path)
    env["UPSTREAM_SYNC_PR_BODY_FILE"] = str(pr_body_path)
    env["GITHUB_OUTPUT"] = str(github_output_path)
    process = subprocess.run(
        [
            str(ROOT / ".github" / "scripts" / "upstream-sync-run.sh"),
            "write-pr-body",
        ],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    return process, pr_body_path


def write_pr_body(output_dir: Path, result: dict[str, object]) -> str:
    process, pr_body_path = run_write_pr_body(output_dir, result)
    if process.returncode != 0:
        print(process.stdout, end="")
        print(process.stderr, end="", file=sys.stderr)
    assert process.returncode == 0
    return pr_body_path.read_text(encoding="utf-8")


def assert_validator_passed(process: subprocess.CompletedProcess[str]) -> None:
    if process.returncode != 0:
        print(process.stdout, end="")
        print(process.stderr, end="", file=sys.stderr)
    assert process.returncode == 0


def assert_validator_failed(process: subprocess.CompletedProcess[str], expected_stderr: str) -> None:
    assert process.returncode != 0
    assert expected_stderr in process.stderr


def main() -> int:
    output_dir = ROOT / ".github" / "upstream-sync" / "work-fixture-test"
    fixture = ROOT / ".github" / "upstream-sync" / "fixtures" / "current-compare.json"
    if output_dir.exists():
        subprocess.check_call(["rm", "-rf", str(output_dir)])

    subprocess.check_call(
        [
            sys.executable,
            str(ROOT / ".github" / "scripts" / "upstream-sync-discover.py"),
            "--fixture",
            str(fixture),
            "--output-dir",
            str(output_dir),
            "--limit",
            "1",
        ],
        cwd=ROOT,
    )

    candidate = json.loads((output_dir / "candidate.json").read_text(encoding="utf-8"))
    candidate_path = output_dir / "candidate.json"
    assert candidate["status"] == "candidate"
    assert candidate["source_pr"] == 10676
    assert candidate["source_merge_commit"].startswith("8ead00cab")
    assert candidate["branch_name"] == "upstream-sync/pr-10676"

    discover = load_discover_module()
    assert not discover.blocks_candidate(
        {"branch_exists": True, "pull_requests": [], "head_pull_requests": []}
    )
    assert not discover.blocks_candidate(
        {
            "branch_exists": False,
            "pull_requests": [{"state": "CLOSED"}],
            "head_pull_requests": [{"state": "CLOSED"}],
        }
    )
    assert discover.blocks_candidate({"branch_exists": False, "pull_requests": [{"state": "OPEN"}]})
    assert discover.blocks_candidate({"branch_exists": False, "pull_requests": [{"state": "MERGED"}]})
    assert discover.blocks_candidate(
        {"branch_exists": True, "pull_requests": [], "head_pull_requests": [{"state": "OPEN"}]}
    )
    assert discover.blocks_candidate(
        {"branch_exists": True, "pull_requests": [], "head_pull_requests": [{"state": "MERGED"}]}
    )
    assert discover.exact_head_pull_requests(
        [
            {"number": 1, "state": "OPEN", "headRefName": "upstream-sync/pr-106760"},
            {"number": 2, "state": "OPEN", "headRefName": "upstream-sync/pr-10676"},
        ],
        "upstream-sync/pr-10676",
    ) == [{"number": 2, "state": "OPEN", "headRefName": "upstream-sync/pr-10676"}]

    upstream_pr_marker = candidate["body_markers"]["upstream_pr"]
    upstream_merge_marker = candidate["body_markers"]["upstream_merge"]
    valid_body = "\n".join(
        [
            "### Motivation",
            "Adapt upstream PR 10676.",
            "### Solution",
            "Carry the behavior into the fork.",
            "### Tests",
            "Fixture validation passed.",
            "AI Disclosure",
            "Codex was used to adapt this change.",
            "### Revert Plan",
            "Revert the generated PR.",
            upstream_pr_marker,
            upstream_merge_marker,
        ]
    )

    valid_result = result_for(candidate, valid_body)
    assert_validator_passed(run_validator(output_dir, candidate_path, valid_result))

    pr_body = write_pr_body(output_dir, valid_result)
    assert pr_body.startswith("AI Confidence: 90% - Open a draft PR for human review.\n\n")

    bad_recommendation = result_for(candidate, valid_body, recommendation="Review #123 before merging.")
    process, _ = run_write_pr_body(output_dir, bad_recommendation)
    assert process.returncode != 0
    assert "final PR body contains a bare issue/PR autolink" in process.stderr

    missing_pr_marker_body = valid_body.replace(f"{upstream_pr_marker}\n", "")
    assert_validator_failed(
        run_validator(output_dir, candidate_path, result_for(candidate, missing_pr_marker_body)),
        f"PR body must include {upstream_pr_marker}",
    )

    wrong_merge_marker_body = valid_body.replace(upstream_merge_marker, "Upstream-Zebra-Merge: deadbeef")
    assert_validator_failed(
        run_validator(output_dir, candidate_path, result_for(candidate, wrong_merge_marker_body)),
        f"PR body must include {upstream_merge_marker}",
    )

    wrong_branch = "upstream-sync/pr-10604"
    assert_validator_failed(
        run_validator(output_dir, candidate_path, result_for(candidate, valid_body, branch_name=wrong_branch)),
        f"result branch_name {wrong_branch} does not match candidate {candidate['branch_name']}",
    )

    missing_fmt_result = result_for(candidate, valid_body)
    missing_fmt_result["validation"] = [
        {
            "command": "git diff --check",
            "status": "passed",
            "output": "fixture",
        }
    ]
    assert_validator_failed(
        run_validator(output_dir, candidate_path, missing_fmt_result),
        "validation must include passing cargo fmt --all -- --check",
    )

    no_edit_result = result_for(candidate, valid_body, status="needs_human", validation=[])
    assert_validator_passed(run_validator(output_dir, candidate_path, no_edit_result))

    subprocess.check_call(["rm", "-rf", str(output_dir)])
    print("OK: fixture discovery selects upstream PR 10676")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
