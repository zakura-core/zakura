#!/usr/bin/env python3
"""Offline fixture checks for the upstream sync discovery script."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def result_for(
    candidate: dict[str, object],
    body: str,
    *,
    branch_name: str | None = None,
) -> dict[str, object]:
    return {
        "status": "applied",
        "source_pr": candidate["source_pr"],
        "confidence_percent": 90,
        "recommendation": "Open a draft PR for human review.",
        "branch_name": branch_name or candidate["branch_name"],
        "pr_title": "fix(state): adapt upstream test fixes",
        "pr_body": body,
        "files_changed": ["docs/upstream-sync/ledger.yml"],
        "validation": [
            {
                "command": "fixture validator",
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
            "### AI Disclosure",
            "Codex was used to adapt this change.",
            "### Revert Plan",
            "Revert the generated PR.",
            upstream_pr_marker,
            upstream_merge_marker,
        ]
    )

    assert_validator_passed(run_validator(output_dir, candidate_path, result_for(candidate, valid_body)))

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

    subprocess.check_call(["rm", "-rf", str(output_dir)])
    print("OK: fixture discovery selects upstream PR 10676")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
