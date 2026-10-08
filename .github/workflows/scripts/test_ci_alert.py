#!/usr/bin/env python3
"""Tests for ci_alert.py and the workflow list in ci-alerts.yml."""

import re
import unittest
from pathlib import Path

import ci_alert


WORKFLOWS = Path(__file__).resolve().parent.parent
BASE = "repos/o/r/actions"


def run(number, conclusion, event="push", attempt=1):
    """Returns a minimal workflow run attempt, shaped like the GitHub API's."""
    return {
        "id": number,
        "run_number": number,
        "run_attempt": attempt,
        "status": "completed",
        "conclusion": conclusion,
        "event": event,
        "name": "Unit Tests",
        "html_url": f"https://github.com/o/r/actions/runs/{number}",
        "head_sha": "0123456789abcdef",
        "head_commit": {"message": "fix(x): y\n\nbody", "author": {"name": "A <B>"}},
        "repository": {"html_url": "https://github.com/o/r"},
    }


def watched_workflows():
    """Returns the workflow names listed under `workflow_run.workflows` in ci-alerts.yml."""
    lines = (WORKFLOWS / "ci-alerts.yml").read_text().splitlines()
    watched = []
    for line in lines[lines.index("    workflows:") + 1 :]:
        match = re.fullmatch(r"      - (.+)", line)
        if not match:
            break
        watched.append(match.group(1))
    return watched


class AlertTextTest(unittest.TestCase):
    def alert(self, responses, run_id, attempt):
        """Returns `ci_alert.alert_text` for one attempt, answered from `responses`."""
        return ci_alert.alert_text("o/r", run_id, attempt, api=lambda path: responses[path])

    def test_a_failed_attempt_posts_its_jobs_and_commit(self):
        responses = {
            f"{BASE}/runs/7/attempts/1": run(7, "failure"),
            f"{BASE}/runs/7/attempts/1/jobs?per_page=100": {
                "jobs": [
                    {"name": "lint", "conclusion": "failure"},
                    {"name": "test success", "conclusion": "failure"},
                ]
            },
        }
        self.assertEqual(
            self.alert(responses, 7, 1).splitlines(),
            [
                ":red_circle: *Unit Tests* failed on `main` (push): "
                "<https://github.com/o/r/actions/runs/7|run 7>",
                "Failed jobs: `lint`",
                "Commit: <https://github.com/o/r/commit/0123456789abcdef|012345678> "
                "fix(x): y (A &lt;B&gt;)",
            ],
        )

    def test_timeouts_and_startup_failures_post(self):
        for conclusion in ("timed_out", "startup_failure"):
            responses = {
                f"{BASE}/runs/7/attempts/1": run(7, conclusion),
                f"{BASE}/runs/7/attempts/1/jobs?per_page=100": {"jobs": []},
            }
            self.assertIn("failed on `main`", self.alert(responses, 7, 1))

    def test_an_attempt_that_did_not_fail_posts_nothing(self):
        for conclusion in ("success", "cancelled", "skipped", None):
            responses = {f"{BASE}/runs/7/attempts/1": run(7, conclusion)}
            self.assertIsNone(self.alert(responses, 7, 1))

    def test_the_reported_attempt_is_judged_not_the_latest(self):
        # Attempt 2 has since passed; attempt 1's failure is still reported.
        responses = {
            f"{BASE}/runs/7/attempts/1": run(7, "failure"),
            f"{BASE}/runs/7/attempts/2": run(7, "success", attempt=2),
            f"{BASE}/runs/7/attempts/1/jobs?per_page=100": {"jobs": []},
        }
        self.assertIn("failed on `main`", self.alert(responses, 7, 1))
        self.assertIsNone(self.alert(responses, 7, 2))


class MessageTest(unittest.TestCase):
    def test_scheduled_failure_omits_the_commit(self):
        self.assertEqual(
            ci_alert.message(run(7, "failure", "schedule"), []),
            ":red_circle: *Unit Tests* failed on `main` (scheduled run): "
            "<https://github.com/o/r/actions/runs/7|run 7>",
        )

    def test_long_job_lists_are_truncated(self):
        jobs = [f"job {n}" for n in range(ci_alert.MAX_LISTED_JOBS + 3)]
        text = ci_alert.message(run(7, "failure"), jobs)
        self.assertIn(f"`job {ci_alert.MAX_LISTED_JOBS - 1}` and 3 more", text)
        self.assertNotIn(f"`job {ci_alert.MAX_LISTED_JOBS}`", text)

    def test_job_names_are_escaped(self):
        self.assertIn(
            "Failed jobs: `test &lt;unit&gt;`",
            ci_alert.message(run(7, "failure"), ["test <unit>"]),
        )

    def test_failed_jobs_keeps_only_failures(self):
        jobs = [
            {"name": "a", "conclusion": "success"},
            {"name": "b", "conclusion": "failure"},
            {"name": "c", "conclusion": "timed_out"},
            {"name": "d", "conclusion": "skipped"},
        ]
        self.assertEqual(ci_alert.failed_jobs(jobs), ["b", "c"])

    def test_failed_jobs_hides_the_summary_job_behind_real_failures(self):
        summary = {"name": "test success", "conclusion": "failure"}
        self.assertEqual(
            ci_alert.failed_jobs([{"name": "b", "conclusion": "failure"}, summary]), ["b"]
        )
        self.assertEqual(ci_alert.failed_jobs([summary]), ["test success"])


class WatchedWorkflowsTest(unittest.TestCase):
    def test_every_watched_workflow_exists(self):
        names = set()
        for path in WORKFLOWS.glob("*.yml"):
            match = re.search(r"^name:\s*(.+?)\s*$", path.read_text(), re.MULTILINE)
            if match:
                names.add(match.group(1).strip("'\""))
        watched = watched_workflows()
        self.assertTrue(watched)
        self.assertEqual(sorted(set(watched) - names), [])


if __name__ == "__main__":
    unittest.main()
