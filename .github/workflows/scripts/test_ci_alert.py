#!/usr/bin/env python3
"""Tests for ci_alert.py and the workflow list in ci-alerts.yml."""

import re
import unittest
from pathlib import Path

import ci_alert


WORKFLOWS = Path(__file__).resolve().parent.parent


def run(number, conclusion, event="push", attempt=1):
    """Returns a minimal workflow run, shaped like the GitHub API's."""
    return {
        "run_number": number,
        "run_attempt": attempt,
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


class AlertKindTest(unittest.TestCase):
    def kind(self, current, history, previous_attempt=None):
        """Returns the alert kind for `current`, given the completed runs in `history`."""
        return ci_alert.alert_kind(
            current,
            ci_alert.previous_conclusion(current, history, previous_attempt),
            ci_alert.superseded(current, history),
        )

    def test_push_failure_alerts_once_per_breakage(self):
        self.assertEqual(self.kind(run(2, "failure"), [run(1, "success")]), "failure")
        self.assertIsNone(self.kind(run(3, "failure"), [run(1, "success"), run(2, "failure")]))

    def test_scheduled_failure_alerts_every_time(self):
        history = [run(1, "failure", "schedule")]
        self.assertEqual(self.kind(run(2, "failure", "schedule"), history), "failure")

    def test_first_failure_alerts(self):
        self.assertEqual(self.kind(run(1, "failure"), []), "failure")

    def test_timeouts_and_startup_failures_count_as_failures(self):
        for conclusion in ("timed_out", "startup_failure"):
            self.assertEqual(self.kind(run(2, conclusion), [run(1, "success")]), "failure")

    def test_pass_after_failure_is_a_recovery(self):
        self.assertEqual(self.kind(run(2, "success"), [run(1, "failure")]), "recovery")
        self.assertIsNone(self.kind(run(2, "success"), [run(1, "success")]))

    def test_the_run_itself_in_history_is_ignored(self):
        current = run(2, "failure")
        self.assertEqual(self.kind(current, [run(1, "success"), current]), "failure")

    def test_cancelled_runs_neither_alert_nor_count(self):
        self.assertIsNone(self.kind(run(3, "cancelled"), [run(1, "success")]))
        history = [run(1, "failure"), run(2, "cancelled")]
        self.assertIsNone(self.kind(run(3, "failure"), history))
        self.assertEqual(self.kind(run(3, "success"), history), "recovery")

    def test_superseded_runs_are_not_reported(self):
        self.assertIsNone(self.kind(run(2, "failure"), [run(1, "success"), run(3, "success")]))
        # A newer run without a verdict does not supersede.
        self.assertEqual(
            self.kind(run(2, "failure"), [run(1, "success"), run(3, "cancelled")]), "failure"
        )

    def test_rerun_follows_its_previous_attempt(self):
        history = [run(1, "success")]
        self.assertEqual(
            self.kind(run(2, "success", attempt=2), history, previous_attempt="failure"),
            "recovery",
        )
        self.assertIsNone(
            self.kind(run(2, "failure", attempt=2), history, previous_attempt="failure")
        )


class MessageTest(unittest.TestCase):
    def test_push_failure_names_jobs_and_commit(self):
        text = ci_alert.message("failure", run(7, "failure"), ["lint", "test <unit>"])
        self.assertEqual(
            text.splitlines(),
            [
                ":red_circle: *Unit Tests* failed on `main` (push): "
                "<https://github.com/o/r/actions/runs/7|run 7>",
                "Failed jobs: `lint`, `test &lt;unit&gt;`",
                "Commit: <https://github.com/o/r/commit/0123456789abcdef|012345678> "
                "fix(x): y (A &lt;B&gt;)",
            ],
        )

    def test_scheduled_failure_omits_the_commit(self):
        text = ci_alert.message("failure", run(7, "failure", "schedule"), [])
        self.assertEqual(
            text,
            ":red_circle: *Unit Tests* failed on `main` (scheduled run): "
            "<https://github.com/o/r/actions/runs/7|run 7>",
        )

    def test_long_job_lists_are_truncated(self):
        jobs = [f"job {n}" for n in range(ci_alert.MAX_LISTED_JOBS + 3)]
        text = ci_alert.message("failure", run(7, "failure"), jobs)
        self.assertIn(f"`job {ci_alert.MAX_LISTED_JOBS - 1}` and 3 more", text)
        self.assertNotIn(f"`job {ci_alert.MAX_LISTED_JOBS}`", text)

    def test_recovery(self):
        self.assertEqual(
            ci_alert.message("recovery", run(8, "success"), []),
            ":large_green_circle: *Unit Tests* is passing again on `main` (push): "
            "<https://github.com/o/r/actions/runs/8|run 8>",
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
