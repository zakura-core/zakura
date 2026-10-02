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
        "workflow_id": 7,
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


def kind(current, runs, attempts_before=(), newer_attempt=None):
    """Returns the alert kind for `current`, given its earlier attempts (newest first) and runs."""
    newest_first = sorted(runs, key=lambda other: other["run_number"], reverse=True)
    previous, contradicted = ci_alert.verdicts(
        current, iter(attempts_before), newer_attempt, newest_first
    )
    return ci_alert.alert_kind(current, previous, contradicted)


def history_path(page):
    """Returns the API path `ci_alert.history` requests for one page of push history."""
    return (
        f"{BASE}/workflows/7/runs?branch=main&event=push"
        f"&status=completed&per_page={ci_alert.HISTORY_PAGE_SIZE}&page={page}"
    )


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
    def test_push_failure_alerts_once_per_breakage(self):
        self.assertEqual(kind(run(2, "failure"), [run(1, "success")]), "failure")
        self.assertIsNone(kind(run(3, "failure"), [run(1, "success"), run(2, "failure")]))

    def test_scheduled_failure_alerts_every_time(self):
        history = [run(1, "failure", "schedule")]
        self.assertEqual(kind(run(2, "failure", "schedule"), history), "failure")

    def test_first_failure_alerts(self):
        self.assertEqual(kind(run(1, "failure"), []), "failure")

    def test_timeouts_and_startup_failures_count_as_failures(self):
        for conclusion in ("timed_out", "startup_failure"):
            self.assertEqual(kind(run(2, conclusion), [run(1, "success")]), "failure")

    def test_pass_after_failure_is_a_recovery(self):
        self.assertEqual(kind(run(2, "success"), [run(1, "failure")]), "recovery")
        self.assertIsNone(kind(run(2, "success"), [run(1, "success")]))

    def test_the_run_itself_in_history_is_ignored(self):
        current = run(2, "failure")
        self.assertEqual(kind(current, [run(1, "success"), current]), "failure")

    def test_cancelled_runs_neither_alert_nor_count(self):
        self.assertIsNone(kind(run(3, "cancelled"), [run(1, "success")]))
        history = [run(1, "failure"), run(2, "cancelled")]
        self.assertIsNone(kind(run(3, "failure"), history))
        self.assertEqual(kind(run(3, "success"), history), "recovery")

    def test_failures_that_finish_together_alert_once(self):
        # Both runs completed before either alert ran.
        history = [run(100, "success"), run(101, "failure"), run(102, "failure")]
        self.assertEqual(kind(run(101, "failure"), history), "failure")
        self.assertIsNone(kind(run(102, "failure"), history))

    def test_recoveries_that_finish_together_report_once(self):
        history = [run(1, "failure"), run(2, "success"), run(3, "success")]
        self.assertEqual(kind(run(2, "success"), history), "recovery")
        self.assertIsNone(kind(run(3, "success"), history))

    def test_a_newer_opposite_verdict_suppresses_the_alert(self):
        self.assertIsNone(kind(run(2, "failure"), [run(1, "success"), run(3, "success")]))
        self.assertIsNone(kind(run(2, "success"), [run(1, "failure"), run(3, "failure")]))
        # A newer run without a verdict does not count.
        self.assertEqual(
            kind(run(2, "failure"), [run(1, "success"), run(3, "cancelled")]), "failure"
        )

    def test_a_newer_attempt_with_the_opposite_verdict_suppresses_the_alert(self):
        history = [run(4, "success")]
        self.assertIsNone(kind(run(5, "failure"), history, newer_attempt="success"))
        self.assertEqual(kind(run(5, "failure"), history, newer_attempt="failure"), "failure")
        self.assertEqual(kind(run(5, "failure"), history, newer_attempt="cancelled"), "failure")

    def test_rerun_follows_its_newest_decided_attempt(self):
        # Attempt 2 was cancelled, attempt 1 failed.
        attempts = ["cancelled", "failure"]
        history = [run(4, "success")]
        self.assertEqual(
            kind(run(5, "success", attempt=3), history, attempts_before=attempts), "recovery"
        )
        self.assertIsNone(kind(run(5, "failure", attempt=3), history, attempts_before=attempts))

    def test_rerun_without_an_earlier_verdict_falls_back_to_earlier_runs(self):
        current = run(5, "failure", attempt=2)
        self.assertEqual(kind(current, [run(4, "success")], attempts_before=["cancelled"]), "failure")
        self.assertIsNone(kind(current, [run(4, "failure")], attempts_before=["cancelled"]))


class AlertTextTest(unittest.TestCase):
    def alert(self, responses, run_id, attempt):
        """Returns `ci_alert.alert_text` for one attempt, answered from `responses`."""
        return ci_alert.alert_text("o/r", run_id, attempt, api=lambda path: responses[path])

    def test_each_attempt_is_judged_on_its_own_conclusion(self):
        # Attempt 2 had already failed again before attempt 1's alert ran.
        first, second = run(50, "failure"), run(50, "failure", attempt=2)
        responses = {
            f"{BASE}/runs/50/attempts/1": first,
            f"{BASE}/runs/50/attempts/2": second,
            f"{BASE}/runs/50": second,
            history_path(1): {"workflow_runs": [second, run(49, "success")]},
            f"{BASE}/runs/50/attempts/1/jobs?per_page=100": {
                "jobs": [{"name": "lint", "conclusion": "failure"}]
            },
        }
        self.assertIn("failed on `main`", self.alert(responses, 50, 1))
        self.assertIsNone(self.alert(responses, 50, 2))

    def test_previous_verdict_is_found_beyond_the_first_history_page(self):
        size = ci_alert.HISTORY_PAGE_SIZE
        current = run(200, "success")
        cancelled = [run(200 - offset, "cancelled") for offset in range(1, size)]
        responses = {
            f"{BASE}/runs/200/attempts/1": current,
            f"{BASE}/runs/200": current,
            history_path(1): {"workflow_runs": [current] + cancelled},
            history_path(2): {"workflow_runs": [run(200 - size, "failure")]},
        }
        self.assertEqual(
            self.alert(responses, 200, 1),
            ":large_green_circle: *Unit Tests* is passing again on `main` (push): "
            "<https://github.com/o/r/actions/runs/200|run 200>",
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
