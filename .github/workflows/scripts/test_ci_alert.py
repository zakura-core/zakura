#!/usr/bin/env python3
"""Tests for ci_alert.py and the workflow list in ci-alerts.yml."""

import unittest
from pathlib import Path

import yaml

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


# These workflows have their own alert policy. Keep exceptions explicit so a new
# scheduled workflow cannot silently miss the central notifier.
ALERT_EXCEPTIONS = {
    "PR node image bake": "Has a dedicated failure notification job.",
    "VCT handoff canary": "Has phase-aware alerts with persistent deduplication.",
    "Upstream PR Triage Pilot": "Intentionally muted until the pilot passes again.",
}


def workflow_events(workflow):
    """Normalizes YAML event mappings, lists, and single event names."""
    events = workflow["on"]
    if isinstance(events, str):
        return {events: None}
    if isinstance(events, list):
        return dict.fromkeys(events)
    return events


def needs_alert_coverage(workflow):
    """Includes scheduled and branch-push workflows, conservatively ignoring branch filters."""
    events = workflow_events(workflow)
    if "schedule" in events:
        return True
    if "push" not in events:
        return False
    push = events["push"] or {}
    # A tags-only filter disables branch pushes. Path filters do not.
    return not (
        ("tags" in push or "tags-ignore" in push)
        and "branches" not in push
        and "branches-ignore" not in push
    )


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

    def test_continuous_sync_preserves_audit_suppression_but_alerts_other_failures(self):
        """Checks the fallback using the step results exposed by GitHub's jobs API."""
        cases = [
            ("delivered or suppressed incident", "failure", "failure", "success", None, False),
            ("SSH setup failed", "failure", "skipped", "skipped", "Configure SSH", True),
            ("audit crashed or delivery failed", "failure", "failure", "skipped", None, True),
            ("later summary check failed", "failure", "failure", "success", "Check daily summary delivery", True),
            ("summary failed after healthy audit", "failure", "success", "success", "Check daily summary delivery", True),
            ("timeout after handled audit", "timed_out", "failure", "success", None, True),
            ("audit itself timed out", "failure", "timed_out", "success", None, True),
            ("receipt step failed", "failure", "failure", "failure", None, True),
        ]
        for label, conclusion, audit, receipt, other, expected in cases:
            with self.subTest(case=label):
                attempt = run(7, conclusion, event="schedule")
                attempt["path"] = ci_alert.CONTINUOUS_SYNC_PATH
                job = {
                    "name": "Continuous sync audit",
                    "conclusion": conclusion,
                    "steps": [
                        {"name": ci_alert.AUDIT_STEP, "conclusion": audit},
                        {"name": ci_alert.AUDIT_HANDLED_STEP, "conclusion": receipt},
                    ],
                }
                if other:
                    job["steps"].append({"name": other, "conclusion": "failure"})
                responses = {
                    f"{BASE}/runs/7/attempts/1": attempt,
                    f"{BASE}/runs/7/attempts/1/jobs?per_page=100": {"jobs": [job]},
                }
                self.assertEqual(self.alert(responses, 7, 1) is not None, expected)
                # The special receipt must never silence another workflow.
                attempt["path"] = ".github/workflows/tests-unit.yml"
                self.assertIsNotNone(self.alert(responses, 7, 1))

    def test_continuous_sync_missing_job_evidence_alerts(self):
        """Missing steps or jobs must not count as proof of handled notifications."""
        attempt = run(7, "failure", event="schedule")
        attempt["path"] = ci_alert.CONTINUOUS_SYNC_PATH
        for jobs in ([], [{"name": "Continuous sync audit", "conclusion": "failure"}]):
            with self.subTest(jobs=jobs):
                responses = {
                    f"{BASE}/runs/7/attempts/1": attempt,
                    f"{BASE}/runs/7/attempts/1/jobs?per_page=100": {"jobs": jobs},
                }
                self.assertIsNotNone(self.alert(responses, 7, 1))

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
    def test_every_automatic_workflow_has_an_alert_policy(self):
        """Requires coverage or a named exception for every automatic workflow."""
        workflows = {}
        for path in sorted(WORKFLOWS.iterdir()):
            if path.suffix not in {".yml", ".yaml"}:
                continue
            # BaseLoader preserves GitHub's `on` key instead of YAML 1.1's True.
            workflow = yaml.load(path.read_text(), Loader=yaml.BaseLoader)
            self.assertNotIn(workflow["name"], workflows, "Workflow names must be unique")
            workflows[workflow["name"]] = workflow
        watched = workflow_events(workflows["CI alerts"])["workflow_run"]["workflows"]
        automatic = {
            name for name, workflow in workflows.items() if needs_alert_coverage(workflow)
        }
        exceptions = set(ALERT_EXCEPTIONS)
        self.assertTrue(watched)
        self.assertEqual(len(watched), len(set(watched)), "Duplicate watched workflow")
        self.assertEqual(set(watched) - workflows.keys(), set(), "Unknown watched workflow")
        self.assertEqual(exceptions - automatic, set(), "Stale alert exception")
        self.assertEqual(set(watched) & exceptions, set(), "Watched and exempted")
        self.assertEqual(
            automatic - set(watched) - exceptions,
            set(),
            "Add missing workflows to ci-alerts.yml or document an alert exception",
        )

    def test_audit_receipt_matches_the_workflow(self):
        """Pins the API-visible receipt to the audit's output and failed-step name."""
        workflow = yaml.load(
            (WORKFLOWS.parents[1] / ci_alert.CONTINUOUS_SYNC_PATH).read_text(),
            Loader=yaml.BaseLoader,
        )
        steps = {step["name"]: step for step in workflow["jobs"]["fleet"]["steps"]}
        self.assertEqual(steps[ci_alert.AUDIT_STEP]["id"], "audit")
        self.assertEqual(
            steps[ci_alert.AUDIT_HANDLED_STEP]["if"],
            "${{ !cancelled() && steps.audit.outputs.notification_handled == 'true' }}",
        )

    def test_event_forms_and_filters(self):
        """Recognizes YAML trigger forms without mistaking job text for triggers."""
        cases = [
            ("on: push", True),
            ('"on": [pull_request, push]', True),
            ("on: {push: {}}", True),
            ("on: {schedule: [{cron: '0 0 * * *'}]}", True),
            ("on: {push: {branches: [main], paths: ['docs/**']}}", True),
            ("on: {push: {branches-ignore: [release], tags: ['v*']}}", True),
            ("on: {push: {tags: ['v*']}}", False),
            ("on: {push: {tags-ignore: ['v*']}}", False),
            ("on: [pull_request, workflow_dispatch]", False),
            ("on: workflow_call", False),
            ("on: workflow_dispatch\njobs: {example: {name: 'schedule: push'}}", False),
        ]
        for source, expected in cases:
            with self.subTest(source=source):
                workflow = yaml.load(source, Loader=yaml.BaseLoader)
                self.assertEqual(needs_alert_coverage(workflow), expected)


if __name__ == "__main__":
    unittest.main()
