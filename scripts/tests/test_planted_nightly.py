"""Tests for scripts/planted-nightly.py, with a fake `gh` and a small repository."""

import contextlib
import datetime
import io
import importlib.util
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "planted-nightly.py"
SPEC = importlib.util.spec_from_file_location("planted_nightly", SCRIPT)
NIGHTLY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(NIGHTLY)

URL = "https://example.test/runs/9"


def job(shard, conclusion):
    return {"name": f"zakura_sim planted shard {shard}/7", "conclusion": conclusion}


class FakeGh:
    """Answers `gh` with the run's `jobs` and the open `issues`, and records
    each call."""

    def __init__(self, jobs, issues=(), runs=None):
        self.jobs = jobs
        self.issues = list(issues)
        self.runs = runs
        self.calls = []

    def __call__(self, *args):
        self.calls.append(args)
        if args[0] == "api":
            return json.dumps([{"jobs": self.jobs[:3]}, {"jobs": self.jobs[3:]}])
        if args[:2] == ("issue", "list"):
            return json.dumps(self.issues)
        if args[:2] == ("run", "list"):
            if self.runs is None:
                raise subprocess.CalledProcessError(1, "gh")
            return json.dumps(self.runs)
        return ""

    def writes(self):
        return [call[:3] for call in self.calls if call[1] in ("comment", "create", "close")]


class Report(unittest.TestCase):
    def test_a_changed_cases_pass_leaves_another_shards_issue_open(self):
        # Shard 2 failed on Tuesday. On Wednesday shard 3 runs in full, and
        # shard 2 runs only its changed cases, which pass.
        gh = FakeGh(
            [job(index, "success") for index in range(7)],
            [{"number": 5, "title": NIGHTLY.title(2)}],
        )
        NIGHTLY.report(3, 9, 1, URL, run_gh=gh)
        self.assertEqual(gh.writes(), [])

    def test_only_the_full_shards_pass_closes_its_issue(self):
        gh = FakeGh(
            [job(index, "success") for index in range(7)],
            [
                {"number": 5, "title": NIGHTLY.title(2)},
                {"number": 6, "title": NIGHTLY.title(3)},
            ],
        )
        NIGHTLY.report(2, 9, 1, URL, run_gh=gh)
        self.assertEqual(
            gh.writes(),
            [("issue", "comment", "5"), ("issue", "close", "5")],
        )

    def test_a_failed_or_cancelled_shard_opens_or_updates_its_own_issue(self):
        jobs = [job(index, "success") for index in range(7)]
        jobs[1] = job(1, "failure")
        jobs[4] = job(4, "cancelled")
        jobs[6] = job(6, "skipped")
        gh = FakeGh(jobs, [{"number": 8, "title": NIGHTLY.title(4)}])
        NIGHTLY.report(4, 9, 1, URL, run_gh=gh)
        self.assertEqual(
            gh.writes(),
            [("issue", "create", "--title"), ("issue", "comment", "8")],
        )
        create = next(call for call in gh.calls if call[1] == "create")
        self.assertEqual(create[3], NIGHTLY.title(1))
        self.assertIn("its changed cases", create[5])

    def test_a_run_without_the_full_shard_fails(self):
        gh = FakeGh([job(index, "success") for index in range(6)])
        with self.assertRaises(SystemExit):
            NIGHTLY.report(6, 9, 1, URL, run_gh=gh)
        self.assertEqual(gh.writes(), [])

    def test_other_jobs_and_counts_are_not_shards(self):
        jobs = [
            {"name": "zakura_sim planted plan", "conclusion": "failure"},
            {"name": "zakura_sim planted shard 1/3", "conclusion": "failure"},
            job(0, "success"),
        ]
        self.assertEqual(NIGHTLY.outcomes(jobs), {0: "success"})


class Base(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        cwd = os.getcwd()
        os.chdir(self.root)
        self.addCleanup(os.chdir, cwd)
        self.git("init", "-q")
        self.commits = [self.commit(day) for day in (1, 5, 12)]
        self.now = datetime.datetime(2026, 1, 14, tzinfo=datetime.timezone.utc)
        quiet = contextlib.redirect_stderr(io.StringIO())
        quiet.__enter__()
        self.addCleanup(quiet.__exit__, None, None, None)

    def git(self, *args, date=None):
        env = dict(os.environ)
        if date:
            env.update(GIT_AUTHOR_DATE=date, GIT_COMMITTER_DATE=date)
        return subprocess.run(
            ["git", "-c", "user.name=t", "-c", "user.email=t@t", *args],
            check=True, capture_output=True, text=True, env=env,
        ).stdout.strip()

    def commit(self, day):
        self.git("commit", "-q", "--allow-empty", "-m", f"day {day}", date=f"2026-01-{day:02}T00:00:00Z")
        return self.git("rev-parse", "HEAD")

    def test_the_previous_completed_nightly_is_the_base(self):
        runs = [
            {"databaseId": 9, "headSha": self.commits[2], "conclusion": None},
            {"databaseId": 8, "headSha": self.commits[2], "conclusion": "cancelled"},
            {"databaseId": 7, "headSha": self.commits[0], "conclusion": "failure"},
        ]
        self.assertEqual(NIGHTLY.base(9, FakeGh([], runs=runs), self.now), self.commits[0])

    def test_an_unknown_nightly_falls_back_to_a_week_ago(self):
        week_ago = self.commits[1]
        for runs in (None, [], [{"databaseId": 7, "headSha": "f" * 40, "conclusion": "success"}]):
            self.assertEqual(NIGHTLY.base(9, FakeGh([], runs=runs), self.now), week_ago, runs)

    def test_a_short_history_falls_back_to_the_root(self):
        now = datetime.datetime(2026, 1, 3, tzinfo=datetime.timezone.utc)
        self.assertEqual(NIGHTLY.base(9, FakeGh([], runs=[]), now), self.commits[0])


if __name__ == "__main__":
    unittest.main()
