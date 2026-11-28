#!/usr/bin/env python3
"""Tests for `link_rot.py`."""

import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import link_rot

START = datetime(2026, 11, 1, 7, 23, tzinfo=timezone.utc)
DEAD = "https://example.org/dead"


def report(*urls):
    """Build a lychee JSON report in which `urls` failed."""
    return {
        "error_map": {
            "README.md": [
                {"url": url, "status": {"text": "Rejected status code: 502 Bad Gateway"}}
                for url in urls
            ]
        },
        "timeout_map": {},
    }


class LinkRot(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.report = Path(self.dir.name, "lychee.json")
        self.state = Path(self.dir.name, "history", "state.json")
        os.environ.pop("GITHUB_STEP_SUMMARY", None)

    def run_day(self, day, *urls):
        """Run the script for a daily check `day` days after START."""
        self.report.write_text(json.dumps(report(*urls)))
        return link_rot.main(
            ["--report", str(self.report), "--state", str(self.state), "--days", "7"],
            now=START + timedelta(days=day),
        )

    def test_fails_only_after_seven_days_of_daily_failures(self):
        for day in range(7):
            self.assertEqual(self.run_day(day, DEAD), 0, f"day {day}")
        self.assertEqual(self.run_day(7, DEAD), 1)

    def test_one_passing_run_resets_the_clock(self):
        for day in range(6):
            self.run_day(day, DEAD)
        self.run_day(6)
        self.assertEqual(json.loads(self.state.read_text()), {})
        for day in range(7, 14):
            self.assertEqual(self.run_day(day, DEAD), 0, f"day {day}")
        self.assertEqual(self.run_day(14, DEAD), 1)


if __name__ == "__main__":
    unittest.main()
