"""Tests for refreshing the dashboard's local DB-IP City Lite database."""

import gzip
import io
import subprocess
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from refresh_peer_location_db import DATABASE_NAME, refresh_database


class RefreshDatabaseTests(unittest.TestCase):
    def setUp(self):
        directory = self.enterContext(TemporaryDirectory())
        self.directory = Path(directory) / "geo"
        self.database = self.directory / DATABASE_NAME
        self.month_file = self.directory / "month"
        self.stderr = self.enterContext(redirect_stderr(io.StringIO()))
        self.enterContext(redirect_stdout(io.StringIO()))

    def run_refresh(self, *downloads):
        requested_months = []

        def curl(args, **kwargs):
            url = args[-1]
            requested_months.append(url.removesuffix(".mmdb.gz")[-7:])
            archive = next(responses)
            if archive is None:
                raise subprocess.CalledProcessError(22, args)
            Path(args[args.index("--output") + 1]).write_bytes(archive)

        responses = iter(downloads)
        with patch("refresh_peer_location_db.subprocess.run", side_effect=curl):
            refresh_database(self.directory, date(2026, 1, 2))
        return requested_months

    def install_old_database(self, month):
        self.directory.mkdir()
        self.database.write_bytes(b"installed database")
        self.month_file.write_text(f"{month}\n", encoding="utf-8")

    def test_uses_current_month_when_available(self):
        self.assertEqual(self.run_refresh(gzip.compress(b"January database")), ["2026-01"])
        self.assertEqual(self.database.read_bytes(), b"January database")
        self.assertEqual(self.month_file.read_text(encoding="utf-8"), "2026-01\n")

    def test_skips_download_when_current_month_is_installed(self):
        self.install_old_database("2026-01")
        self.assertEqual(self.run_refresh(), [])
        self.assertEqual(self.database.read_bytes(), b"installed database")

    def test_falls_back_to_previous_year_in_january(self):
        months = self.run_refresh(None, gzip.compress(b"December database"))
        self.assertEqual(months, ["2026-01", "2025-12"])
        self.assertEqual(self.database.read_bytes(), b"December database")
        self.assertEqual(self.month_file.read_text(encoding="utf-8"), "2025-12\n")

    def test_preserves_installed_database_and_month_on_failure(self):
        self.install_old_database("2025-11")
        incomplete_archive = gzip.compress(b"truncated database")[:-4]
        self.assertEqual(self.run_refresh(incomplete_archive, None), ["2026-01", "2025-12"])
        self.assertEqual(self.database.read_bytes(), b"installed database")
        self.assertEqual(self.month_file.read_text(encoding="utf-8"), "2025-11\n")
        self.assertIn("::warning::", self.stderr.getvalue())

    def test_warns_when_no_database_is_available(self):
        self.assertEqual(self.run_refresh(None, None), ["2026-01", "2025-12"])
        self.assertFalse(self.database.exists())
        self.assertFalse(self.month_file.exists())
        self.assertIn("::warning::", self.stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
