#!/usr/bin/env python3
"""Refresh the dashboard's local DB-IP City Lite database."""

import argparse
import gzip
import shutil
import subprocess
import sys
import tempfile
import zlib
from datetime import UTC, datetime, timedelta
from pathlib import Path

DATABASE_NAME = "dbip-city-lite.mmdb"


def refresh_database(directory, today):
    """Install the newest available monthly database and record its month.

    Stage the download and decompression alongside the installed files so a failed
    download leaves both files intact. A missing download is optional for the dashboard.
    """
    database = directory / DATABASE_NAME
    month_file = directory / "month"
    current_month = today.strftime("%Y-%m")
    if (
        database.is_file()
        and month_file.is_file()
        and month_file.read_text(encoding="utf-8").strip() == current_month
    ):
        return

    directory.mkdir(mode=0o755, parents=True, exist_ok=True)
    # DB-IP can publish the new database a few days into the month.
    previous_month = (today.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
    with tempfile.TemporaryDirectory(dir=directory) as tmp:
        staging = Path(tmp)
        archive = staging / "database.gz"
        unpacked = staging / DATABASE_NAME
        for month in (current_month, previous_month):
            url = f"https://download.db-ip.com/free/dbip-city-lite-{month}.mmdb.gz"
            try:
                subprocess.run(
                    [
                        "curl", "--fail", "--silent", "--show-error", "--location",
                        "--max-time", "180", "--output", str(archive), url,
                    ],
                    check=True,
                    timeout=185,
                )
                with gzip.open(archive, "rb") as source, unpacked.open("wb") as target:
                    shutil.copyfileobj(source, target)
            except (subprocess.SubprocessError, gzip.BadGzipFile, EOFError, zlib.error) as error:
                print(f"Could not fetch DB-IP City Lite {month}: {error}", file=sys.stderr)
                continue

            staged_month = staging / "month"
            staged_month.write_text(f"{month}\n", encoding="utf-8")
            unpacked.chmod(0o644)
            staged_month.chmod(0o644)
            unpacked.replace(database)
            staged_month.replace(month_file)
            print(f"peer location database: DB-IP City Lite {month}")
            return

    kept = "the old copy" if database.is_file() else "no locations"
    print(f"::warning::could not download the DB-IP City Lite database; keeping {kept}", file=sys.stderr)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="directory for the database and its month file")
    args = parser.parse_args(argv)
    refresh_database(args.directory, datetime.now(UTC).date())


if __name__ == "__main__":
    main()
