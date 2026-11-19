"""The sim host's cleanup: what it deletes by age, what it deletes for disk space, and what it keeps."""

import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "mutation_joint_cleanup.py"
SPEC = importlib.util.spec_from_file_location("mutation_joint_cleanup", SCRIPT)
cleanup = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cleanup)

NOW = 1_800_000_000
DAY = 86400


def make(path, age_days, files=("file",)):
    """Create directory `path` holding `files`, each changed `age_days` ago."""
    path.mkdir(parents=True, exist_ok=True)
    for name in files:
        (path / name).write_bytes(b"x" * 4096)
    for entry in [path, *path.rglob("*")]:
        os.utime(entry, (NOW - age_days * DAY, NOW - age_days * DAY))
    return path


def target(path, age_days):
    return make(path, age_days, ("CACHEDIR.TAG", ".rustc_info.json", "lib.rlib"))


def store(path, age_days):
    make(path / "builds" / ("b" * 64), age_days, ("fuzzer", "build.json"))
    make(path / "snapshots", age_days)
    return make(path, age_days, ("store.lock",))


class CleanupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.state = self.root / "mutation-joint" / "state"

    def tearDown(self):
        self.temp.cleanup()

    def run_cleanup(self, used=10, available=90, **overrides):
        options = {"max_age_days": 3, "disk_limit": 80, "busy_hours": 6}
        options.update(overrides)
        with contextlib.redirect_stdout(io.StringIO()):
            deleted = cleanup.cleanup(
                [str(self.root)], [str(self.state)], [str(self.root / "pw-*")],
                now=NOW, usage=lambda _: (used, available), **options,
            )
        return {(entry["kind"], str(Path(entry["path"]).relative_to(self.root)), entry["reason"])
                for entry in deleted}

    def test_old_targets_runs_and_dead_builds_go_and_kept_paths_stay(self):
        target(self.root / "old" / "target", 5)
        target(self.root / "old" / "target" / "x86_64-unknown-linux-gnu", 5)
        target(self.root / "fresh" / "target", 1)
        make(self.root / ".cargo" / "registry", 30, ("CACHEDIR.TAG",))
        target(self.root / "pw-target", 30)
        store(self.root / "fuzz" / "out" / "store", 4)
        make(self.state, 30, ("cumulative.json",))
        make(self.state / "runs" / "20261101T040000Z-aaaa", 10)
        make(self.state / "runs" / "20261102T040000Z-bbbb", 9)
        make(self.state / "runs" / "20261119T040000Z-cccc", 1)
        (self.state / "latest.json").write_text(json.dumps({"pinned": "b" * 40,
                                                           "run": str(self.state / "runs" / "20261102T040000Z-bbbb")}))
        store(self.state / "campaigns" / "sync_wire", 30)
        make(self.state / "archive", 30)

        self.assertEqual(self.run_cleanup(), {
            ("target", "old/target", "age"),
            ("builds", "fuzz/out/store/builds", "age"),
            ("run", "mutation-joint/state/runs/20261101T040000Z-aaaa", "age"),
        })
        self.assertFalse((self.root / "old" / "target").exists())
        self.assertTrue((self.root / "fuzz" / "out" / "store" / "snapshots").exists())
        for path in ["fresh/target", ".cargo/registry", "pw-target", "mutation-joint/state/runs/20261102T040000Z-bbbb",
                     "mutation-joint/state/cumulative.json", "mutation-joint/state/campaigns/sync_wire/builds",
                     "mutation-joint/state/archive"]:
            self.assertTrue((self.root / path).exists(), path)

    def test_a_full_disk_loses_the_least_recently_changed_first_and_never_a_busy_one(self):
        target(self.root / "a" / "target", 2)
        target(self.root / "b" / "target", 1)
        target(self.root / "c" / "target", 0.1)
        size = cleanup.scan(str(self.root / "a" / "target"))[1]
        # 81% full: deleting the oldest target is enough.
        self.assertEqual(self.run_cleanup(used=81 * size, available=19 * size, dry_run=True),
                         {("target", "a/target", "disk")})
        # 99% full: both idle targets go, and the busy one stays.
        self.assertEqual(self.run_cleanup(used=99 * size, available=size),
                         {("target", "a/target", "disk"), ("target", "b/target", "disk")})
        self.assertTrue((self.root / "c" / "target").exists())

    def test_a_dry_run_deletes_nothing(self):
        target(self.root / "old" / "target", 5)
        self.assertEqual(self.run_cleanup(dry_run=True), {("target", "old/target", "age")})
        self.assertTrue((self.root / "old" / "target").exists())


if __name__ == "__main__":
    unittest.main()
