"""Tests for scripts/sim-long-runs.sh with a stub `cargo` that records its calls."""

import json
import os
import shutil
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "sim-long-runs.sh"

# The stub logs each call, fails `planted` with $STUB_PLANTED_RC, and refuses a
# `start` in a store that already holds a campaign, as `prepare_start` does.
STUB = r"""#!/usr/bin/env bash
printf '%s\n' "$*" >> "${STUB_LOG}"
case " $* " in
    *" planted "*) exit "${STUB_PLANTED_RC:-0}" ;;
esac
flag() {
    local want="$1"
    shift
    while [ $# -gt 0 ]; do
        if [ "$1" = "${want}" ]; then echo "$2"; return; fi
        shift
    done
}
case " $* " in
    *" sim-campaign build "*)
        target="$(flag --target "$@")"
        echo "saved fuzz target ${target} (address) from commit c as build $(printf '%s' "${target}" | sha256sum | cut -c1-64)"
        ;;
    *" sim-campaign start "*)
        store="$(flag --store "$@")"
        if [ -e "${store}/campaign" ]; then
            echo "${store} already holds a campaign" >&2
            exit 1
        fi
        [ "${ASAN_OPTIONS:-}" = "detect_leaks=1" ] || exit 3
        touch "${store}/campaign"
        exit "${STUB_START_RC:-0}"
        ;;
esac
"""


@unittest.skipUnless(shutil.which("jq"), "the script's campaign phase needs jq")
class SimLongRunsTest(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir)
        bin_dir = self.dir / "bin"
        bin_dir.mkdir()
        stub = bin_dir / "cargo"
        stub.write_text(STUB, encoding="utf-8")
        stub.chmod(0o755)
        self.log = self.dir / "calls.log"
        self.env = dict(
            os.environ,
            PATH=f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
            STUB_LOG=str(self.log),
        )
        matrix = subprocess.run(
            ["python3", str(ROOT / "scripts" / "check-fuzz-campaign.py"), "--matrix", "sim"],
            check=True,
            capture_output=True,
            text=True,
        )
        self.targets = [row["target"] for row in json.loads(matrix.stdout)["include"]]

    def run_script(self, *args, cwd=None, **env):
        return subprocess.run(
            ["bash", str(SCRIPT), *args],
            cwd=cwd or self.dir,
            env={**self.env, **env},
            capture_output=True,
            text=True,
        )

    def calls(self, verb):
        return [line for line in self.log.read_text().splitlines() if f" {verb} " in f" {line} "]

    def test_every_phase_runs_and_passes(self):
        result = self.run_script("--out", "runs", "--seconds", "5")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(len(self.calls("planted")), 2)
        self.assertEqual(len(self.calls("start")), len(self.targets))

    def test_a_failed_planted_phase_still_runs_the_campaigns(self):
        result = self.run_script("--out", "runs", "--seconds", "5", STUB_PLANTED_RC="1")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertEqual(len(self.calls("planted")), 2)
        self.assertEqual(len(self.calls("start")), len(self.targets))

    def test_a_failed_campaign_fails_the_script(self):
        result = self.run_script("--out", "runs", "--seconds", "5", STUB_START_RC="1")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertEqual(len(self.calls("start")), len(self.targets))

    def test_every_build_precedes_every_campaign(self):
        result = self.run_script("--out", "runs", "--seconds", "5")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        verbs = [
            "build" if " build " in f" {line} " else "start"
            for line in self.calls("sim-campaign")
        ]
        self.assertEqual(verbs, ["build"] * len(self.targets) + ["start"] * len(self.targets))

    def test_a_later_run_with_the_same_out_starts_fresh_campaigns(self):
        first = self.run_script("--out", "runs", "--seconds", "5")
        self.assertEqual(first.returncode, 0, first.stdout + first.stderr)
        # The run directory's name has one-second resolution.
        time.sleep(1.1)
        second = self.run_script("--out", "runs", "--seconds", "5")
        self.assertEqual(second.returncode, 0, second.stdout + second.stderr)
        self.assertEqual(len(list((self.dir / "runs").iterdir())), 2)

    def test_a_relative_out_is_the_callers(self):
        result = self.run_script("--out", "runs")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        (run,) = (self.dir / "runs").iterdir()
        self.assertTrue((run / "planted.log").is_file())
        self.assertEqual(self.calls("sim-campaign"), [])


if __name__ == "__main__":
    unittest.main()
