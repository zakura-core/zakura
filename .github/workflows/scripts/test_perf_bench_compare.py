#!/usr/bin/env python3
"""Unit tests for the perf-bench A/B summary renderer.

Run directly: python3 .github/workflows/scripts/test_perf_bench_compare.py

Loads the script by path because it is hyphenated and therefore not importable
as a module, matching test_mempool_load.py.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    # Register before exec so anything resolving a class's module through
    # sys.modules (dataclasses, pickle) works on Python 3.12+.
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


compare = load_module("perf_bench_compare", SCRIPTS / "perf-bench-compare.py")


# Distinct 40-char hex shas, so a wrong truncation length is visible.
SHAS = {
    "primary": "1a2b3c4d5e6f7890abcdef1234567890abcdef12",
    "baseline": "fedcba0987654321fedcba0987654321fedcba09",
}


def meta(leg: str, **overrides) -> dict:
    base = {
        "leg": leg,
        "sha": SHAS[leg],
        "bps": 100.0,
        "post_bps": 90.0,
        "verdict": "ok",
        "node_exit_status": 0,
    }
    base.update(overrides)
    return base


class Render(unittest.TestCase):
    def test_summary_matches_the_workflow_output_byte_for_byte(self):
        """Golden output: this is the summary the removed YAML heredoc produced.

        The point of extracting the script was to keep that summary identical,
        so assert the whole string -- a changed column set, truncation length,
        or number format is a regression, not a detail.
        """
        # verdict "" is what perf-bench-run.sh writes when it skips or fails
        # classification, which is every live_head run.
        markdown, comparable = compare.render(
            meta("primary", bps=151.75, post_bps=88.5, verdict="faster"),
            meta("baseline", bps=101.0, post_bps=70.25, verdict=""),
        )
        self.assertTrue(comparable)
        self.assertEqual(
            markdown,
            "## A/B result\n"
            "\n"
            "| leg | ref | blocks/s | post-commit blk/s | verdict |\n"
            "|---|---|---:|---:|---|\n"
            "| baseline | `fedcba098` | 101.0 | 70.25 | n/a |\n"
            "| primary | `1a2b3c4d5` | 151.75 | 88.5 | faster |\n"
            "\n"
            "**Speedup (primary vs baseline): 1.50×** "
            "(101.0 → 151.75 blocks/s, both legs on identical parallel droplets)",
        )

    def test_missing_meta_is_not_comparable(self):
        markdown, comparable = compare.render(None, meta("baseline"))
        self.assertFalse(comparable)
        self.assertIn("nothing to compare", markdown)

    def test_failed_leg_names_the_leg_and_blocks_comparison(self):
        markdown, comparable = compare.render(
            meta("primary"), meta("baseline", node_exit_status=101)
        )
        self.assertFalse(comparable)
        self.assertIn("zakurad failed in baseline", markdown)
        self.assertNotIn("A/B result", markdown)

    def test_both_legs_failed_are_both_named(self):
        markdown, _ = compare.render(
            meta("primary", node_exit_status=1), meta("baseline", node_exit_status=1)
        )
        self.assertIn("primary, baseline", markdown)

    def test_zero_baseline_throughput_does_not_divide_by_zero(self):
        markdown, comparable = compare.render(meta("primary"), meta("baseline", bps=0))
        self.assertTrue(comparable)
        self.assertIn("nan×", markdown)


class LoadMeta(unittest.TestCase):
    def load(self, contents: str | None):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "meta.json"
            if contents is not None:
                path.write_text(contents, encoding="utf-8")
            with contextlib.redirect_stderr(io.StringIO()):
                return compare.load_meta(str(path))

    def test_absent_file(self):
        self.assertIsNone(self.load(None))

    def test_truncated_write_is_treated_as_absent(self):
        # perf-bench-run.sh tolerates a failed meta write, and json.dump
        # truncates before writing, so a zero-length file reaches this script.
        self.assertIsNone(self.load(""))
        self.assertIsNone(self.load('{"leg": "primary", "bps":'))

    def test_valid_file_is_parsed(self):
        self.assertEqual(self.load('{"leg": "primary"}'), {"leg": "primary"})


class Main(unittest.TestCase):
    # Seeded so a truncating open() would be caught: this line must survive.
    EXISTING_OUTPUT = "some_other_step_output=1\n"

    def run_main(self, primary, baseline, primary_raw: str | None = None):
        """Run main() in a temp dir and return (exit code, GITHUB_OUTPUT text)."""
        with tempfile.TemporaryDirectory() as tmp:
            paths = []
            for name, value in (("primary", primary), ("baseline", baseline)):
                path = Path(tmp) / f"{name}.json"
                if value is not None:
                    path.write_text(json.dumps(value), encoding="utf-8")
                paths.append(str(path))
            if primary_raw is not None:
                Path(paths[0]).write_text(primary_raw, encoding="utf-8")
            output = Path(tmp) / "github_output"
            output.write_text(self.EXISTING_OUTPUT, encoding="utf-8")
            os.environ["GITHUB_OUTPUT"] = str(output)
            self.addCleanup(os.environ.pop, "GITHUB_OUTPUT", None)
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(
                io.StringIO()
            ):
                code = compare.main(["perf-bench-compare.py", *paths])
            written = output.read_text(encoding="utf-8")
            # The step output file is shared with every other step in the job.
            self.assertTrue(written.startswith(self.EXISTING_OUTPUT))
            return code, written[len(self.EXISTING_OUTPUT) :]

    def test_comparable_run_sets_the_output_flag(self):
        code, output = self.run_main(meta("primary"), meta("baseline"))
        self.assertEqual(code, 0)
        self.assertEqual(output, "compare=true\n")

    def test_absent_leg_artifact_clears_the_output_flag(self):
        code, output = self.run_main(meta("primary"), None)
        self.assertEqual(code, 0)
        self.assertEqual(output, "compare=false\n")

    def test_unusable_meta_clears_the_flag_instead_of_failing_the_step(self):
        # A traceback here would red the compare job and skip the CPU diff.
        code, output = self.run_main(None, meta("baseline"), primary_raw="")
        self.assertEqual(code, 0)
        self.assertEqual(output, "compare=false\n")

    def test_wrong_argument_count_is_a_usage_error(self):
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(compare.main(["perf-bench-compare.py"]), 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
