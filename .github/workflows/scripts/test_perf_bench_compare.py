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


def meta(leg: str, **overrides) -> dict:
    base = {
        "leg": leg,
        "sha": f"{leg[0]}" * 40,
        "bps": 100.0,
        "post_bps": 90.0,
        "verdict": "ok",
        "node_exit_status": 0,
    }
    base.update(overrides)
    return base


class Render(unittest.TestCase):
    def test_reports_speedup_and_both_legs(self):
        markdown, comparable = compare.render(meta("primary", bps=150.0), meta("baseline"))
        self.assertTrue(comparable)
        self.assertIn("## A/B result", markdown)
        self.assertIn("1.50×", markdown)
        # Baseline row first, so the table reads baseline -> primary.
        self.assertLess(markdown.index("| baseline |"), markdown.index("| primary |"))

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

    def test_missing_verdict_renders_as_not_available(self):
        markdown, _ = compare.render(meta("primary", verdict=None), meta("baseline"))
        self.assertIn("| n/a |", markdown)


class Main(unittest.TestCase):
    def run_main(self, primary: dict | None, baseline: dict | None):
        """Run main() in a temp dir and return (exit code, GITHUB_OUTPUT text)."""
        with tempfile.TemporaryDirectory() as tmp:
            paths = []
            for name, value in (("primary", primary), ("baseline", baseline)):
                path = Path(tmp) / f"{name}.json"
                if value is not None:
                    path.write_text(json.dumps(value), encoding="utf-8")
                paths.append(str(path))
            output = Path(tmp) / "github_output"
            output.touch()
            os.environ["GITHUB_OUTPUT"] = str(output)
            try:
                with contextlib.redirect_stdout(io.StringIO()):
                    code = compare.main(["perf-bench-compare.py", *paths])
            finally:
                del os.environ["GITHUB_OUTPUT"]
            return code, output.read_text(encoding="utf-8")

    def test_comparable_run_sets_the_output_flag(self):
        code, output = self.run_main(meta("primary"), meta("baseline"))
        self.assertEqual(code, 0)
        self.assertEqual(output, "compare=true\n")

    def test_absent_leg_artifact_clears_the_output_flag(self):
        code, output = self.run_main(meta("primary"), None)
        self.assertEqual(code, 0)
        self.assertEqual(output, "compare=false\n")

    def test_wrong_argument_count_is_a_usage_error(self):
        self.assertEqual(compare.main(["perf-bench-compare.py"]), 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
