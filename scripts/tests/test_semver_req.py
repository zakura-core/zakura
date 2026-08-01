"""Tests for scripts/lib/semver-req.sh (semver_req_matches).

The library drives cascade planning in prepare-release.sh: it decides
whether a published crate's index requirement can select a version being
published. The rule that caused the v1.1.0-rc0 unpublishable graph — a
requirement without a pre-release tag never matches a pre-release — must
hold here.

Run: python3 -m unittest discover -s scripts/tests -p 'test_*.py'
"""

import pathlib
import subprocess
import unittest

LIB = pathlib.Path(__file__).resolve().parents[1] / "lib" / "semver-req.sh"


def req_matches(req: str, version: str) -> int:
    """Return semver_req_matches' exit status (0 match, 1 no match, 2 error)."""
    result = subprocess.run(
        [
            "bash",
            "-c",
            'source "$1" && semver_req_matches "$2" "$3"',
            "bash",
            str(LIB),
            req,
            version,
        ],
        capture_output=True,
        text=True,
    )
    return result.returncode


class TestPrereleaseRules(unittest.TestCase):
    """The rules that make release-candidate graphs special."""

    def test_stable_req_never_matches_prerelease(self):
        # The v1.1.0-rc0 failure: index zakura-node-services 3.0.0 pins
        # zakura-chain ^3.0.0, which cannot select 3.1.0-rc0.
        self.assertEqual(req_matches("^3.0.0", "3.1.0-rc0"), 1)
        self.assertEqual(req_matches("3.0.0", "3.0.1-rc0"), 1)
        self.assertEqual(req_matches("3.0.0", "3.0.0-rc0"), 1)

    def test_prerelease_req_matches_same_core_only(self):
        self.assertEqual(req_matches("3.1.0-rc0", "3.1.0-rc0"), 0)
        self.assertEqual(req_matches("3.1.0-rc0", "3.1.0-rc1"), 0)
        self.assertEqual(req_matches("3.1.0-rc1", "3.1.0-rc0"), 1)
        self.assertEqual(req_matches("3.1.0-rc0", "3.2.0-rc0"), 1)
        self.assertEqual(req_matches("3.1.0-rc0", "4.0.0-rc0"), 1)

    def test_stable_satisfies_prerelease_lower_bound(self):
        # De-rc'd requirements: ^3.1.0-rc0 matches the stable fold and
        # later compatible versions, but never another major.
        self.assertEqual(req_matches("3.1.0-rc0", "3.1.0"), 0)
        self.assertEqual(req_matches("3.1.0-rc0", "3.2.0"), 0)
        self.assertEqual(req_matches("3.1.0-rc0", "4.0.0"), 1)

    def test_numeric_prerelease_identifiers(self):
        self.assertEqual(req_matches("1.0.0-rc.9", "1.0.0-rc.10"), 0)
        self.assertEqual(req_matches("1.0.0-rc.10", "1.0.0-rc.9"), 1)


class TestCaretRules(unittest.TestCase):
    def test_compatible_range(self):
        self.assertEqual(req_matches("^3.0.0", "3.0.1"), 0)
        self.assertEqual(req_matches("3.0.0", "3.1.0"), 0)
        self.assertEqual(req_matches("3.0.1", "3.0.0"), 1)
        self.assertEqual(req_matches("3.0.0", "4.0.0"), 1)
        self.assertEqual(req_matches("3.1.0", "3.0.5"), 1)

    def test_zero_major(self):
        self.assertEqual(req_matches("0.3.0", "0.3.5"), 0)
        self.assertEqual(req_matches("0.3.0", "0.4.0"), 1)
        self.assertEqual(req_matches("0.0.3", "0.0.3"), 0)
        self.assertEqual(req_matches("0.0.3", "0.0.4"), 1)

    def test_unsupported_grammar_fails_closed(self):
        self.assertEqual(req_matches(">=1, <2", "1.5.0"), 2)
        self.assertEqual(req_matches("1.2", "1.2.0"), 2)
        self.assertEqual(req_matches("1.2.3", "not-a-version"), 2)


if __name__ == "__main__":
    unittest.main()
