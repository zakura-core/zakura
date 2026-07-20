import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).parents[1] / "changelog.py"
SPEC = importlib.util.spec_from_file_location("zakura_changelog", SCRIPT)
assert SPEC and SPEC.loader
changelog = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = changelog
SPEC.loader.exec_module(changelog)


CHANGELOG = """# Changelog

## [Unreleased]

## [{version}] - 2026-07-20

- Previous release.
"""


class ChangelogTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)
        (self.root / "changelog-unreleased").mkdir()
        (self.root / "changelog-unreleased" / "README.md").write_text("# Fragments\n")
        (self.root / "CHANGELOG.md").write_text(CHANGELOG.format(version="1.0.0"))

    def tearDown(self):
        self.temporary_directory.cleanup()

    def test_parses_multi_category_fragment(self):
        path = self.root / "changelog-unreleased" / "123.md"
        path.write_text(
            "## Fixed\n\n- Fixed a bug.\n\n## Added\n\n- Added a feature.\n"
        )

        fragment = changelog.load_fragments(self.root)[0]

        self.assertEqual(fragment.entries["Fixed"], "- Fixed a bug.")
        self.assertEqual(fragment.entries["Added"], "- Added a feature.")

    def test_requires_reason_for_no_changelog_fragment(self):
        path = self.root / "changelog-unreleased" / "123.md"
        path.write_text("<!-- changelog: none -->\n<!-- not a reason -->\n")

        with self.assertRaisesRegex(changelog.ChangelogError, "explain why"):
            changelog.load_fragments(self.root)

    def test_accepts_no_changelog_fragment_with_reason(self):
        path = self.root / "changelog-unreleased" / "123.md"
        path.write_text("<!-- changelog: none -->\n\nThis PR only changes tests.\n")

        fragment = changelog.load_fragments(self.root)[0]

        self.assertEqual(fragment.entries, {})

    def test_rejects_fragment_without_pull_request_number(self):
        path = self.root / "changelog-unreleased" / "my-change.md"
        path.write_text("## Fixed\n\n- Fixed a bug.\n")

        with self.assertRaisesRegex(changelog.ChangelogError, "pull request number"):
            changelog.load_fragments(self.root)

    def test_rejects_nested_fragment_directory(self):
        (self.root / "changelog-unreleased" / "123").mkdir()

        with self.assertRaisesRegex(changelog.ChangelogError, "Markdown files"):
            changelog.load_fragments(self.root)

    def test_pull_request_owns_its_numbered_fragment(self):
        path = self.root / "changelog-unreleased" / "123.md"
        path.write_text("<!-- changelog: none -->\n\nThis PR only changes tests.\n")

        with mock.patch.object(
            changelog,
            "run_git",
            return_value="changelog-unreleased/123.md\n",
        ):
            changelog.check_pull_request(self.root, "base", "head", "123", False, False)

    def test_pull_request_cannot_delete_another_fragment(self):
        path = self.root / "changelog-unreleased" / "123.md"
        path.write_text("<!-- changelog: none -->\n\nThis PR only changes tests.\n")

        with mock.patch.object(
            changelog,
            "run_git",
            return_value=("changelog-unreleased/123.md\nchangelog-unreleased/122.md\n"),
        ):
            with self.assertRaisesRegex(changelog.ChangelogError, "unexpected"):
                changelog.check_pull_request(
                    self.root, "base", "head", "123", False, False
                )

    def test_release_versions_root_changelog_and_consumes_fragments(self):
        path = self.root / "changelog-unreleased" / "123.md"
        path.write_text(
            "## Fixed\n\n- Fixed a bug.\n\n## Added\n\n- Added a feature.\n"
        )

        writes, removals = changelog.release_plan(self.root, "v1.1.0", "2026-07-21")
        for target, rendered in writes.items():
            target.write_text(rendered)
        for target in removals:
            target.unlink()

        self.assertIn(
            "## [1.1.0] - 2026-07-21\n\n### Added",
            (self.root / "CHANGELOG.md").read_text(),
        )
        self.assertIn(
            "### Fixed\n\n- Fixed a bug.",
            (self.root / "CHANGELOG.md").read_text(),
        )
        self.assertFalse(path.exists())

        writes, removals = changelog.release_plan(self.root, "v1.1.0", "2026-07-22")
        self.assertEqual(writes, {})
        self.assertEqual(removals, [])

    def test_release_rejects_new_version_without_changelog_entries(self):
        with self.assertRaisesRegex(changelog.ChangelogError, "Unreleased is empty"):
            changelog.release_plan(self.root, "v1.1.0", "2026-07-21")

    def test_release_rejects_current_version_with_unreleased_entries(self):
        path = self.root / "CHANGELOG.md"
        path.write_text(
            path.read_text().replace(
                "## [Unreleased]\n",
                "## [Unreleased]\n\n### Fixed\n\n- Fixed a bug.\n",
            )
        )

        with self.assertRaisesRegex(changelog.ChangelogError, "already exists"):
            changelog.release_plan(self.root, "v1.0.0", "2026-07-21")

    def test_release_rejects_invalid_date(self):
        with self.assertRaisesRegex(changelog.ChangelogError, "YYYY-MM-DD"):
            changelog.release_plan(self.root, "v1.1.0", "2026-02-30")


if __name__ == "__main__":
    unittest.main()
