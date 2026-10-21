"""Reject ambiguous or substituted dependency sources before building the demo."""

import importlib.util
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("wots_runner", Path(__file__).with_name("run.py"))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class SourceTests(unittest.TestCase):
    def test_manifest_pin_is_the_authoritative_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "Cargo.toml").write_text(
                '[workspace.dependencies]\nengine = {package = "zcash_script", git = "https://example.test/script", rev = "abc"}\n'
            )
            self.assertEqual(runner.expected_sources(root), {
                "zcash_script": "git+https://example.test/script?rev=abc#abc"
            })

    def test_missing_duplicate_registry_and_wrong_revision_sources_are_rejected(self):
        expected = {"zcash_script": "git+https://example.test/script?rev=abc#abc"}
        correct = {"name": "zcash_script", "source": expected["zcash_script"]}
        runner.verify_sources([correct], expected)
        for packages in (
            [], [correct, correct],
            [{"name": "zcash_script", "source": None}],
            [{"name": "zcash_script", "source": "registry+https://github.com/rust-lang/crates.io-index"}],
            [{"name": "zcash_script", "source": correct["source"].replace("abc", "def")}],
        ):
            with self.subTest(packages=packages), self.assertRaises(ValueError):
                runner.verify_sources(packages, expected)


if __name__ == "__main__":
    unittest.main()
