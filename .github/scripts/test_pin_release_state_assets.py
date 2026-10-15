"""Offline regression checks for release preparation's asset pin."""

import importlib.util
import io
import subprocess
import tarfile
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location(
    "pin_assets", Path(__file__).with_name("pin-release-state-assets.py")
)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class PinAssetsTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name)
        self.manifest = self.root / "Cargo.toml"
        self.original = '[workspace.dependencies]\nzakura-assets = { version = "=0.3513777.0" }\n'
        self.manifest.write_text(self.original)
        self.imported = {
            "has_changes": True,
            "height": 3515777,
            "frontier_grid_sha256": MODULE.hashlib.sha256(b"verified grid").hexdigest(),
        }

    def archive(self, grid=b"verified grid"):
        output = io.BytesIO()
        with tarfile.open(fileobj=output, mode="w:gz") as package:
            entry = tarfile.TarInfo("zakura-assets-0.3515777.0/src/mainnet-frontier-grid.bin")
            entry.size = len(grid)
            package.addfile(entry, io.BytesIO(grid))
        return io.BytesIO(output.getvalue())

    def test_new_bundle_advances_manifest_and_lockfile_together(self):
        with patch.object(MODULE.urllib.request, "urlopen", return_value=self.archive()), \
                patch.object(MODULE.subprocess, "run") as cargo:
            MODULE.pin_assets(self.root, self.imported)
        self.assertIn('version = "=0.3515777.0"', self.manifest.read_text())
        cargo.assert_called_once_with(
            ["cargo", "update", "-p", "zakura-assets", "--precise", "0.3515777.0"],
            cwd=self.root, check=True,
        )

    def test_wrong_published_bytes_leave_manifest_unchanged(self):
        with patch.object(MODULE.urllib.request, "urlopen", return_value=self.archive(b"other grid")), \
                patch.object(MODULE.subprocess, "run") as cargo:
            with self.assertRaisesRegex(ValueError, "differs from the verified bundle"):
                MODULE.pin_assets(self.root, self.imported)
        self.assertEqual(self.manifest.read_text(), self.original)
        cargo.assert_not_called()

    def test_unpublished_grid_explains_required_publication(self):
        missing = urllib.error.HTTPError("https://example.test", 404, "missing", {}, None)
        with patch.object(MODULE.urllib.request, "urlopen", side_effect=missing):
            with self.assertRaisesRegex(ValueError, "publish the matching grid"):
                MODULE.pin_assets(self.root, self.imported)
        self.assertEqual(self.manifest.read_text(), self.original)

    def test_no_import_leaves_pin_and_registry_alone(self):
        with patch.object(MODULE.urllib.request, "urlopen") as fetch:
            MODULE.pin_assets(self.root, {"has_changes": False})
        fetch.assert_not_called()
        self.assertEqual(self.manifest.read_text(), self.original)

    def test_workflow_repins_before_validating_or_building_imported_state(self):
        workflow = (Path(__file__).parents[1] / "workflows/prepare-release-pr.yml").read_text()
        import_at = workflow.index("--result-out \"$RUNNER_TEMP/release-state-import.json\"")
        setup_at = workflow.index("id: toolchain")
        pin_at = workflow.index("python3 .github/scripts/pin-release-state-assets.py")
        validate_at = workflow.index("scripts/check-release-state.sh")
        build_at = workflow.index("cargo test -p zakura-state")
        self.assertLess(import_at, setup_at)
        self.assertLess(setup_at, pin_at)
        self.assertLess(pin_at, validate_at)
        self.assertLess(validate_at, build_at)

    def test_resolution_failure_stops_preparation(self):
        with patch.object(MODULE.urllib.request, "urlopen", return_value=self.archive()), \
                patch.object(MODULE.subprocess, "run", side_effect=subprocess.CalledProcessError(1, "cargo")):
            with self.assertRaises(subprocess.CalledProcessError):
                MODULE.pin_assets(self.root, self.imported)


if __name__ == "__main__":
    unittest.main()
