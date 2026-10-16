"""Offline publisher orchestration and workflow safety checks."""

import json
import subprocess
import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import release_state
from release_state_registry import AssetError

ROOT = Path(__file__).resolve().parents[2]


class CandidateTests(unittest.TestCase):
    def setUp(self):
        self.committed = ("0 " + "0" * 64 + "\n1 " + "a" * 64 + "\n").encode()
        self.checkpoints = self.committed + ("2 " + "b" * 64 + "\n").encode()
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name) / "checkout"
        self.bundle = Path(self.scratch.name) / "bundle"
        self.bundle.mkdir()
        for relative, data in ((release_state.IMPORT.CHECKPOINTS, self.committed),
                               (release_state.IMPORT.FRONTIER, b"old"),
                               (release_state.IMPORT.SUBTREES, self.subtrees(1)),
                               (release_state.IMPORT.PROVENANCE, b"{}")):
            target = self.root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
        crate = self.root / "crates/zakura-assets"
        crate.mkdir(parents=True)
        (crate / "Cargo.toml").write_text('name = "zakura-assets"\n')
        self.grid = Path(self.scratch.name) / "previous-grid.bin"
        self.grid.write_bytes(release_state.PACK._build_grid(heights=[0], last_checkpoint=1))
        self.original = {
            "main-checkpoints.txt": self.checkpoints,
            "mainnet-frontier.bin": b"new",
            "mainnet-treestate-subtrees.bin": self.subtrees(2),
            "mainnet-frontier-grid.bin": release_state.PACK._build_grid(heights=[0, 1], last_checkpoint=2),
        }
        for name, data in self.original.items():
            (self.bundle / name).write_bytes(data)
        self.resolution = dict(height=2, block_hash="b" * 64, generated_at="2026-10-15T00:00:00Z",
                               meta_url="https://zakura-release.valargroup.dev/release-state/v1/2/meta.json",
                               meta_sha256="a" * 64)
        self.selected = release_state.select(self.root, self.bundle, self.resolution)

    def subtrees(self, height):
        prefix = release_state.IMPORT.SUBTREE_HEADER_PREFIX.pack(b"ZKVCTST1", 1, 1, height, 0, 0, 0)
        return prefix + hashlib.sha256(prefix).digest()

    def fetch(self, url, digest, destination, resolution_path):
        self.assertEqual(url, self.selected["meta_url"])
        self.assertEqual(digest, self.selected["meta_sha256"])
        destination.mkdir()
        for name, data in self.original.items():
            (destination / name).write_bytes(data)
        resolution_path.write_text(json.dumps(self.resolution))
        return self.resolution

    def validate(self, verifier=None):
        original_run = subprocess.run
        def run_verifier(*args, **kwargs):
            if args[0][0] != "cargo":
                return original_run(*args, **kwargs)
            return verifier(*args, **kwargs) if verifier else subprocess.CompletedProcess(args[0], 0)
        with patch.object(release_state.FETCH, "resolve_pinned_bundle", side_effect=self.fetch), \
                patch.object(release_state, "pinned_grid", return_value=self.grid), \
                patch.object(release_state.subprocess, "run", side_effect=run_verifier) as calls:
            release_state.validate_candidate(self.root, self.bundle, self.selected)
            return calls

    def test_candidate_validation_uses_scratch_and_proves_subtree_roots(self):
        before = (self.root / release_state.IMPORT.CHECKPOINTS).read_bytes()
        run = self.validate()
        self.assertEqual((self.root / release_state.IMPORT.CHECKPOINTS).read_bytes(), before)
        self.assertIn("verify-historical-treestates", run.call_args.args[0])

    def test_rewritten_checkpoint_history_is_rejected_before_semantic_verification(self):
        self.original["main-checkpoints.txt"] = self.checkpoints.replace(b"a" * 64, b"c" * 64)
        (self.bundle / "main-checkpoints.txt").write_bytes(self.original["main-checkpoints.txt"])
        with self.assertRaisesRegex(release_state.IMPORT.BundleImportError, "byte-for-byte"):
            self.validate(lambda *a, **kw: self.fail("semantic verification must not run"))

    def test_conflicting_terminal_hash_is_rejected_before_semantic_verification(self):
        self.resolution["block_hash"] = "c" * 64
        self.selected = release_state.select(self.root, self.bundle, self.resolution)
        with self.assertRaisesRegex(AssetError, "terminal checkpoint"):
            self.validate(lambda *a, **kw: self.fail("semantic verification must not run"))

    def test_malformed_checkpoint_entry_is_rejected_before_semantic_verification(self):
        self.original["main-checkpoints.txt"] = self.committed + b"malformed checkpoint\n" + self.checkpoints[len(self.committed):]
        (self.bundle / "main-checkpoints.txt").write_bytes(self.original["main-checkpoints.txt"])
        with self.assertRaises(subprocess.CalledProcessError):
            self.validate(lambda *a, **kw: self.fail("semantic verification must not run"))

    def test_rewritten_grid_is_rejected_before_publication(self):
        grid = release_state.PACK._build_grid(heights=[1], last_checkpoint=2)
        self.original["mainnet-frontier-grid.bin"] = grid
        (self.bundle / "mainnet-frontier-grid.bin").write_bytes(grid)
        self.selected = release_state.select(self.root, self.bundle, self.resolution)
        with self.assertRaisesRegex(release_state.IMPORT.BundleImportError, "rewrites published"):
            self.validate()

    def test_subtree_verification_failure_propagates(self):
        with self.assertRaises(subprocess.CalledProcessError):
            self.validate(lambda *a, **kw: (_ for _ in ()).throw(subprocess.CalledProcessError(1, "verifier")))

    def test_tampered_preserved_artifact_is_rejected(self):
        (self.bundle / "mainnet-frontier.bin").write_bytes(b"tampered")
        with self.assertRaisesRegex(AssetError, "preserved bundle file"):
            self.validate()


class WorkflowTests(unittest.TestCase):
    def test_publication_is_daily_and_storage_credentials_are_separate(self):
        update = (ROOT / ".github/workflows/update-release-state.yml").read_text()
        self.assertIn('cron: "0 7 * * *"', update)
        self.assertNotIn("create-pull-request@", update)
        self.assertIn("MAINNET_RELEASE_STATE_PUBLICATION_ENABLED", update)
        publish = update.split("  publish-assets:\n")[1].split("  promote:\n")[0]
        promotion = update.split("  promote:\n")[1]
        self.assertIn("needs: [resolve, validate]", publish)
        self.assertIn("!inputs.dry_run", publish)
        self.assertIn("environment: crates-io-assets", publish)
        self.assertNotIn("RELEASE_STATE_R2_SECRET", publish)
        self.assertIn("environment: release-state-storage", promotion)
        self.assertIn("needs: [resolve, validate, publish-assets]", promotion)
        self.assertNotIn("id-token: write", promotion)
        self.assertIn("!inputs.dry_run", promotion)

    def test_adoption_is_weekly_and_dry_runs_cannot_create_prs(self):
        adopt = (ROOT / ".github/workflows/adopt-release-state.yml").read_text()
        self.assertIn('cron: "0 6 * * 1"', adopt)
        self.assertIn("MAINNET_RELEASE_STATE_ADOPTION_ENABLED", adopt)
        self.assertIn("MAINNET_RELEASE_STATE_READY_URL", adopt)
        self.assertNotIn("MAINNET_RELEASE_STATE_LATEST_URL", adopt)
        for step in ("Create release GitHub App token", "Open or update the draft pull request"):
            section = adopt.split(f"      - name: {step}\n")[1].split("        uses:")[0]
            self.assertIn("!inputs.dry_run", section)
            self.assertIn("github.ref == 'refs/heads/main'", section)
        self.assertNotIn("id-token: write", adopt)
        self.assertNotIn("R2_SECRET", adopt)

    def test_preparation_consumes_ready_sets_without_dispatch_or_fallback(self):
        prepare = (ROOT / ".github/workflows/prepare-release-pr.yml").read_text()
        self.assertNotIn("actions: write", prepare)
        self.assertNotIn("ensure-assets", prepare)
        self.assertNotIn("--latest-url", prepare)
        self.assertNotIn("MAINNET_RELEASE_STATE_LATEST_URL", prepare)
        self.assertIn("MAINNET_RELEASE_STATE_READY_URL", prepare)
        self.assertIn("release_state.py verify --committed", prepare)
        for step in ("Create release GitHub App token", "Open or update the draft pull request"):
            guard = prepare.split(f"      - name: {step}\n")[1].splitlines()[0]
            self.assertIn("!inputs.dry_run", guard)
        advisory = (ROOT / ".github/workflows/create-release.yml").read_text()
        self.assertIn("MAINNET_RELEASE_STATE_READY_URL", advisory)
        self.assertNotIn("--latest-url", advisory)


if __name__ == "__main__":
    unittest.main()
