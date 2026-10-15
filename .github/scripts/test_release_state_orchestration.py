"""Offline publisher orchestration and workflow safety checks."""

import importlib.util
import json
import os
import subprocess
import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from release_state_assets import AssetError, NotPublished, publication_needed, verify_visible
from test_pin_release_state_assets import RegistryFixture

ROOT = Path(__file__).resolve().parents[2]


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ENSURE = load("ensure_assets", "ensure-release-state-assets.py")
TOOLS = load("asset_tools", "release-state-assets.py")


class PublisherFixture:
    def __init__(self, selected, conclusion="success"):
        self.selected = selected
        self.conclusion = conclusion
        self.calls = []
        self.run = dict(id=42, workflow_id=7, event="workflow_dispatch", head_branch="main",
                        display_title="Ensure release-state assets prepare-123-1", head_sha="b" * 40,
                        repository=dict(full_name="zakura-core/zakura"), status="completed",
                        conclusion=conclusion, run_attempt=1)
        self.receipt = {**{key: selected[key] for key in ("meta_url", "meta_sha256", "height", "name", "version", "grid_sha256")},
                        "schema_version": 1, "request_id": "prepare-123-1", "outcome": "published",
                        "producer_run_id": 42, "producer_sha": "b" * 40, "producer_attempt": 1}
        self.duplicate = False
        self.missing_receipt = False

    def call(self, *args, **kwargs):
        self.calls.append(args)
        if args[:2] == ("workflow", "run"):
            self.dispatch = args
            return ""
        if args[:2] == ("run", "download"):
            if not self.missing_receipt:
                (Path(args[args.index("--dir") + 1]) / "receipt.json").write_text(json.dumps(self.receipt))
            return ""
        endpoint = args[1]
        if endpoint.endswith("/workflows/update-release-state.yml"):
            return json.dumps(dict(id=7, path=".github/workflows/update-release-state.yml", state="active"))
        if "?branch=" in endpoint:
            return json.dumps(dict(workflow_runs=[self.run] * (2 if self.duplicate else 1)))
        if endpoint.endswith("/runs/42"):
            return json.dumps(self.run)
        raise AssertionError(args)


class OrchestrationTests(unittest.TestCase):
    def setUp(self):
        self.selected = {**RegistryFixture().identity, "has_changes": True}
        self.publisher = PublisherFixture(self.selected)

    def test_dispatch_uses_immutable_metadata_and_accepts_exact_receipt(self):
        receipt = ENSURE.ensure(self.selected, "prepare-123-1", call=self.publisher.call)
        self.assertEqual(receipt["outcome"], "published")
        self.assertIn("--ref", self.publisher.dispatch)
        self.assertIn("main", self.publisher.dispatch)
        self.assertIn(f'meta_sha256={self.selected["meta_sha256"]}', self.publisher.dispatch)
        self.assertFalse(any("latest.json" in argument for call in self.publisher.calls for argument in call))

    def test_failed_cancelled_and_skipped_runs_fail_without_receipt_download(self):
        for conclusion in ("failure", "cancelled", "skipped", "timed_out"):
            publisher = PublisherFixture(self.selected, conclusion)
            with self.subTest(conclusion=conclusion), self.assertRaises(AssetError):
                ENSURE.ensure(self.selected, "prepare-123-1", call=publisher.call)
            self.assertFalse(any(call[:2] == ("run", "download") for call in publisher.calls))

    def test_wrong_or_missing_receipts_fail(self):
        for key, value in (("request_id", "another"), ("height", 1), ("meta_sha256", "c" * 64),
                           ("grid_sha256", "c" * 64), ("producer_run_id", 9),
                           ("producer_sha", "c" * 40), ("producer_attempt", 2), ("outcome", "failed")):
            publisher = PublisherFixture(self.selected)
            publisher.receipt[key] = value
            with self.subTest(key=key), self.assertRaises(AssetError):
                ENSURE.ensure(self.selected, "prepare-123-1", call=publisher.call)
        self.publisher.missing_receipt = True
        with self.assertRaisesRegex(AssetError, "missing"):
            ENSURE.ensure(self.selected, "prepare-123-1", call=self.publisher.call)

    def test_duplicate_request_runs_are_rejected(self):
        self.publisher.duplicate = True
        with self.assertRaisesRegex(AssetError, "multiple"):
            ENSURE.ensure(self.selected, "prepare-123-1", call=self.publisher.call)

    def test_wrong_producer_workflow_or_branch_is_rejected(self):
        for key, value in (("workflow_id", 8), ("head_branch", "feature"), ("event", "pull_request")):
            publisher = PublisherFixture(self.selected)
            publisher.run[key] = value
            with self.subTest(key=key), self.assertRaises(AssetError):
                ENSURE.ensure(self.selected, "prepare-123-1", call=publisher.call)

    def test_missing_run_is_bounded(self):
        now = [0]
        def call(*args, **kwargs):
            if "?branch=" in args[1]:
                return '{"workflow_runs": []}'
            return self.publisher.call(*args, **kwargs)
        def sleep(seconds):
            now[0] += seconds
        with self.assertRaisesRegex(AssetError, "timed out"):
            ENSURE.ensure(self.selected, "prepare-123-1", call=call, clock=lambda: now[0], sleep=sleep, timeout=30)
        self.assertEqual(now[0], 30)

    def test_nonpublishing_guards_reject_branch_and_dry_run(self):
        env = dict(GITHUB_REPOSITORY="zakura-core/zakura", GITHUB_REF="refs/heads/main", DRY_RUN="false")
        for changed in (dict(GITHUB_REF="refs/heads/feature"), dict(DRY_RUN="true")):
            with patch.dict(os.environ, {**env, **changed}), \
                    patch("sys.argv", ["ensure", "--state", "/unused"]), self.assertRaises(AssetError):
                ENSURE.main()

    def test_no_op_and_waived_imports_never_dispatch(self):
        with tempfile.TemporaryDirectory() as scratch:
            state = Path(scratch)
            for changes, waiver in ((False, ""), (True, "approved urgent RC")):
                (state / "selection.json").write_text(json.dumps({**self.selected, "has_changes": changes}))
                env = dict(GITHUB_REPOSITORY="zakura-core/zakura", GITHUB_REF="refs/heads/main",
                           DRY_RUN="false", RELEASE_STATE_WAIVER=waiver)
                with patch.dict(os.environ, env), patch("sys.argv", ["ensure", "--state", scratch]), \
                        patch.object(ENSURE, "ensure") as dispatch, self.assertRaises(AssetError):
                    ENSURE.main()
                dispatch.assert_not_called()

    def test_ensure_mode_never_resolves_latest_and_requires_all_inputs(self):
        with tempfile.TemporaryDirectory() as scratch:
            env = dict(MODE="ensure-assets", META_URL=self.selected["meta_url"],
                       META_SHA256=self.selected["meta_sha256"], REQUEST_ID="prepare-123-1",
                       LATEST_URL="https://example.test/newer/latest.json")
            with patch.dict(os.environ, env), patch.object(TOOLS.FETCH, "resolve_pinned_bundle") as pinned, \
                    patch.object(TOOLS.FETCH, "resolve_bundle") as latest:
                TOOLS.fetch_candidate(Path(scratch))
                pinned.assert_called_once()
                latest.assert_not_called()
            for key in ("META_URL", "META_SHA256", "REQUEST_ID"):
                with patch.dict(os.environ, {**env, key: ""}), self.assertRaises(AssetError):
                    TOOLS.fetch_candidate(Path(scratch))

    def test_fixture_missing_version_can_publish_then_be_resolved(self):
        registry = RegistryFixture()
        self.assertTrue(publication_needed(self.selected, lambda *args: (_ for _ in ()).throw(NotPublished("404"))))
        with patch.object(TOOLS.subprocess, "run") as upload:
            TOOLS.publish(ROOT, True)
            upload.assert_called_once()
        resolve_calls = []
        verify_visible(self.selected, fetch=registry.fetch,
                       resolve=lambda identity, **kw: resolve_calls.append(identity))
        self.assertEqual(resolve_calls, [self.selected])

    def test_upload_failure_prevents_success(self):
        with patch.object(TOOLS.subprocess, "run", side_effect=subprocess.CalledProcessError(1, "cargo")):
            with self.assertRaises(subprocess.CalledProcessError):
                TOOLS.publish(ROOT, True)
        with self.assertRaises(AssetError):
            TOOLS.publish(ROOT, False)

    def test_publisher_rerun_during_receipt_download_is_rejected(self):
        reads = [0]
        def call(*args, **kw):
            if args[:2] == ("api", "repos/zakura-core/zakura/actions/runs/42"):
                reads[0] += 1
                if reads[0] == 2:
                    self.publisher.run["run_attempt"] = 2
            return self.publisher.call(*args, **kw)
        with self.assertRaisesRegex(AssetError, "changed while"):
            ENSURE.ensure(self.selected, "prepare-123-1", call=call)

    def test_receipt_is_written_only_after_availability_verification(self):
        with tempfile.TemporaryDirectory() as scratch:
            state = Path(scratch)
            (state / "selection.json").write_text(json.dumps(self.selected))
            (state / "publication.json").write_text('{"needed": true}')
            env = dict(REQUEST_ID="prepare-123-1", GITHUB_RUN_ID="42", GITHUB_SHA="b" * 40, GITHUB_RUN_ATTEMPT="1")
            with patch.dict(os.environ, env), patch("sys.argv", ["assets", "verify", "--state", scratch]), \
                    patch.object(TOOLS, "verify_visible", side_effect=AssetError("not available")):
                with self.assertRaises(AssetError):
                    TOOLS.main()
                self.assertFalse((state / "receipt.json").exists())
            with patch.dict(os.environ, env), patch("sys.argv", ["assets", "verify", "--state", scratch]), \
                    patch.object(TOOLS, "verify_visible"):
                TOOLS.main()
            self.assertEqual(json.loads((state / "receipt.json").read_text()), self.publisher.receipt)

    def test_dry_run_packages_in_scratch_and_never_uploads(self):
        with tempfile.TemporaryDirectory() as scratch:
            state = Path(scratch)
            (state / "bundle").mkdir()
            grid = TOOLS.PACK._build_grid(heights=[0], last_checkpoint=2)
            (state / "bundle/mainnet-frontier-grid.bin").write_bytes(grid)
            selected = {**self.selected, "version": "0.2.0", "height": 2,
                        "grid_sha256": hashlib.sha256(grid).hexdigest()}
            manifest = TOOLS.ROOT / "crates/zakura-assets/Cargo.toml"
            before = manifest.read_bytes()
            for missing in (False, True):
                with patch.object(TOOLS, "publication_needed", return_value=missing), \
                        patch.object(TOOLS.subprocess, "run") as command:
                    if missing:
                        with self.assertRaisesRegex(AssetError, "non-publishing dry run"):
                            TOOLS.require_existing(state, selected)
                    else:
                        TOOLS.require_existing(state, selected)
                    command.assert_not_called()
            self.assertEqual(manifest.read_bytes(), before)


class CandidateTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name) / "checkout"
        self.bundle = Path(self.scratch.name) / "bundle"
        self.bundle.mkdir()
        for relative, data in ((TOOLS.IMPORT.CHECKPOINTS, b"1 aa\n"),
                               (TOOLS.IMPORT.FRONTIER, b"old"),
                               (TOOLS.IMPORT.SUBTREES, self.subtrees(1)),
                               (TOOLS.IMPORT.PROVENANCE, b"{}")):
            target = self.root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
        crate = self.root / "crates/zakura-assets"
        crate.mkdir(parents=True)
        (crate / "Cargo.toml").write_text('name = "zakura-assets"\n')
        self.grid = Path(self.scratch.name) / "previous-grid.bin"
        self.grid.write_bytes(TOOLS.PACK._build_grid(heights=[0], last_checkpoint=1))
        self.original = {
            "main-checkpoints.txt": b"1 aa\n2 bb\n",
            "mainnet-frontier.bin": b"new",
            "mainnet-treestate-subtrees.bin": self.subtrees(2),
            "mainnet-frontier-grid.bin": TOOLS.PACK._build_grid(heights=[0, 1], last_checkpoint=2),
        }
        for name, data in self.original.items():
            (self.bundle / name).write_bytes(data)
        self.resolution = dict(height=2, block_hash="bb", generated_at="2026-10-15T00:00:00Z",
                               meta_url="https://zakura-release.valargroup.dev/release-state/v1/2/meta.json",
                               meta_sha256="a" * 64)
        self.selected = TOOLS.select(self.root, self.bundle, self.resolution)

    def subtrees(self, height):
        prefix = TOOLS.IMPORT.SUBTREE_HEADER_PREFIX.pack(b"ZKVCTST1", 1, 1, height, 0, 0, 0)
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
        with patch.object(TOOLS.FETCH, "resolve_pinned_bundle", side_effect=self.fetch), \
                patch.object(TOOLS, "previous_grid", return_value=self.grid), \
                patch.object(TOOLS.subprocess, "run", side_effect=verifier) as run:
            TOOLS.validate_candidate(self.root, self.bundle, self.selected)
            return run

    def test_candidate_validation_uses_scratch_and_proves_subtree_roots(self):
        before = (self.root / TOOLS.IMPORT.CHECKPOINTS).read_bytes()
        run = self.validate()
        self.assertEqual((self.root / TOOLS.IMPORT.CHECKPOINTS).read_bytes(), before)
        self.assertIn("verify-historical-treestates", run.call_args.args[0])

    def test_rewritten_checkpoint_history_is_rejected_before_semantic_verification(self):
        self.original["main-checkpoints.txt"] = b"1 cc\n2 bb\n"
        (self.bundle / "main-checkpoints.txt").write_bytes(self.original["main-checkpoints.txt"])
        with self.assertRaisesRegex(TOOLS.IMPORT.BundleImportError, "byte-for-byte"):
            self.validate(lambda *a, **kw: self.fail("semantic verification must not run"))

    def test_rewritten_grid_is_rejected_before_publication(self):
        grid = TOOLS.PACK._build_grid(heights=[1], last_checkpoint=2)
        self.original["mainnet-frontier-grid.bin"] = grid
        (self.bundle / "mainnet-frontier-grid.bin").write_bytes(grid)
        self.selected = TOOLS.select(self.root, self.bundle, self.resolution)
        with self.assertRaisesRegex(TOOLS.IMPORT.BundleImportError, "rewrites published"):
            self.validate()

    def test_subtree_verification_failure_propagates(self):
        with self.assertRaises(subprocess.CalledProcessError):
            self.validate(lambda *a, **kw: (_ for _ in ()).throw(subprocess.CalledProcessError(1, "verifier")))

    def test_tampered_preserved_artifact_is_rejected(self):
        (self.bundle / "mainnet-frontier.bin").write_bytes(b"tampered")
        with self.assertRaisesRegex(AssetError, "preserved bundle file"):
            self.validate()


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.prepare = (ROOT / ".github/workflows/prepare-release-pr.yml").read_text()
        self.update = (ROOT / ".github/workflows/update-release-state.yml").read_text()

    def test_publisher_requires_candidate_validation_and_canonical_main(self):
        publication = self.update.split("  publish-assets:\n")[1].split("  open-pr:\n")[0]
        self.assertIn("needs: [resolve, validate]", publication)
        self.assertIn("github.ref == 'refs/heads/main' && github.repository == 'zakura-core/zakura'", publication)
        self.assertIn("environment: crates-io-assets", publication)
        self.assertLess(publication.index("release-state-assets.py check"), publication.index("Authenticate with crates.io"))
        self.assertLess(publication.index("release-state-assets.py publish"), publication.index("release-state-assets.py verify"))
        self.assertIn("if: steps.existing.outputs.needed == 'true'", publication)
        self.assertIn("cancel-in-progress: false", self.update)

    def test_publication_mode_cannot_open_state_update_pr(self):
        self.assertIn("inputs.mode != 'ensure-assets'", self.update.split("  open-pr:\n")[1].split("    steps:")[0])

    def test_dry_run_cannot_dispatch_and_only_dispatch_job_can_write_actions(self):
        job = self.prepare.split("  ensure-assets:\n")[1].split("  prepare:\n")[0]
        self.assertIn("!inputs.dry_run && github.ref == 'refs/heads/main'", job)
        self.assertIn("inputs.release_state_waiver == ''", job)
        self.assertIn("needs.resolve.outputs.has_changes == 'true'", job)
        self.assertEqual(self.prepare.count("actions: write"), 1)
        self.assertIn("actions: write", job)
        self.assertNotIn("id-token: write", self.prepare)
        for step in ("Create release GitHub App token", "Open or update the draft pull request"):
            guard = self.prepare.split(f"      - name: {step}\n", 1)[1].splitlines()[0]
            self.assertEqual(guard, "        if: ${{ !inputs.dry_run && github.ref == 'refs/heads/main' }}")

    def test_selected_source_and_bundle_are_preserved_between_jobs(self):
        self.assertNotIn("          ref: main\n", self.prepare)
        self.assertIn("          ref: ${{ github.sha }}", self.prepare)
        prepare_job = self.prepare.split("  prepare:\n")[1]
        self.assertNotIn("--latest-url", prepare_job)
        self.assertIn("name: preparation-release-state", prepare_job)
        self.assertLess(prepare_job.index("release-state-assets.py validate"), prepare_job.index("import-release-state.py"))
        self.assertLess(prepare_job.index("pin-release-state-assets.py"), prepare_job.index("check-release-state.sh"))
        self.assertIn("needs.ensure-assets.result == 'success'", prepare_job)


if __name__ == "__main__":
    unittest.main()
