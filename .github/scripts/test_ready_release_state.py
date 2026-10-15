"""CLI-level complete-set tests with loopback S3, a real registry, and real Cargo."""

import copy
import functools
import hashlib
import importlib.util
import json
import os
import struct
import subprocess
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import ready_release_state as ready
import release_state_assets as assets
from release_state_test_services import Services

HERE = Path(__file__).parent


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, HERE / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


TOOLS = load("ready_test_tools", "release-state-assets.py")
CLI = load("ready_test_cli", "ready-release-state.py")
PIN = load("ready_test_pin", "pin-release-state-assets.py")


class CompleteSetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.services = Services()
        cls.workspace = tempfile.TemporaryDirectory(prefix="zakura-ready-e2e-")
        cls.root = Path(cls.workspace.name)
        cls.env = cls.services.cargo_env(cls.root)
        cls.resolve = cls.services.cargo_resolver(cls.env)
        cls.generated = datetime.now(timezone.utc).replace(microsecond=0)
        cls.states = {}
        # Publish three actual archives to the private registry using Cargo. No public upload.
        for height in (1, 2, 3):
            state = cls.root / f"candidate-{height}"
            state.mkdir()
            bundle = state / "bundle"
            bundle.mkdir()
            grid = TOOLS.PACK._build_grid(heights=list(range(height)), last_checkpoint=height)
            subtree_prefix = TOOLS.IMPORT.SUBTREE_HEADER_PREFIX.pack(b"ZKVCTST1", 1, 1, height, 0, 0, 0)
            data = {"main-checkpoints.txt": b"".join(f'{h} {str(h) * 64}\n'.encode() for h in range(1, height + 1)),
                    "mainnet-frontier.bin": struct.pack("<I", height) + struct.pack("<I", 0) * 3,
                    "mainnet-treestate-subtrees.bin": subtree_prefix + hashlib.sha256(subtree_prefix).digest(),
                    "mainnet-frontier-grid.bin": grid}
            meta = dict(schema_version=1, network="Mainnet", height=height, block_hash=str(height) * 64,
                        generated_at=cls.generated.isoformat().replace("+00:00", "Z"),
                        files={name: dict(size=len(value), sha256=hashlib.sha256(value).hexdigest()) for name, value in data.items()})
            meta_bytes = json.dumps(meta, sort_keys=True).encode()
            for name, value in {**data, "meta.json": meta_bytes}.items():
                (bundle / name).write_bytes(value)
            selected = {key: meta[key] for key in ("height", "block_hash", "generated_at")}
            selected.update(meta_url=f'https://zakura-release.valargroup.dev/release-state/v1/{height}/meta.json',
                            meta_sha256=hashlib.sha256(meta_bytes).hexdigest(), has_changes=True,
                            name="zakura-assets", version=f"0.{height}.0", grid_sha256=hashlib.sha256(grid).hexdigest())
            (state / "selection.json").write_text(json.dumps(selected))
            (state / "resolution.json").write_text(json.dumps({k: selected[k] for k in ready.RESOLUTION_KEYS}))
            cls.states[height] = state
            producer = cls.root / f"producer-{height}"
            crate = producer / "crates/zakura-assets"
            (crate / "src").mkdir(parents=True)
            (crate / "Cargo.toml").write_text('[package]\nname = "zakura-assets"\nversion = "0.0.0"\nedition = "2021"\n'
                                               'description = "Private publication fixture"\nlicense = "MIT"\ninclude = ["src/**", "Cargo.toml"]\n')
            (crate / "src/lib.rs").write_text('pub static GRID: &[u8] = include_bytes!("mainnet-frontier-grid.bin");\n')
            TOOLS.PACK.pack(bundle / "mainnet-frontier-grid.bin", crate, 0)
            needed = assets.publication_needed(selected, cls.services.request)
            if not needed:
                raise AssertionError("new private version unexpectedly exists")
            TOOLS.publish(producer, needed, command_env=cls.env, fixture_registry=True)
        cls.verify = functools.partial(assets.verify_visible, fetch=cls.services.request, resolve=cls.resolve,
                                       sleep=lambda seconds: None)
        # No further Cargo cache or toolchain changes occur in the main checkout.

    @classmethod
    def tearDownClass(cls):
        cls.services.close()
        cls.workspace.cleanup()

    def setUp(self):
        self.services.objects.clear()
        self.services.fail_put = None
        self.services.fail_get = None
        self.services.hide_public = None
        self.services.api_status = None
        self.services.publish_status = None
        self.services.race = None
        self.services.index_delays = 0
        self.store = self.services.s3()
        self.state = self.states[2]
        self.selection = json.loads((self.state / "selection.json").read_text())

    def promote(self, state=None, store=None):
        return ready.promote(state or self.state, store or self.store, fetch=self.services.request,
                             verify=self.verify, registry_fetch=self.services.request)

    def consume(self, output, now=None):
        return ready.resolve_ready(ready.PUBLIC_BASE + "/latest.json", output, fetch=self.services.request,
                                   registry_fetch=self.services.request, cargo_resolve=self.resolve, now=now)

    def test_cli_publication_then_real_import_and_cargo_lockfile_pin(self):
        promote = functools.partial(ready.promote, fetch=self.services.request, verify=self.verify,
                                    registry_fetch=self.services.request)
        with patch.dict(os.environ, GITHUB_REPOSITORY="zakura-core/zakura", GITHUB_REF="refs/heads/main"), \
                patch("sys.argv", ["ready", "promote", "--state", str(self.state)]), patch.object(CLI, "promote", promote):
            CLI.main(store_factory=lambda: self.store)
        # A consumer begins on height one, with a real lockfile for its older fixture crate.
        with tempfile.TemporaryDirectory(prefix="ready-consumer-") as scratch:
            root = Path(scratch)
            (root / "src").mkdir()
            (root / "src/lib.rs").write_text("")
            (root / "Cargo.toml").write_text('[package]\nname="ready-consumer"\nversion="0.0.0"\nedition="2021"\n'
                '[workspace.dependencies]\nzakura-assets = { version = "=0.1.0", registry = "fixture" }\n'
                '[dependencies]\nzakura-assets.workspace = true\n')
            for relative, name in ((TOOLS.IMPORT.CHECKPOINTS, "main-checkpoints.txt"),
                                   (TOOLS.IMPORT.FRONTIER, "mainnet-frontier.bin"),
                                   (TOOLS.IMPORT.SUBTREES, "mainnet-treestate-subtrees.bin")):
                target = root / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes((self.states[1] / "bundle" / name).read_bytes())
            (root / TOOLS.IMPORT.PROVENANCE).write_text("{}")
            package = root / "crates/zakura-assets"
            package.mkdir(parents=True)
            (package / "Cargo.toml").write_text('name = "zakura-assets"\n')
            subprocess.run(["cargo", "metadata", "--format-version", "1"], cwd=root, env=self.env,
                           check=True, capture_output=True, timeout=30)
            fetched = root / "ready-input"
            resolver = functools.partial(ready.resolve_ready, fetch=self.services.request,
                                         registry_fetch=self.services.request, cargo_resolve=self.resolve)
            with patch.dict(os.environ, READY_URL=ready.PUBLIC_BASE + "/latest.json"), patch.object(CLI, "ROOT", root), \
                    patch.object(CLI, "resolve_ready", resolver), patch("sys.argv", ["ready", "fetch", "--state", str(fetched)]):
                CLI.main()
            result = TOOLS.IMPORT.import_bundle(root, fetched / "bundle", fetched / "resolution.json", floor_eos=False)
            imported = root / "import.json"
            imported.write_text(json.dumps(result))
            pin = functools.partial(assets.pin_assets, fetch=self.services.request, cargo_resolve=self.resolve,
                                    source="sparse+" + self.services.base + "/registry/index/", command_env=self.env)
            current = Path.cwd()
            try:
                os.chdir(root)
                with patch.object(PIN, "pin_assets", pin), patch("sys.argv", ["pin", "--state", str(fetched), "--import-result", str(imported)]):
                    PIN.main()
            finally:
                os.chdir(current)
            self.assertIn('version = "=0.2.0"', (root / "Cargo.toml").read_text())
            self.assertIn('version = "0.2.0"', (root / "Cargo.lock").read_text())
            self.assertEqual((root / TOOLS.IMPORT.CHECKPOINTS).read_bytes(), (self.state / "bundle/main-checkpoints.txt").read_bytes())

    def test_failed_cargo_upload_and_delayed_registry_visibility(self):
        self.promote(self.states[1])
        old = self.services.objects[ready.STORAGE_PREFIX + "/latest.json"]
        key = ("zakura-assets", "0.2.0")
        entry = self.services.versions.pop(key)
        try:
            self.assertTrue(assets.publication_needed(self.selection, self.services.request))
            self.services.publish_status = 403
            with self.assertRaises(subprocess.CalledProcessError):
                TOOLS.publish(self.root / "producer-2", True, command_env=self.env, fixture_registry=True)
            self.assertNotIn(key, self.services.versions)
            self.assertEqual(self.services.objects[ready.STORAGE_PREFIX + "/latest.json"], old)
        finally:
            self.services.publish_status = None
            self.services.versions[key] = entry
        # The same verification entrypoint polls a temporarily missing sparse index,
        # then asks real Cargo to resolve the exact fixture version.
        self.services.index_delays = 2
        self.assertEqual(self.promote()["height"], 2)
        self.assertEqual(self.services.index_delays, 0)

    def test_tampered_public_files_and_manifest_cannot_be_consumed(self):
        self.promote()
        pointer_key = ready.STORAGE_PREFIX + "/latest.json"
        for name in ("mainnet-frontier.bin", "mainnet-frontier-grid.bin", "meta.json", "latest.json"):
            key = pointer_key if name == "latest.json" else ready.STORAGE_PREFIX + "/v1/2/" + name
            old = self.services.objects[key]
            self.services.objects[key] = b"tampered", '"changed"', ""
            try:
                with tempfile.TemporaryDirectory() as scratch:
                    output = Path(scratch) / "ready"
                    with self.assertRaises((assets.AssetError, ready.FETCH.BundleError)):
                        self.consume(output)
                    self.assertFalse(output.exists())
            finally:
                self.services.objects[key] = old

    def test_branch_entrypoints_refuse_upload_and_storage_credentials(self):
        with patch.dict(os.environ, GITHUB_REPOSITORY="zakura-core/zakura", GITHUB_REF="refs/heads/fixture"), \
                patch("sys.argv", ["ready", "promote", "--state", str(self.state)]):
            with self.assertRaisesRegex(assets.AssetError, "canonical main"):
                CLI.main(store_factory=lambda: self.fail("a branch must not request storage credentials"))
        with patch.dict(os.environ, GITHUB_REPOSITORY="zakura-core/zakura", GITHUB_REF="refs/heads/fixture"), \
                patch("sys.argv", ["assets", "publish", "--state", str(self.state)]), \
                patch.object(TOOLS, "publish", side_effect=lambda *args: self.fail("a branch must not upload")):
            with self.assertRaisesRegex(assets.AssetError, "canonical main"):
                TOOLS.main()

    def test_identical_retry_preserves_original_manifest_and_generation_time(self):
        first = self.promote()
        before = self.services.objects[ready.STORAGE_PREFIX + "/latest.json"]
        self.assertEqual(self.promote(), first)
        self.assertEqual(self.services.objects[ready.STORAGE_PREFIX + "/latest.json"], before)
        self.assertEqual(first["generated_at"], self.selection["generated_at"])

    def test_failure_after_each_immutable_write_never_advertises_partial_data(self):
        base = self.promote(self.states[1])
        old = self.services.objects[ready.STORAGE_PREFIX + "/latest.json"]
        initial = copy.deepcopy(self.services.objects)
        for boundary in range(1, 6):
            self.services.objects = copy.deepcopy(initial)
            store = self.services.s3()
            original = store.immutable
            count = [0]
            def fail(key, value):
                original(key, value)
                count[0] += 1
                if count[0] == boundary:
                    raise RuntimeError("simulated process crash")
            store.immutable = fail
            with self.subTest(boundary=boundary), self.assertRaises(RuntimeError):
                self.promote(store=store)
            self.assertEqual(self.services.objects[ready.STORAGE_PREFIX + "/latest.json"], old)
            self.assertEqual(base["height"], 1)
            self.assertEqual(self.promote()["height"], 2)

    def test_upload_failure_and_cached_public_404_keep_previous_ready_pointer(self):
        self.promote(self.states[1])
        previous = self.services.objects[ready.STORAGE_PREFIX + "/latest.json"]
        for failure in ("upload", "public"):
            self.services.fail_put = (lambda key: "/v1/2/" in key) if failure == "upload" else None
            self.services.hide_public = (lambda key: "/v1/2/" in key) if failure == "public" else None
            with self.subTest(failure=failure), self.assertRaises((assets.AssetError, ready.FETCH.BundleError)):
                self.promote()
            self.assertEqual(self.services.objects[ready.STORAGE_PREFIX + "/latest.json"], previous)
        self.services.hide_public = None
        self.assertEqual(self.promote()["height"], 2)

    def test_missing_or_denied_registry_never_advances_readiness(self):
        self.promote(self.states[1])
        previous = self.services.objects[ready.STORAGE_PREFIX + "/latest.json"]
        for status in (403, 404):
            self.services.api_status = status
            with self.subTest(status=status), self.assertRaises(assets.AssetError):
                ready.promote(self.state, self.store, fetch=self.services.request,
                              verify=lambda selected: assets.registry_grid(selected, self.services.request),
                              registry_fetch=self.services.request)
            self.assertEqual(self.services.objects[ready.STORAGE_PREFIX + "/latest.json"], previous)

    def test_same_height_conflicting_object_and_pointer_regression_are_rejected(self):
        self.promote()
        original = self.services.objects[ready.STORAGE_PREFIX + "/latest.json"]
        with self.assertRaisesRegex(assets.AssetError, "regression"):
            self.promote(self.states[1])
        self.assertEqual(self.services.objects[ready.STORAGE_PREFIX + "/latest.json"], original)
        key = ready.STORAGE_PREFIX + "/v1/2/mainnet-frontier-grid.bin"
        self.services.objects[key] = b"tampered", '"other"', ""
        with self.assertRaisesRegex(assets.AssetError, "different bytes"):
            self.promote()

    def test_atomic_pointer_write_cannot_overwrite_another_publisher(self):
        self.promote(self.states[3])
        newer = self.services.objects.pop(ready.STORAGE_PREFIX + "/latest.json")
        def race(key):
            if key.endswith("/latest.json"):
                with self.services.lock:
                    self.services.objects[key] = newer
                self.services.race = None
        self.services.race = race
        with self.assertRaisesRegex(assets.AssetError, "regression"):
            self.promote()
        self.assertEqual(self.services.objects[ready.STORAGE_PREFIX + "/latest.json"], newer)

    def test_concurrent_identical_publishers_converge(self):
        errors = []
        def worker():
            try:
                self.promote(store=self.services.s3())
            except Exception as error:
                errors.append(error)
        workers = [threading.Thread(target=worker) for _ in range(2)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(timeout=30)
            self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        manifest = ready.parse_ready(self.services.objects[ready.STORAGE_PREFIX + "/latest.json"][0], ready.PUBLIC_BASE + "/latest.json")
        self.assertEqual(manifest["height"], 2)

    def test_candidate_cleanup_cannot_delete_advertised_set(self):
        self.promote()
        # The existing exporter only removes release-state/v1/*, never published/v1/*.
        for key in list(self.services.objects):
            if key.startswith("release-state/v1/"):
                del self.services.objects[key]
        with tempfile.TemporaryDirectory() as scratch:
            self.assertEqual(self.consume(Path(scratch) / "ready")["height"], 2)

    def test_moving_pointer_during_read_does_not_mix_sets(self):
        self.promote()
        observed = [False]
        def fetch(url, limit):
            value = self.services.request(url, limit)
            if url.endswith("/published/latest.json") and not observed[0]:
                observed[0] = True
                self.promote(self.states[3])
            return value
        with tempfile.TemporaryDirectory() as scratch:
            manifest = ready.resolve_ready(ready.PUBLIC_BASE + "/latest.json", Path(scratch) / "ready", fetch=fetch,
                                          registry_fetch=self.services.request, cargo_resolve=self.resolve)
        self.assertEqual(manifest["height"], 2)

    def test_missing_pointer_and_unavailable_or_yanked_asset_fail_closed(self):
        with tempfile.TemporaryDirectory() as scratch:
            with self.assertRaises(assets.NotPublished):
                self.consume(Path(scratch) / "missing")
            self.promote()
            self.services.api_status = 403
            with self.assertRaises(assets.AssetError):
                self.consume(Path(scratch) / "denied")
            self.services.api_status = None
            entry = self.services.versions[("zakura-assets", "0.2.0")]
            entry["yanked"] = True
            try:
                with self.assertRaises(assets.AssetError):
                    self.consume(Path(scratch) / "yanked")
            finally:
                entry["yanked"] = False

    def test_storage_denial_is_not_treated_as_absence(self):
        self.services.fail_get = lambda key: key.endswith("/latest.json")
        with self.assertRaisesRegex(assets.AssetError, "storage read"):
            self.promote()
        self.assertNotIn(ready.STORAGE_PREFIX + "/latest.json", self.services.objects)

    def test_commit_acknowledgement_loss_can_only_leave_a_complete_new_set(self):
        store = self.services.s3()
        put = store.put
        def lost_ack(key, *args, **kwargs):
            result = put(key, *args, **kwargs)
            if key.endswith("/latest.json"):
                raise RuntimeError("connection lost after committed write")
            return result
        store.put = lost_ack
        with self.assertRaises(RuntimeError):
            self.promote(store=store)
        with tempfile.TemporaryDirectory() as scratch:
            self.assertEqual(self.consume(Path(scratch) / "ready")["height"], 2)
        self.assertEqual(self.promote()["height"], 2)

    def test_age_boundaries_future_times_and_retries_do_not_refresh_data(self):
        self.promote()
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch)
            self.assertEqual(self.consume(root / "boundary", self.generated + timedelta(hours=48))["height"], 2)
            with self.assertRaisesRegex(assets.AssetError, "48 hours"):
                self.consume(root / "stale", self.generated + timedelta(hours=48, microseconds=1))
        with self.assertRaisesRegex(assets.AssetError, "future"):
            ready.fresh(self.selection["generated_at"], self.generated - timedelta(minutes=11))
        before = self.services.objects[ready.STORAGE_PREFIX + "/latest.json"]
        with self.assertRaisesRegex(assets.AssetError, "48 hours"):
            ready.promote(self.state, self.store, fetch=self.services.request, verify=self.verify,
                          registry_fetch=self.services.request, now=self.generated + timedelta(hours=49))
        self.assertEqual(before, self.services.objects[ready.STORAGE_PREFIX + "/latest.json"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
