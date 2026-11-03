"""Complete-set publication and consumption against in-memory storage, a loopback registry, and real Cargo."""

import copy
import hashlib
import io
import json
import os
import shutil
import struct
import subprocess
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import release_state
from release_state import FETCH, IMPORT, PACK, POINTER_KEY, READY_URL, STORAGE_PREFIX
from release_state_fakes import FixtureRegistry, MemoryStore
from release_state_registry import AssetError, Registry

BundleError = FETCH.BundleError


def write_candidate(state: Path, height: int, generated: datetime) -> None:
    """Write a selected candidate state directory whose bundle ends at `height`."""
    bundle = state / "bundle"
    bundle.mkdir(parents=True)
    grid = PACK._build_grid(heights=list(range(height)), last_checkpoint=height)
    subtree_prefix = IMPORT.SUBTREE_HEADER_PREFIX.pack(b"ZKVCTST1", 1, 1, height, 0, 0, 0)
    files = {"main-checkpoints.txt": b"".join(f"{h} {str(h) * 64}\n".encode() for h in range(1, height + 1)),
             "mainnet-frontier.bin": struct.pack("<I", height) + struct.pack("<I", 0) * 3,
             "mainnet-treestate-subtrees.bin": subtree_prefix + hashlib.sha256(subtree_prefix).digest(),
             "mainnet-frontier-grid.bin": grid}
    meta = dict(schema_version=1, network="Mainnet", height=height, block_hash=str(height) * 64,
                generated_at=generated.isoformat().replace("+00:00", "Z"),
                files={name: dict(size=len(data), sha256=hashlib.sha256(data).hexdigest())
                       for name, data in files.items()})
    meta_bytes = json.dumps(meta, sort_keys=True).encode()
    for name, data in {**files, "meta.json": meta_bytes}.items():
        (bundle / name).write_bytes(data)
    resolution = dict(height=height, block_hash=meta["block_hash"], generated_at=meta["generated_at"],
                      meta_url=f"https://zakura-release.valargroup.dev/release-state/v1/{height}/meta.json",
                      meta_sha256=hashlib.sha256(meta_bytes).hexdigest())
    selection = {**resolution, "has_changes": True, "name": "zakura-assets", "version": f"0.{height}.0",
                 "grid_sha256": hashlib.sha256(grid).hexdigest()}
    (state / "resolution.json").write_text(json.dumps(resolution))
    (state / "selection.json").write_text(json.dumps(selection))


def publish_crate(registry: Registry, producer: Path, grid: Path) -> None:
    """Publish a grid package for `grid` to the fixture registry with real Cargo."""
    crate = producer / "crates/zakura-assets"
    (crate / "src").mkdir(parents=True)
    (crate / "Cargo.toml").write_text(
        '[package]\nname = "zakura-assets"\nversion = "0.0.0"\nedition = "2021"\n'
        'description = "Private publication fixture"\nlicense = "MIT"\ninclude = ["src/**", "Cargo.toml"]\n')
    (crate / "src/lib.rs").write_text('pub static GRID: &[u8] = include_bytes!("mainnet-frontier-grid.bin");\n')
    PACK.pack(grid, crate, 0)
    registry.publish(producer)


class ReadyUrlTests(unittest.TestCase):
    def test_invalid_ready_urls_are_rejected_without_a_request(self):
        urls = (
            "http://127.0.0.1:1/private",
            "https://example.com/release-state/published/latest.json",
            release_state.PUBLIC_BASE.removesuffix("/published") + "/latest.json",
            READY_URL + "?redirect=elsewhere",
        )
        for url in urls:
            with self.subTest(url=url), tempfile.TemporaryDirectory() as scratch:
                def fetch(url, limit):
                    self.fail("an invalid URL must not be requested")
                with self.assertRaises((AssetError, BundleError)):
                    release_state.fetch_ready(url, Path(scratch) / "state", fetch=fetch)


class S3StoreTests(unittest.TestCase):
    def setUp(self):
        import boto3
        from botocore.config import Config
        from botocore.stub import Stubber
        client = boto3.client("s3", endpoint_url="https://storage.invalid", region_name="auto",
                              aws_access_key_id="fixture-only", aws_secret_access_key="fixture-only",
                              config=Config(retries={"max_attempts": 0}))
        self.stub = Stubber(client)
        self.stub.activate()
        self.addCleanup(self.stub.deactivate)
        self.store = release_state.S3Store(client, "bucket")

    def put_params(self, **condition):
        return dict(Bucket="bucket", Key="latest.json", Body=b"data", ContentLength=4,
                    ContentType="application/json", CacheControl=release_state.UNCACHED, **condition)

    def test_only_a_missing_object_reads_as_absent(self):
        self.stub.add_client_error("get_object", service_error_code="NoSuchKey", http_status_code=404)
        self.assertIsNone(self.store.get("key", 10))
        self.stub.add_client_error("get_object", service_error_code="AccessDenied", http_status_code=403)
        with self.assertRaisesRegex(AssetError, "storage read"):
            self.store.get("key", 10)

    def test_reads_are_bounded(self):
        from botocore.response import StreamingBody
        for data, limit in ((b"fits", 4), (b"too long", 4)):
            self.stub.add_response("get_object", {"Body": StreamingBody(io.BytesIO(data), len(data)), "ETag": '"e"'},
                                   {"Bucket": "bucket", "Key": "key"})
        self.assertEqual(self.store.get("key", 4), (b"fits", '"e"'))
        with self.assertRaisesRegex(AssetError, "size limit"):
            self.store.get("key", 4)

    def test_writes_create_or_replace_conditionally(self):
        self.stub.add_response("put_object", {"ETag": '"new"'}, self.put_params(IfNoneMatch="*"))
        self.stub.add_response("put_object", {"ETag": '"new"'}, self.put_params(IfMatch='"old"'))
        self.assertTrue(self.store.put("latest.json", b"data", cache=release_state.UNCACHED))
        self.assertTrue(self.store.put("latest.json", b"data", etag='"old"', cache=release_state.UNCACHED))
        self.stub.assert_no_pending_responses()

    def test_lost_conditions_return_false_and_other_failures_raise(self):
        for code, status in (("PreconditionFailed", 412), ("ConditionalRequestConflict", 409)):
            self.stub.add_client_error("put_object", service_error_code=code, http_status_code=status)
            self.assertFalse(self.store.put("key", b"data"))
        self.stub.add_client_error("put_object", service_error_code="SlowDown", http_status_code=503)
        with self.assertRaisesRegex(AssetError, "storage write"):
            self.store.put("key", b"data")


class CompleteSetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.crates = FixtureRegistry()
        cls.workspace = tempfile.TemporaryDirectory(prefix="zakura-ready-e2e-")
        cls.root = Path(cls.workspace.name)
        cls.registry = cls.crates.registry(cls.root / "cargo-home")
        cls.generated = datetime.now(timezone.utc).replace(microsecond=0)
        cls.states = {}
        for height in (1, 2, 3):
            state = cls.states[height] = cls.root / f"candidate-{height}"
            write_candidate(state, height, cls.generated)
            selected = json.loads((state / "selection.json").read_text())
            if not cls.registry.publication_needed(selected):
                raise AssertionError("new fixture version unexpectedly exists")
            publish_crate(cls.registry, cls.root / f"producer-{height}", state / "bundle/mainnet-frontier-grid.bin")

    @classmethod
    def tearDownClass(cls):
        cls.crates.close()
        cls.workspace.cleanup()

    def setUp(self):
        self.crates.reset()
        self.store = MemoryStore()
        self.state = self.states[2]
        self.selection = json.loads((self.state / "selection.json").read_text())

    def pointer(self):
        return self.store.objects.get(POINTER_KEY)

    def promote(self, state=None, *, store=None, fetch=None, **kwargs):
        store = store or self.store
        return release_state.promote(state or self.state, store, registry=self.registry,
                                     fetch=fetch or store.public_fetch, **kwargs)

    def consume(self, output, *, fetch=None, now=None):
        return release_state.fetch_ready(READY_URL, output, fetch=fetch or self.store.public_fetch,
                                         registry=self.registry, now=now)

    def test_publication_then_real_import_and_cargo_lockfile_pin(self):
        self.promote()
        # A consumer starts at height one, with a real lockfile for its older fixture package.
        with tempfile.TemporaryDirectory(prefix="ready-consumer-") as scratch:
            root = Path(scratch)
            (root / "src").mkdir()
            (root / "src/lib.rs").write_text("")
            (root / "Cargo.toml").write_text(
                '[package]\nname="ready-consumer"\nversion="0.0.0"\nedition="2021"\n'
                '[workspace.dependencies]\nzakura-assets = { version = "=0.1.0", registry = "fixture" }\n'
                '[dependencies]\nzakura-assets.workspace = true\n')
            for relative, name in ((IMPORT.CHECKPOINTS, "main-checkpoints.txt"),
                                   (IMPORT.FRONTIER, "mainnet-frontier.bin"),
                                   (IMPORT.SUBTREES, "mainnet-treestate-subtrees.bin")):
                (root / relative).parent.mkdir(parents=True, exist_ok=True)
                (root / relative).write_bytes((self.states[1] / "bundle" / name).read_bytes())
            (root / IMPORT.PROVENANCE).write_text("{}")
            (root / PACK.CRATE_DIR).mkdir(parents=True)
            (root / PACK.CRATE_DIR / "Cargo.toml").write_text('name = "zakura-assets"\n')
            subprocess.run(["cargo", "metadata", "--format-version", "1"], cwd=root, env=self.registry.cargo_env,
                           check=True, capture_output=True, timeout=30)
            fetched = root / "ready-input"
            release_state.fetch_ready(READY_URL, fetched, root=root, fetch=self.store.public_fetch,
                                      registry=self.registry)
            imported = IMPORT.import_bundle(root, fetched / "bundle", fetched / "resolution.json", floor_eos=False)
            release_state.pin_ready_asset(root, fetched, imported, self.registry)
            self.assertIn('version = "=0.2.0"', (root / "Cargo.toml").read_text())
            self.assertIn('version = "0.2.0"', (root / "Cargo.lock").read_text())
            self.assertEqual((root / IMPORT.CHECKPOINTS).read_bytes(),
                             (self.state / "bundle/main-checkpoints.txt").read_bytes())

    def test_imports_without_changes_pin_nothing(self):
        self.promote()
        with tempfile.TemporaryDirectory() as scratch:
            self.consume(Path(scratch) / "ready")
            resolution = json.loads((Path(scratch) / "ready/resolution.json").read_text())
            release_state.pin_ready_asset(Path(scratch), Path(scratch) / "ready", {**resolution, "has_changes": False},
                                          Registry(fetch=lambda *args: self.fail("no registry access expected")))

    def test_failed_cargo_upload_and_delayed_registry_visibility(self):
        self.promote(self.states[1])
        old = self.pointer()
        key = ("zakura-assets", "0.2.0")
        entry = self.crates.crates.pop(key)
        try:
            self.assertTrue(self.registry.publication_needed(self.selection))
            self.crates.publish_status = 403
            with self.assertRaises(subprocess.CalledProcessError):
                self.registry.publish(self.root / "producer-2")
            self.assertNotIn(key, self.crates.crates)
            self.assertEqual(self.pointer(), old)
        finally:
            self.crates.publish_status = None
            self.crates.crates[key] = entry
        # Promotion polls a temporarily missing sparse index, then asks real Cargo to resolve the version.
        self.crates.index_misses = 2
        self.assertEqual(self.promote()["height"], 2)
        self.assertEqual(self.crates.index_misses, 0)

    def test_tampered_public_files_and_manifest_cannot_be_consumed(self):
        self.promote()
        for name in ("mainnet-frontier.bin", "mainnet-frontier-grid.bin", "meta.json", "latest.json"):
            key = POINTER_KEY if name == "latest.json" else f"{STORAGE_PREFIX}/v1/2/{name}"
            old = self.store.objects[key]
            self.store.objects[key] = b"tampered", '"changed"'
            try:
                with tempfile.TemporaryDirectory() as scratch:
                    output = Path(scratch) / "ready"
                    with self.subTest(name=name), self.assertRaises((AssetError, BundleError)):
                        self.consume(output)
                    self.assertFalse(output.exists())
            finally:
                self.store.objects[key] = old

    def test_identical_retry_preserves_original_manifest_and_generation_time(self):
        first = self.promote()
        before = self.pointer()
        self.assertEqual(self.promote(), first)
        self.assertEqual(self.pointer(), before)
        self.assertEqual(first["generated_at"], self.selection["generated_at"])

    def test_identical_retry_verifies_existing_objects_without_upload_attempts(self):
        self.promote()
        with patch.object(self.store, "put", side_effect=AssertionError("identical retry must reuse existing objects")):
            self.assertEqual(self.promote()["height"], 2)

    def test_conflicting_checkpoint_identity_is_never_published_or_consumed(self):
        self.promote()
        before = copy.deepcopy(self.store.objects)
        with tempfile.TemporaryDirectory() as scratch:
            state = Path(scratch) / "candidate"
            shutil.copytree(self.state, state)
            meta = json.loads((state / "bundle/meta.json").read_text())
            meta["block_hash"] = "0" * 64
            meta_bytes = json.dumps(meta).encode()
            (state / "bundle/meta.json").write_bytes(meta_bytes)
            selection = {**self.selection, "block_hash": meta["block_hash"],
                         "meta_sha256": hashlib.sha256(meta_bytes).hexdigest()}
            (state / "selection.json").write_text(json.dumps(selection))
            with self.assertRaisesRegex(AssetError, "terminal checkpoint"):
                self.promote(state)
            self.assertEqual(self.store.objects, before)
            # Even a self-consistent pointer and meta digest must not hide a conflicting checkpoint.
            pointer = json.loads(self.pointer()[0])
            pointer.update(block_hash=selection["block_hash"], meta_sha256=selection["meta_sha256"])
            self.store.objects[POINTER_KEY] = json.dumps(pointer).encode(), '"changed"'
            self.store.objects[f"{STORAGE_PREFIX}/v1/2/meta.json"] = meta_bytes, '"changed"'
            output = Path(scratch) / "consumer"
            with self.assertRaisesRegex(AssetError, "terminal checkpoint"):
                self.consume(output)
            self.assertFalse(output.exists())

    def test_identical_retry_requires_a_publicly_readable_pointer(self):
        def hide_pointer(url, limit):
            if url == READY_URL:
                raise BundleError("HTTP Error 404")
            return self.store.public_fetch(url, limit)
        for _attempt in range(2):
            with self.assertRaises(BundleError):
                self.promote(fetch=hide_pointer)
        self.assertEqual(self.promote()["height"], 2)

    def test_identical_retry_rejects_a_cached_older_pointer(self):
        self.promote(self.states[1])
        cached = self.pointer()[0]
        def fetch(url, limit):
            return cached if url == READY_URL else self.store.public_fetch(url, limit)
        for _attempt in range(2):
            with self.assertRaisesRegex(AssetError, "public ready pointer"):
                self.promote(fetch=fetch)
        self.assertEqual(self.promote()["height"], 2)

    def test_failure_after_each_immutable_write_never_advertises_partial_data(self):
        self.promote(self.states[1])
        old = self.pointer()
        initial = copy.deepcopy(self.store.objects)
        put_immutable = release_state.put_immutable
        for boundary in range(1, 6):
            self.store.objects = copy.deepcopy(initial)
            writes = [0]
            def crash_after_boundary(store, key, data):
                put_immutable(store, key, data)
                writes[0] += 1
                if writes[0] == boundary:
                    raise RuntimeError("simulated process crash")
            with self.subTest(boundary=boundary):
                with patch.object(release_state, "put_immutable", crash_after_boundary), self.assertRaises(RuntimeError):
                    self.promote()
                self.assertEqual(self.pointer(), old)
                self.assertEqual(self.promote()["height"], 2)

    def test_upload_failure_and_cached_public_404_keep_previous_ready_pointer(self):
        self.promote(self.states[1])
        previous = self.pointer()
        put = self.store.put
        def failing_put(key, *args, **kwargs):
            if "/v1/2/" in key:
                raise AssetError("object storage write failed")
            return put(key, *args, **kwargs)
        def hidden_set(url, limit):
            if "/v1/2/" in url:
                raise BundleError("HTTP Error 404")
            return self.store.public_fetch(url, limit)
        with patch.object(self.store, "put", failing_put), self.assertRaisesRegex(AssetError, "storage write"):
            self.promote()
        self.assertEqual(self.pointer(), previous)
        with self.assertRaises(BundleError):
            self.promote(fetch=hidden_set)
        self.assertEqual(self.pointer(), previous)
        self.assertEqual(self.promote()["height"], 2)

    def test_missing_or_denied_registry_never_advances_readiness(self):
        self.promote(self.states[1])
        previous = self.pointer()
        for status in (403, 404):
            self.crates.api_status = status
            with self.subTest(status=status), self.assertRaises(AssetError):
                self.promote(visibility_timeout=0)
            self.assertEqual(self.pointer(), previous)

    def test_same_height_conflicting_object_and_pointer_regression_are_rejected(self):
        self.promote()
        original = self.pointer()
        with self.assertRaisesRegex(AssetError, "regression"):
            self.promote(self.states[1])
        self.assertEqual(self.pointer(), original)
        self.store.objects[f"{STORAGE_PREFIX}/v1/2/mainnet-frontier-grid.bin"] = b"tampered", '"other"'
        with self.assertRaisesRegex(AssetError, "different bytes"):
            self.promote()

    def test_atomic_pointer_write_cannot_overwrite_another_publisher(self):
        self.promote(self.states[3])
        newer = self.store.objects.pop(POINTER_KEY)
        put = self.store.put
        def racing_put(key, *args, **kwargs):
            # Another publisher commits a newer pointer between our read and our conditional write.
            if key == POINTER_KEY and POINTER_KEY not in self.store.objects:
                self.store.objects[POINTER_KEY] = newer
            return put(key, *args, **kwargs)
        with patch.object(self.store, "put", racing_put), self.assertRaisesRegex(AssetError, "regression"):
            self.promote()
        self.assertEqual(self.pointer(), newer)

    def test_concurrent_identical_publishers_converge(self):
        errors = []
        def publisher():
            try:
                self.promote()
            except Exception as error:
                errors.append(error)
        threads = [threading.Thread(target=publisher) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
            self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(release_state.parse_ready(self.pointer()[0], READY_URL)["height"], 2)

    def test_consumption_survives_removal_of_candidate_objects(self):
        for height, state in self.states.items():
            for file in (state / "bundle").iterdir():
                release_state.put_immutable(self.store, f"release-state/v1/{height}/{file.name}", file.read_bytes())
        self.promote()
        candidates = [key for key in self.store.objects if key.startswith("release-state/v1/")]
        self.assertTrue(candidates)
        for key in candidates:
            del self.store.objects[key]
        with tempfile.TemporaryDirectory() as scratch:
            self.assertEqual(self.consume(Path(scratch) / "ready")["height"], 2)

    def test_moving_pointer_during_read_does_not_mix_sets(self):
        self.promote()
        moved = [False]
        def fetch(url, limit):
            value = self.store.public_fetch(url, limit)
            if url == READY_URL and not moved[0]:
                moved[0] = True
                self.promote(self.states[3])
            return value
        with tempfile.TemporaryDirectory() as scratch:
            self.assertEqual(self.consume(Path(scratch) / "ready", fetch=fetch)["height"], 2)

    def test_missing_pointer_and_unavailable_or_yanked_asset_fail_closed(self):
        with tempfile.TemporaryDirectory() as scratch:
            with self.assertRaises(BundleError):
                self.consume(Path(scratch) / "missing")
            self.promote()
            self.crates.api_status = 403
            with self.assertRaises(AssetError):
                self.consume(Path(scratch) / "denied")
            self.crates.api_status = None
            entry = self.crates.crates[("zakura-assets", "0.2.0")]
            entry["yanked"] = True
            try:
                with self.assertRaises(AssetError):
                    self.consume(Path(scratch) / "yanked")
            finally:
                entry["yanked"] = False

    def test_commit_acknowledgement_loss_can_only_leave_a_complete_new_set(self):
        put = self.store.put
        def lost_ack(key, *args, **kwargs):
            result = put(key, *args, **kwargs)
            if key == POINTER_KEY:
                raise RuntimeError("connection lost after committed write")
            return result
        with patch.object(self.store, "put", lost_ack), self.assertRaises(RuntimeError):
            self.promote()
        with tempfile.TemporaryDirectory() as scratch:
            self.assertEqual(self.consume(Path(scratch) / "ready")["height"], 2)
        self.assertEqual(self.promote()["height"], 2)

    def test_age_boundaries_future_times_and_retries_do_not_refresh_data(self):
        self.promote()
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch)
            self.assertEqual(self.consume(root / "boundary", now=self.generated + timedelta(hours=48))["height"], 2)
            with self.assertRaisesRegex(AssetError, "48 hours"):
                self.consume(root / "stale", now=self.generated + timedelta(hours=48, microseconds=1))
        with self.assertRaisesRegex(AssetError, "future"):
            release_state.fresh(self.selection["generated_at"], self.generated - timedelta(minutes=11))
        before = self.pointer()
        with self.assertRaisesRegex(AssetError, "48 hours"):
            self.promote(now=self.generated + timedelta(hours=49))
        self.assertEqual(self.pointer(), before)


class CommandLineTests(unittest.TestCase):
    def test_branch_runs_refuse_upload_and_storage_credentials(self):
        with tempfile.TemporaryDirectory() as scratch:
            state = Path(scratch)
            write_candidate(state / "state", 2, datetime.now(timezone.utc))
            branch = {"GITHUB_REPOSITORY": "zakura-core/zakura", "GITHUB_REF": "refs/heads/fixture"}
            with patch.dict(os.environ, branch), \
                    patch.object(release_state, "production_store", side_effect=AssertionError("credentials requested")), \
                    patch.object(Registry, "publish", side_effect=AssertionError("a branch must not upload")):
                for command in ("promote", "publish"):
                    with self.subTest(command=command), self.assertRaisesRegex(AssetError, "canonical main"):
                        release_state.main([command, "--state", str(state / "state")])


if __name__ == "__main__":
    unittest.main(verbosity=2)
