"""Offline regression coverage for registry verification and release-state pinning."""

import hashlib
import io
import json
import subprocess
import tarfile
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch

import release_state_assets as assets


class RegistryFixture:
    def __init__(self):
        self.grid = b"verified frontier grid"
        self.identity = dict(meta_url="https://zakura-release.valargroup.dev/release-state/v1/3517272/meta.json",
                             meta_sha256="a" * 64, height=3517272, name="zakura-assets",
                             version="0.3517272.0", grid_sha256=hashlib.sha256(self.grid).hexdigest())
        self.set_archive(self.grid)
        self.calls = []

    def set_archive(self, grid, *, kind=tarfile.REGTYPE, duplicate=False):
        output = io.BytesIO()
        with tarfile.open(fileobj=output, mode="w:gz") as archive:
            for _ in range(2 if duplicate else 1):
                entry = tarfile.TarInfo("zakura-assets-0.3517272.0/src/mainnet-frontier-grid.bin")
                entry.type = kind
                entry.size = len(grid) if kind == tarfile.REGTYPE else 0
                archive.addfile(entry, io.BytesIO(grid))
        self.archive = output.getvalue()
        self.checksum = hashlib.sha256(self.archive).hexdigest()
        self.record = dict(crate="zakura-assets", num="0.3517272.0", yanked=False, checksum=self.checksum)

    def fetch(self, url, limit):
        self.calls.append(url)
        if url.startswith("https://crates.io/api/"):
            return json.dumps({"version": self.record}).encode()
        if url.startswith("https://index.crates.io/"):
            return json.dumps(dict(vers="0.3517272.0", cksum=self.checksum, yanked=False)).encode()
        if url.startswith("https://static.crates.io/"):
            return self.archive
        raise AssertionError(url)


class RegistryTests(unittest.TestCase):
    def setUp(self):
        self.fixture = RegistryFixture()
        self.identity = self.fixture.identity

    def test_existing_matching_version_is_reused(self):
        self.assertFalse(assets.publication_needed(self.identity, self.fixture.fetch))

    def test_only_confirmed_api_absence_permits_upload(self):
        def fetch(url, limit):
            raise assets.NotPublished("API returned 404")
        self.assertTrue(assets.publication_needed(self.identity, fetch))

    def test_archive_absence_does_not_permit_uploading_existing_version(self):
        def fetch(url, limit):
            if url.startswith("https://static"):
                raise assets.NotPublished("archive is absent")
            return self.fixture.fetch(url, limit)
        with self.assertRaises(assets.NotPublished):
            assets.publication_needed(self.identity, fetch)

    def test_api_access_denial_never_means_absent(self):
        def fetch(url, limit):
            raise assets.AssetError("HTTP 403")
        with self.assertRaisesRegex(assets.AssetError, "403"):
            assets.publication_needed(self.identity, fetch)

    def test_http_403_and_404_remain_distinct(self):
        for code, error_type in ((403, assets.AssetError), (404, assets.NotPublished)):
            error = urllib.error.HTTPError("https://crates.io", code, "error", {}, None)
            with patch.object(assets.urllib.request, "build_opener") as opener:
                opener.return_value.open.side_effect = error
                with self.assertRaises(error_type) as caught:
                    assets.download("https://crates.io", 10)
                if code == 403:
                    self.assertNotIsInstance(caught.exception, assets.NotPublished)

    def test_response_size_is_bounded(self):
        with patch.object(assets.urllib.request, "build_opener") as opener:
            opener.return_value.open.return_value.__enter__.return_value.read.return_value = b"long"
            with self.assertRaisesRegex(assets.AssetError, "size limit"):
                assets.download("https://crates.io", 3)

    def test_wrong_grid_is_rejected_even_with_valid_archive_checksum(self):
        self.fixture.set_archive(b"different grid")
        with self.assertRaisesRegex(assets.AssetError, "differs"):
            assets.registry_grid(self.identity, self.fixture.fetch)

    def test_archive_checksum_is_verified(self):
        self.fixture.record["checksum"] = "0" * 64
        with self.assertRaisesRegex(assets.AssetError, "checksum"):
            assets.registry_grid(self.identity, self.fixture.fetch)

    def test_malformed_or_yanked_metadata_is_rejected(self):
        for key, value in (("yanked", True), ("num", "0.1.0"), ("crate", "other"), ("checksum", "invalid")):
            fixture = RegistryFixture()
            fixture.record[key] = value
            with self.subTest(key=key), self.assertRaises(assets.AssetError):
                assets.registry_grid(fixture.identity, fixture.fetch)
        with self.assertRaises(json.JSONDecodeError):
            assets.registry_grid(self.identity, lambda url, limit: b"not JSON")

    def test_duplicate_and_symlink_grid_entries_are_rejected(self):
        for kind, duplicate in ((tarfile.SYMTYPE, False), (tarfile.REGTYPE, True)):
            self.fixture.set_archive(self.fixture.grid, kind=kind, duplicate=duplicate)
            with self.assertRaises(assets.AssetError):
                assets.registry_grid(self.identity, self.fixture.fetch)

    def test_index_propagation_is_bounded_and_resolves_the_exact_package(self):
        now = [0]
        index_calls = [0]
        resolutions = []
        def fetch(url, limit):
            if url.startswith("https://index"):
                index_calls[0] += 1
                if index_calls[0] == 1:
                    return b""
            return self.fixture.fetch(url, limit)
        def sleep(seconds):
            now[0] += seconds
        assets.verify_visible(self.identity, fetch=fetch, resolve=lambda identity, **kw: resolutions.append(identity),
                              clock=lambda: now[0], sleep=sleep)
        self.assertEqual(now[0], 15)
        self.assertEqual(resolutions, [self.identity])

    def test_publication_visibility_timeout(self):
        now = [0]
        def absent(url, limit):
            raise assets.NotPublished("not visible yet")
        def sleep(seconds):
            now[0] += seconds
        with self.assertRaisesRegex(assets.AssetError, "timed out"):
            assets.verify_visible(self.identity, fetch=absent, clock=lambda: now[0], sleep=sleep, timeout=30)
        self.assertEqual(now[0], 30)

    def test_access_denial_and_mismatch_are_not_retried(self):
        for reason in ("HTTP 403", "digest mismatch"):
            calls = []
            def fail(url, limit):
                calls.append(url)
                raise assets.AssetError(reason)
            with self.assertRaises(assets.AssetError):
                assets.verify_visible(self.identity, fetch=fail, sleep=lambda _: self.fail("must not retry"))
            self.assertEqual(len(calls), 1)

    def test_cargo_must_resolve_the_matching_crates_io_grid(self):
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch)
            (root / "src").mkdir()
            (root / "src/mainnet-frontier-grid.bin").write_bytes(self.fixture.grid)
            package = dict(name="zakura-assets", version="0.3517272.0", manifest_path=str(root / "Cargo.toml"),
                           source="registry+https://github.com/rust-lang/crates.io-index")
            result = subprocess.CompletedProcess([], 0, json.dumps({"packages": [package]}), "")
            with patch.object(assets.subprocess, "run", return_value=result):
                assets.cargo_grid(self.identity)
                (root / "src/mainnet-frontier-grid.bin").write_bytes(b"different")
                with self.assertRaisesRegex(assets.AssetError, "different frontier grid"):
                    assets.cargo_grid(self.identity)


class PinTests(unittest.TestCase):
    def setUp(self):
        self.fixture = RegistryFixture()
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name)
        self.manifest = self.root / "Cargo.toml"
        self.original = '[workspace.dependencies]\nzakura-assets = { version = "=0.3513777.0" }\n'
        self.manifest.write_text(self.original)
        self.lock = self.root / "Cargo.lock"
        self.lock.write_text('version = 4\n[[package]]\nname = "zakura-assets"\nversion = "0.3513777.0"\n'
                             'source = "registry+https://github.com/rust-lang/crates.io-index"\n'
                             f'checksum = "{self.fixture.checksum}"\n'
                             '[[package]]\nname = "unrelated"\nversion = "1.0.0"\n')
        self.imported = {**self.fixture.identity, "has_changes": True,
                         "frontier_grid_sha256": self.fixture.identity["grid_sha256"]}

    def update(self, *args, **kwargs):
        self.assertEqual(args[0], ["cargo", "update", "-p", "zakura-assets", "--precise", "0.3517272.0"])
        self.lock.write_text(self.lock.read_text().replace('"0.3513777.0"', '"0.3517272.0"'))

    def test_pin_moves_manifest_and_lock_together(self):
        with patch.object(assets.subprocess, "run", side_effect=self.update), \
                patch.object(assets, "cargo_grid") as resolve:
            assets.pin_assets(self.root, self.imported, fetch=self.fixture.fetch)
        self.assertIn('version = "=0.3517272.0"', self.manifest.read_text())
        self.assertIn('version = "0.3517272.0"', self.lock.read_text())
        resolve.assert_called_once()

    def test_unrelated_lockfile_changes_fail(self):
        def update(*args, **kw):
            self.update(*args, **kw)
            self.lock.write_text(self.lock.read_text().replace('"1.0.0"', '"2.0.0"'))
        with patch.object(assets.subprocess, "run", side_effect=update):
            with self.assertRaisesRegex(assets.AssetError, "unrelated"):
                assets.pin_assets(self.root, self.imported, fetch=self.fixture.fetch)

    def test_resolution_failure_is_propagated(self):
        with patch.object(assets.subprocess, "run", side_effect=subprocess.CalledProcessError(1, "cargo")):
            with self.assertRaises(subprocess.CalledProcessError):
                assets.pin_assets(self.root, self.imported, fetch=self.fixture.fetch)

    def test_no_op_does_not_access_registry_or_mutate_files(self):
        assets.pin_assets(self.root, {"has_changes": False}, fetch=lambda *a: self.fail("no fetch expected"))
        self.assertEqual(self.manifest.read_text(), self.original)

    def test_ready_archive_checksum_must_match(self):
        with self.assertRaisesRegex(assets.AssetError, "ready manifest"):
            assets.pin_assets(self.root, self.imported, expected_checksum="0" * 64, fetch=self.fixture.fetch)
        self.assertEqual(self.manifest.read_text(), self.original)


if __name__ == "__main__":
    unittest.main()
