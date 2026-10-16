"""Offline coverage for registry verification and asset pinning."""

import gzip
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

import release_state_registry as registry
from release_state_registry import AssetError, NotPublished, Registry

NAME, VERSION = "zakura-assets", "0.3517272.0"
GRID_MEMBER = f"{NAME}-{VERSION}/src/mainnet-frontier-grid.bin"


class FakeCratesIo:
    """Serves one published version through the three crates.io endpoints."""

    def __init__(self):
        self.grid = b"verified frontier grid"
        self.asset = dict(meta_url="https://zakura-release.valargroup.dev/release-state/v1/3517272/meta.json",
                          meta_sha256="a" * 64, height=3517272, name=NAME, version=VERSION,
                          grid_sha256=hashlib.sha256(self.grid).hexdigest())
        self.set_archive(self.grid)

    def set_archive(self, grid, *, kind=tarfile.REGTYPE, duplicate=False):
        output = io.BytesIO()
        with tarfile.open(fileobj=output, mode="w:gz") as archive:
            for _ in range(2 if duplicate else 1):
                entry = tarfile.TarInfo(GRID_MEMBER)
                entry.type = kind
                entry.size = len(grid) if kind == tarfile.REGTYPE else 0
                archive.addfile(entry, io.BytesIO(grid))
        self.set_raw_archive(output.getvalue())

    def set_raw_archive(self, archive):
        self.archive = archive
        self.checksum = hashlib.sha256(archive).hexdigest()
        self.record = dict(crate=NAME, num=VERSION, yanked=False, checksum=self.checksum)

    def fetch(self, url, limit):
        if url.startswith("https://crates.io/api/"):
            return json.dumps({"version": self.record}).encode()
        if url.startswith("https://index.crates.io/"):
            return json.dumps(dict(vers=VERSION, cksum=self.checksum, yanked=False)).encode()
        if url.startswith("https://static.crates.io/"):
            return self.archive
        raise AssertionError(url)

    def registry(self, fetch=None):
        return Registry(fetch=fetch or self.fetch)


class RegistryTests(unittest.TestCase):
    def setUp(self):
        self.crates = FakeCratesIo()
        self.asset = self.crates.asset

    def test_existing_matching_version_is_reused(self):
        self.assertFalse(self.crates.registry().publication_needed(self.asset))

    def test_only_confirmed_api_absence_permits_upload(self):
        def absent(url, limit):
            raise NotPublished("API returned 404")
        self.assertTrue(self.crates.registry(absent).publication_needed(self.asset))

    def test_archive_absence_does_not_permit_uploading_existing_version(self):
        def fetch(url, limit):
            if url.startswith("https://static"):
                raise NotPublished("archive is absent")
            return self.crates.fetch(url, limit)
        with self.assertRaises(NotPublished):
            self.crates.registry(fetch).publication_needed(self.asset)

    def test_api_access_denial_never_means_absent(self):
        def denied(url, limit):
            raise AssetError("HTTP 403")
        with self.assertRaisesRegex(AssetError, "403"):
            self.crates.registry(denied).publication_needed(self.asset)

    def test_http_403_and_404_remain_distinct(self):
        for code, error_type in ((403, AssetError), (404, NotPublished)):
            error = urllib.error.HTTPError("https://crates.io", code, "error", {}, None)
            with patch.object(registry.urllib.request, "build_opener") as opener:
                opener.return_value.open.side_effect = error
                with self.assertRaises(error_type) as caught:
                    registry.download("https://crates.io", 10)
                if code == 403:
                    self.assertNotIsInstance(caught.exception, NotPublished)

    def test_response_size_is_bounded(self):
        with patch.object(registry.urllib.request, "build_opener") as opener:
            opener.return_value.open.return_value.__enter__.return_value.read.return_value = b"long"
            with self.assertRaisesRegex(AssetError, "size limit"):
                registry.download("https://crates.io", 3)

    def test_wrong_grid_is_rejected_even_with_valid_archive_checksum(self):
        self.crates.set_archive(b"different grid")
        with self.assertRaisesRegex(AssetError, "differs"):
            self.crates.registry().archive_checksum(self.asset)

    def test_archive_checksum_is_verified(self):
        self.crates.record["checksum"] = "0" * 64
        with self.assertRaisesRegex(AssetError, "checksum"):
            self.crates.registry().archive_checksum(self.asset)

    def test_malformed_or_yanked_metadata_is_rejected(self):
        for key, value in (("yanked", True), ("num", "0.1.0"), ("crate", "other"), ("checksum", "invalid")):
            crates = FakeCratesIo()
            crates.record[key] = value
            with self.subTest(key=key), self.assertRaises(AssetError):
                crates.registry().archive_checksum(crates.asset)
        with self.assertRaises(json.JSONDecodeError):
            Registry(fetch=lambda url, limit: b"not JSON").archive_checksum(self.asset)

    def test_duplicate_and_symlink_grid_entries_are_rejected(self):
        for kind, duplicate in ((tarfile.SYMTYPE, False), (tarfile.REGTYPE, True)):
            self.crates.set_archive(self.crates.grid, kind=kind, duplicate=duplicate)
            with self.subTest(kind=kind), self.assertRaises(AssetError):
                self.crates.registry().archive_checksum(self.asset)

    def test_decompressed_archive_bound_includes_extended_headers(self):
        output = io.BytesIO()
        with tarfile.open(fileobj=output, mode="w:gz", format=tarfile.PAX_FORMAT) as archive:
            entry = tarfile.TarInfo(GRID_MEMBER)
            entry.size = len(self.crates.grid)
            entry.pax_headers = {"comment": "x" * (64 * 1024)}
            archive.addfile(entry, io.BytesIO(self.crates.grid))
        self.crates.set_raw_archive(output.getvalue())
        self.assertLess(len(self.crates.archive), 16 * 1024)
        with patch.object(registry, "ARCHIVE_LIMIT", 16 * 1024):
            with self.assertRaisesRegex(AssetError, "expanded asset archive"):
                self.crates.registry().archive_checksum(self.asset)

    def test_decompressed_archive_accepts_the_exact_bound(self):
        expanded_size = len(gzip.decompress(self.crates.archive))
        with patch.object(registry, "ARCHIVE_LIMIT", expanded_size):
            self.assertEqual(self.crates.registry().archive_checksum(self.asset), self.crates.checksum)
        with patch.object(registry, "ARCHIVE_LIMIT", expanded_size - 1):
            with self.assertRaisesRegex(AssetError, "expanded asset archive"):
                self.crates.registry().archive_checksum(self.asset)

    def test_sparse_grid_cannot_expand_past_the_size_bound(self):
        for size in (registry.GRID_LIMIT + 1, registry.ARCHIVE_LIMIT + 1):
            output = io.BytesIO()
            with tarfile.open(fileobj=output, mode="w:gz", format=tarfile.PAX_FORMAT) as archive:
                entry = tarfile.TarInfo(GRID_MEMBER)
                entry.size = 1
                entry.pax_headers = {"GNU.sparse.map": "0,1", "GNU.sparse.size": str(size)}
                archive.addfile(entry, io.BytesIO(b"x"))
            self.crates.set_raw_archive(output.getvalue())
            self.assertLess(len(gzip.decompress(self.crates.archive)), registry.ARCHIVE_LIMIT)
            with self.subTest(size=size), \
                    patch.object(tarfile.TarFile, "extractfile", side_effect=AssertionError("oversized grid read")), \
                    self.assertRaisesRegex(AssetError, "bounded, regular frontier grid"):
                self.crates.registry().archive_checksum(self.asset)

    def test_grid_accepts_the_exact_size_bound(self):
        with patch.object(registry, "GRID_LIMIT", len(self.crates.grid)):
            self.assertEqual(self.crates.registry().archive_checksum(self.asset), self.crates.checksum)
        with patch.object(registry, "GRID_LIMIT", len(self.crates.grid) - 1):
            with self.assertRaisesRegex(AssetError, "bounded, regular frontier grid"):
                self.crates.registry().archive_checksum(self.asset)

    def test_index_propagation_is_retried_and_resolves_the_exact_package(self):
        now, index_calls, resolutions = [0], [0], []
        def fetch(url, limit):
            if url.startswith("https://index"):
                index_calls[0] += 1
                if index_calls[0] == 1:
                    return b""
            return self.crates.fetch(url, limit)
        def sleep(seconds):
            now[0] += seconds
        with patch.object(Registry, "resolve", lambda self, asset: resolutions.append(asset)):
            checksum = self.crates.registry(fetch).wait_until_visible(
                self.asset, clock=lambda: now[0], sleep=sleep)
        self.assertEqual(checksum, self.crates.checksum)
        self.assertEqual(now[0], 15)
        self.assertEqual(resolutions, [self.asset])

    def test_publication_visibility_timeout(self):
        now = [0]
        def absent(url, limit):
            raise NotPublished("not visible yet")
        def sleep(seconds):
            now[0] += seconds
        with self.assertRaisesRegex(AssetError, "timed out"):
            self.crates.registry(absent).wait_until_visible(
                self.asset, clock=lambda: now[0], sleep=sleep, timeout=30)
        self.assertEqual(now[0], 30)

    def test_access_denial_and_mismatch_are_not_retried(self):
        for reason in ("HTTP 403", "digest mismatch"):
            calls = []
            def fail(url, limit):
                calls.append(url)
                raise AssetError(reason)
            with self.assertRaises(AssetError):
                self.crates.registry(fail).wait_until_visible(
                    self.asset, sleep=lambda _: self.fail("must not retry"))
            self.assertEqual(len(calls), 1)

    def test_index_path_matches_cargo_layout(self):
        for name, path in (("a", "1/a"), ("ab", "2/ab"), ("abc", "3/a/abc"), ("Zakura-Assets", "za/ku/zakura-assets")):
            self.assertEqual(registry.index_path(name), path)

    def test_cargo_must_resolve_the_matching_registry_grid(self):
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch)
            (root / "src").mkdir()
            (root / "src/mainnet-frontier-grid.bin").write_bytes(self.crates.grid)
            package = dict(name=NAME, version=VERSION, manifest_path=str(root / "Cargo.toml"),
                           source=registry.CRATES_IO_SOURCE)
            result = subprocess.CompletedProcess([], 0, json.dumps({"packages": [package]}), "")
            with patch.object(registry.subprocess, "run", return_value=result):
                Registry().resolve(self.asset)
                (root / "src/mainnet-frontier-grid.bin").write_bytes(b"different")
                with self.assertRaisesRegex(AssetError, "different frontier grid"):
                    Registry().resolve(self.asset)


class PinTests(unittest.TestCase):
    def setUp(self):
        self.crates = FakeCratesIo()
        self.asset = {**self.crates.asset, "checksum": self.crates.checksum}
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name)
        self.manifest = self.root / "Cargo.toml"
        self.manifest.write_text('[workspace.dependencies]\nzakura-assets = { version = "=0.3513777.0" }\n')
        self.lock = self.root / "Cargo.lock"
        self.lock.write_text('version = 4\n[[package]]\nname = "zakura-assets"\nversion = "0.3513777.0"\n'
                             f'source = "{registry.CRATES_IO_SOURCE}"\n'
                             f'checksum = "{self.crates.checksum}"\n'
                             '[[package]]\nname = "unrelated"\nversion = "1.0.0"\n')

    def update(self, *args, **kwargs):
        self.assertEqual(args[0], ["cargo", "update", "-p", NAME, "--precise", VERSION])
        self.lock.write_text(self.lock.read_text().replace('"0.3513777.0"', f'"{VERSION}"'))

    def test_pin_moves_manifest_and_lock_together(self):
        with patch.object(registry.subprocess, "run", side_effect=self.update):
            Registry().pin(self.root, self.asset)
        self.assertIn(f'version = "={VERSION}"', self.manifest.read_text())
        self.assertIn(f'version = "{VERSION}"', self.lock.read_text())

    def test_unrelated_lockfile_changes_fail(self):
        def update(*args, **kwargs):
            self.update(*args, **kwargs)
            self.lock.write_text(self.lock.read_text().replace('"1.0.0"', '"2.0.0"'))
        with patch.object(registry.subprocess, "run", side_effect=update):
            with self.assertRaisesRegex(AssetError, "unrelated"):
                Registry().pin(self.root, self.asset)

    def test_resolution_failure_is_propagated(self):
        with patch.object(registry.subprocess, "run", side_effect=subprocess.CalledProcessError(1, "cargo")):
            with self.assertRaises(subprocess.CalledProcessError):
                Registry().pin(self.root, self.asset)

    def test_locked_checksum_must_match_the_ready_asset(self):
        with patch.object(registry.subprocess, "run", side_effect=self.update):
            with self.assertRaisesRegex(AssetError, "lockfile does not select"):
                Registry().pin(self.root, {**self.asset, "checksum": "0" * 64})

    def test_manifest_must_carry_exactly_one_pin(self):
        self.manifest.write_text("[workspace.dependencies]\n")
        with self.assertRaisesRegex(AssetError, "exactly one"):
            Registry().pin(self.root, self.asset)


if __name__ == "__main__":
    unittest.main()
