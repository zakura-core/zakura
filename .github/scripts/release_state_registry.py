"""Verify, publish, and pin the Cargo package that carries the frontier grid."""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import re
import subprocess
import tarfile
import tempfile
import time
import tomllib
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

GRID_PATH = "src/mainnet-frontier-grid.bin"
GRID_LIMIT = 32 * 1024 * 1024
ARCHIVE_LIMIT = 40 * 1024 * 1024
CRATES_IO_SOURCE = "registry+https://github.com/rust-lang/crates.io-index"
SHA256 = re.compile(r"[0-9a-f]{64}")
PIN = re.compile(r'^(zakura-assets = \{[^}]*version = ")=[^"]+("[^}]*\})$', re.MULTILINE)


class AssetError(RuntimeError):
    """The candidate cannot safely be published or selected."""


class NotPublished(AssetError):
    """The registry reports an object absent, which may only mean it has not propagated yet."""


class _RejectRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise AssetError("unexpected registry redirect")


def download(url: str, limit: int) -> bytes:
    """Read at most `limit` bytes; a 404 raises NotPublished and any other failure AssetError."""
    request = urllib.request.Request(url, headers={"User-Agent": "zakura-release-automation"})
    try:
        with urllib.request.build_opener(_RejectRedirects).open(request, timeout=30) as response:
            data = response.read(limit + 1)
    except urllib.error.HTTPError as error:
        error.close()
        if error.code == 404:
            raise NotPublished(f"registry object is not published: {url}") from error
        raise AssetError(f"registry request failed with HTTP {error.code}") from error
    if len(data) > limit:
        raise AssetError("registry response exceeds its size limit")
    return data


def check_identity(identity: dict) -> None:
    height = identity["height"]
    if type(height) is not int or not 0 < height < 2**32:
        raise AssetError("checkpoint height must be a positive u32")
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", identity["name"]):
        raise AssetError("invalid asset package name")
    if identity["version"] != f"0.{height}.0":
        raise AssetError("asset version does not identify the selected checkpoint")
    for key in ("meta_sha256", "grid_sha256"):
        if not SHA256.fullmatch(identity[key]):
            raise AssetError(f"invalid {key}")


def grid_digest(archive: bytes, member: str) -> str:
    """Hash the single bounded, regular grid file in a `.crate` archive."""
    # Bound decompression before tarfile sees the data, since tarfile allocates extended headers.
    with gzip.GzipFile(fileobj=io.BytesIO(archive)) as compressed:
        expanded = compressed.read(ARCHIVE_LIMIT + 1)
    if len(expanded) > ARCHIVE_LIMIT:
        raise AssetError("expanded asset archive exceeds its size limit")
    with tarfile.open(fileobj=io.BytesIO(expanded), mode="r:") as package:
        grids = [entry for entry in package if entry.name == member]
        # Sparse tar members can expand beyond the decompressed archive's size.
        if len(grids) != 1 or not grids[0].isfile() or grids[0].size > GRID_LIMIT:
            raise AssetError("published archive must contain exactly one bounded, regular frontier grid")
        with package.extractfile(grids[0]) as grid:
            return hashlib.sha256(grid.read(GRID_LIMIT + 1)).hexdigest()


def index_path(name: str) -> str:
    """Return the sparse-index path Cargo uses for a package name."""
    name = name.lower()
    if len(name) <= 2:
        return f"{len(name)}/{name}"
    if len(name) == 3:
        return f"3/{name[0]}/{name}"
    return f"{name[:2]}/{name[2:4]}/{name}"


@dataclass
class Registry:
    """A Cargo registry: crates.io in production, a loopback fixture in tests."""

    api: str = "https://crates.io/api/v1/crates"
    downloads: str = "https://static.crates.io/crates"
    index: str = "https://index.crates.io"
    # The `source` Cargo records in Cargo.lock for packages from this registry.
    source: str = CRATES_IO_SOURCE
    # Cargo's name for an alternative registry; None selects crates.io.
    cargo_name: str | None = None
    cargo_env: dict | None = None
    fetch: Callable[[str, int], bytes] = download
    # Seconds between checks while a new version propagates through the registry.
    poll_interval: float = 15

    def archive_checksum(self, asset: dict) -> str:
        """Return the checksum of the unyanked published archive whose grid matches `asset`."""
        check_identity(asset)
        return self._verify_archive(asset, self._version(asset))

    def publication_needed(self, asset: dict) -> bool:
        """Only an API 404 permits upload; an existing version must match even if its archive is missing."""
        check_identity(asset)
        try:
            record = self._version(asset)
        except NotPublished:
            return True
        self._verify_archive(asset, record)
        return False

    def wait_until_visible(self, asset: dict, *, timeout: float = 300,
                           clock=time.monotonic, sleep=time.sleep) -> str:
        """Retry only while the registry reports the version absent, then require Cargo to resolve it."""
        deadline = clock() + timeout
        while True:
            try:
                checksum = self.archive_checksum(asset)
                # Cargo reads the index, not the API, so the index must agree before Cargo resolves.
                entry = self._index_entry(asset)
                if entry["cksum"] != checksum or entry["yanked"] is not False:
                    raise AssetError("Cargo index disagrees with the expected asset package")
                self.resolve(asset)
                return checksum
            except NotPublished as error:
                if clock() >= deadline:
                    raise AssetError("timed out waiting for asset publication") from error
                sleep(min(self.poll_interval, deadline - clock()))

    def resolve(self, asset: dict) -> None:
        """Require Cargo, outside the workspace, to resolve exactly `asset` and its grid."""
        with tempfile.TemporaryDirectory(prefix="zakura-asset-resolution-") as scratch:
            root = Path(scratch)
            (root / "src").mkdir()
            (root / "src/lib.rs").write_text("")
            registry = f', registry = "{self.cargo_name}"' if self.cargo_name else ""
            (root / "Cargo.toml").write_text(
                '[package]\nname = "zakura-asset-probe"\nversion = "0.0.0"\nedition = "2021"\n'
                f'[dependencies]\n{asset["name"]} = {{ version = "={asset["version"]}"{registry} }}\n'
            )
            result = self._cargo("metadata", "--format-version", "1", "--manifest-path", str(root / "Cargo.toml"),
                                 cwd=None, timeout=120, capture_output=True, text=True)
        packages = [p for p in json.loads(result.stdout)["packages"]
                    if p["name"] == asset["name"] and p["version"] == asset["version"]]
        if len(packages) != 1 or packages[0].get("source") != self.source:
            raise AssetError("Cargo did not resolve the expected registry package")
        with (Path(packages[0]["manifest_path"]).parent / GRID_PATH).open("rb") as grid:
            if hashlib.file_digest(grid, "sha256").hexdigest() != asset["grid_sha256"]:
                raise AssetError("Cargo resolved a different frontier grid")

    def publish(self, root: Path) -> None:
        registry = ["--registry", self.cargo_name] if self.cargo_name else []
        self._cargo("publish", "--manifest-path", "crates/zakura-assets/Cargo.toml", "--allow-dirty",
                    "--no-verify", *registry, cwd=root, timeout=600)

    def pin(self, root: Path, asset: dict) -> None:
        """Move the exact workspace pin to `asset`, changing no other Cargo.lock entry."""
        manifest, lock = root / "Cargo.toml", root / "Cargo.lock"
        updated, count = PIN.subn(rf'\g<1>={asset["version"]}\g<2>', manifest.read_text(encoding="utf-8"))
        if count != 1:
            raise AssetError("Cargo.toml must carry exactly one exact zakura-assets pin")
        before = tomllib.loads(lock.read_text(encoding="utf-8"))
        manifest.write_text(updated, encoding="utf-8")
        self._cargo("update", "-p", asset["name"], "--precise", asset["version"], cwd=root, timeout=300)
        after = tomllib.loads(lock.read_text(encoding="utf-8"))

        def without_asset(lockfile):
            return {**lockfile, "package": [p for p in lockfile["package"] if p["name"] != asset["name"]]}

        if without_asset(before) != without_asset(after):
            raise AssetError("asset pin update changed unrelated locked dependencies")
        # Cargo verifies every download against this checksum, which the ready set bound to the grid.
        pinned = [(p["version"], p.get("source"), p.get("checksum")) for p in after["package"]
                  if p["name"] == asset["name"]]
        if pinned != [(asset["version"], self.source, asset["checksum"])]:
            raise AssetError("lockfile does not select exactly the verified registry asset")

    def _version(self, asset: dict) -> dict:
        name, version = asset["name"], asset["version"]
        record = json.loads(self.fetch(f"{self.api}/{name}/{version}", 1024 * 1024))["version"]
        if (record["crate"] != name or record["num"] != version or record["yanked"] is not False
                or not isinstance(record["checksum"], str) or not SHA256.fullmatch(record["checksum"])):
            raise AssetError("registry metadata does not identify the expected unyanked asset version")
        return record

    def _verify_archive(self, asset: dict, record: dict) -> str:
        name, version = asset["name"], asset["version"]
        archive = self.fetch(f"{self.downloads}/{name}/{name}-{version}.crate", ARCHIVE_LIMIT)
        if hashlib.sha256(archive).hexdigest() != record["checksum"]:
            raise AssetError("published archive checksum differs from registry metadata")
        if grid_digest(archive, f"{name}-{version}/{GRID_PATH}") != asset["grid_sha256"]:
            raise AssetError("published frontier grid differs from the verified bundle")
        return record["checksum"]

    def _index_entry(self, asset: dict) -> dict:
        records = self.fetch(f"{self.index}/{index_path(asset['name'])}", 16 * 1024 * 1024)
        for line in records.splitlines():
            entry = json.loads(line)
            if entry["vers"] == asset["version"]:
                return entry
        raise NotPublished("asset version has not propagated to the Cargo index")

    def _cargo(self, *args, cwd, timeout, **kwargs):
        return subprocess.run(["cargo", *args], cwd=cwd, check=True, timeout=timeout, env=self.cargo_env, **kwargs)
