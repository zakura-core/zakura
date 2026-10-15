"""Shared release-state asset identity, registry verification, and pinning."""

from __future__ import annotations

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
from pathlib import Path

GRID_LIMIT = 32 * 1024 * 1024
ARCHIVE_LIMIT = GRID_LIMIT + 8 * 1024 * 1024
IDENTITY_KEYS = ("meta_url", "meta_sha256", "height", "name", "version", "grid_sha256")
PIN = re.compile(r'^(zakura-assets = \{[^}]*version = ")=[^"]+("[^}]*\})$', re.MULTILINE)


class AssetError(RuntimeError):
    """The candidate cannot safely be published or selected."""


class NotPublished(AssetError):
    """The registry explicitly reports an absent object."""


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise AssetError("unexpected registry redirect")


def download(url: str, limit: int, *, timeout: float = 30) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "zakura-release-automation"})
    try:
        with urllib.request.build_opener(NoRedirect).open(request, timeout=timeout) as response:
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
        if not re.fullmatch(r"[0-9a-f]{64}", identity[key]):
            raise AssetError(f"invalid {key}")


def registry_grid(identity: dict, fetch=download) -> str:
    """Require an unyanked version and verify both its archive checksum and grid."""
    check_identity(identity)
    name, version = identity["name"], identity["version"]
    metadata = json.loads(fetch(f"https://crates.io/api/v1/crates/{name}/{version}", 1024 * 1024))
    record = metadata["version"]
    checksum = record["checksum"]
    if (record["crate"] != name or record["num"] != version or record["yanked"] is not False
            or not isinstance(checksum, str) or not re.fullmatch(r"[0-9a-f]{64}", checksum)):
        raise AssetError("registry metadata does not identify the expected unyanked asset version")
    archive = fetch(f"https://static.crates.io/crates/{name}/{name}-{version}.crate", ARCHIVE_LIMIT)
    if hashlib.sha256(archive).hexdigest() != checksum:
        raise AssetError("published archive checksum differs from registry metadata")
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r|gz") as package:
        count, total, digest = 0, 0, None
        for entry in package:
            count += 1
            total += entry.size
            if count > 128 or total > ARCHIVE_LIMIT:
                raise AssetError("expanded asset archive exceeds its size limit")
            if entry.name == f"{name}-{version}/src/mainnet-frontier-grid.bin":
                if digest is not None or not entry.isfile() or entry.size > GRID_LIMIT:
                    raise AssetError("published archive must contain one bounded, regular frontier grid")
                with package.extractfile(entry) as grid:
                    digest = hashlib.sha256(grid.read(GRID_LIMIT + 1)).hexdigest()
    if digest != identity["grid_sha256"]:
        raise AssetError("published frontier grid differs from the verified bundle")
    return checksum


def publication_needed(identity: dict, fetch=download) -> bool:
    """Only an API 404 permits publication; an inaccessible archive never does."""
    # Fetch the API separately so a missing archive for an existing version is not mistaken
    # for permission to upload that immutable version again.
    name, version = identity["name"], identity["version"]
    api = f"https://crates.io/api/v1/crates/{name}/{version}"
    check_identity(identity)
    try:
        metadata = fetch(api, 1024 * 1024)
    except NotPublished:
        return True
    registry_grid(identity, lambda url, limit: metadata if url == api else fetch(url, limit))
    return False


def cargo_grid(identity: dict, *, timeout: float = 120) -> None:
    """Resolve the asset in isolation, without touching the workspace lockfile."""
    with tempfile.TemporaryDirectory(prefix="zakura-asset-resolution-") as scratch:
        root = Path(scratch)
        (root / "src").mkdir()
        (root / "src/lib.rs").write_text("")
        (root / "Cargo.toml").write_text(
            '[package]\nname = "zakura-asset-probe"\nversion = "0.0.0"\nedition = "2021"\n'
            f'[dependencies]\n{identity["name"]} = "={identity["version"]}"\n'
        )
        result = subprocess.run(
            ["cargo", "metadata", "--format-version", "1", "--manifest-path", str(root / "Cargo.toml")],
            capture_output=True, text=True, check=True, timeout=timeout,
        )
        packages = [p for p in json.loads(result.stdout)["packages"]
                    if p["name"] == identity["name"] and p["version"] == identity["version"]]
        if len(packages) != 1 or packages[0].get("source") != "registry+https://github.com/rust-lang/crates.io-index":
            raise AssetError("Cargo did not resolve the expected crates.io package")
        grid = Path(packages[0]["manifest_path"]).parent / "src/mainnet-frontier-grid.bin"
        with grid.open("rb") as handle:
            digest = hashlib.file_digest(handle, "sha256").hexdigest()
        if digest != identity["grid_sha256"]:
            raise AssetError("Cargo resolved a different frontier grid")


def verify_visible(identity: dict, *, fetch=download, resolve=cargo_grid,
                   clock=time.monotonic, sleep=time.sleep, timeout=300) -> None:
    """Wait only for confirmed propagation delays; denials and mismatches fail immediately."""
    deadline = clock() + timeout
    def bounded_fetch(url, limit):
        remaining = deadline - clock()
        if remaining <= 0:
            raise AssetError("timed out waiting for asset publication")
        return download(url, limit, timeout=min(30, remaining)) if fetch is download else fetch(url, limit)
    while True:
        try:
            checksum = registry_grid(identity, bounded_fetch)
            name = identity["name"]
            # Cargo consumes the index, not the API. Check it before asking Cargo to resolve.
            path = f"{name[:2]}/{name[2:4]}/{name}" if len(name) >= 4 else (
                f"3/{name[0]}/{name}" if len(name) == 3 else f"{len(name)}/{name}"
            )
            records = bounded_fetch(f"https://index.crates.io/{path}", 16 * 1024 * 1024)
            versions = [json.loads(line) for line in records.splitlines()]
            entry = next((v for v in versions if v["vers"] == identity["version"]), None)
            if entry is None:
                raise NotPublished("asset version has not propagated to the Cargo index")
            if entry["cksum"] != checksum or entry["yanked"] is not False:
                raise AssetError("Cargo index disagrees with the expected asset package")
            remaining = deadline - clock()
            if remaining <= 0:
                raise AssetError("timed out waiting for asset publication")
            resolve(identity, timeout=min(remaining, 120))
            return
        except NotPublished as error:
            remaining = deadline - clock()
            if remaining <= 0:
                raise AssetError("timed out waiting for asset publication") from error
            sleep(min(15, remaining))


def validate_receipt(receipt: dict, selected: dict, request_id: str, run: dict) -> None:
    expected = {key: selected[key] for key in IDENTITY_KEYS}
    expected.update(schema_version=1, request_id=request_id,
                    producer_run_id=run["id"], producer_sha=run["head_sha"],
                    producer_attempt=run["run_attempt"])
    if any(receipt.get(key) != value for key, value in expected.items()):
        raise AssetError("publication receipt does not match the selected bundle or producer run")
    if receipt.get("outcome") not in ("published", "reused"):
        raise AssetError("publication receipt has no successful outcome")
    check_identity(receipt)


def pin_assets(root: Path, imported: dict, *, receipt: dict | None = None,
               selected: dict | None = None, request_id: str | None = None, fetch=download) -> None:
    if not imported["has_changes"]:
        return
    manifest = root / "Cargo.toml"
    text = manifest.read_text(encoding="utf-8")
    dependency = tomllib.loads(text)["workspace"]["dependencies"]["zakura-assets"]
    identity = {**imported, "name": dependency.get("package", "zakura-assets"),
                "version": f'0.{imported["height"]}.0',
                "grid_sha256": imported["frontier_grid_sha256"]}
    check_identity(identity)
    if receipt is not None:
        if (selected is None or not request_id or receipt.get("request_id") != request_id
                or receipt.get("schema_version") != 1 or receipt.get("outcome") not in ("published", "reused")
                or any(receipt.get(k) != identity[k] or selected.get(k) != identity[k]
                       for k in IDENTITY_KEYS)):
            raise AssetError("receipt, selection, and imported state do not agree")
    try:
        checksum = registry_grid(identity, fetch)
    except NotPublished as error:
        raise AssetError("matching asset is unavailable; publication through Update Mainnet release state is required") from error
    updated, count = PIN.subn(lambda match: f'{match[1]}={identity["version"]}{match[2]}', text)
    if count != 1:
        raise AssetError("Cargo.toml must carry exactly one exact zakura-assets pin")
    lock = root / "Cargo.lock"
    before = tomllib.loads(lock.read_text(encoding="utf-8"))
    manifest.write_text(updated, encoding="utf-8")
    subprocess.run(["cargo", "update", "-p", identity["name"], "--precise", identity["version"]],
                   cwd=root, check=True, timeout=300)
    after = tomllib.loads(lock.read_text(encoding="utf-8"))
    def unrelated(value):
        return {**value, "package": [p for p in value["package"] if p["name"] != identity["name"]]}
    if unrelated(before) != unrelated(after):
        raise AssetError("asset pin update changed unrelated locked dependencies")
    matches = [p for p in after["package"] if p["name"] == identity["name"]]
    if (len(matches) != 1 or matches[0]["version"] != identity["version"]
            or matches[0].get("source") != "registry+https://github.com/rust-lang/crates.io-index"
            or matches[0].get("checksum") != checksum):
        raise AssetError("lockfile does not select exactly the verified crates.io asset")
    cargo_grid(identity)
