"""Publish and consume complete, immutable Mainnet release-state sets."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import tempfile
import tomllib
from datetime import datetime, timedelta, timezone
from pathlib import Path

from release_state_assets import AssetError, check_identity, registry_grid, verify_visible

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("ready_bundle_fetch", ROOT / ".github/scripts/fetch-release-state.py")
FETCH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FETCH)
RESOLUTION_KEYS = ("height", "block_hash", "generated_at", "meta_url", "meta_sha256")
MAX_AGE = timedelta(hours=48)
PUBLIC_BASE = "https://zakura-release.valargroup.dev/release-state/published"
STORAGE_PREFIX = "release-state/published"
R2_ENDPOINT = "https://152e2a8834283136c2f0575782b1b7aa.r2.cloudflarestorage.com"
R2_BUCKET = "zakura-release-state"
POINTER_KEYS = FETCH.LATEST_REQUIRED_KEYS | {"asset"}
ASSET_KEYS = {"name", "version", "checksum", "grid_sha256"}


def fresh(generated_at: str, now: datetime | None = None) -> None:
    generated = FETCH._timestamp(generated_at, "generated_at")
    now = now or datetime.now(timezone.utc)
    if generated > now + FETCH.FUTURE_SKEW:
        raise AssetError("release-state generation time is unexpectedly in the future")
    if now - generated > MAX_AGE:
        raise AssetError("release-state set is older than 48 hours; publish a fresh export")


def parse_ready(data: bytes, url: str, *, now=None, check_age=True) -> dict:
    FETCH._validate_url(url, "ready pointer")
    if not url.endswith("/published/latest.json"):
        raise AssetError("ready pointer must use the separate published prefix")
    value = FETCH._parse_json(data, "ready manifest")
    FETCH._check_keys(value, POINTER_KEYS, set(), "ready manifest")
    if type(value["schema_version"]) is not int or value["schema_version"] != 2 or value["network"] != "Mainnet":
        raise AssetError("expected a Mainnet ready manifest with schema version 2")
    height = FETCH._integer(value["height"], "height")
    FETCH._hex_digest(value["block_hash"], "block_hash")
    FETCH._hex_digest(value["meta_sha256"], "meta_sha256")
    asset = FETCH._object(value["asset"], "asset")
    FETCH._check_keys(asset, ASSET_KEYS, set(), "asset")
    FETCH._hex_digest(asset["checksum"], "asset.checksum")
    check_identity({**value, **asset})
    # Consumers select the reviewed package family, never a package named by untrusted metadata.
    expected_name = tomllib.loads((ROOT / "crates/zakura-assets/Cargo.toml").read_text())["package"]["name"]
    if asset["name"] != expected_name:
        raise AssetError("ready set names an unexpected asset package")
    if value["meta_url"] != f'{url.removesuffix("latest.json")}v1/{height}/meta.json':
        raise AssetError("ready metadata is outside its immutable published namespace")
    FETCH._validate_url(value["meta_url"], "ready metadata")
    FETCH._timestamp(value["generated_at"], "generated_at")
    if check_age:
        fresh(value["generated_at"], now)
    return value


def validate_checkpoint_identity(bundle: Path, resolution: dict) -> None:
    """Require the checkpoint file to end at the selected height and block hash."""
    checkpoints = (bundle / "main-checkpoints.txt").read_bytes()
    expected = f'{resolution["height"]} {resolution["block_hash"]}'.encode("ascii")
    if not checkpoints.endswith(b"\n") or checkpoints.splitlines()[-1] != expected:
        raise AssetError("bundle terminal checkpoint does not match the selected height and block hash")


def resolve_ready(url: str, output: Path, *, fetch=FETCH._download, registry_fetch=None,
                  cargo_resolve=None, now=None) -> dict:
    """Resolve once, verify all bytes and Cargo, then expose a complete local snapshot."""
    manifest_bytes = fetch(url, FETCH.LATEST_MAX_BYTES)
    manifest = parse_ready(manifest_bytes, url, now=now)
    if output.exists():
        raise AssetError("ready-state output already exists")
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output.parent, prefix=".ready-") as scratch:
        scratch = Path(scratch)
        resolution = FETCH.resolve_pinned_bundle(manifest["meta_url"], manifest["meta_sha256"],
                                                scratch / "bundle", scratch / "resolution.json", fetch=fetch)
        for key in ("height", "block_hash", "generated_at"):
            if resolution[key] != manifest[key]:
                raise AssetError("ready manifest and bundle metadata disagree")
        validate_checkpoint_identity(scratch / "bundle", resolution)
        asset_identity = {**resolution, **manifest["asset"]}
        kwargs = {}
        if registry_fetch is not None:
            kwargs["fetch"] = registry_fetch
        checksum = registry_grid(asset_identity, **kwargs)
        if checksum != manifest["asset"]["checksum"]:
            raise AssetError("ready manifest and registry archive checksums disagree")
        if cargo_resolve is None:
            from release_state_assets import cargo_grid
            cargo_resolve = cargo_grid
        cargo_resolve(asset_identity)
        grid = (scratch / "bundle/mainnet-frontier-grid.bin").read_bytes()
        if hashlib.sha256(grid).hexdigest() != manifest["asset"]["grid_sha256"]:
            raise AssetError("ready grid differs from the published crate")
        fresh(manifest["generated_at"], now)
        (scratch / "ready.json").write_bytes(manifest_bytes)
        scratch.rename(output)
    return manifest


def verify_snapshot(state: Path, *, now=None) -> tuple[dict, dict]:
    """Revalidate the preserved local files without following a moving pointer."""
    selected = json.loads((state / "selection.json").read_text())
    fresh(selected["generated_at"], now)
    meta_bytes = (state / "bundle/meta.json").read_bytes()
    if len(meta_bytes) > FETCH.META_MAX_BYTES or hashlib.sha256(meta_bytes).hexdigest() != selected["meta_sha256"]:
        raise AssetError("preserved bundle metadata differs from its selection")
    meta = FETCH._parse_json(meta_bytes, "preserved metadata")
    # The existing bundle parser supplies all framing, URL, size, and digest checks.
    def local_fetch(url, limit):
        if url == selected["meta_url"]:
            return meta_bytes
        prefix = selected["meta_url"].removesuffix("meta.json")
        if not url.startswith(prefix) or url[len(prefix):] not in FETCH.FILE_LIMITS:
            raise AssetError("unexpected file in preserved bundle")
        path = state / "bundle" / url[len(prefix):]
        if path.stat().st_size > limit:
            raise AssetError("preserved bundle file exceeds its bound")
        return path.read_bytes()
    with tempfile.TemporaryDirectory(prefix="verify-release-state-") as scratch:
        scratch = Path(scratch)
        resolution = FETCH.resolve_pinned_bundle(selected["meta_url"], selected["meta_sha256"],
                                                scratch / "bundle", scratch / "resolution.json", fetch=local_fetch)
    if any(selected[key] != resolution[key] for key in RESOLUTION_KEYS):
        raise AssetError("preserved resolution differs from the selected bundle")
    validate_checkpoint_identity(state / "bundle", resolution)
    if hashlib.sha256((state / "bundle/mainnet-frontier-grid.bin").read_bytes()).hexdigest() != selected["grid_sha256"]:
        raise AssetError("preserved grid differs from the selection")
    return selected, meta


def verify_ready_snapshot(state: Path, *, now=None) -> dict:
    selected, _meta = verify_snapshot(state, now=now)
    manifest_bytes = (state / "ready.json").read_bytes()
    url = selected["meta_url"].split("/v1/", 1)[0] + "/latest.json"
    manifest = parse_ready(manifest_bytes, url, now=now)
    if any(manifest[key] != selected[key] for key in RESOLUTION_KEYS):
        raise AssetError("preserved ready manifest differs from its selection")
    if any(manifest["asset"][key] != selected[key] for key in ("name", "version", "grid_sha256")):
        raise AssetError("preserved asset identity differs from the ready manifest")
    return manifest


class S3Store:
    """Bounded object operations; only confirmed absence permits creation."""
    def __init__(self, client, bucket):
        self.client, self.bucket = client, bucket

    def get(self, key: str, limit: int):
        from botocore.exceptions import ClientError
        try:
            result = self.client.get_object(Bucket=self.bucket, Key=key)
        except ClientError as error:
            if error.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None
            raise AssetError("object storage read failed") from error
        with result["Body"] as stream:
            data = stream.read(limit + 1)
        if len(data) > limit:
            raise AssetError("stored object exceeds its size limit")
        return data, result["ETag"]

    def put(self, key, data, *, etag=None, create=False, cache="public, max-age=31536000, immutable"):
        from botocore.exceptions import ClientError
        conditions = {"IfNoneMatch": "*"} if create else {"IfMatch": etag}
        try:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=data, ContentLength=len(data),
                                   ContentType="application/json" if key.endswith(".json") else "application/octet-stream",
                                   CacheControl=cache, **conditions)
            return True
        except ClientError as error:
            if error.response["Error"]["Code"] in ("PreconditionFailed", "412", "ConditionalRequestConflict", "409"):
                return False
            raise AssetError("object storage write failed") from error

    def immutable(self, key, data):
        existing = self.get(key, len(data))
        if existing is None:
            if self.put(key, data, create=True):
                return
            # Another publisher may have created the object after our read.
            existing = self.get(key, len(data))
        if existing is None or existing[0] != data:
            raise AssetError("immutable published object already has different bytes")


def production_store():
    import boto3
    from botocore.config import Config
    return S3Store(boto3.client("s3", endpoint_url=R2_ENDPOINT, region_name="auto",
                              aws_access_key_id=os.environ["RELEASE_STATE_R2_ACCESS_KEY_ID"],
                              aws_secret_access_key=os.environ["RELEASE_STATE_R2_SECRET_ACCESS_KEY"],
                              config=Config(connect_timeout=10, read_timeout=30, retries={"max_attempts": 0},
                                            request_checksum_calculation="when_required")), R2_BUCKET)


def promote(state: Path, store: S3Store, *, public_base=PUBLIC_BASE, fetch=FETCH._download,
            verify=verify_visible, registry_fetch=None, now=None) -> dict:
    selected, meta = verify_snapshot(state, now=now)
    verify(selected)
    registry_args = {} if registry_fetch is None else {"fetch": registry_fetch}
    checksum = registry_grid(selected, **registry_args)
    # A separate prefix means candidate cleanup can never delete an advertised set.
    prefix = STORAGE_PREFIX + f'/v1/{selected["height"]}'
    for name in meta["files"]:
        store.immutable(f"{prefix}/{name}", (state / "bundle" / name).read_bytes())
    store.immutable(f"{prefix}/meta.json", (state / "bundle/meta.json").read_bytes())
    published_meta = f'{public_base}/v1/{selected["height"]}/meta.json'
    with tempfile.TemporaryDirectory(prefix="public-release-state-") as scratch:
        scratch = Path(scratch)
        FETCH.resolve_pinned_bundle(published_meta, selected["meta_sha256"], scratch / "bundle",
                                   scratch / "resolution.json", fetch=fetch)
    manifest = {key: selected[key] for key in FETCH.LATEST_REQUIRED_KEYS - {"schema_version", "network"}}
    manifest.update(schema_version=2, network="Mainnet", meta_url=published_meta,
                    asset={"name": selected["name"], "version": selected["version"],
                           "checksum": checksum, "grid_sha256": selected["grid_sha256"]})
    pointer_url = public_base + "/latest.json"
    encoded = (json.dumps(manifest, sort_keys=True, indent=2) + "\n").encode()
    parse_ready(encoded, pointer_url, now=now)
    pointer_key = STORAGE_PREFIX + "/latest.json"
    def verify_public_pointer():
        visible = fetch(pointer_url, FETCH.LATEST_MAX_BYTES)
        if parse_ready(visible, pointer_url, now=now) != manifest:
            raise AssetError("public ready pointer did not expose the committed identity; check cache configuration")
        return manifest

    for _ in range(3):
        current = store.get(pointer_key, FETCH.LATEST_MAX_BYTES)
        if current:
            old = parse_ready(current[0], pointer_url, check_age=False)
            if old["height"] > manifest["height"]:
                raise AssetError("a newer ready set already exists; refusing pointer regression")
            if old["height"] == manifest["height"]:
                if old != manifest:
                    raise AssetError("same-height ready set has a conflicting identity")
                return verify_public_pointer()
        fresh(manifest["generated_at"], now)
        if store.put(pointer_key, encoded, etag=current[1] if current else None,
                     create=current is None, cache="no-store, max-age=0"):
            return verify_public_pointer()
    raise AssetError("ready pointer changed concurrently; retry publication")
