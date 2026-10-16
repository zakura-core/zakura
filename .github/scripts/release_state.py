#!/usr/bin/env python3
"""Publish and consume complete, immutable Mainnet release-state sets.

A set is one verified bundle plus the crates.io package that carries its frontier grid.
update-release-state.yml validates a candidate bundle, publishes its package, copies the bundle
under an immutable prefix, and only then advances the ready pointer. adopt-release-state.yml and
prepare-release-pr.yml read that pointer and recheck every byte before importing.

Every subcommand works on one state directory:

  bundle/          the bundle files and their meta.json
  resolution.json  the bundle identity written by fetch-release-state.py
  selection.json   the resolution plus the asset identity: name, version, grid_sha256
  ready.json       consumers only: the ready pointer bytes that selected this set
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from release_state_registry import AssetError, Registry, check_identity

ROOT = Path(__file__).resolve().parents[2]


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


FETCH = _load("fetch_release_state", ROOT / ".github/scripts/fetch-release-state.py")
IMPORT = _load("import_release_state", ROOT / ".github/scripts/import-release-state.py")
PACK = _load("pack_assets", ROOT / "scripts/pack-assets-crate.py")

RESOLUTION_KEYS = ("height", "block_hash", "generated_at", "meta_url", "meta_sha256")
ASSET_KEYS = ("name", "version", "grid_sha256")
MAX_AGE = timedelta(hours=48)
PUBLIC_BASE = "https://zakura-release.valargroup.dev/release-state/published"
READY_URL = PUBLIC_BASE + "/latest.json"
# A separate prefix means candidate cleanup can never delete an advertised set.
STORAGE_PREFIX = "release-state/published"
POINTER_KEY = STORAGE_PREFIX + "/latest.json"
R2_ENDPOINT = "https://152e2a8834283136c2f0575782b1b7aa.r2.cloudflarestorage.com"
R2_BUCKET = "zakura-release-state"
IMMUTABLE = "public, max-age=31536000, immutable"
UNCACHED = "no-store, max-age=0"


# Selection and freshness


def select(root: Path, bundle: Path, resolution: dict) -> dict:
    """Bind a bundle to its grid package and compare its height with committed state."""
    grid = (bundle / PACK.GRID_NAME).read_bytes()
    height, _entries, _payload = PACK.parse_grid(grid, str(bundle))
    if height != resolution["height"]:
        raise AssetError("grid and selected bundle have different checkpoint heights")
    committed = IMPORT._checkpoint_height((root / IMPORT.CHECKPOINTS).read_bytes(), "committed list")
    selected = {**resolution, "has_changes": height > committed,
                "name": PACK.crate_name(root / PACK.CRATE_DIR),
                "version": f"0.{height}.0", "grid_sha256": hashlib.sha256(grid).hexdigest()}
    check_identity(selected)
    return selected


def fresh(generated_at: str, now: datetime | None = None) -> None:
    generated = FETCH._timestamp(generated_at, "generated_at")
    now = now or datetime.now(timezone.utc)
    if generated > now + FETCH.FUTURE_SKEW:
        raise AssetError("release-state generation time is unexpectedly in the future")
    if now - generated > MAX_AGE:
        raise AssetError("release-state set is older than 48 hours; publish a fresh export")


def check_terminal_checkpoint(bundle: Path, resolution: dict) -> None:
    checkpoints = (bundle / IMPORT.CHECKPOINTS.name).read_bytes()
    expected = f'{resolution["height"]} {resolution["block_hash"]}'.encode("ascii")
    if not checkpoints.endswith(b"\n") or checkpoints.splitlines()[-1] != expected:
        raise AssetError("bundle terminal checkpoint does not match the selected height and block hash")


def pinned_grid(root: Path, name: str) -> Path:
    """Locate the grid of the one package the workspace currently pins."""
    result = subprocess.run(["cargo", "metadata", "--format-version", "1", "--locked"],
                            cwd=root, check=True, capture_output=True, text=True, timeout=300)
    packages = [p for p in json.loads(result.stdout)["packages"] if p["name"] == name]
    if len(packages) != 1:
        raise AssetError("expected exactly one currently pinned frontier grid")
    return Path(packages[0]["manifest_path"]).parent / "src" / PACK.GRID_NAME


# Candidate validation


def validate_candidate(root: Path, bundle: Path, selected: dict) -> None:
    """Prove that a candidate extends committed state append-only and that its subtree roots hold."""
    check_identity(selected)
    with tempfile.TemporaryDirectory(prefix="zakura-validate-bundle-") as scratch:
        scratch = Path(scratch)
        # Recheck the preserved files against the immutable metadata, never against latest.json.
        resolution = FETCH.resolve_pinned_bundle(selected["meta_url"], selected["meta_sha256"],
                                                 scratch / "verified", scratch / "resolution.json")
        if select(root, scratch / "verified", resolution) != selected:
            raise AssetError("preserved selection differs from the immutable bundle")
        for name in FETCH.FILE_LIMITS:
            if (bundle / name).read_bytes() != (scratch / "verified" / name).read_bytes():
                raise AssetError(f"preserved bundle file differs from verified metadata: {name}")
        check_terminal_checkpoint(bundle, resolution)
        subprocess.run(["bash", str(ROOT / ".github/scripts/validate-checkpoints.sh"),
                        str(bundle / IMPORT.CHECKPOINTS.name)], check=True, timeout=30)
        _check_extends_committed(root, bundle, selected, scratch)
    subprocess.run(
        ["cargo", "run", "--release", "--locked", "-p", "zakura", "--bin", "zakurad", "--",
         "verify-historical-treestates", "--network", "Mainnet",
         "--subtree-input", str(bundle / IMPORT.SUBTREE_BUNDLE_NAME),
         "--frontier-input", str(bundle / IMPORT.FRONTIER.name),
         "--frontier-grid-input", str(bundle / PACK.GRID_NAME)],
        cwd=root, check=True, timeout=3600,
    )


def _check_extends_committed(root: Path, bundle: Path, selected: dict, scratch: Path) -> None:
    old_grid = pinned_grid(root, selected["name"])
    committed_height = IMPORT._checkpoint_height((root / IMPORT.CHECKPOINTS).read_bytes(), "committed list")
    if selected["height"] < committed_height:
        raise AssetError("committed state advanced beyond this candidate; resolve a new bundle")
    if selected["height"] == committed_height:
        pairs = ((root / IMPORT.CHECKPOINTS, IMPORT.CHECKPOINTS.name),
                 (root / IMPORT.FRONTIER, IMPORT.FRONTIER.name),
                 (root / IMPORT.SUBTREES, IMPORT.SUBTREE_BUNDLE_NAME),
                 (old_grid, PACK.GRID_NAME))
        if any(committed.read_bytes() != (bundle / name).read_bytes() for committed, name in pairs):
            raise AssetError("same-height candidate rewrites committed release state")
        return
    # The importer enforces append-only history; run it against a scratch copy of the checkout.
    checkout = scratch / "checkout"
    for relative in (IMPORT.CHECKPOINTS, IMPORT.FRONTIER, IMPORT.SUBTREES, IMPORT.PROVENANCE):
        (checkout / relative).parent.mkdir(parents=True, exist_ok=True)
        (checkout / relative).write_bytes((root / relative).read_bytes())
    IMPORT.import_bundle(checkout, bundle, scratch / "resolution.json", floor_eos=False,
                         previous_frontier_grid=old_grid)


# Ready pointer


def parse_ready(data: bytes, url: str, *, now: datetime | None = None, check_age: bool = True) -> dict:
    validate_ready_url(url)
    value = FETCH._parse_json(data, "ready manifest")
    FETCH._check_keys(value, FETCH.LATEST_REQUIRED_KEYS | {"asset"}, set(), "ready manifest")
    if type(value["schema_version"]) is not int or value["schema_version"] != 2 or value["network"] != "Mainnet":
        raise AssetError("expected a Mainnet ready manifest with schema version 2")
    height = FETCH._integer(value["height"], "height")
    FETCH._hex_digest(value["block_hash"], "block_hash")
    FETCH._hex_digest(value["meta_sha256"], "meta_sha256")
    asset = FETCH._object(value["asset"], "asset")
    FETCH._check_keys(asset, {*ASSET_KEYS, "checksum"}, set(), "asset")
    FETCH._hex_digest(asset["checksum"], "asset.checksum")
    check_identity({**value, **asset})
    if value["meta_url"] != f'{url.removesuffix("latest.json")}v1/{height}/meta.json':
        raise AssetError("ready metadata is outside its immutable published namespace")
    FETCH._timestamp(value["generated_at"], "generated_at")
    if check_age:
        fresh(value["generated_at"], now)
    return value


def validate_ready_url(url: str) -> None:
    """Reject URLs outside the allowed HTTPS published namespace before any request."""
    FETCH._validate_url(url, "ready pointer")
    if not url.endswith("/published/latest.json"):
        raise AssetError("ready pointer must use the separate published prefix")


def fetch_ready(url: str, output: Path, *, root: Path = ROOT, fetch=FETCH._download,
                registry: Registry | None = None, now: datetime | None = None) -> dict:
    """Resolve the ready pointer once and write a complete, verified state directory to `output`."""
    registry = registry or Registry()
    validate_ready_url(url)
    manifest_bytes = fetch(url, FETCH.LATEST_MAX_BYTES)
    manifest = parse_ready(manifest_bytes, url, now=now)
    if output.exists():
        raise AssetError("ready-state output already exists")
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output.parent, prefix=".ready-") as scratch:
        scratch = Path(scratch)
        resolution = FETCH.resolve_pinned_bundle(manifest["meta_url"], manifest["meta_sha256"],
                                                 scratch / "bundle", scratch / "resolution.json", fetch=fetch)
        if any(resolution[key] != manifest[key] for key in RESOLUTION_KEYS):
            raise AssetError("ready manifest and bundle metadata disagree")
        check_terminal_checkpoint(scratch / "bundle", resolution)
        # Consumers select the reviewed package family, never a package named by the pointer.
        selected = select(root, scratch / "bundle", resolution)
        if any(selected[key] != manifest["asset"][key] for key in ASSET_KEYS):
            raise AssetError("ready asset differs from the repository's selected asset family")
        _check_registry(registry, manifest)
        (scratch / "selection.json").write_text(json.dumps(selected, indent=2) + "\n")
        (scratch / "ready.json").write_bytes(manifest_bytes)
        scratch.rename(output)
    return manifest


def _check_registry(registry: Registry, manifest: dict) -> None:
    asset = {**manifest, **manifest["asset"]}
    if registry.archive_checksum(asset) != asset["checksum"]:
        raise AssetError("ready manifest and registry archive checksums disagree")
    registry.resolve(asset)


# Preserved state


def load_state(state: Path, *, now: datetime | None = None) -> dict:
    """Recheck a preserved state directory against its own metadata and return its selection."""
    selected = json.loads((state / "selection.json").read_text())
    fresh(selected["generated_at"], now)
    with tempfile.TemporaryDirectory(prefix="verify-release-state-") as scratch:
        # Replay the bundle through the fetcher so its framing, size, and digest checks all run again.
        resolution = FETCH.resolve_pinned_bundle(
            selected["meta_url"], selected["meta_sha256"], Path(scratch) / "bundle",
            Path(scratch) / "resolution.json", fetch=_serve_local(state / "bundle", selected["meta_url"]))
    if any(selected[key] != resolution[key] for key in RESOLUTION_KEYS):
        raise AssetError("preserved resolution differs from the selected bundle")
    check_terminal_checkpoint(state / "bundle", resolution)
    if hashlib.sha256((state / "bundle" / PACK.GRID_NAME).read_bytes()).hexdigest() != selected["grid_sha256"]:
        raise AssetError("preserved grid differs from the selection")
    return selected


def load_ready_state(state: Path, *, now: datetime | None = None) -> dict:
    """Recheck a consumer's state directory and return the ready manifest that selected it."""
    selected = load_state(state, now=now)
    url = selected["meta_url"].split("/v1/", 1)[0] + "/latest.json"
    manifest = parse_ready((state / "ready.json").read_bytes(), url, now=now)
    if (any(manifest[key] != selected[key] for key in RESOLUTION_KEYS)
            or any(manifest["asset"][key] != selected[key] for key in ASSET_KEYS)):
        raise AssetError("preserved ready manifest differs from its selection")
    return manifest


def _serve_local(bundle: Path, meta_url: str):
    """Serve preserved bundle files at their immutable URLs."""
    base = meta_url.removesuffix("meta.json")

    def fetch(url: str, limit: int) -> bytes:
        name = url.removeprefix(base)
        if not url.startswith(base) or name not in ("meta.json", *FETCH.FILE_LIMITS):
            raise AssetError("unexpected file in preserved bundle")
        if (bundle / name).stat().st_size > limit:
            raise AssetError("preserved bundle file exceeds its bound")
        return (bundle / name).read_bytes()

    return fetch


# Storage and promotion


class S3Store:
    """Bounded reads and conditional writes; only a confirmed absence reads as missing."""

    def __init__(self, client, bucket):
        self.client, self.bucket = client, bucket

    def get(self, key: str, limit: int) -> tuple[bytes, str] | None:
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

    def put(self, key: str, data: bytes, *, etag: str | None = None, cache: str = IMMUTABLE) -> bool:
        """Write only if `key` still has `etag`, or is absent when `etag` is None; False if another writer won."""
        from botocore.exceptions import ClientError
        condition = {"IfMatch": etag} if etag else {"IfNoneMatch": "*"}
        content_type = "application/json" if key.endswith(".json") else "application/octet-stream"
        try:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=data, ContentLength=len(data),
                                   ContentType=content_type, CacheControl=cache, **condition)
        except ClientError as error:
            if error.response["Error"]["Code"] in ("PreconditionFailed", "412", "ConditionalRequestConflict", "409"):
                return False
            raise AssetError("object storage write failed") from error
        return True


def production_store() -> S3Store:
    import boto3
    from botocore.config import Config
    client = boto3.client("s3", endpoint_url=R2_ENDPOINT, region_name="auto",
                          aws_access_key_id=os.environ["RELEASE_STATE_R2_ACCESS_KEY_ID"],
                          aws_secret_access_key=os.environ["RELEASE_STATE_R2_SECRET_ACCESS_KEY"],
                          config=Config(connect_timeout=10, read_timeout=30, retries={"max_attempts": 0},
                                        request_checksum_calculation="when_required"))
    return S3Store(client, R2_BUCKET)


def put_immutable(store, key: str, data: bytes) -> None:
    """Create `key` with `data`, or confirm that it already holds exactly `data`."""
    existing = store.get(key, len(data))
    if existing is None:
        if store.put(key, data):
            return
        # Another publisher created the object after our read.
        existing = store.get(key, len(data))
    if existing is None or existing[0] != data:
        raise AssetError("immutable published object already has different bytes")


def promote(state: Path, store, *, registry: Registry | None = None, fetch=FETCH._download,
            visibility_timeout: float = 300, now: datetime | None = None) -> dict:
    """Publish one validated set and advance the ready pointer to it, never backwards."""
    registry = registry or Registry()
    selected = load_state(state, now=now)
    checksum = registry.wait_until_visible(selected, timeout=visibility_timeout)
    height = selected["height"]
    for name in (*FETCH.FILE_LIMITS, "meta.json"):
        put_immutable(store, f"{STORAGE_PREFIX}/v1/{height}/{name}", (state / "bundle" / name).read_bytes())
    meta_url = f"{PUBLIC_BASE}/v1/{height}/meta.json"
    with tempfile.TemporaryDirectory(prefix="public-release-state-") as scratch:
        # Read the set back through the public domain, exactly as consumers will.
        FETCH.resolve_pinned_bundle(meta_url, selected["meta_sha256"], Path(scratch) / "bundle",
                                    Path(scratch) / "resolution.json", fetch=fetch)
    manifest = {**{key: selected[key] for key in RESOLUTION_KEYS}, "meta_url": meta_url,
                "schema_version": 2, "network": "Mainnet",
                "asset": {**{key: selected[key] for key in ASSET_KEYS}, "checksum": checksum}}
    encoded = (json.dumps(manifest, sort_keys=True, indent=2) + "\n").encode()

    for _attempt in range(3):
        current = store.get(POINTER_KEY, FETCH.LATEST_MAX_BYTES)
        if current:
            old = parse_ready(current[0], READY_URL, check_age=False)
            if old["height"] > height:
                raise AssetError("a newer ready set already exists; refusing pointer regression")
            if old["height"] == height:
                if old != manifest:
                    raise AssetError("same-height ready set has a conflicting identity")
                break
        fresh(manifest["generated_at"], now)
        if store.put(POINTER_KEY, encoded, etag=current and current[1], cache=UNCACHED):
            break
    else:
        raise AssetError("ready pointer changed concurrently; retry publication")

    if parse_ready(fetch(READY_URL, FETCH.LATEST_MAX_BYTES), READY_URL, now=now) != manifest:
        raise AssetError("public ready pointer did not expose the committed identity; check cache configuration")
    return manifest


# Adoption


def pin_ready_asset(root: Path, state: Path, imported: dict, registry: Registry | None = None) -> None:
    """Pin the asset of the ready set that `imported` came from."""
    manifest = load_ready_state(state)
    if any(imported[key] != manifest[key] for key in RESOLUTION_KEYS):
        raise AssetError("imported data does not match the preserved ready set")
    if not imported["has_changes"]:
        return
    if imported["frontier_grid_sha256"] != manifest["asset"]["grid_sha256"]:
        raise AssetError("imported grid does not match the ready asset")
    (registry or Registry()).pin(root, manifest["asset"])


# Command line


def _require_canonical_main(action: str) -> None:
    if (os.environ.get("GITHUB_REPOSITORY"), os.environ.get("GITHUB_REF")) != ("zakura-core/zakura", "refs/heads/main"):
        raise AssetError(f"{action} is restricted to canonical main")


def _required_env(name: str, variable: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise AssetError(f"{variable} is not configured")
    return value


def _github_output(**values) -> None:
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        for key, value in values.items():
            print(f"{key}={value}", file=output)


def _selection(state: Path) -> dict:
    selected = json.loads((state / "selection.json").read_text())
    fresh(selected["generated_at"])
    return selected


def cmd_candidate(args):
    """Fetch the newest candidate bundle and select its asset."""
    url = _required_env("LATEST_URL", "MAINNET_RELEASE_STATE_LATEST_URL")
    args.state.mkdir(parents=True, exist_ok=True)
    resolution = FETCH.resolve_bundle(url, args.state / "bundle", args.state / "resolution.json")
    selected = select(ROOT, args.state / "bundle", resolution)
    (args.state / "selection.json").write_text(json.dumps(selected, indent=2) + "\n")


def cmd_validate(args):
    """Prove append-only history and subtree roots for the selected bundle."""
    validate_candidate(ROOT, args.state / "bundle", _selection(args.state))


def cmd_pack(args):
    """Materialise the asset crate and check that it matches the selection."""
    selected = _selection(args.state)
    packed = PACK.pack(args.state / "bundle" / PACK.GRID_NAME, ROOT / PACK.CRATE_DIR, 0)
    if (packed["name"], packed["version"], packed["sha256"]) != tuple(selected[key] for key in ASSET_KEYS):
        raise AssetError("packed asset differs from validated selection")


def cmd_check(args):
    """Record whether the asset version still needs publication."""
    needed = Registry().publication_needed(_selection(args.state))
    _github_output(needed=str(needed).lower())
    if needed and args.expect_published:
        raise AssetError("the matching crate must be published from main before this dry run can pass")


def cmd_publish(args):
    """Publish the packed asset crate if the registry still lacks it."""
    _require_canonical_main("crate publication")
    registry = Registry()
    if registry.publication_needed(_selection(args.state)):
        registry.publish(ROOT)


def cmd_visible(args):
    """Wait until Cargo resolves the selected asset."""
    Registry().wait_until_visible(_selection(args.state))


def cmd_promote(args):
    """Copy the set to immutable storage and advance the ready pointer."""
    _require_canonical_main("storage promotion")
    manifest = promote(args.state, production_store())
    (args.state / "promoted-ready.json").write_text(json.dumps(manifest, indent=2) + "\n")


def cmd_ready(args):
    """Fetch the ready set named by READY_URL."""
    fetch_ready(_required_env("READY_URL", "MAINNET_RELEASE_STATE_READY_URL"), args.state)


def cmd_pinned_grid(args):
    """Print the path of the grid the workspace currently pins."""
    print(pinned_grid(ROOT, PACK.crate_name(ROOT / PACK.CRATE_DIR)))


def cmd_pin(args):
    """Pin the ready asset after importing its bundle."""
    pin_ready_asset(ROOT, args.state, json.loads(args.import_result.read_text()))


def cmd_verify(args):
    """Recheck freshness, integrity, and registry availability before opening a PR."""
    manifest = load_ready_state(args.state)
    if args.committed:
        provenance = ROOT / IMPORT.PROVENANCE
        fresh(json.loads(provenance.read_text())["generated_at"])
    _check_registry(Registry(), manifest)


COMMANDS = {
    "candidate": cmd_candidate,
    "validate": cmd_validate,
    "pack": cmd_pack,
    "check": cmd_check,
    "publish": cmd_publish,
    "visible": cmd_visible,
    "promote": cmd_promote,
    "ready": cmd_ready,
    "pinned-grid": cmd_pinned_grid,
    "pin": cmd_pin,
    "verify": cmd_verify,
}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    for name, handler in COMMANDS.items():
        command = commands.add_parser(name, help=handler.__doc__)
        command.set_defaults(handler=handler)
        if name != "pinned-grid":
            command.add_argument("--state", type=Path, required=True)
    commands.choices["check"].add_argument("--expect-published", action="store_true")
    commands.choices["pin"].add_argument("--import-result", type=Path, required=True)
    commands.choices["verify"].add_argument("--committed", action="store_true")
    args = parser.parse_args(argv)
    if hasattr(args, "state"):
        args.state = args.state.resolve()
    args.handler(args)


if __name__ == "__main__":
    main()
