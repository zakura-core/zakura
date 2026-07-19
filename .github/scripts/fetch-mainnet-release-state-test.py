#!/usr/bin/env python3
"""Offline checks for the Mainnet release-state bundle fetcher."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import tempfile
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
FETCHER_PATH = ROOT / ".github" / "scripts" / "fetch-mainnet-release-state.py"
LATEST_URL = "https://zakura-release.valargroup.dev/mainnet/v1/latest.json"
BLOCK_HASH = "01" * 32
BLOCK_METADATA = b"block metadata fixture"
FRONTIER = b"frontier fixture"


def load_fetcher():
    spec = importlib.util.spec_from_file_location("fetch_mainnet_release_state", FETCHER_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fetcher = load_fetcher()


def encoded(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True) + "\n").encode()


def fixture() -> tuple[dict[str, object], dict[str, object], dict[str, bytes]]:
    height = 3_400_000
    manifest_url = (
        "https://zakura-release.valargroup.dev/mainnet/v1/bundles/"
        f"{height}-{BLOCK_HASH}/manifest.json"
    )
    manifest: dict[str, object] = {
        "schema_version": 1,
        "network": "Mainnet",
        "generated_at": "2026-07-18T12:34:56Z",
        "finalized_height": height,
        "finalized_hash": BLOCK_HASH,
        "base_checkpoint_height": 3_358_006,
        "base_checkpoint_hash": "02" * 32,
        "base_checkpoints_sha256": "03" * 32,
        "artifacts": {
            "block_metadata": {
                "file": "block-metadata.bin",
                "size": len(BLOCK_METADATA),
                "sha256": hashlib.sha256(BLOCK_METADATA).hexdigest(),
            },
            "frontier": {
                "file": "mainnet-frontier.bin",
                "size": len(FRONTIER),
                "sha256": hashlib.sha256(FRONTIER).hexdigest(),
            },
        },
    }
    manifest_bytes = encoded(manifest)
    latest: dict[str, object] = {
        "schema_version": 1,
        "network": "Mainnet",
        "height": height,
        "block_hash": BLOCK_HASH,
        "manifest_url": manifest_url,
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
    }
    responses = {
        LATEST_URL: encoded(latest),
        manifest_url: manifest_bytes,
        manifest_url.removesuffix("manifest.json") + "block-metadata.bin": BLOCK_METADATA,
        manifest_url.removesuffix("manifest.json") + "mainnet-frontier.bin": FRONTIER,
    }
    return latest, manifest, responses


def run_fixture(
    latest: dict[str, object],
    manifest: dict[str, object],
    responses: dict[str, bytes],
    *,
    refresh_manifest_digest: bool = True,
):
    manifest_url = str(latest["manifest_url"])
    manifest_bytes = encoded(manifest)
    if refresh_manifest_digest:
        latest["manifest_sha256"] = hashlib.sha256(manifest_bytes).hexdigest()
    responses[LATEST_URL] = encoded(latest)
    responses[manifest_url] = manifest_bytes

    def fetch(url: str, max_bytes: int) -> bytes:
        data = responses[url]
        if len(data) > max_bytes:
            raise fetcher.BundleError("fixture response exceeded limit")
        return data

    temporary = tempfile.TemporaryDirectory()
    root = Path(temporary.name)
    bundle_dir = root / "bundle"
    metadata_out = root / "resolution.json"
    try:
        resolution = fetcher.resolve_bundle(
            LATEST_URL,
            bundle_dir,
            metadata_out,
            fetch=fetch,
            max_age_hours=48,
            now=datetime(2026, 7, 18, 13, tzinfo=timezone.utc),
        )
        return temporary, bundle_dir, metadata_out, resolution
    except BaseException:
        temporary.cleanup()
        raise


def expect_error(expected: str, callback) -> None:
    try:
        callback()
    except fetcher.BundleError as error:
        assert expected in str(error), str(error)
    else:
        raise AssertionError(f"expected BundleError containing {expected!r}")


def test_valid_bundle() -> None:
    latest, manifest, responses = fixture()
    temporary, bundle_dir, metadata_out, resolution = run_fixture(
        latest, manifest, responses
    )
    try:
        assert sorted(path.name for path in bundle_dir.iterdir()) == [
            "block-metadata.bin",
            "mainnet-frontier.bin",
            "manifest.json",
        ]
        assert (bundle_dir / "block-metadata.bin").read_bytes() == BLOCK_METADATA
        assert (bundle_dir / "mainnet-frontier.bin").read_bytes() == FRONTIER
        assert json.loads(metadata_out.read_text()) == resolution
        assert resolution["height"] == 3_400_000
    finally:
        temporary.cleanup()


def test_rejects_untrusted_pointer_host() -> None:
    expect_error(
        "must use zakura-release.valargroup.dev",
        lambda: fetcher.resolve_bundle(
            "https://example.com/mainnet/v1/latest.json",
            Path("unused"),
            Path("unused-metadata"),
            fetch=lambda _url, _limit: b"{}",
        ),
    )


def test_rejects_other_valar_pointer_host() -> None:
    expect_error(
        "must use zakura-release.valargroup.dev",
        lambda: fetcher.resolve_bundle(
            "https://other.valargroup.dev/mainnet/v1/latest.json",
            Path("unused"),
            Path("unused-metadata"),
            fetch=lambda _url, _limit: b"{}",
        ),
    )


def test_rejects_prefixed_pointer_path() -> None:
    expect_error(
        "path must be exactly /mainnet/v1/latest.json",
        lambda: fetcher.resolve_bundle(
            "https://zakura-release.valargroup.dev/extra/mainnet/v1/latest.json",
            Path("unused"),
            Path("unused-metadata"),
            fetch=lambda _url, _limit: b"{}",
        ),
    )


def test_rejects_non_immutable_manifest_path() -> None:
    latest, manifest, responses = fixture()
    latest["manifest_url"] = (
        "https://zakura-release.valargroup.dev/mainnet/v1/bundles/current/manifest.json"
    )
    responses[str(latest["manifest_url"])] = encoded(manifest)
    expect_error(
        "expected immutable bundle path",
        lambda: run_fixture(latest, manifest, responses),
    )


def test_rejects_stale_bundle() -> None:
    latest, manifest, responses = fixture()
    manifest["generated_at"] = "2026-07-16T12:59:59Z"
    expect_error(
        "older than 48 hours",
        lambda: run_fixture(latest, manifest, responses),
    )


def test_rejects_manifest_identity_mismatch() -> None:
    latest, manifest, responses = fixture()
    manifest["finalized_height"] = 3_400_001
    expect_error(
        "different finalized blocks",
        lambda: run_fixture(latest, manifest, responses),
    )


def test_rejects_manifest_digest_mismatch() -> None:
    latest, manifest, responses = fixture()
    latest["manifest_sha256"] = "ff" * 32
    expect_error(
        "manifest digest",
        lambda: run_fixture(
            latest,
            manifest,
            responses,
            refresh_manifest_digest=False,
        ),
    )


def test_rejects_manifest_on_another_origin() -> None:
    latest, manifest, responses = fixture()
    latest["manifest_url"] = str(latest["manifest_url"]).replace(
        "zakura-release.valargroup.dev", "other.valargroup.dev"
    )
    expect_error(
        "must use zakura-release.valargroup.dev",
        lambda: run_fixture(latest, manifest, responses),
    )


def test_rejects_artifact_filename_change() -> None:
    latest, manifest, responses = fixture()
    artifacts = deepcopy(manifest["artifacts"])
    assert isinstance(artifacts, dict)
    block_metadata = artifacts["block_metadata"]
    assert isinstance(block_metadata, dict)
    block_metadata["file"] = "../block-metadata.bin"
    manifest["artifacts"] = artifacts
    expect_error(
        "must be block-metadata.bin",
        lambda: run_fixture(latest, manifest, responses),
    )


def test_rejects_artifact_digest_mismatch() -> None:
    latest, manifest, responses = fixture()
    manifest_url = str(latest["manifest_url"])
    responses[manifest_url.removesuffix("manifest.json") + "mainnet-frontier.bin"] = (
        b"wrongier fixture"
    )
    expect_error(
        "mainnet-frontier.bin digest",
        lambda: run_fixture(latest, manifest, responses),
    )


def test_rejects_existing_output_directory() -> None:
    latest, _manifest, responses = fixture()

    def fetch(url: str, _max_bytes: int) -> bytes:
        return responses[url]

    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        bundle_dir = root / "bundle"
        bundle_dir.mkdir()
        expect_error(
            "already exists",
            lambda: fetcher.resolve_bundle(
                LATEST_URL,
                bundle_dir,
                root / "resolution.json",
                fetch=fetch,
            ),
        )


def main() -> int:
    tests = [
        value
        for name, value in sorted(globals().items())
        if name.startswith("test_") and callable(value)
    ]
    for test in tests:
        test()
        print(f"ok: {test.__name__}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
