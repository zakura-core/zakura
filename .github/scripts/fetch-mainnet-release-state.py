#!/usr/bin/env python3
"""Fetch one immutable Mainnet release-state bundle from its latest pointer."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


LATEST_MAX_BYTES = 64 * 1024
MANIFEST_MAX_BYTES = 1024 * 1024
ARTIFACT_LIMITS = {
    "block_metadata": ("block-metadata.bin", 256 * 1024 * 1024),
    "frontier": ("mainnet-frontier.bin", 64 * 1024 * 1024),
}
HASH_RE = re.compile(r"[0-9a-f]{64}\Z")
PUBLIC_HOST = "zakura-release.valargroup.dev"


class BundleError(RuntimeError):
    """The pointer or its immutable bundle failed validation."""


class _RejectRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001, ANN201
        raise BundleError(f"redirects are not allowed while fetching {req.full_url}")


def _object(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise BundleError(f"{label} must be a JSON object")
    return value


def _integer(value: Any, label: str, *, maximum: int = 2**64 - 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise BundleError(f"{label} must be an integer")
    if value < 0 or value > maximum:
        raise BundleError(f"{label} is outside its valid range")
    return value


def _string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise BundleError(f"{label} must be a non-empty string")
    return value


def _hash(value: Any, label: str) -> str:
    value = _string(value, label)
    if HASH_RE.fullmatch(value) is None:
        raise BundleError(f"{label} must be a lowercase 64-character hex digest")
    return value


def _parse_json(data: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise BundleError(f"{label} is not valid UTF-8 JSON: {error}") from error
    return _object(value, label)


def _validate_public_url(url: str, label: str) -> urllib.parse.SplitResult:
    try:
        parsed = urllib.parse.urlsplit(url)
        port = parsed.port
    except ValueError as error:
        raise BundleError(f"{label} is not a valid URL: {error}") from error

    host = parsed.hostname or ""
    if parsed.scheme != "https":
        raise BundleError(f"{label} must use HTTPS")
    if host != PUBLIC_HOST:
        raise BundleError(f"{label} must use {PUBLIC_HOST}")
    if parsed.username is not None or parsed.password is not None:
        raise BundleError(f"{label} must not contain credentials")
    if port not in (None, 443):
        raise BundleError(f"{label} must use the default HTTPS port")
    if parsed.query or parsed.fragment:
        raise BundleError(f"{label} must not contain a query or fragment")
    if "%" in parsed.path or "//" in parsed.path:
        raise BundleError(f"{label} contains an ambiguous path")
    return parsed


def _download(url: str, max_bytes: int) -> bytes:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json, application/octet-stream;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "User-Agent": "zakura-release-state/1",
        },
    )
    opener = urllib.request.build_opener(_RejectRedirects())
    try:
        with opener.open(request, timeout=30) as response:
            content_length = response.headers.get("Content-Length")
            if content_length is not None and int(content_length) > max_bytes:
                raise BundleError(f"{url} exceeds its maximum allowed size")
            data = response.read(max_bytes + 1)
    except BundleError:
        raise
    except (OSError, ValueError, urllib.error.URLError) as error:
        raise BundleError(f"failed to fetch {url}: {error}") from error

    if len(data) > max_bytes:
        raise BundleError(f"{url} exceeds its maximum allowed size")
    return data


def _validate_generated_at(value: Any) -> datetime:
    value = _string(value, "manifest.generated_at")
    try:
        generated_at = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise BundleError("manifest.generated_at must be an RFC 3339 timestamp") from error
    if generated_at.tzinfo is None:
        raise BundleError("manifest.generated_at must include a timezone")
    return generated_at


def _validate_manifest(
    manifest: dict[str, Any],
    *,
    pointer_height: int,
    pointer_hash: str,
) -> dict[str, dict[str, Any]]:
    if _integer(manifest.get("schema_version"), "manifest.schema_version") != 1:
        raise BundleError("unsupported manifest schema version")
    if manifest.get("network") != "Mainnet":
        raise BundleError("manifest.network must be Mainnet")
    _validate_generated_at(manifest.get("generated_at"))

    finalized_height = _integer(
        manifest.get("finalized_height"),
        "manifest.finalized_height",
        maximum=2**32 - 1,
    )
    finalized_hash = _hash(manifest.get("finalized_hash"), "manifest.finalized_hash")
    if finalized_height != pointer_height or finalized_hash != pointer_hash:
        raise BundleError("latest pointer and manifest identify different finalized blocks")

    _integer(
        manifest.get("base_checkpoint_height"),
        "manifest.base_checkpoint_height",
        maximum=2**32 - 1,
    )
    _hash(manifest.get("base_checkpoint_hash"), "manifest.base_checkpoint_hash")
    _hash(
        manifest.get("base_checkpoints_sha256"),
        "manifest.base_checkpoints_sha256",
    )

    artifacts = _object(manifest.get("artifacts"), "manifest.artifacts")
    validated: dict[str, dict[str, Any]] = {}
    for key, (expected_file, max_size) in ARTIFACT_LIMITS.items():
        artifact = _object(artifacts.get(key), f"manifest.artifacts.{key}")
        if artifact.get("file") != expected_file:
            raise BundleError(
                f"manifest.artifacts.{key}.file must be {expected_file}"
            )
        size = _integer(artifact.get("size"), f"manifest.artifacts.{key}.size")
        if size == 0 or size > max_size:
            raise BundleError(f"manifest artifact {expected_file} has an invalid size")
        validated[key] = {
            "file": expected_file,
            "size": size,
            "sha256": _hash(
                artifact.get("sha256"),
                f"manifest.artifacts.{key}.sha256",
            ),
        }
    return validated


def resolve_bundle(
    latest_url: str,
    output_dir: Path,
    metadata_out: Path,
    *,
    fetch: Callable[[str, int], bytes] = _download,
    max_age_hours: int | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Resolve, download, and verify one pointer without re-reading it."""

    latest_parts = _validate_public_url(latest_url, "latest URL")
    if latest_parts.path != "/mainnet/v1/latest.json":
        raise BundleError("latest URL path must be exactly /mainnet/v1/latest.json")
    if output_dir.exists():
        raise BundleError(f"output directory already exists: {output_dir}")

    latest_bytes = fetch(latest_url, LATEST_MAX_BYTES)
    latest = _parse_json(latest_bytes, "latest pointer")
    if _integer(latest.get("schema_version"), "latest.schema_version") != 1:
        raise BundleError("unsupported latest pointer schema version")
    if latest.get("network") != "Mainnet":
        raise BundleError("latest.network must be Mainnet")
    height = _integer(latest.get("height"), "latest.height", maximum=2**32 - 1)
    block_hash = _hash(latest.get("block_hash"), "latest.block_hash")
    manifest_url = _string(latest.get("manifest_url"), "latest.manifest_url")
    manifest_sha256 = _hash(
        latest.get("manifest_sha256"), "latest.manifest_sha256"
    )

    manifest_parts = _validate_public_url(manifest_url, "manifest URL")
    if (manifest_parts.scheme, manifest_parts.netloc) != (
        latest_parts.scheme,
        latest_parts.netloc,
    ):
        raise BundleError("latest pointer and manifest must use the same origin")
    latest_base = latest_url.removesuffix("latest.json")
    expected_manifest_url = (
        f"{latest_base}bundles/{height}-{block_hash}/manifest.json"
    )
    if manifest_url != expected_manifest_url:
        raise BundleError("manifest URL is not the expected immutable bundle path")

    manifest_bytes = fetch(manifest_url, MANIFEST_MAX_BYTES)
    if hashlib.sha256(manifest_bytes).hexdigest() != manifest_sha256:
        raise BundleError("manifest digest does not match the latest pointer")
    manifest = _parse_json(manifest_bytes, "manifest")
    artifacts = _validate_manifest(
        manifest,
        pointer_height=height,
        pointer_hash=block_hash,
    )
    if max_age_hours is not None:
        if max_age_hours <= 0 or max_age_hours > 30 * 24:
            raise BundleError("maximum bundle age must be between 1 and 720 hours")
        generated_at = _validate_generated_at(manifest["generated_at"])
        now = now or datetime.now(timezone.utc)
        if now.tzinfo is None:
            raise BundleError("current time must include a timezone")
        if generated_at > now + timedelta(minutes=10):
            raise BundleError("manifest.generated_at is unexpectedly in the future")
        if now - generated_at > timedelta(hours=max_age_hours):
            raise BundleError(
                f"release-state bundle is older than {max_age_hours} hours"
            )

    parent = output_dir.parent
    parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=parent))
    try:
        (staging / "manifest.json").write_bytes(manifest_bytes)
        bundle_base = manifest_url.removesuffix("manifest.json")
        for artifact in artifacts.values():
            artifact_url = urllib.parse.urljoin(bundle_base, artifact["file"])
            artifact_bytes = fetch(artifact_url, artifact["size"])
            if len(artifact_bytes) != artifact["size"]:
                raise BundleError(f"{artifact['file']} size does not match the manifest")
            if hashlib.sha256(artifact_bytes).hexdigest() != artifact["sha256"]:
                raise BundleError(f"{artifact['file']} digest does not match the manifest")
            (staging / artifact["file"]).write_bytes(artifact_bytes)

        os.replace(staging, output_dir)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise

    resolution = {
        "latest_url": latest_url,
        "manifest_url": manifest_url,
        "manifest_sha256": manifest_sha256,
        "height": height,
        "block_hash": block_hash,
        "generated_at": manifest["generated_at"],
    }
    metadata_out.parent.mkdir(parents=True, exist_ok=True)
    temporary_metadata = metadata_out.with_name(f".{metadata_out.name}.tmp")
    temporary_metadata.write_text(
        json.dumps(resolution, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary_metadata, metadata_out)
    return resolution


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--latest-url", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--metadata-out", required=True, type=Path)
    parser.add_argument("--max-age-hours", required=True, type=int)
    args = parser.parse_args()

    try:
        resolution = resolve_bundle(
            args.latest_url,
            args.output_dir,
            args.metadata_out,
            max_age_hours=args.max_age_hours,
        )
    except BundleError as error:
        parser.error(str(error))

    print(
        "Fetched Mainnet release state "
        f"at height {resolution['height']} ({resolution['block_hash']})."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
