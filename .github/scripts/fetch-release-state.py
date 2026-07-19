#!/usr/bin/env python3
"""Fetch one immutable Mainnet release-state bundle from its latest.json pointer.

The pointer and bundle live in R2 behind a pinned HTTPS host. Every hop is
digest-verified and size-bounded, redirects are rejected, and the bundle is
staged atomically, so a compromised or misconfigured bucket fails closed
instead of feeding unverified data into the import step.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# The exact hosts release-state bundles may be fetched from. Editing this list
# is a reviewed change; the repository variable only picks the latest.json URL.
ALLOWED_HOSTS = (
    "zakura-release.valargroup.dev",
    "zebra.valargroup.org",
)

LATEST_MAX_BYTES = 64 * 1024
META_MAX_BYTES = 64 * 1024
FILE_LIMITS = {
    "main-checkpoints.txt": 4 * 1024 * 1024,
    "mainnet-frontier.bin": 1 * 1024 * 1024,
}
LATEST_REQUIRED_KEYS = {
    "schema_version",
    "network",
    "height",
    "block_hash",
    "generated_at",
    "meta_url",
    "meta_sha256",
}
META_REQUIRED_KEYS = {
    "schema_version",
    "network",
    "height",
    "block_hash",
    "generated_at",
    "files",
}
META_OPTIONAL_KEYS = {"generator"}
FUTURE_SKEW = timedelta(minutes=10)


class BundleError(RuntimeError):
    """The pointer or its immutable bundle failed validation."""


class _RejectRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001, ANN201
        raise BundleError(f"redirects are not allowed while fetching {req.full_url}")


def _object(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise BundleError(f"{label} must be a JSON object")
    return value


def _string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise BundleError(f"{label} must be a non-empty string")
    return value


def _integer(value: Any, label: str, *, maximum: int = 2**32 - 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise BundleError(f"{label} must be an integer")
    if value < 0 or value > maximum:
        raise BundleError(f"{label} is outside its valid range")
    return value


def _hex_digest(value: Any, label: str, length: int = 64) -> str:
    value = _string(value, label)
    if len(value) != length or any(c not in "0123456789abcdef" for c in value):
        raise BundleError(f"{label} must be a lowercase {length}-character hex digest")
    return value


def _timestamp(value: Any, label: str) -> datetime:
    value = _string(value, label)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise BundleError(f"{label} must be an RFC 3339 timestamp") from error
    if parsed.tzinfo is None:
        raise BundleError(f"{label} must include a timezone")
    return parsed


def _parse_json(data: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise BundleError(f"{label} is not valid UTF-8 JSON: {error}") from error
    return _object(value, label)


def _validate_url(url: str, label: str) -> urllib.parse.SplitResult:
    try:
        parsed = urllib.parse.urlsplit(url)
        port = parsed.port
    except ValueError as error:
        raise BundleError(f"{label} is not a valid URL: {error}") from error

    if parsed.scheme != "https":
        raise BundleError(f"{label} must use HTTPS")
    if (parsed.hostname or "") not in ALLOWED_HOSTS:
        raise BundleError(f"{label} host must be one of {', '.join(ALLOWED_HOSTS)}")
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
            "User-Agent": "zakura-release-state-fetch/1",
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


def _check_keys(obj: dict[str, Any], required: set[str], optional: set[str], label: str) -> None:
    keys = set(obj)
    missing = required - keys
    unknown = keys - required - optional
    if missing:
        raise BundleError(f"{label} is missing keys: {', '.join(sorted(missing))}")
    if unknown:
        raise BundleError(f"{label} has unknown keys: {', '.join(sorted(unknown))}")


def resolve_bundle(
    latest_url: str,
    output_dir: Path,
    metadata_out: Path,
    max_age_hours: int,
    *,
    fetch: Callable[[str, int], bytes] = _download,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Resolve, download, and verify one immutable bundle from the pointer."""

    latest_parts = _validate_url(latest_url, "latest URL")
    if not latest_parts.path.endswith("/latest.json"):
        raise BundleError("latest URL path must end with /latest.json")
    if max_age_hours <= 0 or max_age_hours > 30 * 24:
        raise BundleError("maximum bundle age must be between 1 and 720 hours")
    if output_dir.exists():
        raise BundleError(f"output directory already exists: {output_dir}")

    latest = _parse_json(fetch(latest_url, LATEST_MAX_BYTES), "latest pointer")
    _check_keys(latest, LATEST_REQUIRED_KEYS, set(), "latest pointer")
    if _integer(latest["schema_version"], "latest.schema_version") != 1:
        raise BundleError("unsupported latest pointer schema version")
    if latest["network"] != "Mainnet":
        raise BundleError("latest.network must be Mainnet")
    height = _integer(latest["height"], "latest.height")
    block_hash = _hex_digest(latest["block_hash"], "latest.block_hash")
    meta_url = _string(latest["meta_url"], "latest.meta_url")
    meta_sha256 = _hex_digest(latest["meta_sha256"], "latest.meta_sha256")
    _timestamp(latest["generated_at"], "latest.generated_at")

    meta_parts = _validate_url(meta_url, "meta URL")
    if (meta_parts.scheme, meta_parts.netloc) != (latest_parts.scheme, latest_parts.netloc):
        raise BundleError("latest pointer and meta must use the same origin")
    prefix = latest_url.removesuffix("latest.json")
    if meta_url != f"{prefix}v1/{height}/meta.json":
        raise BundleError("meta URL is not the expected immutable bundle path")

    meta_bytes = fetch(meta_url, META_MAX_BYTES)
    if hashlib.sha256(meta_bytes).hexdigest() != meta_sha256:
        raise BundleError("meta digest does not match the latest pointer")
    meta = _parse_json(meta_bytes, "meta")
    _check_keys(meta, META_REQUIRED_KEYS, META_OPTIONAL_KEYS, "meta")
    if _integer(meta["schema_version"], "meta.schema_version") != 1:
        raise BundleError("unsupported meta schema version")
    if meta["network"] != "Mainnet":
        raise BundleError("meta.network must be Mainnet")
    if _integer(meta["height"], "meta.height") != height:
        raise BundleError("latest pointer and meta identify different heights")
    if _hex_digest(meta["block_hash"], "meta.block_hash") != block_hash:
        raise BundleError("latest pointer and meta identify different blocks")

    generated_at = _timestamp(meta["generated_at"], "meta.generated_at")
    if _string(latest["generated_at"], "latest.generated_at") != meta["generated_at"]:
        raise BundleError("latest pointer and meta have different generation times")
    now = now or datetime.now(timezone.utc)
    if generated_at > now + FUTURE_SKEW:
        raise BundleError("meta.generated_at is unexpectedly in the future")
    if now - generated_at > timedelta(hours=max_age_hours):
        raise BundleError(f"release-state bundle is older than {max_age_hours} hours")

    files = _object(meta["files"], "meta.files")
    _check_keys(files, set(FILE_LIMITS), set(), "meta.files")
    validated: dict[str, dict[str, Any]] = {}
    for name, max_size in FILE_LIMITS.items():
        entry = _object(files[name], f"meta.files.{name}")
        _check_keys(entry, {"size", "sha256"}, set(), f"meta.files.{name}")
        size = _integer(entry["size"], f"meta.files.{name}.size", maximum=max_size)
        if size == 0:
            raise BundleError(f"meta.files.{name}.size must not be zero")
        validated[name] = {
            "size": size,
            "sha256": _hex_digest(entry["sha256"], f"meta.files.{name}.sha256"),
        }

    parent = output_dir.parent
    parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=parent))
    try:
        (staging / "meta.json").write_bytes(meta_bytes)
        bundle_base = meta_url.removesuffix("meta.json")
        for name, entry in validated.items():
            data = fetch(f"{bundle_base}{name}", entry["size"])
            if len(data) != entry["size"]:
                raise BundleError(f"{name} size does not match the bundle meta")
            if hashlib.sha256(data).hexdigest() != entry["sha256"]:
                raise BundleError(f"{name} digest does not match the bundle meta")
            (staging / name).write_bytes(data)
        os.replace(staging, output_dir)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise

    resolution = {
        "height": height,
        "block_hash": block_hash,
        "generated_at": meta["generated_at"],
        "meta_url": meta_url,
        "meta_sha256": meta_sha256,
    }
    metadata_out.parent.mkdir(parents=True, exist_ok=True)
    temporary = metadata_out.with_name(f".{metadata_out.name}.tmp")
    temporary.write_text(json.dumps(resolution, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, metadata_out)
    return resolution


def _self_test() -> int:
    """Exercise the resolver against an in-process fake transport."""

    import unittest

    host = ALLOWED_HOSTS[0]
    latest_url = f"https://{host}/release-state/latest.json"
    checkpoints = b"0 00040fe8ec8471911baa1db1266ea15dd06b4a8a5c453883c000b031973dce08\n"
    frontier = b"\x36\x3d\x33\x00frontier"
    now = datetime(2026, 7, 18, 12, 0, tzinfo=timezone.utc)

    def build(
        *,
        meta_mutate: Callable[[dict[str, Any]], None] = lambda meta: None,
        latest_mutate: Callable[[dict[str, Any]], None] = lambda latest: None,
        file_overrides: dict[str, bytes] | None = None,
    ) -> dict[str, bytes]:
        meta = {
            "schema_version": 1,
            "network": "Mainnet",
            "height": 3415600,
            "block_hash": "aa" * 32,
            "generated_at": "2026-07-18T00:00:00Z",
            "files": {
                name: {
                    "size": len(data),
                    "sha256": hashlib.sha256(data).hexdigest(),
                }
                for name, data in {
                    "main-checkpoints.txt": checkpoints,
                    "mainnet-frontier.bin": frontier,
                }.items()
            },
        }
        meta_mutate(meta)
        meta_bytes = json.dumps(meta).encode()
        latest = {
            "schema_version": 1,
            "network": "Mainnet",
            "height": meta["height"],
            "block_hash": meta["block_hash"],
            "generated_at": meta["generated_at"],
            "meta_url": f"https://{host}/release-state/v1/{meta['height']}/meta.json",
            "meta_sha256": hashlib.sha256(meta_bytes).hexdigest(),
        }
        latest_mutate(latest)
        base = f"https://{host}/release-state/v1/{meta['height']}/"
        responses = {
            latest_url: json.dumps(latest).encode(),
            f"{base}meta.json": meta_bytes,
            f"{base}main-checkpoints.txt": checkpoints,
            f"{base}mainnet-frontier.bin": frontier,
        }
        responses.update(file_overrides or {})
        return responses

    class SelfTest(unittest.TestCase):
        def resolve(self, responses: dict[str, bytes], url: str = latest_url, **kwargs: Any):
            def fake_fetch(fetch_url: str, max_bytes: int) -> bytes:
                data = responses.get(fetch_url)
                if data is None:
                    raise BundleError(f"unexpected fetch of {fetch_url}")
                if len(data) > max_bytes:
                    raise BundleError(f"{fetch_url} exceeds its maximum allowed size")
                return data

            with tempfile.TemporaryDirectory() as scratch:
                return resolve_bundle(
                    url,
                    Path(scratch) / "bundle",
                    Path(scratch) / "resolution.json",
                    kwargs.pop("max_age_hours", 48),
                    fetch=fake_fetch,
                    now=now,
                    **kwargs,
                )

        def test_happy_path(self):
            resolution = self.resolve(build())
            self.assertEqual(resolution["height"], 3415600)

        def test_wrong_host_rejected(self):
            with self.assertRaisesRegex(BundleError, "host"):
                self.resolve(build(), url="https://evil.example/release-state/latest.json")

        def test_http_rejected(self):
            with self.assertRaisesRegex(BundleError, "HTTPS"):
                self.resolve(build(), url=f"http://{host}/release-state/latest.json")

        def test_meta_digest_mismatch_rejected(self):
            responses = build(latest_mutate=lambda latest: latest.update(meta_sha256="bb" * 32))
            with self.assertRaisesRegex(BundleError, "meta digest"):
                self.resolve(responses)

        def test_file_digest_mismatch_rejected(self):
            responses = build(
                file_overrides={
                    f"https://{host}/release-state/v1/3415600/mainnet-frontier.bin": b"\x00"
                    * len(frontier)
                }
            )
            with self.assertRaisesRegex(BundleError, "digest does not match"):
                self.resolve(responses)

        def test_unexpected_meta_path_rejected(self):
            responses = build(
                latest_mutate=lambda latest: latest.update(
                    meta_url=f"https://{host}/other/v1/3415600/meta.json"
                )
            )
            with self.assertRaisesRegex(BundleError, "immutable bundle path"):
                self.resolve(responses)

        def test_stale_bundle_rejected(self):
            stamp = "2026-07-01T00:00:00Z"

            def make_stale(meta: dict[str, Any]) -> None:
                meta["generated_at"] = stamp

            responses = build(
                meta_mutate=make_stale,
                latest_mutate=lambda latest: latest.update(generated_at=stamp),
            )
            with self.assertRaisesRegex(BundleError, "older than"):
                self.resolve(responses)

        def test_future_bundle_rejected(self):
            stamp = "2026-07-19T00:00:00Z"

            def make_future(meta: dict[str, Any]) -> None:
                meta["generated_at"] = stamp

            responses = build(
                meta_mutate=make_future,
                latest_mutate=lambda latest: latest.update(generated_at=stamp),
            )
            with self.assertRaisesRegex(BundleError, "in the future"):
                self.resolve(responses)

        def test_unknown_meta_key_rejected(self):
            def add_key(meta: dict[str, Any]) -> None:
                meta["surprise"] = True

            with self.assertRaisesRegex(BundleError, "unknown keys"):
                self.resolve(build(meta_mutate=add_key))

        def test_oversize_pointer_rejected(self):
            responses = build()
            responses[latest_url] = b"{" + b" " * LATEST_MAX_BYTES + b"}"
            with self.assertRaisesRegex(BundleError, "maximum allowed size"):
                self.resolve(responses)

        def test_malformed_pointer_rejected(self):
            responses = build()
            responses[latest_url] = b"not json"
            with self.assertRaisesRegex(BundleError, "JSON"):
                self.resolve(responses)

        def test_redirects_rejected(self):
            handler = _RejectRedirects()
            request = urllib.request.Request(latest_url)
            with self.assertRaisesRegex(BundleError, "redirects"):
                handler.redirect_request(request, None, 302, "Found", {}, "https://evil.example/")

    suite = unittest.defaultTestLoader.loadTestsFromTestCase(SelfTest)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true", help="run the built-in tests")
    parser.add_argument("--latest-url")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--metadata-out", type=Path)
    parser.add_argument("--max-age-hours", type=int, default=48)
    args = parser.parse_args()

    if args.self_test:
        return _self_test()
    if not (args.latest_url and args.output_dir and args.metadata_out):
        parser.error("--latest-url, --output-dir, and --metadata-out are required")

    try:
        resolution = resolve_bundle(
            args.latest_url,
            args.output_dir,
            args.metadata_out,
            args.max_age_hours,
        )
    except BundleError as error:
        print(f"release-state fetch failed: {error}", file=sys.stderr)
        return 1

    print(
        f"fetched Mainnet release state at height {resolution['height']} "
        f"({resolution['block_hash']})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
