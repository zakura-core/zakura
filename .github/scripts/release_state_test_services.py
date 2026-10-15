"""Loopback-only S3 and Cargo registry fixtures for process-level publication tests."""

import hashlib
import functools
import json
import os
import struct
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlsplit
from urllib.request import Request, urlopen


class Services:
    def __init__(self):
        self.objects = {}
        self.versions = {}
        self.lock = threading.Lock()
        self.fail_put = None
        self.fail_get = None
        self.hide_public = None
        self.index_delays = 0
        self.api_status = None
        self.publish_status = None
        self.writes = []
        self.race = None
        owner = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def reply(self, status, body=b"", headers=None):
                self.send_response(status)
                self.send_header("Content-Length", str(len(body)))
                for name, value in (headers or {}).items():
                    self.send_header(name, value)
                self.end_headers()
                self.wfile.write(body)

            def error(self, status, code):
                self.reply(status, f'<Error><Code>{code}</Code><Message>fixture</Message></Error>'.encode(),
                           {"Content-Type": "application/xml"})

            def do_GET(self):
                path = unquote(urlsplit(self.path).path)
                if path.startswith("/s3/fixture/") or path.startswith("/public/"):
                    key = path.removeprefix("/s3/fixture/") if path.startswith("/s3/") else path.removeprefix("/public/")
                    if owner.fail_get and owner.fail_get(key):
                        return self.error(403, "AccessDenied")
                    if path.startswith("/public/") and owner.hide_public and owner.hide_public(key):
                        return self.error(404, "NoSuchKey")
                    with owner.lock:
                        entry = owner.objects.get(key)
                    if entry is None:
                        return self.error(404, "NoSuchKey")
                    data, etag, cache = entry
                    return self.reply(200, data, {"ETag": etag, "Cache-Control": cache})
                if path == "/registry/index/config.json":
                    return self.reply(200, json.dumps({"dl": owner.base + "/registry/crates/{crate}/{crate}-{version}.crate",
                                                       "api": owner.base + "/registry"}).encode())
                if path.startswith("/registry/api/v1/crates/"):
                    if owner.api_status:
                        return self.error(owner.api_status, "Denied")
                    parts = path.split("/")
                    entry = owner.versions.get((parts[-2], parts[-1]))
                    if entry is None:
                        return self.error(404, "NotFound")
                    name, version = parts[-2:]
                    return self.reply(200, json.dumps({"version": {"crate": name, "num": version,
                        "checksum": entry["cksum"], "yanked": entry.get("yanked", False)}}).encode())
                if path.startswith("/registry/index/"):
                    if owner.index_delays:
                        owner.index_delays -= 1
                        return self.error(404, "NotFound")
                    name = path.rsplit("/", 1)[-1]
                    entries = [value for (crate, _), value in owner.versions.items() if crate == name]
                    if not entries:
                        return self.error(404, "NotFound")
                    return self.reply(200, b"\n".join(json.dumps({key: value[key] for key in
                        ("name", "vers", "deps", "features", "cksum", "yanked")}).encode() for value in entries))
                if path.startswith("/registry/crates/"):
                    for entry in owner.versions.values():
                        if path.endswith(f'/{entry["name"]}-{entry["vers"]}.crate'):
                            return self.reply(200, entry["archive"])
                self.error(404, "NotFound")

            def do_PUT(self):
                path = unquote(urlsplit(self.path).path)
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                if path == "/registry/api/v1/crates/new":
                    if owner.publish_status:
                        return self.error(owner.publish_status, "PublicationFailed")
                    (size,) = struct.unpack_from("<I", body)
                    metadata = json.loads(body[4:4 + size])
                    (archive_size,) = struct.unpack_from("<I", body, 4 + size)
                    archive = body[8 + size:]
                    if len(archive) != archive_size:
                        return self.error(400, "BadArchive")
                    key = metadata["name"], metadata["vers"]
                    if key in owner.versions:
                        return self.error(409, "AlreadyExists")
                    owner.versions[key] = dict(name=key[0], vers=key[1], deps=[], features={},
                                               cksum=hashlib.sha256(archive).hexdigest(), yanked=False, archive=archive)
                    owner.writes.append(("crate", key))
                    return self.reply(200, b'{"warnings":{"invalid_categories":[],"invalid_badges":[],"other":[]}}')
                if not path.startswith("/s3/fixture/"):
                    return self.error(404, "NotFound")
                key = path.removeprefix("/s3/fixture/")
                if owner.fail_put and owner.fail_put(key):
                    return self.error(503, "SlowDown")
                if owner.race:
                    owner.race(key)
                with owner.lock:
                    current = owner.objects.get(key)
                    if self.headers.get("If-None-Match") == "*" and current is not None:
                        return self.error(412, "PreconditionFailed")
                    if "If-Match" in self.headers and (current is None or current[1] != self.headers["If-Match"]):
                        return self.error(412, "PreconditionFailed")
                    etag = '"' + hashlib.md5(body, usedforsecurity=False).hexdigest() + '"'
                    owner.objects[key] = body, etag, self.headers.get("Cache-Control", "")
                    owner.writes.append(("object", key))
                self.reply(200, headers={"ETag": etag})

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.base = f"http://127.0.0.1:{self.server.server_port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def s3(self):
        import boto3
        from botocore.config import Config
        from ready_release_state import S3Store
        client = boto3.client("s3", endpoint_url=self.base + "/s3", region_name="auto",
                              aws_access_key_id="fixture-only", aws_secret_access_key="fixture-only",
                              config=Config(s3={"addressing_style": "path"}, connect_timeout=2, read_timeout=2,
                                            retries={"max_attempts": 0}, request_checksum_calculation="when_required"))
        return S3Store(client, "fixture")

    def request(self, url, limit):
        # Production URLs remain validated by production parsers; only transport is loopback.
        from release_state_assets import AssetError, NotPublished
        from urllib.error import HTTPError
        parts = urlsplit(url)
        if parts.hostname in ("zakura-release.valargroup.dev", "zebra.valargroup.org"):
            endpoint = "/public" + parts.path
        elif parts.hostname == "crates.io":
            endpoint = "/registry" + parts.path
        elif parts.hostname == "static.crates.io":
            endpoint = "/registry" + parts.path
        elif parts.hostname == "index.crates.io":
            endpoint = "/registry/index" + parts.path
        else:
            raise AssertionError(f"unexpected network destination: {parts.hostname}")
        try:
            with urlopen(self.base + endpoint, timeout=5) as response:
                value = response.read(limit + 1)
        except HTTPError as error:
            error.close()
            if error.code == 404:
                raise NotPublished("fixture returned 404") from error
            raise AssetError(f"fixture returned HTTP {error.code}") from error
        if len(value) > limit:
            raise AssetError("fixture response exceeds limit")
        return value

    def cargo_env(self, root):
        home = root / "cargo-home"
        home.mkdir(exist_ok=True)
        (home / "config.toml").write_text('[registries.fixture]\nindex = "sparse+' + self.base + '/registry/index/"\n')
        return {**os.environ, "CARGO_HOME": str(home), "CARGO_REGISTRIES_FIXTURE_TOKEN": "fixture-only",
                "CARGO_NET_RETRY": "0", "CARGO_HTTP_TIMEOUT": "10"}

    def cargo_resolver(self, env):
        from release_state_assets import cargo_grid
        return functools.partial(cargo_grid, registry_index=self.base + "/registry/index/", command_env=env)
