"""In-memory object storage and a loopback Cargo registry for release-state tests."""

import hashlib
import json
import os
import re
import struct
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlsplit

from release_state import FETCH, IMMUTABLE
from release_state_registry import AssetError, Registry

PUBLIC_ORIGIN = "https://zakura-release.valargroup.dev/"
INDEX_KEYS = ("name", "vers", "deps", "features", "cksum", "yanked")


class MemoryStore:
    """Object storage with S3 conditional-write semantics, served at its public domain."""

    def __init__(self):
        self.objects = {}  # key -> (data, etag)
        self.lock = threading.Lock()

    def get(self, key, limit):
        with self.lock:
            entry = self.objects.get(key)
        if entry and len(entry[0]) > limit:
            raise AssetError("stored object exceeds its size limit")
        return entry

    def put(self, key, data, *, etag=None, cache=IMMUTABLE):
        with self.lock:
            current = self.objects.get(key)
            if (current and current[1]) != etag:
                return False
            self.objects[key] = data, '"' + hashlib.md5(data, usedforsecurity=False).hexdigest() + '"'
            return True

    def public_fetch(self, url, limit):
        """Fetch through the public domain, failing like an HTTP 404 for a missing object."""
        entry = self.objects.get(url.removeprefix(PUBLIC_ORIGIN))
        if entry is None:
            raise FETCH.BundleError(f"failed to fetch {url}: HTTP Error 404: Not Found")
        if len(entry[0]) > limit:
            raise FETCH.BundleError(f"{url} exceeds its maximum allowed size")
        return entry[0]


class FixtureRegistry:
    """A loopback sparse registry that real `cargo publish`, `update`, and `metadata` can use."""

    ROUTES = (
        ("GET", r"/index/config\.json", "config"),
        ("GET", r"/index/(?:[^/]+/)+(?P<name>[^/]+)", "index_records"),
        ("GET", r"/api/v1/crates/(?P<name>[^/]+)/(?P<version>[^/]+)", "version"),
        ("GET", r"/crates/(?P<name>[^/]+)/[^/]+-(?P<version>[^-/]+)\.crate", "download"),
        ("PUT", r"/api/v1/crates/new", "publish"),
    )

    def __init__(self):
        self.crates = {}  # (name, version) -> index record plus "archive"
        self.reset()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self.server.daemon_threads = True
        self.base = f"http://127.0.0.1:{self.server.server_port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def reset(self):
        self.api_status = None  # forced status for every version API request
        self.publish_status = None  # forced status for every upload
        self.index_misses = 0  # index requests to answer 404 before the records appear

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def registry(self, cargo_home: Path) -> Registry:
        """Return a Registry whose Cargo reaches this fixture through an isolated CARGO_HOME."""
        cargo_home.mkdir(parents=True, exist_ok=True)
        (cargo_home / "config.toml").write_text(f'[registries.fixture]\nindex = "sparse+{self.base}/index/"\n')
        env = {**os.environ, "CARGO_HOME": str(cargo_home), "CARGO_REGISTRIES_FIXTURE_TOKEN": "fixture-only",
               "CARGO_NET_RETRY": "0", "CARGO_HTTP_TIMEOUT": "10"}
        return Registry(api=f"{self.base}/api/v1/crates", downloads=f"{self.base}/crates",
                        index=f"{self.base}/index", source=f"sparse+{self.base}/index/",
                        cargo_name="fixture", cargo_env=env, poll_interval=0)

    def config(self, body):
        return 200, json.dumps({"dl": self.base + "/crates/{crate}/{crate}-{version}.crate",
                                "api": self.base}).encode()

    def index_records(self, body, name):
        if self.index_misses:
            self.index_misses -= 1
            return 404, b""
        records = [json.dumps({key: crate[key] for key in INDEX_KEYS})
                   for (crate_name, _), crate in self.crates.items() if crate_name == name]
        return (200, "\n".join(records).encode()) if records else (404, b"")

    def version(self, body, name, version):
        crate = self.crates.get((name, version))
        if self.api_status or crate is None:
            return self.api_status or 404, b""
        return 200, json.dumps({"version": {"crate": name, "num": version, "checksum": crate["cksum"],
                                            "yanked": crate["yanked"]}}).encode()

    def download(self, body, name, version):
        crate = self.crates.get((name, version))
        return (200, crate["archive"]) if crate else (404, b"")

    def publish(self, body):
        if self.publish_status:
            return self.publish_status, b'{"errors":[{"detail":"fixture refused publication"}]}'
        # Cargo's upload framing: u32 metadata length, metadata JSON, u32 archive length, archive.
        (size,) = struct.unpack_from("<I", body)
        metadata = json.loads(body[4:4 + size])
        archive = body[8 + size:]
        key = metadata["name"], metadata["vers"]
        if key in self.crates:
            return 409, b'{"errors":[{"detail":"version already exists"}]}'
        self.crates[key] = dict(name=key[0], vers=key[1], deps=[], features={},
                                cksum=hashlib.sha256(archive).hexdigest(), yanked=False, archive=archive)
        return 200, b'{"warnings":{"invalid_categories":[],"invalid_badges":[],"other":[]}}'

    def _handler(self):
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def do_GET(self):
                self.route("GET")

            def do_PUT(self):
                self.route("PUT")

            def route(self, method):
                path = unquote(urlsplit(self.path).path)
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                status, reply = 404, b""
                for verb, pattern, action in fixture.ROUTES:
                    match = re.fullmatch(pattern, path)
                    if verb == method and match:
                        status, reply = getattr(fixture, action)(body, **match.groupdict())
                        break
                self.send_response(status)
                self.send_header("Content-Length", str(len(reply)))
                self.end_headers()
                self.wfile.write(reply)

        return Handler
