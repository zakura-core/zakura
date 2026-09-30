"""Bounded JSON transport and canonical mainnet comparison records."""
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
import urllib.error
import urllib.request

MAX_JSON = 256 * 1024
POOLS = ("sapling", "orchard", "ironwood")


class Unavailable(Exception):
    """A sample cannot establish a comparison result."""


def integer(value, name="height"):
    if type(value) is not int or not 0 <= value <= 0xFFFFFFFF:
        raise Unavailable(f"invalid {name}")
    return value


def hex_bytes(value, length=None):
    if not isinstance(value, str) or not re.fullmatch(r"(?:[0-9a-fA-F]{2})+", value):
        raise Unavailable("invalid hex data")
    result = bytes.fromhex(value)
    if length is not None and len(result) != length:
        raise Unavailable("invalid hash length")
    return result.hex()


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".pending-")
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(value, stream, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
        fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def read_json(path):
    with Path(path).open() as stream:
        return json.load(stream)


def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        raise urllib.error.HTTPError(request.full_url, code, "redirect refused", headers, response)


class Transport:
    def __init__(self, timeout=10):
        self.timeout = timeout
        # Do not send loopback requests through operator-defined HTTP proxies.
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

    def json(self, url, payload=None, headers=None, method=None):
        data = None if payload is None else json.dumps(payload).encode()
        request = urllib.request.Request(
            url, data=data,
            headers={"Content-Type": "application/json", **(headers or {})}, method=method,
        )
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                body = response.read(MAX_JSON + 1)
                if len(body) > MAX_JSON:
                    raise Unavailable("oversize JSON response")
                return json.loads(body)
        except urllib.error.HTTPError:
            raise
        except (OSError, ValueError) as error:
            raise Unavailable(type(error).__name__) from None


class RPC:
    def __init__(self, url, transport=None):
        if not re.fullmatch(r"http://127\.0\.0\.1:[0-9]{1,5}", url):
            raise ValueError("RPC must use a fixed loopback listener")
        self.url = url
        self.transport = transport or Transport()

    def call(self, method, *params):
        try:
            result = self.transport.json(self.url, {"jsonrpc": "2.0", "id": 1,
                                                   "method": method, "params": list(params)})
        except urllib.error.HTTPError as error:
            raise Unavailable(f"RPC HTTP {error.code}") from None
        if not isinstance(result, dict) or result.get("error") or "result" not in result:
            raise Unavailable("RPC result unavailable")
        return result["result"]

    def tip(self):
        info = self.call("getblockchaininfo")
        if info.get("chain") != "main":
            raise Unavailable("reference is not mainnet")
        height = integer(info.get("blocks"))
        return {"height": height, "hash": hex_bytes(self.call("getblockhash", height), 32)}

    def block(self, height):
        integer(height)
        before = hex_bytes(self.call("getblockhash", height), 32)
        tree = self.call("z_gettreestate", before)
        record = {"height": height, "hash": before, "pools": {}}
        if integer(tree.get("height")) != height or hex_bytes(tree.get("hash"), 32) != before:
            raise Unavailable("tree state identity mismatch")
        # The bootstrap gate is above all three pools' activation heights.
        for pool in POOLS:
            try:
                commitments = tree[pool]["commitments"]
                record["pools"][pool] = {
                    "root": hex_bytes(commitments["finalRoot"], 32),
                    "frontier": hex_bytes(commitments["finalState"]),
                }
            except (KeyError, TypeError):
                raise Unavailable(f"missing activated pool: {pool}") from None
        if hex_bytes(self.call("getblockhash", height), 32) != before:
            raise Unavailable("chain changed during read")
        return record


def canonical_record(record, height):
    if integer(record.get("height")) != height:
        raise Unavailable("wrong height returned")
    result = {"height": height, "hash": hex_bytes(record.get("hash"), 32), "pools": {}}
    try:
        for pool in POOLS:
            data = record["pools"][pool]
            result["pools"][pool] = {"root": hex_bytes(data["root"], 32),
                                     "frontier": hex_bytes(data["frontier"])}
    except (KeyError, TypeError):
        raise Unavailable("missing activated pool") from None
    return result
