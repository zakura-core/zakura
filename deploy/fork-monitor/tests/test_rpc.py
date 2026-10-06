"""Tests for the JSON-RPC client, the RPC collector and backfill, against a local stub node."""

from __future__ import annotations

import asyncio
import itertools
import json
import random
import socket
import struct
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from unittest import mock

from zakura_fork_monitor import consensus
from zakura_fork_monitor import rpc as rpc_mod
from zakura_fork_monitor.chain import Chain
from zakura_fork_monitor.consensus import (
    TESTNET,
    ZAKURA_MARKER,
    BlockHeader,
    bits_to_target,
    check_pow,
    expected_bits,
    parse_block,
    sha256d,
    write_compact_size,
)
from zakura_fork_monitor.rpc import (
    BackfillResult,
    ResponseTooLarge,
    RpcClient,
    RpcCollector,
    RpcError,
    RpcTransportError,
    backfill,
    parse_peer,
)
from zakura_fork_monitor.store import Store

FIXTURES = Path(__file__).resolve().parent / "fixtures"
BASE = 100
T0 = 1_790_000_000
POW_LIMIT_BITS = 0x2007FFFF
# P2SH script of Testnet's funding stream t2HifwjUj9uyxr9bknR8LFuQbc98c3vkXtu.
P2SH_FS = bytes.fromhex("a9147a86d6c7eb12ce0aa309d7391a6f338eba3c242b87")


def setUpModule() -> None:
    """Synthetic blocks carry no real Equihash solution, so only their targets are checked."""
    patcher = mock.patch.object(consensus, "check_equihash", return_value=True)
    patcher.start()
    unittest.addModuleCleanup(patcher.stop)


def make_block(
    prev_hash: str, height: int, time: int, tag: bytes = b"zkcodexcoder", *, bits: int = POW_LIMIT_BITS,
    grind: bool = True, fs_value: int | None = None,
) -> str:
    """Build a raw v4 block (hex) with a BIP34 height and a Zakura-marked miner tag in its coinbase.

    The nonce is ground until the header meets its target (about 32 tries at the PoW limit)
    unless `grind` is False, which leaves a block that almost surely fails `check_pow`. The
    coinbase pays a P2PKH miner, or Testnet's funding stream `fs_value` zatoshis when given.
    """
    prefix = (
        struct.pack("<I", 4)
        + bytes.fromhex(prev_hash)[::-1]
        + bytes(64)  # merkle root, block commitments
        + struct.pack("<II", time, bits)
    )
    solution = b"\xfd\x40\x05" + bytes(1344)
    target = bits_to_target(bits)
    for attempt in itertools.count():
        header = prefix + (height | attempt << 64).to_bytes(32, "little") + solution  # nonce
        if not grind or int.from_bytes(sha256d(header), "little") <= target:
            break
    height_push = height.to_bytes((height.bit_length() + 8) // 8, "little")
    marker_push = ZAKURA_MARKER + tag
    script_sig = bytes((len(height_push),)) + height_push + bytes((len(marker_push),)) + marker_push
    if fs_value is None:
        output = struct.pack("<q", 250_000_000) + b"\x19\x76\xa9\x14" + bytes(20) + b"\x88\xac"
    else:
        output = struct.pack("<q", fs_value) + b"\x17" + P2SH_FS
    coinbase = (
        struct.pack("<II", (1 << 31) | 4, 0x892F2085)
        + b"\x01"
        + bytes(32)
        + b"\xff\xff\xff\xff"
        + write_compact_size(len(script_sig))
        + script_sig
        + b"\xff\xff\xff\xff"
        + b"\x01"
        + output
        + bytes(8 + 8)  # lock time, expiry height, value balance
        + b"\x00\x00\x00"  # no spends, outputs or joinsplits
    )
    return (header + b"\x01" + coinbase).hex()


def block_hash(raw_hex: str) -> str:
    """Return the display hash of a raw block."""
    return sha256d(bytes.fromhex(raw_hex)[:1487])[::-1].hex()


class StubNode:
    """State of a fake Zakura node: a best chain, side blocks, chain tips and peers."""

    def __init__(self, best: list[str], base: int = BASE, side: list[str] = ()) -> None:
        """Serve `best` (raw hex, one per height from `base`) plus side-chain blocks."""
        self.base = base
        self.raw = {block_hash(raw): raw for raw in [*best, *side]}
        self.best = [block_hash(raw) for raw in best]
        self.chaintips: list[Any] = []
        self.peers: list[Any] = []
        self.networkinfo = {"subversion": "/Zakura:1.5.0-rc0/", "protocolversion": 170190}
        self.missing_methods: set[str] = set()
        self.unserved: set[str] = set()
        self.height_override: dict[int, str] = {}
        self.best_override: str | None = None
        self.fail = 0
        self.delay = 0.0
        self.status = 200
        self.raw_reply: bytes | None = None
        self.omit_length = False
        self.redirect = False
        self.shuffle = False
        self.lock = threading.Lock()
        self.requests: list[list[str]] = []
        self.headers: list[dict[str, str]] = []
        self.payloads: list[Any] = []

    def set_best(self, raws: list[str]) -> None:
        """Replace the best chain (a reorg); old blocks stay known but are no longer served by hash."""
        for raw in raws:
            self.raw[block_hash(raw)] = raw
        self.best = [block_hash(raw) for raw in raws]

    def methods(self) -> list[str]:
        """Flatten every method called so far."""
        with self.lock:
            return [method for request in self.requests for method in request]

    def answer(self, call: Any) -> dict[str, Any]:
        """Answer one JSON-RPC call object."""
        if not isinstance(call, dict) or call.get("jsonrpc") != "2.0":
            return {"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "Invalid request"}}
        method, params, ident = call.get("method"), call.get("params", []), call.get("id")
        try:
            result = self.dispatch(method, params)
        except RpcError as err:
            return {"jsonrpc": "2.0", "id": ident, "error": {"code": err.code, "message": err.message}}
        return {"jsonrpc": "2.0", "id": ident, "result": result}

    def dispatch(self, method: str, params: list[Any]) -> Any:
        """Implement the subset of the Zakura RPC surface the monitor uses."""
        if method in self.missing_methods:
            raise RpcError(-32601, "Method not found")
        if method == "getbestblockhash":
            return self.best_override or self.best[-1]
        if method == "getblockcount":
            return self.base + len(self.best) - 1
        if method == "getblockhash":
            index = params[0] - self.base
            if not 0 <= index < len(self.best):
                raise RpcError(-1, "Provided index is greater than the current tip")
            return self.best[index]
        if method == "getblock":
            ident = params[0]
            if len(ident) == 64:
                # Zakura only serves best-chain blocks over RPC.
                if ident not in self.best or ident in self.unserved:
                    raise RpcError(-8, "Block not found")
                return self.raw[ident]
            height = int(ident)
            if height in self.height_override:
                return self.raw[self.height_override[height]]
            index = height - self.base
            if not 0 <= index < len(self.best):
                raise RpcError(-8, "Block not found")
            return self.raw[self.best[index]]
        if method == "getchaintips":
            return self.chaintips
        if method == "getpeerinfo":
            return self.peers
        if method == "getnetworkinfo":
            return self.networkinfo
        raise RpcError(-32601, "Method not found")


class _Handler(BaseHTTPRequestHandler):
    """HTTP front end for a StubNode."""

    def do_POST(self) -> None:
        """Serve one JSON-RPC request or batch."""
        node: StubNode = self.server.node
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        payload = json.loads(body)
        calls = payload if isinstance(payload, list) else [payload]
        with node.lock:
            node.requests.append([call.get("method") for call in calls if isinstance(call, dict)])
            node.headers.append(dict(self.headers))
            node.payloads.append(payload)
            failing = node.fail > 0
            node.fail -= failing
        if node.delay:
            time.sleep(node.delay)
        if node.redirect:
            self.send_response(302)
            self.send_header("Location", "http://127.0.0.1:1/")
            self.end_headers()
            return
        if failing:
            self._send(500, b"internal error", "text/plain")
            return
        if node.raw_reply is not None:
            self._send(node.status, node.raw_reply)
            return
        if isinstance(payload, list):
            reply: Any = [node.answer(call) for call in payload]
            if node.shuffle:
                random.Random(7).shuffle(reply)
        else:
            reply = node.answer(payload)
        self._send(node.status, json.dumps(reply).encode())

    def _send(self, status: int, body: bytes, content_type: str = "application/json") -> None:
        """Write a response, optionally without Content-Length (read until close)."""
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        if not self.server.node.omit_length:
            self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        """Keep test output quiet."""


class _QuietServer(ThreadingHTTPServer):
    """ThreadingHTTPServer that ignores clients hanging up early (timeout and size-cap tests)."""

    daemon_threads = True

    def handle_error(self, request: Any, client_address: Any) -> None:
        """Drop the traceback socketserver would print."""


class StubServer:
    """A ThreadingHTTPServer on 127.0.0.1:<free port> serving a StubNode."""

    def __init__(self, node: StubNode) -> None:
        """Start serving `node` in a daemon thread."""
        self.httpd = _QuietServer(("127.0.0.1", 0), _Handler)
        self.httpd.node = node
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}/"
        self.thread = threading.Thread(target=self.httpd.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
        self.thread.start()

    def close(self) -> None:
        """Stop the server."""
        self.httpd.shutdown()
        self.httpd.server_close()


class FakeMonitor:
    """Implements the Monitor surface the RPC module uses, backed by a real Store and Chain."""

    def __init__(self, store: Store, params=TESTNET) -> None:
        """Record every call and mirror ingests into the store and chain."""
        self.store = store
        self.params = params
        self.chain = Chain(params)
        self.config = None
        self.snapshot: dict[str, Any] = {}
        self.ingested: list[tuple[str, int | None, str, str]] = []
        self.tips: list[tuple[str, str, int | None]] = []
        self.requested: list[tuple[str, int | None, str | None]] = []
        self.candidates: list[tuple[str, int, str, str]] = []

    def ingest_block(self, block, header, height, *, source: str, kind: str, at: float) -> None:
        """Store and chain a block, like Monitor.ingest_block."""
        self.ingested.append((header.hash, height, source, kind))
        node = self.chain.add(header, height, block=block, first_seen_at=at)
        self.store.upsert_block(
            block, header, height, miner=node.miner if node else None, is_min_diff=False, seen_at=at, seen_source=source
        )
        self.store.record_sighting(header.hash, source, kind, at)

    def observe_tip(self, source: str, tip_hash: str, at: float, height_hint: int | None = None) -> None:
        """Record a tip observation."""
        self.tips.append((source, tip_hash, height_hint))

    def request_block(self, hash: str, height_hint: int | None = None, prefer_host: str | None = None) -> None:
        """Record a P2P fetch request."""
        self.requested.append((hash, height_hint, prefer_host))

    def add_peer_candidates(self, candidates) -> None:
        """Record peer candidates."""
        self.candidates.extend(candidates)


def build_chain(count: int = 5) -> list[str]:
    """Build `count` linked raw blocks at heights BASE..BASE+count-1."""
    raws = []
    prev = "ab" * 32
    for offset in range(count):
        raw = make_block(prev, BASE + offset, T0 + 60 * offset)
        raws.append(raw)
        prev = block_hash(raw)
    return raws


class StubTestCase(unittest.IsolatedAsyncioTestCase):
    """A stub node with best chain G A B C D (heights 100-104) and side block S on B."""

    def setUp(self) -> None:
        """Start the stub server and open a store."""
        self.raws = build_chain()
        self.hashes = [block_hash(raw) for raw in self.raws]
        self.side_raw = make_block(self.hashes[2], BASE + 3, T0 + 185, tag=b"Foundry")
        self.side = block_hash(self.side_raw)
        self.node = StubNode(self.raws, side=[self.side_raw])
        self.server = StubServer(self.node)
        self.client = RpcClient(self.server.url, timeout=5)
        self._tmp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self._tmp.name) / "monitor.sqlite3", commit_interval=0)
        self.monitor = FakeMonitor(self.store)

    def tearDown(self) -> None:
        """Stop the server and remove the store."""
        self.server.close()
        self.store.close()
        self._tmp.cleanup()

    def preload(self, *indexes: int) -> None:
        """Ingest best-chain blocks by index, as if seen earlier."""
        for index in indexes:
            block = parse_block(bytes.fromhex(self.raws[index]), TESTNET)
            self.monitor.ingest_block(block, block.header, BASE + index, source="test", kind="test", at=1.0)
        self.monitor.ingested.clear()

    def collector(self, **kwargs: Any) -> RpcCollector:
        """Build a collector named n1 against the stub."""
        return RpcCollector("n1", self.client, self.monitor, **kwargs)


class RpcClientTests(StubTestCase):
    """RpcClient framing, errors and bounds."""

    def test_call_sends_jsonrpc2_with_user_agent(self) -> None:
        """A call carries JSON-RPC 2.0 framing, a JSON content type and our User-Agent."""
        self.assertEqual(self.client.call("getblockcount"), 104)
        payload = self.node.payloads[-1]
        self.assertEqual(payload["jsonrpc"], "2.0")
        self.assertEqual(payload["method"], "getblockcount")
        headers = {key.lower(): value for key, value in self.node.headers[-1].items()}
        self.assertTrue(headers["user-agent"].startswith("zakura-fork-monitor/"))
        self.assertEqual(headers["content-type"], "application/json")

    def test_error_reply_raises_rpc_error_with_code(self) -> None:
        """JSON-RPC error objects become RpcError with the server's code."""
        with self.assertRaises(RpcError) as ctx:
            self.client.call("getblock", self.side, 0)
        self.assertEqual(ctx.exception.code, -8)
        self.assertNotIsInstance(ctx.exception, RpcTransportError)
        with self.assertRaises(RpcError) as ctx:
            self.client.call("nosuchmethod")
        self.assertEqual(ctx.exception.code, -32601)

    def test_null_error_with_result_is_success(self) -> None:
        """JSON-RPC 1.0 style replies carry `"error": null` next to the result."""
        self.node.raw_reply = b'{"id": 0, "result": 7, "error": null}'
        self.assertEqual(self.client.call("getblockcount"), 7)

    def test_batch_returns_results_in_call_order_with_item_errors(self) -> None:
        """Batch replies are matched by id; a failed item is returned as an RpcError."""
        self.node.shuffle = True
        calls = [("getblockhash", [BASE + i]) for i in range(5)] + [("getblockhash", [999])]
        results = self.client.batch(calls)
        self.assertEqual(results[:5], self.hashes)
        self.assertIsInstance(results[5], RpcError)
        self.assertEqual(results[5].code, -1)
        self.assertTrue(all(item["jsonrpc"] == "2.0" for item in self.node.payloads[-1]))
        self.assertEqual(self.client.batch([]), [])

    def test_batch_missing_replies_become_errors(self) -> None:
        """Items the server leaves out are reported as transport errors, not dropped."""
        self.node.raw_reply = json.dumps([{"jsonrpc": "2.0", "id": 1, "result": "x"}, {"id": "junk"}, 5]).encode()
        first, second = self.client.batch([("a", []), ("b", [])])
        self.assertIsInstance(first, RpcTransportError)
        self.assertEqual(second, "x")

    def test_batch_is_bounded(self) -> None:
        """Oversized batches are refused before anything is sent."""
        with self.assertRaises(ValueError):
            self.client.batch([("getblockcount", [])] * (rpc_mod.MAX_BATCH_CALLS + 1))

    def test_response_size_cap(self) -> None:
        """Replies over the cap fail, with or without a Content-Length header."""
        small = RpcClient(self.server.url, max_response_bytes=500)
        with self.assertRaises(ResponseTooLarge):
            small.call("getblock", self.hashes[0], 0)
        self.node.omit_length = True
        with self.assertRaises(ResponseTooLarge):
            small.call("getblock", self.hashes[0], 0)
        self.assertEqual(small.call("getblockcount"), 104)

    def test_http_errors(self) -> None:
        """A 5xx with a JSON-RPC error body keeps its code; other failures are transport errors."""
        self.node.status = 500
        self.node.raw_reply = b'{"jsonrpc":"2.0","id":0,"error":{"code":-28,"message":"warming up"}}'
        with self.assertRaises(RpcError) as ctx:
            self.client.call("getblockcount")
        self.assertEqual(ctx.exception.code, -28)
        self.node.raw_reply = b"<html>bad gateway</html>"
        with self.assertRaises(RpcTransportError):
            self.client.call("getblockcount")

    def test_invalid_json_and_wrong_shape(self) -> None:
        """Garbage and non-object replies are transport errors."""
        self.node.raw_reply = b"not json"
        with self.assertRaises(RpcTransportError):
            self.client.call("getblockcount")
        self.node.raw_reply = b"[1, 2]"
        with self.assertRaises(RpcTransportError):
            self.client.call("getblockcount")
        self.node.raw_reply = b'{"jsonrpc":"2.0","id":0}'
        with self.assertRaises(RpcTransportError):
            self.client.call("getblockcount")

    def test_redirects_are_not_followed(self) -> None:
        """A 3xx is an error rather than a silent POST-to-GET redirect."""
        self.node.redirect = True
        with self.assertRaises(RpcTransportError) as ctx:
            self.client.call("getblockcount")
        self.assertIn("302", str(ctx.exception))

    def test_timeout_and_refused_connection(self) -> None:
        """Slow and unreachable endpoints raise RpcTransportError."""
        self.node.delay = 0.5
        with self.assertRaises(RpcTransportError):
            RpcClient(self.server.url, timeout=0.1).call("getblockcount")
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        with self.assertRaises(RpcTransportError):
            RpcClient(f"http://127.0.0.1:{port}/", timeout=1).call("getblockcount")

    def test_url_credentials_become_basic_auth(self) -> None:
        """user:password in the URL is sent as basic auth and kept out of `url`."""
        client = RpcClient(self.server.url.replace("http://", "http://alice:s3cret@"))
        self.assertNotIn("s3cret", client.url)
        self.assertNotIn("s3cret", repr(client))
        client.call("getblockcount")
        headers = {key.lower(): value for key, value in self.node.headers[-1].items()}
        self.assertEqual(headers["authorization"], "Basic YWxpY2U6czNjcmV0")

    def test_error_messages_are_sanitized(self) -> None:
        """Remote error text is capped and stripped of control characters."""
        message = "\x1b[31m" + "x" * 1000
        self.node.raw_reply = json.dumps({"jsonrpc": "2.0", "id": 0, "error": {"code": 1, "message": message}}).encode()
        with self.assertRaises(RpcError) as ctx:
            self.client.call("getblockcount")
        self.assertLessEqual(len(ctx.exception.message), rpc_mod.MAX_ERROR_TEXT)
        self.assertNotIn("\x1b", ctx.exception.message)


class HelperTests(unittest.TestCase):
    """Pure helpers."""

    def test_parse_peer(self) -> None:
        """getpeerinfo entries become (ip, port, subver); inbound peers get the default port."""
        self.assertEqual(
            parse_peer({"addr": "2.28.67.187:18233", "subver": "/Zebra:6.3.0/", "inbound": False}, 18233),
            ("2.28.67.187", 18233, "/Zebra:6.3.0/"),
        )
        self.assertEqual(parse_peer({"addr": "2.28.96.13:35578", "inbound": True}, 18233), ("2.28.96.13", 18233, ""))
        self.assertEqual(parse_peer({"addr": "[2001:db8::1]:18344"}, 18233), ("2001:db8::1", 18344, ""))
        for bad in ({"addr": "127.0.0.1:18233"}, {"addr": "0.0.0.0:1"}, {"addr": "host.example:1"}, {"addr": 5}, "x"):
            self.assertIsNone(parse_peer(bad, 18233), bad)
        _, _, subver = parse_peer({"addr": "1.2.3.4:5", "subver": "/Evil\n\x00:1/" + "a" * 999}, 18233)
        self.assertEqual(len(subver), rpc_mod.MAX_SUBVER)
        self.assertTrue(subver.isprintable())

    def test_backoff_delay(self) -> None:
        """Backoff doubles from the minimum and is capped."""
        self.assertEqual([rpc_mod._backoff_delay(n) for n in range(8)], [0, 1, 2, 4, 8, 16, 30, 30])
        self.assertEqual(rpc_mod._backoff_delay(10_000), 30)


class CollectorTests(StubTestCase):
    """RpcCollector steps against the stub node."""

    async def test_new_tip_is_ingested_with_missing_ancestors(self) -> None:
        """A new tip is ingested, its missing parents walked back, then observed."""
        self.preload(0, 1, 2)
        collector = self.collector()
        self.assertTrue(await collector.poll_tip())
        self.assertEqual(
            self.monitor.ingested,
            [(self.hashes[4], 104, "rpc:n1", "rpc_tip"), (self.hashes[3], 103, "rpc:n1", "rpc_walk")],
        )
        self.assertEqual(self.monitor.tips, [("rpc:n1", self.hashes[4], 104)])
        self.assertEqual(self.monitor.chain.best_tip().hash, self.hashes[4])
        self.assertEqual(self.store.get_block(self.hashes[4])["miner"], "zkcodexcoder · tm9iML…r7Ma")
        self.assertFalse(await collector.poll_tip())
        self.assertEqual(len(self.monitor.tips), 1)
        self.assertEqual(collector.health()["tip_height"], 104)

    async def test_reorg_to_a_side_branch(self) -> None:
        """When the node switches to a sibling branch only the new block is fetched."""
        self.preload(0, 1, 2, 3, 4)
        collector = self.collector()
        await collector.poll_tip()
        self.monitor.ingested.clear()
        self.node.set_best([*self.raws[:3], self.side_raw])
        self.assertTrue(await collector.poll_tip())
        self.assertEqual(self.monitor.ingested, [(self.side, 103, "rpc:n1", "rpc_tip")])
        self.assertEqual(self.monitor.tips[-1], ("rpc:n1", self.side, 103))

    async def test_walk_back_is_bounded(self) -> None:
        """On an empty chain the walk stops after walk_limit ancestors."""
        collector = self.collector(walk_limit=2)
        await collector.poll_tip()
        self.assertEqual([entry[0] for entry in self.monitor.ingested], self.hashes[4:1:-1])

    async def test_walk_back_uses_growing_height_batches(self) -> None:
        """An unbounded walk to the bottom of the node's chain batches by height, then stops cleanly."""
        collector = self.collector()
        await collector.poll_tip()
        self.assertEqual([entry[0] for entry in self.monitor.ingested], self.hashes[::-1])
        batch_sizes = [len(request) for request in self.node.requests if request and request[0] == "getblock"]
        self.assertEqual(batch_sizes[:3], [1, 1, 2])  # tip by hash, then heights 103, then 102-101

    async def test_walk_back_falls_back_to_hash_on_mismatch(self) -> None:
        """If a height resolves to another branch, the expected parent is fetched by hash."""
        self.preload(0, 1, 2)
        self.node.height_override[BASE + 3] = self.side
        await self.collector().poll_tip()
        self.assertEqual([entry[0] for entry in self.monitor.ingested], [self.hashes[4], self.hashes[3]])
        self.assertEqual(self.node.payloads[-1]["params"], [self.hashes[3], 0])

    async def test_walk_back_bridges_a_gap_below_detached_blocks(self) -> None:
        """Detached blocks the chain holds are walked through, so the tip fills the gap under them and attaches."""
        self.preload(0, 1)
        block = parse_block(bytes.fromhex(self.raws[3]), TESTNET)
        self.monitor.chain.add(block.header, BASE + 3, block=block)
        self.assertIsNone(self.monitor.chain.get(self.hashes[3]).cumwork)
        await self.collector().poll_tip()
        self.assertEqual([entry[:2] for entry in self.monitor.ingested],
                         [(self.hashes[4], BASE + 4), (self.hashes[2], BASE + 2)])
        self.assertEqual(self.monitor.chain.best_tip().hash, self.hashes[4])

    async def test_blocks_failing_proof_of_work_are_rejected(self) -> None:
        """A tip claiming huge work without meeting its target is refused and leaves the chain untouched."""
        self.assertTrue(all(check_pow(parse_block(bytes.fromhex(raw), TESTNET).header, TESTNET) for raw in self.raws))
        self.preload(0, 1, 2, 3, 4)
        collector = self.collector()
        await collector.poll_tip()
        self.monitor.ingested.clear()
        forged = make_block(self.hashes[4], BASE + 5, T0 + 300, bits=0x03000001, grind=False)
        self.node.set_best([*self.raws, forged])
        with self.assertRaisesRegex(ValueError, "fails proof of work"):
            await collector.poll_tip()
        self.assertEqual(self.monitor.ingested, [])
        self.assertEqual(self.monitor.chain.best_tip().hash, self.hashes[4])
        self.assertIsNone(self.store.get_block(block_hash(forged)))
        self.assertEqual(len(self.monitor.tips), 1)

    async def test_tip_that_vanished_is_retried_later(self) -> None:
        """A tip the node no longer serves (reorged away) is skipped without error."""
        self.node.best_override = self.side
        collector = self.collector()
        self.assertFalse(await collector.poll_tip())
        self.assertEqual(self.monitor.ingested, [])
        self.node.best_override = None
        self.assertTrue(await collector.poll_tip())

    async def test_chaintips_are_recorded_and_unknown_forks_requested(self) -> None:
        """Chain tips land in the store; unknown valid-fork tips are requested over P2P once per cooldown."""
        self.preload(0, 1, 2, 3, 4)
        stale = "fe" * 32
        self.node.chaintips = [
            {"height": 104, "hash": self.hashes[4], "branchlen": 0, "status": "active"},
            {"height": 103, "hash": self.side, "branchlen": 1, "status": "valid-fork"},
            {"height": 103, "hash": stale, "branchlen": 1, "status": "invalid"},
            {"height": 90, "hash": self.hashes[2], "branchlen": 1, "status": "valid-fork"},  # known: not requested
            {"height": "x", "hash": "nothex"},
            "junk",
        ]
        collector = self.collector()
        self.assertEqual(await collector.poll_chaintips(), 1)
        self.assertEqual(self.monitor.requested, [(self.side, 103, "127.0.0.1")])
        reader = self.store.reader()
        rows = {row["hash"]: dict(row) for row in reader.execute("SELECT * FROM chaintips WHERE source = 'rpc:n1'")}
        self.assertEqual(set(rows), {self.hashes[4], self.side, stale, self.hashes[2]})
        self.assertEqual(rows[self.side]["status"], "valid-fork")
        kinds = reader.execute("SELECT kind FROM sightings WHERE hash = ? AND source = 'rpc:n1'", (stale,))
        self.assertEqual([row[0] for row in kinds], ["chaintip"])
        # The node's own active tip is a timely sighting, like the tip poll's.
        kinds = reader.execute("SELECT kind FROM sightings WHERE hash = ? AND source = 'rpc:n1'", (self.hashes[4],))
        self.assertEqual([row[0] for row in kinds], ["rpc_tip"])
        self.assertEqual(await collector.poll_chaintips(), 0)
        last_at = reader.execute("SELECT last_at FROM chaintips WHERE hash = ?", (self.side,)).fetchone()[0]
        self.assertEqual(last_at, rows[self.side]["last_at"])

    def test_fork_tip_requests_are_rate_limited(self) -> None:
        """An unknown fork tip is re-requested after REQUEST_RETRY, at most REQUEST_ATTEMPTS times."""
        collector = self.collector()
        at, allowed = 1_000.0, []
        for _ in range(4 * rpc_mod.REQUEST_ATTEMPTS):
            allowed.append(collector._should_request(self.side, at))
            at += rpc_mod.REQUEST_RETRY / 2
        self.assertEqual(allowed.count(True), rpc_mod.REQUEST_ATTEMPTS)
        self.assertEqual(allowed[:3], [True, False, True])

    async def test_deep_chaintips_are_ignored(self) -> None:
        """Tips far below the best one (zcashd lists every tip ever) are neither stored nor requested."""
        self.node.chaintips = [
            {"height": 10_000, "hash": self.hashes[4], "branchlen": 0, "status": "active"},
            {"height": 9_999 - rpc_mod.MAX_CHAINTIP_DEPTH, "hash": self.side, "branchlen": 1, "status": "valid-fork"},
        ]
        await self.collector().poll_chaintips()
        self.assertEqual(self.monitor.requested, [])
        count = self.store.reader().execute("SELECT COUNT(*) FROM chaintips").fetchone()[0]
        self.assertEqual(count, 1)

    async def test_peers_become_candidates_and_version_is_recorded(self) -> None:
        """getpeerinfo feeds deduplicated candidates; getnetworkinfo sets the source's impl and version."""
        self.node.peers = [
            {"addr": "2.28.67.187:18233", "subver": "/Zebra:6.3.0/", "inbound": False},
            {"addr": "2.28.96.13:35578", "subver": "/Zebra:6.4.2/", "inbound": True},
            {"addr": "2.28.96.13:41111", "subver": "/Zebra:6.4.2/", "inbound": True},
            {"addr": "9.9.9.9:18233", "subver": "/fork-monitor-probe:0.1/", "inbound": True},
            {"addr": "garbage"},
        ]
        collector = self.collector()
        self.assertEqual(await collector.poll_peers(), 2)
        self.assertEqual(
            self.monitor.candidates,
            [
                ("2.28.67.187", 18233, "/Zebra:6.3.0/", "getpeerinfo:n1"),
                ("2.28.96.13", 18233, "/Zebra:6.4.2/", "getpeerinfo:n1"),
            ],
        )
        source = {row["source"]: row for row in self.store.get_sources()}["rpc:n1"]
        self.assertEqual((source["impl"], source["impl_version"]), ("zakura", "1.5.0-rc0"))
        self.assertEqual(source["user_agent"], "/Zakura:1.5.0-rc0/")
        self.assertEqual(source["protocol_version"], 170190)

    async def test_unsupported_getpeerinfo_is_skipped(self) -> None:
        """An endpoint without getpeerinfo still reports its version, and is not asked again."""
        self.node.missing_methods = {"getpeerinfo"}
        collector = self.collector()
        self.assertEqual(await collector.poll_peers(), 0)
        await collector.poll_peers()
        self.assertEqual(self.node.requests[-1], ["getnetworkinfo"])

    async def test_run_backs_off_and_recovers(self) -> None:
        """Failures mark the source "error" and back off; recovery marks it "ok" and ingests the tip."""
        self.preload(0, 1, 2, 3)
        self.node.fail = 3
        collector = self.collector(interval=0.05, chaintips_interval=0.05, backoff_min=0.05, backoff_max=0.1)
        statuses = []
        original = self.store.upsert_source

        def spy(source: str, **fields: Any) -> None:
            """Record status transitions."""
            if "status" in fields:
                statuses.append(fields["status"])
            original(source, **fields)

        with mock.patch.object(self.store, "upsert_source", spy), self.assertLogs(rpc_mod.log, "WARNING") as logs:
            task = asyncio.create_task(collector.run())
            for _ in range(100):
                await asyncio.sleep(0.05)
                if self.monitor.tips:
                    break
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(len(logs.records), 3)
        self.assertEqual(statuses[:3], ["error", "error", "error"])
        self.assertEqual(statuses[3], "ok")
        self.assertEqual(self.monitor.tips[0][1], self.hashes[4])
        health = collector.health()
        self.assertEqual((health["status"], health["consecutive_errors"]), ("ok", 0))
        self.assertIn("HTTP 500", health["last_error"])
        source = {row["source"]: row for row in self.store.get_sources()}["rpc:n1"]
        self.assertEqual((source["kind"], source["status"], source["discovered_via"]), ("rpc", "ok", "config"))

    async def test_run_disables_unsupported_duties(self) -> None:
        """A -32601 for getchaintips (Zebra) disables that duty without marking the endpoint failed."""
        self.node.missing_methods = {"getchaintips"}
        collector = self.collector(interval=0.05, chaintips_interval=0.05)
        task = asyncio.create_task(collector.run())
        await asyncio.sleep(0.3)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        health = collector.health()
        self.assertEqual(health["disabled"], ["chaintips"])
        self.assertEqual(health["status"], "ok")
        self.assertEqual(self.node.methods().count("getchaintips"), 1)

    def test_from_endpoint(self) -> None:
        """A configured endpoint maps onto the collector and its client."""
        endpoint = rpc_mod.RpcEndpoint(name="as", url=self.server.url, interval=2.0, peerinfo_interval=60.0, timeout=3)
        collector = RpcCollector.from_endpoint(endpoint, self.monitor)
        self.assertEqual((collector.source, collector.client.timeout), ("rpc:as", 3))
        self.assertEqual([duty.interval for duty in collector._duties], [2.0, 3.0, 60.0])


class Nu7RulesTests(StubTestCase):
    """Blocks at NU7 heights must carry the NU7 difficulty when the chain holds their ancestry."""

    def setUp(self) -> None:
        """Hold the 113 blocks below NU7 activation (PoW-limit nBits, 25 s apart) in the chain."""
        super().setUp()
        self.nu7 = TESTNET.nu7_height
        context: list[tuple[int, int]] = []
        prev = "ab" * 32
        for height in range(self.nu7 - TESTNET.max_context_len, self.nu7):
            time_ = T0 + 25 * (height - self.nu7)
            name = sha256d(height.to_bytes(4, "little"))[::-1].hex()
            header = BlockHeader(hash=name, prev_hash=prev, version=4, merkle_root="00" * 32, time=time_,
                                 bits=POW_LIMIT_BITS, nonce="00" * 32, raw=b"")
            self.monitor.chain.add(header, height)
            context.insert(0, (POW_LIMIT_BITS, time_))
            prev = name
        self.anchor = prev
        self.bits = expected_bits(TESTNET, self.nu7, T0, context)  # just below the PoW limit

    def serve(self, *blocks: tuple[str, int], fs_value: int | None = None) -> list[str]:
        """Serve a best chain from NU7 on the held blocks, given as (tag, nBits) per block; return the hashes.

        `fs_value` makes every coinbase pay the funding stream that amount (see `make_block`).
        """
        raws, prev = [], self.anchor
        for offset, (tag, bits) in enumerate(blocks):
            raws.append(make_block(prev, self.nu7 + offset, T0 + 25 * offset, tag.encode(), bits=bits,
                                   fs_value=fs_value))
            prev = block_hash(raws[-1])
        self.server.httpd.node = StubNode(raws, base=self.nu7)
        return [block_hash(raw) for raw in raws]

    async def test_tip_with_the_nu7_difficulty_is_ingested(self) -> None:
        """A tip whose nBits matches the prediction from its held ancestors is accepted."""
        [tip] = self.serve(("ok", self.bits))
        self.assertTrue(await self.collector().poll_tip())
        self.assertEqual(self.monitor.chain.best_tip().hash, tip)

    async def test_tip_with_another_difficulty_is_rejected(self) -> None:
        """A tip from a node still on pre-NU7 rules fails the poll and never reaches the chain or the store."""
        [tip] = self.serve(("old", 0x2003FFFF))
        with self.assertRaisesRegex(ValueError, "wrong difficulty under NU7 rules"):
            await self.collector().poll_tip()
        self.assertEqual((self.monitor.ingested, self.monitor.tips), ([], []))
        self.assertIsNone(self.store.get_block(tip))

    async def test_tip_with_a_pre_nu7_coinbase_is_rejected(self) -> None:
        """A tip with the NU7 difficulty but the pre-NU7 funding-stream amount is an old-rules block."""
        self.serve(("ok", self.bits), fs_value=TESTNET.pre_nu7_funding_stream_value)
        with self.assertRaisesRegex(ValueError, "pre-NU7 coinbase"):
            await self.collector().poll_tip()
        self.assertEqual((self.monitor.ingested, self.monitor.tips), ([], []))
        [current] = self.serve(("ok", self.bits), fs_value=TESTNET.pre_nu7_funding_stream_value - 1)
        self.assertTrue(await self.collector().poll_tip())
        self.assertEqual(self.monitor.chain.best_tip().hash, current)

    async def test_walk_back_checks_the_block_that_links_to_the_chain(self) -> None:
        """The walked-back ancestor whose parent the chain holds is checked before it is ingested."""
        old, tip = self.serve(("old", 0x2003FFFF), ("tip", POW_LIMIT_BITS))
        with self.assertRaisesRegex(ValueError, "wrong difficulty"):
            await self.collector().poll_tip()
        self.assertEqual([entry[0] for entry in self.monitor.ingested], [tip])  # unchecked: no ancestry yet
        self.assertNotIn(old, self.monitor.chain)
        self.assertEqual(self.monitor.tips, [])

    async def test_backfill_skips_rejected_blocks_and_their_descendants(self) -> None:
        """A backfilled block with the wrong difficulty, and every block above it, counts as failed."""
        good, _, _ = self.serve(("ok", self.bits), ("old", 0x2003FFFF), ("above", POW_LIMIT_BITS))
        result = await backfill(self.client, self.monitor, 3)
        self.assertEqual((result.fetched, result.failed), (1, 2))
        self.assertEqual([entry[0] for entry in self.monitor.ingested], [good])


class BackfillTests(StubTestCase):
    """backfill() against the stub node."""

    async def test_backfill_ingests_oldest_first(self) -> None:
        """Every missing canonical block is fetched and ingested in ascending height order."""
        result = await backfill(self.client, self.monitor, 5, name="n1", batch_size=2, concurrency=2)
        self.assertIsInstance(result, BackfillResult)
        counts = (result.tip_height, result.requested, result.skipped, result.fetched, result.failed)
        self.assertEqual(counts, (104, 5, 0, 5, 0))
        expected = [(block, BASE + i, "rpc:n1", "backfill") for i, block in enumerate(self.hashes)]
        self.assertEqual(self.monitor.ingested, expected)
        self.assertEqual(self.monitor.chain.best_tip().hash, self.hashes[4])
        block_requests = [request for request in self.node.requests if request and request[0] == "getblock"]
        self.assertEqual(sorted(len(request) for request in block_requests), [1, 2, 2])

    async def test_backfill_skips_blocks_already_held(self) -> None:
        """Blocks stored with a body are not fetched again."""
        self.preload(0, 1)
        result = await backfill(self.client, self.monitor, 5)
        self.assertEqual((result.requested, result.skipped, result.fetched), (5, 2, 3))
        self.assertEqual(result.failed, 0)
        self.assertTrue(all(entry[2] == "rpc:127.0.0.1" for entry in self.monitor.ingested))
        again = await backfill(self.client, self.monitor, 5)
        self.assertEqual((again.skipped, again.fetched), (5, 0))
        self.assertEqual((await backfill(self.client, self.monitor, 0)).requested, 0)

    async def test_backfill_splits_oversized_batches(self) -> None:
        """A batch whose reply exceeds the size cap is retried in halves."""
        one_block = len(json.dumps({"jsonrpc": "2.0", "id": 0, "result": self.raws[0]}))
        client = RpcClient(self.server.url, max_response_bytes=one_block * 2 - 10)
        result = await backfill(client, self.monitor, 5, batch_size=4)
        self.assertEqual((result.fetched, result.failed), (5, 0))
        tiny = RpcClient(self.server.url, max_response_bytes=one_block // 2)
        self.assertEqual((await backfill(tiny, FakeMonitor(self.store), 5)).failed, 0)  # all stored: none fetched
        self.store.close()
        self.store = Store(Path(self._tmp.name) / "other.sqlite3", commit_interval=0)
        result = await backfill(tiny, FakeMonitor(self.store), 5)
        self.assertEqual((result.fetched, result.failed), (0, 5))  # a single block over the cap fails alone

    async def test_backfill_counts_unserved_blocks(self) -> None:
        """A block the node will not serve is counted as failed and the rest still land."""
        self.node.unserved = {self.hashes[2]}
        result = await backfill(self.client, self.monitor, 5)
        self.assertEqual((result.fetched, result.failed), (4, 1))

    async def test_backfill_rejects_blocks_failing_proof_of_work(self) -> None:
        """A served block that fails its own target is counted as failed, never ingested."""
        forged = make_block(self.hashes[3], BASE + 4, T0 + 240, bits=0x03000001, grind=False)
        self.node.set_best([*self.raws[:4], forged])
        result = await backfill(self.client, self.monitor, 5)
        self.assertEqual((result.fetched, result.failed), (4, 1))
        self.assertNotIn(block_hash(forged), self.monitor.chain)

    async def test_backfill_retries_then_raises(self) -> None:
        """Transient failures are retried; a dead endpoint raises after BACKFILL_ATTEMPTS."""
        with mock.patch.object(rpc_mod, "BACKFILL_RETRY_DELAY", 0.0):
            self.node.fail = rpc_mod.BACKFILL_ATTEMPTS - 1
            self.assertEqual((await backfill(self.client, self.monitor, 5)).fetched, 5)
            self.node.fail = 100
            with self.assertRaises(RpcTransportError):
                await backfill(self.client, self.monitor, 5)

    async def test_backfill_real_blocks(self) -> None:
        """Real v6 testnet blocks from the fixtures parse and carry their miners through the backfill."""
        raws = [(FIXTURES / f"block-test-{h}.hex").read_text().strip() for h in (4410736, 4410737, 4410738)]
        self.server.httpd.node = StubNode(raws, base=4410736)
        result = await backfill(self.client, self.monitor, 3)
        self.assertEqual(result.fetched, 3)
        miners = [self.store.get_block(entry[0])["miner"] for entry in self.monitor.ingested]
        foundry = "Foundry · tmJggj…vvVu"  # the pool's tag family plus its payout address
        self.assertEqual(miners, [foundry, foundry, "tmDDBnPEg12A4GYACyq9KwUEyq5vMiALZQR"])
        self.assertEqual(self.monitor.chain.best_tip().height, 4410738)


if __name__ == "__main__":
    unittest.main()
