"""Tests for zakura_fork_monitor.p2p against in-process fake peers (no network access)."""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import functools
import ipaddress
import itertools
import json
import random
import struct
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from zakura_fork_monitor import consensus, p2p
from zakura_fork_monitor.chain import Chain
from zakura_fork_monitor.consensus import (
    SOLUTION_SIZE_PREFIX,
    TESTNET,
    ZAKURA_MARKER,
    BlockHeader,
    bits_to_target,
    expected_bits,
    parse_header,
    read_compact_size,
    sha256d,
    target_to_bits,
    write_compact_size,
)
from zakura_fork_monitor.store import Store
from zakura_fork_monitor.wire import (
    INV_BLOCK,
    INV_TX,
    MAX_PAYLOAD,
    PROTOCOL_VERSION,
    USER_AGENT,
    VersionMsg,
    decode_addr,
    decode_headers,
    decode_inv,
    decode_ping,
    decode_version,
    encode_getheaders,
    encode_inv,
    encode_ping,
    encode_version,
    frame,
    parse_frame_header,
)

MAGIC = TESTNET.magic
BITS = TESTNET.pow_limit_bits
BASE = 4_400_000
T0 = 1_790_000_000
# Synthetic blocks come just over 6 spacings apart, so Testnet's minimum-difficulty rule makes
# BITS their expected difficulty.
SPACING = 451
# Real Equihash solutions cannot be mined here; the stub verifier accepts exactly this one.
SOLVED = b"\x01" + bytes(1343)
ROOT = "11" * 32
OURS = 30  # MAIN[:OURS] is preloaded into the monitor's chain
ZEBRA_64, ZEBRA_63, ZAKURA = "/Zebra:6.4.2/", "/Zebra:6.3.0/", "/Zakura:1.5.0/"
# Everything the observer may ever send; replies must additionally be empty or echoes.
ALLOWED_COMMANDS = {
    "version",
    "verack",
    "ping",
    "pong",
    "getheaders",
    "getdata",
    "getaddr",
    "addr",
    "inv",
    "notfound",
    "headers",
}
FAST = p2p.Timings(handshake_timeout=2.0, poll_timeout=3.0, fetch_timeout=2.0)
# Real Testnet headers around NU7 activation; see tests/test_consensus.py for the layout.
NU7_HEADERS = Path(__file__).resolve().parent / "fixtures" / "testnet-nu7-headers-4464896-4465122.json"
REAL_EQUIHASH = consensus.check_equihash  # captured before `setUpModule` stubs it


def stub_equihash(header: BlockHeader) -> bool:
    """Stand-in for `consensus.check_equihash`: only the SOLVED solution is valid."""
    return header.raw[143:] == SOLVED


def setUpModule() -> None:
    """Swap in the stub Equihash verifier for this module's tests."""
    patcher = mock.patch.object(consensus, "check_equihash", stub_equihash)
    patcher.start()
    unittest.addModuleCleanup(patcher.stop)


def mine_header(
    prev_hash: str,
    block_time: int,
    salt: bytes,
    *,
    valid: bool = True,
    bits: int = BITS,
    solution: bytes = SOLVED,
) -> BlockHeader:
    """Grind a header on `prev_hash` whose hash meets (or, with valid=False, misses) its `bits` target."""
    merkle = sha256d(salt)
    target = bits_to_target(bits)
    for nonce in range(1 << 20):
        raw = (
            struct.pack("<I", 4)
            + bytes.fromhex(prev_hash)[::-1]
            + merkle
            + bytes(32)
            + struct.pack("<II", block_time, bits)
            + nonce.to_bytes(32, "little")
            + SOLUTION_SIZE_PREFIX
            + solution
        )
        header, _ = parse_header(raw)
        if (int(header.hash, 16) <= target) == valid:
            return header
    raise AssertionError("no suitable nonce")


def build_chain(prev_hash: str, count: int, salt: str, start: int = T0) -> list[BlockHeader]:
    """Return `count` linked headers on `prev_hash`, SPACING apart from `start`."""
    headers = []
    for index in range(count):
        header = mine_header(prev_hash, start + SPACING * index, f"{salt}{index}".encode())
        headers.append(header)
        prev_hash = header.hash
    return headers


@functools.cache
def main_chain() -> tuple[BlockHeader, ...]:
    """The shared synthetic best chain: MAIN[i] is at height BASE + i."""
    return tuple(build_chain(ROOT, 400, "main"))


def make_block(header: BlockHeader, height: int, tag: bytes = b"zkcodexcoder") -> bytes:
    """Serialize a block: the header plus a v4 coinbase with a BIP34 height, the Zakura marker and a tag."""
    script = b"\x03" + height.to_bytes(3, "little") + b"\x04" + ZAKURA_MARKER + bytes([len(tag)]) + tag
    coinbase = (
        struct.pack("<II", 0x80000004, 0x892F2085)
        + b"\x01"
        + bytes(32)
        + b"\xff\xff\xff\xff"
        + bytes([len(script)])
        + script
        + b"\xff\xff\xff\xff"
        + b"\x00"  # no transparent outputs
        + bytes(8)
    )
    return header.raw + b"\x01" + coinbase


def wire_hash(block_hash: str) -> bytes:
    """Return a display-hex hash in wire byte order."""
    return bytes.fromhex(block_hash)[::-1]


def parse_getheaders(payload: bytes) -> tuple[int, list[str]]:
    """Decode a getheaders/getblocks payload into (version, locator display hashes)."""
    (version,) = struct.unpack_from("<I", payload)
    count, off = read_compact_size(payload, 4)
    hashes = [payload[off + 32 * i : off + 32 * (i + 1)][::-1].hex() for i in range(count)]
    return version, hashes


def encode_addr(entries: list[tuple[str, int]]) -> bytes:
    """Serialize an addr payload (IPv4 or IPv6 entries, NODE_NETWORK services)."""
    parts = [write_compact_size(len(entries))]
    for ip, port in entries:
        addr = ipaddress.ip_address(ip)
        packed = (bytes(10) + b"\xff\xff" + addr.packed) if addr.version == 4 else addr.packed
        parts.append(struct.pack("<IQ", T0, 1) + packed + struct.pack(">H", port))
    return b"".join(parts)


class FakePeer:
    """An in-process legacy-protocol peer that serves a synthetic header chain and block bodies."""

    def __init__(
        self,
        headers,
        *,
        user_agent: str = ZEBRA_64,
        version: int = 170160,
        services: int = 1,
        start_height: int | None = None,
        blocks: dict[str, bytes] | None = None,
        serve_blocks: bool = True,
        silent_when_empty: bool = False,
        drop_getheaders: bool = False,
        addrs: list[tuple[str, int]] = (),
        on_verack=None,
        oversize_version: bool = False,
    ) -> None:
        """Configure the peer; `headers` is its best chain, oldest first."""
        self.headers = list(headers)
        self.user_agent = user_agent
        self.version = version
        self.services = services
        self.start_height = start_height
        self.blocks = dict(blocks or {})
        self.serve_blocks = serve_blocks
        self.silent_when_empty = silent_when_empty
        self.drop_getheaders = drop_getheaders
        self.addrs = list(addrs)
        self.on_verack = on_verack
        self.oversize_version = oversize_version
        self.received: list[tuple[str, bytes]] = []
        self.getheaders: list[tuple[int, list[str]]] = []
        self.connections = 0
        self.eofs = 0
        self.writers: list[asyncio.StreamWriter] = []
        self.server: asyncio.Server | None = None
        self.port = 0

    async def start(self) -> None:
        """Listen on an ephemeral localhost port."""
        self.server = await asyncio.start_server(self._serve, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        """Close every connection and the listener."""
        for writer in self.writers:
            writer.close()
        for writer in self.writers:
            with contextlib.suppress(Exception):
                await writer.wait_closed()
        self.server.close()
        await self.server.wait_closed()

    async def send(self, command: str, payload: bytes = b"") -> None:
        """Send a message on the latest connection."""
        await self._send(self.writers[-1], command, payload)

    def commands(self) -> list[str]:
        """Return the commands received so far, in order."""
        return [command for command, _ in self.received]

    def payloads(self, command: str) -> list[bytes]:
        """Return the payloads of every received `command`."""
        return [payload for name, payload in self.received if name == command]

    async def _send(self, writer: asyncio.StreamWriter, command: str, payload: bytes = b"") -> None:
        """Write one framed message."""
        writer.write(frame(MAGIC, command, payload))
        await writer.drain()

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Handle one connection: send our version first (like Zakura), then answer requests."""
        self.connections += 1
        self.writers.append(writer)
        try:
            if self.oversize_version:
                body = bytes(2000)
                writer.write(
                    MAGIC + b"version".ljust(12, b"\0") + struct.pack("<I", len(body)) + sha256d(body)[:4] + body
                )
            else:
                height = self.start_height if self.start_height is not None else BASE + len(self.headers) - 1
                version = VersionMsg(
                    self.version, self.services, int(time.time()), "0.0.0.0", 0, 99, self.user_agent, height, True
                )
                writer.write(frame(MAGIC, "version", encode_version(version)))
            await writer.drain()
            while True:
                header = await reader.readexactly(24)
                command, length, _ = parse_frame_header(MAGIC, header)
                payload = await reader.readexactly(length)
                self.received.append((command, payload))
                await self._respond(writer, command, payload)
        except (asyncio.IncompleteReadError, ConnectionError):
            self.eofs += 1
        finally:
            writer.close()

    async def _respond(self, writer: asyncio.StreamWriter, command: str, payload: bytes) -> None:
        """Answer one message from the observer."""
        if command == "version":
            await self._send(writer, "verack")
        elif command == "verack":
            if self.on_verack is not None:
                await self.on_verack(self, writer)
        elif command == "getheaders":
            version, locator = parse_getheaders(payload)
            self.getheaders.append((version, locator))
            if self.drop_getheaders:  # like Zakura shedding the request: no reply at all
                return
            index = {header.hash: i for i, header in enumerate(self.headers)}
            start = next((index[h] + 1 for h in locator if h in index), 0)
            batch = self.headers[start : start + 160]
            if batch or not self.silent_when_empty:
                body = write_compact_size(len(batch)) + b"".join(header.raw + b"\x00" for header in batch)
                await self._send(writer, "headers", body)
        elif command == "ping":
            await self._send(writer, "pong", payload)
        elif command == "getdata":
            missing = []
            for kind, digest in decode_inv(payload):
                raw = self.blocks.get(digest[::-1].hex()) if kind == INV_BLOCK and self.serve_blocks else None
                if raw is not None:
                    await self._send(writer, "block", raw)
                else:
                    missing.append((kind, digest))
            if missing:
                await self._send(writer, "notfound", encode_inv(missing))
        elif command == "getaddr":
            await self._send(writer, "addr", encode_addr(self.addrs))


class FakeMonitor:
    """The slice of service.Monitor the observer uses, recording every call."""

    def __init__(self, store: Store, chain: Chain) -> None:
        """Wrap a real store and chain."""
        self.params = TESTNET
        self.store = store
        self.chain = chain
        self.config = None
        self.snapshot: dict = {}
        self.events: list[tuple] = []
        self.tips: dict[str, tuple[str, int | None]] = {}
        self.trusted: dict[str, bool] = {}

    def ingest_headers(self, headers, source, at) -> None:
        """Record and add a batch of headers."""
        self.events.append(("headers", source, [header.hash for header in headers], at))
        for header in headers:
            self.chain.add(header, None, first_seen_at=at)

    def ingest_block(self, block, header, height, *, source, kind, at, trusted=True) -> None:
        """Record and add a block body; `trusted` of the last body per hash goes to `trusted`."""
        self.events.append(("block", source, header.hash, kind, at))
        self.trusted[header.hash] = trusted
        self.chain.add(header, height, block=block, first_seen_at=at, trusted=trusted)

    def observe_tip(self, source, tip_hash, at, height_hint=None) -> None:
        """Record a tip transition."""
        self.events.append(("tip", source, tip_hash, height_hint))
        self.tips[source] = (tip_hash, height_hint)

    def batches(self) -> list[list[str]]:
        """Return the hashes of each ingested headers batch."""
        return [event[2] for event in self.events if event[0] == "headers"]


class BookTests(unittest.TestCase):
    """CandidateBook bookkeeping without any I/O."""

    def test_backoff_doubles_then_marks_unreachable(self) -> None:
        """Failures back off 120, 240, 480, 960 s, and the fifth marks the peer unreachable for 6 h."""
        book = p2p.CandidateBook(p2p.Timings())
        cand = book.add("8.8.8.8", 18233, "dns:seed")
        delays = []
        for _ in range(5):
            book.start(cand)
            delays.append(book.finish(cand, 1000.0, ok=False, error="timeout"))
        self.assertEqual(delays, [120.0, 240.0, 480.0, 960.0, 6 * 3600.0])
        self.assertEqual(cand.state, p2p.UNREACHABLE)
        self.assertEqual(cand.next_attempt, 1000.0 + 6 * 3600.0)
        self.assertIsNone(book.due(1000.0 + 6 * 3600.0 - 1))
        self.assertIs(book.due(1000.0 + 6 * 3600.0), cand)

    def test_backoff_is_capped_and_a_stable_session_resets_it(self) -> None:
        """The delay never exceeds max_backoff, and a stable session waits min_backoff with no failures."""
        book = p2p.CandidateBook(p2p.Timings(unreachable_after=10))
        cand = book.add("8.8.8.8", 18233, "dns:seed")
        delays = [book.finish(cand, 0.0, ok=False, error="x") for _ in range(8)]
        self.assertEqual(delays[-3:], [3600.0, 3600.0, 3600.0])
        self.assertEqual(book.finish(cand, 0.0, ok=True, error=None), 120.0)
        self.assertEqual((cand.failures, cand.state), (0, p2p.BACKOFF))

    def test_add_filters_and_normalizes(self) -> None:
        """Gossip must be public unicast with a real port; static entries may be private."""
        book = p2p.CandidateBook(p2p.Timings())
        self.assertIsNone(book.add("10.1.2.3", 18233, "addr:x"))
        self.assertIsNone(book.add("127.0.0.1", 18233, "getpeerinfo"))
        self.assertIsNone(book.add("8.8.8.8", 0, "addr:x"))
        self.assertIsNone(book.add("224.0.0.1", 18233, "static", trusted=True))
        self.assertIsNone(book.add("not an ip", 18233, "static", trusted=True))
        self.assertIsNone(book.add("8.8.8.8", True, "addr:x"))
        self.assertIsNotNone(book.add("10.1.2.3", 18233, "static", trusted=True))
        mapped = book.add("::ffff:8.8.4.4", 18233, "addr:x")
        self.assertEqual(mapped.ip, "8.8.4.4")
        v6 = book.add("[2001:4860:4860::8888]", 18233, "addr:x", ua_hint="/Zebra:6.4.2/\x07")
        self.assertEqual(v6.source, "p2p:[2001:4860:4860::8888]:18233")
        self.assertEqual(v6.ua_hint, "/Zebra:6.4.2/?")

    def test_priority_port_override_cap_and_eviction(self) -> None:
        """Static beats DNS for the port; the cap holds; unreachable gossip is evicted near the cap."""
        book = p2p.CandidateBook(p2p.Timings(), cap=2)
        cand = book.add("8.8.8.8", 18233, "dns:seed")
        self.assertIs(book.add("8.8.8.8", 18300, "addr:y"), cand)
        self.assertEqual(cand.port, 18233)
        book.add("8.8.8.8", 18400, "static", trusted=True)
        self.assertEqual((cand.port, cand.via, cand.priority), (18400, "static", 0))
        other = book.add("1.1.1.1", 18233, "addr:y")
        self.assertIsNone(book.add("9.9.9.9", 18233, "addr:y"))
        for _ in range(5):
            book.finish(other, 0.0, ok=False, error="x")
        self.assertEqual(book.evict(), 1)
        self.assertEqual(len(book), 1)
        self.assertEqual(book.counts(), {p2p.IDLE: 1})

    def test_a_full_book_makes_room_for_better_sources(self) -> None:
        """Gossip cannot lock out better sources: they replace untried gossip, and gossip replaces failed gossip."""
        book = p2p.CandidateBook(p2p.Timings(), cap=3)
        book.add("1.1.1.1", 18233, "addr:x")
        failing = book.add("1.1.1.2", 18233, "addr:x")
        book.add("8.8.8.8", 18233, "dns:seed")
        self.assertIsNone(book.add("1.1.1.3", 18233, "addr:y"))
        self.assertIsNotNone(book.add("9.9.9.9", 18233, "getpeerinfo"))
        self.assertIsNone(book.get("1.1.1.1"))
        book.finish(failing, 0.0, ok=False, error="timeout")
        self.assertIsNotNone(book.add("1.1.1.3", 18233, "addr:y"))
        self.assertIsNone(book.get("1.1.1.2"))
        self.assertEqual(len(book), 3)
        for cand in book.values():
            cand.handshaked = True  # peers we have talked to are never dropped
        self.assertIsNone(book.add("10.0.0.1", 18233, "static", trusted=True))

    def test_due_prefers_priority_and_skips_busy(self) -> None:
        """due() returns static before DNS before gossip and skips connecting peers."""
        book = p2p.CandidateBook(p2p.Timings())
        gossip = book.add("1.1.1.1", 18233, "addr:y")
        dns = book.add("8.8.8.8", 18233, "dns:seed")
        static = book.add("10.0.0.1", 18233, "static", trusted=True)
        self.assertIs(book.due(0.0), static)
        book.start(static)
        self.assertIs(book.due(0.0), dns)
        book.start(dns)
        self.assertIs(book.due(0.0), gossip)
        book.start(gossip)
        self.assertIsNone(book.due(0.0))


class LocatorTests(unittest.TestCase):
    """build_locator walks the chain densely, then sparsely, then to genesis."""

    @classmethod
    def setUpClass(cls) -> None:
        """Load 300 blocks of the synthetic chain."""
        cls.main = main_chain()[:300]
        cls.chain = Chain(TESTNET)
        for index, header in enumerate(cls.main):
            cls.chain.add(header, BASE + index, first_seen_at=1.0)

    def test_dense_then_doubling_then_genesis(self) -> None:
        """Sixteen consecutive heights, then gaps of 2, 4, 8, ... until the window ends."""
        locator = p2p.build_locator(self.chain, self.chain.best_tip(), TESTNET.genesis_hash)
        offsets = [BASE + 299 - height for _, height in locator[:-1]]
        self.assertEqual(offsets, [*range(16), 17, 21, 29, 45, 77, 141, 269])
        self.assertEqual(locator[-1], (TESTNET.genesis_hash, 0))
        self.assertEqual(locator[0][0], self.main[299].hash)

    def test_side_branch_walks_back_to_the_canonical_chain(self) -> None:
        """A head on a side branch lists its own ancestors before joining the canonical chain."""
        chain = Chain(TESTNET)
        for index, header in enumerate(self.main):
            chain.add(header, BASE + index, first_seen_at=1.0)
        side = build_chain(self.main[290].hash, 2, "side")
        for header in side:
            chain.add(header, None, first_seen_at=2.0)
        locator = p2p.build_locator(chain, chain.get(side[-1].hash), TESTNET.genesis_hash)
        self.assertEqual(
            [h for h, _ in locator[:5]],
            [side[1].hash, side[0].hash, self.main[290].hash, self.main[289].hash, self.main[288].hash],
        )
        self.assertEqual([height for _, height in locator[:3]], [BASE + 292, BASE + 291, BASE + 290])

    def test_length_is_bounded(self) -> None:
        """The locator never exceeds max_len, genesis included."""
        locator = p2p.build_locator(self.chain, self.chain.best_tip(), TESTNET.genesis_hash, dense=500)
        self.assertEqual(len(locator), 101)
        self.assertEqual(locator[-1][0], TESTNET.genesis_hash)
        self.assertEqual(locator[99][1], BASE + 299 - 99)


class ObserverTestCase(unittest.IsolatedAsyncioTestCase):
    """Runs an observer against fake peers reachable at virtual IPs."""

    async def asyncSetUp(self) -> None:
        """Create a store, a chain holding MAIN[:OURS] and an empty route table."""
        self.main = main_chain()
        self._tmp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self._tmp.name) / "monitor.sqlite3", commit_interval=0.0)
        self.chain = Chain(TESTNET)
        for index, header in enumerate(self.main[:OURS]):
            self.chain.add(header, BASE + index, first_seen_at=1.0)
        self.monitor = FakeMonitor(self.store, self.chain)
        self.routes: dict[str, FakePeer] = {}
        self.fakes: list[FakePeer] = []
        self.task: asyncio.Task | None = None

    async def asyncTearDown(self) -> None:
        """Stop the observer and the fakes, then check nothing impolite was ever sent."""
        if self.task is not None:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        for fake in self.fakes:
            await fake.stop()
        self.store.close()
        self._tmp.cleanup()
        for fake in self.fakes:
            self.assert_polite(fake)

    async def add_fake(self, ip: str, headers, **kwargs) -> FakePeer:
        """Start a fake peer and route `ip` to it."""
        fake = FakePeer(headers, **kwargs)
        await fake.start()
        self.routes[ip] = fake
        self.fakes.append(fake)
        return fake

    async def open_connection(self, host: str, port: int):
        """Connect to the fake routed at `host`; refuse every other address."""
        fake = self.routes.get(host)
        if fake is None:
            raise ConnectionRefusedError(f"no route to {host}")
        return await asyncio.open_connection("127.0.0.1", fake.port)

    def observer(self, *static: str, **kwargs) -> p2p.P2PObserver:
        """Build an observer that dials only the fakes, never polls on a timer and connects quickly."""
        options = dict(
            static_peers=static,
            connect_rate=200.0,
            poll_interval=3600.0,
            timings=FAST,
            rng=random.Random(7),
            open_connection=self.open_connection,
        )
        options.update(kwargs)
        return p2p.P2PObserver(TESTNET, self.monitor, **options)

    def start(self, observer: p2p.P2PObserver) -> p2p.P2PObserver:
        """Run the observer in the background until teardown."""
        self.task = asyncio.create_task(observer.run())
        return observer

    async def until(self, predicate, what: str, timeout: float = 5.0) -> None:
        """Wait until `predicate()` is true or fail."""
        deadline = time.monotonic() + timeout
        while not predicate():
            if time.monotonic() > deadline:
                self.fail(f"timed out waiting for {what}")
            await asyncio.sleep(0.01)

    async def candidate(self, observer: p2p.P2PObserver, ip: str) -> p2p.Candidate:
        """Wait until seeding has put `ip` in the observer's book, then return it."""
        await self.until(lambda: observer.book.get(ip) is not None, f"candidate {ip}")
        return observer.book.get(ip)

    def rows(self, sql: str, *args) -> list[dict]:
        """Run a read query against the store."""
        return [dict(row) for row in self.store.reader().execute(sql, args)]

    def assert_polite(self, fake: FakePeer) -> None:
        """The observer only sent allowed commands, empty/echo replies, one version per connection and no relay bits."""
        versions = 0
        for command, payload in fake.received:
            self.assertIn(command, ALLOWED_COMMANDS)
            if command == "version":
                versions += 1
                ours = decode_version(payload)
                self.assertEqual(
                    (ours.version, ours.services, ours.relay, ours.user_agent), (PROTOCOL_VERSION, 0, False, USER_AGENT)
                )
            elif command == "inv":
                self.assertEqual(decode_inv(payload), [], "never announce inventory")
            elif command == "addr":
                self.assertEqual(decode_addr(payload), [])
            elif command == "headers":
                self.assertEqual(decode_headers(payload), [])
            elif command == "getdata":
                items = decode_inv(payload)
                self.assertTrue(1 <= len(items) <= 16 and all(kind == INV_BLOCK for kind, _ in items))
            elif command == "getheaders":
                version, locator = parse_getheaders(payload)
                self.assertEqual(version, min(PROTOCOL_VERSION, fake.version))
                self.assertLessEqual(len(locator), 101)
        self.assertLessEqual(versions, fake.connections)


class HandshakeAndPollTests(ObserverTestCase):
    """Handshake, sources rows and tip polling."""

    async def test_handshake_and_first_poll(self) -> None:
        """The handshake is recorded and the first poll ingests the headers past our parent-of-best head."""
        fake = await self.add_fake("10.0.0.1", self.main[:40])
        observer = self.start(self.observer("10.0.0.1:18233"))
        source = "p2p:10.0.0.1:18233"
        await self.until(lambda: source in self.monitor.tips, "the first tip")
        self.assertEqual(self.monitor.tips[source], (self.main[39].hash, BASE + 39))
        self.assertEqual(self.monitor.batches(), [[h.hash for h in self.main[29:40]]])
        version, locator = fake.getheaders[0]
        self.assertEqual(version, 170160)  # negotiated down to the peer's version
        self.assertEqual(locator[0], self.main[OURS - 2].hash)
        ours = decode_version(fake.payloads("version")[0])
        self.assertEqual(ours.start_height, BASE + OURS - 1)
        self.assertEqual(fake.commands().count("getaddr"), 1)
        self.assertEqual(fake.commands()[fake.commands().index("getheaders") + 1], "ping")

        (row,) = self.rows("SELECT * FROM sources")
        self.assertEqual(
            {
                key: row[key]
                for key in (
                    "source",
                    "kind",
                    "impl",
                    "impl_version",
                    "protocol_version",
                    "services",
                    "start_height",
                    "status",
                    "discovered_via",
                    "tip_hash",
                    "tip_height",
                )
            },
            {
                "source": source,
                "kind": "p2p",
                "impl": "zebra",
                "impl_version": "6.4.2",
                "protocol_version": 170160,
                "services": 1,
                "start_height": BASE + 39,
                "status": "connected",
                "discovered_via": "static",
                "tip_hash": self.main[39].hash,
                "tip_height": BASE + 39,
            },
        )
        snap = observer.snapshot()
        json.dumps(snap)
        self.assertEqual((snap["connected"], snap["connected_by_impl"]), (1, {"zebra": 1}))
        self.assertEqual(snap["peers"][0]["tip_hash"], self.main[39].hash)
        self.assertEqual((snap["peers"][0]["source"], snap["peers"][0]["connected"]), (source, True))
        # Zebra's pong follows its headers, after the poll has already finished.
        await self.until(lambda: observer.snapshot()["peers"][0]["rtt_ms"] is not None, "an RTT sample")

    async def test_zakura_peer_negotiates_its_own_version(self) -> None:
        """getheaders carries min(ours, theirs): 170190 for Zakura 1.5."""
        fake = await self.add_fake(
            "10.0.0.1", self.main[:OURS], user_agent=ZAKURA, version=170190, services=p2p.NODE_P2P_V2
        )
        observer = self.start(self.observer("10.0.0.1:18233"))
        await self.until(lambda: fake.getheaders, "a getheaders")
        self.assertEqual(fake.getheaders[0][0], 170190)
        await self.until(lambda: observer.snapshot()["peers"], "the peer view")
        self.assertTrue(observer.snapshot()["peers"][0]["p2p_v2"])

    async def _empty_reply(self, **fake_options) -> FakePeer:
        """A peer sitting exactly at the locator head: the first poll is empty, the follow-up names its tip."""
        fake = await self.add_fake("10.0.0.1", self.main[: OURS - 1], start_height=BASE + OURS - 1, **fake_options)
        self.start(self.observer("10.0.0.1:18233"))
        source = "p2p:10.0.0.1:18233"
        await self.until(lambda: source in self.monitor.tips, "the tip")
        self.assertEqual(self.monitor.tips[source], (self.main[OURS - 2].hash, BASE + OURS - 2))
        heads = [locator[0] for _, locator in fake.getheaders]
        self.assertEqual(heads, [self.main[OURS - 2].hash, self.main[OURS - 3].hash])
        commands = [c for c in fake.commands() if c in ("getheaders", "ping")]
        self.assertEqual(commands[:4], ["getheaders", "ping", "getheaders", "ping"])
        return fake

    async def test_zakura_silent_empty_reply_uses_the_ping_sentinel(self) -> None:
        """Zakura sends nothing for an empty result; the pong resolves the poll."""
        fake = await self._empty_reply(user_agent=ZAKURA, version=170190, silent_when_empty=True)
        self.assertNotIn("headers", [c for c in fake.commands()])

    async def test_zebra_empty_headers_reply(self) -> None:
        """Zebra sends an empty headers message; it resolves the poll the same way."""
        await self._empty_reply()

    async def test_unanswered_polls_record_no_tip(self) -> None:
        """A peer that drops getheaders but answers pings (Zakura shedding load) gets a note, never a tip."""
        fake = await self.add_fake(
            "10.0.0.1", self.main[:OURS], user_agent=ZAKURA, version=170190, drop_getheaders=True
        )
        observer = self.start(self.observer("10.0.0.1:18233"))
        await self.until(lambda: observer.stats["polls_unanswered"], "an unanswered poll")
        cand = observer.book.get("10.0.0.1")
        self.assertEqual((cand.tip_hash, cand.tip_note), (None, "no headers in reply"))
        self.assertEqual(self.monitor.tips, {})
        heads = [locator[0] for _, locator in fake.getheaders[:4]]
        # The locator runs 16 dense entries down from MAIN[28], then MAIN[11], MAIN[7] and genesis.
        self.assertEqual(heads, [self.main[i].hash for i in (OURS - 2, OURS - 3, 11, 7)])

    async def test_a_dropped_poll_keeps_the_last_tip(self) -> None:
        """Once a peer's tip is known, a poll it drops moves nothing, so no false reorg is recorded."""
        fake = await self.add_fake("10.0.0.1", self.main[:OURS], user_agent=ZAKURA, version=170190)
        observer = self.start(self.observer("10.0.0.1:18233"))
        source = "p2p:10.0.0.1:18233"
        await self.until(lambda: source in self.monitor.tips, "the first tip")
        fake.drop_getheaders = True
        await fake.send("inv", encode_inv([(INV_BLOCK, wire_hash(self.main[OURS - 1].hash))]))
        await self.until(lambda: observer.stats["polls_unanswered"], "the dropped poll")
        self.assertEqual(self.monitor.tips[source], (self.main[OURS - 1].hash, BASE + OURS - 1))
        self.assertEqual([event[2] for event in self.monitor.events if event[0] == "tip"], [self.main[OURS - 1].hash])

    async def test_a_peer_on_a_sparse_locator_entry_is_confirmed(self) -> None:
        """A peer sitting exactly on the first sparse locator entry is named by the confirming round."""
        await self.add_fake("10.0.0.1", self.main[:12], start_height=BASE + OURS - 1)
        self.start(self.observer("10.0.0.1:18233"))
        await self.until(lambda: self.monitor.tips, "the tip")
        self.assertEqual(self.monitor.tips["p2p:10.0.0.1:18233"], (self.main[11].hash, BASE + 11))

    async def test_full_batches_are_continued(self) -> None:
        """A 160-header reply is followed by getheaders from its last header until a short batch."""
        await self.add_fake("10.0.0.1", self.main[:380])
        self.start(self.observer("10.0.0.1:18233"))
        await self.until(lambda: self.monitor.tips, "the tip")
        self.assertEqual(self.monitor.tips["p2p:10.0.0.1:18233"], (self.main[379].hash, BASE + 379))
        self.assertEqual([len(batch) for batch in self.monitor.batches()], [160, 160, 31])
        self.assertEqual(self.chain.best_tip().hash, self.main[379].hash)

    async def test_continuations_are_bounded(self) -> None:
        """At most MAX_CONTINUATIONS extra rounds per poll; the tip is the last header seen."""
        with mock.patch.object(p2p, "MAX_CONTINUATIONS", 1):
            await self.add_fake("10.0.0.1", self.main[:380])
            self.start(self.observer("10.0.0.1:18233"))
            await self.until(lambda: self.monitor.tips, "the tip")
        self.assertEqual([len(batch) for batch in self.monitor.batches()], [160, 160])
        self.assertEqual(self.monitor.tips["p2p:10.0.0.1:18233"], (self.main[348].hash, BASE + 348))

    async def test_peer_without_a_common_block_is_polled_rarely(self) -> None:
        """A peer whose chain shares only genesis with our window gets a note, no ingest and no repeat poll."""
        stale = build_chain(TESTNET.genesis_hash, 5, "stale")
        fake = await self.add_fake("10.0.0.1", stale, start_height=5)
        observer = self.start(self.observer("10.0.0.1:18233"))
        cand = await self.candidate(observer, "10.0.0.1")
        await self.until(lambda: cand.tip_note is not None, "the no-common note")
        self.assertEqual(cand.tip_note, "no common block in window")
        self.assertIsNone(cand.tip_hash)
        self.assertEqual(self.monitor.batches(), [])
        await fake.send("inv", encode_inv([(INV_BLOCK, wire_hash(stale[-1].hash))]))
        await asyncio.sleep(0.3)
        self.assertEqual(len(fake.getheaders), 1)
        self.assertEqual(observer.snapshot()["stats"]["polls_no_common"], 1)

    async def test_headers_failing_pow_close_the_connection(self) -> None:
        """A header that misses its own target is a protocol violation: nothing is ingested."""
        bad = mine_header(self.main[OURS - 1].hash, T0 + 99_999, b"bad", valid=False)
        fake = await self.add_fake("10.0.0.1", [*self.main[:OURS], bad])
        observer = self.start(self.observer("10.0.0.1:18233"))
        cand = await self.candidate(observer, "10.0.0.1")
        await self.until(lambda: cand.state == p2p.BACKOFF, "the disconnect")
        self.assertIn("fails proof of work", cand.last_error)
        self.assertEqual(self.monitor.batches(), [])
        await self.until(lambda: fake.eofs, "the peer to see the close")


class AnnouncementTests(ObserverTestCase):
    """inv handling, announce probes, fallback fetches and sampled reprobes."""

    async def _connected(self, observer: p2p.P2PObserver, count: int) -> None:
        """Wait until `count` peers are connected and have answered a poll."""
        await self.until(lambda: observer.snapshot()["connected"] == count, f"{count} connections")
        await self.until(lambda: len(self.monitor.tips) == count, f"{count} tips")

    async def test_inv_probes_the_announcer_and_ingests_the_block(self) -> None:
        """An unknown announced block is fetched from its announcer and recorded as an announce probe."""
        new = self.main[OURS]
        fake = await self.add_fake("10.0.0.1", self.main[:OURS], blocks={new.hash: make_block(new, BASE + OURS)})
        observer = self.start(self.observer("10.0.0.1:18233"))
        await self._connected(observer, 1)
        fake.headers.append(new)
        await fake.send("inv", encode_inv([(INV_BLOCK, wire_hash(new.hash)), (INV_TX, bytes(32))]))
        source = "p2p:10.0.0.1:18233"
        await self.until(lambda: self.monitor.tips[source][0] == new.hash, "the new tip")
        await self.until(lambda: self.rows("SELECT * FROM probes"), "the probe row")

        (sighting,) = self.rows("SELECT * FROM sightings WHERE hash = ?", new.hash)
        self.assertEqual((sighting["source"], sighting["kind"]), (source, "inv"))
        (probe,) = self.rows("SELECT * FROM probes")
        self.assertEqual(
            {key: probe[key] for key in ("source", "impl", "hash", "reason", "result", "announced_by_same_peer")},
            {
                "source": source,
                "impl": "zebra",
                "hash": new.hash,
                "reason": "announce",
                "result": "block",
                "announced_by_same_peer": 1,
            },
        )
        (block_event,) = [event for event in self.monitor.events if event[0] == "block"]
        self.assertEqual(block_event[1:4], (source, new.hash, "getdata"))
        self.assertEqual(block_event[4], sighting["at"])  # first seen at the announcement
        node = self.chain.get(new.hash)
        self.assertTrue(node.body)
        self.assertEqual((node.miner, node.template), ("zkcodexcoder", "zakura"))
        getdata = [decode_inv(payload) for payload in fake.payloads("getdata")]
        self.assertEqual(getdata, [[(INV_BLOCK, wire_hash(new.hash))]])
        self.assertEqual(observer.snapshot()["peers"][0]["last_inv_hash"], new.hash)

    async def test_announcement_flood_disconnects_the_peer(self) -> None:
        """Past the per-peer budget of block hashes the session ends as a failure and nothing more is recorded."""
        fake = await self.add_fake("10.0.0.1", self.main[:OURS])
        timings = dataclasses.replace(FAST, stable_session=0.0)
        observer = self.start(self.observer("10.0.0.1:18233", timings=timings))
        await self._connected(observer, 1)
        cand = observer.book.get("10.0.0.1")
        junk = [bytes([i]) * 32 for i in range(1, p2p.INV_BURST + 2 * p2p.MAX_INV_BLOCKS + 1)]
        batches = [junk[i : i + p2p.MAX_INV_BLOCKS] for i in range(0, len(junk), p2p.MAX_INV_BLOCKS)]
        fake.writers[-1].write(b"".join(frame(MAGIC, "inv", encode_inv([(INV_BLOCK, h) for h in b])) for b in batches))
        with contextlib.suppress(ConnectionError):
            await fake.writers[-1].drain()
        await self.until(lambda: cand.state == p2p.BACKOFF, "the disconnect")
        self.assertEqual(cand.last_error, "too many block announcements")
        self.assertEqual(cand.failures, 1)  # counted as a failure although the session was "stable"
        self.assertEqual(len(self.rows("SELECT * FROM sightings WHERE kind = 'inv'")), p2p.INV_BURST)

    async def test_notfound_falls_back_to_a_zakura_fleet_peer(self) -> None:
        """If the announcer does not serve its block, a Zakura fleet peer is asked instead."""
        new = self.main[OURS]
        body = {new.hash: make_block(new, BASE + OURS)}
        zebra = await self.add_fake("10.0.0.1", self.main[:OURS], user_agent=ZEBRA_63, blocks=body, serve_blocks=False)
        zakura = await self.add_fake("10.0.0.9", self.main[:OURS], user_agent=ZAKURA, version=170190, blocks=body)
        observer = self.start(self.observer("10.0.0.1:18233", fleet_hosts={"10.0.0.9"}))
        await self._connected(observer, 2)
        await zebra.send("inv", encode_inv([(INV_BLOCK, wire_hash(new.hash))]))
        await self.until(lambda: len(self.rows("SELECT * FROM probes")) == 2, "two probes")
        probes = self.rows("SELECT source, impl, reason, result, announced_by_same_peer FROM probes ORDER BY id")
        self.assertEqual(
            probes,
            [
                {
                    "source": "p2p:10.0.0.1:18233",
                    "impl": "zebra",
                    "reason": "announce",
                    "result": "notfound",
                    "announced_by_same_peer": 1,
                },
                {
                    "source": "p2p:10.0.0.9:18233",
                    "impl": "zakura",
                    "reason": "fetch",
                    "result": "block",
                    "announced_by_same_peer": 0,
                },
            ],
        )
        (block_event,) = [event for event in self.monitor.events if event[0] == "block"]
        self.assertEqual(block_event[1], "p2p:10.0.0.9:18233")
        self.assertEqual(len(zakura.payloads("getdata")), 1)
        self.assertTrue(observer.book.get("10.0.0.9").fleet)

    async def test_a_fleet_body_replaces_an_untrusted_one(self) -> None:
        """A body from outside the fleet is untrusted: request_block fetches it again, and only a fleet body replaces it."""
        new = self.main[OURS]
        forged = {new.hash: make_block(new, BASE + OURS, tag=b"forged-by-a-peer")}
        real = {new.hash: make_block(new, BASE + OURS)}
        zebra = await self.add_fake("10.0.0.1", self.main[:OURS], blocks=forged)
        zakura = await self.add_fake("10.0.0.9", self.main[:OURS], user_agent=ZAKURA, version=170190, blocks=real)
        observer = self.start(self.observer("10.0.0.1:18233", fleet_hosts={"10.0.0.9"}))
        await self._connected(observer, 2)
        await zebra.send("inv", encode_inv([(INV_BLOCK, wire_hash(new.hash))]))
        await self.until(lambda: self.chain.get(new.hash) is not None and self.chain.get(new.hash).body, "the body")
        node = self.chain.get(new.hash)
        self.assertFalse(node.body_trusted)
        self.assertIn("forged", node.miner)
        self.assertTrue(observer.request_block(new.hash, BASE + OURS))
        await self.until(lambda: node.body_trusted, "the fleet body")
        self.assertEqual((node.miner, self.monitor.trusted[new.hash]), ("zkcodexcoder", True))
        self.assertEqual(len(zakura.payloads("getdata")), 1)
        self.assertTrue(observer.request_block(new.hash))  # a trusted body is known: nothing to fetch
        self.assertEqual(observer.snapshot()["pending_fetches"], 0)

    async def test_sampled_reprobe_of_a_known_block(self) -> None:
        """With probe_sample=1 a known block announced by Zebra is re-probed once per version group."""
        known = self.main[OURS - 1]
        fake = await self.add_fake(
            "10.0.0.1", self.main[:OURS], blocks={known.hash: make_block(known, BASE + OURS - 1)}
        )
        observer = self.start(self.observer("10.0.0.1:18233", probe_sample=1.0))
        await self._connected(observer, 1)
        for _ in range(2):
            await fake.send("inv", encode_inv([(INV_BLOCK, wire_hash(known.hash))]))
        await self.until(lambda: self.rows("SELECT * FROM probes"), "the reprobe")
        await asyncio.sleep(0.2)
        (probe,) = self.rows("SELECT reason, result, announced_by_same_peer FROM probes")
        self.assertEqual(probe, {"reason": "reprobe", "result": "block", "announced_by_same_peer": 1})
        self.assertEqual(len(fake.payloads("getdata")), 1)

    async def test_request_block_prefers_the_named_host(self) -> None:
        """request_block goes to prefer_host when connected, else to a Zakura peer."""
        bodies = {h.hash: make_block(h, BASE + i) for i, h in enumerate(self.main[OURS : OURS + 2], start=OURS)}
        zakura = await self.add_fake("10.0.0.1", self.main[:OURS], user_agent=ZAKURA, version=170190, blocks=bodies)
        zebra = await self.add_fake("10.0.0.2", self.main[:OURS], blocks=bodies)
        observer = self.start(self.observer("10.0.0.1:18233", "10.0.0.2:18233"))
        await self._connected(observer, 2)
        self.assertTrue(observer.request_block(self.main[OURS].hash, BASE + OURS, prefer_host="10.0.0.2"))
        self.assertTrue(observer.request_block(self.main[OURS + 1].hash))
        self.assertFalse(observer.request_block("zz" * 32))
        self.assertFalse(observer.request_block("00 " * 21 + "0"))
        await self.until(lambda: len(self.rows("SELECT * FROM probes")) == 2, "two fetches")
        self.assertEqual(len(zebra.payloads("getdata")), 1)
        self.assertEqual(decode_inv(zebra.payloads("getdata")[0])[0][1], wire_hash(self.main[OURS].hash))
        self.assertEqual(decode_inv(zakura.payloads("getdata")[0])[0][1], wire_hash(self.main[OURS + 1].hash))
        reasons = {row["reason"] for row in self.rows("SELECT reason FROM probes")}
        self.assertEqual(reasons, {"fetch"})
        self.assertTrue(observer.request_block(self.main[OURS].hash))  # body already known

    async def test_request_block_waits_for_a_peer(self) -> None:
        """A request made before any connection is served once a peer connects."""
        new = self.main[OURS]
        await self.add_fake(
            "10.0.0.1",
            self.main[:OURS],
            user_agent=ZAKURA,
            version=170190,
            blocks={new.hash: make_block(new, BASE + OURS)},
        )
        observer = self.observer("10.0.0.1:18233")
        self.assertTrue(observer.request_block(new.hash, prefer_host="10.0.0.7"))
        self.assertEqual(observer.snapshot()["pending_fetches"], 1)
        self.start(observer)
        await self.until(lambda: self.chain.get(new.hash) is not None and self.chain.get(new.hash).body, "the body")
        self.assertEqual(observer.snapshot()["pending_fetches"], 0)

    async def test_fetches_wait_for_a_zakura_peer_before_asking_zebra(self) -> None:
        """Zebra serves only its best chain, so it is asked only after the grace period."""
        new = self.main[OURS]
        fake = await self.add_fake("10.0.0.1", self.main[:OURS], blocks={new.hash: make_block(new, BASE + OURS)})
        observer = self.start(self.observer("10.0.0.1:18233", timings=p2p.Timings(fetch_grace=1.0)))
        await self._connected(observer, 1)
        observer.request_block(new.hash)
        await asyncio.sleep(0.3)
        self.assertEqual(fake.payloads("getdata"), [])
        await self.until(lambda: fake.payloads("getdata"), "the getdata after the grace period", timeout=5.0)
        await self.until(lambda: self.rows("SELECT * FROM probes"), "the probe")
        (probe,) = self.rows("SELECT reason, result FROM probes")
        self.assertEqual(probe, {"reason": "fetch", "result": "block"})


class ConnectionHygieneTests(ObserverTestCase):
    """Backoff, frame limits, replies to peer requests and address gossip."""

    async def test_backoff_after_the_peer_disconnects(self) -> None:
        """A peer that hangs up is not redialed for at least 120 s and its row says so."""

        async def hang_up(fake, writer):
            """Close right after the handshake."""
            writer.close()

        fake = await self.add_fake("10.0.0.1", self.main[:OURS], on_verack=hang_up)
        observer = self.start(self.observer("10.0.0.1:18233", timings=p2p.Timings()))
        cand = await self.candidate(observer, "10.0.0.1")
        await self.until(lambda: cand.state == p2p.BACKOFF, "backoff")
        self.assertGreater(cand.next_attempt - time.monotonic(), 115)
        self.assertEqual(cand.failures, 1)  # a session this short counts as a failure
        await asyncio.sleep(0.3)
        self.assertEqual(fake.connections, 1)
        (row,) = self.rows("SELECT status, last_error FROM sources")
        self.assertEqual(row["status"], "backoff")
        self.assertIsNotNone(row["last_error"])

    async def test_oversize_frame_closes_the_connection(self) -> None:
        """A frame header announcing more than MAX_PAYLOAD bytes is rejected before reading the body."""

        async def oversize(fake, writer):
            """Announce a payload one byte over the limit."""
            writer.write(MAGIC + b"headers".ljust(12, b"\0") + struct.pack("<I", MAX_PAYLOAD + 1) + bytes(4))
            await writer.drain()

        fake = await self.add_fake("10.0.0.1", self.main[:OURS], on_verack=oversize)
        observer = self.start(self.observer("10.0.0.1:18233"))
        cand = await self.candidate(observer, "10.0.0.1")
        await self.until(lambda: cand.state == p2p.BACKOFF, "the disconnect")
        self.assertIn("exceeds", cand.last_error)
        await self.until(lambda: fake.eofs, "the peer to see the close")

    async def test_pre_handshake_limit(self) -> None:
        """Before the handshake a 2000-byte message is too big (limit 1024); no sources row is written."""
        await self.add_fake("10.0.0.1", self.main[:OURS], oversize_version=True)
        observer = self.start(self.observer("10.0.0.1:18233"))
        cand = await self.candidate(observer, "10.0.0.1")
        await self.until(lambda: cand.state == p2p.BACKOFF, "the rejection")
        self.assertIn("exceeds 1024", cand.last_error)
        self.assertFalse(cand.handshaked)
        self.assertEqual(self.rows("SELECT * FROM sources"), [])

    async def test_peer_requests_are_answered_and_unknown_commands_ignored(self) -> None:
        """ping, getaddr, mempool, getdata, getheaders and getblocks get immediate empty or echo replies."""
        getdata = encode_inv([(INV_BLOCK, wire_hash(self.main[3].hash)), (INV_TX, bytes(range(32)))])
        locator = encode_getheaders(170160, [self.main[0].hash])

        async def ask(fake, writer):
            """Send the requests a Zakura or Zebra peer might send us."""
            for command, payload in (
                ("sendheaders", b""),
                ("feefilter", bytes(8)),
                ("ping", encode_ping(7)),
                ("getaddr", b""),
                ("mempool", b""),
                ("getdata", getdata),
                ("getheaders", locator),
                ("getblocks", locator),
            ):
                await fake._send(writer, command, payload)

        fake = await self.add_fake("10.0.0.1", self.main[:OURS], on_verack=ask)
        observer = self.start(self.observer("10.0.0.1:18233"))
        await self.until(lambda: len(fake.payloads("inv")) == 2 and fake.payloads("headers"), "the replies")
        self.assertIn(7, [decode_ping(payload) for payload in fake.payloads("pong")])
        self.assertEqual(fake.payloads("addr"), [b"\x00"])
        self.assertEqual(fake.payloads("notfound"), [getdata])
        self.assertEqual(fake.payloads("headers"), [b"\x00"])
        self.assertEqual(fake.payloads("inv"), [b"\x00", b"\x00"])
        self.assertEqual(observer.snapshot()["connected"], 1)

    async def test_addr_gossip_adds_only_public_candidates(self) -> None:
        """getaddr is sent once; public addresses join the book, private and port-0 ones do not."""
        addrs = [("8.8.8.8", 18233), ("10.1.2.3", 18233), ("1.1.1.1", 0), ("2001:4860:4860::8888", 18233)]
        fake = await self.add_fake("10.0.0.1", self.main[:OURS], addrs=addrs)
        observer = self.start(self.observer("10.0.0.1:18233"))
        await self.until(lambda: observer.book.get("8.8.8.8") is not None, "the gossiped candidate")
        self.assertEqual(observer.book.get("8.8.8.8").via, "addr:10.0.0.1")
        self.assertIsNotNone(observer.book.get("2001:4860:4860::8888"))
        self.assertIsNone(observer.book.get("10.1.2.3"))
        self.assertIsNone(observer.book.get("1.1.1.1"))
        self.assertEqual(fake.commands().count("getaddr"), 1)
        # The gossiped address is dialed, refused by the route table and backed off.
        await self.until(lambda: observer.book.get("8.8.8.8").state == p2p.BACKOFF, "the refused dial")
        self.assertEqual(observer.book.get("8.8.8.8").last_error, "connection refused")

    async def test_add_candidates_from_getpeerinfo(self) -> None:
        """getpeerinfo entries join the book with their user agent hint; bad entries are skipped."""
        observer = self.observer()
        added = observer.add_candidates(
            [
                ("8.8.8.8", 18233, "/Zebra:6.3.0/"),
                ("8.8.4.4", None),
                ("10.0.0.5", 18233),
                ("x",),
                ("1.1.1.1", 18233, None, "getpeerinfo:tazminer"),
                ("9.9.9.9", "18234"),
            ]
        )
        self.assertEqual(added, 4)
        self.assertEqual(observer.book.get("9.9.9.9").port, 18234)
        self.assertEqual(observer.book.get("8.8.8.8").ua_hint, "/Zebra:6.3.0/")
        self.assertEqual(observer.book.get("8.8.4.4").port, TESTNET.default_port)
        self.assertEqual(observer.book.get("1.1.1.1").via, "getpeerinfo:tazminer")


class DirectTestCase(unittest.IsolatedAsyncioTestCase):
    """An observer over MAIN[:OURS] that never dials; tests call its handlers with unconnected peers."""

    def setUp(self) -> None:
        """Create a store, a chain holding MAIN[:OURS] and the observer."""
        self.main = main_chain()
        self._tmp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self._tmp.name) / "monitor.sqlite3", commit_interval=0.0)
        self.chain = Chain(TESTNET)
        for index, header in enumerate(self.main[:OURS]):
            self.chain.add(header, BASE + index, first_seen_at=1.0)
        self.monitor = FakeMonitor(self.store, self.chain)
        self.observer = p2p.P2PObserver(TESTNET, self.monitor, rng=random.Random(7))

    def tearDown(self) -> None:
        """Close the store."""
        self.store.close()
        self._tmp.cleanup()

    def peer(self, ip: str) -> p2p._Peer:
        """Return a peer object for `ip` with no socket, registered as connected."""
        cand = self.observer.book.add(ip, 18233, "static", trusted=True)
        peer = p2p._Peer(self.observer, cand, None, None, passive=False)
        self.observer._peers[cand.ip] = peer
        return peer


class ValidationTests(DirectTestCase):
    """Headers and bodies from peers are checked before they reach the chain."""

    def test_new_headers_need_a_valid_solution_and_a_sane_time(self) -> None:
        """A forged sibling of the tip cannot skip Equihash or be dated more than 2 h ahead."""
        parent = self.main[OURS - 1]
        check = functools.partial(self.observer._check_headers, parent.hash, BASE + OURS - 1)
        check([self.main[OURS], self.main[OURS + 1]])
        forged = mine_header(parent.hash, parent.time + SPACING, b"forged", solution=bytes(1344))
        with self.assertRaisesRegex(p2p.PeerError, "fails proof of work"):
            check([forged])
        with self.assertRaisesRegex(p2p.PeerError, "fails proof of work"):
            check([self.main[OURS], mine_header(self.main[OURS].hash, T0, b"late", solution=bytes(1344))])
        future = mine_header(parent.hash, int(time.time()) + 3 * 3_600, b"future")
        with self.assertRaisesRegex(p2p.PeerError, "2 h ahead"):
            check([future])

    def test_new_headers_need_the_expected_difficulty(self) -> None:
        """nBits must follow from the parent chain: no heavier-than-allowed header, no early min-difficulty one."""
        hard = target_to_bits(bits_to_target(BITS) // 2)
        ancestors = []
        for index in range(30):  # a chain on schedule (75 s apart) at twice the minimum difficulty
            prev_hash = ancestors[-1].hash if ancestors else ROOT
            ancestors.append(mine_header(prev_hash, T0 + 75 * index, f"steady{index}".encode(), bits=hard))
        chain = Chain(TESTNET)
        for index, header in enumerate(ancestors):
            chain.add(header, BASE + index, first_seen_at=1.0)
        observer = p2p.P2PObserver(TESTNET, FakeMonitor(self.store, chain))
        parent, height = ancestors[-1], BASE + 30
        context = [(header.bits, header.time) for header in reversed(ancestors[2:])]
        on_time = parent.time + 75
        want = expected_bits(TESTNET, height, on_time, context)
        check = functools.partial(observer._check_headers, parent.hash, height - 1)
        check([mine_header(parent.hash, on_time, b"good", bits=want)])
        check([mine_header(parent.hash, parent.time + SPACING, b"slow")])  # the minimum-difficulty rule
        for header in (
            mine_header(parent.hash, on_time, b"heavy", bits=target_to_bits(bits_to_target(want) // 4)),
            mine_header(parent.hash, on_time, b"early"),
        ):
            with self.assertRaisesRegex(p2p.PeerError, "wrong difficulty"):
                check([header])

    def test_real_nu7_activation_headers(self) -> None:
        """Testnet's real headers A - 1 .. A + 2 pass with real Equihash; the pre-NU7 rules reject block A."""
        data = json.loads(NU7_HEADERS.read_text())
        nu7 = TESTNET.nu7_height
        chain = Chain(TESTNET)
        prev_hash = ROOT
        for height in range(min(map(int, data["headers"])), nu7 - 1):
            block_hash, bits, block_time = data["headers"][str(height)]
            known = BlockHeader(block_hash, prev_hash, 4, "00" * 32, block_time, bits, "00" * 32, b"")
            chain.add(known, height, first_seen_at=1.0)
            prev_hash = block_hash
        headers = [parse_header(bytes.fromhex(data["raw"][str(height)]))[0] for height in range(nu7 - 1, nu7 + 3)]
        self.assertEqual(headers[0].prev_hash, prev_hash)
        with mock.patch.object(consensus, "check_equihash", REAL_EQUIHASH):
            observer = p2p.P2PObserver(TESTNET, FakeMonitor(self.store, chain))
            self.assertEqual(observer._context_len, 113)
            observer._check_headers(prev_hash, nu7 - 2, headers)
            stale = p2p.P2PObserver(dataclasses.replace(TESTNET, nu7_height=None), FakeMonitor(self.store, chain))
            with self.assertRaisesRegex(p2p.PeerError, f"header {headers[1].hash} has the wrong difficulty"):
                stale._check_headers(prev_hash, nu7 - 2, headers)

    async def test_block_bodies_must_match_their_place_in_the_chain(self) -> None:
        """A body whose coinbase height contradicts the chain, or whose header is forged, ends the session."""
        peer = self.peer("10.0.0.1")
        loop = asyncio.get_running_loop()

        def ask(block_hash: str) -> asyncio.Future:
            """Make a getdata for `block_hash` the peer's request in flight."""
            peer.current = p2p._Request(p2p._GETDATA, hash=block_hash, future=loop.create_future())
            return peer.current.future

        new, known = self.main[OURS], self.main[OURS - 1]
        future = ask(new.hash)
        self.observer._on_block(peer, make_block(new, BASE + OURS))
        self.assertEqual(future.result()[0], p2p.BLOCK)
        forged = mine_header(known.hash, known.time + SPACING, b"forged", solution=bytes(1344))
        for header, height, error in (
            (new, 5_000_000, "coinbase height 5000000, not 4400030"),
            (known, BASE + OURS, "coinbase height"),
            (forged, BASE + OURS, "fails proof of work"),
        ):
            ask(header.hash)
            with self.assertRaisesRegex(p2p.PeerError, error):
                self.observer._on_block(peer, make_block(header, height))

    def test_bodies_are_taken_only_from_peers_asked_for_them(self) -> None:
        """An unsolicited body for a pending fetch is ignored unless that peer was asked for it."""
        asked, other = self.peer("10.0.0.1"), self.peer("10.0.0.2")
        new = self.main[OURS]
        self.assertTrue(self.observer.request_block(new.hash))  # no Zakura peer yet: it waits
        self.observer._fetches[new.hash].tried.add(asked.cand.ip)
        self.observer._on_block(other, make_block(new, BASE + OURS))
        self.assertIsNone(self.chain.get(new.hash))
        self.observer._on_block(asked, make_block(new, BASE + OURS))
        self.assertTrue(self.chain.get(new.hash).body)


class BudgetTests(DirectTestCase):
    """Per-peer limits on announce fetches and gossip."""

    def test_announce_fetches_are_budgeted(self) -> None:
        """Announce fetches stay on their announcer, are capped per announcer and leave room for request_block."""
        observer = self.observer
        hashes = iter(f"{i:064x}" for i in itertools.count(1))
        full = self.peer("10.0.0.1")
        for _ in range(p2p.MAX_PEER_QUEUE):
            full.enqueue(p2p._Request(p2p._POLL))
        block_hash = next(hashes)
        self.assertFalse(observer._start_fetch(block_hash, peer=full, inv_at=0.0))
        self.assertNotIn(block_hash, observer._fetches)
        first = self.peer("10.0.0.2")
        for _ in range(p2p.MAX_ANNOUNCE_FETCHES):
            self.assertTrue(observer._start_fetch(next(hashes), peer=first, inv_at=0.0))
        self.assertFalse(observer._start_fetch(next(hashes), peer=first, inv_at=0.0))
        for n in itertools.count(1):
            if len(observer._fetches) >= p2p.MAX_FETCHES - p2p.FETCH_RESERVE:
                break
            peer = self.peer(f"10.1.0.{n}")
            for _ in range(p2p.MAX_ANNOUNCE_FETCHES):
                observer._start_fetch(next(hashes), peer=peer, inv_at=0.0)
        self.assertFalse(observer._start_fetch(next(hashes), peer=self.peer("10.2.0.1"), inv_at=0.0))
        self.assertTrue(observer.request_block(next(hashes)))
        self.assertEqual(observer.stats["fetch_dropped"], 3)

    def test_gossip_is_budgeted_per_peer_and_skips_privileged_ports(self) -> None:
        """One peer adds at most MAX_ADDR_PER_PEER new candidates per getaddr interval, and never port 22."""
        observer = self.observer
        peer = self.peer("10.0.0.1")
        entries = [(f"8.8.{i // 250}.{i % 250 + 1}", 18233, 1, T0) for i in range(300)]
        observer._on_addr(peer, [("9.9.9.9", 22, 1, T0), ("9.9.9.8", 18333, 1, T0), *entries])
        self.assertIsNone(observer.book.get("9.9.9.9"))
        self.assertEqual(observer.book.get("9.9.9.8").port, 18333)
        self.assertEqual(peer.cand.addr_added, p2p.MAX_ADDR_PER_PEER)
        self.assertEqual(observer.stats["addr_dropped"], 301 - p2p.MAX_ADDR_PER_PEER)
        self.assertIsNone(observer.book.get(entries[-1][0]))
        peer.cand.addr_window_at -= observer.timings.getaddr_interval
        observer._on_addr(peer, entries[-1:])
        self.assertIsNotNone(observer.book.get(entries[-1][0]))


class SweepTests(ObserverTestCase):
    """The one-shot sweep behind `probe-peers --once`."""

    async def test_sweep_polls_each_peer_once_and_disconnects(self) -> None:
        """Each candidate is dialed once, polled, reported with its relation and left in backoff."""
        zebra = await self.add_fake("10.0.0.1", self.main[:OURS])
        zakura = await self.add_fake("10.0.0.2", self.main[: OURS + 1], user_agent=ZAKURA, version=170190)
        observer = self.observer("10.0.0.1:18233", "10.0.0.2:18233", "10.0.0.3:18233")
        results = {result["ip"]: result for result in await observer.sweep(limit=10)}
        self.assertEqual(set(results), {"10.0.0.1", "10.0.0.2", "10.0.0.3"})
        self.assertEqual(results["10.0.0.3"]["error"], "connection refused")
        self.assertEqual(
            (results["10.0.0.1"]["impl"], results["10.0.0.1"]["tip_hash"]), ("zebra", self.main[OURS - 1].hash)
        )
        self.assertEqual((results["10.0.0.2"]["impl"], results["10.0.0.2"]["tip_height"]), ("zakura", BASE + OURS))
        self.assertEqual(results["10.0.0.2"]["relation"]["kind"], "same")
        self.assertIn(results["10.0.0.1"]["relation"]["kind"], {"same", "behind"})
        self.assertIsNone(results["10.0.0.1"]["error"])
        self.assertIsNotNone(results["10.0.0.1"]["rtt_ms"])  # the round waited for its pong
        json.dumps(results)
        for fake in (zebra, zakura):
            await self.until(lambda fake=fake: fake.eofs == 1, "the sweep to disconnect")
            self.assertNotIn("getaddr", fake.commands())
        for ip in ("10.0.0.1", "10.0.0.2"):
            cand = observer.book.get(ip)
            self.assertEqual((cand.state, cand.failures), (p2p.BACKOFF, 0))
        self.assertEqual({row["status"] for row in self.rows("SELECT status FROM sources")}, {"backoff"})
        self.assertEqual(await observer.sweep(limit=10), [])  # everyone is backing off


if __name__ == "__main__":
    unittest.main()
