"""Asyncio P2P observer: peer tips, block announcements and availability probes over legacy TCP.

`P2PObserver` keeps up to `max_peers` outbound connections and learns, per peer:

- its implementation, version, services and start height from the handshake;
- its best tip, by polling `getheaders` + `ping` with a locator from our chain.
  Peers answer in order, so each round ends with the pong and any `headers`
  reply arrives before it; no headers means "nothing after the locator"
  (Zakura sends no `headers` at all then, Zebra sends an empty one);
- the blocks it announces (`inv`), recorded as sightings, and whether it serves
  them when asked (`getdata` probes).

It never relays: services=0, relay=0, and it only sends version, verack, ping,
pong, getheaders, getdata, getaddr, and empty or echo replies to the peer's own
requests, so the few testnet peers never see it as a relaying node.

The observer must not share an IP with a fleet node: peers keep one
connection per IP and silently drop a second one. Everything, including the
monitor and store calls, runs on the event-loop thread. Timeouts use
`asyncio.timeout()`: Python 3.11's `asyncio.wait_for` can swallow a
cancellation that races with a completed read, leaving a reader running.

Notes:
- `poll_interval`, `probe_sample`, `timings`, `rng`, `open_connection` and
  `resolver` are test seams; `from_config()` builds an observer from a config.
  Fleet hosts are also dialed, as `via="fleet"` candidates.
- Candidates are keyed by IP. The first port seen for an IP is kept unless a
  higher-priority source (static/fleet > getpeerinfo > dns > addr) names another.
  A full book makes room by dropping a never-handshaked candidate of lower
  priority (or a failed one of equal priority).
- `sources` rows are written only for peers that completed a handshake, so
  thousands of unreachable gossip addresses do not flood the table.
- Peers are untrusted: new headers and bodies are validated (`_check_header`,
  `_check_block`), and block announcements, announce fetches and gossiped
  addresses are budgeted per peer. Bodies from peers outside the fleet are
  ingested as untrusted (see `chain.Node.body_trusted`).
- An empty tip poll is followed up (locator minus its head, then the locator
  below its dense part, then that minus its head). Only headers name a tip:
  Zakura also answers nothing to a request it sheds, times out or gets during
  its setup, yet still sends the pong.
- A poll round ends with its pong, which also yields the RTT; a round
  timeout closes the connection. A peer with no block of our window on its
  chain is re-polled only every `no_common_repoll`.
- Sampled availability probes cover every Zebra minor version and use reason
  "reprobe"; "announce" is the probe of an unknown block sent to its
  announcer; "fetch" is any other body fetch. The fallback fetch also runs
  after a timeout or error, not only after `notfound`, and goes to non-Zakura
  peers only after `fetch_grace` (Zebra serves only its best chain, so asking
  it for side-branch blocks just records `notfound`).
- `probes.impl` holds the bare implementation name; the version is on the
  peer's `sources` row.
- `request_block()` returns a bool. With no suitable peer it keeps the request
  for up to `fetch_ttl` and tries at most MAX_FETCH_ATTEMPTS different peers.
- `add_candidates()` takes (ip, port[, user_agent[, via]]) tuples; `sweep()` is
  the one-shot `probe-peers --once` pass.
- Sightings go through `monitor.record_sighting` when the monitor has one, else
  `monitor.store.record_sighting`.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import ipaddress
import itertools
import logging
import random
import re
import socket
import time
from collections import Counter, OrderedDict, deque
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any

from .config import split_host_port
from .consensus import Block, BlockHeader, NetworkParams, ParseError, check_pow, expected_bits, parse_block
from .wire import (
    HEADER_LEN,
    MAX_HEADERS,
    MAX_LOCATOR_HASHES,
    MAX_PAYLOAD,
    MAX_PAYLOAD_PRE_HANDSHAKE,
    NODE_P2P_V2,
    PROTOCOL_VERSION,
    USER_AGENT,
    VersionMsg,
    WireError,
    classify_user_agent,
    decode_addr,
    decode_addrv2,
    decode_headers,
    decode_inv,
    decode_ping,
    decode_version,
    encode_empty_addr,
    encode_empty_headers,
    encode_empty_inv,
    encode_getdata_blocks,
    encode_getheaders,
    encode_inv,
    encode_ping,
    encode_version,
    frame,
    inv_block_hashes,
    parse_frame_header,
    verify_checksum,
)

log = logging.getLogger(__name__)

# Bitcoin-style locator: this many consecutive ancestors, then doubling gaps.
# Forks up to this deep come back starting exactly at the fork point.
DENSE_LOCATOR = 16
# Extra getheaders rounds when a peer returns a full batch (1760 headers per poll at most).
MAX_CONTINUATIONS = 10
# Parent steps a locator may take along a side branch before reaching the canonical chain.
MAX_SIDE_WALK = 2_000
MAX_CANDIDATES = 5_000
# Unreachable gossip candidates are evicted once the book is this full.
EVICT_AT = 0.9
MAX_PEER_QUEUE = 32
# Block hashes taken from one inv; Zakura and Zebra announce exactly one.
MAX_INV_BLOCKS = 8
# Per-peer budget of announced block hashes: a burst, then one hash per INV_REFILL seconds.
# Peers announce each block once (live peak: 8 a minute), so only floods run out.
INV_BURST = 32
INV_REFILL = 5.0
# Announcement records kept for probe sampling and first-inv times (hours of blocks).
MAX_TRACKED_BLOCKS = 4_096
MAX_FETCHES = 512
# Slots announce fetches leave to `request_block`, and the announce fetches one peer may have
# pending, so announcements of made-up hashes cannot crowd out real fetches.
FETCH_RESERVE = 128
MAX_ANNOUNCE_FETCHES = 8
MAX_FETCH_ATTEMPTS = 3
# New candidates one peer's gossip may add per `getaddr_interval`; getaddr replies hold 64-185.
MAX_ADDR_PER_PEER = 256
# Gossiped privileged ports (22, 25, 443, ...) are never Zcash peers; do not dial them.
MIN_GOSSIP_PORT = 1024
# Zakura rejects blocks dated more than 2 h past its clock.
MAX_FUTURE_BLOCK_TIME = 2 * 3_600
MAX_RESOLVED_IPS = 64
MAX_SNAPSHOT_PEERS = 1_000
MAX_TEXT = 256
MAX_ERROR_TEXT = 200
# Ping nonces remembered for RTT; the pong of a poll usually arrives after its headers.
MAX_PINGS = 8
# Larger heights are garbage hints.
MAX_HEIGHT = (1 << 31) - 1

# Candidate states, also written to `sources.status` for handshaked peers.
IDLE, CONNECTING, CONNECTED, BACKOFF, UNREACHABLE = "idle", "connecting", "connected", "backoff", "unreachable"
# Probe reasons and results (the `probes` columns).
ANNOUNCE, FETCH, REPROBE = "announce", "fetch", "reprobe"
BLOCK, NOTFOUND, TIMEOUT, ERROR = "block", "notfound", "timeout", "error"
_POLL, _GETDATA = "poll", "getdata"

_HASH_RE = re.compile(r"[0-9a-fA-F]{64}")
# Lower dials first; the prefix of `via` before ":" selects the priority.
_VIA_PRIORITY = {"fleet": 0, "static": 0, "getpeerinfo": 1, "dns": 2, "addr": 3}

Resolver = Callable[[str, int], Awaitable[list[str]]]
Opener = Callable[[str, int], Awaitable[tuple[asyncio.StreamReader, asyncio.StreamWriter]]]


@dataclass(frozen=True, slots=True)
class Timings:
    """Timeouts and backoff in seconds; tests shrink them."""

    connect_timeout: float = 5.0
    # Zakura and Zebra drop a handshake that takes longer than 3 s.
    handshake_timeout: float = 3.0
    poll_timeout: float = 15.0
    # Zakura can hold an early-advertised block for 15 s before answering.
    fetch_timeout: float = 20.0
    # Peers accept one inbound attempt per IP per 119 s, so never retry sooner.
    min_backoff: float = 120.0
    max_backoff: float = 3_600.0
    unreachable_after: int = 5
    unreachable_retry: float = 6 * 3_600.0
    # Peers ping about every 60 s and we poll every 15 s: 3 silent minutes is a dead link.
    idle_timeout: float = 180.0
    # A shorter session counts as a failure (e.g. a duplicate-IP drop right after the handshake).
    stable_session: float = 60.0
    getaddr_interval: float = 600.0
    dns_refresh: float = 1_800.0
    dns_timeout: float = 10.0
    write_timeout: float = 10.0
    fetch_ttl: float = 600.0
    # Fetches wait this long for a Zakura (or preferred) peer before trying any peer.
    fetch_grace: float = 30.0
    # Such a peer answers every poll with 160 headers from genesis; ask rarely.
    no_common_repoll: float = 600.0


class PeerError(Exception):
    """A peer broke the protocol or stopped answering; its connection is closed."""


@dataclass(slots=True, eq=False)
class Candidate:
    """A dialable peer and what we learned about it; the book keeps one per IP across reconnects."""

    ip: str
    port: int
    via: str
    priority: int
    fleet: bool = False
    trusted: bool = False
    ua_hint: str | None = None
    state: str = IDLE
    failures: int = 0
    next_attempt: float = 0.0  # monotonic
    last_error: str | None = None
    handshaked: bool = False
    last_getaddr: float | None = None  # monotonic
    user_agent: str | None = None
    impl: str | None = None
    impl_version: str | None = None
    protocol_version: int | None = None
    services: int | None = None
    start_height: int | None = None
    tip_hash: str | None = None
    tip_height: int | None = None
    tip_at: float | None = None
    tip_note: str | None = None
    last_inv_hash: str | None = None
    last_inv_at: float | None = None
    rtt: float | None = None
    connected_at: float | None = None
    addr_window_at: float | None = None  # monotonic start of the current gossip budget window
    addr_added: int = 0

    @property
    def source(self) -> str:
        """The `sources.source` key for this peer."""
        return source_key(self.ip, self.port)

    @property
    def probe_group(self) -> str | None:
        """Return "zebra:<major.minor>" for Zebra peers (the sampled-probe groups), else None."""
        if self.impl != "zebra":
            return None
        return "zebra:" + ".".join((self.impl_version or "").split(".")[:2])


class CandidateBook:
    """Candidate peers keyed by IP with per-IP backoff; pure bookkeeping, the caller passes the clock."""

    def __init__(self, timings: Timings, cap: int = MAX_CANDIDATES) -> None:
        """Create an empty book holding at most `cap` candidates."""
        self.timings = timings
        self.cap = cap
        self._by_ip: dict[str, Candidate] = {}

    def __len__(self) -> int:
        """Return the number of candidates."""
        return len(self._by_ip)

    def get(self, ip: str) -> Candidate | None:
        """Return the candidate for `ip`, or None."""
        addr = _parse_ip(ip)
        return self._by_ip.get(str(addr)) if addr is not None else None

    def values(self) -> list[Candidate]:
        """Return every candidate."""
        return list(self._by_ip.values())

    def add(
        self,
        ip: Any,
        port: Any,
        via: str,
        *,
        ua_hint: Any = None,
        fleet: bool = False,
        trusted: bool = False,
    ) -> Candidate | None:
        """Add a peer address or refresh a known one; return it, or None when rejected.

        Untrusted sources (gossip, DNS, getpeerinfo) must name a public unicast
        address; static and fleet entries may be private. A known IP keeps its
        port unless a higher-priority source names another while it is not connected.
        A full book makes room with `_victim`, so gossip cannot lock out better sources.
        """
        addr, port = _parse_ip(ip), _port(port)
        if addr is None or port is None or addr.is_multicast or addr.is_unspecified:
            return None
        if not trusted and not addr.is_global:
            return None
        via = _clean(via, 64)
        priority = _VIA_PRIORITY.get(via.partition(":")[0], len(_VIA_PRIORITY))
        key = str(addr)
        cand = self._by_ip.get(key)
        if cand is not None:
            if priority < cand.priority:
                cand.via, cand.priority = via, priority
                if cand.state not in (CONNECTING, CONNECTED):
                    cand.port = port
            cand.fleet |= fleet
            cand.trusted |= trusted
            if ua_hint and not cand.ua_hint:
                cand.ua_hint = _clean(ua_hint)
            return cand
        if len(self._by_ip) >= self.cap:
            victim = self._victim(priority)
            if victim is None:
                return None
            del self._by_ip[victim.ip]
        cand = Candidate(
            ip=key,
            port=port,
            via=via,
            priority=priority,
            fleet=fleet,
            trusted=trusted,
            ua_hint=_clean(ua_hint) if ua_hint else None,
        )
        self._by_ip[key] = cand
        return cand

    def _victim(self, priority: int) -> Candidate | None:
        """Return the candidate a newcomer of `priority` may replace in a full book, or None.

        Only untrusted, idle candidates that never completed a handshake qualify:
        one of lower priority, or of equal priority that has already failed.
        """
        victim: Candidate | None = None
        for cand in self._by_ip.values():
            if cand.trusted or cand.handshaked or cand.state in (CONNECTING, CONNECTED):
                continue
            if cand.priority < priority or (cand.priority == priority and not cand.failures):
                continue
            if victim is None or (cand.priority, cand.failures) > (victim.priority, victim.failures):
                victim = cand
        return victim

    def due(self, now: float) -> Candidate | None:
        """Return the highest-priority idle candidate whose retry time has come, or None."""
        best: Candidate | None = None
        for cand in self._by_ip.values():
            if cand.state in (CONNECTING, CONNECTED) or cand.next_attempt > now:
                continue
            if best is None or (cand.priority, cand.next_attempt) < (best.priority, best.next_attempt):
                best = cand
        return best

    def start(self, cand: Candidate) -> None:
        """Mark a connection attempt as started."""
        cand.state = CONNECTING

    def connected(self, cand: Candidate) -> None:
        """Mark a completed handshake."""
        cand.state = CONNECTED
        cand.handshaked = True

    def finish(self, cand: Candidate, now: float, *, ok: bool, error: str | None) -> float:
        """Record the end of an attempt or session and schedule the next one; return the delay.

        `ok` (a stable session) resets the failure count and waits `min_backoff`.
        Otherwise the delay doubles per consecutive failure up to `max_backoff`,
        and after `unreachable_after` failures the peer is retried every
        `unreachable_retry`.
        """
        t = self.timings
        if ok:
            cand.failures = 0
            delay, cand.state = t.min_backoff, BACKOFF
        else:
            cand.failures += 1
            if cand.failures >= t.unreachable_after:
                delay, cand.state = t.unreachable_retry, UNREACHABLE
            else:
                delay, cand.state = min(t.max_backoff, t.min_backoff * 2 ** (cand.failures - 1)), BACKOFF
        cand.next_attempt = now + delay
        cand.last_error = error
        return delay

    def evict(self) -> int:
        """Drop unreachable gossip candidates once the book is nearly full; return how many went."""
        if len(self._by_ip) < self.cap * EVICT_AT:
            return 0
        victims = [ip for ip, cand in self._by_ip.items() if cand.state == UNREACHABLE and not cand.trusted]
        for ip in victims:
            del self._by_ip[ip]
        return len(victims)

    def counts(self) -> dict[str, int]:
        """Return the number of candidates in each state."""
        return dict(Counter(cand.state for cand in self._by_ip.values()))


@dataclass(slots=True, eq=False)
class _Request:
    """One queued exchange with a peer; `future`, `nonce` and `sent_at` belong to the round in flight."""

    kind: str  # _POLL | _GETDATA
    hash: str | None = None
    reason: str | None = None
    announced: bool = False
    future: asyncio.Future[tuple[str, Any]] | None = None
    nonce: int | None = None
    sent_at: float = 0.0
    headers: list[BlockHeader] | None = None  # the reply of the poll round in flight


@dataclass(slots=True)
class _Announce:
    """The first announcement of a block and the Zebra version groups already probed for it."""

    first_at: float
    first_source: str
    sampled: bool
    groups: set[str] = field(default_factory=set)
    backdated: bool = False


@dataclass(slots=True, eq=False)
class _Fetch:
    """An outstanding block-body fetch and the peers already tried."""

    hash: str
    created: float  # monotonic
    height_hint: int | None = None
    prefer: frozenset[str] = frozenset()
    inv_at: float | None = None
    tried: set[str] = field(default_factory=set)
    assigned: str | None = None
    announcer: str | None = None  # IP whose announcement started this fetch


class _Peer:
    """One live connection: framing, handshake, immediate replies and a serialized request queue."""

    def __init__(
        self,
        observer: P2PObserver,
        cand: Candidate,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        passive: bool,
    ) -> None:
        """Wrap an open TCP connection to `cand`; `passive` peers (sweeps) never probe or poll on their own."""
        self.obs = observer
        self.cand = cand
        self.source = cand.source
        self.reader = reader
        self.writer = writer
        self.passive = passive
        self.negotiated = PROTOCOL_VERSION
        self.handshaked = False
        self.ready_at: float | None = None  # monotonic time the handshake completed
        self.queue: deque[_Request] = deque()
        self.wake = asyncio.Event()
        self.poll_queued = False
        self.current: _Request | None = None
        self.announced: str | None = None  # latest inv hash not yet confirmed by a poll
        self.pings: OrderedDict[int, float] = OrderedDict()  # outstanding ping nonce -> monotonic send time
        self.no_common_until = 0.0  # monotonic; see Timings.no_common_repoll
        self.last_ok_write = 0.0
        self.closed = False
        self.inv_tokens = float(INV_BURST)
        self.inv_refilled = time.monotonic()
        self.misbehaved = False  # a session ended for abuse counts as a failure, however long it lasted

    def spend_inv(self, count: int, now: float) -> bool:
        """Charge `count` announced block hashes to the peer's budget; False once it is spent."""
        self.inv_tokens = min(float(INV_BURST), self.inv_tokens + (now - self.inv_refilled) / INV_REFILL)
        self.inv_refilled = now
        if self.inv_tokens < count:
            return False
        self.inv_tokens -= count
        return True

    async def send(self, command: str, payload: bytes = b"") -> None:
        """Write one framed message, waiting a bounded time for the socket buffer to drain."""
        self.writer.write(frame(self.obs.params.magic, command, payload))
        async with asyncio.timeout(self.obs.timings.write_timeout):
            await self.writer.drain()

    async def read_message(self) -> tuple[str, bytes]:
        """Read one framed message; WireError on bad magic, an oversize payload or a bad checksum."""
        limit = MAX_PAYLOAD if self.handshaked else MAX_PAYLOAD_PRE_HANDSHAKE
        header = await self.reader.readexactly(HEADER_LEN)
        command, length, checksum = parse_frame_header(self.obs.params.magic, header, limit)
        payload = await self.reader.readexactly(length) if length else b""
        if not verify_checksum(payload, checksum):
            raise WireError(f"bad checksum on {command}")
        return command, payload

    async def handshake(self) -> None:
        """Send our version at once, then read until the peer's version and verack arrive."""
        obs, cand = self.obs, self.cand
        nonce = obs._rng.getrandbits(64)
        best = obs.monitor.chain.best_tip()
        ours = VersionMsg(
            version=PROTOCOL_VERSION,
            services=0,
            timestamp=int(time.time()),
            recv_ip=cand.ip,
            recv_port=cand.port,
            nonce=nonce,
            user_agent=USER_AGENT,
            start_height=best.height if best is not None else 0,
            relay=False,
        )
        got_version = got_verack = False
        try:
            async with asyncio.timeout(obs.timings.handshake_timeout):
                await self.send("version", encode_version(ours))
                while not (got_version and got_verack):
                    command, payload = await self.read_message()
                    if command == "version":
                        if got_version:
                            raise PeerError("duplicate version")
                        self._accept_version(decode_version(payload), nonce)
                        got_version = True
                        await self.send("verack")
                    elif command == "verack":
                        got_verack = True
                    # Like Zakura, skip anything else until the handshake completes.
        except TimeoutError:
            raise PeerError("handshake timeout") from None
        self.handshaked = True
        self.ready_at = time.monotonic()

    def _accept_version(self, version: VersionMsg, our_nonce: int) -> None:
        """Record the peer's version message and the negotiated protocol version."""
        if version.nonce == our_nonce:
            raise PeerError("connected to ourselves")
        cand = self.cand
        cand.user_agent = _clean(version.user_agent)
        cand.impl, cand.impl_version = classify_user_agent(version.user_agent)
        cand.protocol_version = version.version
        cand.services = version.services
        cand.start_height = version.start_height
        # getheaders must carry exactly this version or Zakura drops the connection.
        self.negotiated = min(PROTOCOL_VERSION, version.version)

    async def serve(self) -> None:
        """Run the reader, the request worker and the poll timer until the connection ends; raise why."""
        loops = [self.read_loop(), self.work_loop()]
        if not self.passive:
            loops.append(self.poll_loop())
        tasks = [asyncio.create_task(loop) for loop in loops]
        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        # The reader comes first, so its error (usually the most telling) wins.
        for task in tasks:
            if task.done() and not task.cancelled() and task.exception() is not None:
                raise task.exception()
        raise PeerError("connection closed")

    async def read_loop(self) -> None:
        """Read and dispatch messages until the connection fails or goes idle."""
        try:
            while True:
                try:
                    async with asyncio.timeout(self.obs.timings.idle_timeout):
                        command, payload = await self.read_message()
                except TimeoutError:
                    raise PeerError("idle timeout") from None
                await self.handle(command, payload)
        finally:
            self._fail_current()

    async def handle(self, command: str, payload: bytes) -> None:
        """React to one post-handshake message: answer requests at once, route replies, record announcements."""
        obs = self.obs
        if command == "ping":
            await self.send("pong", encode_ping(decode_ping(payload)))
        elif command == "pong":
            self._on_pong(decode_ping(payload))
        elif command == "headers":
            headers = decode_headers(payload)
            req = self.current
            waiting = req is not None and req.kind == _POLL and req.future is not None and not req.future.done()
            if waiting and req.headers is None:
                req.headers = headers
        elif command == "inv":
            obs._on_inv(self, decode_inv(payload))
        elif command == "block":
            obs._on_block(self, payload)
        elif command == "notfound":
            req = self.current
            if req is not None and req.kind == _GETDATA and req.hash in inv_block_hashes(decode_inv(payload)):
                self._resolve(_GETDATA, NOTFOUND, None)
        elif command == "getdata":
            await self.send("notfound", encode_inv(decode_inv(payload)))
        elif command == "getheaders":
            await self.send("headers", encode_empty_headers())
        elif command in ("getblocks", "mempool"):
            await self.send("inv", encode_empty_inv())
        elif command == "getaddr":
            await self.send("addr", encode_empty_addr())
        elif command == "addr":
            obs._on_addr(self, decode_addr(payload))
        elif command == "addrv2":
            obs._on_addr(self, decode_addrv2(payload))
        # A repeated version/verack, reject, sendheaders, feefilter and unknown commands are ignored.

    def _on_pong(self, nonce: int) -> None:
        """Measure RTT (including the peer's getheaders work) and end the poll round this ping belongs to."""
        sent_at = self.pings.pop(nonce, None)
        if sent_at is None:
            return
        self.cand.rtt = time.monotonic() - sent_at
        req = self.current
        if req is not None and req.nonce == nonce:
            self._resolve(_POLL, "pong", None)

    def _resolve(self, kind: str, outcome: str, value: Any) -> None:
        """Complete the in-flight request of type `kind` with (`outcome`, `value`) if it is still waiting."""
        req = self.current
        if req is not None and req.kind == kind and req.future is not None and not req.future.done():
            req.future.set_result((outcome, value))

    def _fail_current(self) -> None:
        """Wake the in-flight request with a PeerError once the connection is gone."""
        req = self.current
        future = req.future if req is not None else None
        if future is not None and not future.done():
            future.set_exception(PeerError("connection closed"))
            future.exception()  # mark retrieved: its waiter may already be cancelled

    async def work_loop(self) -> None:
        """Execute queued requests one at a time (one outstanding request per peer)."""
        while True:
            while not self.queue:
                self.wake.clear()
                await self.wake.wait()
            await self.execute(self.queue.popleft())

    async def execute(self, req: _Request) -> None:
        """Run one request, making it the target of reply routing while it is in flight."""
        if req.kind == _POLL:
            self.poll_queued = False
        self.current = req
        try:
            if req.kind == _POLL:
                await self.obs._poll(self)
            else:
                await self.obs._getdata(self, req)
        finally:
            self.current = None

    async def poll_loop(self) -> None:
        """Queue a tip poll now and then every poll interval, jittered so peers do not align."""
        while True:
            self.enqueue_poll()
            await asyncio.sleep(self.obs.poll_interval * (0.9 + 0.2 * self.obs._rng.random()))

    def enqueue(self, req: _Request) -> bool:
        """Queue a request for the worker; False if the connection is closed or the queue is full."""
        if self.closed or len(self.queue) >= MAX_PEER_QUEUE:
            return False
        self.queue.append(req)
        self.wake.set()
        return True

    def enqueue_poll(self) -> None:
        """Queue a tip poll unless one is already waiting (polls coalesce)."""
        if not self.poll_queued and self.enqueue(_Request(_POLL)):
            self.poll_queued = True

    async def exchange(
        self, req: _Request, command: str, payload: bytes, *, ping: bool, timeout: float
    ) -> tuple[str, Any]:
        """Send one request (plus a ping sentinel) and wait for the reply the reader routes to `req`."""
        if self.closed:
            raise PeerError("connection closed")
        req.future = asyncio.get_running_loop().create_future()
        req.nonce = self.obs._rng.getrandbits(64) if ping else None
        req.sent_at = time.monotonic()
        await self.send(command, payload)
        if ping:
            self.pings[req.nonce] = req.sent_at
            if len(self.pings) > MAX_PINGS:
                self.pings.popitem(last=False)
            await self.send("ping", encode_ping(req.nonce))
        async with asyncio.timeout(timeout):
            return await req.future

    async def getheaders(self, entries: Sequence[tuple[str, int]]) -> list[BlockHeader]:
        """Ask for the headers after `entries`, waiting for the ping sentinel; [] if the peer has none past them."""
        req = self.current
        if req is None or req.kind != _POLL:
            raise RuntimeError("getheaders outside a poll request")
        payload = encode_getheaders(self.negotiated, [block_hash for block_hash, _ in entries[:MAX_LOCATOR_HASHES]])
        req.headers = None
        try:
            await self.exchange(req, "getheaders", payload, ping=True, timeout=self.obs.timings.poll_timeout)
        except TimeoutError:
            raise PeerError("getheaders timeout") from None
        return req.headers or []

    async def aclose(self) -> None:
        """Close the socket (idempotent) and wait briefly for the close to complete."""
        self.closed = True
        self._fail_current()
        if not self.writer.is_closing():
            self.writer.close()
        with contextlib.suppress(Exception):
            async with asyncio.timeout(1.0):
                await self.writer.wait_closed()


class P2PObserver:
    """Connects to peers, tracks their tips and announcements, and probes block availability."""

    def __init__(
        self,
        params: NetworkParams,
        monitor: Any,
        *,
        max_peers: int = 300,
        connect_rate: float = 5.0,
        poll_interval: float = 15.0,
        probe_sample: float = 0.2,
        dns_seeds: Iterable[str] = (),
        static_peers: Iterable[str] = (),
        fleet_hosts: Iterable[str] = (),
        timings: Timings | None = None,
        rng: random.Random | None = None,
        open_connection: Opener | None = None,
        resolver: Resolver | None = None,
    ) -> None:
        """Configure the observer; nothing connects until `run()` or `sweep()`.

        `monitor` provides `chain`, `store`, `ingest_headers`, `ingest_block`
        and `observe_tip`, and optionally `record_sighting`. `open_connection`
        and `resolver` replace asyncio's TCP connect and DNS lookup in tests.
        """
        if connect_rate <= 0 or poll_interval <= 0:
            raise ValueError("connect_rate and poll_interval must be positive")
        self.params = params
        self.monitor = monitor
        self.max_peers = max(0, int(max_peers))
        self.connect_rate = float(connect_rate)
        self.poll_interval = float(poll_interval)
        self.probe_sample = min(1.0, max(0.0, float(probe_sample)))
        self.dns_seeds = tuple(dns_seeds)
        self.static_peers = tuple(static_peers)
        self.fleet_hosts = frozenset(host.lower() for host in fleet_hosts)
        self.timings = timings or Timings()
        self.book = CandidateBook(self.timings)
        self.stats: Counter[str] = Counter()
        self._rng = rng or random.Random()
        self._open_connection: Opener = open_connection or asyncio.open_connection
        self._resolver: Resolver = resolver or _getaddrinfo
        self._peers: dict[str, _Peer] = {}  # ip -> handshaked peer
        self._sessions: dict[str, asyncio.Task[None]] = {}  # ip -> connection task
        self._host_ips: dict[str, set[str]] = {}  # static/fleet hostname -> resolved IPs
        self._announces: OrderedDict[str, _Announce] = OrderedDict()
        self._fetches: OrderedDict[str, _Fetch] = OrderedDict()
        self._running = False
        # (bits, time) entries the expected difficulty needs: the parent and its ancestors.
        self._context_len = params.averaging_window + params.median_span

    @classmethod
    def from_config(cls, params: NetworkParams, monitor: Any, config: Any, **kwargs: Any) -> P2PObserver:
        """Build an observer from a `config.Config` (its [p2p] table and fleet RPC hosts)."""
        p2p = config.p2p
        return cls(
            params,
            monitor,
            max_peers=p2p.max_peers,
            connect_rate=p2p.connect_rate,
            poll_interval=p2p.poll_interval,
            probe_sample=p2p.probe_sample,
            dns_seeds=p2p.dns_seeds,
            static_peers=p2p.static_peers,
            fleet_hosts=config.fleet_hosts,
            **kwargs,
        )

    # -- public API ------------------------------------------------------------------

    async def run(self) -> None:
        """Seed candidates, then hold up to `max_peers` connections (`connect_rate` new per second) until cancelled."""
        self._running = True
        seeder = asyncio.create_task(self._seed_loop())
        interval = 1.0 / self.connect_rate
        next_maintenance = 0.0
        try:
            while True:
                now = time.monotonic()
                if now >= next_maintenance:
                    self._maintain(now)
                    next_maintenance = now + 1.0
                if len(self._sessions) < self.max_peers:
                    cand = self.book.due(now)
                    if cand is not None:
                        self.book.start(cand)
                        self._sessions[cand.ip] = asyncio.create_task(self._run_session(cand))
                await asyncio.sleep(interval)
        finally:
            self._running = False
            tasks = [seeder, *self._sessions.values()]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def sweep(self, limit: int = 200, *, concurrency: int = 32) -> list[dict[str, Any]]:
        """One-shot survey (`probe-peers --once`): dial up to `limit` due candidates, poll each tip once, hang up.

        Candidates come from fleet hosts, static peers, DNS seeds and anything
        added with `add_candidates()`, fleet and static first. Tips, headers
        and sources rows reach the monitor exactly as in a long-running session.
        """
        await self._seed()
        now = time.monotonic()
        due = sorted(
            (c for c in self.book.values() if c.state not in (CONNECTING, CONNECTED) and c.next_attempt <= now),
            key=lambda c: (c.priority, c.ip),
        )[: max(0, limit)]
        for cand in due:
            self.book.start(cand)
        gate = asyncio.Semaphore(max(1, concurrency))

        async def one(cand: Candidate) -> dict[str, Any]:
            """Survey one candidate under the concurrency gate."""
            async with gate:
                return await self._sweep_one(cand)

        return list(await asyncio.gather(*(one(cand) for cand in due)))

    def add_candidates(self, entries: Iterable[Sequence[Any]], via: str = "getpeerinfo") -> int:
        """Add (ip, port[, user_agent[, via]]) peers, e.g. from getpeerinfo; return how many were new.

        A missing or zero port means the network's default port; a digit string is accepted.
        """
        before = len(self.book)
        for entry in itertools.islice(entries, MAX_CANDIDATES):
            try:
                ip, port = entry[0], entry[1] or self.params.default_port
            except (TypeError, IndexError, KeyError):
                continue
            if isinstance(port, str) and port.isdigit() and len(port) <= 5:
                port = int(port)
            ua_hint = entry[2] if len(entry) > 2 else None
            source = str(entry[3]) if len(entry) > 3 and entry[3] else via
            self.book.add(ip, port, source, ua_hint=ua_hint)
        return len(self.book) - before

    def request_block(self, hash: str, height_hint: int | None = None, prefer_host: str | None = None) -> bool:
        """Fetch a block body: from the peer at `prefer_host` if connected, else a Zakura peer (fleet first), else any.

        Each attempt is recorded as a probe with reason "fetch". Returns True when
        a trusted body is already known or the fetch is queued, False for a malformed
        hash or when too many fetches are pending. A block held with an untrusted body
        is fetched again, but only a fleet peer's body replaces it.
        """
        block_hash = _block_hash(hash)
        if block_hash is None:
            return False
        if self._has_body(block_hash, trusted=True):
            return True
        hint = height_hint if _is_int(height_hint) and 0 <= height_hint <= MAX_HEIGHT else None
        return self._start_fetch(block_hash, height_hint=hint, prefer=self._prefer_ips(prefer_host))

    def snapshot(self) -> dict[str, Any]:
        """Return a JSON-able view for the UI: counts by state and implementation, totals and per-peer state."""
        connected = [peer.cand for peer in self._peers.values()]
        known = sorted(
            (cand for cand in self.book.values() if cand.handshaked),
            key=lambda c: (c.state != CONNECTED, c.impl or "", c.impl_version or "", c.ip),
        )
        return {
            "running": self._running,
            "connected": len(connected),
            "connecting": len(self._sessions) - len(self._peers),
            "candidates": len(self.book),
            "candidate_states": self.book.counts(),
            "connected_by_impl": dict(Counter(cand.impl or "other" for cand in connected)),
            "connected_by_version": dict(
                Counter(f"{cand.impl or 'other'} {cand.impl_version or '?'}" for cand in connected)
            ),
            "pending_fetches": len(self._fetches),
            "stats": dict(self.stats),
            "peers": [self._peer_view(cand) for cand in known[:MAX_SNAPSHOT_PEERS]],
        }

    # -- connections -----------------------------------------------------------------

    async def _run_session(self, cand: Candidate) -> None:
        """Connect to `cand`, serve the connection until it ends, then schedule the next attempt."""
        peer: _Peer | None = None
        error: str | None = None
        shutdown = False
        try:
            peer = await self._open_peer(cand, passive=False)
            self._peers[cand.ip] = peer
            now = time.monotonic()
            if cand.last_getaddr is None or now - cand.last_getaddr >= self.timings.getaddr_interval:
                cand.last_getaddr = now
                await peer.send("getaddr")
            await peer.serve()
        except asyncio.CancelledError:
            shutdown = True
            raise
        except Exception as exc:
            error = _describe(exc)
        finally:
            if self._sessions.get(cand.ip) is asyncio.current_task():
                del self._sessions[cand.ip]
            await self._end_session(cand, peer, error, shutdown=shutdown)

    async def _open_peer(self, cand: Candidate, *, passive: bool) -> _Peer:
        """Open a TCP connection, complete the handshake and write the peer's sources row."""
        try:
            async with asyncio.timeout(self.timings.connect_timeout):
                reader, writer = await self._open_connection(cand.ip, cand.port)
        except TimeoutError:
            raise PeerError("connect timeout") from None
        peer = _Peer(self, cand, reader, writer, passive=passive)
        try:
            await peer.handshake()
        except BaseException:
            await peer.aclose()
            raise
        self.book.connected(cand)
        self.stats["handshakes"] += 1
        now = time.time()
        cand.connected_at = now
        self._upsert(
            cand,
            kind="p2p",
            impl=cand.impl,
            impl_version=cand.impl_version,
            user_agent=cand.user_agent,
            protocol_version=cand.protocol_version,
            services=cand.services,
            discovered_via=cand.via,
            start_height=cand.start_height,
            last_ok_at=now,
            status=CONNECTED,
        )
        return peer

    async def _end_session(self, cand: Candidate, peer: _Peer | None, error: str | None, *, shutdown: bool) -> None:
        """Close the connection, release its fetches and record the outcome in the book and sources."""
        if peer is not None:
            if self._peers.get(cand.ip) is peer:
                del self._peers[cand.ip]
            await peer.aclose()
            for fetch in self._fetches.values():
                if fetch.assigned == cand.ip:
                    fetch.assigned = None
        now = time.monotonic()
        stable = (
            peer is not None
            and not peer.misbehaved
            and peer.ready_at is not None
            and now - peer.ready_at >= self.timings.stable_session
        )
        if shutdown:
            cand.state = IDLE
        else:
            self.stats["connect_failed" if peer is None else "sessions_ended"] += 1
            self.book.finish(cand, now, ok=stable, error=error)
        if cand.handshaked:
            fields: dict[str, Any] = {"status": cand.state}
            if error:
                fields.update(last_error=error, last_error_at=time.time())
            self._upsert(cand, **fields)
        log.debug("p2p %s ended (%s): %s", cand.source, cand.state, error)

    async def _sweep_one(self, cand: Candidate) -> dict[str, Any]:
        """Handshake with one candidate, poll its tip once, disconnect and describe the outcome."""
        started = time.monotonic()
        peer: _Peer | None = None
        error: str | None = None
        try:
            peer = await self._open_peer(cand, passive=True)
            reader = asyncio.create_task(peer.read_loop())
            try:
                await peer.execute(_Request(_POLL))
            except PeerError:
                # A dead reader explains the failure better than "connection closed".
                if reader.done() and not reader.cancelled() and reader.exception() is not None:
                    raise reader.exception() from None
                raise
            finally:
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)
        except Exception as exc:
            error = _describe(exc)
        finally:
            if peer is not None:
                await peer.aclose()
            self.book.finish(cand, time.monotonic(), ok=peer is not None and error is None, error=error)
            if cand.handshaked:
                fields: dict[str, Any] = {"status": cand.state}
                if error:
                    fields.update(last_error=error, last_error_at=time.time())
                self._upsert(cand, **fields)
        view = self._peer_view(cand)
        view["error"] = error
        view["elapsed"] = round(time.monotonic() - started, 3)
        view["relation"] = self._relation(cand.tip_hash)
        return view

    # -- tip polling -----------------------------------------------------------------

    async def _poll(self, peer: _Peer) -> None:
        """Learn the peer's tip with getheaders + ping, following up on empty replies and continuing full batches."""
        if peer.no_common_until > time.monotonic():
            return
        locator = self._locator_for(peer)
        if not locator:
            return
        self.stats["polls"] += 1
        # An empty reply means the peer's tip is the first locator entry on its chain.
        # Dropping the head, then the dense part, turns that into headers that name the tip;
        # the last round, without the previous round's head, confirms a peer sitting on it.
        rounds = [locator]
        for tail in (locator[1:], locator[DENSE_LOCATOR:]):
            if not tail:
                break
            rounds.append(tail)
        rounds.append(rounds[-1][1:])
        for attempt in rounds:
            if not attempt:
                break
            headers = await peer.getheaders(attempt)
            if headers:
                await self._accept_headers(peer, locator, headers)
                return
        # Silence is no tip: Zakura also sends nothing for a request it sheds, times out or
        # gets during its setup, yet still answers the ping.
        self.stats["polls_unanswered"] += 1
        peer.cand.tip_note = "no headers in reply"

    async def _accept_headers(self, peer: _Peer, locator: list[tuple[str, int]], headers: list[BlockHeader]) -> None:
        """Validate and ingest a non-empty headers reply, fetch continuations, then record the peer's tip."""
        known = dict(locator)
        genesis = self.params.genesis_hash
        tip: tuple[str, int] | None = None
        for round_ in range(MAX_CONTINUATIONS + 1):
            anchor = headers[0].prev_hash
            base = known.get(anchor)
            if base is None:
                raise PeerError("headers do not connect to the locator")
            if anchor == genesis and genesis not in self.monitor.chain:
                # Nothing in our window is on the peer's chain: far behind, or on an ancient fork.
                peer.cand.tip_note = "no common block in window"
                peer.no_common_until = time.monotonic() + self.timings.no_common_repoll
                self.stats["polls_no_common"] += 1
                return
            self._check_headers(anchor, base, headers)
            at = time.time()
            self.stats["headers"] += len(headers)
            self._call(self.monitor.ingest_headers, headers, source=peer.source, at=at)
            self._backdate(header.hash for header in headers)
            tip = (headers[-1].hash, base + len(headers))
            if len(headers) < MAX_HEADERS or round_ == MAX_CONTINUATIONS:
                break
            known[tip[0]] = tip[1]
            headers = await peer.getheaders([tip, *locator[: MAX_LOCATOR_HASHES - 1]])
            if not headers:
                break
        if tip is not None:
            self._set_tip(peer, tip[0], tip[1], time.time())

    def _check_headers(self, anchor: str, height: int, headers: list[BlockHeader]) -> None:
        """Raise PeerError unless the headers chain from `anchor` (at `height`) and each new one is valid."""
        context = self._ancestry(anchor)
        now = time.time()
        prev = anchor
        for header in headers:
            if header.prev_hash != prev:
                raise PeerError("headers are not a chain")
            height += 1
            # A header the chain holds was checked (or came from a fleet node) when it was added.
            if header.hash not in self.monitor.chain:
                self._check_header(header, height, context, now)
            context.insert(0, (header.bits, header.time))
            del context[self._context_len :]
            prev = header.hash

    def _check_header(
        self, header: BlockHeader, height: int | None, context: list[tuple[int, int]], now: float
    ) -> None:
        """Raise PeerError unless `header`, at `height` on a parent whose ancestry is `context`, is valid.

        Valid means dated at most 2 h ahead, nBits as `expected_bits` predicts, and
        passing `check_pow`. `context` holds (bits, time) of the parent and its
        ancestors, newest first (see `_ancestry`); nBits is checked only when it is complete.
        """
        if header.time > now + MAX_FUTURE_BLOCK_TIME:
            raise PeerError(f"header {header.hash} is dated more than 2 h ahead")
        if (
            height is not None
            and len(context) >= self._context_len
            and header.bits != expected_bits(self.params, height, header.time, context)
        ):
            raise PeerError(f"header {header.hash} has the wrong difficulty")
        if not check_pow(header, self.params):
            raise PeerError(f"header {header.hash} fails proof of work")

    def _ancestry(self, block_hash: str) -> list[tuple[int, int]]:
        """Return (bits, time) of `block_hash` and its ancestors, newest first, as many as `expected_bits` needs."""
        chain = self.monitor.chain
        node = chain.get(block_hash)
        context: list[tuple[int, int]] = []
        while node is not None and len(context) < self._context_len:
            context.append((node.bits, node.time))
            node = _parent(chain, node)
        return context

    def _locator_for(self, peer: _Peer) -> list[tuple[str, int]]:
        """Build the poll locator, headed by the parent of the block we expect the peer to be at.

        That parent makes a peer at the expected block answer with that block's
        header (confirming which block it holds at that height) rather than with nothing.
        """
        chain = self.monitor.chain
        best = chain.best_tip()
        if best is None:
            return []
        for expected in (peer.announced, peer.cand.tip_hash):
            node = chain.get(expected) if expected else None
            parent = _parent(chain, node) if node is not None else None
            if parent is not None:
                return build_locator(chain, parent, self.params.genesis_hash)
        height = best.height
        start = peer.cand.start_height
        if start and 0 < start < height:
            height = start
        head_hash = chain.canonical_hash_at(height - 1)
        head = chain.get(head_hash) if head_hash is not None else None
        return build_locator(chain, head or best, self.params.genesis_hash)

    def _set_tip(self, peer: _Peer, tip_hash: str, height: int, at: float) -> None:
        """Record the peer's confirmed tip; report a change to the monitor and the sources row."""
        cand = peer.cand
        cand.tip_at, cand.tip_note = at, None
        if peer.announced == tip_hash:
            peer.announced = None
        if tip_hash == cand.tip_hash:
            if at - peer.last_ok_write >= 60.0:
                peer.last_ok_write = at
                self._upsert(cand, last_ok_at=at)
            return
        cand.tip_hash, cand.tip_height = tip_hash, height
        peer.last_ok_write = at
        self.stats["tip_changes"] += 1
        self._call(self.monitor.observe_tip, peer.source, tip_hash, at, height_hint=height)
        self._upsert(cand, tip_hash=tip_hash, tip_height=height, tip_at=at, tip_via="p2p:getheaders", last_ok_at=at)

    # -- announcements, probes and fetches ----------------------------------------------

    def _on_inv(self, peer: _Peer, items: list[tuple[int, bytes]]) -> None:
        """Record block announcements, probe or sample their availability and poll the announcer's tip.

        A peer that announces more block hashes than its budget allows is disconnected.
        """
        hashes = inv_block_hashes(items)
        if not hashes:
            self.stats["inv_other"] += 1  # transaction announcements are ignored
            return
        hashes = hashes[-MAX_INV_BLOCKS:]
        if not peer.spend_inv(len(hashes), time.monotonic()):
            peer.misbehaved = True
            raise PeerError("too many block announcements")
        at = time.time()
        for block_hash in hashes:
            self.stats["inv_blocks"] += 1
            self._sighting(block_hash, peer.source, "inv", at)
            peer.announced = block_hash
            peer.cand.last_inv_hash, peer.cand.last_inv_at = block_hash, at
            if not peer.passive:
                self._probe_announcement(peer, block_hash, at)
        if not peer.passive:
            peer.enqueue_poll()

    def _probe_announcement(self, peer: _Peer, block_hash: str, at: float) -> None:
        """Fetch an unknown announced block from its announcer, or sample a Zebra announcer for availability."""
        record = self._announces.get(block_hash)
        if record is None:
            record = _Announce(at, peer.source, self._rng.random() < self.probe_sample)
            self._announces[block_hash] = record
            if len(self._announces) > MAX_TRACKED_BLOCKS:
                self._announces.popitem(last=False)
        group = peer.cand.probe_group
        if block_hash not in self.monitor.chain and block_hash not in self._fetches:
            if self._start_fetch(block_hash, peer=peer, inv_at=at) and group is not None:
                record.groups.add(group)
        elif (
            record.sampled
            and group is not None
            and group not in record.groups
            and peer.enqueue(_Request(_GETDATA, hash=block_hash, reason=REPROBE, announced=True))
        ):
            record.groups.add(group)

    def _on_block(self, peer: _Peer, payload: bytes) -> None:
        """Route a `block` message to the getdata waiting for it, or to a fetch that asked this peer earlier.

        Blocks nobody asked this peer for are ignored.
        """
        try:
            block = parse_block(payload, self.params)
        except ParseError as exc:
            peer._resolve(_GETDATA, ERROR, f"unparseable block: {exc}")
            return
        block_hash = block.header.hash
        req = peer.current
        if req is not None and req.kind == _GETDATA and req.hash == block_hash:
            self._check_block(block)
            peer._resolve(_GETDATA, BLOCK, block)
        elif (fetch := self._fetches.get(block_hash)) is not None and peer.cand.ip in fetch.tried:
            # A late reply to a getdata that timed out.
            self._check_block(block)
            self._accept_block(peer, block, FETCH)

    def _check_block(self, block: Block) -> None:
        """Raise PeerError unless a new block's header is valid and the coinbase height matches the chain.

        Nothing binds the other coinbase fields to the header; see `chain.Node.body_trusted`.
        """
        header = block.header
        chain = self.monitor.chain
        node = chain.get(header.hash)
        if node is not None:
            height: int | None = node.height
        else:
            parent = chain.get(header.prev_hash)
            height = parent.height + 1 if parent is not None else None
            self._check_header(header, height, self._ancestry(header.prev_hash), time.time())
        coinbase = block.coinbase
        if height is not None and coinbase is not None and coinbase.height != height:
            raise PeerError(f"block {header.hash} has coinbase height {coinbase.height}, not {height}")

    async def _getdata(self, peer: _Peer, req: _Request) -> None:
        """Ask the peer for one block, record the probe, ingest the body, or retry the fetch elsewhere."""
        fetch = self._fetches.get(req.hash)
        if req.reason != REPROBE and (fetch is None or self._has_body(req.hash, trusted=peer.cand.fleet)):
            self._fetches.pop(req.hash, None)
            return
        at, started = time.time(), time.monotonic()
        try:
            outcome, value = await peer.exchange(
                req, "getdata", encode_getdata_blocks([req.hash]), ping=False, timeout=self.timings.fetch_timeout
            )
        except TimeoutError:
            outcome, value = TIMEOUT, None
        latency_ms = round((time.monotonic() - started) * 1000)
        self.stats[f"probe_{outcome}"] += 1
        self._call(
            self.monitor.store.record_probe,
            at=at,
            source=peer.source,
            impl=peer.cand.impl,
            hash=req.hash,
            reason=req.reason,
            result=outcome,
            latency_ms=latency_ms,
            peer_tip_hash=peer.cand.tip_hash,
            announced_by_same_peer=int(req.announced),
        )
        if outcome == BLOCK:
            self._accept_block(peer, value, req.reason)
        elif fetch is not None and req.reason != REPROBE:
            fetch.assigned = None
            if len(fetch.tried) >= MAX_FETCH_ATTEMPTS:
                self._fetches.pop(req.hash, None)
                self.stats["fetch_gave_up"] += 1
            else:
                self._assign_next(fetch)

    def _accept_block(self, peer: _Peer, block: Any, reason: str | None) -> None:
        """Ingest a fetched body unless the chain already has one as trusted, closing any fetch for that hash."""
        block_hash = block.header.hash
        fetch = self._fetches.pop(block_hash, None)
        if self._has_body(block_hash, trusted=peer.cand.fleet):
            return
        # The announcer showed us this block at inv time; later fetches are only when we got it.
        inv_at = fetch.inv_at if fetch is not None and reason == ANNOUNCE else None
        self.stats["blocks"] += 1
        self._call(
            self.monitor.ingest_block,
            block,
            block.header,
            fetch.height_hint if fetch is not None else None,
            source=peer.source,
            kind="getdata",
            at=inv_at if inv_at is not None else time.time(),
            trusted=peer.cand.fleet,
        )
        self._backdate((block_hash,))

    def _start_fetch(
        self,
        block_hash: str,
        *,
        peer: _Peer | None = None,
        inv_at: float | None = None,
        height_hint: int | None = None,
        prefer: frozenset[str] = frozenset(),
    ) -> bool:
        """Track a body fetch and hand it to `peer` (an announce probe) or the best available peer.

        An announce fetch starts only on its announcer, within MAX_ANNOUNCE_FETCHES per
        announcer and outside the FETCH_RESERVE kept for `request_block`.
        """
        fetch = self._fetches.get(block_hash)
        if fetch is not None:
            if prefer and not fetch.prefer:
                fetch.prefer = prefer
            return True
        now = time.monotonic()
        self._expire_fetches(now)
        announcer = peer.cand.ip if peer is not None else None
        if len(self._fetches) >= (MAX_FETCHES if peer is None else MAX_FETCHES - FETCH_RESERVE) or (
            announcer is not None
            and sum(other.announcer == announcer for other in self._fetches.values()) >= MAX_ANNOUNCE_FETCHES
        ):
            self.stats["fetch_dropped"] += 1
            return False
        fetch = _Fetch(block_hash, now, height_hint=height_hint, prefer=prefer, inv_at=inv_at, announcer=announcer)
        self._fetches[block_hash] = fetch
        if peer is None:
            self._assign_next(fetch)
        elif not self._assign(fetch, peer, ANNOUNCE, announced=True):
            del self._fetches[block_hash]
            self.stats["fetch_dropped"] += 1
            return False
        return True

    def _assign(self, fetch: _Fetch, peer: _Peer, reason: str, *, announced: bool) -> bool:
        """Queue `fetch` on `peer`; False if its queue is full or closed."""
        if not peer.enqueue(_Request(_GETDATA, hash=fetch.hash, reason=reason, announced=announced)):
            return False
        fetch.tried.add(peer.cand.ip)
        fetch.assigned = peer.cand.ip
        return True

    def _assign_next(self, fetch: _Fetch) -> bool:
        """Queue `fetch` on the best untried peer: preferred host, Zakura (fleet first), then any after the grace."""
        anyone = time.monotonic() - fetch.created >= self.timings.fetch_grace
        options = [
            peer
            for peer in self._peers.values()
            if not peer.passive
            and not peer.closed
            and peer.cand.ip not in fetch.tried
            and len(peer.queue) < MAX_PEER_QUEUE
            and (anyone or peer.cand.impl == "zakura" or peer.cand.ip in fetch.prefer)
        ]
        if not options:
            return False
        peer = min(
            options,
            key=lambda p: (
                p.cand.ip not in fetch.prefer,
                p.cand.impl != "zakura",
                not p.cand.fleet,
                len(p.queue) + (p.current is not None),
                self._rng.random(),
            ),
        )
        return self._assign(fetch, peer, FETCH, announced=False)

    def _expire_fetches(self, now: float) -> None:
        """Forget fetches older than `fetch_ttl` (the dict is in creation order)."""
        while self._fetches:
            fetch = next(iter(self._fetches.values()))
            if now - fetch.created < self.timings.fetch_ttl:
                break
            self._fetches.popitem(last=False)
            self.stats["fetch_expired"] += 1

    def _maintain(self, now: float) -> None:
        """Once a second: expire fetches, hand unassigned ones to peers, and trim the candidate book."""
        self._expire_fetches(now)
        for fetch in list(self._fetches.values()):
            if fetch.assigned is not None:
                continue
            if len(fetch.tried) >= MAX_FETCH_ATTEMPTS:
                self._fetches.pop(fetch.hash, None)
                self.stats["fetch_gave_up"] += 1
            else:
                self._assign_next(fetch)
        self.stats["candidates_evicted"] += self.book.evict()

    def _backdate(self, hashes: Iterable[str]) -> None:
        """Re-record inv sightings made before the block row existed, so `blocks.first_seen_at` is the inv time."""
        for block_hash in hashes:
            record = self._announces.get(block_hash)
            if record is not None and not record.backdated:
                record.backdated = True
                self._sighting(block_hash, record.first_source, "inv", record.first_at)

    def _on_addr(self, peer: _Peer, entries: list[tuple[str, int, int, int]]) -> None:
        """Add gossiped public unicast addresses on unprivileged ports to the candidate book.

        Each peer adds at most MAX_ADDR_PER_PEER new candidates per `getaddr_interval`.
        """
        self.stats["addr_entries"] += len(entries)
        cand = peer.cand
        now = time.monotonic()
        if cand.addr_window_at is None or now - cand.addr_window_at >= self.timings.getaddr_interval:
            cand.addr_window_at, cand.addr_added = now, 0
        via = f"addr:{cand.ip}"
        for ip, port, _services, _time in entries:
            if port < MIN_GOSSIP_PORT or self.book.get(ip) is not None:
                continue
            if cand.addr_added >= MAX_ADDR_PER_PEER:
                self.stats["addr_dropped"] += 1
                continue
            if self.book.add(ip, port, via) is not None:
                cand.addr_added += 1

    # -- discovery -------------------------------------------------------------------

    async def _seed_loop(self) -> None:
        """Seed now and then again every `dns_refresh` seconds."""
        while True:
            try:
                await self._seed()
            except Exception:
                log.exception("p2p: seeding failed")
            await asyncio.sleep(self.timings.dns_refresh)

    async def _seed(self) -> None:
        """Add fleet hosts, static peers and DNS seed results to the candidate book."""
        port = self.params.default_port
        fleet = sorted(self.fleet_hosts)
        for host, ips in zip(fleet, await asyncio.gather(*(self._resolve(h, port) for h in fleet)), strict=True):
            self._host_ips[host] = set(ips)
            for ip in ips:
                self.book.add(ip, port, "fleet", fleet=True, trusted=True)
        statics: list[tuple[str, int]] = []
        for entry in self.static_peers:
            try:
                statics.append(split_host_port(entry, port))
            except ValueError as exc:
                log.warning("p2p: ignoring static peer %r: %s", entry, exc)
        for (host, peer_port), ips in zip(
            statics, await asyncio.gather(*(self._resolve(h, p) for h, p in statics)), strict=True
        ):
            self._host_ips.setdefault(host.lower(), set()).update(ips)
            for ip in ips:
                self.book.add(ip, peer_port, "static", trusted=True)
        seeds = list(self.dns_seeds)
        for seed, ips in zip(seeds, await asyncio.gather(*(self._resolve(s, port) for s in seeds)), strict=True):
            for ip in ips:
                self.book.add(ip, port, f"dns:{seed}")

    async def _resolve(self, host: str, port: int) -> list[str]:
        """Return up to MAX_RESOLVED_IPS unique IPs for `host` (itself when it is an IP literal); [] on failure."""
        literal = _parse_ip(host)
        if literal is not None:
            return [str(literal)]
        try:
            async with asyncio.timeout(self.timings.dns_timeout):
                found = await self._resolver(host, port)
        except (OSError, TimeoutError, UnicodeError) as exc:
            log.warning("p2p: cannot resolve %s: %s", host, exc)
            return []
        ips: list[str] = []
        for value in found:
            addr = _parse_ip(value)
            if addr is not None and str(addr) not in ips:
                ips.append(str(addr))
        return ips[:MAX_RESOLVED_IPS]

    # -- helpers ---------------------------------------------------------------------

    def _prefer_ips(self, host: str | None) -> frozenset[str]:
        """Map a `prefer_host` (IP literal or a resolved static/fleet hostname) to peer IPs."""
        if not host:
            return frozenset()
        literal = _parse_ip(host)
        if literal is not None:
            return frozenset({str(literal)})
        return frozenset(self._host_ips.get(host.lower(), ()))

    def _has_body(self, block_hash: str, *, trusted: bool = False) -> bool:
        """Return True if the chain holds `block_hash` with a body, with `trusted` only a trusted one (a fleet body)."""
        node = self.monitor.chain.get(block_hash)
        return node is not None and bool(node.body) and (not trusted or bool(node.body_trusted))

    def _relation(self, tip_hash: str | None) -> dict[str, Any] | None:
        """Return the chain's relation of `tip_hash` to the best tip as a dict, or None."""
        if tip_hash is None:
            return None
        relation = self._call(self.monitor.chain.relation, tip_hash)
        return dataclasses.asdict(relation) if dataclasses.is_dataclass(relation) else None

    def _peer_view(self, cand: Candidate) -> dict[str, Any]:
        """Return the JSON-able per-peer view used by `snapshot()` and `sweep()`."""
        return {
            "source": cand.source,
            "ip": cand.ip,
            "port": cand.port,
            "via": cand.via,
            "fleet": cand.fleet,
            "connected": cand.state == CONNECTED,
            "status": cand.state,
            "impl": cand.impl,
            "version": cand.impl_version,
            "user_agent": cand.user_agent,
            "protocol_version": cand.protocol_version,
            "services": cand.services,
            "p2p_v2": bool(cand.services & NODE_P2P_V2) if cand.services is not None else None,
            "start_height": cand.start_height,
            "tip_hash": cand.tip_hash,
            "tip_height": cand.tip_height,
            "tip_at": cand.tip_at,
            "tip_note": cand.tip_note,
            "last_inv_hash": cand.last_inv_hash,
            "last_inv_at": cand.last_inv_at,
            "rtt_ms": round(cand.rtt * 1000, 1) if cand.rtt is not None else None,
            "connected_at": cand.connected_at,
            "failures": cand.failures,
            "last_error": cand.last_error,
        }

    def _upsert(self, cand: Candidate, **fields: Any) -> None:
        """Write fields of `cand`'s sources row."""
        self._call(self.monitor.store.upsert_source, cand.source, **fields)

    def _sighting(self, block_hash: str, source: str, kind: str, at: float) -> None:
        """Record a sighting through the monitor (so it can update the chain) or directly in the store."""
        hook = getattr(self.monitor, "record_sighting", None) or self.monitor.store.record_sighting
        self._call(hook, block_hash, source, kind, at)

    def _call(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Call a monitor or store hook, logging its error instead of tearing down the peer."""
        try:
            return fn(*args, **kwargs)
        except Exception:
            self.stats["hook_errors"] += 1
            log.exception("p2p: %s failed", getattr(fn, "__qualname__", fn))
            return None


def build_locator(
    chain: Any,
    head: Any,
    genesis_hash: str,
    *,
    dense: int = DENSE_LOCATOR,
    max_len: int = MAX_LOCATOR_HASHES,
) -> list[tuple[str, int]]:
    """Return (hash, height) pairs walking back from `head` along its own ancestry.

    The first `dense` entries are consecutive, then the gaps double; the walk
    stops at the bottom of the chain's window and genesis is always last, so a
    peer with no block of ours in common still intersects somewhere.
    """
    entries: list[tuple[str, int]] = []
    node = head
    step = 1
    budget = MAX_SIDE_WALK
    while node is not None and len(entries) < max_len - 1:
        entries.append((node.hash, node.height))
        if len(entries) >= dense:
            step *= 2
        target = node.height - step
        if target < 0:
            break
        # Follow parents until the canonical chain, then jump through its height index.
        while node is not None and node.height > target:
            if chain.canonical_hash_at(node.height) == node.hash:
                canonical = chain.canonical_hash_at(target)
                node = chain.get(canonical) if canonical is not None else None
            elif budget > 0:
                budget -= 1
                node = _parent(chain, node)
            else:
                node = None
    if not entries or entries[-1][0] != genesis_hash:
        entries.append((genesis_hash, 0))
    return entries


def source_key(ip: str, port: int) -> str:
    """Return the `sources.source` key "p2p:<ip>:<port>" (IPv6 in brackets)."""
    host = f"[{ip}]" if ":" in ip else ip
    return f"p2p:{host}:{port}"


def _parent(chain: Any, node: Any) -> Any:
    """Return `node`'s parent if the chain holds it at the height just below, else None."""
    parent = chain.get(node.prev_hash)
    return parent if parent is not None and parent.height == node.height - 1 else None


async def _getaddrinfo(host: str, port: int) -> list[str]:
    """Resolve `host` with the event loop's resolver; return the IP strings."""
    infos = await asyncio.get_running_loop().getaddrinfo(host, port, type=socket.SOCK_STREAM)
    return [str(info[4][0]) for info in infos]


def _parse_ip(value: Any) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
    """Parse an IP literal (brackets and an IPv6 scope allowed), unwrapping IPv4-mapped IPv6; None if invalid."""
    if not isinstance(value, str) or len(value) > 64:
        return None
    text = value.strip().removeprefix("[").removesuffix("]").split("%", 1)[0]
    try:
        addr = ipaddress.ip_address(text)
    except ValueError:
        return None
    if isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped is not None:
        return addr.ipv4_mapped
    return addr


def _port(value: Any) -> int | None:
    """Return `value` if it is a TCP port number (1-65535), else None."""
    return value if _is_int(value) and 1 <= value <= 65_535 else None


def _is_int(value: Any) -> bool:
    """Return True for a plain int (bool excluded)."""
    return isinstance(value, int) and not isinstance(value, bool)


def _block_hash(value: Any) -> str | None:
    """Return a lowercase 64-hex-digit block hash, or None."""
    return value.lower() if isinstance(value, str) and _HASH_RE.fullmatch(value) else None


def _clean(text: Any, limit: int = MAX_TEXT) -> str:
    """Return `text` as printable ASCII (other characters become '?'), capped at `limit` characters."""
    return "".join(ch if " " <= ch <= "~" else "?" for ch in str(text)[:limit])


def _describe(exc: BaseException) -> str:
    """Return a short, bounded description of why a connection attempt or session ended."""
    if isinstance(exc, (PeerError, WireError)):
        text = str(exc)
    elif isinstance(exc, asyncio.IncompleteReadError):
        text = "closed by peer"
    elif isinstance(exc, TimeoutError):
        text = "timeout"
    elif isinstance(exc, ConnectionRefusedError):
        text = "connection refused"
    elif isinstance(exc, ConnectionResetError):
        text = "connection reset"
    elif isinstance(exc, OSError):
        text = f"{type(exc).__name__}: {exc.strerror or exc}"
    else:
        text = f"{type(exc).__name__}: {exc}"
    return _clean(text, MAX_ERROR_TEXT)
