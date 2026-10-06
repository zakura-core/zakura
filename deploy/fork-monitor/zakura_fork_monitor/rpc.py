"""JSON-RPC client, per-endpoint collector and canonical-chain backfill.

`RpcClient` is a small blocking JSON-RPC 2.0 client on stdlib urllib. Every
request carries our User-Agent, redirects are refused and responses are capped
at MAX_RESPONSE_BYTES. Batches use JSON-RPC 2.0 framing, because Zakura rejects
batch items without `"jsonrpc": "2.0"`.

`RpcCollector` polls one endpoint from the asyncio loop (blocking calls run in
`asyncio.to_thread`) and feeds the Monitor with tips, chain tips and peer
candidates. `backfill` loads recent canonical blocks at startup. Monitor
methods may be plain or async. Sighting kinds: "rpc_tip" (the polled tip,
or the node's active `getchaintips` entry), "rpc_walk" (ancestors fetched
behind it, tip first, then downward), "chaintip" and "backfill"; the last two
do not time the block's arrival.

Live quirks handled here:
- `getblock <hash>` fails for side-chain blocks (-8 at verbosity 0), so unknown
  `valid-fork` tips from `getchaintips` go to `monitor.request_block` (P2P
  `getdata`; Zakura serves any retained chain), and a tip that was reorged away
  between `getbestblockhash` and `getblock` is simply retried on the next poll.
- `getbestblockheightandhash` returns the hash as a byte array, so tips are
  read with `getbestblockhash`.
- Zebra 6.x has no `getchaintips`; a -32601 reply disables that duty.

Notes:
- `RpcClient.batch` returns results in call order with each failed call as an
  `RpcError` instance; only a whole-request failure raises. Network, HTTP and
  protocol failures raise `RpcTransportError` (an `RpcError` whose `code` is
  None), and an oversized response raises its subclass `ResponseTooLarge`.
- `backfill` runs in two phases: `getblockhash` over the range (batches of
  HASH_BATCH), then `getblock <hash> 0` for the hashes the store or chain does
  not hold with a trusted body (batches of `batch_size`, halved when a response
  is too large). Skipping by hash also refetches heights that reorged while the
  monitor was down. Blocks are ingested in ascending height order and a
  `BackfillResult` is returned; `name`, `batch_size` and `concurrency` are
  keyword options.
- Every fetched block must pass `consensus.check_pow` (target and Equihash
  solution), as over P2P. A block from NU7 on must not carry a pre-NU7
  coinbase and, when the chain holds its parent with enough ancestors, must
  carry the nBits `expected_bits` predicts (`_check_rules`), so a fleet node
  left on pre-NU7 rules is an error rather than a source of stale blocks.
- Peer candidates are passed as (ip, port, subver, via) tuples; loopback,
  unspecified, multicast and link-local addresses and monitor user agents are
  dropped.
- Extras: `RpcCollector.from_endpoint`, `RpcCollector.health()`, the
  `poll_tip`/`poll_chaintips`/`poll_peers` steps, and the endpoint's
  `getnetworkinfo` subversion stored as the source's impl/version.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import http.client
import inspect
import ipaddress
import json
import logging
import math
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import OrderedDict
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any

from . import __version__
from .config import RpcEndpoint, split_host_port
from .consensus import Block, NetworkParams, ParseError, check_pow, expected_bits, is_pre_nu7_body, parse_block
from .wire import classify_user_agent

log = logging.getLogger(__name__)

USER_AGENT = f"zakura-fork-monitor/{__version__}"
# A 2 MB block is 4 MB of hex; the cap leaves room for a few in one batch.
MAX_RESPONSE_BYTES = 16 * 1024 * 1024
DEFAULT_TIMEOUT = 10.0
# One request never carries more calls than this, whatever the caller asks for.
MAX_BATCH_CALLS = 1_000
MAX_ERROR_TEXT = 256

METHOD_NOT_FOUND = -32601
# Zakura's "not found" / "not in best chain" codes for getblock, getblockhash and getblockheader.
NOT_FOUND_CODES = frozenset({-8, -5, -1})

# Walk prev_hash back at most this far from a new tip.
MAX_WALK_BACK = 2_000
# Walk-back batches start small (usually one parent is missing) and double up to this.
MAX_WALK_BATCH = 50
BACKOFF_MIN = 1.0
BACKOFF_MAX = 30.0
# Unchanged sources rows are rewritten at most this often (1 Hz polls would churn the DB).
STATUS_WRITE_INTERVAL = 15.0
# Unchanged getchaintips rows are rewritten at most this often.
CHAINTIP_WRITE_INTERVAL = 30.0
# zcashd lists every tip since genesis; only the newest entries matter.
MAX_CHAINTIPS = 256
# Chain tips further than this below the best one are too old to fetch or record.
MAX_CHAINTIP_DEPTH = 2_000
# An unknown fork tip is re-requested over P2P after this long, at most REQUEST_ATTEMPTS times.
REQUEST_RETRY = 60.0
REQUEST_ATTEMPTS = 3
MAX_REQUEST_MEMO = 4_096
# Matches the P2P observer's candidate cap.
MAX_PEERS = 5_000
MAX_SUBVER = 256

# getblockhash responses are tiny, so the hash phase batches more per request.
HASH_BATCH = 500
BACKFILL_BATCH = 50
BACKFILL_CONCURRENCY = 4
BACKFILL_ATTEMPTS = 3
BACKFILL_RETRY_DELAY = 1.0
PROGRESS_INTERVAL = 5.0

_HEX = frozenset("0123456789abcdef")
_ZERO_HASH = "0" * 64


class RpcError(Exception):
    """A JSON-RPC error reply (`code`, `message`); `code` is None for transport failures."""

    def __init__(self, code: int | None, message: str) -> None:
        """Store the error code and a length-capped, printable message."""
        self.code = code
        self.message = _clean(message, MAX_ERROR_TEXT)
        super().__init__(f"{code}: {self.message}" if code is not None else self.message)


class RpcTransportError(RpcError):
    """The request failed below JSON-RPC: network, HTTP status, size cap or malformed reply."""

    def __init__(self, message: str) -> None:
        """Wrap a transport failure description."""
        super().__init__(None, message)


class ResponseTooLarge(RpcTransportError):
    """The response body exceeded the client's `max_response_bytes`."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Refuse redirects: urllib would silently turn the POST into a GET elsewhere."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        """Return None so urllib raises the 3xx as an HTTPError."""
        return None


class RpcClient:
    """Blocking JSON-RPC 2.0 client for one endpoint (thread-safe; each call is one HTTP request)."""

    def __init__(
        self,
        url: str,
        timeout: float = DEFAULT_TIMEOUT,
        user_agent: str = USER_AGENT,
        *,
        max_response_bytes: int = MAX_RESPONSE_BYTES,
    ) -> None:
        """Prepare a client for `url`; `user:password@` in the URL becomes HTTP basic auth."""
        parts = urllib.parse.urlsplit(url)
        if parts.scheme not in ("http", "https") or not parts.hostname:
            raise ValueError(f"RPC URL must be an absolute http(s) URL: {url!r}")
        netloc = parts.hostname if ":" not in parts.hostname else f"[{parts.hostname}]"
        if parts.port is not None:
            netloc += f":{parts.port}"
        self.url = urllib.parse.urlunsplit((parts.scheme, netloc, parts.path or "/", parts.query, ""))
        self.host = parts.hostname.lower()
        self.timeout = timeout
        self.max_response_bytes = max_response_bytes
        self._headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": user_agent,
        }
        if parts.username is not None:
            userinfo = f"{urllib.parse.unquote(parts.username)}:{urllib.parse.unquote(parts.password or '')}"
            self._headers["Authorization"] = "Basic " + base64.b64encode(userinfo.encode()).decode()
        self._opener = urllib.request.build_opener(_NoRedirect)

    def __repr__(self) -> str:
        """Show the endpoint without credentials."""
        return f"RpcClient({self.url!r})"

    def call(self, method: str, *params: Any) -> Any:
        """Call one method and return its result; raise RpcError on an error reply."""
        reply = self._post({"jsonrpc": "2.0", "id": 0, "method": method, "params": list(params)})
        if not isinstance(reply, dict):
            raise RpcTransportError(f"{method}: expected a JSON object, got {type(reply).__name__}")
        return _unwrap(reply, method)

    def batch(self, calls: Sequence[tuple[str, Sequence[Any]]]) -> list[Any]:
        """Send `calls` as one JSON-RPC 2.0 batch; return results in call order.

        A call that failed is returned as its `RpcError` instead of raising, so
        one missing block does not discard the rest of the batch.
        """
        if not calls:
            return []
        if len(calls) > MAX_BATCH_CALLS:
            raise ValueError(f"batch of {len(calls)} calls exceeds {MAX_BATCH_CALLS}")
        payload = [
            {"jsonrpc": "2.0", "id": index, "method": method, "params": list(params)}
            for index, (method, params) in enumerate(calls)
        ]
        reply = self._post(payload)
        if isinstance(reply, dict):
            # Some servers answer a rejected batch with one error object.
            _unwrap(reply, "batch")
            raise RpcTransportError("batch: expected a JSON array")
        if not isinstance(reply, list):
            raise RpcTransportError(f"batch: expected a JSON array, got {type(reply).__name__}")
        results: list[Any] = [RpcTransportError(f"{method}: no reply in batch") for method, _ in calls]
        for item in reply[: len(calls)]:
            index = item.get("id") if isinstance(item, dict) else None
            if isinstance(index, int) and not isinstance(index, bool) and 0 <= index < len(calls):
                try:
                    results[index] = _unwrap(item, calls[index][0])
                except RpcError as err:
                    results[index] = err
        return results

    def _post(self, payload: Any) -> Any:
        """POST a JSON payload and return the decoded reply, enforcing the size cap."""
        body = json.dumps(payload, separators=(",", ":")).encode()
        request = urllib.request.Request(self.url, data=body, headers=self._headers, method="POST")
        try:
            with self._opener.open(request, timeout=self.timeout) as response:
                raw = self._read(response)
        except urllib.error.HTTPError as err:
            # Some servers send JSON-RPC errors with a 4xx/5xx status.
            with err:
                try:
                    raw = self._read(err)
                    reply = json.loads(raw)
                except (OSError, ValueError, RecursionError, RpcTransportError):
                    reply = None
            if isinstance(reply, dict) and reply.get("error") is not None:
                _unwrap(reply, "request")
            raise RpcTransportError(f"HTTP {err.code} from {self.url}") from None
        except (OSError, http.client.HTTPException) as err:
            raise RpcTransportError(f"{self.url}: {err}") from None
        try:
            return json.loads(raw)
        except (ValueError, RecursionError) as err:
            raise RpcTransportError(f"{self.url}: invalid JSON reply: {err}") from None

    def _read(self, response: Any) -> bytes:
        """Read a response body, raising ResponseTooLarge past `max_response_bytes`."""
        limit = self.max_response_bytes
        length = response.headers.get("Content-Length")
        if length is not None and length.isdigit() and int(length) > limit:
            raise ResponseTooLarge(f"{self.url}: response of {length} bytes exceeds {limit}")
        raw = response.read(limit + 1)
        if len(raw) > limit:
            raise ResponseTooLarge(f"{self.url}: response exceeds {limit} bytes")
        return raw


def _unwrap(reply: dict[str, Any], method: str) -> Any:
    """Return a reply's result, raising RpcError for its error object (a null error is not one)."""
    error = reply.get("error")
    if error is not None:
        if isinstance(error, dict):
            code = error.get("code")
            code = code if isinstance(code, int) and not isinstance(code, bool) else -32603
            raise RpcError(code, f"{method}: {error.get('message', '')}")
        raise RpcError(-32603, f"{method}: {error}")
    if "result" not in reply:
        raise RpcTransportError(f"{method}: reply has neither result nor error")
    return reply["result"]


def _clean(text: Any, limit: int) -> str:
    """Make remote text safe to log and store: printable characters only, length-capped."""
    text = text if isinstance(text, str) else str(text)
    return "".join(ch if ch.isprintable() else "?" for ch in text[:limit])


def _block_hash(value: Any, what: str) -> str:
    """Validate a display-hex block hash from a remote reply; return it lowercased."""
    if isinstance(value, str) and len(value) == 64:
        lowered = value.lower()
        if _HEX.issuperset(lowered):
            return lowered
    raise ValueError(f"{what}: not a block hash: {_clean(value, 80)!r}")


def _height(value: Any, what: str) -> int:
    """Validate a block height from a remote reply."""
    if isinstance(value, int) and not isinstance(value, bool) and 0 <= value < 1 << 31:
        return value
    raise ValueError(f"{what}: not a block height: {_clean(value, 40)!r}")


def _parse_raw(raw: Any, params: NetworkParams, expected: str | None) -> Block:
    """Decode and parse a `getblock <id> 0` hex result, checking its hash when `expected` is given.

    Raises ValueError for a block that fails `check_pow`: the chain takes work from the
    header's bits, so one unchecked reply could claim unbeatable work.
    """
    if not isinstance(raw, str):
        raise ValueError("getblock: expected a hex string")
    try:
        data = bytes.fromhex(raw)
    except ValueError:
        raise ValueError("getblock: result is not hex") from None
    block = parse_block(data, params)
    if expected is not None and block.header.hash != expected:
        raise ValueError(f"getblock: asked for {expected}, got {block.header.hash}")
    if not check_pow(block.header, params):
        raise ValueError(f"getblock: block {block.header.hash} fails proof of work")
    return block


def _check_rules(chain: Any, params: NetworkParams, block: Block) -> None:
    """Raise ValueError if `block` breaks NU7 rules: a pre-NU7 coinbase (`is_pre_nu7_body`) or other nBits.

    nBits must be what `expected_bits` predicts, checked only at an NU7 height when the chain holds
    the parent and every ancestor the rules read; RPC endpoints are trusted otherwise.
    """
    header, coinbase = block.header, block.coinbase
    if coinbase is not None and is_pre_nu7_body(params, coinbase.height, coinbase.payouts, coinbase.branch_id):
        raise ValueError(f"block {header.hash} has a pre-NU7 coinbase under NU7 rules")
    parent = chain.get(header.prev_hash)
    if parent is None or params.nu7_height is None or parent.height + 1 < params.nu7_height:
        return
    height = parent.height + 1
    needed = params.difficulty_rules(height).context_len
    context: list[tuple[int, int]] = []
    node = parent
    while node is not None and len(context) < needed:
        context.append((node.bits, node.time))
        child, node = node, chain.get(node.prev_hash)
        if node is not None and node.height != child.height - 1:  # ends the walk on inconsistent heights
            node = None
    if len(context) == needed and header.bits != expected_bits(params, height, header.time, context):
        raise ValueError(f"block {header.hash} at {height} has the wrong difficulty under NU7 rules")


def _skip_detached(chain: Any, want: str, height: int | None) -> tuple[str, int | None]:
    """Follow `want` down through detached blocks the chain holds; return the first other hash and its child's height.

    Bounded by the chain's size, since every step visits a different node.
    """
    for _ in range(len(chain)):
        node = chain.get(want)
        if node is None or node.cumwork is not None:
            break
        want, height = node.prev_hash, node.height
    return want, height


def _bip34_height(block: Block) -> int | None:
    """Return the coinbase height of a parsed block, if any."""
    return block.coinbase.height if block.coinbase is not None else None


async def _monitor_call(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Call a Monitor method that may be plain or async."""
    result = fn(*args, **kwargs)
    if inspect.isawaitable(result):
        result = await result
    return result


def _backoff_delay(failures: int, low: float = BACKOFF_MIN, high: float = BACKOFF_MAX) -> float:
    """Exponential backoff after `failures` consecutive errors: low, 2*low, ... capped at high."""
    if failures <= 0:
        return 0.0
    return min(high, low * 2 ** min(failures - 1, 32))


def parse_peer(entry: Any, default_port: int) -> tuple[str, int, str] | None:
    """Turn one `getpeerinfo` entry into (ip, port, subver), or None if unusable.

    Inbound peers connected from an ephemeral port, so they get `default_port`.
    """
    if not isinstance(entry, dict) or not isinstance(entry.get("addr"), str):
        return None
    try:
        host, port = split_host_port(entry["addr"][:64], default_port)
        ip = ipaddress.ip_address(host)
    except ValueError:
        return None
    if ip.is_unspecified or ip.is_loopback or ip.is_multicast or ip.is_link_local:
        return None
    if entry.get("inbound") is True:
        port = default_port
    subver = entry.get("subver")
    subver = _clean(subver, MAX_SUBVER) if isinstance(subver, str) else ""
    return ip.compressed, port, subver


@dataclass(slots=True)
class _Duty:
    """One periodic collector step."""

    name: str
    interval: float
    step: Callable[[], Awaitable[Any]]
    due: float = 0.0
    enabled: bool = True


class RpcCollector:
    """Polls one RPC endpoint and feeds tips, chain tips and peer candidates to the Monitor.

    Every `interval` it polls `getbestblockhash` and, on change, ingests the raw
    tip plus any missing ancestors (at most `walk_limit`) before calling
    `monitor.observe_tip`. Every `chaintips_interval` it records `getchaintips`
    and asks the P2P side for unknown `valid-fork` tips; every
    `peerinfo_interval` it passes `getpeerinfo` addresses on as candidates.
    Any failure backs the whole endpoint off (BACKOFF_MIN doubling to
    BACKOFF_MAX) and marks its `sources` row "error"; success marks it "ok".
    """

    def __init__(
        self,
        name: str,
        client: RpcClient,
        monitor: Any,
        interval: float = 1.0,
        chaintips_interval: float = 3.0,
        peerinfo_interval: float = 120.0,
        *,
        walk_limit: int = MAX_WALK_BACK,
        backoff_min: float = BACKOFF_MIN,
        backoff_max: float = BACKOFF_MAX,
    ) -> None:
        """Bind the collector to `monitor` (see the service module's collector hooks)."""
        self.name = name
        self.source = f"rpc:{name}"
        self.client = client
        self.monitor = monitor
        self.walk_limit = walk_limit
        self.backoff_min = backoff_min
        self.backoff_max = backoff_max
        self._duties = (
            _Duty("tip", interval, self.poll_tip),
            _Duty("chaintips", chaintips_interval, self.poll_chaintips),
            _Duty("peers", peerinfo_interval, self.poll_peers),
        )
        self._tip: str | None = None
        self._tip_height: int | None = None
        self._failures = 0
        self._status: str | None = None
        self._status_written = -math.inf
        self._last_ok_at: float | None = None
        self._last_error: str | None = None
        self._last_error_at: float | None = None
        self._peerinfo_supported = True
        self._prefer_host: str | None = None
        self._chaintips: dict[str, tuple[Any, ...]] = {}
        self._requested: OrderedDict[str, tuple[int, float]] = OrderedDict()

    @classmethod
    def from_endpoint(cls, endpoint: RpcEndpoint, monitor: Any, **kwargs: Any) -> RpcCollector:
        """Build a collector (and its client) from a configured `[[rpc]]` endpoint."""
        client = RpcClient(endpoint.url, timeout=endpoint.timeout)
        return cls(
            endpoint.name,
            client,
            monitor,
            interval=endpoint.interval,
            chaintips_interval=endpoint.chaintips_interval,
            peerinfo_interval=endpoint.peerinfo_interval,
            **kwargs,
        )

    def health(self) -> dict[str, Any]:
        """Return a JSON-able status summary for the live snapshot."""
        return {
            "source": self.source,
            "url": self.client.url,
            "status": self._status or "starting",
            "tip_hash": self._tip,
            "tip_height": self._tip_height,
            "last_ok_at": self._last_ok_at,
            "last_error": self._last_error,
            "last_error_at": self._last_error_at,
            "consecutive_errors": self._failures,
            "disabled": [duty.name for duty in self._duties if not duty.enabled],
        }

    async def run(self) -> None:
        """Poll until cancelled; errors back the endpoint off instead of ending the task."""
        loop = asyncio.get_running_loop()
        self.monitor.store.upsert_source(self.source, kind="rpc", discovered_via="config")
        while True:
            for duty in self._duties:
                if not duty.enabled or duty.due > loop.time():
                    continue
                started = loop.time()
                try:
                    await duty.step()
                except asyncio.CancelledError:
                    raise
                except Exception as err:
                    if isinstance(err, RpcError) and err.code == METHOD_NOT_FOUND:
                        duty.enabled = False
                        log.info("%s: %s is not supported; disabling it", self.source, duty.name)
                        continue
                    delay = self._failed(err, duty.name)
                    resume = loop.time() + delay
                    for other in self._duties:
                        other.due = max(other.due, resume)
                    break
                self._succeeded()
                duty.due = max(started + duty.interval, loop.time())
            enabled = [duty.due for duty in self._duties if duty.enabled]
            if not enabled:
                log.warning("%s: no supported RPC duties left; collector stopping", self.source)
                return
            await asyncio.sleep(max(0.0, min(enabled) - loop.time()))

    async def poll_tip(self) -> bool:
        """Poll `getbestblockhash`; on a new tip ingest it and its missing ancestors. True if it changed."""
        best = _block_hash(await self._call("getbestblockhash"), "getbestblockhash")
        if best == self._tip:
            return False
        at = time.time()
        try:
            block = _parse_raw(await self._call("getblock", best, 0), self.monitor.params, best)
        except RpcError as err:
            if err.code in NOT_FOUND_CODES:
                # The node reorged between the two calls; the next poll sees the new tip.
                log.debug("%s: tip %s vanished before getblock: %s", self.source, best, err)
                return False
            raise
        _check_rules(self.monitor.chain, self.monitor.params, block)
        height = _bip34_height(block)
        await _monitor_call(
            self.monitor.ingest_block, block, block.header, height, source=self.source, kind="rpc_tip", at=at
        )
        await self._walk_back(block, height, at)
        await _monitor_call(self.monitor.observe_tip, self.source, best, at, height_hint=height)
        self._tip, self._tip_height = best, height
        return True

    async def _walk_back(self, block: Block, height: int | None, at: float) -> int:
        """Ingest ancestors of `block` until one is attached to the chain; return how many were added.

        Ancestors are fetched by height in growing batches and verified against
        the expected `prev_hash`; a mismatch (the node switched branches) falls
        back to one fetch by hash, and a hash the node no longer serves ends the walk.
        Detached blocks the chain already holds are walked through without a fetch,
        so a tip can bridge a gap below them (e.g. a hole left by a failed backfill).
        """
        chain = self.monitor.chain
        want = block.header.prev_hash
        added = 0
        size = 1
        while added < self.walk_limit and want != _ZERO_HASH:
            want, height = _skip_detached(chain, want, height)
            if want in chain or want == _ZERO_HASH:
                break
            count = min(size, self.walk_limit - added, height if height is not None else 0)
            linked = 0
            if count > 0:
                replies = await self._batch([("getblock", [str(height - offset), 0]) for offset in range(1, count + 1)])
                for reply in replies:
                    if isinstance(reply, RpcError):
                        break
                    candidate = _parse_raw(reply, self.monitor.params, None)
                    if candidate.header.hash != want:
                        break
                    _check_rules(chain, self.monitor.params, candidate)
                    height = height - 1
                    await self._ingest_ancestor(candidate, height, at)
                    want, linked = candidate.header.prev_hash, linked + 1
                    if want in chain or want == _ZERO_HASH:
                        break
            if linked == 0:
                try:
                    candidate = _parse_raw(await self._call("getblock", want, 0), self.monitor.params, want)
                except RpcError as err:
                    if err.code in NOT_FOUND_CODES:
                        log.debug("%s: walk-back stopped at %s: %s", self.source, want, err)
                        return added
                    raise
                _check_rules(chain, self.monitor.params, candidate)
                height = _bip34_height(candidate) if height is None else height - 1
                await self._ingest_ancestor(candidate, height, at)
                want, linked = candidate.header.prev_hash, 1
            added += linked
            size = min(size * 2, MAX_WALK_BATCH)
        return added

    async def _ingest_ancestor(self, block: Block, height: int | None, at: float) -> None:
        """Hand one walked-back ancestor to the Monitor."""
        await _monitor_call(
            self.monitor.ingest_block, block, block.header, height, source=self.source, kind="rpc_walk", at=at
        )

    async def poll_chaintips(self) -> int:
        """Record `getchaintips` and request unknown valid-fork tips over P2P; return how many were requested."""
        tips = await self._call("getchaintips")
        if not isinstance(tips, list):
            raise ValueError("getchaintips: expected an array")
        entries = []
        for tip in tips[:MAX_CHAINTIPS]:
            if not isinstance(tip, dict):
                continue
            try:
                block_hash = _block_hash(tip.get("hash"), "getchaintips")
                height = _height(tip.get("height"), "getchaintips")
            except ValueError:
                continue
            branchlen = tip.get("branchlen")
            branchlen = branchlen if isinstance(branchlen, int) and not isinstance(branchlen, bool) else None
            status = _clean(tip.get("status", ""), 32)
            entries.append((block_hash, height, branchlen, status))
        if not entries:
            return 0
        top = max(height for _, height, _, _ in entries)
        at = time.time()
        store = self.monitor.store
        seen: dict[str, tuple[Any, ...]] = {}
        requested = 0
        for block_hash, height, branchlen, status in entries:
            if height < top - MAX_CHAINTIP_DEPTH:
                continue
            previous = self._chaintips.get(block_hash)
            changed = previous is None or previous[:3] != (height, branchlen, status)
            if changed or at - previous[3] >= CHAINTIP_WRITE_INTERVAL:
                store.upsert_chaintip(self.source, block_hash, height, branchlen, status, at)
                if previous is None:
                    # The node's own best tip is as timely as the tip poll's sighting of it, which this may precede.
                    kind = "rpc_tip" if status == "active" else "chaintip"
                    store.record_sighting(block_hash, self.source, kind, at)
                previous = (height, branchlen, status, at)
            seen[block_hash] = previous
            if status == "valid-fork" and block_hash not in self.monitor.chain and self._should_request(block_hash, at):
                await _monitor_call(
                    self.monitor.request_block, block_hash, height, prefer_host=await self._host_for_p2p()
                )
                requested += 1
        self._chaintips = seen
        return requested

    def _should_request(self, block_hash: str, at: float) -> bool:
        """Rate-limit P2P requests for one unknown fork tip (REQUEST_RETRY apart, REQUEST_ATTEMPTS max)."""
        attempts, last = self._requested.get(block_hash, (0, -math.inf))
        if attempts >= REQUEST_ATTEMPTS or at - last < REQUEST_RETRY:
            return False
        self._requested[block_hash] = (attempts + 1, at)
        self._requested.move_to_end(block_hash)
        while len(self._requested) > MAX_REQUEST_MEMO:
            self._requested.popitem(last=False)
        return True

    async def _host_for_p2p(self) -> str:
        """Return the endpoint's IP (resolved once) so the P2P side can match it to a connected peer."""
        if self._prefer_host is None:
            host = self.client.host
            try:
                ipaddress.ip_address(host)
            except ValueError:
                try:
                    infos = await asyncio.to_thread(socket.getaddrinfo, host, None, type=socket.SOCK_STREAM)
                    host = infos[0][4][0] if infos else host
                except OSError:
                    return host  # try resolving again next time
            self._prefer_host = host
        return self._prefer_host

    async def poll_peers(self) -> int:
        """Record the endpoint's version and pass `getpeerinfo` addresses on; return the candidate count."""
        calls: list[tuple[str, Sequence[Any]]] = [("getnetworkinfo", [])]
        if self._peerinfo_supported:
            calls.append(("getpeerinfo", []))
        replies = await self._batch(calls)
        info = replies[0]
        if isinstance(info, dict) and isinstance(info.get("subversion"), str):
            subversion = _clean(info["subversion"], MAX_SUBVER)
            impl, version = classify_user_agent(subversion)
            protocol = info.get("protocolversion")
            self.monitor.store.upsert_source(
                self.source,
                impl=impl,
                impl_version=version,
                user_agent=subversion,
                protocol_version=protocol if isinstance(protocol, int) and not isinstance(protocol, bool) else None,
            )
        if not self._peerinfo_supported:
            if isinstance(info, RpcError):
                raise info
            return 0
        peers = replies[1]
        if isinstance(peers, RpcError):
            if peers.code == METHOD_NOT_FOUND:
                self._peerinfo_supported = False
                return 0
            raise peers
        if not isinstance(peers, list):
            raise ValueError("getpeerinfo: expected an array")
        default_port = self.monitor.params.default_port
        via = f"getpeerinfo:{self.name}"
        candidates: dict[tuple[str, int], tuple[str, int, str, str]] = {}
        for entry in peers[:MAX_PEERS]:
            parsed = parse_peer(entry, default_port)
            if parsed is None or classify_user_agent(parsed[2])[0] == "monitor":
                continue
            ip, port, subver = parsed
            candidates.setdefault((ip, port), (ip, port, subver, via))
        if candidates:
            await _monitor_call(self.monitor.add_peer_candidates, list(candidates.values()))
        return len(candidates)

    async def _call(self, method: str, *params: Any) -> Any:
        """Run one blocking RPC call off the event loop."""
        return await asyncio.to_thread(self.client.call, method, *params)

    async def _batch(self, calls: Sequence[tuple[str, Sequence[Any]]]) -> list[Any]:
        """Run one blocking RPC batch off the event loop."""
        return await asyncio.to_thread(self.client.batch, calls)

    def _succeeded(self) -> None:
        """Clear the error streak and mark the source "ok" (throttled while nothing changes)."""
        now = time.time()
        self._failures = 0
        self._last_ok_at = now
        if self._status != "ok" or now - self._status_written >= STATUS_WRITE_INTERVAL:
            self.monitor.store.upsert_source(self.source, status="ok", last_ok_at=now)
            self._status, self._status_written = "ok", now

    def _failed(self, err: Exception, duty: str) -> float:
        """Record a failure, mark the source "error" and return the backoff delay."""
        now = time.time()
        self._failures += 1
        delay = _backoff_delay(self._failures, self.backoff_min, self.backoff_max)
        message = _clean(f"{duty}: {err}", MAX_ERROR_TEXT)
        if isinstance(err, (RpcError, ParseError, ValueError)):
            log.warning("%s: %s (retry in %.0fs)", self.source, message, delay)
        else:
            log.exception("%s: unexpected error in %s (retry in %.0fs)", self.source, duty, delay)
        self._last_error, self._last_error_at = message, now
        self.monitor.store.upsert_source(self.source, status="error", last_error=message, last_error_at=now)
        self._status, self._status_written = "error", now
        return delay


@dataclass(frozen=True, slots=True)
class BackfillResult:
    """What one `backfill` run did."""

    tip_height: int
    requested: int  # heights in the range
    skipped: int  # already stored or in the chain with a body
    fetched: int  # blocks ingested
    failed: int  # blocks the endpoint did not serve or that failed to parse
    elapsed: float


async def backfill(
    client: RpcClient,
    monitor: Any,
    blocks: int,
    *,
    name: str | None = None,
    batch_size: int = BACKFILL_BATCH,
    concurrency: int = BACKFILL_CONCURRENCY,
) -> BackfillResult:
    """Fetch the last `blocks` canonical blocks the monitor does not hold yet and ingest them oldest first.

    Sightings use source `rpc:<name>` (default: the client's host) and kind
    "backfill". A request that still fails after BACKFILL_ATTEMPTS tries raises
    its RpcError; blocks ingested before that are kept.
    """
    if blocks < 0 or batch_size < 1 or concurrency < 1:
        raise ValueError("blocks must be >= 0, batch_size and concurrency >= 1")
    started = time.monotonic()
    source = f"rpc:{name or client.host}"
    tip = _height(await asyncio.to_thread(_retrying, client.call, "getblockcount"), "getblockcount")
    low = max(0, tip - blocks + 1)
    heights = list(range(low, tip + 1)) if blocks else []

    hash_chunks = [heights[i : i + HASH_BATCH] for i in range(0, len(heights), HASH_BATCH)]
    hashes: list[tuple[int, str]] = []
    failed = 0

    async def fetch_hashes(chunk: list[int]) -> list[Any]:
        """Resolve one chunk of heights to canonical hashes."""
        return await asyncio.to_thread(_retrying, client.batch, [("getblockhash", [h]) for h in chunk])

    async with contextlib.aclosing(_ordered(hash_chunks, fetch_hashes, concurrency)) as batches:
        async for chunk, replies in batches:
            for height, reply in zip(chunk, replies, strict=True):
                try:
                    hashes.append((height, _block_hash(reply, "getblockhash")))
                except ValueError:
                    failed += 1

    have = _stored_with_body(monitor, low, tip)
    chain = monitor.chain
    wanted = []
    for height, block_hash in hashes:
        node = chain.get(block_hash)
        if block_hash in have or (node is not None and node.body and node.body_trusted):
            continue
        wanted.append((height, block_hash))
    skipped = len(hashes) - len(wanted)

    params = monitor.params
    block_chunks = [wanted[i : i + batch_size] for i in range(0, len(wanted), batch_size)]
    fetched = 0
    last_log = time.monotonic()

    async def fetch_blocks(chunk: list[tuple[int, str]]) -> list[Block | Exception]:
        """Fetch and parse one chunk of blocks in a worker thread."""
        return await asyncio.to_thread(_fetch_blocks, client, chunk, params)

    rejected: set[str] = set()
    log.info("backfill: %d heights up to %d, %d already held, fetching %d", len(heights), tip, skipped, len(wanted))
    async with contextlib.aclosing(_ordered(block_chunks, fetch_blocks, concurrency)) as batches:
        async for chunk, results in batches:
            at = time.time()
            for (height, block_hash), result in zip(chunk, results, strict=True):
                if not isinstance(result, Exception):
                    try:
                        if result.header.prev_hash in rejected:
                            raise ValueError(f"block {block_hash} extends a rejected block")
                        _check_rules(chain, params, result)
                    except ValueError as err:
                        rejected.add(block_hash)
                        result = err
                if isinstance(result, Exception):
                    failed += 1
                    log.debug("backfill: %s at %d: %s", block_hash, height, result)
                    continue
                await _monitor_call(
                    monitor.ingest_block, result, result.header, height, source=source, kind="backfill", at=at
                )
                fetched += 1
            await asyncio.sleep(0)  # let collectors and the web snapshot run between chunks
            if time.monotonic() - last_log >= PROGRESS_INTERVAL:
                last_log = time.monotonic()
                rate = fetched / max(last_log - started, 1e-9)
                log.info("backfill: %d/%d blocks (%d failed, %.0f blocks/s)", fetched, len(wanted), failed, rate)
    result = BackfillResult(
        tip_height=tip,
        requested=len(heights),
        skipped=skipped,
        fetched=fetched,
        failed=failed,
        elapsed=time.monotonic() - started,
    )
    log.info(
        "backfill: done, %d fetched, %d skipped, %d failed in %.1fs",
        result.fetched,
        result.skipped,
        result.failed,
        result.elapsed,
    )
    return result


async def _ordered(
    chunks: list[Any], fetch: Callable[[Any], Awaitable[Any]], concurrency: int
) -> AsyncIterator[tuple[Any, Any]]:
    """Yield (chunk, fetch(chunk)) in chunk order while keeping up to `concurrency` fetches in flight."""
    pending: list[tuple[Any, asyncio.Task[Any]]] = []
    index = 0
    try:
        while index < len(chunks) or pending:
            while index < len(chunks) and len(pending) < concurrency:
                pending.append((chunks[index], asyncio.ensure_future(fetch(chunks[index]))))
                index += 1
            chunk, task = pending.pop(0)
            yield chunk, await task
    finally:
        for _, task in pending:
            task.cancel()


def _retrying(fn: Callable[..., Any], *args: Any) -> Any:
    """Call `fn` up to BACKFILL_ATTEMPTS times on transport errors, backing off from BACKFILL_RETRY_DELAY."""
    for attempt in range(BACKFILL_ATTEMPTS):
        try:
            return fn(*args)
        except ResponseTooLarge:
            raise
        except RpcTransportError:
            if attempt == BACKFILL_ATTEMPTS - 1:
                raise
            time.sleep(BACKFILL_RETRY_DELAY * 2**attempt)
    raise AssertionError("unreachable")


def _fetch_blocks(client: RpcClient, chunk: list[tuple[int, str]], params: NetworkParams) -> list[Block | Exception]:
    """Fetch `getblock <hash> 0` for a chunk and parse each; halve the chunk when a reply is too large."""
    try:
        replies = _retrying(client.batch, [("getblock", [block_hash, 0]) for _, block_hash in chunk])
    except ResponseTooLarge as err:
        if len(chunk) == 1:
            return [err]
        middle = len(chunk) // 2
        return _fetch_blocks(client, chunk[:middle], params) + _fetch_blocks(client, chunk[middle:], params)
    results: list[Block | Exception] = []
    for (_, block_hash), reply in zip(chunk, replies, strict=True):
        if isinstance(reply, Exception):
            results.append(reply)
            continue
        try:
            results.append(_parse_raw(reply, params, block_hash))
        except (ParseError, ValueError) as err:
            results.append(err)
    return results


def _stored_with_body(monitor: Any, low: int, high: int) -> set[str]:
    """Return hashes the store already holds with a trusted body in [low, high] (committed rows only)."""
    reader = getattr(monitor.store, "reader", None)
    if reader is None:
        return set()
    rows = reader().execute(
        "SELECT hash FROM blocks WHERE height BETWEEN ? AND ? AND body = 1 AND body_trusted = 1", (low, high)
    )
    return {row[0] for row in rows}
