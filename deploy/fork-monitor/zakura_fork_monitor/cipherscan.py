"""Optional CipherScan importer: copies CipherScan's orphan records into `external_orphans`.

CipherScan (https://github.com/Kenbak/cipherscan) runs one indexer node and
records every block that node reorged away (`GET /api/uncles`, newest height
first, `?limit=&offset=` with limit <= 200 and offset <= 50,000) and one fork
event per reorg (`GET /api/uncles/forks`, limit <= 100). The monitor uses them
only as a cross-check of its own orphan list.

Politeness: at most one request per MIN_REQUEST_GAP (2 per second), our
User-Agent on every request, a timeout, a response size cap and no redirects.
Each poll fetches one small page of recent orphans (plus one page of fork
events); when resuming and a whole page is new, older pages are fetched until a
known orphan appears (at most MAX_CATCHUP_PAGES). `backfill_pages` pages of 200
are fetched once at startup. Failures back off from `interval` doubling to
ERROR_BACKOFF_MAX.

Every field is validated before it is stored: hashes must be 64 hex digits,
numbers must be in range, text is printable and capped. `raw` holds a small
whitelisted JSON summary (CipherScan ids, pool name, first-seen source, the
canonical block's attribution and, when known, the fork event's depth), not the
full reply.

The importer is a `CipherscanImporter` class with
`run()`, `poll_recent()`, `backfill()` and `health()`; health is kept in memory
for the snapshot instead of a `sources` row (CipherScan is not a vantage point
with a tip).
"""

from __future__ import annotations

import asyncio
import hashlib
import http.client
import json
import logging
import math
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any

from . import __version__

log = logging.getLogger(__name__)

SOURCE = "cipherscan"
USER_AGENT = f"zakura-fork-monitor/{__version__}"
# CipherScan is a volunteer-run service: never more than 2 requests per second.
MIN_REQUEST_GAP = 0.5
DEFAULT_TIMEOUT = 20.0
# A full 200-orphan page is about 250 KB.
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
RECENT_LIMIT = 50
PAGE_LIMIT = 200  # server maximum for /api/uncles
FORKS_LIMIT = 100  # server maximum for /api/uncles/forks
MAX_OFFSET = 50_000  # the server answers 400 past this
MAX_CATCHUP_PAGES = 5
# Deep-reorg bursts can add a few hundred orphans between polls; this bounds memory, not correctness.
MAX_SEEN = 50_000
MAX_FORKS = 10_000
ERROR_BACKOFF_MAX = 900.0
MAX_TEXT = 64
MAX_HEIGHT = (1 << 31) - 1
MAX_BLOCK_SIZE = 2_000_000
MAX_TIME = (1 << 32) - 1

_HASH_RE = re.compile(r"[0-9a-f]{64}")
_ADDRESS_RE = re.compile(r"[0-9A-Za-z]{1,128}")
_MISSING = object()


class CipherscanError(Exception):
    """CipherScan could not be reached or answered with something unusable."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Refuse redirects so requests only ever go to the configured host."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        """Return None so urllib raises the 3xx as an HTTPError."""
        return None


def _text(value: Any, limit: int = MAX_TEXT) -> str | None:
    """Return a printable, length-capped string, or None for non-strings and empty text."""
    if not isinstance(value, str):
        return None
    cleaned = "".join(ch for ch in value[:limit] if ch.isprintable())
    return cleaned or None


def _int(value: Any, low: int, high: int) -> int | None:
    """Return an integer in [low, high] (not a bool), else None."""
    if isinstance(value, int) and not isinstance(value, bool) and low <= value <= high:
        return value
    return None


def _hash(value: Any) -> str | None:
    """Return a lowercase 64-hex block hash, else None."""
    if isinstance(value, str) and len(value) == 64:
        lowered = value.lower()
        if _HASH_RE.fullmatch(lowered):
            return lowered
    return None


def _address(value: Any) -> str | None:
    """Return a plausible transparent/unified address (alphanumeric, <= 128 chars), else None."""
    return value if isinstance(value, str) and _ADDRESS_RE.fullmatch(value) else None


def _difficulty(value: Any) -> float | None:
    """Parse CipherScan's difficulty (a decimal string) into a finite non-negative float."""
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        return None
    if isinstance(value, str) and len(value) > 40:
        return None
    try:
        number = float(value)
    except ValueError:
        return None
    return number if math.isfinite(number) and number >= 0 else None


def parse_timestamp(value: Any) -> float | None:
    """Parse an ISO 8601 timestamp such as "2026-09-29T10:00:03.988Z" into unix seconds (UTC if naive)."""
    if not isinstance(value, str) or not 10 <= len(value) <= 40:
        return None
    try:
        moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.timestamp()


def orphan_row(item: Any, forks: dict[int, dict[str, Any]] | None = None) -> dict[str, Any] | None:
    """Map one `/api/uncles` entry to `external_orphans` fields, or None if it lacks a hash or height."""
    if not isinstance(item, dict):
        return None
    block_hash = _hash(item.get("hash"))
    height = _int(item.get("height"), 0, MAX_HEIGHT)
    if block_hash is None or height is None:
        return None
    canonical = item.get("canonicalBlock") if isinstance(item.get("canonicalBlock"), dict) else {}
    fork_id = _int(item.get("forkEventId"), 0, MAX_HEIGHT)
    raw: dict[str, Any] = {
        "id": _int(item.get("id"), 0, MAX_HEIGHT),
        "forkEventId": fork_id,
        "source": _text(item.get("source")),
        "firstSeenSource": _text(item.get("firstSeenSource")),
        "firstSeenPollIntervalMs": _int(item.get("firstSeenPollIntervalMs"), 0, 86_400_000),
        "minerPool": _text(item.get("minerPool")),
        "transactionCount": _int(item.get("transactionCount"), 0, MAX_BLOCK_SIZE),
        "consensusValid": item.get("consensusValid") if isinstance(item.get("consensusValid"), bool) else None,
        "reportedBy": _text(item.get("reportedBy")),
        "canonical": {
            "firstSeenAt": parse_timestamp(canonical.get("firstSeenAt")),
            "minerAddress": _address(canonical.get("minerAddress")),
            "minerPool": _text(canonical.get("minerPool")),
            "timestamp": _int(canonical.get("timestamp"), 0, MAX_TIME),
            "size": _int(canonical.get("size"), 0, MAX_BLOCK_SIZE),
        },
    }
    if forks and fork_id in forks:
        raw["fork"] = forks[fork_id]
    return {
        "source": SOURCE,
        "hash": block_hash,
        "height": height,
        "prev_hash": _hash(item.get("previousBlockHash")),
        "canonical_hash": _hash(item.get("canonicalHash")) or _hash(canonical.get("hash")),
        "time": _int(item.get("timestamp"), 0, MAX_TIME),
        "difficulty": _difficulty(item.get("difficulty")),
        "miner_address": _address(item.get("minerAddress")),
        "size": _int(item.get("size"), 0, MAX_BLOCK_SIZE),
        "first_seen_at": parse_timestamp(item.get("firstSeenAt")),
        "detected_at": parse_timestamp(item.get("detectedAt")),
        "raw": raw,
    }


def fork_summary(item: Any) -> tuple[int, dict[str, Any]] | None:
    """Map one `/api/uncles/forks` entry to (fork id, {depth, forkHeight, canonicalTip}), or None."""
    if not isinstance(item, dict):
        return None
    fork_id = _int(item.get("id"), 0, MAX_HEIGHT)
    if fork_id is None:
        return None
    return fork_id, {
        "depth": _int(item.get("depth"), 0, MAX_HEIGHT),
        "forkHeight": _int(item.get("forkHeight"), 0, MAX_HEIGHT),
        "canonicalTip": _int(item.get("canonicalTip"), 0, MAX_HEIGHT),
    }


class CipherscanImporter:
    """Polls CipherScan's orphan list and stores it through `monitor.store.upsert_external_orphan`."""

    def __init__(
        self,
        base_url: str,
        monitor: Any,
        *,
        interval: float = 60.0,
        backfill_pages: int = 0,
        fetch_forks: bool = True,
        timeout: float = DEFAULT_TIMEOUT,
        min_request_gap: float = MIN_REQUEST_GAP,
        user_agent: str = USER_AGENT,
        max_response_bytes: int = MAX_RESPONSE_BYTES,
    ) -> None:
        """Configure the importer; `base_url` is the API origin, e.g. https://api.testnet.cipherscan.app."""
        parts = urllib.parse.urlsplit(base_url)
        if parts.scheme not in ("http", "https") or not parts.hostname:
            raise ValueError(f"CipherScan URL must be an absolute http(s) URL: {base_url!r}")
        self.base_url = base_url.rstrip("/")
        self.monitor = monitor
        self.interval = interval
        self.backfill_pages = max(0, backfill_pages)
        self.fetch_forks = fetch_forks
        self.timeout = timeout
        self.min_request_gap = max(min_request_gap, 0.0)
        self.max_response_bytes = max_response_bytes
        self._headers = {"Accept": "application/json", "User-Agent": user_agent}
        self._opener = urllib.request.build_opener(_NoRedirect)
        self._next_request = 0.0
        self._seen: OrderedDict[str, bytes | None] = OrderedDict()
        self._forks: OrderedDict[int, dict[str, Any]] = OrderedDict()
        self._failures = 0
        self._status = "starting"
        self._last_ok_at: float | None = None
        self._last_error: str | None = None
        self._last_error_at: float | None = None
        self._requests = 0
        self._written = 0
        self._invalid = 0

    @classmethod
    def from_config(cls, config: Any, monitor: Any, **kwargs: Any) -> CipherscanImporter:
        """Build an importer from a `CipherscanConfig`."""
        return cls(
            config.base_url, monitor, interval=config.interval, backfill_pages=config.backfill_pages, **kwargs
        )

    def health(self) -> dict[str, Any]:
        """Return a JSON-able status summary for the live snapshot."""
        return {
            "source": SOURCE,
            "base_url": self.base_url,
            "status": self._status,
            "last_ok_at": self._last_ok_at,
            "last_error": self._last_error,
            "last_error_at": self._last_error_at,
            "consecutive_errors": self._failures,
            "requests": self._requests,
            "written": self._written,
            "invalid": self._invalid,
        }

    async def run(self) -> None:
        """Seed from the store, run the startup backfill, then poll until cancelled."""
        self._seed()
        if self.backfill_pages:
            try:
                await self.backfill(self.backfill_pages)
            except asyncio.CancelledError:
                raise
            except Exception as err:
                self._failed(err)
        while True:
            try:
                await self.poll_recent()
            except asyncio.CancelledError:
                raise
            except Exception as err:
                delay = self._failed(err)
            else:
                self._succeeded()
                delay = self.interval
            await asyncio.sleep(delay)

    async def poll_recent(self) -> int:
        """Store new or changed recent orphans; return how many rows were written."""
        resuming = bool(self._seen)
        if self.fetch_forks:
            await self._refresh_forks(1)
        items, more = await self._page("/api/uncles", "orphanedBlocks", 0, RECENT_LIMIT)
        written, new = self._store(items)
        offset, pages = len(items), 0
        # A page with nothing we already hold may hide a gap since the last poll or run.
        while resuming and more and items and new == len(items) and pages < MAX_CATCHUP_PAGES:
            items, more = await self._page("/api/uncles", "orphanedBlocks", offset, PAGE_LIMIT)
            page_written, new = self._store(items)
            written += page_written
            offset, pages = offset + len(items), pages + 1
        return written

    async def backfill(self, pages: int) -> int:
        """Store up to `pages` pages of 200 orphans, newest first; return how many rows were written."""
        if self.fetch_forks:
            try:
                await self._refresh_forks(2 * pages)  # there are fewer fork events than orphans
            except CipherscanError as err:
                log.warning("cipherscan: fork events unavailable: %s", err)
        written = offset = 0
        for _ in range(pages):
            items, more = await self._page("/api/uncles", "orphanedBlocks", offset, PAGE_LIMIT)
            written += self._store(items)[0]
            offset += len(items)
            if not more or not items:
                break
        log.info("cipherscan: backfill stored %d orphans from %d rows", written, offset)
        return written

    async def _refresh_forks(self, pages: int) -> None:
        """Fetch `pages` pages of fork events into the id -> depth map used to annotate orphans."""
        offset = 0
        for _ in range(pages):
            items, more = await self._page("/api/uncles/forks", "forks", offset, FORKS_LIMIT)
            for item in items:
                summary = fork_summary(item)
                if summary is not None:
                    self._forks[summary[0]] = summary[1]
                    self._forks.move_to_end(summary[0])
            while len(self._forks) > MAX_FORKS:
                self._forks.popitem(last=False)
            offset += len(items)
            if not more or not items:
                break

    async def _page(self, path: str, key: str, offset: int, limit: int) -> tuple[list[Any], bool]:
        """Fetch one page; return (items, whether more pages exist)."""
        if offset > MAX_OFFSET:
            return [], False
        reply = await self._get(path, limit=limit, offset=offset)
        items = reply.get(key)
        if not isinstance(items, list):
            raise CipherscanError(f"{path}: reply has no {key} list")
        items = items[: 2 * limit]
        pagination = reply.get("pagination")
        more = pagination.get("hasMore") if isinstance(pagination, dict) else None
        if not isinstance(more, bool):
            more = len(items) >= limit
        return items, more

    async def _get(self, path: str, **query: Any) -> dict[str, Any]:
        """GET an API path at no more than one request per `min_request_gap`."""
        loop = asyncio.get_running_loop()
        wait = self._next_request - loop.time()
        if wait > 0:
            await asyncio.sleep(wait)
        self._next_request = loop.time() + self.min_request_gap
        url = f"{self.base_url}{path}?{urllib.parse.urlencode(query)}"
        self._requests += 1
        reply = await asyncio.to_thread(self._fetch, url)
        if not isinstance(reply, dict) or reply.get("success") is False:
            raise CipherscanError(f"{path}: unexpected reply")
        return reply

    def _fetch(self, url: str) -> Any:
        """Blocking GET returning decoded JSON; CipherscanError on any failure."""
        request = urllib.request.Request(url, headers=self._headers)
        limit = self.max_response_bytes
        try:
            with self._opener.open(request, timeout=self.timeout) as response:
                length = response.headers.get("Content-Length")
                if length is not None and length.isdigit() and int(length) > limit:
                    raise CipherscanError(f"response of {length} bytes exceeds {limit}")
                body = response.read(limit + 1)
        except urllib.error.HTTPError as err:
            err.close()
            raise CipherscanError(f"HTTP {err.code} for {url}") from None
        except (OSError, http.client.HTTPException) as err:
            raise CipherscanError(f"{url}: {err}") from None
        if len(body) > limit:
            raise CipherscanError(f"response exceeds {limit} bytes")
        try:
            return json.loads(body)
        except (ValueError, RecursionError) as err:
            raise CipherscanError(f"{url}: invalid JSON: {err}") from None

    def _store(self, items: list[Any]) -> tuple[int, int]:
        """Upsert new or changed orphans; return (rows written, orphans not seen before)."""
        written = new = 0
        store = self.monitor.store
        for item in items:
            row = orphan_row(item, self._forks)
            if row is None:
                self._invalid += 1
                continue
            digest = hashlib.blake2b(json.dumps(row, sort_keys=True).encode(), digest_size=16).digest()
            previous = self._seen.get(row["hash"], _MISSING)
            if previous is _MISSING:
                new += 1
            if previous != digest:
                store.upsert_external_orphan(**row)
                written += 1
            self._remember(row["hash"], digest)
        self._written += written
        return written, new

    def _remember(self, block_hash: str, digest: bytes | None) -> None:
        """Remember an orphan's latest digest, forgetting the oldest past MAX_SEEN."""
        self._seen[block_hash] = digest
        self._seen.move_to_end(block_hash)
        while len(self._seen) > MAX_SEEN:
            self._seen.popitem(last=False)

    def _seed(self) -> None:
        """Mark orphans already stored from a previous run as known (their digests are unknown)."""
        reader = getattr(self.monitor.store, "reader", None)
        if reader is None:
            return
        rows = reader().execute(
            "SELECT hash FROM external_orphans WHERE source = ? ORDER BY height DESC LIMIT ?", (SOURCE, MAX_SEEN)
        )
        for (block_hash,) in reversed(rows.fetchall()):
            self._remember(block_hash, None)

    def _succeeded(self) -> None:
        """Clear the error streak."""
        self._failures = 0
        self._status = "ok"
        self._last_ok_at = time.time()

    def _failed(self, err: Exception) -> float:
        """Record a failure and return the backoff delay."""
        self._failures += 1
        delay = min(ERROR_BACKOFF_MAX, self.interval * 2 ** min(self._failures, 16))
        self._status = "error"
        self._last_error = _text(str(err), 256)
        self._last_error_at = time.time()
        if isinstance(err, (CipherscanError, ValueError)):
            log.warning("cipherscan: %s (retry in %.0fs)", self._last_error, delay)
        else:
            log.exception("cipherscan: unexpected error (retry in %.0fs)", delay)
        return delay
