"""Tests for the CipherScan importer against a local stub API."""

from __future__ import annotations

import asyncio
import json
import tempfile
import threading
import time
import unittest
import urllib.parse
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from zakura_fork_monitor import cipherscan as cs
from zakura_fork_monitor.cipherscan import CipherscanError, CipherscanImporter, orphan_row, parse_timestamp
from zakura_fork_monitor.config import CipherscanConfig
from zakura_fork_monitor.store import Store

# A live /api/uncles entry (2026-09-29), trimmed of nothing.
LIVE_UNCLE = {
    "id": 6037,
    "height": 4414208,
    "hash": "00002bf14beb6bf00d6f998b6348624f7ae4c9f7aaa38cb362c84e4bf97f5932",
    "firstSeenAt": "2026-09-29T10:00:03.988Z",
    "firstSeenSource": "local-node-rpc",
    "firstSeenPollIntervalMs": 1000,
    "canonicalHash": "0001cf8efafc3d89facbc3cbba560a58c4bd60dd19cc476f0db5d597aea5de1e",
    "timestamp": 1790675997,
    "transactionCount": 1,
    "size": 7591,
    "difficulty": "406.335440432076",
    "minerAddress": None,
    "minerPool": None,
    "previousBlockHash": "000038156c0b972cc7ca9e109b0fe152472b5fb10aef0b4de563b40d9ed46c97",
    "source": "indexer",
    "reportedBy": None,
    "consensusValid": None,
    "detectedAt": "2026-09-29T10:00:13.302Z",
    "forkEventId": 3110,
    "canonicalBlock": {
        "hash": "0001cf8efafc3d89facbc3cbba560a58c4bd60dd19cc476f0db5d597aea5de1e",
        "firstSeenAt": "2026-09-29T10:00:00.977Z",
        "firstSeenSource": "local-node-rpc",
        "firstSeenPollIntervalMs": 1000,
        "height": 4414208,
        "timestamp": 1790675997,
        "transactionCount": 1,
        "size": 7591,
        "minerAddress": "tmJggjzf2qPBbmUFfr7eYdqFUV15y1MvvVu",
        "minerPool": None,
        "minerPoolUrl": None,
        "minerPoolRegion": None,
    },
}


def iso(seconds: float) -> str:
    """Format unix seconds like CipherScan does."""
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def uncle(n: int) -> dict[str, Any]:
    """Build a synthetic orphan record number `n` (higher n = newer, higher block)."""
    return {
        **LIVE_UNCLE,
        "id": 10_000 + n,
        "height": 4_400_000 + n,
        "hash": f"{n:064x}",
        "previousBlockHash": f"{n:063x}f",
        "canonicalHash": f"{n:063x}e",
        "timestamp": 1_790_000_000 + n,
        "firstSeenAt": iso(1_790_000_005 + n),
        "detectedAt": iso(1_790_000_010 + n),
        "forkEventId": 5_000 + n,
        "minerAddress": "tmJggjzf2qPBbmUFfr7eYdqFUV15y1MvvVu",
    }


def fork(n: int, depth: int = 1) -> dict[str, Any]:
    """Build a synthetic fork event for orphan `n`."""
    height = 4_400_000 + n
    return {"id": 5_000 + n, "forkHeight": height, "depth": depth, "canonicalTip": height, "comparisons": []}


class StubApi:
    """CipherScan API state: orphans and forks, newest first, plus failure switches."""

    def __init__(self) -> None:
        """Start with no records."""
        self.uncles: list[dict[str, Any]] = []
        self.forks: list[dict[str, Any]] = []
        self.fail = 0
        self.raw_reply: bytes | None = None
        self.redirect = False
        self.lock = threading.Lock()
        self.requests: list[tuple[str, dict[str, str], float, dict[str, str]]] = []

    def uncle_offsets(self) -> list[tuple[int, int]]:
        """Return (offset, limit) of each /api/uncles request."""
        return [(int(q["offset"]), int(q["limit"])) for path, q, _, _ in self.requests if path == "/api/uncles"]

    def page(self, rows: list[dict[str, Any]], key: str, query: dict[str, str], max_limit: int) -> dict[str, Any]:
        """Paginate like CipherScan's parseSafeListPagination."""
        limit = min(max(int(query.get("limit", "20")), 1), max_limit)
        offset = max(int(query.get("offset", "0")), 0)
        return {
            "success": True,
            key: rows[offset : offset + limit],
            "pagination": {"total": len(rows), "limit": limit, "offset": offset, "hasMore": offset + limit < len(rows)},
        }


class _Handler(BaseHTTPRequestHandler):
    """HTTP front end for a StubApi."""

    def do_GET(self) -> None:
        """Serve /api/uncles and /api/uncles/forks."""
        api: StubApi = self.server.api
        parts = urllib.parse.urlsplit(self.path)
        query = dict(urllib.parse.parse_qsl(parts.query))
        with api.lock:
            api.requests.append((parts.path, query, time.monotonic(), dict(self.headers)))
            failing = api.fail > 0
            api.fail -= failing
        if api.redirect:
            self.send_response(301)
            self.send_header("Location", "http://127.0.0.1:1/")
            self.end_headers()
            return
        if failing:
            self._send(500, b"oops")
        elif api.raw_reply is not None:
            self._send(200, api.raw_reply)
        elif parts.path == "/api/uncles":
            self._send(200, json.dumps(api.page(api.uncles, "orphanedBlocks", query, 200)).encode())
        elif parts.path == "/api/uncles/forks":
            self._send(200, json.dumps(api.page(api.forks, "forks", query, 100)).encode())
        else:
            self._send(404, b'{"error":"not found"}')

    def _send(self, status: int, body: bytes) -> None:
        """Write a JSON response."""
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        """Keep test output quiet."""


class _QuietServer(ThreadingHTTPServer):
    """ThreadingHTTPServer without per-error tracebacks."""

    daemon_threads = True

    def handle_error(self, request: Any, client_address: Any) -> None:
        """Ignore clients that hang up early."""


class ImporterTestCase(unittest.IsolatedAsyncioTestCase):
    """A stub API on a free port and a fresh store."""

    def setUp(self) -> None:
        """Start the stub API and open a store."""
        self.api = StubApi()
        self.httpd = _QuietServer(("127.0.0.1", 0), _Handler)
        self.httpd.api = self.api
        self.base_url = f"http://127.0.0.1:{self.httpd.server_address[1]}"
        threading.Thread(target=self.httpd.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True).start()
        self._tmp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self._tmp.name) / "monitor.sqlite3", commit_interval=0)
        self.monitor = SimpleNamespace(store=self.store)

    def tearDown(self) -> None:
        """Stop the API and remove the store."""
        self.httpd.shutdown()
        self.httpd.server_close()
        self.store.close()
        self._tmp.cleanup()

    def importer(self, **kwargs: Any) -> CipherscanImporter:
        """Build an importer against the stub with a short request gap."""
        kwargs.setdefault("min_request_gap", 0.0)
        return CipherscanImporter(self.base_url, self.monitor, **kwargs)

    def stored(self) -> dict[str, dict[str, Any]]:
        """Return stored CipherScan orphans by hash."""
        rows = self.store.reader().execute("SELECT * FROM external_orphans WHERE source = 'cipherscan'")
        return {row["hash"]: dict(row) for row in rows}


class RowTests(unittest.TestCase):
    """Mapping and validation of CipherScan records."""

    def test_live_record_maps_to_columns(self) -> None:
        """A live record fills every external_orphans column; the fork depth lands in raw."""
        row = orphan_row(LIVE_UNCLE, {3110: {"depth": 1, "forkHeight": 4414208, "canonicalTip": 4414208}})
        raw = row.pop("raw")
        self.assertEqual(
            row,
            {
                "source": "cipherscan",
                "hash": LIVE_UNCLE["hash"],
                "height": 4414208,
                "prev_hash": LIVE_UNCLE["previousBlockHash"],
                "canonical_hash": LIVE_UNCLE["canonicalHash"],
                "time": 1790675997,
                "difficulty": 406.335440432076,
                "miner_address": None,
                "size": 7591,
                "first_seen_at": 1790676003.988,
                "detected_at": 1790676013.302,
            },
        )
        self.assertEqual(raw["forkEventId"], 3110)
        self.assertEqual(raw["fork"]["depth"], 1)
        self.assertEqual(raw["canonical"]["minerAddress"], "tmJggjzf2qPBbmUFfr7eYdqFUV15y1MvvVu")
        self.assertEqual(raw["canonical"]["firstSeenAt"], 1790676000.977)
        self.assertEqual(raw["firstSeenSource"], "local-node-rpc")

    def test_invalid_records_and_fields(self) -> None:
        """Records without a valid hash or height are dropped; bad fields become None or are cleaned."""
        bad_records = (
            {**LIVE_UNCLE, "hash": "zz" * 32},
            {**LIVE_UNCLE, "height": "4414208"},
            {**LIVE_UNCLE, "height": -1},
            [],
            None,
        )
        for bad in bad_records:
            self.assertIsNone(orphan_row(bad), bad)
        row = orphan_row(
            {
                **LIVE_UNCLE,
                "hash": LIVE_UNCLE["hash"].upper(),
                "canonicalHash": "nope",
                "difficulty": "NaN",
                "minerAddress": "<script>alert(1)</script>",
                "minerPool": "\x1b[1mEvil" + "p" * 500,
                "size": 10**12,
                "timestamp": True,
                "firstSeenAt": "yesterday",
                "canonicalBlock": "junk",
            }
        )
        self.assertEqual(row["hash"], LIVE_UNCLE["hash"])
        self.assertIsNone(row["canonical_hash"])
        self.assertIsNone(row["difficulty"])
        self.assertIsNone(row["miner_address"])
        self.assertIsNone(row["size"])
        self.assertIsNone(row["time"])
        self.assertIsNone(row["first_seen_at"])
        self.assertEqual(len(row["raw"]["minerPool"]), cs.MAX_TEXT - 1)
        self.assertTrue(row["raw"]["minerPool"].isprintable())
        for value in ("inf", "-1", "abc", "1" * 50, [1]):
            self.assertIsNone(orphan_row({**LIVE_UNCLE, "difficulty": value})["difficulty"], value)
        fallback = orphan_row({**LIVE_UNCLE, "canonicalHash": None})
        self.assertEqual(fallback["canonical_hash"], LIVE_UNCLE["canonicalBlock"]["hash"])

    def test_parse_timestamp(self) -> None:
        """ISO timestamps with Z, offsets or no zone parse to unix seconds; junk does not."""
        self.assertEqual(parse_timestamp("2026-09-29T10:00:03.988Z"), 1790676003.988)
        self.assertEqual(parse_timestamp("2026-09-29T12:00:03.988+02:00"), 1790676003.988)
        self.assertEqual(parse_timestamp("2026-09-29T10:00:03.988"), 1790676003.988)
        for junk in (None, 5, "", "tomorrow", "2026-13-01T00:00:00Z", "9" * 100):
            self.assertIsNone(parse_timestamp(junk), junk)


class ImporterTests(ImporterTestCase):
    """Polling, paging, politeness and error handling."""

    async def test_poll_stores_recent_page_with_fork_depth(self) -> None:
        """One poll reads one fork page and one small orphan page, politely identified."""
        self.api.uncles = [uncle(n) for n in range(80, 0, -1)]
        self.api.forks = [fork(n, depth=2) for n in range(80, 40, -1)]
        importer = self.importer()
        self.assertEqual(await importer.poll_recent(), cs.RECENT_LIMIT)
        rows = self.stored()
        self.assertEqual(len(rows), cs.RECENT_LIMIT)
        newest = rows[f"{80:064x}"]
        self.assertEqual(newest["height"], 4_400_080)
        self.assertEqual(json.loads(newest["raw"])["fork"]["depth"], 2)
        self.assertNotIn("fork", json.loads(rows[f"{31:064x}"]["raw"]))
        paths = [(path, query["limit"], query["offset"]) for path, query, _, _ in self.api.requests]
        self.assertEqual(paths, [("/api/uncles/forks", "100", "0"), ("/api/uncles", "50", "0")])
        agents = {headers.get("User-Agent") for _, _, _, headers in self.api.requests}
        self.assertEqual(agents, {cs.USER_AGENT})

    async def test_unchanged_orphans_are_not_rewritten(self) -> None:
        """Only new or changed records are written on later polls."""
        self.api.uncles = [uncle(n) for n in range(5, 0, -1)]
        importer = self.importer(fetch_forks=False)
        self.assertEqual(await importer.poll_recent(), 5)
        self.assertEqual(await importer.poll_recent(), 0)
        self.api.uncles[2] = {**self.api.uncles[2], "canonicalHash": "ab" * 32}
        self.assertEqual(await importer.poll_recent(), 1)
        self.assertEqual(self.stored()[f"{3:064x}"]["canonical_hash"], "ab" * 32)
        self.assertEqual(importer.health()["written"], 6)

    async def test_resuming_catches_up_to_a_known_orphan(self) -> None:
        """After a restart, pages are followed until one contains an orphan already stored."""
        self.api.uncles = [uncle(n) for n in range(10, 0, -1)]
        await self.importer(fetch_forks=False).poll_recent()
        self.api.uncles = [uncle(n) for n in range(130, 0, -1)]
        self.api.requests.clear()
        resumed = self.importer(fetch_forks=False)
        resumed._seed()
        await resumed.poll_recent()
        self.assertEqual(self.api.uncle_offsets(), [(0, 50), (50, 200)])
        self.assertEqual(len(self.stored()), 130)

    async def test_fresh_start_reads_only_the_recent_page(self) -> None:
        """With nothing stored and no backfill, only the recent page is read."""
        self.api.uncles = [uncle(n) for n in range(300, 0, -1)]
        importer = self.importer(fetch_forks=False)
        importer._seed()
        await importer.poll_recent()
        self.assertEqual(self.api.uncle_offsets(), [(0, 50)])

    async def test_catch_up_is_bounded(self) -> None:
        """A long gap is followed for at most MAX_CATCHUP_PAGES pages per poll."""
        self.api.uncles = [uncle(n) for n in range(3, 0, -1)]
        importer = self.importer(fetch_forks=False)
        await importer.poll_recent()
        self.api.uncles = [uncle(n) for n in range(3_000, 0, -1)]
        await importer.poll_recent()
        self.assertEqual(len(self.api.uncle_offsets()), 2 + cs.MAX_CATCHUP_PAGES)

    async def test_backfill_pages(self) -> None:
        """Backfill reads pages of 200 (after fork pages) and stops at the last page."""
        self.api.uncles = [uncle(n) for n in range(450, 0, -1)]
        self.api.forks = [fork(n, depth=3) for n in range(450, 0, -1)]
        importer = self.importer()
        self.assertEqual(await importer.backfill(5), 450)
        self.assertEqual(self.api.uncle_offsets(), [(0, 200), (200, 200), (400, 200)])
        fork_pages = [query["offset"] for path, query, _, _ in self.api.requests if path == "/api/uncles/forks"]
        self.assertEqual(fork_pages, ["0", "100", "200", "300", "400"])
        self.assertTrue(all(json.loads(row["raw"])["fork"]["depth"] == 3 for row in self.stored().values()))

    async def test_requests_are_spaced(self) -> None:
        """Consecutive requests start at least min_request_gap apart."""
        self.api.uncles = [uncle(n) for n in range(450, 0, -1)]
        await self.importer(min_request_gap=0.1, fetch_forks=False).backfill(3)
        starts = [at for _, _, at, _ in self.api.requests]
        self.assertEqual(len(starts), 3)
        self.assertTrue(all(b - a >= 0.09 for a, b in zip(starts, starts[1:], strict=False)), starts)

    async def test_invalid_items_are_counted_not_stored(self) -> None:
        """Malformed records are skipped and counted."""
        self.api.uncles = [uncle(2), {"hash": "junk"}, "junk", uncle(1)]
        importer = self.importer(fetch_forks=False)
        await importer.poll_recent()
        self.assertEqual(len(self.stored()), 2)
        self.assertEqual(importer.health()["invalid"], 2)

    async def test_bad_replies_raise(self) -> None:
        """HTTP errors, redirects, junk, oversize and wrong shapes raise CipherscanError."""
        importer = self.importer(fetch_forks=False)
        self.api.fail = 1
        with self.assertRaisesRegex(CipherscanError, "HTTP 500"):
            await importer.poll_recent()
        for body in (b"not json", b'{"success": false}', b'{"success": true}', b"[]"):
            self.api.raw_reply = body
            with self.assertRaises(CipherscanError, msg=body):
                await importer.poll_recent()
        self.api.raw_reply = None
        self.api.uncles = [uncle(n) for n in range(50, 0, -1)]
        with self.assertRaisesRegex(CipherscanError, "exceeds"):
            await self.importer(max_response_bytes=1_000).poll_recent()
        self.api.redirect = True
        with self.assertRaisesRegex(CipherscanError, "HTTP 301"):
            await importer.poll_recent()
        self.assertEqual(self.stored(), {})

    async def test_fork_page_failure_does_not_block_backfill(self) -> None:
        """Backfill carries on without fork depths when the forks endpoint fails."""
        self.api.uncles = [uncle(n) for n in range(3, 0, -1)]
        self.api.fail = 1
        with self.assertLogs(cs.log, "WARNING"):
            self.assertEqual(await self.importer().backfill(1), 3)

    async def test_run_backs_off_and_recovers(self) -> None:
        """run() reports errors in health(), backs off, then recovers."""
        self.api.uncles = [uncle(n) for n in range(3, 0, -1)]
        self.api.fail = 2
        importer = self.importer(interval=0.02, fetch_forks=False, backfill_pages=1)
        with self.assertLogs(cs.log, "WARNING") as logs:
            task = asyncio.create_task(importer.run())
            for _ in range(100):
                await asyncio.sleep(0.02)
                if importer.health()["status"] == "ok":
                    break
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(len(logs.records), 2)
        health = importer.health()
        self.assertEqual((health["status"], health["consecutive_errors"]), ("ok", 0))
        self.assertIn("HTTP 500", health["last_error"])
        self.assertEqual(len(self.stored()), 3)

    def test_from_config_and_url_validation(self) -> None:
        """The config maps onto the importer; non-http URLs are refused."""
        config = CipherscanConfig(base_url=self.base_url + "/", interval=30.0, backfill_pages=4)
        importer = CipherscanImporter.from_config(config, self.monitor)
        self.assertEqual((importer.base_url, importer.interval, importer.backfill_pages), (self.base_url, 30.0, 4))
        with self.assertRaises(ValueError):
            CipherscanImporter("ftp://example.com", self.monitor)


if __name__ == "__main__":
    unittest.main()
