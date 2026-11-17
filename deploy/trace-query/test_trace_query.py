"""Tests for trace_query against a stdlib model of the node's raw trace endpoint."""

import http.server
import io
import json
import os
import sqlite3
import sys
import tempfile
import threading
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

import trace_query  # noqa: E402


class FakeEndpoint(http.server.ThreadingHTTPServer):
    """Serves `<dir>/<name>.jsonl` with the node's HTTP rules (docs/raw-trace-endpoint.md)."""

    def __init__(self, directory):
        self.directory = Path(directory)
        self.requests = []
        # Test hooks that make the server misbehave.
        self.lie_content_range = False
        self.truncate_body_after = None
        self.ignore_if_range = False
        self.fail_with = None
        super().__init__(("127.0.0.1", 0), FakeHandler)

    @property
    def url(self):
        return f"http://127.0.0.1:{self.server_address[1]}"


def etag_of(stat):
    return f'"{stat.st_dev:x}-{stat.st_ino:x}-0"'


class FakeHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def send(self, status, body=b"", headers=(), length=None):
        self.send_response(status)
        for name, value in headers:
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body) if length is None else length))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        server = self.server
        server.requests.append((self.path, self.headers.get("Range"), self.headers.get("If-Range")))
        if self.headers.get("Host") != f"127.0.0.1:{server.server_address[1]}":
            return self.send(403)
        if server.fail_with is not None:
            return self.send(server.fail_with)
        if self.path == "/traces":
            tables = [{"name": path.stem, "bytes": path.stat().st_size, "etag": etag_of(path.stat())}
                      for path in sorted(server.directory.glob("*.jsonl"))]
            return self.send(200, json.dumps({"tables": tables}).encode(),
                             [("Content-Type", "application/json")])
        table = self.path.removeprefix("/traces/")
        path = server.directory / f"{table}.jsonl"
        if not trace_query.TABLE_NAME.fullmatch(table) or not path.exists():
            return self.send(404)
        stat = path.stat()
        data = path.read_bytes()
        etag = etag_of(stat)
        identity = [("ETag", etag), ("Accept-Ranges", "bytes")]
        requested = self.headers.get("Range")
        if_range = self.headers.get("If-Range")
        if requested and (if_range is None or if_range == etag or server.ignore_if_range):
            start = int(requested.removeprefix("bytes=").rstrip("-"))
            if start >= len(data):
                return self.send(416, b"", identity + [("Content-Range", f"bytes */{len(data)}")])
            body = data[start:]
            end = len(data) - 1 + (1 if server.lie_content_range else 0)
            headers = identity + [("Content-Range", f"bytes {start}-{end}/{len(data)}")]
            status = 206
        else:
            body, headers, status = data, identity, 200
        if server.truncate_body_after is not None:
            return self.send(status, body[:server.truncate_body_after], headers, length=len(body))
        self.send(status, body, headers)


def row(event, **fields):
    return (json.dumps({"ts": 1, "node": "n", "process_trace_id": "p", "event": event, **fields})
            + "\n").encode()


class EndpointCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.server = FakeEndpoint(self.dir.name)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        # Small chunks so every poll spans many reads.
        self.endpoint = trace_query.Endpoint(self.server.url, chunk_bytes=16)
        self.path = Path(self.dir.name) / "commit_state.jsonl"

    def append(self, data):
        with open(self.path, "ab") as file:
            file.write(data)

    def events(self, poll):
        return [fields["event"] for _, fields in poll.rows]


class EndpointTest(EndpointCase):
    def test_one_request_per_poll_reads_only_appended_rows(self):
        self.append(row("a", height=1) + row("b", height=2))
        first = self.endpoint.poll("commit_state", trace_query.Cursor())
        self.assertEqual(self.events(first), ["a", "b"])
        self.assertEqual(first.cursor, trace_query.Cursor(etag_of(self.path.stat()), self.path.stat().st_size))
        self.assertFalse(first.reset)

        self.append(row("c", height=3))
        self.server.requests.clear()
        second = self.endpoint.poll("commit_state", first.cursor)
        self.assertEqual(self.events(second), ["c"])
        self.assertEqual(second.rows[0][0], first.cursor.offset)
        self.assertEqual(self.server.requests, [
            ("/traces/commit_state", f"bytes={first.cursor.offset}-", first.cursor.file_id)])

        idle = self.endpoint.poll("commit_state", second.cursor)
        self.assertEqual((idle.rows, idle.cursor, idle.reset), ([], second.cursor, False))

    def test_partial_line_waits_for_its_newline(self):
        whole = row("a") + row("b")
        self.append(whole[:-5])
        first = self.endpoint.poll("commit_state", trace_query.Cursor())
        self.assertEqual(self.events(first), ["a"])
        self.assertEqual(first.cursor.offset, len(row("a")))
        self.append(whole[-5:])
        second = self.endpoint.poll("commit_state", first.cursor)
        self.assertEqual(self.events(second), ["b"])

    def test_replacement_returns_the_new_file(self):
        self.append(row("old") * 3)
        first = self.endpoint.poll("commit_state", trace_query.Cursor())
        replacement = self.path.with_suffix(".new")
        replacement.write_bytes(row("new"))
        os.replace(replacement, self.path)
        second = self.endpoint.poll("commit_state", first.cursor)
        self.assertTrue(second.reset)
        self.assertEqual(self.events(second), ["new"])
        self.assertEqual(second.rows[0][0], 0)
        self.assertNotEqual(second.cursor.file_id, first.cursor.file_id)

    def test_truncation_restarts_at_byte_zero(self):
        self.append(row("a") * 3)
        first = self.endpoint.poll("commit_state", trace_query.Cursor())
        os.truncate(self.path, 0)
        self.append(row("b"))
        truncated = self.endpoint.poll("commit_state", first.cursor)
        self.assertEqual((truncated.rows, truncated.cursor, truncated.reset), ([], trace_query.Cursor(), True))
        self.assertEqual(self.events(self.endpoint.poll("commit_state", truncated.cursor)), ["b"])

    def test_missing_and_empty_tables(self):
        cursor = trace_query.Cursor('"1-2-0"', 10)
        self.assertEqual(self.endpoint.poll("commit_state", cursor).cursor, cursor)
        self.path.write_bytes(b"")
        empty = self.endpoint.poll("commit_state", trace_query.Cursor())
        self.assertEqual((empty.rows, empty.cursor, empty.reset), ([], trace_query.Cursor(), False))

    def test_budget_splits_a_catch_up_and_long_lines_still_progress(self):
        self.append(row("a", pad="x" * 40) + row("b"))
        first = self.endpoint.poll("commit_state", trace_query.Cursor(), budget_bytes=16)
        self.assertEqual(self.events(first), ["a"])
        self.assertTrue(first.more)
        second = self.endpoint.poll("commit_state", first.cursor, budget_bytes=16)
        self.assertEqual(self.events(second), ["b"])
        self.assertFalse(second.more)

    def test_malformed_lines_are_counted_and_skipped(self):
        self.append(b"not json\n[1]\n" + row("a"))
        poll = self.endpoint.poll("commit_state", trace_query.Cursor())
        self.assertEqual((self.events(poll), poll.malformed), (["a"], 2))

    def test_tunnelled_host_header_index_and_dump(self):
        self.append(row("a") + row("b"))
        tunnel = trace_query.Endpoint(self.server.url.replace("127.0.0.1", "localhost"),
                                      host_header=f"127.0.0.1:{self.server.server_address[1]}",
                                      chunk_bytes=16)
        self.assertEqual(b"".join(tunnel.dump("commit_state")), self.path.read_bytes())
        self.assertEqual(tunnel.tables(), [{"name": "commit_state", "bytes": self.path.stat().st_size,
                                            "etag": etag_of(self.path.stat())}])
        for name in ("../etc/passwd", "Conn", "x" * 65):
            with self.subTest(name=name), self.assertRaises(ValueError):
                list(tunnel.dump(name))

    def test_oversized_unfinished_line_fails_without_advancing(self):
        self.append(b"x" * 100)
        cursor = trace_query.Cursor()
        with patch.object(trace_query, "MAX_LINE_BYTES", 32):
            with self.assertRaises(trace_query.EndpointError):
                self.endpoint.poll("commit_state", cursor, budget_bytes=16)
        self.assertEqual(cursor, trace_query.Cursor())

    def test_mismatched_range_etag_or_short_body_is_rejected(self):
        self.append(row("a") + row("b"))
        first = self.endpoint.poll("commit_state", trace_query.Cursor())
        self.append(row("c"))
        cases = {
            "content range": (first.cursor, "lie_content_range", True),
            "short body": (first.cursor, "truncate_body_after", 3),
        }
        for name, (cursor, hook, value) in cases.items():
            with self.subTest(name), patch.object(self.server, hook, value):
                with self.assertRaises(trace_query.EndpointError):
                    self.endpoint.poll("commit_state", cursor)
        # A 206 under a different ETag than the cursor's means the server ignored If-Range.
        with patch.object(self.server, "ignore_if_range", True), self.assertRaises(trace_query.EndpointError):
            self.endpoint.poll("commit_state", trace_query.Cursor('"other-0-0"', first.cursor.offset))

    def test_invalid_metadata_does_not_poison_sqlite_batch(self):
        self.append(row([], ts=2**100) + row("a"))
        poll = self.endpoint.poll("commit_state", trace_query.Cursor())
        self.assertEqual((self.events(poll), poll.malformed), (["a"], 1))


    def test_unexpected_status_is_an_endpoint_error(self):
        self.append(row("a"))
        for status in (204, 503):
            with self.subTest(status=status), patch.object(self.server, "fail_with", status), \
                    self.assertRaisesRegex(trace_query.EndpointError, f"HTTP {status}"):
                self.endpoint.poll("commit_state", trace_query.Cursor())


class MirrorTest(EndpointCase):
    def mirror(self):
        return trace_query.Mirror(str(Path(self.dir.name) / "mirror.sqlite"))

    def test_follow_mirrors_rows_once_and_queries_them(self):
        self.append(row("commit_finish", hash="aa", height=7) + row("commit_start", hash="aa"))
        mirror = self.mirror()
        endpoints = {"local": self.endpoint}
        self.assertEqual(trace_query.follow(endpoints, ["commit_state", "block_sync"], mirror), 2)
        self.assertEqual(trace_query.follow(endpoints, ["commit_state"], mirror), 0)
        self.append(row("commit_finish", hash="bb", height=8))
        self.assertEqual(trace_query.follow(endpoints, ["commit_state"], mirror), 1)

        columns, rows = trace_query.run_query(
            mirror_path(mirror),
            "SELECT json_extract(row, '$.hash') AS hash, json_extract(row, '$.height') "
            "FROM commit_state WHERE event = ? ORDER BY offset", ["commit_finish"])
        self.assertEqual(columns[0], "hash")
        self.assertEqual(rows, [("aa", 7), ("bb", 8)])

    def test_event_filter_and_retention(self):
        self.append(row("commit_finish") + row("commit_start"))
        mirror = self.mirror()
        trace_query.follow({"local": self.endpoint}, ["commit_state"], mirror, events={"commit_finish"})
        _, rows = trace_query.run_query(mirror_path(mirror), "SELECT event FROM trace_rows")
        self.assertEqual(rows, [("commit_finish",)])
        self.assertEqual(mirror.prune(older_than_unix=float("inf")), 1)

    def test_queries_are_read_only(self):
        mirror = self.mirror()
        with self.assertRaises(sqlite3.DatabaseError):
            trace_query.run_query(mirror_path(mirror), "DELETE FROM trace_rows")
        for query in ("PRAGMA query_only=OFF", "ATTACH ':memory:' AS other",
                      "CREATE TEMP TABLE temp_table (x)"):
            with self.subTest(query=query), self.assertRaises(sqlite3.DatabaseError):
                trace_query.run_query(mirror_path(mirror), query)

    def test_follow_yields_to_other_tables_before_catching_up(self):
        self.append(row("a") * 10)
        (Path(self.dir.name) / "block_sync.jsonl").write_bytes(row("b"))
        mirror = self.mirror()
        trace_query.follow({"local": self.endpoint}, ["commit_state", "block_sync"],
                           mirror, budget_bytes=16)
        self.assertLess(mirror.cursor("local", "commit_state").offset, self.path.stat().st_size)
        self.assertGreater(mirror.cursor("local", "block_sync").offset, 0)

    def test_replayed_rows_report_zero_inserts(self):
        self.append(row("a"))
        poll = self.endpoint.poll("commit_state", trace_query.Cursor())
        mirror = self.mirror()
        self.assertEqual(mirror.store("local", "commit_state", poll), 1)
        self.assertEqual(mirror.store("local", "commit_state", poll), 0)

    def test_follow_without_tables_uses_the_index(self):
        self.append(row("a"))
        (Path(self.dir.name) / "trace_rows.jsonl").write_bytes(row("b"))
        mirror = self.mirror()
        self.assertEqual(trace_query.follow({"local": self.endpoint}, None, mirror), 2)
        # A table named like the mirror's own table gets no view, so the mirror stays intact.
        _, rows = trace_query.run_query(mirror_path(mirror), "SELECT tbl, event FROM trace_rows ORDER BY tbl")
        self.assertEqual(rows, [("commit_state", "a"), ("trace_rows", "b")])

    def test_follow_skips_a_failing_node_and_keeps_going(self):
        self.append(row("a"))
        other = FakeEndpoint(self.dir.name)
        threading.Thread(target=other.serve_forever, daemon=True).start()
        self.addCleanup(other.server_close)
        self.addCleanup(other.shutdown)
        other.fail_with = 503
        endpoints = {"down": trace_query.Endpoint(other.url), "local": self.endpoint}
        with self.assertLogs("trace_query", "WARNING"):
            self.assertEqual(trace_query.follow(endpoints, ["commit_state"], self.mirror()), 1)

    def test_open_read_does_not_block_follow(self):
        self.append(row("a"))
        mirror = self.mirror()
        reader = sqlite3.connect(mirror_path(mirror))
        self.addCleanup(reader.close)
        reader.execute("BEGIN")
        reader.execute("SELECT count(*) FROM trace_rows").fetchall()
        self.assertEqual(trace_query.follow({"local": self.endpoint}, ["commit_state"], mirror), 1)

    def test_locked_mirror_keeps_the_cursor_for_the_next_pass(self):
        self.append(row("a"))
        with patch.object(trace_query, "BUSY_TIMEOUT_SECS", 0.05):
            mirror = self.mirror()
        writer = sqlite3.connect(mirror_path(mirror), isolation_level=None)
        self.addCleanup(writer.close)
        writer.execute("BEGIN IMMEDIATE")
        endpoints = {"local": self.endpoint}
        with self.assertLogs("trace_query", "WARNING") as logs:
            self.assertEqual(trace_query.follow(endpoints, ["commit_state"], mirror), 0)
        self.assertIn("database is locked", logs.output[0])
        self.assertEqual(mirror.cursor("local", "commit_state"), trace_query.Cursor())
        writer.execute("COMMIT")
        self.assertEqual(trace_query.follow(endpoints, ["commit_state"], mirror), 1)

    def test_cli_pull_then_sql(self):
        self.append(row("commit_finish", hash="aa"))
        db = str(Path(self.dir.name) / "cli.sqlite")
        self.assertEqual(trace_query.main(
            ["pull", "--db", db, "--node", f"local={self.server.url}", "--table", "commit_state"]), 0)
        output = io.StringIO()
        with redirect_stdout(output):
            trace_query.main(["sql", "--db", db, "--format", "json",
                              "SELECT node, json_extract(row, '$.hash') AS hash FROM commit_state"])
        self.assertEqual(json.loads(output.getvalue()), {"node": "local", "hash": "aa"})


def mirror_path(mirror):
    return mirror.db.execute("PRAGMA database_list").fetchone()[2]


if __name__ == "__main__":
    unittest.main()
