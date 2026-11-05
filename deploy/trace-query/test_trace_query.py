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
    """Serves `<dir>/<table>.jsonl` with the node's range and token rules (raw_endpoint.rs)."""

    def __init__(self, directory, range_limit):
        self.directory = Path(directory)
        self.range_limit = range_limit
        self.requests = []
        super().__init__(("127.0.0.1", 0), FakeHandler)

    @property
    def url(self):
        return f"http://127.0.0.1:{self.server_address[1]}"


class FakeHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def send(self, status, body=b"", headers=()):
        self.send_response(status)
        for name, value in headers:
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        server = self.server
        server.requests.append((self.path, self.headers.get("Range"), self.headers.get("X-Trace-File-Id")))
        expected_host = f"127.0.0.1:{server.server_address[1]}"
        if self.headers.get("Host") != expected_host:
            return self.send(403)
        table = self.path.removeprefix("/traces/")
        path = server.directory / f"{table}.jsonl"
        if not path.exists():
            return self.send(404)
        stat = path.stat()
        token = self.headers.get("X-Trace-File-Id")
        if token:
            device, inode, length = (int(part) for part in token.split(":"))
            if (device, inode) != (stat.st_dev, stat.st_ino) or stat.st_size < length:
                return self.send(409)
        else:
            device, inode, length = stat.st_dev, stat.st_ino, stat.st_size
        start_text, _, end_text = self.headers["Range"].removeprefix("bytes=").partition("-")
        start = int(start_text)
        if start >= length:
            return self.send(416)
        end = min(int(end_text) if end_text else start + server.range_limit - 1, length - 1)
        if end - start >= server.range_limit:
            return self.send(416)
        if start > 0 and not token:
            return self.send(428)
        with open(path, "rb") as file:
            file.seek(start)
            body = file.read(end + 1 - start)
        self.send(206, body, [("X-Trace-File-Id", f"{device}:{inode}:{length}"),
                              ("Content-Range", f"bytes {start}-{end}/{length}")])


def row(event, **fields):
    return (json.dumps({"ts": 1, "node": "n", "process_trace_id": "p", "event": event, **fields})
            + "\n").encode()


class EndpointCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.server = FakeEndpoint(self.dir.name, range_limit=16)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.endpoint = trace_query.Endpoint(self.server.url, range_bytes=16)
        self.path = Path(self.dir.name) / "commit_state.jsonl"

    def append(self, data):
        with open(self.path, "ab") as file:
            file.write(data)

    def events(self, poll):
        return [fields["event"] for _, fields in poll.rows]


class EndpointTest(EndpointCase):
    def test_poll_reads_only_appended_rows_across_small_ranges(self):
        self.append(row("a", height=1) + row("b", height=2))
        first = self.endpoint.poll("commit_state", trace_query.Cursor())
        self.assertEqual(self.events(first), ["a", "b"])
        self.assertEqual(first.cursor.offset, self.path.stat().st_size)
        self.assertFalse(first.reset)

        self.append(row("c", height=3))
        self.server.requests.clear()
        second = self.endpoint.poll("commit_state", first.cursor)
        self.assertEqual(self.events(second), ["c"])
        self.assertEqual(second.rows[0][0], first.cursor.offset)
        # The probe starts at 0; every other range resumes at or after the old cursor.
        ranges = [int(r.removeprefix("bytes=").split("-")[0]) for _, r, _ in self.server.requests]
        self.assertEqual(ranges[0], 0)
        self.assertTrue(all(start >= first.cursor.offset for start in ranges[1:]))

    def test_partial_line_waits_for_its_newline(self):
        whole = row("a") + row("b")
        self.append(whole[:-5])
        first = self.endpoint.poll("commit_state", trace_query.Cursor())
        self.assertEqual(self.events(first), ["a"])
        self.assertEqual(first.cursor.offset, len(row("a")))
        self.append(whole[-5:])
        second = self.endpoint.poll("commit_state", first.cursor)
        self.assertEqual(self.events(second), ["b"])

    def test_replacement_restarts_at_the_new_file(self):
        self.append(row("old") * 3)
        first = self.endpoint.poll("commit_state", trace_query.Cursor())
        replacement = self.path.with_suffix(".new")
        replacement.write_bytes(row("new"))
        os.replace(replacement, self.path)
        second = self.endpoint.poll("commit_state", first.cursor)
        self.assertTrue(second.reset)
        self.assertEqual(self.events(second), ["new"])
        self.assertNotEqual(second.cursor.file_id, first.cursor.file_id)

    def test_missing_and_empty_tables(self):
        cursor = trace_query.Cursor("1:2", 10)
        self.assertEqual(self.endpoint.poll("commit_state", cursor).cursor, cursor)
        self.path.write_bytes(b"")
        empty = self.endpoint.poll("commit_state", cursor)
        self.assertTrue(empty.reset)
        self.assertEqual((empty.rows, empty.cursor), ([], trace_query.Cursor()))

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

    def test_tunnelled_host_header_and_dump(self):
        self.append(row("a") + row("b"))
        tunnel = trace_query.Endpoint(self.server.url.replace("127.0.0.1", "localhost"),
                                      host_header=f"127.0.0.1:{self.server.server_address[1]}",
                                      range_bytes=16)
        self.assertEqual(b"".join(tunnel.dump("commit_state")), self.path.read_bytes())
        with self.assertRaises(ValueError):
            list(tunnel.dump("../etc/passwd"))

    def test_replacement_mid_poll_preserves_row_identity(self):
        self.append(row("old") * 3)
        original = self.endpoint.read_range
        old_id = self.endpoint.probe("commit_state").file_id

        def replace_after_first_row(table, token, start, end):
            if start >= len(row("old")):
                raise trace_query.FileReplaced(table)
            return original(table, token, start, end)

        with patch.object(self.endpoint, "read_range", side_effect=replace_after_first_row):
            poll = self.endpoint.poll("commit_state", trace_query.Cursor())
        self.assertEqual(self.events(poll), ["old"])
        self.assertEqual(poll.cursor.file_id, old_id)
        self.assertEqual(poll.cursor.offset, 0)

    def test_oversized_unfinished_line_fails_without_advancing(self):
        self.append(b"x" * 100)
        cursor = trace_query.Cursor()
        with patch.object(trace_query, "MAX_LINE_BYTES", 32):
            with self.assertRaises(trace_query.EndpointError):
                self.endpoint.poll("commit_state", cursor, budget_bytes=16)
        self.assertEqual(cursor, trace_query.Cursor())

    def test_wrong_range_or_token_is_rejected(self):
        self.append(row("a"))
        token = self.endpoint.probe("commit_state").token
        for headers in ({"content-range": "bytes 0-1/99", "x-trace-file-id": token},
                        {"content-range": f"bytes 0-1/{self.path.stat().st_size}",
                         "x-trace-file-id": "1:2:99"}):
            with self.subTest(headers=headers), patch.object(
                    self.endpoint, "_request", return_value=(206, headers, b"xx")):
                with self.assertRaises(trace_query.EndpointError):
                    self.endpoint.read_range("commit_state", token, 0, 2)

    def test_invalid_metadata_does_not_poison_sqlite_batch(self):
        self.append(row([], ts=2**100) + row("a"))
        poll = self.endpoint.poll("commit_state", trace_query.Cursor())
        self.assertEqual((self.events(poll), poll.malformed), (["a"], 1))


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

    def test_cli_pull_then_sql(self):
        self.server.range_limit = trace_query.MAX_RANGE_BYTES
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
