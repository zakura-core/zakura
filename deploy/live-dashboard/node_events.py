"""Bounded, validated local node events. Never expose raw datagrams to browsers."""

from collections import OrderedDict
import json
import re

MAX_EVENT_BYTES = 8192
MAX_ATTEMPTS = 4096
APPLY_EVENTS = {"block_submit_queued", "commit_start", "commit_finish"}
BLOCK_EVENTS = APPLY_EVENTS | {"block_inventory_received", "block_body_received", "block_relay_started", "block_relay_finished"}
RESULTS = {"committed", "duplicate", "rejected", "unavailable", "timed_out"}


def integer(value, maximum=2**64 - 1):
    return type(value) is int and 0 <= value <= maximum


def parse_block_event(data):
    """Allowlist public fields and require a complete process/attempt identity."""
    if len(data) > MAX_EVENT_BYTES:
        return None
    try:
        row = json.loads(data)
        if not isinstance(row, dict) or type(row.get("version")) is not int or row.get("version") != 1:
            return None
        process = row.get("process")
        if not isinstance(process, str) or not re.fullmatch(r"[0-9]{1,20}-[0-9]{1,30}", process):
            return None
        if not all(integer(row.get(key)) for key in ("sequence", "monotonic_ns", "unix_ms")):
            return None
        event = row.get("event")
        if not isinstance(event, dict) or event.get("event") not in BLOCK_EVENTS:
            return None
        block_hash = event.get("hash")
        if not isinstance(block_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", block_hash):
            return None
        kind = event["event"]
        public = {"process": process, "sequence": row["sequence"],
                  "monotonic_ns": row["monotonic_ns"], "unix_ms": row["unix_ms"],
                  "kind": kind, "hash": block_hash}
        if kind in APPLY_EVENTS:
            if not integer(event.get("apply_token")) or not integer(event.get("height"), 2**32 - 1):
                return None
            result = event.get("result")
            if kind == "commit_finish" and result not in RESULTS:
                return None
            public.update(height=event["height"], attempt=event["apply_token"],
                          result=result if kind == "commit_finish" else None)
        elif kind in ("block_inventory_received", "block_body_received"):
            if event.get("transport") not in ("zakura", "legacy", "unknown"):
                return None
            public["transport"] = event["transport"]
            if kind == "block_body_received":
                if type(event.get("first")) is not bool:
                    return None
                public["first"] = event["first"]
        else:
            if not integer(event.get("relay_attempt")):
                return None
            public["relay_attempt"] = event["relay_attempt"]
            if kind == "block_relay_finished":
                if type(event.get("succeeded")) is not bool:
                    return None
                public["succeeded"] = event["succeeded"]
        return public
    except (ValueError, TypeError, RecursionError):
        return None


class BlockAttempts:
    """Join only boundaries from the same process, hash, and apply token.

    Records may arrive out of order. Missing or backwards boundaries never turn
    into zero durations. Retention is bounded by capacity and receipt age.
    """
    def __init__(self, capacity=MAX_ATTEMPTS):
        if not 1 <= capacity <= MAX_ATTEMPTS:
            raise ValueError("invalid attempt capacity")
        self.capacity = capacity
        self.attempts = OrderedDict()

    def ingest(self, event, received_at):
        self.expire(received_at)
        key = (event["process"], event["hash"], event["attempt"])
        record = self.attempts.setdefault(key, {"hash": event["hash"], "height": event["height"],
                                               "received_at": received_at, "events": {}})
        # Retransmitted/duplicate records cannot change an already observed boundary.
        record["events"].setdefault(event["kind"], event)
        while len(self.attempts) > self.capacity:
            self.attempts.popitem(last=False)

    def expire(self, now):
        for key in list(self.attempts):
            if now - self.attempts[key]["received_at"] > 86400:
                del self.attempts[key]

    def block(self, block_hash, now):
        self.expire(now)
        result = []
        for record in self.attempts.values():
            if record["hash"] != block_hash:
                continue
            events = record["events"]
            def span(start, end):
                first, last = events.get(start), events.get(end)
                if first is None or last is None or last["monotonic_ns"] < first["monotonic_ns"]:
                    return None
                return (last["monotonic_ns"] - first["monotonic_ns"]) / 1_000_000
            finished = events.get("commit_finish")
            result.append({"height": record["height"], "observed_at": record["received_at"],
                           "queue_ms": span("block_submit_queued", "commit_start"),
                           "verify_and_commit_ms": span("commit_start", "commit_finish"),
                           "result": finished["result"] if finished else "incomplete",
                           "boundaries": sorted(events)})
        return result


class EventFeed:
    """Local receiver with bounded persistent history and no node RPC access."""
    def __init__(self, database, socket_path=None):
        import sqlite3
        import threading
        self.db = sqlite3.connect(database, check_same_thread=False, timeout=1)
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.socket_path = socket_path
        self.socket = None
        self.thread = None
        self.received = 0
        self.rejected = 0
        self.last_received = None
        self.error = None
        with self.db:
            self.db.execute("CREATE TABLE IF NOT EXISTS node_block_events (process TEXT, sequence TEXT, hash TEXT, received REAL, body TEXT, PRIMARY KEY(process, sequence))")
            self.db.execute("CREATE INDEX IF NOT EXISTS node_events_hash ON node_block_events(hash, received)")
            self.db.execute("CREATE INDEX IF NOT EXISTS node_events_received ON node_block_events(received)")

    def ingest(self, datagrams, now):
        accepted = []
        for data in datagrams[:256]:
            event = parse_block_event(data)
            if event is None:
                self.rejected += 1
            else:
                accepted.append(event)
        with self.lock, self.db:
            for event in accepted:
                self.db.execute("INSERT OR IGNORE INTO node_block_events VALUES (?, ?, ?, ?, ?)",
                                (event["process"], str(event["sequence"]), event["hash"], now, json.dumps(event)))
            self.db.execute("DELETE FROM node_block_events WHERE received < ?", (now - 86400,))
            self.db.execute("DELETE FROM node_block_events WHERE rowid IN (SELECT rowid FROM node_block_events ORDER BY received DESC, rowid DESC LIMIT -1 OFFSET 49152)")
            if accepted:
                self.received += len(accepted)
                self.last_received = now
        return len(accepted)

    def block(self, block_hash, now):
        with self.lock:
            # Bound pathological retries for one hash without hiding missing boundaries.
            rows = self.db.execute("SELECT body, received FROM node_block_events WHERE hash = ? AND received >= ? ORDER BY received DESC, rowid DESC LIMIT 192",
                                   (block_hash, now - 86400)).fetchall()
        attempts = BlockAttempts(capacity=192)
        events = []
        for body, received in reversed(rows):
            event = json.loads(body)
            events.append(event)
            if event["kind"] in APPLY_EVENTS:
                attempts.ingest(event, received)
        return {"attempts": attempts.block(block_hash, now), "arrival": arrival_summary(events),
                "status": self.status(), "limited": len(rows) == 192}

    def status(self):
        return {"enabled": bool(self.socket_path), "listening": self.socket is not None and self.error is None,
                "received": self.received, "rejected": self.rejected,
                "last_received": self.last_received, "error": self.error}

    def start(self):
        import os
        import socket
        import threading
        if not self.socket_path:
            return
        # The service owns a private runtime directory. Never replace an existing
        # socket or file: a second receiver must fail rather than steal delivery.
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        try:
            self.socket.bind(self.socket_path)
            os.chmod(self.socket_path, 0o660)
            self.socket.settimeout(0.5)
        except OSError:
            self.socket.close()
            self.socket = None
            self.error = "Event socket unavailable"
            return
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def run(self):
        import socket
        import time
        while not self.stop.is_set():
            try:
                # Reading one byte beyond the cap detects truncated oversized packets.
                data = self.socket.recv(MAX_EVENT_BYTES + 1)
                self.ingest([data], time.time())
            except socket.timeout:
                pass
            except Exception:
                self.error = "Event receiver stopped"
                return

    def close(self):
        import os
        self.stop.set()
        if self.thread:
            self.thread.join(timeout=3)
        if self.socket:
            self.socket.close()
            self.socket = None
            try:
                os.unlink(self.socket_path)
            except FileNotFoundError:
                pass
        with self.lock:
            self.db.close()


def arrival_summary(events):
    """Show local observations separately for each node run, never miner time."""
    runs = OrderedDict()
    for event in events:
        runs.setdefault(event["process"], []).append(event)
    summaries = []
    for rows in runs.values():
        rows.sort(key=lambda row: (row["monotonic_ns"], row["sequence"]))
        inventory = next((r for r in rows if r["kind"] == "block_inventory_received"), None)
        bodies = [r for r in rows if r["kind"] == "block_body_received"]
        body = bodies[0] if bodies else None
        commits = [r for r in rows if r["kind"] == "commit_finish" and r.get("result") == "committed"]
        committed = commits[0] if commits else None
        def elapsed(first, last):
            if first is None or last is None or last["monotonic_ns"] < first["monotonic_ns"]:
                return None
            return (last["monotonic_ns"] - first["monotonic_ns"]) / 1_000_000
        relays = [r for r in rows if r["kind"] == "block_relay_finished"]
        summaries.append({"inventory_at": inventory["unix_ms"] / 1000 if inventory else None,
                          "body_at": body["unix_ms"] / 1000 if body else None,
                          "body_transport": body["transport"] if body else None,
                          "bodies_observed": len(bodies),
                          "duplicate_bodies": sum(not r["first"] for r in bodies),
                          "inventory_to_body_ms": elapsed(inventory, body),
                          "body_to_commit_ms": elapsed(body, committed),
                          "relay_successes": sum(r["succeeded"] for r in relays),
                          "relay_failures": sum(not r["succeeded"] for r in relays),
                          "relay_observed": bool(relays)})
    return summaries
