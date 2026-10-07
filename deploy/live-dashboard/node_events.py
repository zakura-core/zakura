"""Bounded, validated local node events. Never expose raw datagrams to browsers."""

from collections import OrderedDict
import json
import re
from transaction_events import parse_transaction_event, summarize_transactions

MAX_EVENT_BYTES = 8192
MAX_ATTEMPTS = 4096
APPLY_EVENTS = {"block_submit_queued", "commit_start", "commit_finish"}
STAGE_EVENTS = {"block_stage_started", "block_stage_finished"}
STAGES = {"contextual_validation", "initial_checks", "transparent_spends", "shielded_anchors", "parallel_state_update"}
BLOCK_EVENTS = APPLY_EVENTS | STAGE_EVENTS | {"block_inventory_received", "block_body_received", "block_relay_started", "block_relay_finished"}
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
        if kind in STAGE_EVENTS:
            if not integer(event.get("stage_token")) or event.get("stage") not in STAGES:
                return None
            public.update(stage=event["stage"], stage_token=event["stage_token"])
            if kind == "block_stage_finished":
                if type(event.get("success")) is not bool:
                    return None
                public["success"] = event["success"]
        elif kind in APPLY_EVENTS:
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
                duration = event.get("request_queue_to_body_ms")
                if duration is not None and (type(duration) not in (int, float) or not 0 <= duration <= 86_400_000):
                    return None
                public["request_queue_to_body_ms"] = duration
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


def parse_network_event(data):
    if len(data) > MAX_EVENT_BYTES:
        return None
    try:
        row = json.loads(data)
        if not isinstance(row, dict) or type(row.get("version")) is not int or row["version"] != 1:
            return None
        process = row.get("process")
        if not isinstance(process, str) or not re.fullmatch(r"[0-9]{1,20}-[0-9]{1,30}", process):
            return None
        event = row.get("event")
        if not isinstance(event, dict) or event.get("event") != "native_connection":
            return None
        fields = ("connection", "rx_bytes", "tx_bytes", "lost_packets", "lost_bytes")
        if not all(integer(event.get(key)) for key in fields) or not integer(row.get("monotonic_ns")):
            return None
        if type(event.get("closed")) is not bool:
            return None
        rtt = event.get("rtt_ms")
        if rtt is not None and (type(rtt) not in (int, float) or not 0 <= rtt <= 600_000):
            return None
        return {**{key: event[key] for key in fields}, "closed": event["closed"], "rtt_ms": rtt,
                "process": process, "monotonic_ns": row["monotonic_ns"]}
    except (ValueError, TypeError, RecursionError):
        return None


CRYPTO_UNITS = {"halo2": "actions", "groth16_sapling": "spends_and_outputs",
                "ed25519": "signatures", "redpallas": "signatures", "redjubjub": "signatures"}


def parse_crypto_event(data):
    """Only completed, nonempty batches with finite measured durations are public."""
    if len(data) > MAX_EVENT_BYTES:
        return None
    try:
        row = json.loads(data)
        if not isinstance(row, dict) or type(row.get("version")) is not int or row["version"] != 1:
            return None
        process = row.get("process")
        if not isinstance(process, str) or not re.fullmatch(r"[0-9]{1,20}-[0-9]{1,30}", process):
            return None
        if not all(integer(row.get(key)) for key in ("sequence", "monotonic_ns", "unix_ms")):
            return None
        event = row.get("event")
        if not isinstance(event, dict) or event.get("event") != "crypto_batch":
            return None
        verifier = event.get("verifier")
        if not isinstance(verifier, str) or verifier not in CRYPTO_UNITS or event.get("unit") != CRYPTO_UNITS[verifier]:
            return None
        if event.get("mode") not in ("batch", "fallback", "drop_flush") or type(event.get("success")) is not bool:
            return None
        if not integer(event.get("items")) or event["items"] == 0 or not integer(event.get("work_units")):
            return None
        durations = ("in_batch_wait_ms", "scheduling_ms", "execution_ms")
        # A day is also the history horizon. Reject infinities, NaNs and booleans.
        if not all(type(event.get(key)) in (int, float) and 0 <= event[key] <= 86_400_000 for key in durations):
            return None
        return {**{key: row[key] for key in ("process", "sequence", "monotonic_ns", "unix_ms")},
                **{key: event[key] for key in ("verifier", "unit", "mode", "success", "items", "work_units", *durations)}}
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
        self.network = OrderedDict()
        self.received = 0
        self.rejected = 0
        self.last_received = None
        self.error = None
        with self.db:
            self.db.execute("CREATE TABLE IF NOT EXISTS node_block_events (process TEXT, sequence TEXT, hash TEXT, received REAL, body TEXT, PRIMARY KEY(process, sequence))")
            self.db.execute("CREATE INDEX IF NOT EXISTS node_events_hash ON node_block_events(hash, received)")
            self.db.execute("CREATE INDEX IF NOT EXISTS node_events_received ON node_block_events(received)")
            self.db.execute("CREATE TABLE IF NOT EXISTS node_crypto_events (process TEXT, sequence TEXT, received REAL, body TEXT, PRIMARY KEY(process, sequence))")
            self.db.execute("CREATE INDEX IF NOT EXISTS node_crypto_received ON node_crypto_events(received)")
            self.db.execute("CREATE TABLE IF NOT EXISTS node_transaction_events (process TEXT, sequence TEXT, received REAL, body TEXT, PRIMARY KEY(process, sequence))")
            self.db.execute("CREATE INDEX IF NOT EXISTS node_transaction_received ON node_transaction_events(received)")

    def ingest(self, datagrams, now):
        accepted = []
        crypto = []
        transactions = []
        for data in datagrams[:256]:
            event = parse_block_event(data)
            if event is not None:
                accepted.append(event)
                continue
            batch = parse_crypto_event(data)
            if batch is not None:
                crypto.append(batch)
                continue
            transaction = parse_transaction_event(data)
            if transaction is not None:
                transactions.append(transaction)
                continue
            network = parse_network_event(data)
            if network is None:
                self.rejected += 1
                continue
            with self.lock:
                key = (network["process"], network["connection"])
                previous = self.network.get(key)
                if previous and network["monotonic_ns"] <= previous["monotonic_ns"]:
                    continue
                network["received_at"] = now
                seconds = (network["monotonic_ns"] - previous["monotonic_ns"]) / 1e9 if previous else 0
                for field in ("rx_bytes", "tx_bytes"):
                    network[field + "_ps"] = ((network[field] - previous[field]) / seconds
                        if previous and 0 < seconds <= 15 and network[field] >= previous[field] else None)
                self.network[key] = network
                self.network.move_to_end(key)
                while len(self.network) > 512:
                    self.network.popitem(last=False)
                self.received += 1
                self.last_received = now
        if not accepted and not crypto and not transactions:
            return 0
        with self.lock, self.db:
            for event in accepted:
                self.db.execute("INSERT OR IGNORE INTO node_block_events VALUES (?, ?, ?, ?, ?)",
                                (event["process"], str(event["sequence"]), event["hash"], now, json.dumps(event)))
            self.db.execute("DELETE FROM node_block_events WHERE received < ?", (now - 86400,))
            self.db.execute("DELETE FROM node_block_events WHERE rowid IN (SELECT rowid FROM node_block_events ORDER BY received DESC, rowid DESC LIMIT -1 OFFSET 49152)")
            for event in crypto:
                self.db.execute("INSERT OR IGNORE INTO node_crypto_events VALUES (?, ?, ?, ?)",
                                (event["process"], str(event["sequence"]), now, json.dumps(event)))
            if crypto:
                self.db.execute("DELETE FROM node_crypto_events WHERE received < ?", (now - 86400,))
                self.db.execute("DELETE FROM node_crypto_events WHERE rowid IN (SELECT rowid FROM node_crypto_events ORDER BY received DESC, rowid DESC LIMIT -1 OFFSET 32768)")
            for event in transactions:
                self.db.execute("INSERT OR IGNORE INTO node_transaction_events VALUES (?, ?, ?, ?)",
                                (event["process"], str(event["sequence"]), now, json.dumps(event)))
            if transactions:
                self.db.execute("DELETE FROM node_transaction_events WHERE received < ?", (now - 86400,))
                self.db.execute("DELETE FROM node_transaction_events WHERE rowid IN (SELECT rowid FROM node_transaction_events ORDER BY received DESC, rowid DESC LIMIT -1 OFFSET 65536)")
            self.received += len(accepted) + len(crypto) + len(transactions)
            self.last_received = now
        return len(accepted) + len(crypto) + len(transactions)

    def transactions(self, start, end):
        with self.lock:
            rows = self.db.execute("SELECT body, received FROM node_transaction_events WHERE received >= ? AND received <= ? ORDER BY received DESC, rowid DESC LIMIT 8193",
                                   (end - 86400, end)).fetchall()
            retained = self.db.execute("SELECT COUNT(*) FROM node_transaction_events").fetchone()[0]
        events = [{**json.loads(body), "at": received} for body, received in rows[:8192]]
        return {**summarize_transactions(events, start, end),
                "limited": len(rows) > 8192 or retained >= 65536, "status": self.status()}

    def crypto(self, start, end):
        """Return individual completions, never repeated polling snapshots.

        Receipt time selects the window consistently with other collected history.
        Keep node wall time separately, since its clock can jump across restarts.
        """
        with self.lock:
            rows = self.db.execute("SELECT body, received FROM node_crypto_events WHERE received >= ? AND received <= ? ORDER BY received DESC, rowid DESC LIMIT 4097",
                                   (start, end)).fetchall()
            retained = self.db.execute("SELECT COUNT(*) FROM node_crypto_events").fetchone()[0]
        return {"samples": [{**json.loads(body), "at": received} for body, received in reversed(rows[:4096])],
                "limited": len(rows) > 4096 or retained >= 32768, "status": self.status()}

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
        return {"attempts": attempts.block(block_hash, now), "arrival": arrival_summary(events), "stages": stage_summary(events),
                "status": self.status(), "limited": len(rows) == 192}

    def native(self, now):
        with self.lock:
            rows = [row for row in self.network.values() if not row["closed"] and 0 <= now - row["received_at"] <= 15]
            public = [{key: row[key] for key in ("connection", "rtt_ms", "rx_bytes_ps", "tx_bytes_ps", "lost_packets", "received_at")} for row in rows]
        return {"connections": public, "status": self.status()}

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
        import select
        import socket
        import time
        while not self.stop.is_set():
            try:
                # Reading one byte beyond the cap detects truncated oversized packets.
                datagrams = [self.socket.recv(MAX_EVENT_BYTES + 1)]
                # Drain a bounded burst into one SQLite transaction. This socket
                # has a single reader, so readiness cannot be consumed elsewhere.
                for _ in range(255):
                    if not select.select([self.socket], [], [], 0)[0]:
                        break
                    datagrams.append(self.socket.recv(MAX_EVENT_BYTES + 1))
                self.ingest(datagrams, time.time())
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
                          "request_queue_to_body_ms": body.get("request_queue_to_body_ms") if body else None,
                          "relay_successes": sum(r["succeeded"] for r in relays),
                          "relay_failures": sum(not r["succeeded"] for r in relays),
                          "relay_observed": bool(relays)})
    return summaries


def stage_summary(events):
    """Pair stage occurrences only by process/hash/token, never by nearest time."""
    occurrences = OrderedDict()
    for row in sorted(events, key=lambda event: (event["monotonic_ns"], event["sequence"])):
        if row["kind"] in STAGE_EVENTS:
            key = (row["process"], row["hash"], row["stage_token"], row["stage"])
            occurrences.setdefault(key, {}).setdefault(row["kind"], row)
    stages = []
    for (_, _, token, name), boundaries in occurrences.items():
        start = boundaries.get("block_stage_started")
        end = boundaries.get("block_stage_finished")
        valid = start is not None and end is not None and end["monotonic_ns"] >= start["monotonic_ns"]
        stages.append({"stage": name, "occurrence": str(token),
                       "started_at": start["unix_ms"] / 1000 if start else None,
                       "duration_ms": (end["monotonic_ns"] - start["monotonic_ns"]) / 1e6 if valid else None,
                       "success": end["success"] if valid else None,
                       "complete": valid})
    return stages
