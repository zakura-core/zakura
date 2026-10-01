#!/usr/bin/env python3
"""Bounded sequential comparison, invoked by the existing fleet watchdog."""
import argparse
import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import time
import select
import subprocess

from common import (RPC, Transport, Unavailable, atomic_json, canonical_record,
                    hex_bytes, integer, read_json)


class Mismatch(Unavailable):
    pass


class Disagreement(Unavailable):
    pass


class Remote:
    """One private SSH session per cycle; no host addresses in diagnostics."""
    def __init__(self, config="/etc/zakura-mac-verifier/ssh/config", deadline=None):
        self.deadline = deadline or time.monotonic() + 10
        self.buffer = b""
        self.process = subprocess.Popen(
            ["ssh", "-F", str(config), "-T", "mac-verifier"], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)

    def close(self):
        with contextlib.suppress(OSError):
            self.process.stdin.close()
        self.process.terminate()
        try:
            self.process.wait(timeout=1)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=1)
        self.process.stdout.close()

    def get(self, request):
        if time.monotonic() >= self.deadline:
            raise Unavailable("SSH sample unavailable")
        try:
            self.process.stdin.write((json.dumps(request) + "\n").encode())
            self.process.stdin.flush()
            while b"\n" not in self.buffer:
                remaining = self.deadline - time.monotonic()
                if remaining <= 0 or not select.select([self.process.stdout], [], [], remaining)[0]:
                    raise Unavailable("SSH sample unavailable")
                part = os.read(self.process.stdout.fileno(), 4096)
                if not part or len(self.buffer) + len(part) > 256 * 1024:
                    raise Unavailable("SSH sample unavailable")
                self.buffer += part
            line, self.buffer = self.buffer.split(b"\n", 1)
            result = json.loads(line)
            if not isinstance(result, dict) or "error" in result:
                raise Unavailable("SSH sample unavailable")
            return result
        except (OSError, ValueError):
            raise Unavailable("SSH sample unavailable") from None

    def status(self):
        return self.get({"operation": "status"})

    def block(self, height):
        return canonical_record(self.get({"operation": "block", "height": integer(height)}), height)


class Comparison:
    def __init__(self, directory, expected, linux=None, remote=None):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.expected = expected
        self.linux = linux or RPC("http://127.0.0.1:8232")
        self.remote = remote or Remote()
        path = self.directory / "cursor.json"
        self.state = read_json(path) if path.exists() else {
            "schema_version": 2, "bootstrap": expected["bootstrap_height"],
            "receipt_digest": self.receipt_digest(expected),
            "cursor": expected["bootstrap_height"], "history": {}, "coverage_gap": False,
        }
        if (self.state.get("receipt_digest") != self.receipt_digest(expected)
                or self.state.get("bootstrap") != expected["bootstrap_height"]):
            raise ValueError("receipt changed: explicit transition required")
        if self.state.get("schema_version") != 2:
            raise ValueError("unsupported comparison state schema")

    @staticmethod
    def receipt_digest(receipt):
        return hashlib.sha256(json.dumps(receipt, sort_keys=True).encode()).hexdigest()

    def save(self):
        atomic_json(self.directory / "cursor.json", self.state)

    def audit(self, event):
        path = self.directory / "audit.jsonl"
        if path.exists() and path.stat().st_size >= 16 * 1024 * 1024:
            for index in range(3, 0, -1):
                src = path if index == 1 else Path(str(path) + f".{index - 1}")
                if src.exists():
                    os.replace(src, str(path) + f".{index}")
        with path.open("a") as stream:
            stream.write(json.dumps(event, sort_keys=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    def confirmed_mismatch(self, height, left, right, now):
        evidence = {"height": height, "linux": left, "mac": right, "receipt": self.expected}
        key = hashlib.sha256(json.dumps(evidence, sort_keys=True).encode()).hexdigest()
        path = self.directory / "incidents" / (key + ".json")
        if not path.exists():
            evidence.update(event="confirmed tree state mismatch", time=now)
            atomic_json(path, evidence)
            self.audit(evidence)

    def validate_status(self, status, now):
        if (not isinstance(status, dict) or status.get("schema_version") != 1
                or status.get("architecture") not in ("arm64", "aarch64")
                or status.get("receipt") != self.expected
                or status.get("binary_sha256") != self.expected["binary_sha256"]
                or status.get("config_sha256") != self.expected["config_sha256"]):
            raise Unavailable("unexpected build or configuration")
        stamp = status.get("sample_time")
        if type(stamp) not in (int, float) or not -10 <= now - stamp <= 60:
            raise Unavailable("stale sample")
        integer(status["tip"]["height"])
        hex_bytes(status["tip"]["hash"], 32)

    def pair(self, height):
        return self.linux.block(height), self.remote.block(height)

    def reorg(self, now):
        cursor = self.state["cursor"]
        if cursor == self.state["bootstrap"]:
            anchor, other = self.pair(cursor)
            expected = self.expected["bootstrap_record"]
            if anchor != expected or other != expected:
                raise Unavailable("bootstrap anchor changed")
            return
        left, right = self.pair(cursor)
        saved = self.state["history"].get(str(cursor))
        if left["hash"] == right["hash"] == saved:
            # Also recheck trees at the cursor, not merely block identities.
            if left != right:
                left2, right2 = self.pair(cursor)
                if left2 == left and right2 == right:
                    self.confirmed_mismatch(cursor, left, right, now)
                    raise Mismatch("tree state mismatch")
                raise Unavailable("unstable cursor read")
            return
        lower = max(self.state["bootstrap"], cursor - 1000)
        search = self.state.get("reorg_search")
        upper = search["next"] if search and search["cursor"] == cursor else cursor - 1
        for height in range(upper, lower - 1, -1):
            left, right = self.pair(height)
            saved = (self.expected["bootstrap_record"]["hash"] if height == self.state["bootstrap"]
                     else self.state["history"].get(str(height)))
            if left["hash"] == right["hash"] == saved and left == right:
                self.audit({"event": "reorg", "from": cursor, "to": height, "time": now})
                self.state["cursor"] = height
                self.state["history"] = {h: v for h, v in self.state["history"].items()
                                          if int(h) <= height}
                self.state.pop("reorg_search", None)
                return
            self.state["reorg_search"] = {"cursor": cursor, "next": height - 1}
            self.save()
        self.state["coverage_gap"] = True
        raise Unavailable("reorg exceeds retained coverage")

    def compare(self, target, now):
        self.reorg(now)
        for height in range(self.state["cursor"] + 1, min(target, self.state["cursor"] + 16) + 1):
            left, right = self.pair(height)
            if left["hash"] != right["hash"]:
                raise Disagreement("chain disagreement")
            if left != right:
                left2, right2 = self.pair(height)
                if left2 == left and right2 == right:
                    self.confirmed_mismatch(height, left, right, now)
                    raise Mismatch("tree state mismatch")
                raise Unavailable("unstable tree read")
            # An audit record precedes its durable cursor: a crash can replay, never skip.
            self.audit({"event": "matched", "time": now, "height": height, "record": left})
            self.state["cursor"] = height
            self.state["history"][str(height)] = left["hash"]
            self.state["history"] = {h: v for h, v in self.state["history"].items()
                                      if int(h) >= height - 1000}
            self.save()

    def step(self, now=None):
        now = time.time() if now is None else now
        status, reference, condition = None, None, "unavailable"
        try:
            reference = self.linux.tip()
            status = self.remote.status()
            self.validate_status(status, now)
            if self.state["coverage_gap"]:
                condition = "coverage_gap"
            else:
                target = min(reference["height"], status["tip"]["height"]) - 3
                if target < self.state["cursor"]:
                    self.reorg(now)
                    raise Unavailable("tips behind comparison cursor")
                self.compare(target, now)
                condition = ("matching" if self.state["cursor"] >= target
                             and status["tip"]["height"] >= reference["height"] - 3 else "catching_up")
        except Mismatch:
            condition = "tree_mismatch"
        except Disagreement:
            condition = "chain_disagreement"
        except (Unavailable, OSError, ValueError, KeyError, TypeError):
            condition = "coverage_gap" if self.state["coverage_gap"] else "unavailable"
        self.save()
        result = {"schema_version": 1, "sample_time": now, "reference": reference,
                  "verifier": status, "coverage_start": self.state["bootstrap"] + 1,
                  "compared_through": self.state["cursor"], "caught_up": condition == "matching",
                  "condition": condition, "error": None if condition == "matching" else condition,
                  "incidents": {} if condition == "matching" else {condition: {}},
                  "alerts_muted": True}
        atomic_json(self.directory / "status.json", result)
        return result


@contextlib.contextmanager
def exclusive(directory):
    Path(directory).mkdir(parents=True, exist_ok=True)
    with (Path(directory) / "monitor.lock").open("a") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["once"])
    parser.add_argument("--directory", default="/var/lib/zakura-mac-verifier")
    parser.add_argument("--ssh-config", default="/etc/zakura-mac-verifier/ssh/config")
    parser.add_argument("--receipt", default="/etc/zakura-mac-verifier/receipt.json")
    args = parser.parse_args()
    with exclusive(args.directory):
        expected = read_json(args.receipt)
        transport = Transport(timeout=2, deadline=time.monotonic() + 10)
        remote = Remote(args.ssh_config, deadline=transport.deadline)
        try:
            Comparison(args.directory, expected, RPC("http://127.0.0.1:8232", transport), remote).step()
        finally:
            remote.close()


if __name__ == "__main__":
    main()
