#!/usr/bin/env python3
"""Durable sequential comparison, incident transitions and Slack DM delivery."""
import argparse
import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import time
import urllib.error
import uuid

from common import (RPC, Transport, Unavailable, atomic_json, canonical_record,
                    hex_bytes, integer, read_json)


class Remote:
    def __init__(self, url="http://127.0.0.1:28233", transport=None):
        if url != "http://127.0.0.1:28233":
            raise ValueError("adapter must use pinned loopback tunnel")
        self.url, self.transport = url, transport or Transport()

    def get(self, path):
        try:
            return self.transport.json(self.url + path)
        except urllib.error.HTTPError as error:
            raise Unavailable(f"adapter HTTP {error.code}") from None

    def status(self):
        return self.get("/v1/status")

    def block(self, height):
        return canonical_record(self.get(f"/v1/block/{height}"), height)


class Slack:
    def __init__(self, token, user, team, transport=None):
        self.token, self.user, self.team = token, user, team
        self.transport = transport or Transport()
        self.channel = None
        self.retry_at = 0

    def call(self, method, data):
        result = self.transport.json("https://slack.com/api/" + method, data,
                                     {"Authorization": "Bearer " + self.token})
        if not result.get("ok"):
            raise Unavailable("Slack delivery rejected")
        return result

    def send(self, item, now):
        if now < self.retry_at:
            return False
        try:
            if self.channel is None:
                auth = self.call("auth.test", {})
                if auth.get("team_id") != self.team:
                    raise Unavailable("unexpected Slack workspace")
                channel = self.call("conversations.open", {"users": self.user})["channel"]
                if not channel["id"].startswith("D"):
                    raise Unavailable("Slack destination is not a DM")
                self.channel = channel["id"]
            self.call("chat.postMessage", {"channel": self.channel, "text": item["text"],
                                           "client_msg_id": item["id"],
                                           "unfurl_links": False, "unfurl_media": False})
            self.retry_at = 0
            return True
        except urllib.error.HTTPError as error:
            try:
                delay = int(error.headers.get("Retry-After", "60"))
            except ValueError:
                delay = 60
            self.retry_at = now + min(3600, max(30, delay))
        except (Unavailable, KeyError, TypeError):
            self.retry_at = now + 60
        return False


class Monitor:
    def __init__(self, directory, expected, linux=None, remote=None, slack=None):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.expected = expected
        self.linux = linux or RPC("http://127.0.0.1:8232")
        self.remote = remote or Remote()
        self.slack = slack
        state = self.directory / "cursor.json"
        self.state = read_json(state) if state.exists() else {
            "schema_version": 1, "bootstrap": expected["bootstrap_height"],
            "receipt_digest": self.receipt_digest(expected),
            "cursor": expected["bootstrap_height"], "history": {}, "incidents": {},
            "outbox": [], "outbox_overflow": False, "healthy_since": None,
            "qualified": False, "last_sample": None, "initial_tip": None,
            "unavailable_since": None, "disagreement_count": 0,
            "tip_watch": None, "caught_up": False,
        }
        if (self.state.get("receipt_digest") != self.receipt_digest(expected)
                or self.state.get("bootstrap") != expected["bootstrap_height"]):
            raise ValueError("receipt changed: explicit rebootstrap required")

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

    def notify(self, text):
        if len(self.state["outbox"]) >= 128:
            self.state["outbox_overflow"] = True
            return
        self.state["outbox"].append({"id": str(uuid.uuid4()), "text": text})

    def incident(self, name, active, now, latched=False):
        incidents = self.state["incidents"]
        item = incidents.get(name)
        if active:
            if item is None:
                incidents[name] = {"since": now, "latched": latched, "good_samples": 0}
                self.notify(f"Zakura Mac verifier: {name}")
            else:
                item["good_samples"] = 0
        elif item and not item["latched"]:
            item["good_samples"] += 1
            if item["good_samples"] >= 2:
                del incidents[name]
                self.notify(f"Zakura Mac verifier recovered: {name}")

    def ack(self, name):
        if name not in self.state["incidents"]:
            raise ValueError("unknown incident")
        self.audit({"event": "acknowledged", "incident": name, "time": time.time()})
        del self.state["incidents"][name]
        self.state["healthy_since"] = None
        self.save()

    def validate_status(self, status, now):
        if (status.get("schema_version") != 1
                or status.get("architecture") not in ("arm64", "aarch64")
                or status.get("receipt") != self.expected
                or status.get("binary_sha256") != self.expected["binary_sha256"]
                or status.get("config_sha256") != self.expected["config_sha256"]):
            self.incident("unexpected build or configuration", True, now, latched=True)
            raise Unavailable("unexpected build or configuration")
        stamp = status.get("sample_time")
        if type(stamp) not in (int, float) or not -10 <= now - stamp <= 60:
            raise Unavailable("stale sample")
        integer(status["tip"]["height"])
        hex_bytes(status["tip"]["hash"], 32)
        if status["resources"]["free_disk_bytes"] < 20 * 10**9:
            self.incident("disk below 20 GB", True, now)
        else:
            self.incident("disk below 20 GB", False, now)
        memory = status["resources"].get("memory_free_percent")
        rss = status["resources"].get("node_rss_bytes")
        self.incident("resource sample incomplete", memory is None or rss is None, now)
        self.incident("memory pressure", memory is not None and memory < 10, now)

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
                    self.incident("confirmed tree state mismatch", True, now, latched=True)
                    raise Unavailable("tree state mismatch")
                raise Unavailable("unstable cursor read")
            return
        lower = max(self.state["bootstrap"], cursor - 1000)
        for height in range(cursor - 1, lower - 1, -1):
            left, right = self.pair(height)
            saved = (self.expected["bootstrap_record"]["hash"] if height == self.state["bootstrap"]
                     else self.state["history"].get(str(height)))
            if left["hash"] == right["hash"] == saved and left == right:
                self.audit({"event": "reorg", "from": cursor, "to": height, "time": now})
                self.state["cursor"] = height
                self.state["history"] = {h: v for h, v in self.state["history"].items()
                                          if int(h) <= height}
                self.state["healthy_since"] = None
                return
        self.incident("coverage gap: rebootstrap required", True, now, latched=True)
        raise Unavailable("reorg exceeds retained coverage")

    def compare(self, target, now):
        self.reorg(now)
        for height in range(self.state["cursor"] + 1, min(target, self.state["cursor"] + 16) + 1):
            left, right = self.pair(height)
            if left["hash"] != right["hash"]:
                self.state["disagreement_count"] += 1
                self.incident("persistent chain disagreement", self.state["disagreement_count"] >= 3, now)
                raise Unavailable("chain disagreement")
            if left != right:
                left2, right2 = self.pair(height)
                if left2 == left and right2 == right:
                    self.incident("confirmed tree state mismatch", True, now, latched=True)
                raise Unavailable("tree state mismatch or racing read")
            # An audit record precedes its durable cursor: a crash can replay, never skip.
            self.audit({"event": "matched", "time": now, "height": height, "record": left})
            self.state["cursor"] = height
            self.state["history"][str(height)] = left["hash"]
            self.state["history"] = {h: v for h, v in self.state["history"].items()
                                      if int(h) >= height - 1000}
            self.save()
        self.state["disagreement_count"] = 0
        self.incident("persistent chain disagreement", False, now)

    def step(self, now=None):
        now = time.time() if now is None else now
        status, reference, error = None, None, None
        try:
            reference = self.linux.tip()
            status = self.remote.status()
            self.validate_status(status, now)
            self.state["unavailable_since"] = None
            self.incident("verifier unavailable", False, now)
            if self.state["initial_tip"] is None:
                self.state["initial_tip"] = reference["height"]
            mac_height = status["tip"]["height"]
            watch = self.state["tip_watch"]
            if watch is None or watch["height"] != mac_height:
                watch = {"height": mac_height, "since": now, "reference": reference["height"]}
                self.state["tip_watch"] = watch
            self.incident("verifier stalled", now - watch["since"] >= 600
                          and reference["height"] >= watch["reference"] + 3, now)
            target = min(reference["height"], mac_height) - 3
            if target < self.state["cursor"]:
                self.reorg(now)
                raise Unavailable("tips behind comparison cursor")
            self.compare(target, now)
            caught_up = self.state["cursor"] >= reference["height"] - 3 and mac_height >= reference["height"] - 3
            self.state["caught_up"] = caught_up
            self.incident("catch-up exceeds two hours", not caught_up
                          and now - self.expected["deployed_at"] > 7200, now)
            self.incident("coverage incomplete", False, now)
        except (Unavailable, OSError, ValueError, KeyError, TypeError) as exc:
            error = type(exc).__name__ + ": " + str(exc)
            self.incident("coverage incomplete", True, now)
            since = self.state["unavailable_since"]
            if since is None:
                since = now
                self.state["unavailable_since"] = since
            self.incident("verifier unavailable", now - since >= 180, now)
        gap = self.state["last_sample"] is None or now - self.state["last_sample"] > 90
        healthy = (error is None and not self.state["incidents"] and self.state["caught_up"]
                   and not self.state["outbox_overflow"] and not self.state["outbox"])
        if not healthy or gap:
            self.state["healthy_since"] = now if healthy else None
        elif self.state["healthy_since"] is None:
            self.state["healthy_since"] = now
        if (healthy and not self.state["qualified"]
                and now - self.state["healthy_since"] >= 86400
                and self.state["cursor"] >= self.state["initial_tip"] + 100):
            self.state["qualified"] = True
            self.notify("Zakura Mac verifier qualified: 24 healthy hours and 100 new mainnet blocks")
        self.state["last_sample"] = now
        # Persist notification IDs before external delivery, and retain on failure.
        self.save()
        if self.slack and self.state["outbox"]:
            if self.slack.send(self.state["outbox"][0], now):
                self.state["outbox"].pop(0)
                self.save()
        atomic_json(self.directory / "status.json", {
            "schema_version": 1, "sample_time": now, "reference": reference,
            "verifier": status, "coverage_start": self.state["bootstrap"] + 1,
            "compared_through": self.state["cursor"], "caught_up": self.state["caught_up"],
            "incidents": self.state["incidents"], "error": error,
            "healthy_since": self.state["healthy_since"], "qualified": self.state["qualified"],
            "pending_alerts": len(self.state["outbox"]),
            "alert_overflow": self.state["outbox_overflow"],
        })


@contextlib.contextmanager
def exclusive(directory):
    Path(directory).mkdir(parents=True, exist_ok=True)
    with (Path(directory) / "monitor.lock").open("a") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["run", "once", "status", "ack"])
    parser.add_argument("--directory", default="/var/lib/zakura-mac-verifier")
    parser.add_argument("--receipt", default="/etc/zakura-mac-verifier/receipt.json")
    parser.add_argument("--incident")
    args = parser.parse_args()
    if args.command == "status":
        print(json.dumps(read_json(Path(args.directory) / "status.json"), indent=2))
        return
    with exclusive(args.directory):
        slack = None
        if args.command == "run":
            slack = Slack(os.environ["MAC_VERIFIER_SLACK_BOT_TOKEN"], "U0A81KAPYMR", "T0A80TZAXK5")
        monitor = Monitor(args.directory, read_json(args.receipt), slack=slack)
        if args.command == "ack":
            monitor.ack(args.incident)
        elif args.command == "once":
            monitor.step()
        else:
            while True:
                started = time.monotonic()
                monitor.step()
                time.sleep(max(0, 30 - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
