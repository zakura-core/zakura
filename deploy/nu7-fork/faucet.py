#!/usr/bin/env python3
"""Rate-limited NU7 faucet for Ironwood-capable Unified Addresses."""

from __future__ import annotations

import argparse
import ipaddress
import json
import os
import re
import secrets
import sqlite3
import subprocess
import threading
import time
import urllib.request
from contextlib import contextmanager
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


PAYOUT_ZAT = 10_000_000
FEE_ZAT = 100_000
DAILY_CAP_ZAT = 1_000_000_000
ADDRESS_COOLDOWN_SECONDS = 86_400
IP_CLAIMS_PER_DAY = 2
MAX_QUEUE = 10
MIN_CLAIM_SPACING_SECONDS = 30
MAX_ATTEMPTS_PER_IP = 10
ATTEMPT_WINDOW_SECONDS = 600
ALLOWED_ORIGIN = "https://nu7.valargroup.dev"
CLAIM_ID = re.compile(r"[A-Za-z0-9_-]{20,40}\Z")
TXID = re.compile(r"FAUCET_TXID=([0-9a-f]{64})\b")


def rpc(port: int, method: str, params: list | None = None):
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}/",
        json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []}).encode(),
        {"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=8) as response:
        payload = json.load(response)
    if payload.get("error") is not None:
        raise ValueError(f"{method} rejected the request")
    return payload["result"]


def client_ip(text: str) -> str:
    address = ipaddress.ip_address(text)
    if isinstance(address, ipaddress.IPv6Address):
        return str(ipaddress.ip_network((address, 64), strict=False).network_address) + "/64"
    return str(address)


class Faucet:
    def __init__(self, db: Path, rpc_port: int, miner_address: str, sender: Path, config: Path):
        self.db = db
        self.rpc_port = rpc_port
        self.miner_address = miner_address
        self.sender = sender
        self.config = config
        self.lock = threading.Lock()
        self.funds_cache: tuple[float, dict] | None = None
        self.attempts: dict[str, list[float]] = {}
        self.last_attempt_cleanup = 0.0
        db.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS claims (
                    id TEXT PRIMARY KEY,
                    address TEXT NOT NULL,
                    ip TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    txid TEXT
                )
            """)
            connection.execute("CREATE INDEX IF NOT EXISTS claims_address_time ON claims(address, created_at)")
            connection.execute("CREATE INDEX IF NOT EXISTS claims_ip_time ON claims(ip, created_at)")
            # A restarted worker must never retry an in-flight send automatically.
            connection.execute("UPDATE claims SET status = 'review' WHERE status = 'processing'")

    @contextmanager
    def connect(self):
        connection = sqlite3.connect(self.db, timeout=10)
        connection.row_factory = sqlite3.Row
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def funding_status(self) -> dict:
        with self.lock:
            if self.funds_cache and time.time() - self.funds_cache[0] < 20:
                return self.funds_cache[1]
        tip = rpc(self.rpc_port, "getblockcount")
        utxos = rpc(self.rpc_port, "getaddressutxos", [{"addresses": [self.miner_address]}])
        # The sender leaves two confirmations beyond the 100-block maturity rule.
        matured = sum(item["height"] <= tip - 102 and item["satoshis"] >= PAYOUT_ZAT + FEE_ZAT
                      for item in utxos)
        result = {"tipHeight": tip, "maturedOutputs": matured, "ready": matured > 0}
        with self.lock:
            self.funds_cache = (time.time(), result)
        return result

    def status(self) -> dict:
        now = int(time.time())
        day_start = int(datetime.now(timezone.utc).replace(hour=0, minute=0, second=0,
                                                           microsecond=0).timestamp())
        with self.connect() as connection:
            used = connection.execute("SELECT COUNT(*) FROM claims WHERE created_at >= ?",
                                      (day_start,)).fetchone()[0]
            queued = connection.execute("SELECT COUNT(*) FROM claims WHERE status IN ('queued', 'processing')").fetchone()[0]
        try:
            funds = self.funding_status()
        except (OSError, ValueError, KeyError):
            funds = {"tipHeight": None, "maturedOutputs": 0, "ready": False}
        remaining = max(0, DAILY_CAP_ZAT // PAYOUT_ZAT - used)
        return {
            "ready": funds["ready"] and remaining > 0 and queued < MAX_QUEUE,
            "amountZat": PAYOUT_ZAT,
            "addressCooldownSeconds": ADDRESS_COOLDOWN_SECONDS,
            "dailyCapZat": DAILY_CAP_ZAT,
            "remainingClaimsToday": remaining,
            "queuedClaims": queued,
            "maturedOutputs": funds["maturedOutputs"],
            "tipHeight": funds["tipHeight"],
            "observedAt": now,
        }

    def validate_address(self, address: str) -> None:
        if not (address.startswith("utest1") and 60 <= len(address) <= 400):
            raise ValueError("Enter a NU7 Testnet Unified Address")
        receivers = rpc(self.rpc_port, "z_listunifiedreceivers", [address])
        if not receivers.get("orchard"):
            raise ValueError("The Unified Address needs an Orchard receiver for Ironwood")

    def throttle_attempt(self, ip: str) -> None:
        now = time.monotonic()
        with self.lock:
            if now - self.last_attempt_cleanup >= 60:
                for key in list(self.attempts):
                    recent = [when for when in self.attempts[key]
                              if now - when < ATTEMPT_WINDOW_SECONDS]
                    if recent:
                        self.attempts[key] = recent
                    else:
                        del self.attempts[key]
                self.last_attempt_cleanup = now
            times = self.attempts.setdefault(ip, [])
            times[:] = [when for when in times if now - when < ATTEMPT_WINDOW_SECONDS]
            if len(times) >= MAX_ATTEMPTS_PER_IP:
                raise PermissionError("Too many faucet requests from this connection; try later")
            times.append(now)

    def reserve(self, address: str, ip: str) -> str:
        self.throttle_attempt(ip)
        self.validate_address(address)
        if not self.funding_status()["ready"]:
            raise RuntimeError("Faucet is waiting for mature miner rewards")
        now = int(time.time())
        day_start = int(datetime.now(timezone.utc).replace(hour=0, minute=0, second=0,
                                                           microsecond=0).timestamp())
        claim_id = secrets.token_urlsafe(18)
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if connection.execute("SELECT 1 FROM claims WHERE address = ? AND created_at > ?",
                                  (address, now - ADDRESS_COOLDOWN_SECONDS)).fetchone():
                raise PermissionError("This address has claimed in the last 24 hours")
            if connection.execute("SELECT COUNT(*) FROM claims WHERE ip = ? AND created_at > ?",
                                  (ip, now - ADDRESS_COOLDOWN_SECONDS)).fetchone()[0] >= IP_CLAIMS_PER_DAY:
                raise PermissionError("This connection has reached its 24-hour claim limit")
            if connection.execute("SELECT COUNT(*) FROM claims WHERE created_at >= ?",
                                  (day_start,)).fetchone()[0] >= DAILY_CAP_ZAT // PAYOUT_ZAT:
                raise PermissionError("Today's faucet allocation is exhausted")
            if connection.execute("SELECT COUNT(*) FROM claims WHERE status IN ('queued', 'processing')").fetchone()[0] >= MAX_QUEUE:
                raise PermissionError("The payout queue is full; try again later")
            last = connection.execute("SELECT MAX(created_at) FROM claims").fetchone()[0]
            if last is not None and now - last < MIN_CLAIM_SPACING_SECONDS:
                raise PermissionError("The faucet is spacing out payouts; try again shortly")
            connection.execute("INSERT INTO claims VALUES (?, ?, ?, ?, 'queued', NULL)",
                               (claim_id, address, ip, now))
        return claim_id

    def claim(self, claim_id: str) -> dict | None:
        with self.connect() as connection:
            row = connection.execute("SELECT status, txid, created_at FROM claims WHERE id = ?",
                                     (claim_id,)).fetchone()
        return dict(row) if row else None

    def run_worker(self):
        while True:
            try:
                self.process_next()
            except Exception:
                # The worker stays alive; details stay in the operator journal.
                import traceback
                traceback.print_exc()
            time.sleep(2)

    def process_next(self):
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT id, address FROM claims WHERE status = 'queued' ORDER BY created_at LIMIT 1").fetchone()
            if row:
                connection.execute("UPDATE claims SET status = 'processing' WHERE id = ?", (row["id"],))
        if not row:
            return
        credential_dir = os.environ.get("CREDENTIALS_DIRECTORY")
        if not credential_dir:
            raise RuntimeError("systemd did not provide the miner key credential")
        command = [
            str(self.sender), "--rpc", f"127.0.0.1:{self.rpc_port}",
            "--config", str(self.config), "--address", self.miner_address,
            "--secret-key-file", str(Path(credential_dir) / "miner-key.hex"),
            "--recipient", row["address"], "--amount-zat", str(PAYOUT_ZAT),
            "--fee", str(FEE_ZAT), "--count", "1",
        ]
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=180,
                                    check=False)
            match = TXID.search(result.stdout)
            status = "sent" if result.returncode == 0 and match else "review"
            txid = match.group(1) if match else None
            if status == "review":
                diagnostic = [line.strip() for line in re.sub(r"\x1b\[[0-9;]*m", "", result.stderr).splitlines()
                              if re.match(r"\s*\d+: ", line)]
                print(f"claim {row['id']} sender exit={result.returncode}: "
                      f"{'; '.join(diagnostic[:2])[:400]}", flush=True)
        except (OSError, subprocess.TimeoutExpired):
            status, txid = "review", None
        with self.connect() as connection:
            connection.execute("UPDATE claims SET status = ?, txid = ? WHERE id = ?",
                               (status, txid, row["id"]))
        print(f"claim {row['id']} {status} txid={txid or 'none'}", flush=True)


class Handler(BaseHTTPRequestHandler):
    faucet: Faucet

    def respond(self, status: int, payload: dict):
        body = json.dumps(payload, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        if self.headers.get("Origin") == ALLOWED_ORIGIN:
            self.send_header("Access-Control-Allow-Origin", ALLOWED_ORIGIN)
            self.send_header("Vary", "Origin")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        if self.path != "/v1/faucet/claim" or self.headers.get("Origin") != ALLOWED_ORIGIN:
            self.respond(403, {"error": "Origin not allowed"})
            return
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", ALLOWED_ORIGIN)
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Max-Age", "600")
        self.send_header("Vary", "Origin")
        self.end_headers()

    def do_GET(self):
        if self.path == "/v1/faucet/status":
            self.respond(200, self.faucet.status())
        elif self.path.startswith("/v1/faucet/claim/"):
            claim_id = self.path.removeprefix("/v1/faucet/claim/")
            claim = self.faucet.claim(claim_id) if CLAIM_ID.fullmatch(claim_id) else None
            self.respond(200 if claim else 404, claim or {"error": "Claim not found"})
        else:
            self.respond(404, {"error": "Not found"})

    def do_POST(self):
        if self.path != "/v1/faucet/claim":
            self.respond(404, {"error": "Not found"})
            return
        if self.headers.get("Origin") not in (None, ALLOWED_ORIGIN):
            self.respond(403, {"error": "Origin not allowed"})
            return
        try:
            size = int(self.headers.get("Content-Length", "0"))
            if not 1 <= size <= 1024 or self.headers.get("Content-Type", "").split(";")[0] != "application/json":
                raise ValueError("Send a JSON claim under 1 KB")
            payload = json.loads(self.rfile.read(size))
            address = payload["address"]
            if not isinstance(address, str):
                raise ValueError("Address must be text")
            ip = client_ip(self.headers.get("X-Real-IP") or self.client_address[0])
            claim_id = self.faucet.reserve(address.strip(), ip)
        except (ValueError, KeyError, TypeError) as error:
            self.respond(400, {"error": str(error)})
            return
        except PermissionError as error:
            self.respond(429, {"error": str(error)})
            return
        except (RuntimeError, OSError):
            self.respond(503, {"error": "Faucet is temporarily unavailable"})
            return
        self.respond(202, {"claimId": claim_id, "status": "queued", "amountZat": PAYOUT_ZAT})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--miner-address", required=True)
    parser.add_argument("--sender", type=Path, required=True)
    parser.add_argument("--config", type=Path, default=Path("/etc/zakura/zakura.toml"))
    parser.add_argument("--rpc-port", type=int, default=18232)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8094)
    args = parser.parse_args()
    faucet = Faucet(args.db, args.rpc_port, args.miner_address, args.sender, args.config)
    Handler.faucet = faucet
    threading.Thread(target=faucet.run_worker, daemon=True).start()
    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
