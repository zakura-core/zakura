#!/usr/bin/env python3
"""Rehearse an old-binary database upgrade and compare with fresh block replay.

Requires explicit local binaries. Logs, configs, and replayable block hex remain
in --artifacts on success or failure. Every RPC and process wait is bounded.
"""

import argparse
import json
import pathlib
import re
import signal
import subprocess
import time
import urllib.error
import urllib.request


NU7 = 1104
RESTART_HEIGHT = NU7 + 2
RPC_ENDPOINT = re.compile(rb"Opened RPC endpoint at 127\.0\.0\.1:(\d+)")


def assigned_rpc_port(log_path, offset):
    with log_path.open("rb") as log:
        log.seek(offset)
        matches = RPC_ENDPOINT.findall(log.read())
    return int(matches[-1]) if matches else None


class Node:
    def __init__(self, binary, directory, activate):
        self.binary = str(pathlib.Path(binary).resolve(strict=True))
        self.directory = directory
        directory.mkdir(parents=True, exist_ok=True)
        self.rpc_port = None
        self.process = None
        self.log = None
        self.activate = activate
        self.write_config()

    def write_config(self):
        upgrades = "NU5 = 2"
        disbursement = ""
        if self.activate:
            upgrades += f", NU7 = {NU7}"
            disbursement = ', lockbox_disbursements = [{ address = "t2RnBRiqrN1nW4ecZs1Fj3WWjNdnSs4kiX8", amount = 0 }]'

        text = f'''[network]
network = {{ params = {{ activation_heights = {{ {upgrades} }}{disbursement} }} }}
listen_addr = "127.0.0.1:0"
p2p_stack = "legacy"
initial_testnet_peers = []
cache_dir = false
[state]
cache_dir = {json.dumps(str(self.directory / 'state'))}
ephemeral = false
[rpc]
listen_addr = "127.0.0.1:0"
enable_cookie_auth = false
[mining]
miner_address = "tmJymvcUCn1ctbghvTJpXBwHiMEB8P6wxNV"
[tracing]
filter = "info"
'''
        (self.directory / "zakura.toml").write_text(text)

    def start(self):
        log_path = self.directory / "node.log"
        rpc_log_offset = log_path.stat().st_size if log_path.exists() else 0
        self.rpc_port = None
        self.log = log_path.open("ab")
        self.process = subprocess.Popen(
            [self.binary, "-c", str(self.directory / "zakura.toml"), "start"],
            stdout=self.log, stderr=subprocess.STDOUT,
        )
        self.wait(
            lambda: self.set_rpc_port(log_path, rpc_log_offset),
            "OS-assigned RPC listener",
        )
        self.wait(lambda: self.call("getblockcount") is not None, "RPC startup")

    def set_rpc_port(self, log_path, offset):
        self.rpc_port = assigned_rpc_port(log_path, offset)
        return self.rpc_port is not None

    def call(self, method, params=None):
        data = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []}).encode()
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.rpc_port}", data,
            {"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=60 if method == "generate" else 15) as response:
            result = json.load(response)
        if result.get("error"):
            raise RuntimeError(f"{method}: {result['error']}")
        return result["result"]

    def wait(self, predicate, operation):
        deadline = time.monotonic() + 60
        last = None
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"node exited during {operation}; see {self.directory / 'node.log'}")
            try:
                if predicate():
                    return
            except (urllib.error.URLError, TimeoutError, RuntimeError) as error:
                last = error
            time.sleep(0.1)
        raise TimeoutError(f"{operation}: {last}; see {self.directory / 'node.log'}")

    def wait_backup(self):
        block_hash = self.call("getbestblockhash")
        raw = self.call("getblock", [block_hash, 0])
        backup = self.directory / "state" / "non_finalized_state" / "regtest" / block_hash
        self.wait(lambda: backup.exists() and backup.stat().st_size >= len(raw) // 2,
                  "complete non-finalized backup")

    def stop(self, abrupt=False):
        if self.process is not None and self.process.poll() is None:
            self.process.send_signal(signal.SIGKILL if abrupt else signal.SIGINT)
            try:
                self.process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
                raise
        if self.log:
            self.log.close()


def compare(left, right, height):
    left.wait(lambda: left.call("getblockcount") == height, f"upgraded tip {height}")
    right.wait(lambda: right.call("getblockcount") == height, f"replay tip {height}")
    assert left.call("getbestblockhash") == right.call("getbestblockhash"), "tip mismatch"
    for field in ("valuePools", "chainSupply"):
        a, b = left.call("getblockchaininfo"), right.call("getblockchaininfo")
        assert field in a and field in b, f"missing accounting response {field}"
        assert a[field] == b[field], f"{field} mismatch at {height}"
    assert left.call("getblocksubsidy", [height + 1]) == right.call("getblocksubsidy", [height + 1]), "next-parent reward mismatch"


def run(args):
    artifacts = pathlib.Path(args.artifacts).resolve()
    artifacts.mkdir(parents=True, exist_ok=True)
    if any(artifacts.iterdir()):
        raise RuntimeError("artifact directory must be empty; never reuse an old chain accidentally")
    old = Node(args.old_binary, artifacts / "upgraded", False)
    fresh = Node(args.new_binary, artifacts / "fresh", True)
    try:
        old.start()
        old.call("generate", [NU7 - 1])
        old.wait(lambda: old.call("getblockcount") == NU7 - 1, "old-binary funding history")
        blocks = [old.call("getblock", [old.call("getblockhash", [height]), 0]) for height in range(1, NU7)]
        (artifacts / "preactivation-blocks.json").write_text(json.dumps(blocks))
        old.wait_backup()
        old.stop()
        old.binary = str(pathlib.Path(args.new_binary).resolve(strict=True))
        old.activate = True
        old.write_config()
        old.start()
        fresh.start()
        for block in blocks:
            assert fresh.call("submitblock", [block]) is None, "fresh replay rejected old-binary block"
        compare(old, fresh, NU7 - 1)
        for height in range(NU7, NU7 + 7):
            hashes = old.call("generate", [1])
            block = old.call("getblock", [hashes[0], 0])
            (artifacts / f"block-{height}.hex").write_text(block)
            assert fresh.call("submitblock", [block]) is None, f"replay rejected NU7 block {height}"
            compare(old, fresh, height)
            # Restart after activation. SIGINT exercises persisted state;
            # SIGKILL checks recovery with the latest non-finalized block replayed.
            if height in (RESTART_HEIGHT - 1, RESTART_HEIGHT):
                if height == RESTART_HEIGHT - 1:
                    old.wait_backup()
                old.stop(abrupt=height == RESTART_HEIGHT)
                old.start()
                tip = old.call("getblockcount")
                if tip < height:
                    for missing in range(tip + 1, height + 1):
                        raw = fresh.call("getblock", [fresh.call("getblockhash", [missing]), 0])
                        assert old.call("submitblock", [raw]) is None
                compare(old, fresh, height)
    finally:
        old.stop()
        fresh.stop()
    print("Old-binary upgrade, fresh replay, activation, and crash recovery agree")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--old-binary", required=True)
    parser.add_argument("--new-binary", required=True)
    parser.add_argument("--artifacts", required=True)
    run(parser.parse_args())
