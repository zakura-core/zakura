"""Read-only, one-session macOS probe executed by a restricted SSH key.

CI installs this after common.py's source in one root-owned standalone script.
There is no listening socket or launchd job.
"""
import json
from pathlib import Path
import re
import time
from common import RPC, Transport, Unavailable, MAX_JSON, digest, integer, read_json, hex_bytes

import platform
import shutil
import signal
import subprocess
import sys

BASE = Path("/Library/Application Support/ZakuraVerifier")

def resources(base):
    result = {"free_disk_bytes": shutil.disk_usage(base).free,
              "node_rss_bytes": None, "memory_free_percent": None}
    try:
        pid = int((Path(base) / "run/node.pid").read_text())
        result["node_rss_bytes"] = int(subprocess.check_output(
            ["ps", "-o", "rss=", "-p", str(pid)], timeout=3).strip()) * 1024
        if platform.system() == "Darwin":
            output = subprocess.check_output(["/usr/bin/memory_pressure", "-Q"], timeout=3).decode()
            match = re.search(r"System-wide memory free percentage: (\d+)%", output)
            if match:
                result["memory_free_percent"] = int(match.group(1))
    except (OSError, ValueError, subprocess.SubprocessError):
        pass
    return result


def fork_anchor(rpc, tip):
    """Sample eleven-block fork evidence without accepting a racing tip read."""
    if tip["height"] < 10:
        return None
    height = tip["height"] - 10
    block_hash = hex_bytes(rpc.call("getblockhash", height), 32)
    if hex_bytes(rpc.call("getblockhash", tip["height"]), 32) != tip["hash"]:
        raise Unavailable("chain changed during fork sample")
    return {"height": height, "hash": block_hash}



def sample(base, rpc):
    receipt = read_json(base / "receipt.json")
    tip = rpc.tip()
    active = subprocess.run(
        ["launchctl", "print", "system/dev.valargroup.zakura-verifier-node"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=2).returncode == 0
    return {"schema_version": 1, "sample_time": time.time(), "receipt": receipt,
            "tip": tip, "fork_anchor": fork_anchor(rpc, tip), "node_active": active,
            "resources": resources(base), "binary_sha256": digest(base / "bin/zakurad"),
            "config_sha256": digest(base / "zakurad.toml"), "architecture": platform.machine()}


def reply(request, base, rpc):
    if not isinstance(request, dict):
        raise Unavailable("invalid request")
    if request == {"operation": "status"}:
        return sample(base, rpc)
    if set(request) == {"operation", "height"} and request["operation"] == "block":
        height = integer(request["height"])
        if not read_json(base / "receipt.json")["bootstrap_height"] <= height <= rpc.tip()["height"]:
            raise Unavailable("height outside coverage")
        return rpc.block(height)
    raise Unavailable("unsupported operation")


def main():
    # Bound the remote process even if its SSH client disappears or sends no data.
    signal.alarm(15)
    rpc = RPC("http://127.0.0.1:28232", Transport(timeout=2))
    for _ in range(128):
        line = sys.stdin.buffer.readline(MAX_JSON + 1)
        if not line:
            return
        if len(line) > MAX_JSON or not line.endswith(b"\n"):
            return
        try:
            value = reply(json.loads(line), BASE, rpc)
            output = json.dumps(value)
            if len(output.encode()) > MAX_JSON:
                raise Unavailable("response too large")
        except (Unavailable, OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError):
            output = '{"error":"sample unavailable"}'
        print(output, flush=True)


if __name__ == "__main__":
    main()
