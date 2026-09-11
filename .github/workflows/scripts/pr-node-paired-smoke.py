#!/usr/bin/env python3
"""Run an isolated PR 966 downloader against the disposable mainnet seed."""

import argparse
import json
import re
import signal
import subprocess
import time
import urllib.request
from pathlib import Path

EXPECTED_SHA = "e3328340aa040971681ed222a5bfc63fe43d8991"
COHORT = "pr966-retention-sync-20260911"
OUT = Path("/root/out/paired")
SEED_RPC = "http://127.0.0.1:8232"
CLIENT_RPC = "http://127.0.0.1:18232"
BINARY = "/usr/local/bin/zakurad-downloader"
SEED_SHA = "e3328340aa040971681ed222a5bfc63fe43d8991"


def rpc(url, method, params=None):
    request = urllib.request.Request(
        url,
        data=json.dumps({"jsonrpc": "2.0", "id": "paired-smoke", "method": method,
                         "params": params or []}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        result = json.load(response)
    if result.get("error"):
        raise RuntimeError(str(result["error"]))
    return result["result"]


def metrics(port):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=10) as r:
        return r.read().decode()


def metric(text, name):
    values = re.findall(
        rf"^{re.escape(name)}(?:_total)?(?:\{{[^}}]*\}})?\s+([-+0-9.eE]+)$",
        text, re.MULTILINE,
    )
    return sum(float(value) for value in values)


def emit(kind, **fields):
    row = {"event": kind, "unix_time": time.time(), **fields}
    print(json.dumps(row), flush=True)
    with (OUT / "events.jsonl").open("a") as f:
        f.write(json.dumps(row) + "\n")


def seed_identity(deadline):
    while time.monotonic() < deadline:
        try:
            text = Path("/var/log/zakura/zakura.log").read_text(errors="replace")
            for line in reversed(text.splitlines()):
                if "Zakura P2P endpoint ready" not in line:
                    continue
                match = re.search(r'node_id\s*=\s*"?([a-zA-Z0-9]{40,80})', line)
                if match:
                    rpc(SEED_RPC, "getblockcount")
                    return match[1]
        except (OSError, RuntimeError):
            pass
        time.sleep(5)
    raise RuntimeError("seed endpoint did not become ready")


def child_config(state, seed_id):
    return f'''[network]
network = "Mainnet"
p2p_stack = "zakura"
listen_addr = "127.0.0.1:18233"
identity_dir = "/root/paired-client-identity"
cache_dir = "/root/paired-client-network-cache"

[network.zakura]
listen_addr = "127.0.0.1:18234"
bootstrap_peers = ["{seed_id}@127.0.0.1:8234"]
dev_network = "{COHORT}"
trace_dir = "/root/out/paired/traces"

[state]
cache_dir = {json.dumps(str(state))}
storage_mode = "pruned"

[rpc]
listen_addr = "127.0.0.1:18232"
enable_cookie_auth = false

[metrics]
endpoint_addr = "127.0.0.1:19999"

[consensus]
checkpoint_sync = true
vct_fast_sync = true

[tracing]
log_file = "/root/out/paired/downloader.log"
use_color = false
'''


def stop(proc):
    if proc is None or proc.poll() is not None:
        return
    proc.send_signal(signal.SIGINT)
    try:
        proc.wait(timeout=90)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=15)
        raise RuntimeError("downloader did not stop within 90 seconds")
    if proc.returncode not in (0, -signal.SIGINT):
        raise RuntimeError(f"downloader shutdown returned {proc.returncode}")


def check_hash(height):
    seed_hash = rpc(SEED_RPC, "getblockhash", [height])
    client_hash = rpc(CLIENT_RPC, "getblockhash", [height])
    if seed_hash != client_hash:
        raise AssertionError(f"seed and downloader disagree at height {height}")
    return client_hash


def run_phase(proc, name, start, needed, deadline):
    last_print = 0
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"downloader exited unexpectedly: {proc.returncode}")
        sample = {"phase": name, "start_height": start}
        try:
            client = rpc(CLIENT_RPC, "getblockchaininfo")
            seed = rpc(SEED_RPC, "getblockchaininfo")
            sample.update(height=client["blocks"], seed_height=seed["blocks"],
                          pruneheight=client.get("pruneheight"),
                          seed_pruneheight=seed.get("pruneheight"))
            text = metrics(19999)
            sample["native_bodies"] = metric(text, "sync_block_body_received")
            sample["native_requests"] = metric(text, "sync_block_request_sent")
            sample["legacy_fallbacks"] = metric(text, "sync_zakura_legacy_fallback_engaged")
            if sample["legacy_fallbacks"]:
                raise AssertionError("native-only downloader used a legacy fallback")
            if (sample["height"] >= start + needed and sample["native_bodies"] >= needed
                    and sample["native_requests"] > 0):
                sample["block_hash"] = check_hash(sample["height"])
                (OUT / f"{name}-metrics.txt").write_text(text)
                emit("phase_pass", **sample)
                return sample
        except AssertionError:
            raise
        except (OSError, ValueError, RuntimeError) as exc:
            sample["sample_error"] = str(exc)
        now = time.monotonic()
        if now - last_print >= 30:
            emit("sample", **sample)
            last_print = now
        time.sleep(1)
    raise RuntimeError(f"{name} did not verify {needed} new native blocks before its deadline")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument("--duration-minutes", required=True, type=float)
    args = parser.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    result = {"sha": EXPECTED_SHA, "seed_sha": SEED_SHA, "network": "mainnet", "cohort": COHORT,
              "topology": "two full nodes on one disposable host, native QUIC over loopback",
              "seed_upstream": "public legacy peers", "phases": [], "pass": False}
    proc = None
    deadline = time.monotonic() + args.duration_minutes * 60
    stderr = (OUT / "downloader-console.log").open("a")
    try:
        preparation = json.loads((OUT.parent / "seed-priming/summary.json").read_text())
        if preparation["verdict"] != "ok":
            raise RuntimeError("seed priming did not pass")
        result.update(seed_preparation=preparation, seed_vct_fast_sync=False,
                      downloader_vct_fast_sync=True)
        actual = subprocess.check_output(["git", "-C", "/root/zakura", "rev-parse", "HEAD"], text=True).strip()
        if actual != EXPECTED_SHA:
            raise RuntimeError(f"unexpected tested source revision: {actual}")
        seed_id = seed_identity(min(deadline, time.monotonic() + 360))
        config = OUT / "downloader.toml"
        config.write_text(child_config(args.state, seed_id))
        initial = subprocess.check_output(
            [BINARY, "-c", str(config), "tip-height", "--cache-dir", str(args.state),
             "--network", "Mainnet"], text=True, stderr=subprocess.STDOUT, timeout=180,
        )
        heights = re.findall(r"^([0-9]+)$", initial, re.MULTILINE)
        if not heights:
            raise RuntimeError("cannot determine downloader's retained snapshot height")
        start = int(heights[-1])
        checkpoint = int(Path('/root/zakura/crates/zakura-chain/src/parameters/checkpoint/main-checkpoints.txt').read_text().splitlines()[-1].split()[0])
        if start >= checkpoint:
            raise RuntimeError("fixture must start below the checkpoint handoff")
        result.update(seed_id=seed_id, start_height=start, max_checkpoint=checkpoint,
                      seed_before=rpc(SEED_RPC, "getblockchaininfo"))
        emit("starting", sha=actual, seed_id=seed_id, start_height=start)

        def launch():
            return subprocess.Popen([BINARY, "-c", str(config), "start"], stdout=stderr, stderr=stderr)

        proc = launch()
        first = run_phase(proc, "initial-sync", start, checkpoint + 64 - start,
                          min(deadline - 300, time.monotonic() + 900))
        result["phases"].append(first)
        if first["pruneheight"] is None or first["pruneheight"] <= 0:
            raise RuntimeError("downloader must remain pruned")
        stop(proc)
        # Read persisted height after shutdown so startup work cannot fake restart progress.
        stopped_tip = subprocess.check_output(
            [BINARY, "-c", str(config), "tip-height", "--cache-dir", str(args.state),
             "--network", "Mainnet"], text=True, stderr=subprocess.STDOUT, timeout=120,
        )
        restart_start = int(re.findall(r"^([0-9]+)$", stopped_tip, re.MULTILINE)[-1])
        emit("restarting", persisted_height=restart_start)
        proc = launch()
        second = run_phase(proc, "restart-sync", restart_start, 1, deadline)
        result["phases"].append(second)
        # Observe a live native connection after catch-up and restart.
        stable_until = min(deadline, time.monotonic() + 120)
        while time.monotonic() < stable_until:
            if proc.poll() is not None:
                raise RuntimeError("downloader exited during stability observation")
            info = rpc(CLIENT_RPC, "getblockchaininfo")
            seed_info = rpc(SEED_RPC, "getblockchaininfo")
            peers = rpc(CLIENT_RPC, "getpeerinfo")
            if not peers:
                raise RuntimeError("downloader lost its native peer")
            check_hash(info["blocks"])
            emit("stability", height=info["blocks"], seed_height=seed_info["blocks"],
                 pruneheight=info.get("pruneheight"), peers=len(peers))
            time.sleep(15)
        result["final_client"] = rpc(CLIENT_RPC, "getblockchaininfo")
        result["final_seed"] = rpc(SEED_RPC, "getblockchaininfo")
        result["final_hash"] = check_hash(result["final_client"]["blocks"])
        (OUT / "final-client-metrics.txt").write_text(metrics(19999))
        (OUT / "final-seed-metrics.txt").write_text(metrics(9999))
        stop(proc)
        proc = None
        stderr.flush()
        if "panicked at" in (OUT / "downloader-console.log").read_text(errors="replace"):
            raise RuntimeError("downloader panic found in the console log")
        statuses = []
        for path in (OUT / "traces").rglob("block_sync*.jsonl"):
            for line in path.open():
                row = json.loads(line)
                if row.get("event") == "block_status_sent":
                    statuses.append(row)
        result["advertised_status_samples"] = len(statuses)
        result["advertised_floors"] = sorted({row["range_start"] for row in statuses})
        if not statuses or any(row["range_start"] <= 0 for row in statuses):
            raise RuntimeError("pruned downloader did not advertise its retained range")
        if len(result["advertised_floors"]) < 2:
            raise RuntimeError("retained range did not advance while syncing")
        result["pass"] = True
    except Exception as exc:
        result["error"] = str(exc)
        emit("failed", error=str(exc))
    finally:
        try:
            stop(proc)
        except Exception as exc:
            result["cleanup_error"] = str(exc)
            result["pass"] = False
        stderr.close()
        (OUT / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
        emit("complete", passed=result["pass"])
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
