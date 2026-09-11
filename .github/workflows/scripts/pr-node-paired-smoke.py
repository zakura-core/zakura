#!/usr/bin/env python3
"""Exercise stable header serving with two full nodes and live mainnet data."""

import argparse
import json
import re
import signal
import struct
import subprocess
import time
import urllib.request
from pathlib import Path

EXPECTED_SHA = "0e6398c22f0705b9ccf7a071a5229e743dd1572b"
OUT = Path("/root/out/paired")
SEED_RPC = "http://127.0.0.1:8232"
CLIENT_RPC = "http://127.0.0.1:18232"
BINARY = "/usr/local/bin/zakurad"


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
max_connections = 1
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
filter = "info,zakura_consensus::block=trace,zakura_consensus::transaction=trace,zakura_state::service=debug"
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


def running_height(proc, deadline):
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"downloader exited during startup: {proc.returncode}")
        try:
            return rpc(CLIENT_RPC, "getblockcount")
        except (OSError, ValueError, RuntimeError):
            time.sleep(1)
    raise RuntimeError("downloader RPC did not become ready")


def check_hash(height):
    seed_hash = rpc(SEED_RPC, "getblockhash", [height])
    client_hash = rpc(CLIENT_RPC, "getblockhash", [height])
    if seed_hash != client_hash:
        raise AssertionError(f"seed and downloader disagree at height {height}")
    return client_hash


def run_phase(proc, name, start, needed, deadline, *, minimum_height=0, minimum_vct=0):
    last_print = 0
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"downloader exited unexpectedly: {proc.returncode}")
        sample = {"phase": name, "start_height": start}
        try:
            sample["height"] = rpc(CLIENT_RPC, "getblockcount")
            sample["seed_height"] = rpc(SEED_RPC, "getblockcount")
            text = metrics(19999)
            sample["native_bodies"] = metric(text, "sync_block_body_received")
            sample["native_requests"] = metric(text, "sync_block_request_sent")
            sample["vct_fast_blocks"] = metric(text, "state_vct_fast_block_count")
            sample["legacy_fallbacks"] = metric(text, "sync_zakura_legacy_fallback_engaged")
            if sample["legacy_fallbacks"]:
                raise AssertionError("native-only downloader used a legacy fallback")
            if (sample["height"] >= max(start + needed, minimum_height)
                    and sample["native_bodies"] >= needed and sample["native_requests"] > 0
                    and sample["vct_fast_blocks"] >= minimum_vct):
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
        time.sleep(10)
    raise RuntimeError(f"{name} did not meet its progress, handoff and VCT gates before its deadline: {sample}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument("--duration-minutes", required=True, type=float)
    args = parser.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    result = {"sha": EXPECTED_SHA, "network": "mainnet", "cohort": None,
              "topology": "two full nodes on one disposable host, native QUIC over loopback",
              "seed_upstream": "public mainnet peers, dual transport", "phases": [], "pass": False}
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
        first_seed = rpc(SEED_RPC, "getblockcount")
        emit("waiting_for_seed_progress", height=first_seed)
        while rpc(SEED_RPC, "getblockcount") < first_seed + 100:
            if time.monotonic() >= deadline - 180:
                raise RuntimeError("seed did not advance 100 blocks before the paired test")
            time.sleep(2)
        emit("seed_advancing", height=rpc(SEED_RPC, "getblockcount"))
        config = OUT / "downloader.toml"
        config.write_text(child_config(args.state, seed_id))
        result.update(seed_id=seed_id)
        emit("starting", sha=actual, seed_id=seed_id)

        def launch():
            return subprocess.Popen([BINARY, "-c", str(config), "start"], stdout=stderr, stderr=stderr)

        handoff_bytes = Path("/root/zakura/crates/zakura-state/src/service/finalized_state/vct/mainnet-frontier.bin").read_bytes()
        handoff = struct.unpack("<I", handoff_bytes[:4])[0]
        # The node can install its trusted VCT bootstrap during startup. Measure
        # new progress from its running tip and require actual VCT work in this process.
        result.update(vct_handoff=handoff, required_vct_fast_blocks=1)
        proc = launch()
        start = running_height(proc, deadline - 120)
        result.update(start_height=start)
        emit("running", start_height=start)
        first = run_phase(proc, "sync-while-serving", start, 512,
                          min(deadline - 120, time.monotonic() + 600),
                          minimum_height=handoff + 64, minimum_vct=1)
        result["phases"].append(first)
        stop(proc)
        proc = launch()
        restart_start = running_height(proc, deadline)
        emit("restarting", restored_running_height=restart_start)
        second = run_phase(proc, "restart-sync", restart_start, 64, deadline)
        result["phases"].append(second)
        trace = []
        for line in Path("/var/log/zakura/seed-traces/header_sync.jsonl").open():
            try: trace.append(json.loads(line))
            except ValueError: pass
        latest_process = trace[-1]["process_trace_id"]
        trace = [r for r in trace if r.get("process_trace_id") == latest_process]
        connected = next(r["ts"] for r in trace if r["event"] == "header_peer_connected")
        snapshots = [r for r in trace if r["event"] == "header_snapshot_observed" and r["ts"] >= connected]
        latest = None
        across_updates = []
        successes_during_updates = 0
        for row in trace:
            if row["event"] == "header_snapshot_observed": latest = row
            if row["event"] != "header_response_served" or latest is None: continue
            if snapshots and row["ts"] < snapshots[-1]["ts"]:
                successes_during_updates += 1
                if row["header_generation"] < latest["header_generation"]:
                    across_updates.append({"request_id": row["request_id"], "target_hash": row["target_hash"],
                        "request_generation": row["header_generation"], "current_generation": latest["header_generation"],
                        "header_count": row["header_count"], "ts": row["ts"]})
        client_trace = [json.loads(line) for line in (OUT / "traces/header_sync.jsonl").read_text().splitlines()]
        client_peers = {row["peer"] for row in client_trace if row["event"] == "header_peer_connected"}
        if len(client_peers) != 1:
            raise RuntimeError("downloader did not remain connected to its single seed")
        received = {(row["request_id"], row["target_hash"]) for row in client_trace if row["event"] == "header_response_received"}
        delivered_across_updates = [row for row in across_updates if (row["request_id"], row["target_hash"]) in received]
        result["serving_evidence"] = {"client_peer_count": len(client_peers),
            "spanning_responses_received_by_client": len(delivered_across_updates),
            "snapshot_updates": len(snapshots),
            "successful_responses_while_advancing": successes_during_updates,
            "successful_responses_across_generation_changes": across_updates,
            "busy_replies": sum(r["event"] == "header_outcome" and r.get("outcome") == "busy" for r in trace)}
        if len(snapshots) < 100 or not delivered_across_updates:
            raise RuntimeError("the capture did not prove serving across ongoing head updates")
        stop(proc)
        proc = None
        stderr.flush()
        if "panicked at" in (OUT / "downloader-console.log").read_text(errors="replace"):
            raise RuntimeError("downloader panic found in the console log")
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
