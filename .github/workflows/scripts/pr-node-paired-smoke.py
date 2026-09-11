#!/usr/bin/env python3
"""Run an isolated PR 966 downloader against the disposable mainnet seed."""

import argparse
import json
import os
import re
import signal
import subprocess
import time
import urllib.request
from pathlib import Path

EXPECTED_SHA = os.environ["SHA"]
COHORT = "pr966-retention-sync-20260911"
OUT = Path("/root/out/paired")
SEED_RPC = "http://127.0.0.1:8232"
CLIENT_RPC = "http://127.0.0.1:18232"
BINARY = "/usr/local/bin/zakurad-downloader"
SEED_SHA = EXPECTED_SHA


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
    result = {"sha": EXPECTED_SHA, "seed_sha": SEED_SHA, "network": "mainnet",
              "cohort": COHORT, "pass": False, "probe_sent": False}
    proc = None
    deadline = time.monotonic() + args.duration_minutes * 60
    console = (OUT / "downloader-console.log").open("w")
    checkpoint_since = None
    last_sample = 0
    probe_file = OUT / "trigger-state-tip-probe"
    try:
        preparation = json.loads((OUT.parent / "seed-priming/summary.json").read_text())
        if preparation["verdict"] != "ok":
            raise RuntimeError("seed priming did not pass")
        actual = subprocess.check_output(["git", "-C", "/root/zakura", "rev-parse", "HEAD"], text=True).strip()
        if actual != EXPECTED_SHA:
            raise RuntimeError(f"unexpected source {actual}")
        seed_id = seed_identity(min(deadline, time.monotonic() + 360))
        config = OUT / "downloader.toml"
        config.write_text(child_config(args.state, seed_id))
        initial = subprocess.check_output(
            [BINARY, "-c", str(config), "tip-height", "--cache-dir", str(args.state),
             "--network", "Mainnet"], text=True, stderr=subprocess.STDOUT, timeout=180)
        start = int(re.findall(r"^([0-9]+)$", initial, re.MULTILINE)[-1])
        checkpoint = int(Path('/root/zakura/crates/zakura-chain/src/parameters/checkpoint/main-checkpoints.txt').read_text().splitlines()[-1].split()[0])
        if start >= checkpoint:
            raise RuntimeError("fixture must start below checkpoint")
        result.update(start_height=start, checkpoint=checkpoint, seed_id=seed_id)
        emit("starting", **result)
        env = os.environ.copy()
        env["ZAKURA_HANDOFF_PROBE_FILE"] = str(probe_file)
        proc = subprocess.Popen([BINARY, "-c", str(config), "start"], env=env,
                                stdout=console, stderr=console)
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                raise RuntimeError(f"client exited {proc.returncode}")
            try:
                info = rpc(CLIENT_RPC, "getblockchaininfo")
                now = time.monotonic()
                height = info["blocks"]
                if now - last_sample >= 15:
                    text = metrics(19999)
                    native = metric(text, "sync_block_body_received")
                    (OUT / f"metrics-{int(time.time())}.txt").write_text(text)
                    emit("sample", height=height, native_bodies=native,
                         probe_sent=result["probe_sent"], pruneheight=info.get("pruneheight"))
                    last_sample = now
                if height == checkpoint:
                    checkpoint_since = checkpoint_since or now
                    if now - checkpoint_since >= 120 and not result["probe_sent"]:
                        (OUT / "before-probe-metrics.txt").write_text(metrics(19999))
                        (OUT / "before-probe-info.json").write_text(json.dumps(info, indent=2))
                        emit("trigger_probe", height=height, stalled_seconds=now-checkpoint_since)
                        probe_file.write_text("one diagnostic Request::Tip\n")
                        result.update(probe_sent=True, probe_unix_time=time.time())
                elif height > checkpoint:
                    result.setdefault("crossed_checkpoint_unix_time", time.time())
                    if height >= checkpoint + 128:
                        result.update(end_height=height, final_hash=check_hash(height),
                                      outcome="resumed_after_one_state_query" if result["probe_sent"] else "uninterrupted")
                        result["client_tree"] = rpc(CLIENT_RPC, "z_gettreestate", [str(height)])
                        result["seed_tree"] = rpc(SEED_RPC, "z_gettreestate", [str(height)])
                        if result["client_tree"] != result["seed_tree"]:
                            raise AssertionError("client and seed tree states disagree")
                        result["pass"] = True
                        emit("diagnostic_complete", height=height, outcome=result["outcome"])
                        break
            except (OSError, ValueError) as exc:
                emit("sample_error", error=str(exc))
            time.sleep(1)
        if not result["pass"]:
            raise RuntimeError("instrumented sync did not cross checkpoint before deadline")
    except Exception as exc:
        result["error"] = str(exc)
        emit("failed", error=str(exc))
    finally:
        try:
            (OUT / "final-client-metrics.txt").write_text(metrics(19999))
        except Exception as exc:
            result["final_metrics_error"] = str(exc)
        try:
            stop(proc)
        except Exception as exc:
            result["shutdown_error"] = str(exc)
            result["pass"] = False
        console.close()
        (OUT / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
        emit("complete", passed=result["pass"])
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
