#!/usr/bin/env python3
"""Run two private legacy nodes, then restart one after a measured tip baseline.

Requires the development worker's zakura-state helper and a task-owned Cargo
target directory. Never uses or restarts an existing node. All children stay
under this foreground process; diagnostics survive in the task's unpublished/.
"""

import argparse
import base64
import concurrent.futures
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import urllib.request


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def progress(workspace, phase, detail, state="working"):
    """Publish bounded progress without peer endpoints or RPC credentials."""
    value = {"phase": phase, "summary": detail, "state": state,
             "established": "Private baseline using instrumented main; selection unchanged",
             "open": "Restart measurements pending" if state != "done" else "",
             "next": detail, "needs_you": "", "eta": ""}
    if (workspace / ".ai-dev").is_dir():
        write_json(workspace / ".ai-dev/status.json", value)
    print(json.dumps({"time": time.time(), **value}), flush=True)


def run_check(argv, repo, output, workspace, timeout=2700):
    """Run a bounded child while refreshing the managed task's progress."""
    with output.open("w") as log:
        child = subprocess.Popen(argv, cwd=repo, stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + timeout
            while child.poll() is None:
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"command exceeded {timeout}s: {argv}")
                progress(workspace, "validation", "Running " + " ".join(argv))
                try:
                    child.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    pass
            if child.returncode:
                raise RuntimeError(f"command failed ({child.returncode}); see {output}")
        finally:
            stop(child)


def stop(child):
    if child.poll() is None:
        child.terminate()
        try:
            child.wait(timeout=30)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait(timeout=10)


class Node:
    """A node whose process, state, cookies, traces and listeners belong to this run."""

    def __init__(self, name, root, state, p2p_port, rpc_port, metrics_port, binary, public_ip):
        self.name = name
        self.root = root / name
        self.root.mkdir()
        self.rpc_port = rpc_port
        self.metrics_port = metrics_port
        self.binary = binary
        self.process = None
        self.log = None
        self.generation = 0
        self.config = self.root / "zakurad.toml"
        self.config.write_text(f'''[network]
network = "Mainnet"
p2p_stack = "legacy"
listen_addr = "0.0.0.0:{p2p_port}"
external_addr = "{public_ip}:{p2p_port}"
cache_dir = {json.dumps(str(self.root / "peer-cache"))}
identity_dir = {json.dumps(str(self.root / "identity"))}
expose_peer_addresses = true
[network.zakura]
trace_dir = {json.dumps(str(self.root / "traces"))}
[state]
cache_dir = {json.dumps(str(state))}
storage_mode = "pruned"
[rpc]
listen_addr = "127.0.0.1:{rpc_port}"
cookie_dir = {json.dumps(str(self.root))}
[metrics]
endpoint_addr = "127.0.0.1:{metrics_port}"
''')

    def start(self):
        self.generation += 1
        self.log = (self.root / f"node-{self.generation}.log").open("w")
        self.process = subprocess.Popen(
            [str(self.binary), "-c", str(self.config), "start"],
            stdout=self.log, stderr=subprocess.STDOUT)

    def stop(self):
        if self.process is not None:
            stop(self.process)
        if self.log is not None:
            self.log.close()

    def sample(self):
        row = {"node": self.name, "generation": self.generation,
               "wall_time": time.time(), "monotonic": time.monotonic()}
        try:
            cookie = (self.root / ".cookie").read_bytes().strip()
            authorization = "Basic " + base64.b64encode(cookie).decode()
            for method in ("getblockchaininfo", "getpeerinfo"):
                request = urllib.request.Request(
                    f"http://127.0.0.1:{self.rpc_port}",
                    data=json.dumps({"jsonrpc": "2.0", "id": method,
                                     "method": method, "params": []}).encode(),
                    headers={"Authorization": authorization, "Content-Type": "application/json"})
                with urllib.request.urlopen(request, timeout=3) as response:
                    result = json.load(response)
                if result.get("error"):
                    raise RuntimeError(f"RPC error in {method}")
                row[method] = result["result"]
            with urllib.request.urlopen(f"http://127.0.0.1:{self.metrics_port}", timeout=3) as response:
                # Separate scrape stream keeps RPC sampling manageable.
                with (self.root / "metrics.jsonl").open("a") as output:
                    output.write(json.dumps({"wall_time": time.time(), "text": response.read().decode()}) + "\n")
        except (OSError, ValueError, RuntimeError) as error:
            row["error"] = type(error).__name__
        row["sample_finished"] = time.time()
        return row


def near_tip(rows):
    """Require both independent connections to agree and be close to their estimates."""
    infos = [row.get("getblockchaininfo") for row in rows]
    return all(infos) and all(
        info["headers"] - info["blocks"] <= 1
        and info["estimatedheight"] - info["blocks"] <= 2 for info in infos
    ) and len({info["bestblockhash"] for info in infos}) == 1


def measure(nodes, workspace, root, before_seconds, after_seconds, catchup_seconds):
    phase = "catchup"
    deadline = time.monotonic() + catchup_seconds
    initial_heights = None
    stable = 0
    next_progress = 0
    with (root / "samples.jsonl").open("a") as output, concurrent.futures.ThreadPoolExecutor(2) as pool:
        while True:
            start = time.monotonic()
            if any(node.process.poll() is not None for node in nodes):
                raise RuntimeError("a private node exited; inspect its node log")
            rows = list(pool.map(lambda node: node.sample(), nodes))
            for row in rows:
                output.write(json.dumps({"phase": phase, **row}) + "\n")
            output.flush()
            heights = [row.get("getblockchaininfo", {}).get("blocks", 0) for row in rows]
            if initial_heights is None and all(heights):
                initial_heights = heights
            if phase == "catchup":
                advanced = initial_heights and all(h > first for h, first in zip(heights, initial_heights))
                stable = stable + 1 if advanced and near_tip(rows) else 0
                if stable >= 3:
                    phase = "before_restart"
                    deadline = start + before_seconds
                    write_json(root / "baseline-start.json", {"wall_time": time.time(), "heights": heights})
                elif start >= deadline:
                    raise TimeoutError("nodes did not establish advancing, matching near-tip state")
            elif start >= deadline:
                if phase == "before_restart":
                    write_json(root / "restart.json", {"wall_time": time.time(), "heights": heights})
                    nodes[0].stop()
                    nodes[0].start()
                    phase = "after_restart"
                    deadline = time.monotonic() + after_seconds
                else:
                    write_json(root / "completed.json", {"wall_time": time.time(), "heights": heights})
                    return
            if start >= next_progress:
                progress(workspace, phase, f"Sampling private nodes; heights={heights}; remaining={max(0, int(deadline-start))}s")
                next_progress = start + 30
            time.sleep(max(0, 10 - (time.monotonic() - start)))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--public-ip", required=True)
    parser.add_argument("--before-seconds", type=int, default=1800)
    parser.add_argument("--after-seconds", type=int, default=1800)
    parser.add_argument("--catchup-seconds", type=int, default=7200)
    args = parser.parse_args(argv)
    os.umask(0o077)
    repo = Path(__file__).resolve().parents[1]
    workspace = repo.parents[1]
    target = Path(os.environ["CARGO_TARGET_DIR"]).resolve()
    root = workspace / "unpublished" / "legacy-restart-baseline"
    root.mkdir(parents=True, exist_ok=False)
    for port in (8233, 28333, 28232, 28233, 29990, 29991):
        with socket.socket() as listener:
            listener.bind(("0.0.0.0", port))
    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, timeout=10).decode().strip()
    write_json(root / "run.json", {"sha": sha, "started": time.time(), "settings": vars(args)})
    commands = [
        ["cargo", "fmt", "-p", "zakura-network", "--", "--check"],
        ["cargo", "check", "-p", "zakura-network", "--tests", "--locked"],
        ["cargo", "test", "-p", "zakura-network", "legacy_trace::tests", "--locked"],
        ["cargo", "test", "-p", "zakura-network", "--lib", "--locked"],
        ["cargo", "build", "-p", "zakurad", "--release", "--locked"],
    ]
    nodes = []
    try:
        for index, command in enumerate(commands):
            run_check(command, repo, root / f"check-{index}.log", workspace)
        for name, p2p, rpc, metrics in (("experiment", 8233, 28232, 29990), ("reference", 28333, 28233, 29991)):
            state = target.parent / f"baseline-state-{name}"
            run_check(["zakura-state", "checkout", "mainnet-pruned", "--dest", str(state)], repo,
                      root / f"snapshot-{name}.log", workspace, timeout=300)
            nodes.append(Node(name, root, state, p2p, rpc, metrics, target / "release/zakurad", args.public_ip))
        for node in nodes:
            node.start()
        measure(nodes, workspace, root, args.before_seconds, args.after_seconds, args.catchup_seconds)
    except BaseException as error:
        write_json(root / "failure.json", {"error": str(error), "wall_time": time.time()})
        progress(workspace, "failed", str(error), "blocked")
        raise
    finally:
        for node in nodes:
            node.stop()
    progress(workspace, "done", "Baseline complete; both private nodes stopped and traces retained", "done")


def terminate(_signal, _frame):
    raise SystemExit("terminated")


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, terminate)
    main()
