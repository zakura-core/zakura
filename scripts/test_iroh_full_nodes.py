#!/usr/bin/env python3
"""Verify mixed-cohort legacy sync with independently pinned, local regtest binaries."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import socket
import subprocess
import time
import urllib.request


def rpc(port, method, params=None):
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}",
        json.dumps({"jsonrpc": "2.0", "id": 1, "method": method,
                    "params": params or []}).encode(),
        {"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        result = json.load(response)
    if result.get("error"):
        raise RuntimeError(f"{method}: {result['error']}")
    return result["result"]


def wait_for(description, processes, predicate, seconds=120):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        for process in processes:
            if process.poll() is not None:
                raise RuntimeError(f"node exited with {process.returncode} while {description}")
        try:
            value = predicate()
            if value:
                return value
        except (OSError, ValueError, RuntimeError) as error:
            last = str(error)
        time.sleep(0.5)
    raise RuntimeError(f"timeout waiting for {description}: {last}")


def metrics(port):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as response:
        return response.read().decode()


def metric(text, name):
    return sum(float(line.split()[-1]) for line in text.splitlines()
               if line.startswith(name + " ") or line.startswith(name + "{"))


def configuration(index, ports, seed_port, root):
    tcp, udp, rpc_port, metrics_port = ports
    peers = [] if index == 1 else [f"127.0.0.1:{seed_port}"]
    return f'''[network]
network = "Regtest"
p2p_stack = "dual"
listen_addr = "127.0.0.1:{tcp}"
cache_dir = false
initial_testnet_peers = {json.dumps(peers)}
max_connections_per_ip = 10
zakura_node_secret_key = "{f'{index:02x}' * 32}"
identity_dir = {json.dumps(str(root / 'identity'))}

[network.zakura]
listen_addr = "127.0.0.1:{udp}"
bootstrap_peers = []
trace_dir = {json.dumps(str(root / 'traces'))}

[state]
ephemeral = true

[rpc]
listen_addr = "127.0.0.1:{rpc_port}"
enable_cookie_auth = false

[metrics]
endpoint_addr = "127.0.0.1:{metrics_port}"

[mining]
internal_miner = false
miner_address = "tmJymvcUCn1ctbghvTJpXBwHiMEB8P6wxNV"

[tracing]
filter = "info,zakura_network::zakura=debug"
'''


def run_pair(seed_binary, client_binary, output, catchup_timeout, blocks_per_peer):
    output.mkdir(parents=True, exist_ok=False)
    reservations = []
    ports = []
    processes = []
    logs = []
    result = {"seed": str(seed_binary), "client": str(client_binary), "passed": False}
    try:
        for _ in range(2):
            node_ports = []
            for kind in (socket.SOCK_STREAM, socket.SOCK_DGRAM, socket.SOCK_STREAM, socket.SOCK_STREAM):
                sock = socket.socket(socket.AF_INET, kind)
                sock.bind(("127.0.0.1", 0))
                reservations.append(sock)
                node_ports.append(sock.getsockname()[1])
            ports.append(node_ports)
        for index, binary in enumerate((seed_binary, client_binary)):
            root = output / f"node{index + 1}"
            root.mkdir()
            config = root / "config.toml"
            config.write_text(configuration(index + 1, ports[index], ports[0][0], root))
            for sock in reservations[index * 4:(index + 1) * 4]:
                sock.close()
            log = (root / "node.log").open("wb")
            logs.append(log)
            processes.append(subprocess.Popen(
                [str(binary), "--config", str(config), "start"],
                stdout=log, stderr=subprocess.STDOUT,
                env={**os.environ, "RUST_BACKTRACE": "1"},
            ))
            wait_for(f"node{index + 1} RPC", processes,
                     lambda: rpc(ports[index][2], "getblockcount") >= 0)
        wait_for("retained legacy connection", processes,
                 lambda: all(len(rpc(p[2], "getpeerinfo")) > 0 for p in ports))
        for miner, receiver in ((0, 1), (1, 0)):
            for _ in range(blocks_per_peer):
                mined = rpc(ports[miner][2], "generate", [1])
                assert len(mined) == 1
                block_hash = mined[0]
                wait_for("mined block propagation", processes,
                         lambda: rpc(ports[receiver][2], "getbestblockhash") == block_hash)
        result["height_before_restart"] = rpc(ports[0][2], "getblockcount")
        # A fresh ephemeral client must catch up while the seed is idle.
        processes[1].terminate()
        processes[1].wait(timeout=30)
        log = (output / "node2" / "restart.log").open("wb")
        logs.append(log)
        processes[1] = subprocess.Popen(
            [str(client_binary), "--config", str(output / "node2" / "config.toml"), "start"],
            stdout=log, stderr=subprocess.STDOUT,
        )
        tip = rpc(ports[0][2], "getbestblockhash")
        started = time.monotonic()
        wait_for("fresh client catch-up", processes,
                 lambda: rpc(ports[1][2], "getbestblockhash") == tip, seconds=catchup_timeout)
        result["catchup_seconds"] = round(time.monotonic() - started, 2)
        result["tip_after_restart"] = tip
        result["heights"] = [rpc(p[2], "getblockcount") for p in ports]
        result["legacy_peer_counts"] = [len(rpc(p[2], "getpeerinfo")) for p in ports]
        for index, p in enumerate(ports):
            text = metrics(p[3])
            (output / f"node{index + 1}" / "metrics.txt").write_text(text)
            # Connection gauges are emitted lazily; an absent gauge is normal when
            # no native session has ever registered. Require evidence of the
            # advertised native capability so disabled networking cannot pass.
            assert metric(text, "zakura_p2p_handshake_service_bit_advertised") > 0
            assert metric(text, "zakura_p2p_conn_active") == 0, "mixed cohorts must stay on TCP"
            assert metric(text, "zakura_p2p_handshake_upgraded") == 0, "unexpected native handoff"
        assert all(result["legacy_peer_counts"])
        result["passed"] = True
    except Exception as error:
        result["error"] = str(error)
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        for log in logs:
            log.close()
        for sock in reservations:
            sock.close()
        (output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--old-bin", type=Path, required=True)
    parser.add_argument("--new-bin", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--catchup-timeout", type=int, default=720,
                        help="Includes the existing ten-minute dual-stack fallback window.")
    parser.add_argument("--blocks-per-peer", type=int, default=3)
    args = parser.parse_args()
    if args.blocks_per_peer < 1 or args.catchup_timeout < 1:
        parser.error("block count and timeout must be positive")
    binaries = {"old": args.old_bin.resolve(), "new": args.new_bin.resolve()}
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(run_pair, binaries[seed], binaries[client],
                   args.output_dir.resolve() / f"{seed}-seed-{client}-client",
                   args.catchup_timeout, args.blocks_per_peer)
                   for seed, client in (("old", "new"), ("new", "old"))]
        results = [future.result() for future in futures]
    for result in results:
        print(json.dumps(result), flush=True)
    return 0 if all(result["passed"] for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
