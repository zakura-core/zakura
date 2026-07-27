#!/usr/bin/env python3
"""Submit zecd self-sends round-robin and grade cross-node propagation."""

from __future__ import annotations

import argparse
import base64
import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import urlparse

from spam_report import load_events, write_reports

SCRIPT_DIR = Path(__file__).resolve().parent
HARNESS_DIR = SCRIPT_DIR.parent
DEFAULT_AMOUNT = 0.00020000
FORBIDDEN_PATH_PARTS = (
    "mnemonic",
    "recovery.json",
    "identity.txt",
    "keys.toml",
    "wallet.dat",
    "seed",
)

STOP = False
PROC: subprocess.Popen | None = None
ROOT = Path("/root/ironwood-spam")
ZECD = ROOT / "bin/zecd"
CONF = ROOT / "wallet/zecd.toml"
IDENTITY = ROOT / "wallet/data/identity.txt"
PASS_FILE = ROOT / "wallet/rpc.password"
PID_FILE = ROOT / "logs/zecd.pid"
ZECD_LOG = ROOT / "logs/zecd.log"
RUN_LOG = ROOT / "logs/round-robin.jsonl"
STATUS_FILE = ROOT / "logs/round-robin-status.json"
ZECD_RPC_URL = "http://127.0.0.1:18888"
NETWORK = "test"


class RunDurationElapsed(TimeoutError):
    """The configured transaction submission duration elapsed."""


def remaining_timeout(deadline: float | None, maximum: float) -> float:
    if deadline is None:
        return maximum
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise RunDurationElapsed("transaction submission duration elapsed")
    return min(maximum, remaining)


def on_stop(*_args) -> None:
    global STOP
    STOP = True


signal.signal(signal.SIGTERM, on_stop)
signal.signal(signal.SIGINT, on_stop)


def rpc_call(url: str, method: str, params=None, timeout=30, auth=None):
    payload = json.dumps(
        {"jsonrpc": "1.0", "id": "mempool-spam", "method": method, "params": params or []}
    ).encode()
    headers = {"Content-Type": "application/json"}
    if auth:
        headers["Authorization"] = "Basic " + base64.b64encode(auth.encode()).decode()
    request = urllib.request.Request(url, data=payload, headers=headers)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = json.loads(response.read())
    if body.get("error"):
        raise RuntimeError(f"{url} {method}: {body['error']}")
    return body["result"]


def zecd_auth() -> str:
    return f"smoke:{PASS_FILE.read_text().strip()}"


def zecd_rpc(method: str, params=None, timeout=300):
    return rpc_call(ZECD_RPC_URL, method, params, timeout=timeout, auth=zecd_auth())


def log_event(event: dict) -> None:
    event = {"ts": time.time(), **event}
    with RUN_LOG.open("a") as log_file:
        log_file.write(json.dumps(event) + "\n")
    print(json.dumps(event), flush=True)


def write_status(status: dict) -> None:
    STATUS_FILE.write_text(json.dumps(status, indent=2) + "\n")


def tip_height(rpc_url: str, timeout=30) -> int:
    return int(rpc_call(rpc_url, "getblockchaininfo", timeout=timeout)["blocks"])


def wait_next_block(
    rpc_url: str, start_height: int, timeout=600, run_deadline: float | None = None
) -> int:
    deadline = time.monotonic() + timeout
    if run_deadline is not None:
        deadline = min(deadline, run_deadline)
    while time.monotonic() < deadline:
        if STOP:
            raise InterruptedError("stopped")
        height = tip_height(rpc_url, timeout=remaining_timeout(deadline, 30))
        if height > start_height:
            return height
        time.sleep(min(3, max(0, deadline - time.monotonic())))
    if run_deadline is not None and time.monotonic() >= run_deadline:
        raise RunDurationElapsed("transaction submission duration elapsed")
    raise TimeoutError(f"no new block after height {start_height}")


def transaction_state(
    node: dict, txid: str, deadline: float | None = None
) -> tuple[bool, dict | None]:
    try:
        if txid in rpc_call(
            node["rpc_url"], "getrawmempool", timeout=remaining_timeout(deadline, 10)
        ):
            return True, None
    except RunDurationElapsed:
        raise
    except Exception:  # noqa: BLE001
        pass
    try:
        tx = rpc_call(
            node["rpc_url"],
            "getrawtransaction",
            [txid, 1],
            timeout=remaining_timeout(deadline, 10),
        )
        return True, tx if isinstance(tx, dict) else None
    except RunDurationElapsed:
        raise
    except Exception:  # noqa: BLE001
        return False, None


def first_seen_matrix(
    nodes: list[dict],
    txid: str,
    duration=30.0,
    interval=1.0,
    run_deadline: float | None = None,
) -> dict[str, float]:
    started = time.monotonic()
    deadline = started + duration
    if run_deadline is not None:
        deadline = min(deadline, run_deadline)
    seen: dict[str, float] = {}
    while time.monotonic() < deadline:
        if STOP:
            break
        for node in nodes:
            if node["name"] in seen:
                continue
            try:
                known, _ = transaction_state(node, txid, deadline=deadline)
            except RunDurationElapsed:
                return seen
            if known:
                seen[node["name"]] = round(time.monotonic() - started, 3)
        if len(seen) == len(nodes):
            break
        time.sleep(min(interval, max(0, deadline - time.monotonic())))
    return seen


def mined_state(nodes: list[dict], txid: str) -> tuple[int | None, int]:
    for node in nodes:
        try:
            tx = rpc_call(node["rpc_url"], "getrawtransaction", [txid, 1], timeout=10)
            if not isinstance(tx, dict):
                continue
            confirmations = int(tx.get("confirmations") or 0)
            if confirmations <= 0:
                continue
            height = tx.get("height") or tx.get("blockheight")
            if height is None:
                height = tip_height(node["rpc_url"]) - confirmations + 1
            return int(height), confirmations
        except Exception:  # noqa: BLE001
            continue
    return None, 0


def drain_transactions(nodes: list[dict], txids: list[str], minutes: float) -> None:
    pending = set(txids)
    results: dict[str, tuple[int | None, int]] = {}
    deadline = time.monotonic() + minutes * 60
    while pending and time.monotonic() < deadline and not STOP:
        for txid in list(pending):
            state = mined_state(nodes, txid)
            if state[1] > 0:
                results[txid] = state
                pending.remove(txid)
        if pending:
            time.sleep(min(5, max(0, deadline - time.monotonic())))
    for txid in txids:
        height, confirmations = results.get(txid, mined_state(nodes, txid))
        log_event(
            {
                "event": "drain_result",
                "txid": txid,
                "mined_height": height,
                "confirmations": confirmations,
            }
        )


def server_uri(rpc_url: str) -> str:
    parsed = urlparse(rpc_url)
    default_port = 18232 if NETWORK == "test" else 8232
    return f"zebra://{parsed.hostname}:{parsed.port or default_port}"


def inventory_pools(deadline: float | None = None) -> dict:
    pools = {}
    for note in zecd_rpc(
        "listunspent", [0], timeout=remaining_timeout(deadline, 300)
    ) or []:
        pool = note.get("pool", "?")
        record = pools.setdefault(pool, {"notes": 0, "taz": 0.0})
        record["notes"] += 1
        record["taz"] += float(note["amount"])
    for record in pools.values():
        record["taz"] = round(record["taz"], 8)
    return pools


def ensure_conf() -> None:
    CONF.parent.mkdir(parents=True, exist_ok=True)
    rpc_port = urlparse(ZECD_RPC_URL).port or (18232 if NETWORK == "test" else 8232)
    CONF.write_text(
        f"""network = "{NETWORK}"
datadir = "{ROOT / 'wallet/data'}"
default_wallet = "default"

[wallets.default]
dir = "{ROOT / 'wallet/data/default'}"

[backend]
server = "zebra://127.0.0.1:{18232 if NETWORK == 'test' else 8232}"
connect_timeout_secs = 10
reconnect_base_secs = 1
reconnect_max_secs = 60

[rpc]
bind = "127.0.0.1"
port = {rpc_port}
user = "smoke"

[keys]
age_identity = "{IDENTITY}"

[health]
enabled = true
bind = "127.0.0.1"
port = 9233
"""
    )
    os.chmod(CONF, 0o600)


def stop_zecd() -> None:
    global PROC
    if PROC is not None and PROC.poll() is None:
        PROC.terminate()
        try:
            PROC.wait(timeout=15)
        except subprocess.TimeoutExpired:
            PROC.kill()
            PROC.wait(timeout=5)
    elif PID_FILE.exists():
        try:
            pid = int(PID_FILE.read_text().strip())
            os.kill(pid, signal.SIGTERM)
            for _ in range(50):
                try:
                    os.kill(pid, 0)
                    time.sleep(0.1)
                except ProcessLookupError:
                    break
        except (ValueError, ProcessLookupError, PermissionError):
            pass
    PROC = None
    time.sleep(1)


def wait_zecd_ready(timeout=120, run_deadline: float | None = None) -> None:
    deadline = time.monotonic() + timeout
    if run_deadline is not None:
        deadline = min(deadline, run_deadline)
    last_error = None
    while time.monotonic() < deadline:
        if STOP:
            raise InterruptedError("stopped")
        try:
            zecd_rpc("getwalletinfo", timeout=remaining_timeout(deadline, 10))
            zecd_rpc("getblockchaininfo", timeout=remaining_timeout(deadline, 10))
            return
        except RunDurationElapsed:
            raise
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
            time.sleep(min(0.5, max(0, deadline - time.monotonic())))
    if run_deadline is not None and time.monotonic() >= run_deadline:
        raise RunDurationElapsed("transaction submission duration elapsed")
    raise RuntimeError(f"zecd not ready: {last_error}")


def start_zecd(server: str, run_deadline: float | None = None) -> None:
    global PROC
    stop_zecd()
    ensure_conf()
    ZECD_LOG.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["ZECD_RPC_PASSWORD"] = PASS_FILE.read_text().strip()
    with ZECD_LOG.open("a") as log_file:
        PROC = subprocess.Popen(
            [
                str(ZECD),
                "--conf",
                str(CONF),
                "--age-identity",
                str(IDENTITY),
                "--server",
                server,
                "run",
            ],
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
    PID_FILE.write_text(f"{PROC.pid}\n")
    wait_zecd_ready(run_deadline=run_deadline)


def send_with_retry(
    node: dict,
    amount: float,
    max_retries: int = 4,
    run_deadline: float | None = None,
) -> tuple[str, int, dict, dict]:
    last_error = None
    for attempt in range(max_retries):
        before = inventory_pools(deadline=run_deadline)
        height = tip_height(
            node["rpc_url"], timeout=remaining_timeout(run_deadline, 30)
        )
        address = zecd_rpc(
            "getnewaddress", [], timeout=remaining_timeout(run_deadline, 300)
        )
        try:
            txid = zecd_rpc(
                "sendtoaddress",
                [address, amount],
                timeout=remaining_timeout(run_deadline, 600),
            )
            return txid, height, before, inventory_pools(deadline=run_deadline)
        except RunDurationElapsed:
            raise
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            log_event(
                {
                    "event": "send_retry",
                    "attempt": attempt + 1,
                    "submit": node["name"],
                    "error": str(exc),
                    "height": height,
                }
            )
            wait_next_block(node["rpc_url"], height, run_deadline=run_deadline)
    raise RuntimeError(f"sendtoaddress failed after retries: {last_error}")


def load_config(args: argparse.Namespace) -> tuple[dict, str]:
    if args.config:
        path = args.config
        environment = "custom"
    else:
        path = HARNESS_DIR / "envs" / args.environment / "config.json"
        environment = args.environment
    if not path.is_file():
        raise SystemExit(f"environment config does not exist: {path}")
    config = json.loads(path.read_text())
    if config.get("network") not in {"test", "main"}:
        raise SystemExit("config network must be 'test' or 'main'")
    nodes = config.get("nodes")
    if not isinstance(nodes, list) or not nodes:
        raise SystemExit("config nodes must be a non-empty list")
    for node in nodes:
        if not all(node.get(key) for key in ("name", "impl", "rpc_url")):
            raise SystemExit(f"node requires name, impl, and rpc_url: {node}")
    return config, environment


def configure_paths(config: dict, out_dir: Path | None) -> Path:
    global ROOT, ZECD, CONF, IDENTITY, PASS_FILE, PID_FILE, ZECD_LOG
    global RUN_LOG, STATUS_FILE, ZECD_RPC_URL, NETWORK
    controller = config.get("controller") or {}
    ROOT = Path(controller.get("spam_root", "/root/ironwood-spam"))
    NETWORK = config["network"]
    ZECD_RPC_URL = controller.get(
        "zecd_rpc",
        "http://127.0.0.1:18888" if NETWORK == "test" else "http://127.0.0.1:8232",
    )
    ZECD = ROOT / "bin/zecd"
    CONF = ROOT / "wallet/zecd.toml"
    IDENTITY = ROOT / "wallet/data/identity.txt"
    PASS_FILE = ROOT / "wallet/rpc.password"
    PID_FILE = ROOT / "logs/zecd.pid"
    ZECD_LOG = ROOT / "logs/zecd.log"
    output = out_dir or ROOT / "logs"
    lowered = str(output).lower()
    if any(fragment in lowered for fragment in FORBIDDEN_PATH_PARTS):
        raise SystemExit(f"refusing output path that looks like key material: {output}")
    output.mkdir(parents=True, exist_ok=True)
    RUN_LOG = output / "round-robin.jsonl"
    STATUS_FILE = output / "round-robin-status.json"
    return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--environment", choices=("testnet", "mainnet"), default="testnet")
    group.add_argument("--config", type=Path)
    parser.add_argument("--out-dir", type=Path)
    parser.add_argument("--amount", type=float, default=DEFAULT_AMOUNT)
    parser.add_argument("--max-rounds", type=int, default=0, help="0 = unlimited")
    parser.add_argument("--duration-minutes", type=float, default=0, help="0 = unlimited")
    parser.add_argument("--matrix-secs", type=float, default=30.0)
    parser.add_argument("--drain-minutes", type=float, default=10.0)
    parser.add_argument("--require-all-seen", action="store_true")
    parser.add_argument("--require-mined", action="store_true")
    parser.add_argument("--wait-block", action="store_true")
    args = parser.parse_args()
    if args.amount <= 0:
        parser.error("--amount must be positive")
    if min(args.max_rounds, args.duration_minutes, args.matrix_secs, args.drain_minutes) < 0:
        parser.error("round, duration, matrix, and drain values cannot be negative")
    return args


def main() -> int:
    args = parse_args()
    config, environment = load_config(args)
    output = configure_paths(config, args.out_dir)
    nodes = config["nodes"]
    RUN_LOG.write_text("")
    submitted_txids: list[str] = []
    run_started = time.monotonic()
    run_deadline = (
        run_started + args.duration_minutes * 60 if args.duration_minutes else None
    )
    run_error = None

    log_event(
        {
            "event": "start",
            "mode": "single",
            "environment": environment,
            "network": NETWORK,
            "nodes": [node["name"] for node in nodes],
            "amount": args.amount,
            "duration_minutes": args.duration_minutes,
            "wait_block": args.wait_block,
            "orchard_allowed": True,
        }
    )
    round_index = 0
    restore_error = None
    try:
        while not STOP:
            if args.max_rounds and round_index >= args.max_rounds:
                break
            remaining_timeout(run_deadline, float("inf"))
            node = nodes[round_index % len(nodes)]
            round_index += 1
            server = server_uri(node["rpc_url"])
            write_status(
                {
                    "phase": "switch_upstream",
                    "round": round_index,
                    "submit": node["name"],
                    "server": server,
                }
            )
            log_event(
                {
                    "event": "switch_upstream",
                    "round": round_index,
                    "submit": node["name"],
                    "server": server,
                }
            )
            start_zecd(server, run_deadline=run_deadline)
            inventory = inventory_pools(deadline=run_deadline)
            spendable = sum(
                inventory.get(pool, {}).get("notes", 0) for pool in ("ironwood", "orchard")
            )
            if spendable < 1:
                raise RuntimeError(f"no spendable Ironwood or Orchard notes: {inventory}")
            started = time.monotonic()
            txid, height, before, after = send_with_retry(
                node, args.amount, run_deadline=run_deadline
            )
            submitted_txids.append(txid)
            seen = first_seen_matrix(
                nodes,
                txid,
                duration=args.matrix_secs,
                run_deadline=run_deadline,
            )
            missing = [candidate["name"] for candidate in nodes if candidate["name"] not in seen]
            log_event(
                {
                    "event": "submitted",
                    "round": round_index,
                    "submit": node["name"],
                    "impl": node["impl"],
                    "txid": txid,
                    "height_at_submit": height,
                    "inventory_pre": before,
                    "inventory_post": after,
                    "first_seen": seen,
                    "missing": missing,
                    "seen_count": len(seen),
                    "seen_all": not missing,
                    "send_secs": round(time.monotonic() - started, 3),
                    "wait_block": args.wait_block,
                }
            )
            if args.wait_block:
                new_height = wait_next_block(
                    node["rpc_url"], height, run_deadline=run_deadline
                )
                log_event(
                    {"event": "block", "round": round_index, "height": new_height, "txid": txid}
                )
    except RunDurationElapsed:
        log_event({"event": "duration_elapsed", "rounds": round_index})
    except TimeoutError as exc:
        if run_deadline is not None and time.monotonic() >= run_deadline:
            log_event({"event": "duration_elapsed", "rounds": round_index})
        else:
            run_error = str(exc)
            log_event({"event": "fatal", "error": run_error})
    except (Exception, InterruptedError) as exc:  # noqa: BLE001
        run_error = str(exc)
        log_event({"event": "fatal", "error": run_error})
    finally:
        try:
            if submitted_txids:
                drain_transactions(nodes, submitted_txids, args.drain_minutes)
            log_event({"event": "stop", "rounds": round_index, "error": run_error})
        finally:
            try:
                local_port = 18232 if NETWORK == "test" else 8232
                start_zecd(f"zebra://127.0.0.1:{local_port}")
                log_event(
                    {
                        "event": "restored_upstream",
                        "server": f"zebra://127.0.0.1:{local_port}",
                    }
                )
            except Exception as exc:  # noqa: BLE001
                restore_error = str(exc)
                log_event({"event": "restore_failed", "error": restore_error})
                stop_zecd()
        report = write_reports(load_events(RUN_LOG), output)

    summary = report["summary"]
    failed_empty = (
        args.require_all_seen or args.require_mined
    ) and summary["submitted"] == 0
    failed_seen = args.require_all_seen and summary["missed"] > 0
    failed_mined = args.require_mined and summary["unconfirmed"] > 0
    return 2 if run_error or restore_error or failed_empty or failed_seen or failed_mined else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except urllib.error.URLError as exc:
        print(f"RPC transport error: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
