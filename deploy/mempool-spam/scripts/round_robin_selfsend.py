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


class RpcError(RuntimeError):
    """A JSON-RPC error answer, with its code."""

    def __init__(self, message: str, code: int | None):
        super().__init__(message)
        self.code = code


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
        error = body["error"]
        code = error.get("code") if isinstance(error, dict) else None
        raise RpcError(f"{url} {method}: {error}", code)
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


IN_POOL = "pool"
IN_CHAIN = "chain"
# The node answered that it holds the transaction nowhere.
ABSENT = "absent"
# `getrawtransaction`'s error code for a transaction a node does not know.
RPC_NOT_FOUND = -5
# The exit status of a run with no miss but a round no pool was seen to hold.
EXIT_INCONCLUSIVE = 3


def transaction_state(node: dict, txid: str, deadline: float | None = None) -> str | None:
    """Where `node` holds `txid`: IN_POOL, IN_CHAIN once mined, ABSENT, or None
    if its answers do not settle it.

    A node that only knows the transaction from a block never showed it held
    the transaction in its pool, so the two are told apart. ABSENT needs a
    definite answer: the pool listing without the transaction, and the
    lookup's "not found" error. A failed or unclear answer is None.
    """
    try:
        if txid in rpc_call(
            node["rpc_url"], "getrawmempool", timeout=remaining_timeout(deadline, 10)
        ):
            return IN_POOL
        listed = True
    except RunDurationElapsed:
        raise
    except Exception:  # noqa: BLE001
        listed = False
    try:
        tx = rpc_call(
            node["rpc_url"],
            "getrawtransaction",
            [txid, 1],
            timeout=remaining_timeout(deadline, 10),
        )
    except RunDurationElapsed:
        raise
    except RpcError as exc:
        return ABSENT if listed and exc.code == RPC_NOT_FOUND else None
    except Exception:  # noqa: BLE001
        return None
    # Only an answer about this transaction settles anything.
    if not isinstance(tx, dict) or tx.get("txid") != txid:
        return None
    if int(tx.get("confirmations") or 0) > 0:
        return IN_CHAIN
    # A pooled transaction has no block. One in a side-chain block has a block
    # hash and height -1, and shows nothing about the pool.
    if tx.get("blockhash") is None and tx.get("height") is None:
        return IN_POOL
    return None


def first_seen_matrix(
    nodes: list[dict],
    txid: str,
    duration=30.0,
    interval=1.0,
    run_deadline: float | None = None,
) -> tuple[dict[str, float], dict[str, float], list[str]]:
    """When each node first showed `txid`: in its pool, or only once mined.

    Returns two maps from node name to seconds since the poll started, and
    the other nodes whose last answer was not a definite ABSENT, or every
    unsettled node if the window was cut short. A node in the second map was
    first sampled after the block, and one in the list was never definitely
    without the transaction for the whole window, so for either, whether its
    pool held it is unknown. Only the rest are misses.
    """
    started = time.monotonic()
    window_end = started + duration
    deadline = window_end if run_deadline is None else min(window_end, run_deadline)
    in_pool: dict[str, float] = {}
    after_mined: dict[str, float] = {}
    last: dict[str, str | None] = {}
    run_elapsed = False
    while time.monotonic() < deadline and not STOP:
        for node in nodes:
            if node["name"] in in_pool or node["name"] in after_mined:
                continue
            try:
                # A sample begun inside the window finishes, so the window's
                # end never cuts its answer short. Only the run's own
                # deadline does.
                state = transaction_state(node, txid, deadline=run_deadline)
            except RunDurationElapsed:
                # The interrupted sample settles nothing, whatever came before.
                last[node["name"]] = None
                run_elapsed = True
                break
            last[node["name"]] = state
            elapsed = round(time.monotonic() - started, 3)
            if state == IN_POOL:
                in_pool[node["name"]] = elapsed
            elif state == IN_CHAIN:
                after_mined[node["name"]] = elapsed
        if run_elapsed or len(in_pool) + len(after_mined) == len(nodes):
            break
        time.sleep(min(interval, max(0, deadline - time.monotonic())))
    # A window the run's deadline or a stop cut short gave no node its full
    # time, so no absence in it is a miss.
    cut_short = STOP or run_elapsed or deadline < window_end
    unobserved = [
        node["name"]
        for node in nodes
        if node["name"] not in in_pool
        and node["name"] not in after_mined
        and (cut_short or last.get(node["name"]) != ABSENT)
    ]
    return in_pool, after_mined, unobserved


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
            continue
        # The transaction is out, so its round is graded whatever happens to
        # the wallet inventory, which only describes the round. If the run's
        # deadline passes now, its pools are never sampled, which makes the
        # round inconclusive rather than lost.
        try:
            after = inventory_pools(deadline=run_deadline)
        except Exception:  # noqa: BLE001
            after = {}
        return txid, height, before, after
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
    # Sightings are kept by name, so two nodes with one name would hide one.
    names = [node["name"] for node in nodes]
    if len(set(names)) != len(names):
        raise SystemExit(f"node names must be unique: {names}")
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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the driver's arguments from `argv`, or the command line."""
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
    # Grading is strict unless turned off, so a run that proves nothing never
    # exits 0 by default.
    parser.add_argument(
        "--require-all-seen", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument("--require-mined", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--wait-block", action="store_true")
    args = parser.parse_args(argv)
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
            seen, seen_after_mined, unobserved = first_seen_matrix(
                nodes,
                txid,
                duration=args.matrix_secs,
                run_deadline=run_deadline,
            )
            # A miss needs a node's definite answer that it holds the
            # transaction nowhere. Anything less is inconclusive.
            missing = [
                candidate["name"]
                for candidate in nodes
                if candidate["name"] not in seen
                and candidate["name"] not in seen_after_mined
                and candidate["name"] not in unobserved
            ]
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
                    "seen_after_mined": seen_after_mined,
                    "unobserved": unobserved,
                    "missing": missing,
                    "seen_count": len(seen),
                    "seen_all": len(seen) == len(nodes),
                    "inconclusive": bool(seen_after_mined or unobserved) and not missing,
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

    return run_status(
        report["summary"],
        require_all_seen=args.require_all_seen,
        require_mined=args.require_mined,
        errored=bool(run_error or restore_error),
    )


def run_status(summary: dict, *, require_all_seen: bool, require_mined: bool, errored: bool) -> int:
    """The process exit status for a run's report `summary`.

    2 for an error or a failed requirement. With `require_all_seen`, a run
    with no miss but a round not seen in every pool is EXIT_INCONCLUSIVE,
    since that round is neither a pass nor a miss and is repeated. 0
    otherwise.
    """
    failed_empty = (require_all_seen or require_mined) and summary["submitted"] == 0
    failed_seen = require_all_seen and summary["missed"] > 0
    failed_mined = require_mined and summary["unconfirmed"] > 0
    if errored or failed_empty or failed_seen or failed_mined:
        return 2
    # A pass is every round seen in every pool, whatever kept a round from it.
    if require_all_seen and summary["seen_all"] < summary["submitted"]:
        return EXIT_INCONCLUSIVE
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except urllib.error.URLError as exc:
        print(f"RPC transport error: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
