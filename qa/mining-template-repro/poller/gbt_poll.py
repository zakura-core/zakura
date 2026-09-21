#!/usr/bin/env python3
"""Long-poll storm client for the zakura#1080 getblocktemplate repro.

Runs N concurrent getblocktemplate long polls against one node and records every
response, error and latency as JSONL. A withhold shows up here as either an error
response ("template parent changed; retry" and friends) or as a long poll that
hangs across a tip change it should have returned for.
"""

import argparse
import json
import queue
import sys
import threading
import time
import urllib.error
import urllib.request

CAPABILITIES = ["longpoll", "coinbasetxn", "workid", "proposal", "serverlist", "mutable"]


def rpc(url, method, params, timeout):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def client_loop(idx, url, deadline, timeout, out_q, stop):
    long_poll_id = None
    while not stop.is_set() and time.time() < deadline:
        params = [{"capabilities": CAPABILITIES}]
        if long_poll_id is not None:
            params[0]["longpollid"] = long_poll_id
        started = time.time()
        try:
            reply = rpc(url, "getblocktemplate", params, timeout)
        except urllib.error.URLError as err:
            out_q.put({"ts": started, "client": idx, "latency": time.time() - started,
                       "outcome": "transport_error", "detail": str(err)})
            time.sleep(0.05)
            continue
        except Exception as err:  # noqa: BLE001 - the harness must never die mid-run
            out_q.put({"ts": started, "client": idx, "latency": time.time() - started,
                       "outcome": "client_error", "detail": repr(err)})
            time.sleep(0.05)
            continue

        latency = time.time() - started
        if "error" in reply and reply["error"]:
            err = reply["error"]
            out_q.put({"ts": started, "client": idx, "latency": latency, "outcome": "withhold",
                       "detail": err.get("message", json.dumps(err))})
            # A withheld template leaves the long poll id stale; drop back to a
            # plain request so the next call re-reads the current parent.
            long_poll_id = None
            continue

        result = reply.get("result") or {}
        long_poll_id = result.get("longpollid")
        out_q.put({"ts": started, "client": idx, "latency": latency, "outcome": "template",
                   "height": result.get("height"),
                   "prev": result.get("previousblockhash"),
                   "longpollid": long_poll_id})


def writer_loop(path, out_q, stop):
    with open(path, "w", buffering=1) as handle:
        while not (stop.is_set() and out_q.empty()):
            try:
                event = out_q.get(timeout=0.2)
            except queue.Empty:
                continue
            handle.write(json.dumps(event) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rpc", default="127.0.0.1:18232")
    parser.add_argument("--clients", type=int, default=8)
    parser.add_argument("--duration", type=float, default=60.0)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    url = f"http://{args.rpc}"
    out_q: "queue.Queue[dict]" = queue.Queue()
    stop = threading.Event()
    deadline = time.time() + args.duration

    writer = threading.Thread(target=writer_loop, args=(args.out, out_q, stop), daemon=True)
    writer.start()

    workers = [
        threading.Thread(target=client_loop,
                         args=(i, url, deadline, args.timeout, out_q, stop), daemon=True)
        for i in range(args.clients)
    ]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()

    stop.set()
    writer.join(timeout=5)
    print(f"poller finished, events written to {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
