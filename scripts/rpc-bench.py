#!/usr/bin/env python3
"""Measure one JSON-RPC method over persistent HTTP connections."""

from __future__ import annotations

import argparse
import http.client
import json
import math
import threading
import time
from array import array


def positive_int(value: str) -> int:
    """Parse a positive integer argument."""
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def percentile(sorted_values: array, fraction: float) -> int:
    """Return the nearest-rank percentile from sorted nanosecond values."""
    if not sorted_values:
        return 0
    rank = max(0, math.ceil(fraction * len(sorted_values)) - 1)
    return sorted_values[rank]


def worker(
    host: str,
    port: int,
    body: bytes,
    deadline: float,
    latencies: array,
    errors: list[str],
) -> None:
    """Send requests on one persistent connection until the deadline."""
    connection = http.client.HTTPConnection(host, port, timeout=10)
    headers = {"Content-Type": "application/json"}
    try:
        while time.monotonic() < deadline:
            started = time.perf_counter_ns()
            try:
                connection.request("POST", "/", body=body, headers=headers)
                response = connection.getresponse()
                payload = response.read()
                if response.status != 200:
                    raise RuntimeError(f"HTTP {response.status}")
                decoded = json.loads(payload)
                if decoded.get("error") is not None:
                    raise RuntimeError(str(decoded["error"]))
                latencies.append(time.perf_counter_ns() - started)
            except Exception as error:  # noqa: BLE001 - count every request failure.
                errors.append(str(error))
                connection.close()
                connection = http.client.HTTPConnection(host, port, timeout=10)
    finally:
        connection.close()


def run(args: argparse.Namespace) -> dict[str, object]:
    """Run one warmup and one measured concurrency level."""
    body = json.dumps(
        {"jsonrpc": "2.0", "id": "rpc-bench", "method": args.method, "params": []},
        separators=(",", ":"),
    ).encode()

    if args.warmup_seconds:
        warmup_deadline = time.monotonic() + args.warmup_seconds
        warmup_threads = []
        for _ in range(args.concurrency):
            thread = threading.Thread(
                target=worker,
                args=(args.host, args.port, body, warmup_deadline, array("Q"), []),
            )
            thread.start()
            warmup_threads.append(thread)
        for thread in warmup_threads:
            thread.join()

    deadline = time.monotonic() + args.duration_seconds
    samples = [array("Q") for _ in range(args.concurrency)]
    thread_errors = [[] for _ in range(args.concurrency)]
    threads = []
    started = time.monotonic()
    for index in range(args.concurrency):
        thread = threading.Thread(
            target=worker,
            args=(args.host, args.port, body, deadline, samples[index], thread_errors[index]),
        )
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()
    elapsed = time.monotonic() - started

    latencies = array("Q")
    for sample in samples:
        latencies.extend(sample)
    latencies = array("Q", sorted(latencies))
    errors = [error for group in thread_errors for error in group]
    count = len(latencies)
    return {
        "method": args.method,
        "concurrency": args.concurrency,
        "duration_seconds": elapsed,
        "requests": count,
        "requests_per_second": count / elapsed if elapsed else 0,
        "errors": len(errors),
        "first_error": errors[0] if errors else None,
        "latency_ms": {
            "p50": percentile(latencies, 0.50) / 1_000_000,
            "p95": percentile(latencies, 0.95) / 1_000_000,
            "p99": percentile(latencies, 0.99) / 1_000_000,
            "max": (latencies[-1] / 1_000_000) if latencies else 0,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=positive_int, default=8232)
    parser.add_argument("--method", default="getblockchaininfo")
    parser.add_argument("--concurrency", type=positive_int, required=True)
    parser.add_argument("--duration-seconds", type=positive_int, default=30)
    parser.add_argument("--warmup-seconds", type=int, default=5)
    args = parser.parse_args()
    if args.warmup_seconds < 0:
        parser.error("--warmup-seconds must be non-negative")
    print(json.dumps(run(args), sort_keys=True))


if __name__ == "__main__":
    main()
