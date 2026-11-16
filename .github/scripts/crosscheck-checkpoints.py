#!/usr/bin/env python3
"""Cross-checks checkpoint hashes against independent nodes' JSON-RPC.

Reads `<height> <hash>` lines and asks each `--rpc` node for `getblockhash` at
every height. A node that disagrees on any hash fails the run. A node that
cannot answer (down, behind, or erroring) is skipped with a warning, and the
run fails unless at least `--min-agreeing` nodes confirmed every hash.
"""

import argparse
import json
import sys
import urllib.request

BATCH_SIZE = 500
TIMEOUT_SECONDS = 60


class NodeUnavailable(Exception):
    """The node could not answer every height, so it proves nothing either way."""


def parse_checkpoints(lines):
    """Returns `(height, hash)` pairs from checkpoint-list lines, skipping blanks."""
    checkpoints = []
    for number, line in enumerate(lines, start=1):
        line = line.strip()
        if not line:
            continue
        parts = line.split(" ")
        if len(parts) != 2 or not parts[0].isdigit() or len(parts[1]) != 64:
            raise ValueError(f"line {number}: expected '<height> <hash>', got {line!r}")
        checkpoints.append((int(parts[0]), parts[1]))
    return checkpoints


def rpc_block_hashes(url, heights):
    """Fetches `getblockhash` for each height in JSON-RPC batches, in order."""
    hashes = []
    for start in range(0, len(heights), BATCH_SIZE):
        chunk = heights[start : start + BATCH_SIZE]
        body = json.dumps(
            [
                {"jsonrpc": "2.0", "id": index, "method": "getblockhash", "params": [height]}
                for index, height in enumerate(chunk)
            ]
        ).encode()
        request = urllib.request.Request(
            url, data=body, headers={"content-type": "application/json"}
        )
        try:
            with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
                replies = json.load(response)
        except (OSError, ValueError) as error:
            raise NodeUnavailable(f"request failed: {error}") from error
        if not isinstance(replies, list) or len(replies) != len(chunk):
            raise NodeUnavailable("batch reply does not match the request")
        by_id = {reply.get("id"): reply for reply in replies if isinstance(reply, dict)}
        for index, height in enumerate(chunk):
            reply = by_id.get(index, {})
            if reply.get("error") is not None or not isinstance(reply.get("result"), str):
                raise NodeUnavailable(f"height {height}: {reply.get('error')!r}")
            hashes.append(reply["result"])
    return hashes


def check_node(checkpoints, fetch):
    """Returns the mismatched `(height, expected, actual)` triples for one node.

    `fetch` maps a list of heights to their hashes and may raise `NodeUnavailable`.
    """
    actual = fetch([height for height, _ in checkpoints])
    return [
        (height, expected, got)
        for (height, expected), got in zip(checkpoints, actual)
        if expected != got
    ]


def crosscheck(checkpoints, nodes, min_agreeing):
    """Checks every node and returns the process exit code, reporting to stderr.

    `nodes` maps a display name to a `fetch` callable (see `check_node`).
    """
    agreeing = 0
    failed = False
    for name, fetch in nodes.items():
        try:
            mismatches = check_node(checkpoints, fetch)
        except NodeUnavailable as error:
            print(f"warning: {name} skipped: {error}", file=sys.stderr)
            continue
        if mismatches:
            failed = True
            for height, expected, got in mismatches[:20]:
                print(
                    f"error: {name} height {height}: list has {expected}, node has {got}",
                    file=sys.stderr,
                )
            print(f"error: {name} disagrees on {len(mismatches)} checkpoints", file=sys.stderr)
            continue
        agreeing += 1
        print(f"{name} confirms all {len(checkpoints)} checkpoints", file=sys.stderr)
    if failed:
        return 1
    if agreeing < min_agreeing:
        print(
            f"error: only {agreeing} node(s) confirmed the list, need {min_agreeing}",
            file=sys.stderr,
        )
        return 1
    return 0


def self_test():
    """Exercises parsing and the agree, disagree, and unavailable paths offline."""
    a, b = "a" * 64, "b" * 64
    checkpoints = parse_checkpoints(["10 " + a, "", "20 " + b])
    assert checkpoints == [(10, a), (20, b)]
    try:
        parse_checkpoints(["10 short"])
        raise AssertionError("malformed line accepted")
    except ValueError:
        pass

    def good(heights):
        return [{10: a, 20: b}[height] for height in heights]

    def bad(heights):
        return [a for _ in heights]

    def down(_heights):
        raise NodeUnavailable("down")

    assert crosscheck(checkpoints, {"good": good}, 1) == 0
    assert crosscheck(checkpoints, {"good": good, "down": down}, 1) == 0
    assert crosscheck(checkpoints, {"down": down}, 1) == 1
    assert crosscheck(checkpoints, {"good": good, "bad": bad}, 1) == 1
    assert crosscheck(checkpoints, {"good": good, "down": down}, 2) == 1
    print("self-test passed", file=sys.stderr)


def main():
    """Parses arguments and runs the cross-check or the self-test."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkpoints", help="file of new '<height> <hash>' lines")
    parser.add_argument("--rpc", action="append", default=[], help="node JSON-RPC URL")
    parser.add_argument("--min-agreeing", type=int, default=1)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        self_test()
        return 0
    if not args.checkpoints or not args.rpc:
        parser.error("--checkpoints and at least one --rpc are required")

    with open(args.checkpoints, encoding="ascii") as file:
        checkpoints = parse_checkpoints(file)
    if not checkpoints:
        print("no checkpoints to check", file=sys.stderr)
        return 0
    nodes = {url: (lambda heights, url=url: rpc_block_hashes(url, heights)) for url in args.rpc}
    return crosscheck(checkpoints, nodes, args.min_agreeing)


if __name__ == "__main__":
    sys.exit(main())
