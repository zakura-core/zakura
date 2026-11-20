#!/usr/bin/env python3
"""Measure a pool canary against its neighbours: tip lag, pool overlap and templates.

`sample` polls every node's RPC and writes one JSON line per node per round.
`overlap` and `templates` grade those lines. `tip-lag` grades `tip_published`
trace rows mirrored by `deploy/trace-query/trace_query.py pull`.

Each grade ends in a verdict: `pass`, `fail` or `inconclusive`. Missing rows,
too few samples or a block some node never published make a window
inconclusive, never a pass. The exit status is 0 for pass, 1 for fail, 3 for
inconclusive and 2 for a usage or runtime error.
"""

import argparse
import concurrent.futures
import itertools
import json
import math
import sqlite3
import statistics
import sys
import time
import urllib.request

EXIT_PASS, EXIT_FAIL, EXIT_ERROR, EXIT_INCONCLUSIVE = 0, 1, 2, 3
EXIT_CODES = {"pass": EXIT_PASS, "fail": EXIT_FAIL, "inconclusive": EXIT_INCONCLUSIVE}


class CanaryError(Exception):
    """A usage or input problem the command reports with exit status 2."""


def rpc(url, method, params=None, timeout=20.0):
    """Calls one JSON-RPC method on `url` and returns its result, or raises."""
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []})
    request = urllib.request.Request(url, body.encode(), {"content-type": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        reply = json.load(response)
    if reply.get("error"):
        raise RuntimeError(f"{method}: {reply['error']}")
    return reply["result"]


def parse_nodes(values):
    """Returns `{name: url}` from `NAME=URL` arguments, refusing repeats."""
    nodes = {}
    for value in values:
        name, sep, url = value.partition("=")
        if not sep or not name or not url:
            raise CanaryError(f"expected NAME=URL, got {value!r}")
        if name in nodes:
            raise CanaryError(f"node {name!r} given twice")
        nodes[name] = url
    if len(nodes) < 2:
        raise CanaryError("give at least two nodes")
    return nodes


def read_pool(url, call=rpc, clock=time.time):
    """Reads `url`'s tip, then its pool, then its tip again.

    Returns `(tip, txids, at, err)`: the pool read's midpoint and half its
    length, in seconds, bound when the node answered. `tip` and `txids` are
    `None` when the tip moved between the two tip reads, since the pool may
    then belong to either tip.
    """
    before = call(url, "getbestblockhash")
    sent = clock()
    pool = call(url, "getrawmempool")
    answered = clock()
    after = call(url, "getbestblockhash")
    at, err = round((sent + answered) / 2, 3), round((answered - sent) / 2, 3)
    if before != after:
        return None, None, at, err
    return before, sorted(pool), at, err


def read_template(url, call=rpc, clock=time.time):
    """Returns `url`'s block template as `{prev, txids, fees, t, t_err}`, with
    the read's midpoint and half its length as `read_pool` gives them."""
    sent = clock()
    template = call(url, "getblocktemplate", [], timeout=30.0)
    answered = clock()
    transactions = template.get("transactions", [])
    return {
        "prev": template.get("previousblockhash"),
        "txids": sorted(tx["hash"] for tx in transactions),
        "fees": sum(tx.get("fee", 0) for tx in transactions),
        "t": round((sent + answered) / 2, 3),
        "t_err": round((answered - sent) / 2, 3),
    }


def sample_round(nodes, run, round_index, with_templates, call=rpc):
    """Reads every node at once and returns one row per node for this round.

    Pool reads go first, then, once every node answered, template reads, again
    all at once, so a slow pool read doesn't push one node's template later
    than the others'. `run` names the `sample` invocation, so rounds from runs
    appended to one file never pair with each other.
    """

    def one(name):
        row = {"run": run, "round": round_index, "node": name}
        started = time.time()
        try:
            tip, txids, row["t"], row["t_err"] = read_pool(nodes[name], call)
        except Exception as error:  # A node that is down or restarting.
            row["t"] = round(started, 3)
            row["error"] = str(error)[:200]
            return row
        if tip is None:
            row["tip_moved"] = True
        else:
            row["tip"], row["txids"] = tip, txids
        return row

    def template(row):
        # Kept apart, so a failed template read leaves the pool read's time as
        # it was. A node that didn't answer its pool read isn't asked.
        if with_templates and "error" not in row:
            try:
                row["template"] = read_template(nodes[row["node"]], call)
            except Exception as error:
                row["template_error"] = str(error)[:200]
        return row

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(nodes)) as pool:
        rows = list(pool.map(one, nodes))
        return list(pool.map(template, rows))


def cmd_sample(args):
    """Runs `sample`: polls the nodes for `--duration` seconds into `--out`."""
    nodes = parse_nodes(args.node)
    deadline = time.time() + args.duration
    run = f"{time.time():.3f}"
    with open(args.out, "a", buffering=1) as out:
        for round_index in itertools.count():
            started = time.time()
            if started >= deadline:
                break
            with_templates = args.templates_every > 0 and round_index % args.templates_every == 0
            for row in sample_round(nodes, run, round_index, with_templates):
                out.write(json.dumps(row, separators=(",", ":")) + "\n")
            time.sleep(max(0.0, args.interval - (time.time() - started)))
    return EXIT_PASS


def load_rows(path):
    """Returns every row of a `sample` output file, skipping a torn last line."""
    rows = []
    with open(path) as lines:
        for line in lines:
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def by_round(rows):
    """Groups sample rows as `{(run, round): {node: row}}`."""
    rounds = {}
    for row in rows:
        rounds.setdefault((row.get("run"), row["round"]), {})[row["node"]] = row
    return rounds


def pair_overlap(rounds, a, b, max_skew):
    """Scores `a` against `b` over every round where both read the same tip.

    A pair counts only when both nodes read a stable tip, the same one, and
    their pool reads were at most `max_skew` seconds apart, allowing for how
    long each read took. The score is the share of the two pools they have in
    common. Pairs where both pools are empty are counted apart, since they
    agree without saying anything.
    """
    scores, empty, skipped = [], 0, 0
    for nodes in rounds.values():
        x, y = nodes.get(a), nodes.get(b)
        if not x or not y or "txids" not in x or "txids" not in y:
            skipped += 1
            continue
        apart = abs(x["t"] - y["t"]) + x.get("t_err", 0.0) + y.get("t_err", 0.0)
        if x["tip"] != y["tip"] or apart > max_skew:
            skipped += 1
            continue
        pool_a, pool_b = set(x["txids"]), set(y["txids"])
        union = pool_a | pool_b
        if not union:
            empty += 1
            continue
        scores.append(len(pool_a & pool_b) / len(union))
    return {
        "pairs": len(scores),
        "empty_pairs": empty,
        "skipped": skipped,
        # Exact, since the grade compares it with a floor.
        "mean": statistics.fmean(scores) if scores else None,
        "p10": round(quantile(scores, 0.10), 4) if scores else None,
        "exact": sum(1 for score in scores if score == 1.0),
    }


def quantile(values, q):
    """Returns the `q` quantile of `values` by nearest rank: the smallest value
    with at least a `q` share of the values at or below it."""
    ordered = sorted(values)
    return ordered[max(0, math.ceil(q * len(ordered)) - 1)]


def answered_shares(rounds, names, answered):
    """Returns `{name: share}`, the share of `rounds` in which each node
    answered, as `answered(row)` judges its row. A node with no row in a round
    did not answer it."""
    return {
        name: sum(1 for nodes in rounds if name in nodes and answered(nodes[name]))
        / len(rounds)
        for name in names
    } if rounds else {name: None for name in names}


def missed_too_often(report, shares, canary, max_missed):
    """Fails `report` when the canary answered less often than every
    neighbour, by more than `max_missed`. Its other grades only use rounds
    it answered, so without this a canary that stopped answering after
    reaching the minimum would still pass. Returns whether it failed."""
    report["answered"] = shares
    floor = min(share for name, share in shares.items() if name != canary) - max_missed
    if shares[canary] < floor:
        report["verdict"] = "fail"
        report["reason"] = (f"the canary answered {shares[canary]:.2%} of rounds, below "
                            f"the neighbours' lowest less --max-missed, {floor:.2%}")
        return True
    return False


def grade_overlap(rows, canary, max_skew, min_pairs, tolerance, max_missed=0.01):
    """Grades the canary's pool overlap against its neighbours' with each other.

    Passes when every canary pair's mean score is at least the lowest
    neighbour pair's mean minus `tolerance`. Any pair with fewer than
    `min_pairs` valid pairs makes the result inconclusive. A canary that
    answered its pool reads less often fails. See `missed_too_often`.
    """
    rounds = by_round(rows)
    names = sorted({row["node"] for row in rows})
    if canary not in names:
        raise CanaryError(f"no rows for canary {canary!r}")
    neighbours = [name for name in names if name != canary]
    if len(neighbours) < 2:
        raise CanaryError("overlap needs the canary and at least two neighbours")
    report = {"canary": canary, "pairs": {}}
    for a, b in itertools.combinations(names, 2):
        report["pairs"][f"{a}~{b}"] = pair_overlap(rounds, a, b, max_skew)
    canary_pairs = [v for k, v in report["pairs"].items() if canary in k.split("~")]
    neighbour_pairs = [v for k, v in report["pairs"].items() if canary not in k.split("~")]
    if any(pair["pairs"] < max(1, min_pairs) for pair in report["pairs"].values()):
        report["verdict"] = "inconclusive"
        report["reason"] = f"a pair has fewer than {min_pairs} valid pairs"
        return report
    shares = answered_shares(list(rounds.values()), names, lambda row: "error" not in row)
    if missed_too_often(report, shares, canary, max_missed):
        return report
    floor = min(pair["mean"] for pair in neighbour_pairs) - tolerance
    report["floor"] = floor
    report["verdict"] = "pass" if all(p["mean"] >= floor for p in canary_pairs) else "fail"
    return report


def grade_templates(rows, canary, min_pairs, tolerance, max_missed=0.01, max_skew=3.0):
    """Grades the canary's templates against each neighbour's at the same tip.

    For each neighbour it pairs rounds where both templates build on the same
    previous block, and compares transaction sets and total fees. Pairs where
    both templates are empty are counted apart, since they can't show a
    canary leaving transactions out. Like `pair_overlap`, two templates read
    more than `max_skew` seconds apart, allowing for how long each read took,
    don't pair, since transactions that arrived between them would read as
    fees one left out. Passes when, over the pairs, the
    canary's median fee total is at least `1 - tolerance` of every
    neighbour's. A canary that answered template reads less often fails. See
    `missed_too_often`.
    """
    rounds = by_round(rows)
    names = sorted({row["node"] for row in rows})
    if canary not in names:
        raise CanaryError(f"no rows for canary {canary!r}")
    report = {"canary": canary, "neighbours": {}}
    for neighbour in (name for name in names if name != canary):
        fees, neighbour_fees, scores, skipped, empty = [], [], [], 0, 0
        for nodes in rounds.values():
            x = nodes.get(canary, {}).get("template")
            y = nodes.get(neighbour, {}).get("template")
            if not x or not y or x["prev"] != y["prev"]:
                skipped += 1
                continue
            # A template with no read time, from an older `sample`, can't
            # show the two reads lined up, so it pairs with nothing.
            if "t" not in x or "t" not in y:
                skipped += 1
                continue
            apart = abs(x["t"] - y["t"]) + x.get("t_err", 0.0) + y.get("t_err", 0.0)
            if apart > max_skew:
                skipped += 1
                continue
            set_x, set_y = set(x["txids"]), set(y["txids"])
            union = set_x | set_y
            if not union:
                empty += 1
                continue
            scores.append(len(set_x & set_y) / len(union))
            fees.append(x["fees"])
            neighbour_fees.append(y["fees"])
        median, neighbour_median = (
            (statistics.median(fees), statistics.median(neighbour_fees)) if fees else (None, None)
        )
        report["neighbours"][neighbour] = {
            "pairs": len(scores),
            "empty_pairs": empty,
            "skipped": skipped,
            "exact": sum(1 for score in scores if score == 1.0),
            "mean_overlap": round(statistics.fmean(scores), 4) if scores else None,
            "median_fees": median,
            "neighbour_median_fees": neighbour_median,
            # A neighbour whose median template has no fees is matched by any.
            "median_fee_ratio": (None if median is None
                                 else median / neighbour_median if neighbour_median > 0
                                 else 1.0),
        }
    results = report["neighbours"].values()
    if not results or any(result["pairs"] < max(1, min_pairs) for result in results):
        report["verdict"] = "inconclusive"
        report["reason"] = f"a neighbour has fewer than {min_pairs} template pairs"
        return report
    # Only rounds that asked for templates count, which is the rounds where
    # some node has a template read.
    asked = [nodes for nodes in rounds.values()
             if any("template" in row or "template_error" in row for row in nodes.values())]
    shares = answered_shares(asked, names, lambda row: "template" in row)
    if missed_too_often(report, shares, canary, max_missed):
        return report
    good = all(result["median_fee_ratio"] >= 1 - tolerance for result in results)
    report["verdict"] = "pass" if good else "fail"
    return report


def wall_micros(process_trace_id, ts):
    """Returns a trace row's wall-clock time in microseconds since the epoch.

    `process_trace_id` is `<pid>-<unix nanoseconds when ts started>`; see
    `deploy/trace-query/README.md`.
    """
    _, _, start_ns = process_trace_id.partition("-")
    return int(start_ns) // 1000 + int(ts)


def tip_times(db_path, nodes, low, high):
    """Returns `{(node, hash): first wall µs}` for `tip_published` rows in a height range."""
    db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        found = db.execute(
            "SELECT node, process_trace_id, ts, json_extract(row, '$.tip_hash') "
            "FROM trace_rows WHERE tbl = 'chain_tip' AND event = 'tip_published' "
            "AND json_extract(row, '$.tip_height') BETWEEN ? AND ?",
            (low, high),
        ).fetchall()
    finally:
        db.close()
    times = {}
    for node, process_trace_id, ts, block_hash in found:
        if node not in nodes or not process_trace_id or ts is None:
            continue
        wall = wall_micros(process_trace_id, ts)
        key = (node, block_hash)
        times[key] = min(wall, times.get(key, wall))
    return times


def grade_tip_lag(blocks, times, nodes, reference, margin_ms, clock_bound_ms, min_blocks):
    """Grades every node's tip lag behind `reference` over `blocks`.

    `blocks` is `[(height, hash)]` for the window. Every node must have
    published every block, or the window is inconclusive. A lag within
    `clock_bound_ms` counts as none, since the clocks can't tell it apart.
    Passes when each node's p90 lag is at most `margin_ms`. Without a margin
    the window is inconclusive, since nothing says what lag is too much.
    """
    missing = [
        {"height": height, "hash": block_hash, "node": node}
        for height, block_hash in blocks
        for node in nodes
        if (node, block_hash) not in times
    ]
    report = {"reference": reference, "blocks": len(blocks), "missing": missing[:50],
              "missing_count": len(missing), "clock_bound_ms": clock_bound_ms, "nodes": {}}
    for node in nodes:
        if node == reference:
            continue
        lags = sorted(
            (times[(node, h)] - times[(reference, h)]) / 1000
            for _, h in blocks
            if (node, h) in times and (reference, h) in times
        )
        lags = [0.0 if abs(lag) <= clock_bound_ms else lag for lag in lags]
        report["nodes"][node] = {
            "n": len(lags),
            "p50_ms": round(quantile(lags, 0.5), 1) if lags else None,
            # Exact, since the grade compares it with the margin.
            "p90_ms": quantile(lags, 0.9) if lags else None,
            "max_ms": round(max(lags), 1) if lags else None,
        }
    if missing or len(blocks) < min_blocks:
        report["verdict"] = "inconclusive"
        report["reason"] = (f"{len(missing)} missing rows" if missing
                            else f"fewer than {min_blocks} blocks")
        return report
    if margin_ms is None:
        report["verdict"] = "inconclusive"
        report["reason"] = "no --margin-ms, so the lags are reported but not graded"
        return report
    worst = max(result["p90_ms"] for result in report["nodes"].values())
    report["verdict"] = "pass" if worst <= margin_ms else "fail"
    return report


def window_blocks(url, low, high, call=rpc):
    """Returns `[(height, hash)]` for heights `low..=high` on the node at `url`."""
    return [(height, call(url, "getblockhash", [height])) for height in range(low, high + 1)]


def finish(report, out):
    """Prints `report` as JSON, writes it to `out` if given, and returns its exit status."""
    text = json.dumps(report, indent=1)
    print(text)
    if out:
        with open(out, "w") as file:
            file.write(text + "\n")
    return EXIT_CODES[report["verdict"]]


def cmd_overlap(args):
    """Runs `overlap` on a `sample` file."""
    report = grade_overlap(load_rows(args.samples), args.canary, args.max_skew,
                           args.min_pairs, args.tolerance, args.max_missed)
    return finish(report, args.out)


def cmd_templates(args):
    """Runs `templates` on a `sample` file."""
    report = grade_templates(load_rows(args.samples), args.canary, args.min_pairs,
                             args.tolerance, args.max_missed, args.max_skew)
    return finish(report, args.out)


def cmd_tip_lag(args):
    """Runs `tip-lag` on a trace mirror, taking the window's blocks from `--rpc`."""
    nodes = args.node
    if len(set(nodes)) < 2:
        raise CanaryError("give at least two distinct --node names")
    reference = args.reference or nodes[0]
    if reference not in nodes:
        raise CanaryError(f"reference {reference!r} is not a --node")
    if args.from_height > args.to_height:
        raise CanaryError("--from-height is above --to-height")
    blocks = window_blocks(args.rpc, args.from_height, args.to_height)
    times = tip_times(args.db, set(nodes), args.from_height, args.to_height)
    report = grade_tip_lag(blocks, times, nodes, reference, args.margin_ms,
                           args.clock_bound_ms, args.min_blocks)
    return finish(report, args.out)


def build_parser():
    """Returns the command-line parser."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)

    sample = commands.add_parser("sample", help="poll pools and templates into a file")
    sample.add_argument("--node", action="append", required=True, metavar="NAME=URL")
    sample.add_argument("--out", required=True, help="JSON lines file, appended to")
    sample.add_argument("--interval", type=float, default=5.0, help="seconds between rounds")
    sample.add_argument("--duration", type=float, required=True, help="seconds to run")
    sample.add_argument("--templates-every", type=int, default=6,
                        help="read templates every N rounds, 0 for never")
    sample.set_defaults(run=cmd_sample)

    overlap = commands.add_parser("overlap", help="grade pool overlap")
    overlap.add_argument("--samples", required=True)
    overlap.add_argument("--canary", required=True)
    overlap.add_argument("--max-skew", type=float, default=3.0,
                         help="most seconds between two paired reads")
    overlap.add_argument("--min-pairs", type=int, default=100)
    overlap.add_argument("--tolerance", type=float, default=0.0,
                         help="how far below the neighbours' floor the canary may score")
    overlap.add_argument("--max-missed", type=float, default=0.01,
                         help="how much smaller a share of rounds the canary may answer")
    overlap.add_argument("--out")
    overlap.set_defaults(run=cmd_overlap)

    templates = commands.add_parser("templates", help="grade block templates")
    templates.add_argument("--samples", required=True)
    templates.add_argument("--canary", required=True)
    templates.add_argument("--min-pairs", type=int, default=20)
    templates.add_argument("--max-skew", type=float, default=3.0,
                           help="most seconds between two paired template reads")
    templates.add_argument("--tolerance", type=float, default=0.01,
                           help="how far below a neighbour's fees the canary's median may be")
    templates.add_argument("--max-missed", type=float, default=0.01,
                           help="how much smaller a share of rounds the canary may answer")
    templates.add_argument("--out")
    templates.set_defaults(run=cmd_templates)

    tip_lag = commands.add_parser("tip-lag", help="grade tip lag from mirrored traces")
    tip_lag.add_argument("--db", required=True, help="trace_query.py mirror")
    tip_lag.add_argument("--rpc", required=True, help="a node's RPC URL, for the block list")
    tip_lag.add_argument("--node", action="append", required=True,
                         help="a mirrored node name; the first is the reference by default")
    tip_lag.add_argument("--reference")
    tip_lag.add_argument("--from-height", type=int, required=True)
    tip_lag.add_argument("--to-height", type=int, required=True)
    tip_lag.add_argument("--margin-ms", type=float,
                         help="most p90 lag allowed; without it the window is inconclusive")
    tip_lag.add_argument("--clock-bound-ms", type=float, default=0.0,
                         help="the hosts' clock offset bound; smaller lags count as none")
    tip_lag.add_argument("--min-blocks", type=int, default=10)
    tip_lag.add_argument("--out")
    tip_lag.set_defaults(run=cmd_tip_lag)
    return parser


def main(argv=None):
    """Runs one subcommand and returns its exit status."""
    args = build_parser().parse_args(argv)
    try:
        return args.run(args)
    except (CanaryError, OSError, sqlite3.Error, RuntimeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return EXIT_ERROR


if __name__ == "__main__":
    sys.exit(main())
