"""The path of each block through the fleet, joined from every node's own record of it.

Each node's profiler (deploy/profiler/) reports, per block, the wall-clock times at
which the node first heard of the block, asked for it, had its bytes, queued it for
the state writer, published it as its tip and told its peers, plus what it exchanged
with each peer it can tell apart. `join` puts the reports of all nodes on one time
axis that starts when the first of them heard of the block.

Two kinds of peer can be told apart. A Zakura peer is written as a label derived
from its node id, which `peer_label` reproduces from the node ids in the node list. A
legacy peer is written under the name its operator gave it.
"""

import hashlib
import time

# The points on a block's way through one node, in order.
POINTS = ("heard", "asked", "body", "queued", "tip", "told")
# What a node did between two of those points.
INTERVALS = (
    ("wait_to_ask", "heard", "asked"),
    ("download", "asked", "body"),
    ("verify", "body", "queued"),
    ("commit", "queued", "tip"),
    ("tell_peers", "tip", "told"),
)
# The steps of one hop, the first three on the sender's clock and the last on the
# receiver's.
HOP_STEPS = ("told", "asked", "sent")
NAMED_PEER = "name:"
OUTSIDE = "outside"
# A node log and trace clock that disagree about a block's tip time by more than this
# are not one clock: the node's times are then moved onto its log's.
CLOCK_TRUST_SECONDS = 0.05


def peer_label(node_id_hex):
    """The label a node writes in trace rows for the Zakura peer with this node id.

    Mirrors `peer_label` in crates/zakura-network/src/zakura/trace.rs.
    """
    digest = hashlib.blake2b(bytes.fromhex(node_id_hex), digest_size=8, person=b"zakura-peer-lbl")
    return "peer:" + digest.hexdigest()


def fleet_peers(node_ids):
    """How a trace names each fleet node, from `{node name: node id or ""}`."""
    names = {NAMED_PEER + name: name for name in node_ids}
    for name, node_id in node_ids.items():
        # A node id is 32 bytes; a node without the Zakura stack has none.
        if len(node_id) != 64:
            continue
        try:
            names[peer_label(node_id)] = name
        except ValueError:
            continue
    return names


def median(values):
    ordered = sorted(values)
    return ordered[len(ordered) // 2] if ordered else None


def ninth_decile(values):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, len(ordered) * 9 // 10)] if ordered else None


def node_blocks(report, node, fleet):
    """One node's report as `{block hash: its record}`, with peers resolved to node names.

    Times stay in unix seconds. A record whose trace clock disagrees with the node log
    is moved onto the log's clock and marked `clock: "log"`: times within a node run
    are exact, so the log's time for the tip places all of them.
    """
    blocks = {}
    for record in report.get("blocks", ()):
        wall = record.get("wall") or {}
        tip, logged = wall.get("tip"), record.get("t")
        if tip is None:
            continue
        shift, clock = 0.0, "trace"
        if logged is not None and abs(logged - tip) > CLOCK_TRUST_SECONDS:
            shift, clock = logged - tip, "log"
        peers = {}
        for peer, steps in (record.get("peers") or {}).items():
            name = fleet.get(peer)
            if name is not None and name != node:
                peers.setdefault(name, {}).update({step: when + shift for step, when in steps.items()})
        blocks[record["hash"]] = {
            "height": record.get("height"),
            "path": record.get("path"),
            "reorg": bool(record.get("reorg")),
            "clock": clock,
            "from": fleet.get(record.get("heard_from"), OUTSIDE),
            "wall": {point: wall[point] + shift for point in POINTS if point in wall},
            "peers": peers,
        }
    return blocks


def joined_block(block_hash, records):
    """One block across the nodes that recorded it; times in ms from its first sighting."""
    # A node that never recorded hearing of the block is placed by the earliest point it has.
    earliest = {node: min(record["wall"].values()) for node, record in records.items()}
    origin = min(earliest.values())

    def offset(when):
        return round((when - origin) * 1000, 3)

    nodes = {
        node: {
            "path": record["path"],
            "clock": record["clock"],
            "from": record["from"],
            "at": {point: offset(when) for point, when in record["wall"].items()},
        }
        for node, record in records.items()
    }
    hops = []
    for sender, record in records.items():
        for receiver, steps in record["peers"].items():
            if "told" not in steps:
                continue
            hop = {"from": sender, "to": receiver}
            hop.update({step: offset(steps[step]) for step in HOP_STEPS if step in steps})
            heard = records.get(receiver, {}).get("peers", {}).get(sender, {}).get("heard")
            if heard is not None:
                hop["heard"] = offset(heard)
            hops.append(hop)
    tips = [view["at"]["tip"] for view in nodes.values()]
    first = min(earliest, key=earliest.get)
    return {
        "hash": block_hash,
        "height": next(iter(records.values()))["height"],
        "reorg": any(record["reorg"] for record in records.values()),
        "origin": round(origin, 6),
        "first_node": first,
        "first_from": records[first]["from"],
        "spread_ms": max(tips),
        "nodes": nodes,
        "hops": sorted(hops, key=lambda hop: (hop["told"], hop["from"], hop["to"])),
    }


def summarize(blocks, node_names):
    """Medians over `blocks`: per node, and per pair of nodes with a hop both ends recorded."""
    per_node = {}
    for node in node_names:
        views = [block["nodes"][node] for block in blocks if node in block["nodes"]]
        if not views:
            continue
        behind = [view["at"]["tip"] for view in views]
        intervals = {}
        for name, start, end in INTERVALS:
            spans = [
                view["at"][end] - view["at"][start]
                for view in views
                if start in view["at"] and end in view["at"] and view["at"][end] >= view["at"][start]
            ]
            if spans:
                intervals[name] = {"median_ms": round(median(spans), 3), "p90_ms": round(ninth_decile(spans), 3)}
        per_node[node] = {
            "blocks": len(views),
            "first": sum(1 for block in blocks if block["first_node"] == node),
            "tip_median_ms": median(behind),
            "tip_p90_ms": ninth_decile(behind),
            "intervals": intervals,
        }
    pairs = {}
    for block in blocks:
        for hop in block["hops"]:
            if "heard" in hop:
                pairs.setdefault((hop["from"], hop["to"]), []).append(hop["heard"] - hop["told"])
    return {
        "nodes": per_node,
        "pairs": [
            {"from": sender, "to": receiver, "hops": len(times), "median_ms": round(median(times), 3)}
            for (sender, receiver), times in sorted(pairs.items())
        ],
    }


def join(reports, node_ids, limit=100, now=None):
    """The cluster view from `{node name: its profiler's propagation report, or None}`.

    `node_ids` maps every fleet node name to its Zakura node id (empty when it has
    none). The newest `limit` blocks are returned, newest first; a node whose report
    is missing is listed under `unreachable`.
    """
    fleet = fleet_peers(node_ids)
    by_hash = {}
    for node, report in reports.items():
        if not isinstance(report, dict):
            continue
        for block_hash, record in node_blocks(report, node, fleet).items():
            by_hash.setdefault(block_hash, {})[node] = record
    blocks = [joined_block(block_hash, records) for block_hash, records in by_hash.items()]
    blocks.sort(key=lambda block: (block["height"] or 0, block["origin"]), reverse=True)
    blocks = blocks[:limit]
    return {
        "generated_at": round(time.time() if now is None else now, 3),
        "nodes": list(node_ids),
        "unreachable": sorted(node for node, report in reports.items() if not isinstance(report, dict)),
        "clocks": {
            node: report.get("clock") for node, report in reports.items() if isinstance(report, dict)
        },
        "points": list(POINTS),
        "intervals": [name for name, _, _ in INTERVALS],
        "blocks": blocks,
        "summary": summarize(blocks, list(node_ids)),
    }
