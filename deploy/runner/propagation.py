"""The path of each block through the fleet, joined from every node's own record of it.

Each node's profiler (deploy/profiler/) reports, per block, the wall-clock times at
which the node first heard of the block, asked for it, had its bytes, queued it for
the state writer, published it as its tip and told its peers, plus what it exchanged
with each peer it can tell apart. `join` puts the reports of all nodes on one time
axis that starts when the first of them heard of the block.

Two kinds of peer can be told apart. A Zakura peer is written as a label derived
from its node id, which `peer_label` reproduces from the node ids in the node list. A
legacy peer is written under the name its operator gave it.

A node that is allowed to write its peers' addresses also reports, for every attempt
to download a block, the peer's IP address and the software it says it runs. Those
peers are listed by address, except the ones the caller says must stay private: they
are reduced to "a private peer" here, before anything is summarised or returned.
"""

import hashlib
import math
import time

from peer_addresses import canonical_ip

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
# The steps of one hop after the first word of the block, on the sender's clock; the
# receiver's "heard" is added on its own clock.
HOP_STEPS = ("asked", "sent")
# What a sender may have said first about a block: a Zakura node announces the header
# ("announced") before it has the block and says it has the block ("told") after.
FIRST_WORD_STEPS = ("announced", "told")
NAMED_PEER = "name:"
# Where a node first heard of a block when it was not one of ours: a peer outside the
# fleet, or nowhere the node recorded (no announcement of the block was written).
OUTSIDE, UNKNOWN = "outside", "unknown"
# A node log and trace clock that disagree about a block's tip time by more than this
# are not one clock: the node's times are then moved onto its log's.
CLOCK_TRUST_SECONDS = 0.05
# Who served a download, as it may be shown: one of our nodes, a peer outside the fleet
# by its address, a peer whose address must not be shown, or one whose address the
# node did not record.
PEER_FLEET, PEER_OUTSIDE, PEER_PRIVATE, PEER_HIDDEN = "fleet", "outside", "private", "hidden"
DOWNLOADED = "available"
SLOWEST_DOWNLOADS = 50
MAX_OUTSIDE_PEERS = 200
MAX_AGENT_LENGTH = 64


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


def download_peer(download, fleet, fleet_addresses, is_private):
    """Who served one download, holding only what may be shown about that peer.

    A private address is recognised first, so that nothing else a node reported about
    that peer (its software, its Zakura label) is carried any further.
    """
    address = canonical_ip(download.get("addr"))
    if address is not None and is_private(address):
        return {"kind": PEER_PRIVATE}
    name = fleet.get(download.get("peer")) or fleet_addresses.get(address)
    if name is not None:
        return {"kind": PEER_FLEET, "name": name}
    agent = download.get("agent")
    agent = agent[:MAX_AGENT_LENGTH] if isinstance(agent, str) and agent else None
    if address is None:
        return {"kind": PEER_HIDDEN, "agent": agent}
    return {"kind": PEER_OUTSIDE, "ip": address, "agent": agent}


def node_downloads(record, fleet, fleet_addresses, is_private):
    """One node's attempts to download one block, each with the peer that served it."""
    downloads = []
    for download in record.get("downloads") or ():
        if not isinstance(download, dict):
            continue
        took, size = download.get("ms"), download.get("bytes")
        downloads.append(
            {
                **download_peer(download, fleet, fleet_addresses, is_private),
                "stack": download.get("stack"),
                "ms": took if isinstance(took, (int, float)) else None,
                "bytes": size if isinstance(size, int) else None,
                "result": str(download.get("result")),
                "route": download.get("route"),
            }
        )
    return downloads


def outside_announcer(record, fleet, fleet_addresses, is_private):
    """The legacy peer that first told a node of a block, filtered like a download's peer.

    `None` when the node did not record which connection that was or its address.
    """
    peer = record.get("heard_from_peer")
    if not isinstance(peer, dict):
        return None
    return download_peer({"addr": peer.get("addr"), "agent": peer.get("agent")}, fleet, fleet_addresses, is_private)


def median(values):
    ordered = sorted(values)
    return ordered[len(ordered) // 2] if ordered else None


def ninth_decile(values):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, len(ordered) * 9 // 10)] if ordered else None


def seconds(value):
    """`value` as a time in seconds; raises `TypeError` unless it is a finite number."""
    if type(value) not in (int, float) or not math.isfinite(value):
        raise TypeError("a time is a finite number")
    return value


def block_source(record, wall, fleet):
    """Which of our nodes a node first heard of a block from, `OUTSIDE` for any other
    peer, or `UNKNOWN` when the node recorded no announcement of the block."""
    name = fleet.get(record.get("heard_from"))
    if name is not None:
        return name
    return OUTSIDE if "heard" in wall else UNKNOWN


def node_record(record, node, fleet, fleet_addresses, is_private):
    """One block record of a node's report with peers resolved to node names, or `None`
    when it lacks a tip time.

    Raises `KeyError`, `TypeError`, `ValueError` or `AttributeError` for a record that is
    not shaped the way the profiler writes it.
    """
    wall = record.get("wall") or {}
    tip, logged = wall.get("tip"), record.get("t")
    if tip is None:
        return None
    if not isinstance(record["hash"], str) or not isinstance(record.get("height"), (int, type(None))):
        raise TypeError("a block is named by a hash string and an integer height")
    shift, clock = 0.0, "trace"
    if logged is not None and abs(seconds(logged) - seconds(tip)) > CLOCK_TRUST_SECONDS:
        shift, clock = logged - tip, "log"
    peers = {}
    for peer, steps in (record.get("peers") or {}).items():
        name = fleet.get(peer)
        if name is not None and name != node:
            peers.setdefault(name, {}).update({step: seconds(when) + shift for step, when in steps.items()})
    return {
        "height": record.get("height"),
        "path": record.get("path"),
        "reorg": bool(record.get("reorg")),
        "size": record.get("size") if isinstance(record.get("size"), int) else None,
        "clock": clock,
        "from": block_source(record, wall, fleet),
        "from_peer": outside_announcer(record, fleet, fleet_addresses, is_private),
        "wall": {point: seconds(wall[point]) + shift for point in POINTS if point in wall},
        "peers": peers,
        "downloads": node_downloads(record, fleet, fleet_addresses, is_private),
    }


def node_blocks(report, node, fleet, fleet_addresses, is_private):
    """One node's report as `{block hash: its record}`, with peers resolved to node names.

    Times stay in unix seconds. A record whose trace clock disagrees with the node log
    is moved onto the log's clock and marked `clock: "log"`: times within a node run
    are exact, so the log's time for the tip places all of them.

    A record that is not shaped the way the profiler writes it is left out, so that one
    bad record costs only that block on that node, not the whole view.
    """
    blocks = {}
    records = report.get("blocks")
    for record in records if isinstance(records, list) else ():
        try:
            joined = node_record(record, node, fleet, fleet_addresses, is_private)
        except (KeyError, TypeError, ValueError, AttributeError):
            continue
        if joined is not None:
            blocks[record["hash"]] = joined
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
            "downloads": record["downloads"],
            # Only when the node recorded who the outside peer that told it first was.
            **({"from_peer": record["from_peer"]} if record["from_peer"] else {}),
        }
        for node, record in records.items()
    }
    hops = []
    for sender, record in records.items():
        for receiver, steps in record["peers"].items():
            said = [steps[step] for step in FIRST_WORD_STEPS if step in steps]
            if not said:
                continue
            # "told" is the sender's first word of the block to the receiver, so the hop
            # compares like with like: that word and the receiver hearing it.
            hop = {"from": sender, "to": receiver, "told": offset(min(said))}
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
        # In bytes, from whichever node recorded it; `None` when none did.
        "size": next((record["size"] for record in records.values() if record["size"] is not None), None),
        "origin": round(origin, 6),
        "first_node": first,
        "first_from": records[first]["from"],
        # When the last node that recorded the block had it as tip; a node can lack a
        # block even when its profiler answers, so this is not always every node.
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


def summarize_downloads(blocks):
    """`(outside peers, outside software, slowest downloads)` over `blocks`.

    An outside peer is one row per IP address: the software it last said it runs, which
    of our nodes downloaded from it, how many attempts there were and how many failed,
    and the median, 9-in-10 and slowest time of the ones that delivered the block. The
    slowest downloads are single attempts, whoever served them, each with the node that
    made it and the block's height, hash and size.

    The outside software is the same downloads grouped by the software the peer named
    when connecting (`None` when it named none): how many peers ran it, which of our
    nodes used them, and the attempts, failures and times of all their downloads.
    """
    peers, software, attempts = {}, {}, []
    for block in blocks:
        for node, view in block["nodes"].items():
            for download in view["downloads"]:
                if download["ms"] is not None:
                    attempts.append(
                        {"node": node, "height": block["height"], "hash": block["hash"], "size": block["size"],
                         **download}
                    )  # fmt: skip
                if download["kind"] != PEER_OUTSIDE:
                    continue
                peer = peers.setdefault(
                    download["ip"],
                    {"ip": download["ip"], "agent": None, "nodes": set(), "downloads": 0, "failed": 0,
                     "times": [], "last": 0.0},
                )  # fmt: skip
                # Blocks come newest first, so the first software seen is the latest.
                peer["agent"] = peer["agent"] or download["agent"]
                group = software.setdefault(
                    download["agent"],
                    {"agent": download["agent"], "peers": set(), "nodes": set(), "downloads": 0, "failed": 0,
                     "times": [], "last": 0.0},
                )  # fmt: skip
                group["peers"].add(download["ip"])
                for tally in (peer, group):
                    tally["downloads"] += 1
                    tally["nodes"].add(node)
                    tally["last"] = max(tally["last"], block["origin"])
                    if download["result"] != DOWNLOADED:
                        tally["failed"] += 1
                    elif download["ms"] is not None:
                        tally["times"].append(download["ms"])
    outside = [
        {
            "ip": peer["ip"], "agent": peer["agent"], "nodes": sorted(peer["nodes"]),
            "downloads": peer["downloads"], "failed": peer["failed"],
            "median_ms": median(peer["times"]), "p90_ms": ninth_decile(peer["times"]),
            "max_ms": max(peer["times"], default=None), "last": peer["last"],
        }
        for peer in peers.values()
    ]  # fmt: skip
    # Slowest first; a peer that never delivered a block has no time and comes last.
    outside.sort(key=lambda peer: (peer["p90_ms"] is None, -(peer["p90_ms"] or 0), peer["ip"]))
    versions = [
        {
            "agent": group["agent"], "peers": len(group["peers"]), "nodes": sorted(group["nodes"]),
            "downloads": group["downloads"], "failed": group["failed"],
            "median_ms": median(group["times"]), "p90_ms": ninth_decile(group["times"]),
            "max_ms": max(group["times"], default=None), "last": group["last"],
        }
        for group in software.values()
    ]  # fmt: skip
    # The software most peers run first.
    versions.sort(key=lambda group: (-group["peers"], -group["downloads"], group["agent"] or ""))
    attempts.sort(key=lambda attempt: (-attempt["ms"], attempt["node"]))
    return outside[:MAX_OUTSIDE_PEERS], versions[:MAX_OUTSIDE_PEERS], attempts[:SLOWEST_DOWNLOADS]


def join(reports, node_ids, limit=100, now=None, fleet_addresses=None, is_private=None):
    """The cluster view from `{node name: its profiler's propagation report, or None}`.

    `node_ids` maps every fleet node name to its Zakura node id (empty when it has
    none). The newest `limit` blocks are returned, newest first; a node whose report
    is missing is listed under `unreachable`.

    `fleet_addresses` maps the IP address of each fleet node to its name, so a download
    from one of them is shown under that name. `is_private` says whether an IP address
    must never be shown; such a peer appears only as a private peer, without its
    address or software.
    """
    fleet = fleet_peers(node_ids)
    addresses = {canonical_ip(ip): name for ip, name in (fleet_addresses or {}).items()}
    addresses.pop(None, None)
    is_private = is_private or (lambda ip: False)
    by_hash = {}
    for node, report in reports.items():
        if not isinstance(report, dict):
            continue
        for block_hash, record in node_blocks(report, node, fleet, addresses, is_private).items():
            by_hash.setdefault(block_hash, {})[node] = record
    blocks = [joined_block(block_hash, records) for block_hash, records in by_hash.items()]
    blocks.sort(key=lambda block: (block["height"] or 0, block["origin"]), reverse=True)
    blocks = blocks[:limit]
    outside_peers, outside_software, slowest_downloads = summarize_downloads(blocks)
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
        "outside_peers": outside_peers,
        "outside_software": outside_software,
        "slowest_downloads": slowest_downloads,
    }
