"""Who mined a block, from its coinbase transaction, and whether a miner dropped off.

A block's coinbase input carries an arbitrary byte string after the height
push; pools usually leave a readable tag there. The coinbase outputs pay the
miner, the funding streams and (from NU7) nothing else transparent, so the
largest transparent output that is not a funding stream is the miner's payout
address. Some miners take their payout shielded, leaving only the funding
stream outputs transparent; those blocks are grouped by tag or, failing that,
counted as one anonymous shielded miner.
"""

from __future__ import annotations

import math
import re

PRINTABLE_RUN = re.compile(rb"[\x20-\x7e]{4,}")
MINED_BY = re.compile(r"mined by ([A-Za-z0-9_.-]+)", re.IGNORECASE)

# Pool name by a lowercase substring of its coinbase tag, as seen on mainnet.
POOL_TAGS = [
    ("luxor", "Luxor"),
    ("2miners", "2Miners"),
    ("foundry", "Foundry"),
    ("kupool", "KuPool"),
    ("zecminingpool", "zecminingpool.com"),
    ("sluicey", "Sluicey"),
    ("viabtc", "ViaBTC"),
    ("f2pool", "F2Pool"),
    ("poolin", "Poolin"),
    ("nicehash", "NiceHash"),
    ("herominers", "HeroMiners"),
    ("k1pool", "K1Pool"),
    ("zergpool", "Zergpool"),
    ("solopool", "SoloPool"),
]
ABSENT_PROBABILITY = 0.01
LOW_MIN_EXPECTED = 9.0


def coinbase_tag(scriptsig_hex):
    """The longest readable ASCII run in a coinbase scriptSig after the height push, or None."""
    try:
        raw = bytes.fromhex(scriptsig_hex or "")
    except ValueError:
        return None
    if not raw:
        return None
    push = raw[0]
    body = raw[1 + push:] if push <= 75 else raw
    runs = PRINTABLE_RUN.findall(body)
    if not runs:
        return None
    text = max(runs, key=len).decode("ascii")
    return text.strip(" /\\:;,.|*#$@!\"'()[]{}<>-_=+~`") or None


def miner_payout(vouts, funding_stream_addresses):
    """The address of the largest transparent output that is not a funding stream, or None when shielded."""
    skip = set(funding_stream_addresses or [])
    best = None
    for vout in vouts or []:
        addresses = (vout.get("scriptPubKey") or {}).get("addresses") or vout.get("addresses") or []
        value = vout.get("valueZat")
        if value is None and isinstance(vout.get("value"), (int, float)):
            value = int(round(vout["value"] * 100_000_000))
        if not addresses or value is None or addresses[0] in skip:
            continue
        if best is None or value > best[0]:
            best = (value, addresses[0])
    return best[1] if best else None


def miner_id(tag, address):
    """A stable miner label: a known pool, a solo tag, the payout address, or a shielded bucket."""
    lowered = (tag or "").lower()
    for needle, name in POOL_TAGS:
        if needle in lowered:
            return name
    solo = MINED_BY.search(tag or "")
    if solo:
        return "solo:" + solo.group(1)
    if address:
        return "addr:" + address[:6] + "…" + address[-4:]
    if tag:
        return "shielded:" + tag[:24]
    return "shielded:unknown"


def miner_shares(rows):
    """Blocks, share, and last block per miner over `rows` (dicts with miner_id, height, time)."""
    counts = {}
    total = 0
    for row in rows:
        miner = row.get("miner_id")
        if not miner:
            continue
        total += 1
        entry = counts.setdefault(miner, {"blocks": 0, "lastHeight": None, "lastTime": None})
        entry["blocks"] += 1
        if entry["lastHeight"] is None or row["height"] > entry["lastHeight"]:
            entry["lastHeight"] = row["height"]
            entry["lastTime"] = row.get("time")
    for entry in counts.values():
        entry["share"] = entry["blocks"] / total if total else None
    return {"blocks": total, "miners": counts}


def miner_status(baseline_share, recent_blocks, recent_total):
    """ok, low or absent: whether a miner's recent block count is consistent with its baseline share.

    `absent` needs the chance of mining nothing at the baseline share to be
    under 1%; `low` needs at least 9 expected blocks and an observed count more
    than three standard deviations below it. Small pools are never flagged.
    """
    if not baseline_share or recent_total <= 0:
        return "ok"
    expected = baseline_share * recent_total
    if recent_blocks == 0 and (1.0 - baseline_share) ** recent_total < ABSENT_PROBABILITY:
        return "absent"
    if expected >= LOW_MIN_EXPECTED and recent_blocks < expected - 3.0 * math.sqrt(expected):
        return "low"
    return "ok"


def miner_table(baseline, recent, now):
    """Rows for the miners table, baseline miners first by share, then new miners."""
    rows = []
    seen = set()
    for miner, entry in sorted(baseline["miners"].items(), key=lambda item: -item[1]["blocks"]):
        seen.add(miner)
        current = recent["miners"].get(miner, {"blocks": 0, "share": 0.0, "lastHeight": None, "lastTime": None})
        last_time = current.get("lastTime") or entry.get("lastTime")
        rows.append({
            "miner": miner,
            "baselineShare": entry["share"],
            "baselineBlocks": entry["blocks"],
            "recentShare": current.get("share") or 0.0,
            "recentBlocks": current["blocks"],
            "lastHeight": current.get("lastHeight") or entry.get("lastHeight"),
            "lastSeenSeconds": (now - last_time) if last_time else None,
            "status": miner_status(entry["share"], current["blocks"], recent["blocks"]),
        })
    for miner, entry in sorted(recent["miners"].items(), key=lambda item: -item[1]["blocks"]):
        if miner in seen:
            continue
        rows.append({
            "miner": miner, "baselineShare": None, "baselineBlocks": 0,
            "recentShare": entry["share"], "recentBlocks": entry["blocks"],
            "lastHeight": entry["lastHeight"],
            "lastSeenSeconds": (now - entry["lastTime"]) if entry.get("lastTime") else None,
            "status": "new",
        })
    return rows


def miner_buckets(rows, bucket_seconds, top=8):
    """Blocks per miner per time bucket for a stacked chart: the top miners by count, the rest as 'other'."""
    totals = {}
    for row in rows:
        if row.get("miner_id"):
            totals[row["miner_id"]] = totals.get(row["miner_id"], 0) + 1
    ranked = [miner for miner, _ in sorted(totals.items(), key=lambda item: -item[1])]
    keep = ranked[:top]
    buckets = {}
    for row in rows:
        miner = row.get("miner_id")
        if not miner:
            continue
        label = miner if miner in keep else "other"
        start = row["time"] - row["time"] % bucket_seconds
        buckets.setdefault(start, {})
        buckets[start][label] = buckets[start].get(label, 0) + 1
    starts = sorted(buckets)
    series = keep + (["other"] if len(ranked) > top else [])
    return {
        "bucketSeconds": bucket_seconds,
        "starts": starts,
        "miners": series,
        "counts": {miner: [buckets[start].get(miner, 0) for start in starts] for miner in series},
    }
