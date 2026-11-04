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

# A miner is active while its newest block is at most this old, and inactive
# when it mined within SEEN_WITHIN_SECONDS but not within ACTIVE_WITHIN_SECONDS.
ACTIVE_WITHIN_SECONDS = 3600
SEEN_WITHIN_SECONDS = 7 * 86400

# A miner gets its own table row when it carries a known pool tag or mined at
# least this share of the blocks (this week or in the baseline); the rest are
# combined into one "other" row.
MAJOR_SHARE = 0.01
OTHER = "other"

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


def miner_activity(last_seen_seconds):
    """active, inactive or gone from the age of a miner's newest block.

    `active`: a block within the last hour. `inactive`: a block within the
    last week but none for over an hour. `gone`: nothing in a week, or never
    seen. A small miner can go an hour without a block by chance; the share
    check (`miner_status`) says whether a gap is statistically unusual.
    """
    if last_seen_seconds is None or last_seen_seconds > SEEN_WITHIN_SECONDS:
        return "gone"
    if last_seen_seconds > ACTIVE_WITHIN_SECONDS:
        return "inactive"
    return "active"


def _latest(*entries):
    """The (lastHeight, lastTime) of the newest block among share entries."""
    best = (None, None)
    for entry in entries:
        if entry and entry.get("lastHeight") is not None and (best[0] is None or entry["lastHeight"] > best[0]):
            best = (entry["lastHeight"], entry.get("lastTime"))
    return best


def is_pool(miner):
    """Whether a miner id names a known pool (from POOL_TAGS) rather than a solo tag, an address or a shielded group."""
    return miner in {name for _, name in POOL_TAGS}


def is_major(miner, baseline, week):
    """Whether a miner is a known pool or mined at least MAJOR_SHARE of the baseline or this week's blocks."""
    if is_pool(miner):
        return True
    for shares in (baseline, week):
        entry = shares["miners"].get(miner)
        if entry and entry["share"] >= MAJOR_SHARE:
            return True
    return False


def combine_rows(rows, now):
    """One table row standing for `rows`: counts and shares added up, the newest block of any of them."""
    last_height, last_time = None, None
    for row in rows:
        if row["lastHeight"] is not None and (last_height is None or row["lastHeight"] > last_height):
            last_height = row["lastHeight"]
            last_time = now - row["lastSeenSeconds"]
    last_seen = (now - last_time) if last_time is not None else None
    return {
        "miner": OTHER,
        "miners": len(rows),
        "baselineShare": sum(row["baselineShare"] or 0.0 for row in rows),
        "baselineBlocks": sum(row["baselineBlocks"] for row in rows),
        "recentShare": sum(row["recentShare"] for row in rows),
        "recentBlocks": sum(row["recentBlocks"] for row in rows),
        "weekBlocks": sum(row["weekBlocks"] for row in rows),
        "lastHeight": last_height,
        "lastSeenSeconds": last_seen,
        "activity": miner_activity(last_seen),
        "status": OTHER,
    }


def miner_table(baseline, recent, week, now):
    """Rows for the miners table: the major miners, baseline ones first by share, then the rest combined.

    `week` holds the shares over the last SEEN_WITHIN_SECONDS, so every major
    miner seen in that time has a row and an activity; minor miners (see
    `is_major`) are added up into one "other" row at the end.
    """
    rows = all_miner_rows(baseline, recent, week, now)
    major = [row for row in rows if is_major(row["miner"], baseline, week)]
    minor = [row for row in rows if not is_major(row["miner"], baseline, week)]
    return major + ([combine_rows(minor, now)] if minor else [])


def all_miner_rows(baseline, recent, week, now):
    """A row per miner: baseline miners first by share, then the rest by blocks this week."""
    empty = {"blocks": 0, "share": 0.0, "lastHeight": None, "lastTime": None}
    rows = []
    seen = set()

    def row(miner, base, current, weekly, status):
        last_height, last_time = _latest(weekly, current, base)
        last_seen = (now - last_time) if last_time else None
        return {
            "miner": miner,
            "baselineShare": base["share"] if base else None,
            "baselineBlocks": base["blocks"] if base else 0,
            "recentShare": current.get("share") or 0.0,
            "recentBlocks": current["blocks"],
            "weekBlocks": weekly["blocks"],
            "lastHeight": last_height,
            "lastSeenSeconds": last_seen,
            "activity": miner_activity(last_seen),
            "status": status,
        }

    for miner, entry in sorted(baseline["miners"].items(), key=lambda item: -item[1]["blocks"]):
        seen.add(miner)
        current = recent["miners"].get(miner, empty)
        rows.append(row(miner, entry, current, week["miners"].get(miner, empty),
                        miner_status(entry["share"], current["blocks"], recent["blocks"])))
    others = {**week["miners"], **{m: e for m, e in recent["miners"].items() if m not in week["miners"]}}
    for miner, weekly in sorted(others.items(), key=lambda item: -item[1]["blocks"]):
        if miner in seen:
            continue
        current = recent["miners"].get(miner, empty)
        rows.append(row(miner, None, current, week["miners"].get(miner, empty), "new"))
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
