#!/usr/bin/env python3
"""Classify zakura#1080 withhold evidence from a node log plus poller JSONL.

Node-side evidence comes from the `gbt_withhold` tracing target added by
instrumentation.patch. Three things are derived:

  1. Every withhold, attributed to one of the five exits in the
     getblocktemplate path, and classified sideways / watch_behind / watch_ahead
     by comparing the template's tip height against the best-tip watch height.
  2. Equal-height sideways flips, read directly off the `publish.best_tip`
     sequence: two consecutive best-tip publications at the same height with
     different hashes is a same-height non-finalized reorg, independent of
     anything the RPC did.
  3. How long each withhold actually lasted, measured from the poller: the gap
     between a client's first withheld response and its next served template.
"""

import argparse
import collections
import datetime as dt
import json
import re
import sys

# The negative lookbehind keeps dotted span fields such as `otel.kind="server"`
# from colliding with the probe's own `kind` field.
FIELD = re.compile(r'(?<![\w.])(\w+)=("(?:[^"\\]|\\.)*"|\S+)')
RPC_METHOD = re.compile(r'rpc\.method=(\w+)')
TS = re.compile(r"^(\d{4}-\d{2}-\d{2}T[\d:.]+Z)")

WITHHOLD_SITES = {
    "guard_a", "guard_b",
    "parent_check.tip_moved", "parent_check.parent_changed", "parent_check.revision_stale",
    "saturated", "recovery_recheck", "check_proposal_validity",
}


def parse_ts(line):
    match = TS.match(line)
    if not match:
        return None
    return dt.datetime.strptime(match.group(1), "%Y-%m-%dT%H:%M:%S.%fZ").replace(
        tzinfo=dt.timezone.utc).timestamp()


def parse_node_log(path):
    events = []
    with open(path, errors="replace") as handle:
        for line in handle:
            if "gbt_withhold" not in line:
                continue
            fields = {}
            for key, raw in FIELD.findall(line):
                fields[key] = raw[1:-1] if raw.startswith('"') else raw
            if "site" not in fields:
                continue
            fields["ts"] = parse_ts(line)
            method = RPC_METHOD.search(line)
            fields["rpc_method"] = method.group(1) if method else "-"
            events.append(fields)
    return events


def sideways_flips(events):
    """Same-height best-tip changes, read off the publication sequence."""
    flips = []
    previous = None
    for event in events:
        if event["site"] != "publish.best_tip":
            continue
        current = (event.get("height"), event.get("hash"), event["ts"])
        if previous and current[0] == previous[0] and current[1] != previous[1]:
            flips.append({"height": current[0], "from": previous[1],
                          "to": current[1], "ts": current[2]})
        previous = current
    return flips


def publication_windows(events):
    """Duration of each non-atomic window between the two state publications."""
    windows = []
    pending = None
    for event in events:
        if event["site"] == "publish.non_finalized_state":
            pending = event
        elif event["site"] == "publish.best_tip" and pending is not None:
            if pending["ts"] is not None and event["ts"] is not None:
                windows.append(event["ts"] - pending["ts"])
            pending = None
    return windows


def withhold_durations(poll_path):
    """Per-client gap from the withheld response to the next served template.

    Measured response-to-response. Timing from the request start would fold in the
    long-poll wait, which is time the client is legitimately parked waiting for a
    tip change, not work it lost.
    """
    by_client = collections.defaultdict(list)
    with open(poll_path) as handle:
        for line in handle:
            event = json.loads(line)
            by_client[event["client"]].append(event)

    durations = []
    for events in by_client.values():
        events.sort(key=lambda e: e["ts"])
        streak_start = None
        for event in events:
            if event["outcome"] == "withhold":
                if streak_start is None:
                    streak_start = event["ts"] + event["latency"]
            elif event["outcome"] == "template" and streak_start is not None:
                durations.append(event["ts"] + event["latency"] - streak_start)
                streak_start = None
    return durations


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(fraction * len(ordered)))
    return ordered[index]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node-log", required=True)
    parser.add_argument("--poll-log")
    parser.add_argument("--json-out")
    args = parser.parse_args()

    events = parse_node_log(args.node_log)
    withholds = [e for e in events if e["site"] in WITHHOLD_SITES]
    publications = [e for e in events if e["site"].startswith("publish.")]
    tip_publications = [e for e in publications if e["site"] == "publish.best_tip"]

    by_site = collections.Counter(e["site"] for e in withholds)
    by_kind = collections.Counter(e.get("kind", "-") for e in withholds)
    by_pair = collections.Counter((e["site"], e.get("kind", "-")) for e in withholds)
    by_method = collections.Counter(e["rpc_method"] for e in withholds)
    flips = sideways_flips(events)
    windows = publication_windows(events)

    report = {
        "tip_publications": len(tip_publications),
        "withholds_total": len(withholds),
        "withholds_by_site": dict(by_site),
        "withholds_by_kind": dict(by_kind),
        "withholds_by_site_and_kind": {f"{s}/{k}": n for (s, k), n in by_pair.items()},
        "withholds_by_calling_method": dict(by_method),
        "equal_height_flips_total": len(flips),
        "equal_height_flips": flips[:20],
        "publication_window_ms": {
            "count": len(windows),
            "p50": (percentile(windows, 0.50) or 0) * 1000,
            "p99": (percentile(windows, 0.99) or 0) * 1000,
            "max": (max(windows) if windows else 0) * 1000,
        },
    }

    if args.poll_log:
        durations = withhold_durations(args.poll_log)
        report["client_withhold_seconds"] = {
            "count": len(durations),
            "p50": percentile(durations, 0.50),
            "p90": percentile(durations, 0.90),
            "max": max(durations) if durations else None,
        }

    print(json.dumps(report, indent=2))
    if args.json_out:
        with open(args.json_out, "w") as handle:
            json.dump(report, handle, indent=2)

    if not withholds:
        print("\nNo withholds observed.", file=sys.stderr)


if __name__ == "__main__":
    main()
