#!/usr/bin/env bash
# Compare one zakurad build against another on the two workloads that withhold.
#
# Usage: ZAKURAD=/path/to/zakurad ./run/compare-fix.sh <label>
#
# 1083 turns the withhold into an internal retry, so the client-visible outcome is
# what matters — and it needs no instrumentation to measure. Retries are not free
# though: each one rebuilds the template, so this also records RPC latency and the
# node's CPU time.
source "$(dirname "$0")/lib.sh"
require_binary

LABEL="${1:?usage: compare-fix.sh <label>}"
LOG="$OUT/cmp-$LABEL-node.log"
POLL="$OUT/cmp-$LABEL-poll.jsonl"
DRIVER="$HARNESS/driver/target/release/gbt-repro-driver"
DURATION="${DURATION:-45}"

mkdir -p "$OUT"
trap stop_all EXIT
stop_node "$LOG"

cpu_seconds() { # cpu_seconds <pid>
  awk '{print ($14 + $15) / '"$(getconf CLK_TCK)"'}' "/proc/$1/stat" 2>/dev/null || echo 0
}

start_node "$HARNESS/configs/node-a-powoff.toml" "$LOG" 127.0.0.1:18232
PID=$(cat "$LOG.pid")
rpc 127.0.0.1:18232 generate '[8]' > /dev/null

echo "--- workload 1: high block rate, 4 concurrent producers, 16 long-poll clients"
CPU_START=$(cpu_seconds "$PID")
python3 "$HARNESS/poller/gbt_poll.py" --rpc 127.0.0.1:18232 \
  --clients 16 --duration "$DURATION" --out "$POLL" &
POLLER=$!
GENERATORS=()
for _ in 1 2 3 4; do
  ( END=$((SECONDS + DURATION))
    while [ $SECONDS -lt $END ]; do rpc 127.0.0.1:18232 generate '[1]' > /dev/null 2>&1; done ) &
  GENERATORS+=($!)
done
wait $POLLER
wait "${GENERATORS[@]}"
CPU_END=$(cpu_seconds "$PID")

echo "--- workload 2: deterministic equal-height flip, 32 builds in flight"
"$DRIVER" --rpc 127.0.0.1:18232 --order lesser-first --rounds 6 \
  > "$OUT/cmp-$LABEL-flip.txt" 2>&1 || echo "  driver reported a failure (see the log)"
"$DRIVER" --rpc 127.0.0.1:18232 --order greater-first --rounds 6 \
  > "$OUT/cmp-$LABEL-control.txt" 2>&1 || echo "  driver reported a failure (see the log)"

HEIGHT=$(rpc 127.0.0.1:18232 getblockchaininfo '[]' | python3 -c 'import json,sys; print(json.load(sys.stdin)["result"]["blocks"])')
stop_node "$LOG"

LABEL="$LABEL" POLL="$POLL" OUT="$OUT" HEIGHT="$HEIGHT" \
CPU_USED="$(echo "$CPU_END - $CPU_START" | bc)" DURATION="$DURATION" \
python3 - <<'PYEOF'
import json, os, re, collections

label, poll, out = os.environ["LABEL"], os.environ["POLL"], os.environ["OUT"]

counts = collections.Counter()
latencies = []
messages = collections.Counter()
for line in open(poll):
    event = json.loads(line)
    counts[event["outcome"]] += 1
    if event["outcome"] == "template":
        latencies.append(event["latency"])
    elif event["outcome"] == "withhold":
        messages[event["detail"]] += 1

def pct(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    return round(ordered[min(len(ordered) - 1, int(fraction * len(ordered)))], 4)

def driver_totals(path):
    served = withheld = 0
    flips = collections.Counter()
    for line in open(path):
        found = re.search(r"in-flight templates: (\d+) served, (\d+) withheld", line)
        if found:
            served += int(found.group(1))
            withheld += int(found.group(2))
        found = re.search(r"sideways flip: (\w+) \(expected (\w+)\)", line)
        if found:
            flips["match" if found.group(1) == found.group(2) else "mismatch"] += 1
    return {"served": served, "withheld": withheld, "flip_rounds": dict(flips)}

report = {
    "label": label,
    "blocks_produced": int(os.environ["HEIGHT"]),
    "node_cpu_seconds": round(float(os.environ["CPU_USED"]), 1),
    "workload1_longpoll": {
        "templates": counts["template"],
        "withholds": counts["withhold"],
        "withhold_messages": dict(messages),
        "latency_p50": pct(latencies, 0.50),
        "latency_p90": pct(latencies, 0.90),
        "latency_max": pct(latencies, 0.999),
    },
    "workload2_flip": driver_totals(f"{out}/cmp-{label}-flip.txt"),
    "workload2_control": driver_totals(f"{out}/cmp-{label}-control.txt"),
}
print(json.dumps(report, indent=2))
with open(f"{out}/cmp-{label}.json", "w") as handle:
    json.dump(report, handle, indent=2)
PYEOF
