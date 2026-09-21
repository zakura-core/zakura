#!/usr/bin/env bash
# Lever D — withhold rate as a function of block interval, at a fixed consumer count.
#
# Lever A's producers both consumed templates and produced blocks, so its two axes
# could not be separated, and its block interval was an output rather than an input.
# Here the source node is the only one calling generate and the target node is the
# only one serving templates, so BLOCK_INTERVAL is a real knob and the tip changes
# reach the measured node over p2p the way they do in production.
source "$(dirname "$0")/lib.sh"
require_binary

INTERVAL="${BLOCK_INTERVAL:-1.0}"   # seconds between blocks at the source
CLIENTS="${CLIENTS:-16}"            # concurrent long-poll consumers on the target
BLOCKS="${BLOCKS:-40}"              # blocks to produce, so runs are comparable by block count
LABEL="${LABEL:-i$INTERVAL}"

SRC_LOG="$OUT/lever-d-source-$LABEL.log"
TGT_LOG="$OUT/lever-d-target-$LABEL.log"
POLL="$OUT/lever-d-poll-$LABEL.jsonl"

mkdir -p "$OUT"
trap stop_all EXIT
stop_node "$SRC_LOG"; stop_node "$TGT_LOG"

start_node "$HARNESS/configs/node-d-source.toml" "$SRC_LOG" 127.0.0.1:18432
start_node "$HARNESS/configs/node-d-target.toml" "$TGT_LOG" 127.0.0.1:18442

height() { rpc "$1" getblockchaininfo '[]' | python3 -c 'import json,sys; print(json.load(sys.stdin)["result"]["blocks"])'; }

# Prime past the configured NU5 height and confirm blocks actually propagate,
# otherwise the target would sit at its own genesis and serve one static parent.
rpc 127.0.0.1:18432 generate '[8]' > /dev/null
for _ in $(seq 1 60); do
  [ "$(height 127.0.0.1:18442)" -ge 8 ] && break
  sleep 1
done
if [ "$(height 127.0.0.1:18442)" -lt 8 ]; then
  echo "blocks are not propagating to the target; aborting" >&2
  exit 1
fi
echo "primed: source=$(height 127.0.0.1:18432) target=$(height 127.0.0.1:18442)"

# Match the poller window to the production window. Any slack past the last block
# is dead time: with nothing moving the tip, every long poll just blocks.
DURATION=$(python3 -c "print(int($BLOCKS * $INTERVAL + 8))")
python3 "$HARNESS/poller/gbt_poll.py" --rpc 127.0.0.1:18442 \
  --clients "$CLIENTS" --duration "$DURATION" --out "$POLL" &
POLLER=$!

sleep 2
for _ in $(seq 1 "$BLOCKS"); do
  rpc 127.0.0.1:18432 generate '[1]' > /dev/null 2>&1
  sleep "$INTERVAL"
done
# Let the last blocks propagate before reading the target height.
sleep 3
echo "produced $BLOCKS blocks: source=$(height 127.0.0.1:18432) target=$(height 127.0.0.1:18442)"

wait $POLLER
stop_node "$SRC_LOG"; stop_node "$TGT_LOG"

LABEL="$LABEL" POLL="$POLL" INTERVAL="$INTERVAL" CLIENTS="$CLIENTS" BLOCKS="$BLOCKS" \
python3 - <<'PYEOF'
import collections, json, os

counts = collections.Counter()
heights = []
for line in open(os.environ["POLL"]):
    event = json.loads(line)
    counts[event["outcome"]] += 1
    if event["outcome"] == "template" and event.get("height") is not None:
        heights.append(event["height"])

observed = max(heights) - min(heights) if heights else 0
withholds = counts["withhold"]
print(json.dumps({
    "label": os.environ["LABEL"],
    "block_interval_s": float(os.environ["INTERVAL"]),
    "consumers": int(os.environ["CLIENTS"]),
    "tip_changes_seen": observed,
    "templates": counts["template"],
    "withholds": withholds,
    "withholds_per_tip_change": round(withholds / observed, 4) if observed else None,
}, indent=2))
PYEOF
