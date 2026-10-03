#!/usr/bin/env bash
# Lever C — two genuinely mining nodes racing for the same heights.
#
# Both nodes run the internal Equihash solver on Regtest and start with no peers,
# so each mines its own independent chain. `addnode` then joins them, and every
# height they both mined becomes an equal-work competitor that has to be resolved
# by the raw tip-hash tie-break while getblocktemplate is being served.
source "$(dirname "$0")/lib.sh"
require_binary

CYCLES="${CYCLES:-3}"
MINE_SECONDS="${MINE_SECONDS:-25}"
JOIN_SECONDS="${JOIN_SECONDS:-25}"

mkdir -p "$OUT"
trap stop_all EXIT

height() { rpc "$1" getblockchaininfo '[]' | python3 -c 'import json,sys; print(json.load(sys.stdin)["result"]["blocks"])'; }

for cycle in $(seq 1 "$CYCLES"); do
  echo "=== cycle $cycle ==="
  LOG1="$OUT/lever-c-c1-$cycle.log"
  LOG2="$OUT/lever-c-c2-$cycle.log"
  POLL="$OUT/lever-c-poll-$cycle.jsonl"

  start_node "$HARNESS/configs/node-c1.toml" "$LOG1" 127.0.0.1:18332
  start_node "$HARNESS/configs/node-c2.toml" "$LOG2" 127.0.0.1:18342

  # A long-poll client on c1, so the withhold is observed the way a real miner
  # would see it and not only in the node's own logs.
  python3 "$HARNESS/poller/gbt_poll.py" --rpc 127.0.0.1:18332 --clients 8 \
    --duration $((MINE_SECONDS + JOIN_SECONDS)) --out "$POLL" &
  POLLER=$!

  echo "  partitioned mining for ${MINE_SECONDS}s..."
  sleep "$MINE_SECONDS"
  echo "  heights before join: c1=$(height 127.0.0.1:18332) c2=$(height 127.0.0.1:18342)"

  echo "  joining the partition"
  rpc 127.0.0.1:18342 addnode '["127.0.0.1:18333", "add"]'
  echo

  sleep "$JOIN_SECONDS"
  echo "  heights after join:  c1=$(height 127.0.0.1:18332) c2=$(height 127.0.0.1:18342)"

  wait $POLLER 2>/dev/null || true
  stop_node "$LOG1"
  stop_node "$LOG2"

  echo "  --- c1 ---"
  python3 "$HARNESS/run/analyze.py" --node-log "$LOG1" --poll-log "$POLL" \
    --json-out "$OUT/lever-c-c1-$cycle.json" | python3 -c '
import json,sys
report = json.load(sys.stdin)
report.pop("equal_height_flips", None)
print(json.dumps(report, indent=2))'
done
