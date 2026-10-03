#!/usr/bin/env bash
# Lever A — forward-advance withhold, no reorg involved.
#
# One PoW-disabled node. A long-poll storm runs against it while `generate`
# advances the tip. Any withhold observed here is caused by the tip moving under
# an in-flight template build or between the two state publications, NOT by a
# reorg.
source "$(dirname "$0")/lib.sh"
require_binary

DURATION="${DURATION:-45}"
CLIENTS="${CLIENTS:-16}"
# One serialized generator: two concurrent `generate` calls would themselves
# build competing same-height blocks, which is Lever B's job. Keeping it to one
# means no fork can exist, so every withhold observed here is a forward advance.
GEN_JOBS="${GEN_JOBS:-1}"
LOG="$OUT/lever-a-node.log"
POLL="$OUT/lever-a-poll.jsonl"

mkdir -p "$OUT"
trap stop_all EXIT
stop_node "$LOG"

start_node "$HARNESS/configs/node-a-powoff.toml" "$LOG" 127.0.0.1:18232

# Prime the chain past the configured NU5 height so templates are built on the
# post-upgrade path rather than the trivial early-height one.
rpc 127.0.0.1:18232 generate '[8]' > /dev/null

python3 "$HARNESS/poller/gbt_poll.py" --rpc 127.0.0.1:18232 \
  --clients "$CLIENTS" --duration "$DURATION" --out "$POLL" &
POLLER=$!

GENERATORS=()
for _ in $(seq 1 "$GEN_JOBS"); do
  (
    END=$((SECONDS + DURATION))
    while [ $SECONDS -lt $END ]; do
      rpc 127.0.0.1:18232 generate '[1]' > /dev/null 2>&1
    done
  ) &
  GENERATORS+=($!)
done
wait $POLLER
# Only the generators: a bare `wait` would also block on the node itself.
wait "${GENERATORS[@]}"

stop_node "$LOG"
python3 "$HARNESS/run/analyze.py" --node-log "$LOG" --poll-log "$POLL" \
  --json-out "$OUT/lever-a-report.json"
