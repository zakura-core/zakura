#!/usr/bin/env bash
# Lever B — deterministic equal-height sideways flip, with a matched control.
#
# Runs the flip order (expect a same-height best-tip change) and then the reverse
# order (expect none), both with a template build in flight, so the withhold can
# be attributed to the flip rather than to block submission itself.
source "$(dirname "$0")/lib.sh"
require_binary

ROUNDS="${ROUNDS:-10}"
LOG="$OUT/lever-b-node.log"
POLL="$OUT/lever-b-poll.jsonl"
DRIVER="$HARNESS/driver/target/release/gbt-repro-driver"

mkdir -p "$OUT"
if [ ! -x "$DRIVER" ]; then
  echo "building the driver..."
  (cd "$HARNESS/driver" && cargo +1.98.0 build --release)
fi

trap stop_all EXIT
stop_node "$LOG"

start_node "$HARNESS/configs/node-a-powoff.toml" "$LOG" 127.0.0.1:18232
rpc 127.0.0.1:18232 generate '[8]' > /dev/null

python3 "$HARNESS/poller/gbt_poll.py" --rpc 127.0.0.1:18232 \
  --clients "${POLL_CLIENTS:-16}" --duration 120 --out "$POLL" &
POLLER=$!

echo "=== flip order (lesser-hash sibling first) ==="
"$DRIVER" --rpc 127.0.0.1:18232 --order lesser-first --rounds "$ROUNDS" --flip-delay-ms "${FLIP_DELAY_MS:-250}"

echo "=== control order (greater-hash sibling first) ==="
"$DRIVER" --rpc 127.0.0.1:18232 --order greater-first --rounds "$ROUNDS" --flip-delay-ms "${FLIP_DELAY_MS:-250}"

kill $POLLER 2>/dev/null || true
wait $POLLER 2>/dev/null || true

stop_node "$LOG"
python3 "$HARNESS/run/analyze.py" --node-log "$LOG" --poll-log "$POLL" \
  --json-out "$OUT/lever-b-report.json"
