#!/usr/bin/env bash
# Lever E — the fallback-recovery branch.
#
# Neither the saturation exit nor the recovery re-check ever fired in Levers A-D,
# so they are the untested part of #1080, and #1088 deliberately leaves both
# unchanged. The node builds its own templates, so they are valid by construction
# and the branch cannot be reached without forcing a rejection; fault-injection.patch
# adds an env-gated counter that forces N of them.
#
# The tip is held still on purpose. set_parent clears the rejection records on any
# parent change, so rejections only accumulate while the parent stays put.
source "$(dirname "$0")/lib.sh"
require_binary

REJECTIONS="${REJECTIONS:-5}"
CLIENTS="${CLIENTS:-16}"
DURATION="${DURATION:-30}"
LABEL="${LABEL:-r$REJECTIONS}"

LOG="$OUT/lever-e-$LABEL.log"
POLL="$OUT/lever-e-poll-$LABEL.jsonl"

mkdir -p "$OUT"
trap stop_all EXIT
stop_node "$LOG"

GBT_REPRO_FORCE_TEMPLATE_REJECTIONS="$REJECTIONS" \
  start_node "$HARNESS/configs/node-a-powoff.toml" "$LOG" 127.0.0.1:18232
rpc 127.0.0.1:18232 generate '[8]' > /dev/null

# Optional block production. Each tip change calls set_parent, which clears the
# rejection records, so producing blocks re-arms the injection on every new parent
# and is the only way to exercise the recovery re-check repeatedly.
GENERATORS=()
if [ "${GEN_JOBS:-0}" -gt 0 ]; then
  for _ in $(seq 1 "$GEN_JOBS"); do
    ( END=$((SECONDS + DURATION))
      while [ $SECONDS -lt $END ]; do rpc 127.0.0.1:18232 generate '[1]' > /dev/null 2>&1; done ) &
    GENERATORS+=($!)
  done
fi

python3 "$HARNESS/poller/gbt_poll.py" --rpc 127.0.0.1:18232 \
  --clients "$CLIENTS" --duration "$DURATION" --out "$POLL" --no-long-poll
[ ${#GENERATORS[@]} -gt 0 ] && wait "${GENERATORS[@]}"

stop_node "$LOG"

LABEL="$LABEL" POLL="$POLL" LOG="$LOG" REJECTIONS="$REJECTIONS" python3 - <<'PYEOF'
import collections, json, os

outcomes = collections.Counter()
messages = collections.Counter()
for line in open(os.environ["POLL"]):
    event = json.loads(line)
    outcomes[event["outcome"]] += 1
    if event["outcome"] == "withhold":
        messages[event["detail"]] += 1

injected = sum(1 for line in open(os.environ["LOG"], errors="replace")
               if 'site="injected_rejection"' in line)

print(json.dumps({
    "label": os.environ["LABEL"],
    "rejections_requested": int(os.environ["REJECTIONS"]),
    "rejections_actually_injected": injected,
    "templates": outcomes["template"],
    "withholds": outcomes["withhold"],
    "withhold_messages": dict(messages),
}, indent=2))
PYEOF
