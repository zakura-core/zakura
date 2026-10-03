# Shared helpers for the getblocktemplate withhold repro levers.
set -euo pipefail

HARNESS="${HARNESS:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
REPO="${REPO:-$(git -C "$HARNESS" rev-parse --show-toplevel)}"
# Override ZAKURAD to measure another build, which is how two branches are compared.
ZAKURAD="${ZAKURAD:-$REPO/target/release/zakurad}"
OUT="${OUT:-$HARNESS/out}"

rpc() { # rpc <addr> <method> <params-json>
  curl -s --max-time 30 -H 'Content-Type: application/json' \
    --data "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$2\",\"params\":$3}" \
    "http://$1"
}

start_node() { # start_node <config> <log> <rpc-addr>
  local config="$1" log="$2" addr="$3"
  "$ZAKURAD" -c "$config" start > "$log" 2>&1 &
  echo $! > "$log.pid"
  for _ in $(seq 1 120); do
    if rpc "$addr" getblockchaininfo '[]' | grep -q '"result"'; then
      echo "node up: $config (rpc $addr, pid $(cat "$log.pid"))"
      return 0
    fi
    sleep 1
  done
  echo "node failed to come up; last 40 log lines:" >&2
  tail -40 "$log" >&2
  return 1
}

stop_node() { # stop_node <log>
  local pidfile="$1.pid"
  [ -f "$pidfile" ] || return 0
  kill "$(cat "$pidfile")" 2>/dev/null || true
  wait "$(cat "$pidfile")" 2>/dev/null || true
  rm -f "$pidfile"
}

stop_all() {
  for pidfile in "$OUT"/*.log.pid; do
    [ -e "$pidfile" ] || continue
    stop_node "${pidfile%.pid}"
  done
}

require_binary() {
  if [ ! -x "$ZAKURAD" ]; then
    echo "missing zakurad at $ZAKURAD" >&2
    echo "build it with: cargo build --release -p zakura --features internal-miner" >&2
    exit 1
  fi
}
