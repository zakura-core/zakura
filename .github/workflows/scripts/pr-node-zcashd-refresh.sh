#!/usr/bin/env bash
# Refreshes a cloned zcashd mainnet fixture against the freshly baked pruned
# Zakura tip, then leaves only reusable chain state and provenance on disk.
set -euo pipefail

: "${ZCASHD_VOLUME_NAME:?missing ZCASHD_VOLUME_NAME}"
: "${FIXTURE_STATE_DIR:?missing FIXTURE_STATE_DIR}"
: "${ZCASHD_TX_RETENTION:?missing ZCASHD_TX_RETENTION}"

if ! [[ "$ZCASHD_TX_RETENTION" =~ ^[0-9]+$ ]] || (( ZCASHD_TX_RETENTION == 0 )); then
  echo "ZCASHD_TX_RETENTION must be a positive integer" >&2
  exit 1
fi

DEVICE="/dev/disk/by-id/scsi-0DO_Volume_${ZCASHD_VOLUME_NAME}"
for _ in $(seq 1 60); do [[ -e "$DEVICE" ]] && break; sleep 2; done
[[ -e "$DEVICE" ]] || { echo "zcashd fixture volume device not found: $DEVICE" >&2; exit 1; }

ZCASHD_DIR=/mnt/bake-zcashd-mainnet
mkdir -p "$ZCASHD_DIR"
existing_mount=$(findmnt -rn -S "$DEVICE" -o TARGET | head -n 1 || true)
if [[ -n "$existing_mount" && "$existing_mount" != "$ZCASHD_DIR" ]]; then
  umount "$existing_mount"
fi
mountpoint -q "$ZCASHD_DIR" || mount "$DEVICE" "$ZCASHD_DIR"

ZAKURAD=/root/cargo-target/release/zakurad
CONFIG=/root/zcashd-fixture-zakurad.toml
LOG=/root/zcashd-fixture-refresh.log
PID=""

stop_node() {
  [[ -n "$PID" ]] || return 0
  kill -INT "$PID" 2>/dev/null || true
  for _ in $(seq 1 66); do
    if ! kill -0 "$PID" 2>/dev/null; then
      if ! wait "$PID"; then
        echo "zakurad exited unsuccessfully during supervised zcashd shutdown" >&2
        PID=""
        return 1
      fi
      PID=""
      return 0
    fi
    sleep 5
  done
  echo "zakurad did not finish its supervised zcashd shutdown within 330 seconds" >&2
  return 1
}

cleanup() {
  stop_node || true
  sync
  mountpoint -q "$ZCASHD_DIR" && umount "$ZCASHD_DIR" || true
}
trap cleanup EXIT

for directory in blocks chainstate unity; do
  [[ -d "$ZCASHD_DIR/$directory" ]] || {
    echo "zcashd fixture is missing $directory/" >&2
    exit 1
  }
done
MANIFEST="$ZCASHD_DIR/fixture-manifest.json"
[[ -f "$MANIFEST" ]] || { echo "zcashd fixture manifest is missing" >&2; exit 1; }
jq -e '.schema_version == 1 and .fixture == "zcashd-mainnet" and .network == "mainnet"
       and (.status == "candidate" or .status == "verified")
       and (.tip.height | type == "number") and (.tip.hash | test("^[0-9a-f]{64}$"))' \
  "$MANIFEST" >/dev/null

ZAKURA_MANIFEST="$FIXTURE_STATE_DIR/fixture-manifest.json"
[[ -f "$ZAKURA_MANIFEST" ]] || { echo "Zakura tip fixture manifest is missing" >&2; exit 1; }
ZAKURA_CAPTURED_HEIGHT=$(jq -er '.tip.height // .height' "$ZAKURA_MANIFEST")
ZCASHD_CAPTURED_HEIGHT=$(jq -er '.tip.height' "$MANIFEST")
MANIFEST_TX_RETENTION=$(jq -er '.tx_retention' "$ZAKURA_MANIFEST")
[[ "$ZAKURA_CAPTURED_HEIGHT" =~ ^[0-9]+$ && "$ZCASHD_CAPTURED_HEIGHT" =~ ^[0-9]+$ ]] || {
  echo "fixture manifests contain invalid heights" >&2
  exit 1
}
[[ "$MANIFEST_TX_RETENTION" == "$ZCASHD_TX_RETENTION" ]] || {
  echo "Zakura fixture retention $MANIFEST_TX_RETENTION does not match configured retention $ZCASHD_TX_RETENTION" >&2
  exit 1
}
(( ZCASHD_CAPTURED_HEIGHT <= ZAKURA_CAPTURED_HEIGHT )) || {
  echo "refusing refresh: captured zcashd tip $ZCASHD_CAPTURED_HEIGHT is ahead of Zakura $ZAKURA_CAPTURED_HEIGHT" >&2
  exit 1
}
CAPTURED_LAG=$((ZAKURA_CAPTURED_HEIGHT - ZCASHD_CAPTURED_HEIGHT))
(( CAPTURED_LAG < ZCASHD_TX_RETENTION )) || {
  echo "refusing refresh: captured zcashd lag $CAPTURED_LAG is outside Zakura's $ZCASHD_TX_RETENTION-block pruning retention" >&2
  exit 1
}

AVAILABLE_KIB=$(df --output=avail "$ZCASHD_DIR" | tail -n 1 | tr -d ' ')
(( AVAILABLE_KIB >= 10 * 1024 * 1024 )) || {
  echo "zcashd fixture volume has less than 10 GiB free before refresh" >&2
  exit 1
}

cat > "$CONFIG" <<TOML
[network]
network = "Mainnet"
listen_addr = "127.0.0.1:8233"
cache_dir = "/root/.cache/zakura-fixture-network"
identity_dir = "/root/.zakura-fixture-identity"
p2p_stack = "legacy"

[state]
cache_dir = "${FIXTURE_STATE_DIR}"
storage_mode = { pruned = { tx_retention = ${ZCASHD_TX_RETENTION} } }

[rpc]
listen_addr = "127.0.0.1:18232"
enable_cookie_auth = false

[tracing]
log_file = "${LOG}"
use_color = false

[zcashd_compat]
enabled = true
manage_zcashd = true
zcashd_source = "embedded"
zcashd_datadir = "${ZCASHD_DIR}"
zcashd_extra_args = ["-rpcbind=127.0.0.1", "-rpcallowip=127.0.0.1", "-disablewallet=1"]
shutdown_grace_period = "5m"
TOML

rpc_result() {
  local url="$1" cookie="$2" method="$3" params="${4:-[]}" auth=()
  [[ -z "$cookie" ]] || auth=(--user "$(cat "$cookie")")
  curl --noproxy '*' -fsS "${auth[@]}" \
    -H 'Content-Type: application/json' \
    --data "{\"jsonrpc\":\"1.0\",\"id\":\"fixture\",\"method\":\"${method}\",\"params\":${params}}" \
    "$url" | jq -er 'if .error == null then .result else error(.error | tostring) end'
}

: > "$LOG"
"$ZAKURAD" -c "$CONFIG" start --zcashd-compat >>"$LOG" 2>&1 &
PID=$!

ZCASHD_COOKIE="$ZCASHD_DIR/.cookie"
for _ in $(seq 1 120); do
  kill -0 "$PID" 2>/dev/null || { echo "zakurad exited during fixture startup" >&2; tail -n 200 "$LOG" >&2; exit 1; }
  if rpc_result http://127.0.0.1:18232 '' getblockcount >/dev/null 2>&1 &&
     [[ -f "$ZCASHD_COOKIE" ]] &&
     rpc_result http://127.0.0.1:8232 "$ZCASHD_COOKIE" getblockcount >/dev/null 2>&1; then
    break
  fi
  sleep 5
done
rpc_result http://127.0.0.1:18232 '' getblockcount >/dev/null
rpc_result http://127.0.0.1:8232 "$ZCASHD_COOKIE" getblockcount >/dev/null

# Verify that the claimed seed chain is actually present before accepting any
# blocks synced during this refresh.
CLAIMED_HEIGHT=$(jq -er '.tip.height' "$MANIFEST")
CLAIMED_HASH=$(jq -er '.tip.hash' "$MANIFEST")
OBSERVED_CLAIMED_HASH=$(rpc_result http://127.0.0.1:8232 "$ZCASHD_COOKIE" getblockhash "[$CLAIMED_HEIGHT]")
[[ "$OBSERVED_CLAIMED_HASH" == "$CLAIMED_HASH" ]] || {
  echo "zcashd fixture hash at height $CLAIMED_HEIGHT does not match its manifest" >&2
  exit 1
}

ZCASHD_INITIAL=$(rpc_result http://127.0.0.1:8232 "$ZCASHD_COOKIE" getblockcount)
ZAKURA_INITIAL=$(rpc_result http://127.0.0.1:18232 '' getblockcount)
(( ZCASHD_INITIAL <= ZAKURA_INITIAL )) || {
  echo "refusing refresh: live zcashd tip $ZCASHD_INITIAL is ahead of Zakura $ZAKURA_INITIAL" >&2
  exit 1
}
INITIAL_LAG=$((ZAKURA_INITIAL - ZCASHD_INITIAL))
(( INITIAL_LAG < ZCASHD_TX_RETENTION )) || {
  echo "refusing refresh: live zcashd lag $INITIAL_LAG is outside Zakura's $ZCASHD_TX_RETENTION-block pruning retention" >&2
  exit 1
}

SYNC_TIMEOUT_SECONDS=${ZCASHD_SYNC_TIMEOUT_SECONDS:-10800}
FINAL_MAX_DRIFT=${ZCASHD_FINAL_MAX_DRIFT:-10}
DEADLINE=$(( $(date +%s) + SYNC_TIMEOUT_SECONDS ))
STABLE=0
while (( $(date +%s) < DEADLINE )); do
  kill -0 "$PID" 2>/dev/null || { echo "zakurad exited while refreshing zcashd" >&2; exit 1; }
  ZCASHD_HEIGHT=$(rpc_result http://127.0.0.1:8232 "$ZCASHD_COOKIE" getblockcount)
  ZAKURA_HEIGHT=$(rpc_result http://127.0.0.1:18232 '' getblockcount)
  (( ZCASHD_HEIGHT <= ZAKURA_HEIGHT )) || {
    echo "refusing refresh: zcashd tip $ZCASHD_HEIGHT advanced ahead of Zakura $ZAKURA_HEIGHT" >&2
    exit 1
  }
  FORWARD_LAG=$((ZAKURA_HEIGHT - ZCASHD_HEIGHT))
  (( FORWARD_LAG < ZCASHD_TX_RETENTION )) || {
    echo "zcashd lag reached $FORWARD_LAG blocks, outside the $ZCASHD_TX_RETENTION-block retention window" >&2
    exit 1
  }
  DRIFT=$FORWARD_LAG
  echo "fixture refresh heights: zakurad=$ZAKURA_HEIGHT zcashd=$ZCASHD_HEIGHT drift=$DRIFT"
  if (( DRIFT <= FINAL_MAX_DRIFT )); then
    STABLE=$((STABLE + 1))
    (( STABLE >= 2 )) && break
  else
    STABLE=0
  fi
  sleep 30
done
(( STABLE >= 2 )) || { echo "zcashd fixture did not catch up before timeout" >&2; exit 1; }

ZCASHD_HEIGHT=$(rpc_result http://127.0.0.1:8232 "$ZCASHD_COOKIE" getblockcount)
ZCASHD_HASH=$(rpc_result http://127.0.0.1:8232 "$ZCASHD_COOKIE" getblockhash "[$ZCASHD_HEIGHT]")
ZAKURA_HEIGHT=$(rpc_result http://127.0.0.1:18232 '' getblockcount)
ZAKURA_HASH=$(rpc_result http://127.0.0.1:18232 '' getblockhash "[$ZAKURA_HEIGHT]")
ZAKURA_HASH_AT_ZCASHD_HEIGHT=$(rpc_result http://127.0.0.1:18232 '' getblockhash "[$ZCASHD_HEIGHT]")
FINAL_LAG=$((ZAKURA_HEIGHT - ZCASHD_HEIGHT))
(( FINAL_LAG >= 0 )) || {
  echo "refusing fixture: final zcashd tip $ZCASHD_HEIGHT is ahead of Zakura $ZAKURA_HEIGHT" >&2
  exit 1
}
(( FINAL_LAG < ZCASHD_TX_RETENTION )) || {
  echo "refusing fixture: final zcashd lag $FINAL_LAG is outside Zakura's $ZCASHD_TX_RETENTION-block pruning retention" >&2
  exit 1
}
[[ "$ZAKURA_HASH_AT_ZCASHD_HEIGHT" == "$ZCASHD_HASH" ]] || {
  echo "refusing fixture: Zakura and zcashd are on different chains at height $ZCASHD_HEIGHT" >&2
  exit 1
}

ZAKURAD_VERSION=$("$ZAKURAD" --version | head -n 1)
ZCASHD_BIN=$(find "$FIXTURE_STATE_DIR/zcashd-compat/bin" -type f -name zcashd -perm -111 | sort | tail -n 1)
[[ -n "$ZCASHD_BIN" ]] || { echo "managed zcashd binary was not cached" >&2; exit 1; }
ZCASHD_VERSION=$("$ZCASHD_BIN" --version | head -n 1)
ZCASHD_BINARY_SHA256=$(sha256sum "$ZCASHD_BIN" | awk '{print $1}')

stop_node
pgrep -x zcashd >/dev/null && { echo "zcashd remained running after clean Zakura shutdown" >&2; exit 1; }
for evidence in \
  "Shutdown: main: done" \
  "zcashd-compat zcashd exited cleanly after SIGTERM" \
  "zcashd-compat zcashd child stopped on shutdown" \
  "stopping zakurad"; do
  grep -Fq "$evidence" "$LOG" || {
    echo "clean-shutdown evidence missing from fixture refresh: $evidence" >&2
    exit 1
  }
done

# These are runtime artifacts, not reusable chain state. Keep deletion explicit
# so an unexpected new top-level zcashd artifact fails the bake below.
for runtime_file in .cookie .lock banlist.dat db.log debug.log fee_estimates.dat mempool.dat peers.dat wallet.dat zcash.conf; do
  rm -f "$ZCASHD_DIR/$runtime_file"
done
rm -rf "$ZCASHD_DIR/database" "$ZCASHD_DIR/wallets"

CAPTURED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
TMP_MANIFEST="$ZCASHD_DIR/fixture-manifest.json.tmp"
jq \
  --arg captured_at "$CAPTURED_AT" \
  --arg zcashd_hash "$ZCASHD_HASH" \
  --arg zakura_hash "$ZAKURA_HASH" \
  --arg zakurad_version "$ZAKURAD_VERSION" \
  --arg zcashd_version "$ZCASHD_VERSION" \
  --arg zcashd_binary_sha256 "$ZCASHD_BINARY_SHA256" \
  --arg source_snapshot_id "${ZCASHD_SOURCE_SNAPSHOT_ID:-}" \
  --argjson zcashd_height "$ZCASHD_HEIGHT" \
  --argjson zakura_height "$ZAKURA_HEIGHT" \
  --argjson lag_blocks "$FINAL_LAG" \
  --argjson tx_retention "$ZCASHD_TX_RETENTION" \
  '.captured_at = $captured_at
   | .tip = {height: $zcashd_height, hash: $zcashd_hash}
   | .refresh = {
       captured_at: $captured_at,
       clean_shutdown: true,
       zakura_tip: {height: $zakura_height, hash: $zakura_hash},
       lag_blocks: $lag_blocks,
       tx_retention: $tx_retention,
       zakurad_version: $zakurad_version,
       zcashd_version: $zcashd_version,
       zcashd_binary_sha256: $zcashd_binary_sha256,
       source_snapshot_id: (if $source_snapshot_id == "" then null else $source_snapshot_id end)
     }
   | .contents = ["blocks", "chainstate", "unity", "fixture-manifest.json"]' \
  "$MANIFEST" > "$TMP_MANIFEST"
mv "$TMP_MANIFEST" "$MANIFEST"

ZAKURA_TMP="$FIXTURE_STATE_DIR/fixture-manifest.json.tmp"
jq --arg captured_at "$CAPTURED_AT" --arg hash "$ZAKURA_HASH" --argjson height "$ZAKURA_HEIGHT" \
  '.captured_at = $captured_at | .tip = {height: $height, hash: $hash}' \
  "$ZAKURA_MANIFEST" > "$ZAKURA_TMP"
mv "$ZAKURA_TMP" "$ZAKURA_MANIFEST"

for path in "$ZCASHD_DIR"/* "$ZCASHD_DIR"/.[!.]* "$ZCASHD_DIR"/..?*; do
  [[ -e "$path" ]] || continue
  case "$(basename "$path")" in
    blocks|chainstate|unity|fixture-manifest.json|lost+found) ;;
    *) echo "unexpected top-level zcashd fixture artifact: $path" >&2; exit 1 ;;
  esac
done

AVAILABLE_KIB=$(df --output=avail "$ZCASHD_DIR" | tail -n 1 | tr -d ' ')
(( AVAILABLE_KIB >= 5 * 1024 * 1024 )) || {
  echo "zcashd fixture volume has less than 5 GiB free after refresh" >&2
  exit 1
}

rm -f "$CONFIG"
rm -rf /root/.cache/zakura-fixture-network /root/.zakura-fixture-identity
sync
umount "$ZCASHD_DIR"
trap - EXIT
echo "zcashd fixture refreshed cleanly at height $ZCASHD_HEIGHT (Zakura $ZAKURA_HEIGHT, drift $FINAL_LAG)."
