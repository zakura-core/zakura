#!/usr/bin/env bash
# Runs ON an ephemeral perf-bench droplet (zakura-perf-bench.yml). Historical
# mode syncs a baked sandblast state range. Live-head mode restores the baked
# pruned tip, catches up without profiling, then records a fixed window after
# the verified body tip matches the live peer header tip.
#
# The volume clone is disposable and deleted with the droplet, so the node runs
# directly on it — no snapshot download, no fork management, no binary cache.
# Profiling is best-effort throughout: a missing perf/inferno degrades to a
# logged warning and never fails the bench.
#
# Config via /root/run.env (sourced by the caller before exec):
#   GH_REPO / GH_CLONE_TOKEN  repo slug + per-run token for the ref fetch
#   SHA                       commit to bench
#   LEG                       primary | baseline (labels outputs)
#   WORKLOAD                  historical_sync | live_head
#   VERIFY_MODE               checkpoint | semantic
#   PROFILE                   cpu | off
#   STOP_HEIGHT               debug_stop_at_height
#   WALL_CAP                  hard wall-clock cap for the sync, seconds
#   START_HEIGHT              volume state tip height (sandblast bake: 1707210)
#   HEAD_PROFILE_MINUTES      live-head measurement window
#   PEERSET_SIZE              target peer count
#   VOLUME_NAME               DO volume holding the sandblast state
# Optional knobs (defaults below): PROFILE_SECONDS, PROFILE_FREQ,
#   PROFILE_DWARF_STACK, CKPT_LIMIT, DL_LIMIT, P2P_STACK, FEED_PEER,
#   HEAD_STABLE_SAMPLES, HEAD_MAX_ESTIMATED_DISTANCE, HEAD_MIN_HEALTHY_PEERS.
#   P2P_STACK defaults to the production network default for live head and to
#   Zakura for historical sync.
#
# Helper scripts scp'd next to this one by the workflow (from the workflow's
# own checkout, so the benched ref does not need to contain them):
#   /root/zakura-bench-digest.py       collapse/top/latency digests
#   /root/zakura-metrics-dashboard.py  metrics recorder + bottleneck classifier
set -euo pipefail

OUT_DIR=/root/out
DIGEST_PY=/root/zakura-bench-digest.py
DASHBOARD_PY=/root/zakura-metrics-dashboard.py
PROFILE_SECONDS="${PROFILE_SECONDS:-300}"
# 49Hz keeps DWARF unwinding fast while retaining enough samples for comparison.
PROFILE_FREQ="${PROFILE_FREQ:-49}"
PROFILE_DWARF_STACK="${PROFILE_DWARF_STACK:-8192}"
CKPT_LIMIT="${CKPT_LIMIT:-1500}"
DL_LIMIT="${DL_LIMIT:-150}"
P2P_STACK="${P2P_STACK:-}"
METRICS_PORT=9999
SAMPLE_INTERVAL=5
HEAD_STABLE_SAMPLES="${HEAD_STABLE_SAMPLES:-6}"
HEAD_MAX_ESTIMATED_DISTANCE="${HEAD_MAX_ESTIMATED_DISTANCE:-12}"
HEAD_MIN_HEALTHY_PEERS="${HEAD_MIN_HEALTHY_PEERS:-3}"
mkdir -p "$OUT_DIR"

log() { printf '[perf-bench %(%H:%M:%S)T] %s\n' -1 "$*" >&2; }
die() { log "FATAL: $*"; exit 1; }

# same private-cohort bootstrap peers the checkpoint-sync bench pins for the
# Zakura P2P v2 stack
ZAKURA_BOOTSTRAP_PEERS=(
  "9ec67ad6834bc2ca0d659c240e042d3446c37cabcc092b527d459c87d938b4a4@159.65.183.89:8234"
  "bd3dc5d2a3d44c6bf90e364bf446231dbf9737e38a562ccf9e91ea631ea59b22@143.244.184.176:8234"
  "14ab98fa0c4b07d40119e1dbc9f3c36d20c8f226ae5ba4216218a2034f148e57@159.203.38.10:8234"
  "681d21b18644cd82ec13256a97f92bec1fff815683ef6f65dc7c993f098a4fe5@64.227.44.93:8234"
  "058b3f20dc9bef7bb447f94d7663d793cfbc036720f97e52d7f13661b21818e1@161.35.156.226:8234"
  "291323d78eb7186c3fa225ef5e305e95363e0ef06d42dca91bd4ef0254aed1ae@139.59.64.115:8234"
  "85e425233a68697d4be91dd5d542305a8a327cd06d992d53c0913cef2fa75084@168.144.173.250:8234"
)

NODE_PID=""; PERF_PID=""; STAT_PID=""; REC_PID=""
cleanup() {
  { [[ -n "$PERF_PID" ]] && kill "$PERF_PID"; } 2>/dev/null || true
  { [[ -n "$STAT_PID" ]] && kill "$STAT_PID"; } 2>/dev/null || true
  { [[ -n "$REC_PID" ]] && kill "$REC_PID"; } 2>/dev/null || true
  { [[ -n "$NODE_PID" ]] && kill -9 "$NODE_PID"; } 2>/dev/null || true
  return 0
}
trap cleanup EXIT INT TERM

cloud-init status --wait >/dev/null 2>&1 || true

WORKLOAD="${WORKLOAD:-historical_sync}"
case "$WORKLOAD" in
  historical_sync)
    SNAPSHOT_MODE=sandblast
    STORAGE_MODE=archive
    ;;
  live_head)
    SNAPSHOT_MODE=tip
    STORAGE_MODE=pruned
    [[ "$LEG" == primary ]] || die "live_head only supports the primary leg"
    HEAD_PROFILE_MINUTES="${HEAD_PROFILE_MINUTES:-60}"
    [[ "$HEAD_PROFILE_MINUTES" =~ ^[1-9][0-9]*$ ]] \
      || die "HEAD_PROFILE_MINUTES must be a positive integer"
    PROFILE_SECONDS=$(( HEAD_PROFILE_MINUTES * 60 ))
    ;;
  *) die "unknown workload: $WORKLOAD" ;;
esac

if [[ -z "$P2P_STACK" ]]; then
  if [[ "$WORKLOAD" == live_head ]]; then
    P2P_STACK=default
  else
    P2P_STACK=zakura
  fi
fi
case "$P2P_STACK" in
  default|legacy|zakura|dual) ;;
  *) die "unknown P2P_STACK: $P2P_STACK" ;;
esac

# ---------------------------------------------------------------------------- #
# State: mount the requested state tree from the cloned baked volume snapshot
# ---------------------------------------------------------------------------- #

DEV="/dev/disk/by-id/scsi-0DO_Volume_${VOLUME_NAME}"
for _ in $(seq 1 30); do [ -e "$DEV" ] && break; sleep 2; done
[ -e "$DEV" ] || die "state volume device not found: $DEV"
mkdir -p /mnt/snapshots
mount "$DEV" /mnt/snapshots
STATE_CACHE_DIR="/mnt/snapshots/${SNAPSHOT_MODE}"
[ -d "$STATE_CACHE_DIR" ] || die "no ${SNAPSHOT_MODE}/ state on the volume"
df -h /mnt/snapshots >&2

# ---------------------------------------------------------------------------- #
# Source: fetch the benched ref into the baked clone
# ---------------------------------------------------------------------------- #

cd /root/zakura
GIT_AUTH=$(printf 'x-access-token:%s' "${GH_CLONE_TOKEN}" | base64 -w0)
git -c http.extraheader="AUTHORIZATION: basic ${GIT_AUTH}" \
  fetch --no-tags origin "${SHA}"
git checkout --detach "${SHA}"
rm -f /root/run.env
unset GH_CLONE_TOKEN GIT_AUTH

# DB-format preflight: a mismatched volume state would silently sync from
# scratch and invalidate the numbers, so fail loudly instead.
CODE_VER=$(grep -oE 'DATABASE_FORMAT_VERSION: .* [0-9]+' zakura-state/src/constants.rs | grep -oE '[0-9]+' | tail -n1)
DIR_VER=$(find "$STATE_CACHE_DIR/state" -mindepth 1 -maxdepth 1 -type d -name 'v*' 2>/dev/null | \
  sed 's#.*/v##' | sort -n | tail -1)
if [ -z "$DIR_VER" ]; then
  die "no state/v* directory under $STATE_CACHE_DIR"
elif [ "$DIR_VER" != "$CODE_VER" ] && [ "$DIR_VER" != "$((CODE_VER - 1))" ]; then
  die "DB format mismatch: volume is v${DIR_VER}, benched tree is v${CODE_VER}; re-bake the state snapshot"
fi

# ---------------------------------------------------------------------------- #
# Build against the baked cargo cache
# ---------------------------------------------------------------------------- #

export CARGO_TARGET_DIR=/root/cargo-target
# shellcheck source=/dev/null
[[ -f "$HOME/.cargo/env" ]] && . "$HOME/.cargo/env"
BUILD_START=$(date +%s)
cargo build --release -p zakura --features prometheus,commit-metrics --locked >&2 \
  || die "cargo build failed for ${SHA}"
BUILD_SECS=$(( $(date +%s) - BUILD_START ))
ZAKURAD_BIN="$CARGO_TARGET_DIR/release/zakurad"
[[ -x "$ZAKURAD_BIN" ]] || die "build produced no zakurad binary"
log "built ${SHA} in ${BUILD_SECS}s (warm baked cache): $("$ZAKURAD_BIN" --version | head -1)"

# ---------------------------------------------------------------------------- #
# Profiler setup (best-effort; mirrors scripts/checkpoint-sync-bench.sh)
# ---------------------------------------------------------------------------- #

PERF_EVENT="cycles:u"
INFERNO_OK=0
DEMANGLE=(cat)
if [[ "$PROFILE" == "cpu" ]]; then
  if ! command -v perf >/dev/null 2>&1; then
    log "perf not found; installing linux-tools"
    apt-get install -y -qq "linux-tools-$(uname -r)" 2>/dev/null \
      || apt-get install -y -qq linux-tools-generic 2>/dev/null || true
  fi
  # glibc's internal (static) allocator symbols are not in the stripped libc's
  # dynsym, which made ~20% of leaf frames "[unknown]" under malloc/free in the
  # A/A validation; libc6-dbg restores them, and debuginfod covers other system
  # libraries at `perf script` time. Both best-effort.
  apt-get install -y -qq libc6-dbg 2>/dev/null || true
  export DEBUGINFOD_URLS="${DEBUGINFOD_URLS:-https://debuginfod.ubuntu.com}"
  # the droplet's binutils demangler predates Rust v0 mangling, so digest
  # tables show raw _R... names without rustfilt
  if ! command -v rustfilt >/dev/null 2>&1 && command -v cargo >/dev/null 2>&1; then
    log "installing rustfilt (Rust symbol demangler) via cargo ..."
    cargo install rustfilt --locked >/dev/null 2>&1 || true
  fi
  command -v rustfilt >/dev/null 2>&1 && DEMANGLE=(rustfilt)
  found=0
  if command -v perf >/dev/null 2>&1; then
    # Prefer hardware cycles when available, then fall back to cpu-clock.
    for event in "cycles:u" "cpu-clock:u"; do
      if perf record -o /root/.perf-probe -e "$event" -F 9 -- true >/dev/null 2>&1; then
        PERF_EVENT="$event"; found=1; break
      fi
    done
    rm -f /root/.perf-probe
  fi
  if (( ! found )); then
    log "WARNING: perf cannot record on this droplet; disabling CPU profiling"
    PROFILE="off"
  else
    if command -v inferno-flamegraph >/dev/null 2>&1; then
      INFERNO_OK=1
    elif command -v cargo >/dev/null 2>&1; then
      log "installing inferno (flamegraph renderer) via cargo ..."
      cargo install inferno --locked >/dev/null 2>&1 || true
      command -v inferno-flamegraph >/dev/null 2>&1 && INFERNO_OK=1
    fi
    (( INFERNO_OK )) || log "inferno unavailable; folded stacks + digest only"
    log "CPU profiling on: event=$PERF_EVENT freq=${PROFILE_FREQ}Hz window=${PROFILE_SECONDS}s"
  fi
fi

# ---------------------------------------------------------------------------- #
# Node config + launch
# ---------------------------------------------------------------------------- #

TRACE_DIR="$OUT_DIR/zakura-traces"
mkdir -p "$TRACE_DIR"
CFG=/root/bench-config.toml
{
  echo '[network]'
  echo 'network = "Mainnet"'
  echo "cache_dir = \"$STATE_CACHE_DIR\""
  echo 'listen_addr = "127.0.0.1:8233"'
  [[ -n "${FEED_PEER:-}" ]] && echo "initial_mainnet_peers = [\"$FEED_PEER\"]"
  echo "peerset_initial_target_size = ${PEERSET_SIZE}"
  echo "p2p_stack = \"$P2P_STACK\""
  echo ''
  if [[ "$P2P_STACK" == "zakura" || "$P2P_STACK" == "dual" ]]; then
    echo '[network.zakura]'
    echo "trace_dir = \"$TRACE_DIR\""
    echo 'bootstrap_peers = ['
    for peer in "${ZAKURA_BOOTSTRAP_PEERS[@]}"; do
      echo "  \"$peer\","
    done
    echo ']'
    echo ''
  fi
  if [[ "$VERIFY_MODE" == "semantic" ]]; then
    # Full semantic verification of the volume range: mandatory checkpoints end
    # below the sandblast tip, so every synced block gets script+proof checks.
    echo '[consensus]'
    echo 'checkpoint_sync = false'
    echo ''
  fi
  echo '[state]'
  echo "cache_dir = \"$STATE_CACHE_DIR\""
  echo "storage_mode = \"$STORAGE_MODE\""
  [[ "$WORKLOAD" == historical_sync ]] && echo "debug_stop_at_height = $STOP_HEIGHT"
  echo ''
  echo '[sync]'
  echo "checkpoint_verify_concurrency_limit = $CKPT_LIMIT"
  echo "download_concurrency_limit = $DL_LIMIT"
  echo 'full_verify_concurrency_limit = 20'
  echo ''
  echo '[metrics]'
  echo "endpoint_addr = \"127.0.0.1:$METRICS_PORT\""
  echo ''
  if [[ "$WORKLOAD" == live_head ]]; then
    echo '[rpc]'
    echo 'listen_addr = "127.0.0.1:8232"'
    echo 'enable_cookie_auth = false'
    echo ''
  fi
  echo '[tracing]'
  echo 'filter = "info"'
} > "$CFG"

[[ "$WORKLOAD" == live_head ]] && SNAPSHOT_HEIGHT=""

LOGF="$OUT_DIR/node.log"
log "starting zakurad ($SHA), workload=$WORKLOAD leg=$LEG verify_mode=$VERIFY_MODE p2p_stack=$P2P_STACK cap=${WALL_CAP}s peers=${FEED_PEER:-DNS-seeders}/${PEERSET_SIZE}"
"$ZAKURAD_BIN" -c "$CFG" start >"$LOGF" 2>&1 &
NODE_PID=$!
T0=$(date +%s)
sleep 3
kill -0 "$NODE_PID" 2>/dev/null || { tail -20 "$LOGF" >&2; die "zakurad died on startup"; }

# Metrics recorder sidecar for the historical-sync bottleneck verdict.
REC_DIR="$OUT_DIR/recorded"
start_recorder() {
  [[ -z "$REC_PID" ]] || return 0
  command -v python3 >/dev/null 2>&1 && [[ -f "$DASHBOARD_PY" ]] || return 0
  mkdir -p "$REC_DIR"
  python3 "$DASHBOARD_PY" --no-serve --record "$REC_DIR" \
    --target "127.0.0.1:$METRICS_PORT" --interval 2 \
    --label "$LEG-$SHA" --ckpt-limit "$CKPT_LIMIT" --dl-limit "$DL_LIMIT" \
    --github-url "${GITHUB_RUN_URL:-}" --github-run-id "${GITHUB_RUN_ID:-}" \
    --github-repo "${GH_REPO}" \
    >"$OUT_DIR/recorder.log" 2>&1 &
  REC_PID=$!
}
[[ "$WORKLOAD" == historical_sync ]] && start_recorder

# ---------------------------------------------------------------------------- #
# Sample the selected workload and start profiling only at its measurement gate
# ---------------------------------------------------------------------------- #

HEIGHT_METRICS="state_finalized_block_height state_checkpoint_finalized_block_height checkpoint_finalized_block_height checkpoint_verified_height"
METRICS_SNAP="$OUT_DIR/metrics-final.prom"
METRICS_BASELINE="$OUT_DIR/metrics-start.prom"
capture_metrics() {
  local output="$1" page
  page="$(curl -fsS --max-time 4 "127.0.0.1:${METRICS_PORT}/metrics" 2>/dev/null || true)"
  [[ -n "$page" ]] || return 1
  printf '%s\n' "$page" > "$output"
}

scrape_height() {
  local page m v c
  page="$(curl -fsS --max-time 4 "127.0.0.1:${METRICS_PORT}/metrics" 2>/dev/null || true)"
  [[ -n "$page" ]] || return 0
  printf '%s\n' "$page" > "$METRICS_SNAP.tmp" 2>/dev/null || true
  c="$(awk '/^state_finalized_block_count /{printf "%d", $2; exit}' <<<"$page")"
  [[ -n "$c" ]] && { echo "$(( START_HEIGHT + c ))"; return; }
  for m in $HEIGHT_METRICS; do
    v="$(awk -v n="$m" '$1==n {printf "%d", $2; exit}' <<<"$page")"
    [[ -n "$v" && "$v" -gt 0 ]] && { echo "$v"; return; }
  done
}

scrape_head_state() {
  local page height header peers estimated_distance rpc_info rpc_height rpc_estimated rpc_peers
  if [[ "$P2P_STACK" == "default" || "$P2P_STACK" == "legacy" ]]; then
    rpc_info="$(rpc_chain_info)"
    rpc_peers="$(rpc_connection_count)"
    [[ -n "$rpc_info" && "$rpc_peers" =~ ^[0-9]+$ ]] || return 0
    IFS=$'\t' read -r rpc_height _ rpc_estimated <<<"$rpc_info"
    [[ "$rpc_height" =~ ^[1-9][0-9]*$ && "$rpc_estimated" =~ ^[0-9]+$ ]] || return 0
    estimated_distance=$((rpc_estimated - rpc_height))
    (( estimated_distance >= 0 )) || estimated_distance=0
    printf '%s\t%s\t%s\t%s\n' \
      "$rpc_height" "$rpc_height" "$rpc_peers" "$estimated_distance"
    return
  fi

  page="$(curl -fsS --max-time 4 "127.0.0.1:${METRICS_PORT}/metrics" 2>/dev/null || true)"
  [[ -n "$page" ]] || return 0
  printf '%s\n' "$page" > "$METRICS_SNAP.tmp" 2>/dev/null || true
  metric() { awk -v n="$1" '$1==n {printf "%.0f", $2; exit}' <<<"$page"; }
  height="$(metric zcash_chain_verified_block_height)"
  [[ -n "$height" ]] || height="$(metric sync_block_verified_tip_height)"
  header="$(metric sync_block_best_header_tip_height)"
  [[ -n "$header" ]] || header="$(metric sync_header_best_tip_height)"
  peers="$(metric zakura_p2p_healthy_peers)"
  [[ -n "$peers" ]] || peers="$(metric zakura_p2p_connected_peers)"
  estimated_distance="$(metric sync_estimated_distance_to_tip)"
  if [[ ! "$height" =~ ^[1-9][0-9]*$ || ! "$estimated_distance" =~ ^[0-9]+$ ]]; then
    rpc_info="$(rpc_chain_info)"
    if [[ -n "$rpc_info" ]]; then
      IFS=$'\t' read -r rpc_height _ rpc_estimated <<<"$rpc_info"
      [[ "$height" =~ ^[1-9][0-9]*$ ]] || height="$rpc_height"
      if [[ ! "$estimated_distance" =~ ^[0-9]+$ ]]; then
        estimated_distance=$((rpc_estimated - height))
        (( estimated_distance >= 0 )) || estimated_distance=0
      fi
    fi
  fi
  if [[ ! "$header" =~ ^[1-9][0-9]*$ ]]; then
    header="$height"
  fi
  rpc_peers="$(rpc_connection_count)"
  if [[ "$rpc_peers" =~ ^[0-9]+$ \
    && ( ! "$peers" =~ ^[0-9]+$ || "$rpc_peers" -gt "$peers" ) ]]; then
    peers="$rpc_peers"
  fi
  [[ "$height" =~ ^[0-9]+$ && "$header" =~ ^[0-9]+$ \
    && "$peers" =~ ^[0-9]+$ && "$estimated_distance" =~ ^[0-9]+$ ]] || return 0
  printf '%s\t%s\t%s\t%s\n' "$height" "$header" "$peers" "$estimated_distance"
}

rpc_chain_info() {
  curl -fsS --max-time 10 -H 'content-type: application/json' \
    --data-binary '{"jsonrpc":"1.0","id":"perf-bench","method":"getblockchaininfo","params":[]}' \
    http://127.0.0.1:8232 2>/dev/null \
    | jq -er '.result | [.blocks, .bestblockhash, (.estimatedheight // .blocks)] | @tsv' \
      2>/dev/null || true
}

rpc_connection_count() {
  curl -fsS --max-time 10 -H 'content-type: application/json' \
    --data-binary '{"jsonrpc":"1.0","id":"perf-bench","method":"getnetworkinfo","params":[]}' \
    http://127.0.0.1:8232 2>/dev/null \
    | jq -er '.result.connections' 2>/dev/null || true
}

head_sample_is_healthy() {
  local height="$1" header="$2" peers="$3" estimated_distance="$4"
  (( height >= header \
    && peers >= HEAD_MIN_HEALTHY_PEERS \
    && estimated_distance <= HEAD_MAX_ESTIMATED_DISTANCE ))
}

start_profile() {
  [[ "$PROFILE" == "cpu" && -z "$PERF_PID" ]] || return 0
  perf record -o "$OUT_DIR/perf.data" -e "$PERF_EVENT" -F "$PROFILE_FREQ" \
    --call-graph "dwarf,$PROFILE_DWARF_STACK" -p "$NODE_PID" -- sleep "$PROFILE_SECONDS" \
    >"$OUT_DIR/perf.log" 2>&1 &
  PERF_PID=$!
  perf stat -x, -o "$OUT_DIR/perf-stat.csv" -p "$NODE_PID" \
    -e task-clock -e cycles:u -e instructions:u -- sleep "$PROFILE_SECONDS" \
    >"$OUT_DIR/perf-stat.log" 2>&1 &
  STAT_PID=$!
  log "CPU profile window started: ${PROFILE_SECONDS}s @ ${PROFILE_FREQ}Hz ($PERF_EVENT)"
}

CSV="$OUT_DIR/samples.csv"
T_ESCAPE=""; END_HEIGHT="$START_HEIGHT"; CLEAN_STOP=0; NODE_EXIT_STATUS=0; LAST_BEAT=0
START_HASH=""; END_HASH=""; ESTIMATED_START=""; ESTIMATED_END=""
HEADER_START=""; HEADER_END=""; CATCHUP_SECONDS=0; WINDOW_COMPLETE=0
MAX_ESTIMATED_DISTANCE=0; MAX_HEADER_LAG=0; MAX_UNHEALTHY_SAMPLES=0
FAILURE_REASON=""

stop_node() {
  [[ -n "$NODE_PID" ]] || return 0
  if [[ "$WORKLOAD" == live_head ]]; then
    capture_metrics "$METRICS_SNAP" || true
  fi
  kill "$NODE_PID" 2>/dev/null || true
  for _ in $(seq 1 12); do
    kill -0 "$NODE_PID" 2>/dev/null || break
    sleep 5
  done
  kill -9 "$NODE_PID" 2>/dev/null || true
  wait "$NODE_PID" 2>/dev/null || true
  NODE_PID=""
}

if [[ "$WORKLOAD" == historical_sync ]]; then
  echo "epoch,elapsed,height" > "$CSV"
  while :; do
    NOW=$(date +%s); ELAPSED=$((NOW - T0))
    H="$(scrape_height)" || true
    if [[ -n "$H" ]] && (( H >= START_HEIGHT && H <= STOP_HEIGHT + 200 )); then
      echo "$NOW,$ELAPSED,$H" >> "$CSV"
      END_HEIGHT="$H"
      if [[ -z "$T_ESCAPE" && "$H" -gt "$START_HEIGHT" ]]; then
        T_ESCAPE=$NOW; log "escaped cold-start at +${ELAPSED}s, height $H"
        start_profile
      fi
      if (( NOW - LAST_BEAT >= 120 )); then
        LAST_BEAT=$NOW
        log "height $H (+${ELAPSED}s, $(( H - START_HEIGHT )) blocks)"
      fi
    fi
    if ! kill -0 "$NODE_PID" 2>/dev/null; then
      if wait "$NODE_PID" 2>/dev/null; then
        CLEAN_STOP=1
      else
        NODE_EXIT_STATUS=$?
        log "zakurad exited with status ${NODE_EXIT_STATUS} before reaching stop height"
      fi
      NODE_PID=""
      break
    fi
    if (( ELAPSED >= WALL_CAP )); then
      log "wall cap ${WALL_CAP}s reached; stopping zakurad"
      stop_node
      break
    fi
    sleep "$SAMPLE_INTERVAL"
  done
else
  echo "epoch,elapsed,height,header_height,peer_count,estimated_distance,phase" > "$CSV"
  STABLE=0
  PROFILE_START_EPOCH=0
  PROFILE_END_EPOCH=0
  log "catching up from the baked tip; profiling starts after ${HEAD_STABLE_SAMPLES} stable head samples"
  while :; do
    NOW=$(date +%s); ELAPSED=$((NOW - T0))
    HEAD_STATE="$(scrape_head_state)" || true
    if [[ -n "$HEAD_STATE" ]]; then
      IFS=$'\t' read -r H HEADER PEERS EST_DISTANCE <<<"$HEAD_STATE"
      echo "$NOW,$ELAPSED,$H,$HEADER,$PEERS,$EST_DISTANCE,catchup" >> "$CSV"
      END_HEIGHT="$H"
      if [[ -z "$SNAPSHOT_HEIGHT" ]]; then
        SNAPSHOT_HEIGHT="$H"
        START_HEIGHT="$H"
        log "initial observed tip: $SNAPSHOT_HEIGHT"
      fi
      if (( HEADER >= SNAPSHOT_HEIGHT )) \
        && head_sample_is_healthy "$H" "$HEADER" "$PEERS" "$EST_DISTANCE"; then
        STABLE=$((STABLE + 1))
      else
        STABLE=0
      fi
      if (( NOW - LAST_BEAT >= 60 )); then
        LAST_BEAT=$NOW
        log "head gate: body=$H header=$HEADER peers=$PEERS estimated_distance=$EST_DISTANCE stable=$STABLE/$HEAD_STABLE_SAMPLES (+${ELAPSED}s)"
      fi
      if (( STABLE >= HEAD_STABLE_SAMPLES )); then
        RPC_INFO="$(rpc_chain_info)"
        if [[ -n "$RPC_INFO" ]]; then
          IFS=$'\t' read -r START_HEIGHT START_HASH ESTIMATED_START <<<"$RPC_INFO"
          if (( START_HEIGHT >= HEADER )); then
            END_HEIGHT="$START_HEIGHT"
            HEADER_START="$HEADER"
            CATCHUP_SECONDS="$ELAPSED"
            PROFILE_START_EPOCH="$NOW"
            PROFILE_END_EPOCH=$((PROFILE_START_EPOCH + PROFILE_SECONDS))
            LAST_HEALTHY_EPOCH="$NOW"
            UNHEALTHY=0
            T_ESCAPE="$PROFILE_START_EPOCH"
            capture_metrics "$METRICS_BASELINE" || true
            start_profile
            log "live head locked at $START_HEIGHT ($START_HASH); recording ${PROFILE_SECONDS}s"
            break
          fi
        fi
        STABLE=0
      fi
    fi
    if ! kill -0 "$NODE_PID" 2>/dev/null; then
      if wait "$NODE_PID" 2>/dev/null; then NODE_EXIT_STATUS=1; else NODE_EXIT_STATUS=$?; fi
      NODE_PID=""
      FAILURE_REASON="zakurad exited before reaching live head"
      break
    fi
    if (( ELAPSED >= WALL_CAP )); then
      NODE_EXIT_STATUS=124
      FAILURE_REASON="live head gate was not reached within ${WALL_CAP}s"
      log "$FAILURE_REASON"
      stop_node
      break
    fi
    sleep "$SAMPLE_INTERVAL"
  done

  while [[ -n "$NODE_PID" && "$PROFILE_START_EPOCH" -gt 0 ]]; do
    NOW=$(date +%s); ELAPSED=$((NOW - PROFILE_START_EPOCH))
    HEAD_STATE="$(scrape_head_state)" || true
    if [[ -n "$HEAD_STATE" ]]; then
      IFS=$'\t' read -r H HEADER PEERS EST_DISTANCE <<<"$HEAD_STATE"
      echo "$NOW,$ELAPSED,$H,$HEADER,$PEERS,$EST_DISTANCE,profile" >> "$CSV"
      END_HEIGHT="$H"; HEADER_END="$HEADER"; ESTIMATED_END=$((H + EST_DISTANCE))
      (( EST_DISTANCE > MAX_ESTIMATED_DISTANCE )) && MAX_ESTIMATED_DISTANCE="$EST_DISTANCE"
      HEADER_LAG=$((HEADER - H)); (( HEADER_LAG >= 0 )) || HEADER_LAG=0
      (( HEADER_LAG > MAX_HEADER_LAG )) && MAX_HEADER_LAG="$HEADER_LAG"
      if head_sample_is_healthy "$H" "$HEADER" "$PEERS" "$EST_DISTANCE"; then
        UNHEALTHY=0
        LAST_HEALTHY_EPOCH="$NOW"
      else
        UNHEALTHY=$((UNHEALTHY + 1))
      fi
      if (( NOW - LAST_BEAT >= 120 )); then
        LAST_BEAT=$NOW
        log "live profile: body=$H header=$HEADER peers=$PEERS estimated_distance=$EST_DISTANCE unhealthy=$UNHEALTHY/$HEAD_STABLE_SAMPLES (+${ELAPSED}/${PROFILE_SECONDS}s)"
      fi
    else
      UNHEALTHY=$((UNHEALTHY + 1))
    fi
    (( UNHEALTHY > MAX_UNHEALTHY_SAMPLES )) && MAX_UNHEALTHY_SAMPLES="$UNHEALTHY"
    if (( UNHEALTHY >= HEAD_STABLE_SAMPLES )); then
      NODE_EXIT_STATUS=125
      FAILURE_REASON="live head was lost for ${UNHEALTHY} consecutive samples (body=${H:-n/a} header=${HEADER:-n/a} peers=${PEERS:-n/a} estimated_distance=${EST_DISTANCE:-n/a})"
      log "$FAILURE_REASON"
      stop_node
      break
    fi
    if ! kill -0 "$NODE_PID" 2>/dev/null; then
      if wait "$NODE_PID" 2>/dev/null; then NODE_EXIT_STATUS=1; else NODE_EXIT_STATUS=$?; fi
      NODE_PID=""
      FAILURE_REASON="zakurad exited during the live-head profile"
      break
    fi
    if (( NOW >= PROFILE_END_EPOCH )); then
      if (( NOW - LAST_HEALTHY_EPOCH < HEAD_STABLE_SAMPLES * SAMPLE_INTERVAL )); then
        WINDOW_COMPLETE=1
      else
        NODE_EXIT_STATUS=125
        FAILURE_REASON="live head was not healthy at the end of the profile window"
        log "$FAILURE_REASON"
        stop_node
      fi
      break
    fi
    sleep "$SAMPLE_INTERVAL"
  done

  if (( WINDOW_COMPLETE )); then
    for _ in $(seq 1 6); do
      RPC_INFO="$(rpc_chain_info)"
      [[ -n "$RPC_INFO" ]] && break
      sleep 2
    done
    if [[ -n "$RPC_INFO" ]]; then
      IFS=$'\t' read -r END_HEIGHT END_HASH ESTIMATED_END <<<"$RPC_INFO"
    fi
    if [[ -n "$PERF_PID" ]]; then wait "$PERF_PID" 2>/dev/null || true; PERF_PID=""; fi
    if [[ -n "$STAT_PID" ]]; then wait "$STAT_PID" 2>/dev/null || true; STAT_PID=""; fi
    stop_node
  fi
fi

T_END=$(date +%s)
if [[ -n "$REC_PID" ]]; then kill "$REC_PID" 2>/dev/null || true; wait "$REC_PID" 2>/dev/null || true; REC_PID=""; fi
if [[ "$WORKLOAD" == historical_sync ]]; then
  { [[ -f "$METRICS_SNAP.tmp" ]] && mv -f "$METRICS_SNAP.tmp" "$METRICS_SNAP"; } 2>/dev/null || true
elif [[ ! -s "$METRICS_SNAP" ]]; then
  { [[ -f "$METRICS_SNAP.tmp" ]] && mv -f "$METRICS_SNAP.tmp" "$METRICS_SNAP"; } 2>/dev/null || true
fi

# ---------------------------------------------------------------------------- #
# Profile digest: folded stacks, flamegraph SVG, top-functions markdown
# ---------------------------------------------------------------------------- #

PROFILE_NOTE="workload=$WORKLOAD verify_mode=$VERIFY_MODE p2p_stack=$P2P_STACK $PERF_EVENT @ ${PROFILE_FREQ}Hz, ${PROFILE_SECONDS}s window"
if [[ -n "$PERF_PID" ]]; then
  if [[ "$WORKLOAD" != live_head || "$WINDOW_COMPLETE" -ne 1 ]]; then
    kill "$PERF_PID" 2>/dev/null || true
  fi
  wait "$PERF_PID" 2>/dev/null || true
  PERF_PID=""
fi
if [[ -n "$STAT_PID" ]]; then
  if [[ "$WORKLOAD" != live_head || "$WINDOW_COMPLETE" -ne 1 ]]; then
    kill "$STAT_PID" 2>/dev/null || true
  fi
  wait "$STAT_PID" 2>/dev/null || true
  STAT_PID=""
fi
if [[ -s "$OUT_DIR/perf.data" ]]; then
  # --no-inline skips per-sample inline-frame resolution, the dominant cost of
  # dwarf perf.script (tens of minutes without it at these sample volumes);
  # inline info is partial on line-tables-only builds anyway
  log "folding $(du -m "$OUT_DIR/perf.data" | cut -f1)MB of perf data (dwarf unwinding) ..."
  FOLD_START=$(date +%s)
  if ! perf script --no-inline -i "$OUT_DIR/perf.data" 2>>"$OUT_DIR/perf.log" \
        | "${DEMANGLE[@]}" \
        | python3 "$DIGEST_PY" collapse > "$OUT_DIR/profile.folded" \
        || [[ ! -s "$OUT_DIR/profile.folded" ]]; then
    log "WARNING: perf script/collapse produced no stacks:" \
      "$(head -2 "$OUT_DIR/perf.log" 2>/dev/null | tr '\n' ' ')"
    rm -f "$OUT_DIR/profile.folded"
  else
    if (( INFERNO_OK )); then
      inferno-flamegraph --title "zakurad CPU — $LEG $SHA" --subtitle "$PROFILE_NOTE" \
        < "$OUT_DIR/profile.folded" > "$OUT_DIR/flamegraph.svg" 2>>"$OUT_DIR/perf.log" \
        || { log "WARNING: flamegraph render failed"; rm -f "$OUT_DIR/flamegraph.svg"; }
    fi
    python3 "$DIGEST_PY" top --folded "$OUT_DIR/profile.folded" \
      --title "$LEG $SHA" --note "$PROFILE_NOTE" > "$OUT_DIR/profile.md" 2>>"$OUT_DIR/perf.log" \
      || { log "WARNING: profile digest failed"; rm -f "$OUT_DIR/profile.md"; }
    log "profile folded + digested in $(( $(date +%s) - FOLD_START ))s"
  fi
  rm -f "$OUT_DIR/perf.data"
elif [[ "$PROFILE" == "cpu" ]]; then
  log "WARNING: no perf data captured"
fi
if [[ -s "$OUT_DIR/perf-stat.csv" ]]; then
  python3 "$DIGEST_PY" stat --csv "$OUT_DIR/perf-stat.csv" \
    --title "$LEG $SHA" > "$OUT_DIR/perf-stat.md" 2>>"$OUT_DIR/perf-stat.log" \
    || { log "WARNING: perf stat digest failed"; rm -f "$OUT_DIR/perf-stat.md"; }
fi

# ---------------------------------------------------------------------------- #
# Latency digest, verdict, throughput numbers, leg summary + meta
# ---------------------------------------------------------------------------- #

LATENCY_ARGS=(latency --traces "$TRACE_DIR" --metrics "$METRICS_SNAP"
  --json-out "$OUT_DIR/latency.json" --title "$LEG $SHA ($WORKLOAD/$VERIFY_MODE)")
if [[ "$WORKLOAD" == live_head ]]; then
  BLOCKS=$((END_HEIGHT - START_HEIGHT))
  (( BLOCKS >= 0 )) || BLOCKS=0
  LATENCY_ARGS+=(--min-height "$(( START_HEIGHT + 1 ))" --observed-blocks "$BLOCKS")
  [[ -s "$METRICS_BASELINE" ]] && LATENCY_ARGS+=(--metrics-baseline "$METRICS_BASELINE")
fi
python3 "$DIGEST_PY" "${LATENCY_ARGS[@]}" \
  > "$OUT_DIR/latency.md" 2>"$OUT_DIR/digest.log" \
  || { log "WARNING: latency digest failed"; rm -f "$OUT_DIR/latency.md"; }

VERDICT=""
if [[ -d "$REC_DIR" && -f "$REC_DIR/samples.jsonl" ]]; then
  cp "$REC_DIR/samples.jsonl" "$OUT_DIR/samples.jsonl" 2>/dev/null || true
  if [[ "$WORKLOAD" == historical_sync ]] && python3 "$DASHBOARD_PY" --classify "$REC_DIR" \
       --verdict-out "$OUT_DIR/verdict.json" --label "$LEG-$SHA" \
       --ckpt-limit "$CKPT_LIMIT" --dl-limit "$DL_LIMIT" > "$OUT_DIR/verdict.md" 2>/dev/null; then
    VERDICT="$(awk -F'\\*\\*' '/^\*\*/{print $2; exit}' "$OUT_DIR/verdict.md")"
  fi
fi

if [[ "$WORKLOAD" == live_head ]]; then
  if (( WINDOW_COMPLETE )); then
    TOTAL="$PROFILE_SECONDS"
  elif (( PROFILE_START_EPOCH > 0 )); then
    TOTAL=$((T_END - PROFILE_START_EPOCH)); (( TOTAL > 0 )) || TOTAL=1
  else
    TOTAL=0
  fi
  BPS="0.00"
  PBPS="0.00"
  STOP_RESULT=$( (( WINDOW_COMPLETE )) && echo yes || echo no )
else
  if (( CLEAN_STOP )); then
    END_HEIGHT="$STOP_HEIGHT"
    STOP_RESULT="yes"
  elif (( NODE_EXIT_STATUS != 0 )); then
    STOP_RESULT="no (exit ${NODE_EXIT_STATUS})"
  else
    STOP_RESULT="no (wall cap)"
  fi
  BLOCKS=$((END_HEIGHT - START_HEIGHT))
  TOTAL=$((T_END - T0)); (( TOTAL > 0 )) || TOTAL=1
  POST=$TOTAL
  [[ -n "$T_ESCAPE" ]] && POST=$((T_END - T_ESCAPE)); (( POST > 0 )) || POST=1
  BPS="$(awk -v b="$BLOCKS" -v t="$TOTAL" 'BEGIN{printf "%.2f", b/t}')"
  PBPS="$(awk -v b="$BLOCKS" -v t="$POST" 'BEGIN{printf "%.2f", b/t}')"
fi
ERRS="$(grep -iE 'panic|ERROR committing|resetting state queue' "$LOGF" 2>/dev/null \
          | grep -viE 'zakura_network|peer' | head -3 || true)"

{
  if [[ "$WORKLOAD" == live_head ]]; then
    echo "### Live-head profile: $LEG — \`$SHA\`"
    echo ""
    echo "Observational profile of real mainnet head traffic with p2p_stack=$P2P_STACK; no baseline or speedup is implied."
    echo ""
    echo "| initial observed tip | catch-up | profile window | start tip | end tip | committed blocks | complete |"
    echo "|---:|---:|---:|---:|---:|---:|---|"
    printf '| %s | %ss | %ss | %s | %s | %s | %s |\n' \
      "$SNAPSHOT_HEIGHT" "$CATCHUP_SECONDS" "$PROFILE_SECONDS" "$START_HEIGHT" \
      "$END_HEIGHT" "$BLOCKS" "$STOP_RESULT"
    echo ""
    echo "start hash: \`${START_HASH:-n/a}\`; end hash: \`${END_HASH:-n/a}\`; peer header tip: ${HEADER_START:-n/a} → ${HEADER_END:-n/a}; estimated tip: ${ESTIMATED_START:-n/a} → ${ESTIMATED_END:-n/a}"
  else
    echo "### Leg: $LEG — \`$SHA\` ($VERIFY_MODE mode)"
    echo ""
    echo "| leg | end height | blocks | time | blocks/s | post-commit blk/s | reached stop | verdict |"
    echo "|---|---:|---:|---:|---:|---:|---|---|"
    printf '| %s | %s | %s | %ss | %s | %s | %s | %s |\n' \
      "$LEG" "$END_HEIGHT" "$BLOCKS" "$TOTAL" "$BPS" "$PBPS" \
      "$STOP_RESULT" "${VERDICT:-n/a}"
  fi
  echo ""
  echo "build: ${BUILD_SECS}s (warm baked cache); profile: $( [[ -s "$OUT_DIR/profile.folded" ]] && echo "captured ($PERF_EVENT)" || echo "n/a" )"
  if [[ -n "$ERRS" ]]; then
    echo ""
    echo "⚠ node log errors:"
    echo '```'
    echo "$ERRS"
    echo '```'
  fi
} > "$OUT_DIR/leg-summary.md"
# section order: CPU profile first, then block latency, verdict as the closer
[[ -f "$OUT_DIR/profile.md" ]] && { echo ""; cat "$OUT_DIR/profile.md"; } >> "$OUT_DIR/leg-summary.md"
[[ -f "$OUT_DIR/perf-stat.md" ]] && { echo ""; cat "$OUT_DIR/perf-stat.md"; } >> "$OUT_DIR/leg-summary.md"
[[ -f "$OUT_DIR/latency.md" ]] && { echo ""; cat "$OUT_DIR/latency.md"; } >> "$OUT_DIR/leg-summary.md"
[[ -f "$OUT_DIR/verdict.md" ]] && { echo ""; cat "$OUT_DIR/verdict.md"; } >> "$OUT_DIR/leg-summary.md"

# package traces + trim logs for scp
( cd "$OUT_DIR" && tar -cf - zakura-traces | zstd -T0 -q -f -o zakura-traces.tar.zst && rm -rf zakura-traces ) || true
tail -n 2000 "$LOGF" > "$OUT_DIR/node-tail.log" 2>/dev/null || true
zstd -T0 -q -f "$LOGF" -o "$OUT_DIR/node-full.log.zst" 2>/dev/null || true
rm -f "$LOGF"
rm -rf "$REC_DIR"

# machine-readable leg result for the compare job (best-effort: a meta failure
# costs the compare, never the leg — the A/A validation died here on a shell
# true/false leaking into python)
PROFILED=$( [[ -s "$OUT_DIR/profile.folded" ]] && echo 1 || echo 0 )
python3 - "$OUT_DIR/meta.json" <<PY || log "WARNING: meta.json write failed"
import json, sys
json.dump({
    "leg": "$LEG", "sha": "$SHA", "workload": "$WORKLOAD",
    "verify_mode": "$VERIFY_MODE", "p2p_stack": "$P2P_STACK",
    "snapshot_height": ${SNAPSHOT_HEIGHT:-$START_HEIGHT},
    "start_height": $START_HEIGHT, "end_height": $END_HEIGHT,
    "start_hash": "${START_HASH}", "end_hash": "${END_HASH}",
    "blocks": $BLOCKS, "seconds": $TOTAL, "bps": $BPS, "post_bps": $PBPS,
    "catchup_seconds": $CATCHUP_SECONDS,
    "estimated_start_height": ${ESTIMATED_START:-0},
    "estimated_end_height": ${ESTIMATED_END:-0},
    "max_estimated_distance": $MAX_ESTIMATED_DISTANCE,
    "max_header_lag": $MAX_HEADER_LAG,
    "max_unhealthy_samples": $MAX_UNHEALTHY_SAMPLES,
    "clean_stop": bool($CLEAN_STOP or $WINDOW_COMPLETE), "build_secs": $BUILD_SECS,
    "node_exit_status": $NODE_EXIT_STATUS,
    "verdict": "${VERDICT}", "profiled": bool($PROFILED),
}, open(sys.argv[1], "w"), indent=2)
PY

if (( NODE_EXIT_STATUS != 0 )); then
  die "${FAILURE_REASON:-zakurad exited with status ${NODE_EXIT_STATUS}}"
fi
if [[ "$WORKLOAD" == live_head ]]; then
  (( WINDOW_COMPLETE )) || die "live-head profile window did not complete"
  log "live-head profile done: $START_HEIGHT -> $END_HEIGHT ($BLOCKS blocks in ${TOTAL}s)"
else
  log "leg $LEG done: $BLOCKS blocks in ${TOTAL}s ($BPS blk/s), verdict=${VERDICT:-n/a}"
fi
