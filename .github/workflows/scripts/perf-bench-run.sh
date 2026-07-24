#!/usr/bin/env bash
# Runs ON an ephemeral perf-bench droplet (zakura-perf-bench.yml): mounts the
# cloned sandblast state volume, checks out the requested commit in the baked
# repo clone, builds zakurad incrementally against the baked cargo cache, then
# syncs mainnet forward from the volume state to a stop height while collecting
# a sampled CPU profile (perf), the Zakura JSONL traces, a recorded metrics
# series with a bottleneck verdict, and block-latency + CPU digests.
#
# The volume clone is disposable and deleted with the droplet, so the node runs
# directly on it — no snapshot download, no fork management, no binary cache.
# Profiling is best-effort throughout: a missing perf/inferno degrades to a
# logged warning and never fails the bench.
#
# Config via /root/run.env (sourced by the caller before exec):
#   GH_REPO / GH_CLONE_TOKEN  repo slug + per-run token for the ref fetch
#   SHA / REFSPEC             commit to bench + refspec that reaches it
#   LEG                       primary | baseline (labels outputs)
#   VERIFY_MODE               checkpoint | semantic
#   PROFILE                   cpu | off
#   STOP_HEIGHT               debug_stop_at_height
#   WALL_CAP                  hard wall-clock cap for the sync, seconds
#   START_HEIGHT              volume state tip height (sandblast bake: 1707210)
#   PEERSET_SIZE / FEED_PEER  peer shaping (blank FEED_PEER = DNS seeders)
#   VOLUME_NAME               DO volume holding the sandblast state
# Optional knobs (defaults below): PROFILE_SECONDS, PROFILE_FREQ,
#   PROFILE_DWARF_STACK, CKPT_LIMIT, DL_LIMIT, P2P_STACK.
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
# 49Hz (not 99) keeps `perf script` DWARF unwinding tractable: the A/A
# validation run measured ~55 minutes of unwinding for a 99Hz x 300s window on
# a c-16 droplet; halving the samples plus --no-inline brings it to minutes.
PROFILE_FREQ="${PROFILE_FREQ:-49}"
PROFILE_DWARF_STACK="${PROFILE_DWARF_STACK:-8192}"
CKPT_LIMIT="${CKPT_LIMIT:-1500}"
DL_LIMIT="${DL_LIMIT:-150}"
P2P_STACK="${P2P_STACK:-zakura}"
METRICS_PORT=9999
SAMPLE_INTERVAL=5
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

NODE_PID=""; PERF_PID=""; REC_PID=""
cleanup() {
  { [[ -n "$PERF_PID" ]] && kill "$PERF_PID"; } 2>/dev/null || true
  { [[ -n "$REC_PID" ]] && kill "$REC_PID"; } 2>/dev/null || true
  { [[ -n "$NODE_PID" ]] && kill -9 "$NODE_PID"; } 2>/dev/null || true
  return 0
}
trap cleanup EXIT INT TERM

cloud-init status --wait >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------- #
# State: mount the per-run clone of the baked sandblast volume snapshot
# ---------------------------------------------------------------------------- #

DEV="/dev/disk/by-id/scsi-0DO_Volume_${VOLUME_NAME}"
for _ in $(seq 1 30); do [ -e "$DEV" ] && break; sleep 2; done
[ -e "$DEV" ] || die "state volume device not found: $DEV"
mkdir -p /mnt/snapshots
mount "$DEV" /mnt/snapshots
STATE_CACHE_DIR="/mnt/snapshots/sandblast"
[ -d "$STATE_CACHE_DIR" ] || die "no sandblast/ state on the volume"
df -h /mnt/snapshots >&2

# ---------------------------------------------------------------------------- #
# Source: fetch the benched ref into the baked clone
# ---------------------------------------------------------------------------- #

cd /root/zakura
GIT_AUTH=$(printf 'x-access-token:%s' "${GH_CLONE_TOKEN}" | base64 -w0)
git -c http.extraheader="AUTHORIZATION: basic ${GIT_AUTH}" \
  fetch --no-tags origin "${REFSPEC}"
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
    # DO droplets expose no PMU, so hardware cycles falls back to cpu-clock
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
  if [[ "$P2P_STACK" != "legacy" ]]; then
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
  echo "debug_stop_at_height = $STOP_HEIGHT"
  echo ''
  echo '[sync]'
  echo "checkpoint_verify_concurrency_limit = $CKPT_LIMIT"
  echo "download_concurrency_limit = $DL_LIMIT"
  echo 'full_verify_concurrency_limit = 20'
  echo ''
  echo '[metrics]'
  echo "endpoint_addr = \"127.0.0.1:$METRICS_PORT\""
  echo ''
  echo '[tracing]'
  echo 'filter = "info"'
} > "$CFG"

LOGF="$OUT_DIR/node.log"
log "starting zakurad ($SHA), leg=$LEG verify_mode=$VERIFY_MODE p2p_stack=$P2P_STACK stop=$STOP_HEIGHT cap=${WALL_CAP}s peers=${FEED_PEER:-DNS-seeders}/${PEERSET_SIZE}"
"$ZAKURAD_BIN" -c "$CFG" start >"$LOGF" 2>&1 &
NODE_PID=$!
T0=$(date +%s)
sleep 3
kill -0 "$NODE_PID" 2>/dev/null || { tail -20 "$LOGF" >&2; die "zakurad died on startup"; }

# metrics recorder sidecar for the bottleneck verdict (best-effort)
REC_DIR="$OUT_DIR/recorded"
if command -v python3 >/dev/null 2>&1 && [[ -f "$DASHBOARD_PY" ]]; then
  mkdir -p "$REC_DIR"
  python3 "$DASHBOARD_PY" --no-serve --record "$REC_DIR" \
    --target "127.0.0.1:$METRICS_PORT" --interval 2 \
    --label "$LEG-$SHA" --ckpt-limit "$CKPT_LIMIT" --dl-limit "$DL_LIMIT" \
    --github-url "${GITHUB_RUN_URL:-}" --github-run-id "${GITHUB_RUN_ID:-}" \
    --github-repo "${GH_REPO}" \
    >"$OUT_DIR/recorder.log" 2>&1 &
  REC_PID=$!
fi

# ---------------------------------------------------------------------------- #
# Sample height until stop/cap; profile after cold-start escape
# ---------------------------------------------------------------------------- #

HEIGHT_METRICS="state_finalized_block_height state_checkpoint_finalized_block_height checkpoint_finalized_block_height checkpoint_verified_height"
METRICS_SNAP="$OUT_DIR/metrics-final.prom"
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

start_profile() {
  [[ "$PROFILE" == "cpu" && -z "$PERF_PID" ]] || return 0
  perf record -o "$OUT_DIR/perf.data" -e "$PERF_EVENT" -F "$PROFILE_FREQ" \
    --call-graph "dwarf,$PROFILE_DWARF_STACK" -p "$NODE_PID" -- sleep "$PROFILE_SECONDS" \
    >"$OUT_DIR/perf.log" 2>&1 &
  PERF_PID=$!
  log "CPU profile window started: ${PROFILE_SECONDS}s @ ${PROFILE_FREQ}Hz ($PERF_EVENT)"
}

CSV="$OUT_DIR/samples.csv"
echo "epoch,elapsed,height" > "$CSV"
T_ESCAPE=""; END_HEIGHT="$START_HEIGHT"; CLEAN_STOP=0; LAST_BEAT=0
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
    # liveness heartbeat for the CI log (the loop is otherwise silent)
    if (( NOW - LAST_BEAT >= 120 )); then
      LAST_BEAT=$NOW
      log "height $H (+${ELAPSED}s, $(( H - START_HEIGHT )) blocks)"
    fi
  fi
  if ! kill -0 "$NODE_PID" 2>/dev/null; then
    wait "$NODE_PID" 2>/dev/null || true
    CLEAN_STOP=1
    break
  fi
  if (( ELAPSED >= WALL_CAP )); then
    log "wall cap ${WALL_CAP}s reached; stopping zakurad"
    kill "$NODE_PID" 2>/dev/null || true; sleep 5; kill -9 "$NODE_PID" 2>/dev/null || true
    break
  fi
  sleep "$SAMPLE_INTERVAL"
done
NODE_PID=""
T_END=$(date +%s)
if [[ -n "$REC_PID" ]]; then kill "$REC_PID" 2>/dev/null || true; wait "$REC_PID" 2>/dev/null || true; REC_PID=""; fi
{ [[ -f "$METRICS_SNAP.tmp" ]] && mv -f "$METRICS_SNAP.tmp" "$METRICS_SNAP"; } 2>/dev/null || true

# ---------------------------------------------------------------------------- #
# Profile digest: folded stacks, flamegraph SVG, top-functions markdown
# ---------------------------------------------------------------------------- #

PROFILE_NOTE="verify_mode=$VERIFY_MODE $PERF_EVENT @ ${PROFILE_FREQ}Hz, ${PROFILE_SECONDS}s window"
if [[ -n "$PERF_PID" ]]; then
  kill "$PERF_PID" 2>/dev/null || true
  wait "$PERF_PID" 2>/dev/null || true
  PERF_PID=""
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

# ---------------------------------------------------------------------------- #
# Latency digest, verdict, throughput numbers, leg summary + meta
# ---------------------------------------------------------------------------- #

python3 "$DIGEST_PY" latency --traces "$TRACE_DIR" --metrics "$METRICS_SNAP" \
  --json-out "$OUT_DIR/latency.json" --title "$LEG $SHA ($VERIFY_MODE)" \
  > "$OUT_DIR/latency.md" 2>"$OUT_DIR/digest.log" \
  || { log "WARNING: latency digest failed"; rm -f "$OUT_DIR/latency.md"; }

VERDICT=""
if [[ -d "$REC_DIR" && -f "$REC_DIR/samples.jsonl" ]]; then
  cp "$REC_DIR/samples.jsonl" "$OUT_DIR/samples.jsonl" 2>/dev/null || true
  if python3 "$DASHBOARD_PY" --classify "$REC_DIR" \
       --verdict-out "$OUT_DIR/verdict.json" --label "$LEG-$SHA" \
       --ckpt-limit "$CKPT_LIMIT" --dl-limit "$DL_LIMIT" > "$OUT_DIR/verdict.md" 2>/dev/null; then
    VERDICT="$(awk -F'\\*\\*' '/^\*\*/{print $2; exit}' "$OUT_DIR/verdict.md")"
  fi
fi

if (( CLEAN_STOP )); then END_HEIGHT="$STOP_HEIGHT"; fi
BLOCKS=$((END_HEIGHT - START_HEIGHT))
TOTAL=$((T_END - T0)); (( TOTAL > 0 )) || TOTAL=1
POST=$TOTAL
[[ -n "$T_ESCAPE" ]] && POST=$((T_END - T_ESCAPE)); (( POST > 0 )) || POST=1
BPS="$(awk -v b="$BLOCKS" -v t="$TOTAL" 'BEGIN{printf "%.2f", b/t}')"
PBPS="$(awk -v b="$BLOCKS" -v t="$POST" 'BEGIN{printf "%.2f", b/t}')"
ERRS="$(grep -iE 'panic|ERROR committing|resetting state queue' "$LOGF" 2>/dev/null \
          | grep -viE 'zakura_network|peer' | head -3 || true)"

{
  echo "### Leg: $LEG — \`$SHA\` ($VERIFY_MODE mode)"
  echo ""
  echo "| leg | end height | blocks | time | blocks/s | post-commit blk/s | reached stop | verdict |"
  echo "|---|---:|---:|---:|---:|---:|---|---|"
  printf '| %s | %s | %s | %ss | %s | %s | %s | %s |\n' \
    "$LEG" "$END_HEIGHT" "$BLOCKS" "$TOTAL" "$BPS" "$PBPS" \
    "$( (( CLEAN_STOP )) && echo yes || echo "wall-capped" )" "${VERDICT:-n/a}"
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
    "leg": "$LEG", "sha": "$SHA", "verify_mode": "$VERIFY_MODE",
    "start_height": $START_HEIGHT, "end_height": $END_HEIGHT,
    "blocks": $BLOCKS, "seconds": $TOTAL, "bps": $BPS, "post_bps": $PBPS,
    "clean_stop": bool($CLEAN_STOP), "build_secs": $BUILD_SECS,
    "verdict": "${VERDICT}", "profiled": bool($PROFILED),
}, open(sys.argv[1], "w"), indent=2)
PY

log "leg $LEG done: $BLOCKS blocks in ${TOTAL}s ($BPS blk/s), verdict=${VERDICT:-n/a}"
