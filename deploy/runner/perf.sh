#!/usr/bin/env bash
#
# perf.sh — deterministic isolated perf-test harness for the 1.8M snapshot.
#
# Stands up two frozen serving nodes that hold a static block range and serve
# ONLY us over a private Zakura cohort (the dev_network feature, PR #262); the
# local bench node then peers exclusively with those two, so runs are repeatable
# and immune to other engineers churning the public Zakura fleet.
#
# Lifecycle (one-time / occasional):
#   perf.sh seed-serving     # deploy the 2 nodes, sync from public mainnet to SEED_HEIGHT
#   perf.sh status           # watch until both report height >= SEED_HEIGHT
#   perf.sh peers            # capture each node's node_id@ip:8234 into cohort.env
#   perf.sh freeze-serving   # cut them off from public, serve the static range
#
# Repeated loop:
#   perf.sh run <label> [stop] [met]      # bench from the snapshot, isolated to the 2
#   perf.sh analyze <label> [lo hi]       # steady-state window summary + verdict
#   perf.sh dashboard                     # live panels
#
# Helpers:
#   perf.sh build-local      # rebuild the instrumented bench binary (BENCH_BIN)
#   perf.sh stage-bin <name> # rebuild and copy BENCH_BIN to perf-artifacts/<name>
#   perf.sh verify-isolation [met]        # confirm the bench node sees only the cohort
#   perf.sh logs [label]     # follow the bench node log (drift spam filtered; RAW=1 for all)
set -euo pipefail

RUNNER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(git -C "$RUNNER_DIR" rev-parse --show-toplevel)"
ENV_FILE="$RUNNER_DIR/cohort.env"

[ -f "$ENV_FILE" ] || { echo "FATAL: $ENV_FILE not found (copy/edit the source-of-truth)"; exit 1; }
# shellcheck source=/dev/null
source "$ENV_FILE"

DEPLOYER="$REPO/deploy/deployer/deploy.py"
NODES_TMPL="$RUNNER_DIR/nodes.toml.tmpl"
FEED_RUN="$RUNNER_DIR/feed_run.sh"
ANALYZE="$RUNNER_DIR/feed_analyze.py"
DASH="$RUNNER_DIR/zebra-metrics-dashboard.py"
BENCH_CONFIG="$RUNNER_DIR/zebra-bench-config.toml"
PERF_ARTIFACT_DIR="${PERF_ARTIFACT_DIR:-$REPO/perf-artifacts}"
ZAKURA_PORT="${ZAKURA_PORT:-8234}"
LOG_DIR="$RUNNER_DIR/.logs"

die()  { echo "FATAL: $*" >&2; exit 1; }
note() { echo "[perf] $*" >&2; }

validate_artifact_name() {
  local name="${1:-}"
  [ -n "$name" ] || die "missing binary name"
  case "$name" in
    */*|.|..) die "binary name must be a file name under perf-artifacts: $name" ;;
  esac
}

staged_bench_bin() {
  local name="$1"
  validate_artifact_name "$name"
  printf '%s/%s\n' "$PERF_ARTIFACT_DIR" "$name"
}

selected_bench_bin() {
  if [ -n "${PERF_BIN_NAME:-}" ]; then
    staged_bench_bin "$PERF_BIN_NAME"
  else
    printf '%s\n' "$BENCH_BIN"
  fi
}

# --- rendering helpers ------------------------------------------------------

# TOML array of the captured cohort peers (or [] if not captured yet).
bootstrap_toml() {
  local items=()
  [ -n "${NODE_A_PEER:-}" ] && items+=("\"$NODE_A_PEER\"")
  [ -n "${NODE_B_PEER:-}" ] && items+=("\"$NODE_B_PEER\"")
  if [ ${#items[@]} -eq 0 ]; then echo "[]"; else
    local IFS=, ; echo "[${items[*]}]"
  fi
}

# Render nodes.toml.tmpl -> a temp deployer config. Args: p2p_stack (dual|zakura|...)
render_nodes() {
  local p2p_stack="$1" out
  out="$(mktemp /tmp/perf-nodes.XXXXXX.toml)"
  sed -e "s#@@COMMIT@@#${SERVE_COMMIT}#g" \
      -e "s#@@COHORT@@#${COHORT_TAG}#g" \
      -e "s#@@BOOTSTRAP@@#$(bootstrap_toml)#g" \
      -e "s#@@P2P_STACK@@#${p2p_stack}#g" \
      -e "s#@@STORAGE@@#archive#g" \
      "$NODES_TMPL" > "$out"
  echo "$out"
}

# Render the bench config with the cohort tag + peers substituted in.
render_bench_config() {
  local out
  out="$(mktemp /tmp/perf-bench.XXXXXX.toml)"
  sed -e "s#@@COHORT@@#${COHORT_TAG}#g" \
      -e "s#@@BOOTSTRAP@@#$(bootstrap_toml)#g" \
      "$BENCH_CONFIG" > "$out"
  echo "$out"
}

deployer() { python3 "$DEPLOYER" "$@"; }

# --- subcommands ------------------------------------------------------------

cmd_seed_serving() {
  note "deploying serving nodes with p2p_stack=dual (sync from public mainnet to >= ${SEED_HEIGHT})"
  local nodes; nodes="$(render_nodes dual)"
  deployer deploy --config "$nodes"
  note "seeding. Poll with: $0 status   (wait until both heights >= ${SEED_HEIGHT})"
}

cmd_peers() {
  mkdir -p "$LOG_DIR"
  local nodes; nodes="$(render_nodes dual)"
  note "fetching node logs to read each Zakura identity"
  deployer logs fetch --config "$nodes" --out-dir "$LOG_DIR"

  local id_a id_b
  id_a="$(grep -h 'Zakura P2P endpoint ready' "$LOG_DIR/serve-a.log" 2>/dev/null \
          | grep -oE 'node_id=[0-9a-f]+' | tail -1 | cut -d= -f2 || true)"
  id_b="$(grep -h 'Zakura P2P endpoint ready' "$LOG_DIR/serve-b.log" 2>/dev/null \
          | grep -oE 'node_id=[0-9a-f]+' | tail -1 | cut -d= -f2 || true)"
  [ -n "$id_a" ] || die "could not read serve-a node_id (is Zakura P2P up? check $LOG_DIR/serve-a.log)"
  [ -n "$id_b" ] || die "could not read serve-b node_id (check $LOG_DIR/serve-b.log)"

  local peer_a="${id_a}@${NODE_A_IP}:${ZAKURA_PORT}"
  local peer_b="${id_b}@${NODE_B_IP}:${ZAKURA_PORT}"
  # Persist into cohort.env (single source of truth).
  sed -i -e "s#^NODE_A_PEER=.*#NODE_A_PEER=\"${peer_a}\"#" \
         -e "s#^NODE_B_PEER=.*#NODE_B_PEER=\"${peer_b}\"#" "$ENV_FILE"
  note "captured into cohort.env:"
  note "  NODE_A_PEER=$peer_a"
  note "  NODE_B_PEER=$peer_b"
}

cmd_freeze_serving() {
  [ -n "${NODE_A_PEER:-}" ] && [ -n "${NODE_B_PEER:-}" ] \
    || die "NODE_*_PEER empty — run '$0 peers' first"
  note "redeploying serving nodes with p2p_stack=zakura (freeze) — they now serve a static range"
  local nodes; nodes="$(render_nodes zakura)"
  deployer deploy --config "$nodes"
  note "frozen. Serving cohort '${COHORT_TAG}' to the bench node only."
}

cmd_status() {
  local nodes; nodes="$(render_nodes zakura)"
  deployer status --config "$nodes"
}

cmd_run() {
  local label="${1:?usage: perf.sh run <label> [stop] [met]}"; shift || true
  [ -n "${NODE_A_PEER:-}" ] || die "NODE_*_PEER empty — seed/peers/freeze the cohort first"
  local bin; bin="$(selected_bench_bin)"
  if [ -n "${PERF_BIN_NAME:-}" ] && [ ! -x "$bin" ]; then
    die "staged binary not executable: $bin (run 'make perf-build-stage-bin $PERF_BIN_NAME' first)"
  fi
  local cfg; cfg="$(render_bench_config)"
  note "bench '$label' isolated to cohort '${COHORT_TAG}' ($(bootstrap_toml))"
  note "using bench binary: $bin"
  CONFIG_SRC="$cfg" "$FEED_RUN" "$label" "$bin" "$@"
}

cmd_analyze() {
  local label="${1:?usage: perf.sh analyze <label> [lo hi]}"; shift || true
  python3 "$ANALYZE" "${BENCH_WORK_DIR:-/root/wal-bench}/feedrun-${label}.csv" "$@"
}

cmd_dashboard() {
  # Re-running should always work: drop any prior dashboard still holding the
  # HTTP port (otherwise the new one dies with "Address already in use"). Safe
  # from inside perf.sh — our own argv does not contain the dashboard path.
  if pkill -f "zebra-metrics-dashboard.py" 2>/dev/null; then
    note "stopped a previous dashboard instance"
    sleep 1
  fi
  note "dashboard serving on http://0.0.0.0:8090/ (Ctrl-C to stop)"
  python3 "$DASH" "$@"
}

cmd_build_local() {
  : "${CARGO_TARGET_DIR:=/mnt/roman-dev-2-data/cargo-target-vct}"
  export CARGO_TARGET_DIR
  note "building instrumented bench binary (commit-metrics) -> $BENCH_BIN"
  ( cd "$REPO" && cargo build --release -p zakura --features commit-metrics --locked )
  local bench_dir bench_tmp
  bench_dir="$(dirname "$BENCH_BIN")"
  bench_tmp="$(mktemp "${bench_dir}/.$(basename "$BENCH_BIN").XXXXXX")"
  if install -m 0755 "$CARGO_TARGET_DIR/release/zebrad" "$bench_tmp"; then
    mv -f "$bench_tmp" "$BENCH_BIN"
  else
    rm -f "$bench_tmp"
    return 1
  fi
  note "installed $BENCH_BIN"
}

cmd_stage_bin() {
  local name="${1:?usage: perf.sh stage-bin <name>}"
  local staged_bin; staged_bin="$(staged_bench_bin "$name")"

  cmd_build_local

  mkdir -p "$PERF_ARTIFACT_DIR"
  local staged_tmp
  staged_tmp="$(mktemp "${PERF_ARTIFACT_DIR}/.${name}.XXXXXX")"
  if install -m 0755 "$BENCH_BIN" "$staged_tmp"; then
    mv -f "$staged_tmp" "$staged_bin"
  else
    rm -f "$staged_tmp"
    return 1
  fi

  note "staged $staged_bin"
}

cmd_bench_bin() {
  local bin; bin="$(selected_bench_bin)"
  if [ -n "${PERF_BIN_NAME:-}" ] && [ ! -x "$bin" ]; then
    die "staged binary not executable: $bin (run 'make perf-build-stage-bin $PERF_BIN_NAME' first)"
  fi
  printf '%s\n' "$bin"
}

cmd_verify_isolation() {
  local met="${1:-19998}"
  local url="http://127.0.0.1:${met}/metrics"
  note "scraping $url for cohort isolation signals"
  local body
  if ! body="$(curl -fsS "$url" 2>/dev/null)"; then
    die "no metrics at $url — start a bench first ('perf.sh run <label>'), then re-run with the run's metrics port."
  fi
  echo "--- zakura peer / connection metrics ---"
  echo "$body" | grep -iE 'zakura.*(peer|conn|accepted)' | grep -v '^#' || echo "(none)"
  echo "--- cohort reject counters (expect ZERO against our peers) ---"
  echo "$body" | grep -iE 'wrong_network|wrong_chain|reject' | grep -v '^#' || echo "(none)"
  echo "Expectation: a small, stable peer count (the 2 serving nodes) and no growing rejects."
}

cmd_logs() {
  local label="${1:-r1}"
  # Mirror feed_run.sh's log path resolution: BENCH_LOG_DIR, else BENCH_WORK_DIR.
  local dir="${BENCH_LOG_DIR:-${BENCH_WORK_DIR:-/root/wal-bench}}"
  local log="$dir/feedrun-${label}.log"
  [ -f "$log" ] || die "no log at $log (run 'perf.sh run $label' first)"
  # The Zakura block-sync guard floods this file with 'byte-budget audit drift'
  # WARNs at hundreds of lines/sec, drowning real progress. Filter them by
  # default; RAW=1 shows everything. Initial backlog is filtered from a wide
  # window so useful context survives the spam.
  if [ -n "${RAW:-}" ]; then
    note "tailing $log (RAW — all lines; Ctrl-C to stop)"
    exec tail -n "${LINES:-40}" -f "$log"
  fi
  note "tailing $log (drift spam filtered; RAW=1 for all; Ctrl-C to stop)"
  tail -n "${LINES:-800}" -f "$log" | grep --line-buffered -v "byte-budget audit drift"
}

usage() { sed -n '3,29p' "${BASH_SOURCE[0]}"; }

case "${1:-}" in
  seed-serving)     shift; cmd_seed_serving "$@" ;;
  peers)            shift; cmd_peers "$@" ;;
  freeze-serving)   shift; cmd_freeze_serving "$@" ;;
  status)           shift; cmd_status "$@" ;;
  run)              shift; cmd_run "$@" ;;
  analyze)          shift; cmd_analyze "$@" ;;
  dashboard)        shift; cmd_dashboard "$@" ;;
  build-local)      shift; cmd_build_local "$@" ;;
  stage-bin)        shift; cmd_stage_bin "$@" ;;
  bench-bin)        shift; cmd_bench_bin "$@" ;;
  verify-isolation) shift; cmd_verify_isolation "$@" ;;
  logs)             shift; cmd_logs "$@" ;;
  ""|-h|--help|help) usage ;;
  *) echo "unknown subcommand: $1" >&2; usage; exit 1 ;;
esac
