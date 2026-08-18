#!/usr/bin/env bash
#
# Drive and assert the Zakura regtest e2e.
#
# Topology (all on 127.0.0.1 via host networking):
#   node1  dual-stack seed (legacy TCP + Zakura)  rpc 18232  metrics 19001
#          fixed iroh identity, stable Zakura QUIC port 18234
#   node2  PURE Zakura-only (p2p_stack = zakura)   rpc 18332  metrics 19002
#          joins solely by dialing node1's Zakura bootstrap_peers entry
#   node3  legacy-only (p2p_stack = legacy)       rpc 18432  metrics 19003  -> node1
#   node4  dual-stack (legacy TCP + Zakura)       rpc 18532  metrics 19004  -> node1
#          dials node1 over legacy TCP, then upgrades to Zakura
#
# Asserts:
#   1. all four nodes come up on a fresh Regtest chain,
#   2. legacy TCP compatibility: node3 peers with node1 (getpeerinfo),
#   3. the legacy->Zakura upgrade ran (zakura_p2p_handshake_upgraded on node1/node4),
#   4. the pure Zakura-only node2 has zero legacy peers (no legacy stack at all),
#   4b. the pure Zakura-only node2 bootstraps the genesis block over Zakura: with
#      the Regtest genesis self-seed disabled (sync.debug_skip_regtest_genesis_self_seed)
#      and no legacy stack, node2 can only reach genesis by downloading it from
#      node1 over Zakura — the production Mainnet/Testnet bootstrap path,
#   5. blocks generated on node1 propagate to the pure-Zakura node2 AND the
#      legacy-only node3 — so node2, which has no legacy stack, proves
#      pure-Zakura propagation,
#   6. Zakura v2 nodes reach sync.block.verified_tip.height ==
#      sync.block.best_header_tip.height after gossip propagation,
#   7. Reset pure-Zakura node2 from scratch while node1 remains idle at tip.
#      Node2 downloads the configured checkpoint prefix through compatibility requests.
#      Node2 hands ownership to native header sync.
#      Kind-6 block sync downloads the remaining suffix.
#      Node1 does not advertise the old blocks again.
#   8. a non-finalized reorg converges with no block-sync byte-budget leak.
#
# Each container runs a zakurad binary bind-mounted into debian:trixie-slim.
# Linux hosts use the host-built debug binary. On macOS, the harness builds and
# caches a Linux release artifact because a Mach-O host binary cannot run in the
# Linux container. Override with ZAKURAD_BIN=/path/to/a/Linux/zakurad.
#
# Usage: docker/zakura-regtest-e2e/run.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/../docker-compose.zakura-regtest-e2e.yml"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

log()  { printf '\n=== %s ===\n' "$*"; }
fail() { printf '\nFAILED: %s\n' "$*" >&2; exit 1; }

sed_in_place() {
  local script="$1"
  local file="$2"
  local tmp="${file}.tmp.$$"
  sed "${script}" "${file}" > "${tmp}" \
    && mv "${tmp}" "${file}" \
    || { rm -f "${tmp}"; return 1; }
}

# Rewrite a bind-mounted config without replacing its inode.
# The restart matrix restarts the debug-stop container after the stop.
# Preserving the inode lets that container observe the restored config.
rewrite_mounted_config_in_place() {
  local script="$1"
  local file="$2"
  local tmp="${file}.tmp.$$"
  sed "${script}" "${file}" > "${tmp}" \
    && cp "${tmp}" "${file}" \
    && rm -f "${tmp}" \
    || { rm -f "${tmp}"; return 1; }
}

ZAKURA_E2E_MODE="${ZAKURA_E2E_MODE:-smoke}"

# Keep the dedicated long lanes deep.
# Scale the restart matrix to its checkpoint boundaries.
# `ZAKURA_E2E_LONG_BLOCKS` overrides the mode-specific total.
case "${ZAKURA_E2E_MODE}" in
  smoke)
    DEFAULT_GENERATE_BLOCKS=3
    DEFAULT_CATCHUP_BLOCKS=200
    DEFAULT_CHECKPOINT_INTERVAL=100
    DEFAULT_PROPAGATE_TIMEOUT=150
    DEFAULT_CATCHUP_TIMEOUT=300
    ZAKURA_E2E_DISABLE_CHECKPOINTS=0
    ZAKURA_E2E_RESTART_MATRIX=0
    ZAKURA_E2E_REQUIRE_HANDOFF=0
    ;;
  pr-gate)
    DEFAULT_GENERATE_BLOCKS=3
    DEFAULT_CATCHUP_BLOCKS=160
    DEFAULT_CHECKPOINT_INTERVAL=80
    DEFAULT_PROPAGATE_TIMEOUT=180
    DEFAULT_CATCHUP_TIMEOUT=450
    ZAKURA_E2E_DISABLE_CHECKPOINTS=0
    ZAKURA_E2E_RESTART_MATRIX=0
    ZAKURA_E2E_REQUIRE_HANDOFF=1
    ;;
  checkpoint-long)
    ZAKURA_E2E_LONG_BLOCKS="${ZAKURA_E2E_LONG_BLOCKS:-4000}"
    DEFAULT_GENERATE_BLOCKS=3
    DEFAULT_CATCHUP_BLOCKS=$(( ZAKURA_E2E_LONG_BLOCKS - DEFAULT_GENERATE_BLOCKS ))
    DEFAULT_CHECKPOINT_INTERVAL=400
    DEFAULT_PROPAGATE_TIMEOUT=300
    DEFAULT_CATCHUP_TIMEOUT=1200
    ZAKURA_E2E_DISABLE_CHECKPOINTS=0
    ZAKURA_E2E_RESTART_MATRIX=0
    ZAKURA_E2E_REQUIRE_HANDOFF=1
    ;;
  no-checkpoint-long)
    ZAKURA_E2E_LONG_BLOCKS="${ZAKURA_E2E_LONG_BLOCKS:-4000}"
    DEFAULT_GENERATE_BLOCKS=3
    DEFAULT_CATCHUP_BLOCKS=$(( ZAKURA_E2E_LONG_BLOCKS - DEFAULT_GENERATE_BLOCKS ))
    DEFAULT_CHECKPOINT_INTERVAL=0
    DEFAULT_PROPAGATE_TIMEOUT=300
    DEFAULT_CATCHUP_TIMEOUT=1800
    ZAKURA_E2E_DISABLE_CHECKPOINTS=1
    ZAKURA_E2E_RESTART_MATRIX=0
    ZAKURA_E2E_REQUIRE_HANDOFF=0
    ;;
  restart-matrix)
    ZAKURA_E2E_LONG_BLOCKS="${ZAKURA_E2E_LONG_BLOCKS:-400}"
    DEFAULT_GENERATE_BLOCKS=3
    DEFAULT_CATCHUP_BLOCKS=$(( ZAKURA_E2E_LONG_BLOCKS - DEFAULT_GENERATE_BLOCKS ))
    DEFAULT_CHECKPOINT_INTERVAL=40
    DEFAULT_PROPAGATE_TIMEOUT=300
    DEFAULT_CATCHUP_TIMEOUT=1800
    ZAKURA_E2E_DISABLE_CHECKPOINTS=0
    ZAKURA_E2E_RESTART_MATRIX=1
    ZAKURA_E2E_REQUIRE_HANDOFF=1
    ;;
  header-faults)
    DEFAULT_GENERATE_BLOCKS=3
    DEFAULT_CATCHUP_BLOCKS=240
    DEFAULT_CHECKPOINT_INTERVAL=80
    DEFAULT_PROPAGATE_TIMEOUT=180
    DEFAULT_CATCHUP_TIMEOUT=600
    ZAKURA_E2E_DISABLE_CHECKPOINTS=0
    ZAKURA_E2E_RESTART_MATRIX=1
    ZAKURA_E2E_REQUIRE_HANDOFF=1
    ZAKURA_E2E_REQUIRE_V7_IDS=1
    ;;
  *)
    fail "unknown ZAKURA_E2E_MODE='${ZAKURA_E2E_MODE}' (expected smoke, pr-gate, checkpoint-long, no-checkpoint-long, restart-matrix, header-faults)"
    ;;
esac

ZAKURA_E2E_REQUIRE_V7_IDS="${ZAKURA_E2E_REQUIRE_V7_IDS:-0}"

(( DEFAULT_CATCHUP_BLOCKS >= 0 )) || fail "ZAKURA_E2E_LONG_BLOCKS must be >= ${DEFAULT_GENERATE_BLOCKS}"

# Generate at least three blocks.
# Zebra discards locator responses that extend only one block.
# Three blocks also prevent a one-block remainder after a dropped response.
GENERATE_BLOCKS="${GENERATE_BLOCKS:-${DEFAULT_GENERATE_BLOCKS}}"
# Mine extra blocks on node1 before the from-scratch reset.
# The kind-6 catch-up then downloads enough bodies to fill the inbound wire queue.
# Set this value to 0 to keep the three-block catch-up.
CATCHUP_BLOCKS="${CATCHUP_BLOCKS:-${DEFAULT_CATCHUP_BLOCKS}}"
READY_TIMEOUT="${READY_TIMEOUT:-120}"
# Zakura's application idle reaper retires a hard-stopped QUIC peer after 150 seconds.
# This timeout also includes the JSONL writer's approximately 17-second flush interval.
NODE4_DISCONNECT_TIMEOUT="${NODE4_DISCONNECT_TIMEOUT:-210}"
# Propagation to the Zakura peer can take a little while: the dual-stack tries
# the (empty) legacy peer set first, and the legacy->Zakura upgrade re-dials a
# few times before the connection settles. The loop exits as soon as the block
# arrives, so a generous ceiling only matters on failure.
PROPAGATE_TIMEOUT="${PROPAGATE_TIMEOUT:-${DEFAULT_PROPAGATE_TIMEOUT}}"
# The from-scratch reset catch-up is given its own, larger ceiling. After a
# restart the reconnecting node returns under the same identity but reuses its
# stable loopback endpoint, so the seed briefly holds the dead pre-reset
# connection; re-establishing the kind-6 block-sync peer can take until the
# Zakura app idle reaper releases that stale connection (bounded by the ~150s
# idle window, but variable). Recovery is reliable but not fast, so the 120s
# READY_TIMEOUT used for the (fast) startup assertions is too tight here. This
# ceiling only matters on failure: the waits exit as soon as catch-up starts.
CATCHUP_TIMEOUT="${CATCHUP_TIMEOUT:-${DEFAULT_CATCHUP_TIMEOUT}}"
# The Zakura JSONL trace writer (zakura-jsonl-trace) only flushes to disk every
# DEFAULT_FILE_FLUSH_INTERVAL (~17s) or after DEFAULT_BUFFER_FLUSH_BYTES (256
# KiB), and production zakurad uses JsonlTracer::spawn (no guard / no shutdown
# flush), so stopping the nodes does not flush the tail. The oracle reads the
# trace files, so it must wait for the final commit_finish rows to be flushed
# before running, or it sees commit_start rows with no matching finish. This
# ceiling only needs to exceed the writer's flush interval; it exits as soon as
# every commit_start has a matching commit_finish.
TRACE_FLUSH_TIMEOUT="${TRACE_FLUSH_TIMEOUT:-45}"
CHECKPOINT_INTERVAL="${CHECKPOINT_INTERVAL:-${DEFAULT_CHECKPOINT_INTERVAL}}"
RUN_LABEL="${ZAKURA_REGTEST_E2E_LABEL:-zakura-${ZAKURA_E2E_MODE}}"
RUN_LABEL_DIGEST=$(printf '%s' "${RUN_LABEL}" | sha256sum | cut -c1-16)
ZAKURA_NODE2_STATE_VOLUME="${ZAKURA_NODE2_STATE_VOLUME:-zakura-regtest-e2e-node2-state-${RUN_LABEL_DIGEST}}"
[[ "${ZAKURA_NODE2_STATE_VOLUME}" =~ ^zakura-regtest-e2e-node2-state-[0-9a-f]{16}$ ]] \
  || fail "unsafe node2 state volume name: ${ZAKURA_NODE2_STATE_VOLUME}"
export ZAKURA_NODE2_STATE_VOLUME

command -v docker >/dev/null || fail "docker is required"
command -v jq >/dev/null || fail "jq is required to parse RPC responses"
command -v python3 >/dev/null || fail "python3 is required to run the trace oracle"

# Ensure a container-compatible zakurad binary exists. Docker Desktop runs Linux
# containers on macOS, so its bind-mounted executable must be Linux too.
if [[ -z "${ZAKURAD_BIN:-}" && "$(uname -s)" == "Darwin" ]]; then
  ZAKURAD_BIN="${REPO_DIR}/target/zakura-regtest-e2e-linux/zakurad"
  log "building cached Linux zakurad for Docker Desktop"
  mkdir -p "$(dirname "${ZAKURAD_BIN}")"
  docker build \
    --file "${REPO_DIR}/docker/ubuntu-package.Dockerfile" \
    --target artifact \
    --output "type=local,dest=$(dirname "${ZAKURAD_BIN}")" \
    "${REPO_DIR}"
else
  ZAKURAD_BIN="${ZAKURAD_BIN:-${REPO_DIR}/target/debug/zakurad}"
fi
if [[ ! -x "${ZAKURAD_BIN}" ]]; then
  log "building host zakurad (debug) — no in-container build"
  ( cd "${REPO_DIR}" && CXXFLAGS="-include cstdint" cargo build -p zakura --bin zakurad )
fi
[[ -x "${ZAKURAD_BIN}" ]] || fail "zakurad binary not found at ${ZAKURAD_BIN}"
docker run --rm \
  -v "${ZAKURAD_BIN}:/usr/local/bin/zakurad:ro" \
  debian:trixie-slim \
  /usr/local/bin/zakurad --version >/dev/null 2>&1 \
  || fail "zakurad binary is not executable in the Linux container: ${ZAKURAD_BIN}"
export ZAKURAD_BIN
ZAKURA_E2E_TRACE_DIR="${ZAKURA_E2E_TRACE_DIR:-/tmp/zakura-regtest-e2e-traces-${RUN_LABEL}}"
export ZAKURA_E2E_TRACE_DIR
mkdir -p \
  "${ZAKURA_E2E_TRACE_DIR}/node1" \
  "${ZAKURA_E2E_TRACE_DIR}/node2" \
  "${ZAKURA_E2E_TRACE_DIR}/node3" \
  "${ZAKURA_E2E_TRACE_DIR}/node4"
TIMELINE_FILE="${ZAKURA_E2E_TRACE_DIR}/timeline.jsonl"
CONFIG_DIR="$(mktemp -d "${TMPDIR:-/tmp}/zakura-regtest-e2e-configs.XXXXXX")"
for node in 1 2 3 4; do
  cp "${SCRIPT_DIR}/node${node}.toml" "${CONFIG_DIR}/node${node}.toml"
done
if [[ "${ZAKURA_E2E_DISABLE_CHECKPOINTS}" == "1" ]]; then
  sed_in_place \
    's|^network = "Regtest"$|network = { params = { checkpoints = false } }|' \
    "${CONFIG_DIR}/node2.toml"
  grep -q '^network = { params = { checkpoints = false } }' "${CONFIG_DIR}/node2.toml" \
    || fail "failed to disable node2 Regtest checkpoints"
fi
if [[ "${ZAKURA_E2E_RESTART_MATRIX}" == "1" ]]; then
  sed_in_place \
    's|^ephemeral = true$|cache_dir = "/tmp/zakura-node2-state"\nephemeral = false\ndebug_skip_non_finalized_state_backup_task = true|' \
    "${CONFIG_DIR}/node2.toml"
  grep -q '^ephemeral = false$' "${CONFIG_DIR}/node2.toml" \
    || fail "failed to make node2 state persistent for restart-matrix"
  grep -q '^debug_skip_non_finalized_state_backup_task = true$' "${CONFIG_DIR}/node2.toml" \
    || fail "failed to make restart-matrix non-finalized backups synchronous"
fi
# Header sync has no block-relay toggle.
# The from-scratch reset removes gossip and exercises kind-6 block sync.
export ZAKURA_NODE1_CONFIG="${CONFIG_DIR}/node1.toml"
export ZAKURA_NODE2_CONFIG="${CONFIG_DIR}/node2.toml"
export ZAKURA_NODE3_CONFIG="${CONFIG_DIR}/node3.toml"
export ZAKURA_NODE4_CONFIG="${CONFIG_DIR}/node4.toml"

log "run mode: ${ZAKURA_E2E_MODE} (${RUN_LABEL})"
log "using zakurad binary: ${ZAKURAD_BIN}"
log "writing Zakura traces under: ${ZAKURA_E2E_TRACE_DIR}"

ORACLE_RAN=0
OPTIONAL_NODE4_QUIESCED=0

trace_dir_has_jsonl() {
  [[ -d "${ZAKURA_E2E_TRACE_DIR}" ]] \
    && find "${ZAKURA_E2E_TRACE_DIR}"/node* -maxdepth 1 -type f -name '*.jsonl' -print -quit 2>/dev/null | grep -q .
}

assert_trace_layout() {
  local missing=0
  for file in \
    node1/commit_state.jsonl \
    node1/block_sync.jsonl \
    node1/header_sync.jsonl \
    node2/commit_state.jsonl \
    node2/block_sync.jsonl \
    node2/header_sync.jsonl \
    node4/commit_state.jsonl \
    node4/block_sync.jsonl \
    node4/header_sync.jsonl
  do
    if [[ ! -s "${ZAKURA_E2E_TRACE_DIR}/${file}" ]]; then
      printf '  missing expected trace file: %s\n' "${ZAKURA_E2E_TRACE_DIR}/${file}" >&2
      missing=1
    fi
  done
  if [[ "${missing}" != "0" ]]; then
    log "trace directory contents"
    find "${ZAKURA_E2E_TRACE_DIR}" -maxdepth 3 -type f -print | sort >&2 || true
    fail "Zakura traces were not written to the expected node*/ layout"
  fi
}

# Wait until each node's traces are fully flushed to disk before the oracle reads
# them. The writer (zakura-jsonl-trace) flushes on a ~17s timer / 256 KiB, and
# production zakurad never flushes on shutdown, so a trace read right after the
# final commits has a buffered tail. We require two things to be on disk:
#   1. commit_state.jsonl: every commit_start has a matching commit_finish
#      (guards commit_start_has_finish / checkpoint_to_full_handoff_observed).
#   2. block_sync.jsonl: the last block_sync_state row is drained, i.e.
#      applying+budget_reserved+reorder+outstanding == 0 (guards
#      final_block_sync_state_has_no_leaks). The reactor emits these release
#      rows just after the commit_finish rows, so they flush a cycle later.
# Best-effort: on timeout we log and run the oracle anyway so a genuine stall or
# real leak still surfaces.
wait_for_trace_flush() {
  trace_dir_has_jsonl || return 0
  local deadline=$((SECONDS + TRACE_FLUSH_TIMEOUT)) node file starts finishes pending last leak
  log "waiting for Zakura traces to flush before the oracle"
  while (( SECONDS < deadline )); do
    pending=0
    for node in node1 node2 node4; do
      file="${ZAKURA_E2E_TRACE_DIR}/${node}/commit_state.jsonl"
      if [[ -s "${file}" ]]; then
        starts=$(grep -c 'commit_start' "${file}" 2>/dev/null || true)
        finishes=$(grep -c 'commit_finish' "${file}" 2>/dev/null || true)
        if (( finishes < starts )); then
          printf '  %s commit_state starts=%s finishes=%s (waiting for flush)\n' \
            "${node}" "${starts}" "${finishes}"
          pending=1
        fi
      fi
      file="${ZAKURA_E2E_TRACE_DIR}/${node}/block_sync.jsonl"
      if [[ -s "${file}" ]]; then
        last=$(grep 'block_sync_state' "${file}" 2>/dev/null | tail -1)
        if [[ -n "${last}" ]]; then
          leak=$(printf '%s' "${last}" \
            | jq -r '[(.applying//0),(.budget_reserved//0),(.reorder//0),(.outstanding//0)]|add' \
            2>/dev/null || printf '1')
          if [[ "${leak}" != "0" ]]; then
            printf '  %s block_sync_state not yet drained (applying+budget+reorder+outstanding=%s, waiting for flush)\n' \
              "${node}" "${leak}"
            pending=1
          fi
        fi
      fi
    done
    (( pending == 0 )) && { log "traces flushed"; return 0; }
    sleep 3
  done
  log "trace flush wait timed out after ${TRACE_FLUSH_TIMEOUT}s; running oracle anyway"
}

wait_for_commit_trace_balance() {
  local node="$1" label="$2"
  local file="${ZAKURA_E2E_TRACE_DIR}/${node}/commit_state.jsonl"
  local deadline=$((SECONDS + TRACE_FLUSH_TIMEOUT)) starts finishes

  [[ -s "${file}" ]] || fail "${label} commit trace is missing"
  log "waiting for ${label} commit trace to flush before reset"
  while (( SECONDS < deadline )); do
    starts=$(grep -c 'commit_start' "${file}" 2>/dev/null || true)
    finishes=$(grep -c 'commit_finish' "${file}" 2>/dev/null || true)
    printf '  %s commit_state starts=%s finishes=%s\n' \
      "${label}" "${starts}" "${finishes}"
    (( starts == finishes )) && return 0
    sleep 3
  done
  fail "${label} commit trace did not balance within ${TRACE_FLUSH_TIMEOUT}s"
}

run_trace_oracle() {
  trace_dir_has_jsonl || return 0
  ORACLE_RAN=1
  wait_for_trace_flush
  log "running Zakura trace oracle"
  local oracle_args=(
    "--commit-elapsed-ms" "${ZAKURA_E2E_ORACLE_COMMIT_ELAPSED_MS:-1800000}"
    "--persistent-lag-seconds" "${ZAKURA_E2E_ORACLE_PERSISTENT_LAG_SECONDS:-180}"
    "--handoff-stall-seconds" "${ZAKURA_E2E_ORACLE_HANDOFF_STALL_SECONDS:-180}"
  )
  if [[ "${ZAKURA_E2E_REQUIRE_HANDOFF}" == "1" ]]; then
    oracle_args+=("--require-handoff-boundary")
  fi
  if [[ "${ZAKURA_E2E_REQUIRE_V7_IDS}" == "1" ]]; then
    oracle_args+=("--require-v7-request-ids")
  fi
  if ! strict_upgrade; then
    oracle_args+=("--optional-lag-node" "node4")
  fi
  python3 "${SCRIPT_DIR}/trace_oracle.py" "${oracle_args[@]}" "${ZAKURA_E2E_TRACE_DIR}"
}

cleanup() {
  local status=$?
  set +e
  snapshot_timeline "cleanup" || true
  if [[ "${ORACLE_RAN}" != "1" ]]; then
    run_trace_oracle
  fi
  log "node logs (tail)"
  docker compose -f "${COMPOSE_FILE}" logs --tail=30 || true
  if [[ -d "${ZAKURA_E2E_TRACE_DIR}" ]]; then
    docker compose -f "${COMPOSE_FILE}" logs --no-color --timestamps \
      > "${ZAKURA_E2E_TRACE_DIR}/docker-compose.log" 2>&1 || true
  fi
  log "tearing down"
  docker compose -f "${COMPOSE_FILE}" down --volumes --remove-orphans --timeout 5 || true
  if docker volume inspect "${ZAKURA_NODE2_STATE_VOLUME}" >/dev/null 2>&1; then
    log "node2 run-scoped state volume leaked after compose teardown: ${ZAKURA_NODE2_STATE_VOLUME}"
    docker volume rm "${ZAKURA_NODE2_STATE_VOLUME}" >/dev/null 2>&1 || true
    if docker volume inspect "${ZAKURA_NODE2_STATE_VOLUME}" >/dev/null 2>&1; then
      log "could not remove leaked node2 run-scoped state volume"
      (( status == 0 )) && status=1
    fi
  fi
  rm -rf "${CONFIG_DIR}"
  return "${status}"
}
trap cleanup EXIT

# Fetch localhost-only node endpoints from inside node1's Linux network
# namespace. Docker Desktop does not expose host-network container ports to the
# macOS host unless its optional host-networking feature is enabled.
container_http() {
  local port="$1" path="$2" payload="$3" max_time="$4"
  docker exec zakura-node-1 perl -MIO::Socket::INET -e '
    my ($port, $path, $payload, $timeout) = @ARGV;
    $SIG{ALRM} = sub { exit 28 };
    alarm $timeout;
    my $socket = IO::Socket::INET->new(
      PeerAddr => "127.0.0.1",
      PeerPort => $port,
      Proto => "tcp",
      Timeout => $timeout,
    ) or exit 1;
    my $method = length($payload) ? "POST" : "GET";
    my $headers = "Host: 127.0.0.1\r\nConnection: close\r\n";
    if (length($payload)) {
      $headers .= "Content-Type: application/json\r\nContent-Length: " . length($payload) . "\r\n";
    }
    print {$socket} "$method $path HTTP/1.1\r\n$headers\r\n$payload";
    my $response = do { local $/; <$socket> };
    $response =~ /\AHTTP\/\S+ 2\d\d[^\r\n]*\r?\n.*?\r?\n\r?\n(.*)\z/s or exit 1;
    print $1;
  ' "${port}" "${path}" "${payload}" "${max_time}"
}

# rpc <port> <method> [json-params] [max-time-seconds] -> raw JSON-RPC response
rpc() {
  local port="$1" method="$2" params="${3:-[]}" max_time="${4:-10}"
  container_http \
    "${port}" \
    "/" \
    "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"${method}\",\"params\":${params}}" \
    "${max_time}"
}

# metric <port> <name> -> counter value (0 if absent)
metric() {
  local port="$1" name="$2"
  container_http "${port}" "/metrics" "" 5 2>/dev/null \
    | awk -v n="${name}" '$1==n {v=$2} END {print (v==""?0:v)}'
}

timeline_node_snapshot() {
  local phase="$1" node="$2" rpc_port="$3" metrics_port="$4"
  local rpc_height best_header verified zakura_peers legacy_peers
  local requests bodies_received bodies_served budget reorder applying outstanding

  rpc_height=$(block_count "${rpc_port}" 2>/dev/null || printf '0')
  best_header=$(metric "${metrics_port}" sync_block_best_header_tip_height)
  verified=$(metric "${metrics_port}" sync_block_verified_tip_height)
  zakura_peers=$(metric "${metrics_port}" zakura_p2p_conn_active)
  legacy_peers=$(peer_count "${rpc_port}" 2>/dev/null || printf '0')
  requests=$(metric "${metrics_port}" sync_block_request_sent)
  bodies_received=$(metric "${metrics_port}" sync_block_body_received)
  bodies_served=$(metric "${metrics_port}" sync_block_body_served)
  budget=$(metric "${metrics_port}" sync_block_budget_reserved_bytes)
  reorder=$(metric "${metrics_port}" sync_block_reorder_buffered_bytes)
  applying=$(metric "${metrics_port}" sync_block_applying)
  outstanding=$(metric "${metrics_port}" sync_block_outstanding)

  jq -n -c \
    --arg phase "${phase}" \
    --arg mode "${ZAKURA_E2E_MODE}" \
    --arg node "${node}" \
    --argjson seconds "${SECONDS}" \
    --argjson rpc_height "${rpc_height}" \
    --argjson best_header_tip "${best_header}" \
    --argjson verified_body_tip "${verified}" \
    --argjson active_zakura_peers "${zakura_peers}" \
    --argjson legacy_peer_count "${legacy_peers}" \
    --argjson block_sync_request_sent "${requests}" \
    --argjson block_sync_body_received "${bodies_received}" \
    --argjson block_sync_body_served "${bodies_served}" \
    --argjson budget_reserved_bytes "${budget}" \
    --argjson reorder_buffered_bytes "${reorder}" \
    --argjson applying "${applying}" \
    --argjson outstanding "${outstanding}" \
    '{
      phase: $phase,
      mode: $mode,
      seconds: $seconds,
      node: $node,
      rpc_height: $rpc_height,
      best_header_tip: $best_header_tip,
      verified_body_tip: $verified_body_tip,
      active_zakura_peers: $active_zakura_peers,
      legacy_peer_count: $legacy_peer_count,
      block_sync_request_sent: $block_sync_request_sent,
      block_sync_body_received: $block_sync_body_received,
      block_sync_body_served: $block_sync_body_served,
      budget_reserved_bytes: $budget_reserved_bytes,
      reorder_buffered_bytes: $reorder_buffered_bytes,
      applying: $applying,
      outstanding: $outstanding
    }' >> "${TIMELINE_FILE}"
}

snapshot_timeline() {
  local phase="$1"
  timeline_node_snapshot "${phase}" node1 18232 19001 || true
  timeline_node_snapshot "${phase}" node2 18332 19002 || true
  timeline_node_snapshot "${phase}" node3 18432 19003 || true
  timeline_node_snapshot "${phase}" node4 18532 19004 || true
}

wait_metric_zero() {
  local port="$1" name="$2" label="$3" deadline=$((SECONDS + READY_TIMEOUT))
  local value
  while (( SECONDS < deadline )); do
    value=$(metric "${port}" "${name}")
    printf '  %s %s=%s (want 0)\n' "${label}" "${name}" "${value}"
    if awk "BEGIN{exit !(${value} == 0)}"; then
      return 0
    fi
    sleep 3
  done
  fail "${label} ${name} did not return to zero within ${READY_TIMEOUT}s"
}

wait_metric_at_least() {
  local port="$1" name="$2" want="$3" label="$4" timeout="${5:-${READY_TIMEOUT}}"
  local deadline=$((SECONDS + timeout)) value
  while (( SECONDS < deadline )); do
    value=$(metric "${port}" "${name}")
    printf '  %s %s=%s (want >= %s)\n' "${label}" "${name}" "${value}" "${want}"
    if awk "BEGIN{exit !(${value} >= ${want})}"; then
      return 0
    fi
    sleep 3
  done
  fail "${label} ${name} stayed below ${want} within ${timeout}s"
}

trace_line_count() {
  local file="$1"
  if [[ -f "${file}" ]]; then
    wc -l < "${file}"
  else
    printf '0\n'
  fi
}

trace_rows_after() {
  local file="$1" lines_before="$2"
  [[ -f "${file}" ]] || return 0
  awk -v lines_before="${lines_before}" 'NR > lines_before' "${file}"
}

# The compatibility downloader fetches only genesis. Follow the append-only
# legacy trace from this restart and validate the exact durable handoff boundary.
wait_for_genesis_handoff() {
  local expected_height="$1" lines_before="$2" timeout="$3"
  local file="${ZAKURA_E2E_TRACE_DIR}/node2/legacy_sync.jsonl"
  local deadline=$((SECONDS + timeout)) summary verified_height handoff_count
  local handoff_row legacy_summary

  while (( SECONDS < deadline )); do
    summary=$(trace_rows_after "${file}" "${lines_before}" \
      | jq -sc --argjson expected "${expected_height}" '
          [ .[] | select(
              .event == "block_finish"
              and .result == "verified"
              and (.height | type == "number")
              and .height <= $expected
            )
          ] as $verified
          | [ .[] | select(
              .event == "round_finish"
              and .reason == "checkpoint_handoff"
            )
          ] as $handoffs
          | {
              verified_height: (($verified | map(.height) | max) // -1),
              handoff_count: ($handoffs | length),
              handoff: ($handoffs | last // null)
            }
        ')
    verified_height=$(printf '%s' "${summary}" | jq -r '.verified_height')
    handoff_count=$(printf '%s' "${summary}" | jq -r '.handoff_count')
    printf '  node2 genesis bootstrap legacy_verified=%s handoffs=%s (target %s)\n' \
      "${verified_height}" "${handoff_count}" "${expected_height}"
    if (( handoff_count >= 1 )); then
      handoff_row=$(printf '%s' "${summary}" | jq -c '.handoff')
      break
    fi
    sleep 3
  done

  [[ -n "${handoff_row:-}" ]] \
    || fail "node2 did not emit genesis handoff at height ${expected_height} within ${timeout}s"
  [[ "${handoff_count}" == "1" ]] \
    || fail "node2 emitted ${handoff_count} genesis handoffs at height ${expected_height} during one catch-up"
  printf '%s' "${handoff_row}" \
    | jq -e --argjson expected "${expected_height}" '
        .checkpoint_height == $expected
        and .state_tip == $expected
        and ((.process_trace_id | strings | length) > 0)
        and (.ts | type == "number")
      ' >/dev/null \
    || fail "node2 genesis handoff did not match the exact expected state boundary ${expected_height}"

  CHECKPOINT_HANDOFF_PROCESS_TRACE_ID=$(printf '%s' "${handoff_row}" | jq -r '.process_trace_id')
  CHECKPOINT_HANDOFF_TS=$(printf '%s' "${handoff_row}" | jq -r '.ts')
  legacy_summary=$(trace_rows_after "${file}" "${lines_before}" \
    | jq -sc \
        --arg process "${CHECKPOINT_HANDOFF_PROCESS_TRACE_ID}" \
        --argjson expected "${expected_height}" '
          [ .[] | select(
              .process_trace_id == $process
              and .event == "block_finish"
              and .result == "verified"
              and (.height | type == "number")
              and .height >= 0
              and .height <= $expected
            )
            | .height
          ]
          | unique
          | sort
          | {
              first: (first // -1),
              last: (last // -1),
              count: length
            }
        ')
  printf '%s' "${legacy_summary}" \
    | jq -e --argjson expected "${expected_height}" '
        .first == 0 and .last == $expected and .count == ($expected + 1)
      ' >/dev/null \
    || fail "node2 compatibility trace did not verify only genesis before handoff"
  log "node2 compatibility bootstrap verified genesis and handed height 1 onward to native sync"
}

# Native header and body work can prefetch during checkpoint verification.
# Require a complete header lifecycle rooted at the checkpoint boundary.
# Gate each native commit on the durable checkpoint handoff.
wait_for_native_suffix_coverage() {
  local suffix_start="$1" suffix_end="$2" block_lines_before="$3"
  local header_lines_before="$4" timeout="$5"
  local block_file="${ZAKURA_E2E_TRACE_DIR}/node2/block_sync.jsonl"
  local header_file="${ZAKURA_E2E_TRACE_DIR}/node2/header_sync.jsonl"
  local deadline=$((SECONDS + timeout)) request_summary covered=0
  local first_request_ts lifecycle_count apply_summary apply_ready=0

  while (( SECONDS < deadline )); do
    request_summary=$(trace_rows_after "${block_file}" "${block_lines_before}" \
      | jq -sc \
          --arg process "${CHECKPOINT_HANDOFF_PROCESS_TRACE_ID}" \
          --argjson handoff_ts "${CHECKPOINT_HANDOFF_TS}" \
          --argjson suffix_start "${suffix_start}" \
          --argjson suffix_end "${suffix_end}" '
            [ .[] | select(
                .process_trace_id == $process
                and .event == "block_get_blocks_sent"
                and (.range_start | type == "number")
                and (.range_count | type == "number")
              )
            ] as $requests
            | [ $requests[]
                | range(.range_start; (.range_start + .range_count))
                | select(. >= $suffix_start and . <= $suffix_end)
              ]
              | unique
              | sort as $covered
            | {
                request_count: ($requests | length),
                prefetched_requests:
                  ([$requests[] | select(.ts <= $handoff_ts)] | length),
                first_request_ts: (($requests | map(.ts) | min) // null),
                out_of_suffix:
                  ([$requests[]
                    | select(
                        .range_start < $suffix_start
                        or (.range_start + .range_count - 1) > $suffix_end
                      )
                   ] | length),
                covered_first: ($covered | first // null),
                covered_last: ($covered | last // null),
                covered_count: ($covered | length),
                expected_count: ($suffix_end - $suffix_start + 1)
              }
          ')
    covered=$(printf '%s' "${request_summary}" \
      | jq -r '(.covered_count == .expected_count) and (.expected_count > 0)')
    printf '  node2 native suffix request coverage=%s/%s requests=%s (target %s..%s)\n' \
      "$(printf '%s' "${request_summary}" | jq -r '.covered_count')" \
      "$(printf '%s' "${request_summary}" | jq -r '.expected_count')" \
      "$(printf '%s' "${request_summary}" | jq -r '.request_count')" \
      "${suffix_start}" "${suffix_end}"
    [[ "${covered}" == "true" ]] && break
    sleep 3
  done

  [[ "${covered}" == "true" ]] \
    || fail "node2 native kind-6 requests did not cover checkpoint suffix ${suffix_start}..${suffix_end} within ${timeout}s"
  printf '%s' "${request_summary}" \
    | jq -e \
        --argjson suffix_start "${suffix_start}" \
        --argjson suffix_end "${suffix_end}" '
          .out_of_suffix == 0
          and .covered_first == $suffix_start
          and .covered_last == $suffix_end
          and .covered_count == .expected_count
          and (.first_request_ts | type == "number")
        ' >/dev/null \
    || fail "node2 native requests were not confined to suffix ${suffix_start}..${suffix_end}"

  # RPC height can advance before the asynchronous trace writer flushes the
  # matching block_apply_finished row. Poll the trace within the request deadline.
  while true; do
    apply_summary=$(trace_rows_after "${block_file}" "${block_lines_before}" \
      | jq -sc \
          --arg process "${CHECKPOINT_HANDOFF_PROCESS_TRACE_ID}" \
          --argjson handoff_ts "${CHECKPOINT_HANDOFF_TS}" \
          --argjson suffix_start "${suffix_start}" \
          --argjson suffix_end "${suffix_end}" '
          [ .[] | select(
              .process_trace_id == $process
              and .event == "block_apply_finished"
              and .result == "committed"
              and (.height | type == "number")
            )
          ] as $applies
          | [ $applies[]
              | select(.ts > $handoff_ts)
              | .height
              | select(. >= $suffix_start and . <= $suffix_end)
            ]
            | unique
            | sort as $committed
          | {
              applies_before_or_at_handoff:
                ([$applies[] | select(.ts <= $handoff_ts)] | length),
              out_of_suffix:
                ([$applies[] | select(
                    .height < $suffix_start or .height > $suffix_end
                  )] | length),
              committed_first: ($committed | first // null),
              committed_last: ($committed | last // null),
              committed_count: ($committed | length),
              expected_count: ($suffix_end - $suffix_start + 1)
            }
          ')
    apply_ready=$(printf '%s' "${apply_summary}" \
      | jq -r \
        --argjson suffix_start "${suffix_start}" \
        --argjson suffix_end "${suffix_end}" '
          .applies_before_or_at_handoff == 0
          and .out_of_suffix == 0
          and .committed_first == $suffix_start
          and .committed_last == $suffix_end
          and .committed_count == .expected_count
        ')
    printf '  node2 native suffix commit coverage=%s/%s (target %s..%s)\n' \
      "$(printf '%s' "${apply_summary}" | jq -r '.committed_count')" \
      "$(printf '%s' "${apply_summary}" | jq -r '.expected_count')" \
      "${suffix_start}" "${suffix_end}"
    [[ "${apply_ready}" == "true" ]] && break
    (( SECONDS < deadline )) || break
    sleep 3
  done

  [[ "${apply_ready}" == "true" ]] \
    || fail "node2 did not commit exactly the native suffix ${suffix_start}..${suffix_end} after checkpoint handoff"

  first_request_ts=$(printf '%s' "${request_summary}" | jq -r '.first_request_ts')
  lifecycle_count=$(trace_rows_after "${header_file}" "${header_lines_before}" \
    | jq -sc \
        --arg process "${CHECKPOINT_HANDOFF_PROCESS_TRACE_ID}" \
        --argjson ancestor_height "$(( suffix_start - 1 ))" \
        --argjson suffix_end "${suffix_end}" \
        --argjson expected_count "$(( suffix_end - suffix_start + 1 ))" \
        --argjson first_request_ts "${first_request_ts}" '
          . as $rows
          | [ $rows[] as $snapshot
              | select(
                  $snapshot.process_trace_id == $process
                  and $snapshot.event == "header_snapshot_observed"
                  and $snapshot.cause == "advance"
                  and $snapshot.new_selected_height == $suffix_end
                  and $snapshot.ts < $first_request_ts
                )
              | $rows[] as $response
              | select(
                  $response.process_trace_id == $process
                  and $response.event == "header_response_received"
                  and $response.branch_anchor == $snapshot.branch_anchor
                  and $response.branch_target == $snapshot.branch_target
                  and $response.target_hash == $snapshot.new_selected_hash
                  and $response.common_ancestor_hash == $response.branch_anchor
                  and $response.common_ancestor_height == $ancestor_height
                  and $response.header_count == $expected_count
                  and $response.complete == true
                  and $response.ts < $snapshot.ts
                )
              | $rows[] as $request
              | select(
                  $request.process_trace_id == $process
                  and $request.event == "header_request_sent"
                  and $request.request_id == $response.request_id
                  and $request.session_id == $response.session_id
                  and $request.peer == $response.peer
                  and $request.branch_anchor == $response.branch_anchor
                  and $request.branch_target == $response.branch_target
                  and $request.locator_head == $response.common_ancestor_hash
                  and $request.target_hash == $response.target_hash
                  and $request.header_count >= $expected_count
                  and $request.ts < $response.ts
                )
          ] | length
        ')
  (( lifecycle_count >= 1 )) \
    || fail "node2 did not complete and durably admit the native header lifecycle before its first kind-6 body request"
  log "node2 prefetched an authenticated native suffix and committed it only after checkpoint handoff (${suffix_start}..${suffix_end})"
}

wait_ready() {
  local port="$1" name="$2" deadline=$((SECONDS + READY_TIMEOUT))
  while (( SECONDS < deadline )); do
    if rpc "${port}" getblockchaininfo | jq -e '.result' >/dev/null 2>&1; then
      printf '  %s RPC ready on %s\n' "${name}" "${port}"; return 0
    fi
    sleep 2
  done
  fail "${name} RPC did not become ready within ${READY_TIMEOUT}s"
}

stop_node2_for_reset() {
  local label="$1"
  docker compose -f "${COMPOSE_FILE}" stop zakura-node-2 \
    || fail "could not stop node2 for ${label}"
  if [[ "${ZAKURA_E2E_RESTART_MATRIX}" == "1" ]]; then
    docker compose -f "${COMPOSE_FILE}" rm -f zakura-node-2 \
      || fail "could not remove node2 container for ${label}"
    if docker volume inspect "${ZAKURA_NODE2_STATE_VOLUME}" >/dev/null 2>&1; then
      local volume_project
      volume_project=$(docker volume inspect \
        --format '{{ index .Labels "com.docker.compose.project" }}' \
        "${ZAKURA_NODE2_STATE_VOLUME}")
      [[ "${volume_project}" == "zakura-regtest-e2e" ]] \
        || fail "refusing to reset node2 volume with unexpected Compose owner: ${volume_project:-<none>}"
      docker volume rm "${ZAKURA_NODE2_STATE_VOLUME}" >/dev/null \
        || fail "could not reset run-scoped node2 state volume for ${label}"
    fi
  fi
}

start_node2_after_reset() {
  local label="$1"
  if [[ "${ZAKURA_E2E_RESTART_MATRIX}" == "1" ]]; then
    docker compose -f "${COMPOSE_FILE}" up -d zakura-node-2 \
      || fail "could not recreate node2 for ${label}"
  else
    docker compose -f "${COMPOSE_FILE}" start zakura-node-2 \
      || fail "could not restart node2 for ${label}"
  fi
  wait_ready 18332 "node2 (${label})"
}

reset_node2_from_scratch() {
  local label="$1"
  stop_node2_for_reset "${label}"
  start_node2_after_reset "${label}"
}

restart_node2_preserving_state() {
  local label="$1"
  docker compose -f "${COMPOSE_FILE}" stop zakura-node-2 \
    || fail "could not stop node2 for ${label}"
  docker compose -f "${COMPOSE_FILE}" start zakura-node-2 \
    || fail "could not restart node2 for ${label}"
  wait_ready 18332 "node2 (${label})"
}

docker_log_line_count() {
  docker logs zakura-node-2 2>&1 | wc -l
}

node2_logs_after() {
  local lines_before="$1"
  docker logs zakura-node-2 2>&1 | awk -v lines_before="${lines_before}" 'NR > lines_before'
}

wait_for_node2_exact_reopen() {
  local height="$1" lines_before="$2" label="$3" timeout="$4"
  local deadline=$((SECONDS + timeout)) logs

  while (( SECONDS < deadline )); do
    logs=$(node2_logs_after "${lines_before}")
    if printf '%s\n' "${logs}" \
      | grep -F "starting sync, obtaining new tips state_tip=Some(Height(${height}))" >/dev/null
    then
      if printf '%s\n' "${logs}" \
        | grep -F "starting genesis block download and verify" >/dev/null
      then
        fail "node2 ${label} replayed genesis instead of reopening durable height ${height}"
      fi
      printf '  node2 %s reopened the existing database at exact height %s without genesis replay\n' \
        "${label}" "${height}"
      return 0
    fi
    sleep 2
  done

  fail "node2 ${label} did not log an exact durable reopen at height ${height}"
}

assert_node2_post_reorg_restore() {
  local lines_before="$1" target="$2" expected_hash="$3"
  local logs restored_count reopened_hash

  logs=$(node2_logs_after "${lines_before}")
  if printf '%s\n' "${logs}" \
    | grep -F "starting genesis block download and verify" >/dev/null
  then
    fail "node2 post-reorg restart replayed genesis instead of restoring its database"
  fi
  restored_count=$(printf '%s\n' "${logs}" \
    | sed -n 's/.*num_blocks_restored=\([0-9][0-9]*\).*/\1/p' \
    | tail -1)
  [[ -n "${restored_count}" && "${restored_count}" -gt 0 ]] \
    || fail "node2 post-reorg restart did not prove non-finalized backup restoration"
  reopened_hash=$(block_hash 18332 "${target}")
  [[ "${reopened_hash}" == "${expected_hash}" ]] \
    || fail "node2 post-reorg restart selected ${reopened_hash:-<none>} at height ${target}, expected ${expected_hash}"
  printf '  node2 post-reorg restart restored %s non-finalized block(s) and exact tip %s\n' \
    "${restored_count}" "${expected_hash}"
}

configure_node2_debug_stop() {
  local height="$1"
  local config="${CONFIG_DIR}/node2.toml"
  local debug_trace_dir="/traces/debug-stops/node2-height-${height}"
  local evidence_dir="${ZAKURA_E2E_TRACE_DIR}/debug-stops/node2-height-${height}"

  [[ "${height}" =~ ^[0-9]+$ ]] || fail "invalid node2 debug stop height: ${height}"
  ! grep -q '^debug_stop_at_height = ' "${config}" \
    || fail "node2 debug stop height is already configured"
  grep -q '^trace_dir = "/traces/node2"$' "${config}" \
    || fail "node2 trace directory is not in its normal configuration"

  # Create the bind-mounted destination as the host user.
  # The harness can then preserve the container log, exit code, and raw traces.
  mkdir -p "${evidence_dir}" \
    || fail "could not create node2 debug-stop evidence directory at height ${height}"
  [[ -w "${evidence_dir}" ]] \
    || fail "node2 debug-stop evidence directory is not host-writable at height ${height}"

  # debug_stop_at_height exits from the state writer before the ordinary
  # block-sync driver can emit commit_finish. Preserve those intentional-stop
  # traces as raw evidence, but keep them out of node2's strict-oracle stream.
  rewrite_mounted_config_in_place \
    "s|^trace_dir = \"/traces/node2\"$|trace_dir = \"${debug_trace_dir}\"|; /^ephemeral = false$/a\\
debug_stop_at_height = ${height}" \
    "${config}" \
    || fail "could not configure node2 debug stop at height ${height}"

  grep -q "^debug_stop_at_height = ${height}$" "${config}" \
    || fail "node2 debug stop height ${height} was not injected"
  grep -q "^trace_dir = \"${debug_trace_dir}\"$" "${config}" \
    || fail "node2 debug-stop trace directory was not injected"
}

clear_node2_debug_stop() {
  local height="$1"
  local config="${CONFIG_DIR}/node2.toml"
  local debug_trace_dir="/traces/debug-stops/node2-height-${height}"

  grep -q "^debug_stop_at_height = ${height}$" "${config}" \
    || fail "node2 debug stop height ${height} is missing before restore"
  grep -q "^trace_dir = \"${debug_trace_dir}\"$" "${config}" \
    || fail "node2 debug-stop trace directory is missing before restore"

  rewrite_mounted_config_in_place \
    "/^debug_stop_at_height = ${height}$/d; s|^trace_dir = \"${debug_trace_dir}\"$|trace_dir = \"/traces/node2\"|" \
    "${config}" \
    || fail "could not clear node2 debug stop at height ${height}"

  ! grep -q '^debug_stop_at_height = ' "${config}" \
    || fail "node2 debug stop height remained configured after restore"
  grep -q '^trace_dir = "/traces/node2"$' "${config}" \
    || fail "node2 normal trace directory was not restored"
}

wait_for_node2_debug_stop() {
  local height="$1" label="$2" timeout="$3"
  local deadline=$((SECONDS + timeout)) status exit_code
  local evidence_dir="${ZAKURA_E2E_TRACE_DIR}/debug-stops/node2-height-${height}"
  local evidence_log="${evidence_dir}/docker.log"

  log "waiting for node2 to durably stop at exact height ${height} (${label})"
  while (( SECONDS < deadline )); do
    status=$(docker inspect --format '{{.State.Status}}' zakura-node-2 2>/dev/null || true)
    case "${status}" in
      exited)
        exit_code=$(docker inspect --format '{{.State.ExitCode}}' zakura-node-2 2>/dev/null || true)
        [[ "${exit_code}" == "0" ]] \
          || fail "node2 debug stop at height ${height} exited with status ${exit_code:-unknown}"
        mkdir -p "${evidence_dir}"
        docker logs zakura-node-2 > "${evidence_log}" 2>&1 \
          || fail "could not preserve node2 debug-stop log at height ${height}"
        printf '%s\n' "${exit_code}" > "${evidence_dir}/docker.exit.txt"
        if ! awk -v height="${height}" '
              /stopping at configured height, flushing database to disk/ {
                exact_height = "height=Height\\(" height "\\)"
                plain_height = "height=" height "([^0-9]|$)"
                if ($0 ~ exact_height || $0 ~ plain_height) found = 1
              }
              END { exit(found ? 0 : 1) }
            ' "${evidence_log}"
        then
          fail "node2 exited cleanly without proving its durable debug stop at exact height ${height}"
        fi
        printf '  node2 %s durably stopped at exact height %s with exit code 0\n' \
          "${label}" "${height}"
        return 0
        ;;
      dead)
        fail "node2 container died while stopping at height ${height}"
        ;;
      created|running|restarting|paused|removing|"")
        sleep 2
        ;;
      *)
        fail "node2 had unexpected container status '${status}' while stopping at height ${height}"
        ;;
    esac
  done

  fail "node2 did not durably stop at exact height ${height} within ${timeout}s"
}

peer_count() { rpc "$1" getpeerinfo | jq -r 'if .result then (.result | length) else 0 end'; }
block_count() { rpc "$1" getblockcount | jq -r '.result // 0'; }
block_hash() { rpc "$1" getblockhash "[$2]" | jq -r '.result // empty'; }
strict_upgrade() { [[ "${ZAKURA_REGTEST_E2E_STRICT_UPGRADE:-0}" == "1" ]]; }

# Extended lanes only stress node1 and node2 after the initial upgrade smoke.
# Stop the known-optional upgraded peer at a verified idle trace boundary so
# its unrelated catch-up cannot outlive it and keep header requests open.
quiesce_optional_node4_before_extended_work() {
  case "${ZAKURA_E2E_MODE}" in
    checkpoint-long|no-checkpoint-long|restart-matrix) ;;
    *) return 0 ;;
  esac
  strict_upgrade && return 0

  log "quiescing optional node4 before extended ${ZAKURA_E2E_MODE} work"
  wait_metric_zero 19004 sync_block_budget_reserved_bytes "node4 pre-extended budget"
  wait_metric_zero 19004 sync_block_reorder_buffered_bytes "node4 pre-extended reorder"
  wait_metric_zero 19004 sync_block_applying "node4 pre-extended applying"
  wait_metric_zero 19004 sync_block_outstanding "node4 pre-extended outstanding"
  wait_for_commit_trace_balance node4 "node4 pre-extended"
  wait_for_trace_flush

  local file="${ZAKURA_E2E_TRACE_DIR}/node4/block_sync.jsonl" last leak
  local header_file="${ZAKURA_E2E_TRACE_DIR}/node1/header_sync.jsonl"
  local active_sessions_before connections_before connections_after
  local disconnects_before disconnects_after deadline node2_connections
  last=$(grep 'block_sync_state' "${file}" 2>/dev/null | tail -1 || true)
  [[ -n "${last}" ]] || fail "node4 pre-extended block-sync trace is missing"
  leak=$(printf '%s' "${last}" \
    | jq -er '[(.applying//0),(.budget_reserved//0),(.reorder//0),(.outstanding//0)]|add') \
    || fail "could not read node4 pre-extended block-sync trace"
  [[ "${leak}" == "0" ]] \
    || fail "node4 pre-extended block-sync trace is not drained (total ${leak})"

  connections_before=$(metric 19001 zakura_p2p_conn_active)
  if ! awk "BEGIN{exit !(${connections_before} == 2)}"; then
    fail "node1 expected exactly node2 and node4 before optional-peer quiescence, found ${connections_before} active Zakura peers"
  fi
  active_sessions_before=$(jq -sc '
    ([.[] | select(.event == "header_peer_connected") | .session_id]
      - [.[] | select(.event == "header_peer_disconnected") | .session_id])
    | unique
  ' "${header_file}") \
    || fail "could not read node1 active header sessions before stopping node4"
  [[ "$(printf '%s' "${active_sessions_before}" | jq -r 'length')" == "2" ]] \
    || fail "node1 trace did not contain exactly two active header sessions before stopping node4"
  disconnects_before=$(grep -c '"event":"header_peer_disconnected"' "${header_file}" 2>/dev/null || true)

  docker compose -f "${COMPOSE_FILE}" stop zakura-node-4 \
    || fail "could not stop optional node4 before extended work"

  deadline=$((SECONDS + NODE4_DISCONNECT_TIMEOUT))
  while (( SECONDS < deadline )); do
    connections_after=$(metric 19001 zakura_p2p_conn_active)
    disconnects_after=$(grep -c '"event":"header_peer_disconnected"' "${header_file}" 2>/dev/null || true)
    printf '  node1 post-node4-quiesce active=%s disconnect_rows=%s (want <=1 and >%s)\n' \
      "${connections_after}" "${disconnects_after}" "${disconnects_before}"
    if awk "BEGIN{exit !(${connections_after} <= 1)}" \
      && (( disconnects_after > disconnects_before )); then
      last=$(grep '"event":"header_peer_disconnected"' "${header_file}" | tail -1)
      printf '%s' "${last}" \
        | jq -e --argjson active "${active_sessions_before}" '
            .session_id as $session
            | ($session | type == "number")
              and ($active | index($session) != null)
              and (.direction == "inbound")
              and (.inbound_count == 1)
              and (.outbound_count == 0)
              and ((.reason | strings | length) > 0)
          ' \
          >/dev/null \
        || fail "node1 post-node4 disconnect trace boundary does not retire one of the two pre-stop inbound sessions"
      node2_connections=$(metric 19002 zakura_p2p_conn_active)
      if ! awk "BEGIN{exit !(${node2_connections} >= 1)}"; then
        fail "node2 was not connected after node1 retired the stopped node4 session"
      fi
      OPTIONAL_NODE4_QUIESCED=1
      return 0
    fi
    sleep 3
  done
  fail "node1 did not retire and flush the stopped node4 session within ${NODE4_DISCONNECT_TIMEOUT}s"
}

invalidate_block_if_present() {
  local port="$1" height="$2" hash="$3" label="$4"
  local current_height current_hash

  current_height=$(block_count "${port}")
  if [[ "${current_height}" -lt "${height}" ]]; then
    printf '  %s skipping invalidate: height=%s below %s\n' \
      "${label}" "${current_height}" "${height}"
    return 0
  fi

  current_hash=$(block_hash "${port}" "${height}")
  if [[ "${current_hash}" != "${hash}" ]]; then
    printf '  %s skipping invalidate: hash mismatch at height %s (have %s, want %s)\n' \
      "${label}" "${height}" "${current_hash}" "${hash}"
    return 0
  fi

  rpc "${port}" invalidateblock "[\"${hash}\"]" | jq -e '.error == null' >/dev/null \
    || fail "invalidateblock failed on RPC port ${port}"
}

wait_block_count_at_least() {
  local port="$1" want="$2" label="$3" timeout="${4:-${PROPAGATE_TIMEOUT}}"
  local deadline=$((SECONDS + timeout)) height
  while (( SECONDS < deadline )); do
    height=$(block_count "${port}")
    printf '  %s height=%s (want >= %s)\n' "${label}" "${height}" "${want}"
    [[ "${height}" -ge "${want}" ]] && return 0
    sleep 3
  done
  fail "${label} did not reach height ${want}"
}

wait_block_count_equal() {
  local port="$1" want="$2" label="$3" deadline=$((SECONDS + READY_TIMEOUT))
  local height
  while (( SECONDS < deadline )); do
    height=$(block_count "${port}")
    printf '  %s height=%s (want %s)\n' "${label}" "${height}" "${want}"
    [[ "${height}" -eq "${want}" ]] && return 0
    sleep 3
  done
  fail "${label} did not settle at height ${want}"
}

wait_zakura_body_frontier_at_tip() {
  local metrics_port="$1" rpc_port="$2" target="$3" label="$4" timeout="${5:-${PROPAGATE_TIMEOUT}}"
  local deadline=$((SECONDS + timeout)) header body rpc_height
  while (( SECONDS < deadline )); do
    header=$(metric "${metrics_port}" sync_block_best_header_tip_height)
    body=$(metric "${metrics_port}" sync_block_verified_tip_height)
    rpc_height=$(block_count "${rpc_port}")
    printf '  %s body_tip=%s header_tip=%s rpc_height=%s (target %s)\n' \
      "${label}" "${body}" "${header}" "${rpc_height}" "${target}"
    if awk "BEGIN{exit !(${header} >= ${target} && ${body} == ${header} && ${rpc_height} >= ${target})}"; then
      return 0
    fi
    sleep 3
  done
  fail "${label} did not reach body frontier == header tip at target ${target}"
}

wait_zakura_body_frontiers_at_tip() {
  local target="$1" phase="$2"
  wait_zakura_body_frontier_at_tip 19001 18232 "${target}" "node1 ${phase}"
  wait_zakura_body_frontier_at_tip 19002 18332 "${target}" "node2 ${phase}"
  if strict_upgrade; then
    wait_zakura_body_frontier_at_tip 19004 18532 "${target}" "node4 ${phase}"
  elif [[ "${OPTIONAL_NODE4_QUIESCED}" == "1" ]]; then
    printf '  node4 %s quiesced after upgrade smoke coverage\n' "${phase}"
  else
    h4=$(block_count 18532)
    header4=$(metric 19004 sync_block_best_header_tip_height)
    body4=$(metric 19004 sync_block_verified_tip_height)
    printf '  node4 %s optional body_tip=%s header_tip=%s rpc_height=%s (target %s)\n' \
      "${phase}" "${body4}" "${header4}" "${h4}" "${target}"
  fi
}

assert_block_sync_budget_empty() {
  local phase="$1"
  wait_metric_zero 19001 sync_block_budget_reserved_bytes "node1 ${phase} budget"
  wait_metric_zero 19002 sync_block_budget_reserved_bytes "node2 ${phase} budget"
  wait_metric_zero 19001 sync_block_reorder_buffered_bytes "node1 ${phase} reorder"
  wait_metric_zero 19002 sync_block_reorder_buffered_bytes "node2 ${phase} reorder"
  wait_metric_zero 19001 sync_block_applying "node1 ${phase} applying"
  wait_metric_zero 19002 sync_block_applying "node2 ${phase} applying"
  wait_metric_zero 19001 sync_block_outstanding "node1 ${phase} outstanding"
  wait_metric_zero 19002 sync_block_outstanding "node2 ${phase} outstanding"
  if strict_upgrade; then
    wait_metric_zero 19004 sync_block_budget_reserved_bytes "node4 ${phase} budget"
    wait_metric_zero 19004 sync_block_reorder_buffered_bytes "node4 ${phase} reorder"
    wait_metric_zero 19004 sync_block_applying "node4 ${phase} applying"
    wait_metric_zero 19004 sync_block_outstanding "node4 ${phase} outstanding"
  elif [[ "${OPTIONAL_NODE4_QUIESCED}" == "1" ]]; then
    printf '  node4 %s quiesced after upgrade smoke coverage\n' "${phase}"
  else
    printf '  node4 %s optional budget=%s reorder=%s applying=%s outstanding=%s\n' \
      "${phase}" \
      "$(metric 19004 sync_block_budget_reserved_bytes)" \
      "$(metric 19004 sync_block_reorder_buffered_bytes)" \
      "$(metric 19004 sync_block_applying)" \
      "$(metric 19004 sync_block_outstanding)"
  fi
  snapshot_timeline "${phase}-cleanup"
}

restart_node2_at_height_then_catch_up() {
  local restart_height="$1" label="$2" target="$3" reopen_log_lines
  log "restart-matrix: resetting node2 and creating exact durable height ${restart_height} (${label})"
  stop_node2_for_reset "restart-matrix ${label} exact-height reset"
  configure_node2_debug_stop "${restart_height}"
  docker compose -f "${COMPOSE_FILE}" up -d zakura-node-2 \
    || fail "could not recreate node2 for restart-matrix ${label} debug stop"
  # Do not wait for RPC readiness.
  # Height 0 can commit before the RPC server becomes observable.
  # The clean exit and state-writer log prove the durable pre-restart height.
  wait_for_node2_debug_stop "${restart_height}" "restart-matrix ${label}" "${CATCHUP_TIMEOUT}"

  # Restore the config through the existing bind mount, then restart this same
  # container so its named-volume database survives the reopen.
  clear_node2_debug_stop "${restart_height}"
  reopen_log_lines=$(docker_log_line_count)
  docker compose -f "${COMPOSE_FILE}" start zakura-node-2 \
    || fail "could not reopen node2 at restart-matrix ${label}"
  wait_ready 18332 "node2 (restart-matrix ${label} reopen)"
  wait_for_node2_exact_reopen \
    "${restart_height}" "${reopen_log_lines}" "restart-matrix ${label}" "${READY_TIMEOUT}"
  if (( $(block_count 18332) < target )); then
    wait_metric_at_least 19002 sync_block_request_sent 1 "node2 restart-matrix ${label}" "${CATCHUP_TIMEOUT}"
  fi
  wait_block_count_at_least 18332 "${target}" "node2 restart-matrix ${label} catch-up" "${CATCHUP_TIMEOUT}"
  wait_zakura_body_frontier_at_tip 19002 18332 "${target}" "node2 restart-matrix ${label}" "${CATCHUP_TIMEOUT}"
  assert_block_sync_budget_empty "restart-matrix ${label}"
}

run_restart_matrix() {
  local target="$1"
  # Exercise both sides of the configured checkpoint boundary, then two
  # target-relative points so the same matrix scales with an explicit long run.
  local before_checkpoint=$(( CHECKPOINT_INTERVAL - 1 ))
  local at_checkpoint=${CHECKPOINT_INTERVAL}
  local after_checkpoint=$(( CHECKPOINT_INTERVAL + 1 ))
  local midpoint=$(( target / 2 ))
  local near_tip_gap=$(( target / 4 ))
  local near_tip_gap_height=$(( target - near_tip_gap ))

  restart_node2_at_height_then_catch_up 0 "height-0" "${target}"
  restart_node2_at_height_then_catch_up "${before_checkpoint}" "height-${before_checkpoint}" "${target}"
  restart_node2_at_height_then_catch_up "${at_checkpoint}" "height-${at_checkpoint}" "${target}"
  restart_node2_at_height_then_catch_up "${after_checkpoint}" "height-${after_checkpoint}" "${target}"
  restart_node2_at_height_then_catch_up "${midpoint}" "height-${midpoint}" "${target}"
  restart_node2_at_height_then_catch_up "${near_tip_gap_height}" "near-tip-${near_tip_gap}-gap" "${target}"
}

log "starting bootstrap node"
docker compose -f "${COMPOSE_FILE}" up -d zakura-node-1

log "waiting for bootstrap node readiness"
wait_ready 18232 node1

# `depends_on` only controls container start order, not application readiness.
# Wait for node1 to finish opening its legacy listener before its initial peers
# dial it, otherwise a fast peer can get ConnectionRefused and remain in address
# backoff for the entire test.
log "starting peer nodes"
docker compose -f "${COMPOSE_FILE}" up -d \
  zakura-node-2 \
  zakura-node-3 \
  zakura-node-4

log "waiting for peer RPC readiness"
wait_ready 18332 node2
wait_ready 18432 node3
wait_ready 18532 node4
snapshot_timeline "rpc-ready"

log "asserting legacy TCP backwards-compat (legacy-only node3 peers with node1)"
# node3 speaks only the legacy protocol, so it stays a legacy TCP peer of node1.
# node4 is dual-stack: its legacy connection to node1 auto-upgrades to Zakura and
# the legacy connection is dropped, so node4's legacy getpeerinfo is expected to
# fall back to 0 -- that is the upgrade working, not a failure.
deadline=$((SECONDS + READY_TIMEOUT))
while (( SECONDS < deadline )); do
  n3=$(peer_count 18432)
  printf '  node3 legacy peers=%s\n' "${n3}"
  [[ "${n3}" -ge 1 ]] && break
  sleep 3
done
[[ "${n3}" -ge 1 ]] || fail "legacy-only node3 never peered with node1 over legacy TCP"
snapshot_timeline "legacy-peers-ready"

log "asserting pure-Zakura node2 has no legacy peers (p2p_stack = zakura)"
# node2 has no legacy stack at all, so it must never have a legacy TCP peer; its
# only connectivity is the Zakura bootstrap dial to node1.
n2_legacy=$(peer_count 18332)
printf '  node2 legacy peers=%s (want 0)\n' "${n2_legacy}"
[[ "${n2_legacy}" -eq 0 ]] || fail \
  "pure-Zakura node2 unexpectedly has ${n2_legacy} legacy peer(s); it should have none"

log "asserting legacy->Zakura upgrade (zakura_p2p_handshake_upgraded metric)"
# node1 and the dual-stack node4 register each other over the upgraded connection.
deadline=$((SECONDS + READY_TIMEOUT)); upgraded=0
while (( SECONDS < deadline )); do
  u1=$(metric 19001 zakura_p2p_handshake_upgraded)
  u4=$(metric 19004 zakura_p2p_handshake_upgraded)
  printf '  node1 upgraded=%s  node4 upgraded=%s\n' "${u1}" "${u4}"
  if awk "BEGIN{exit !(${u1}+${u4} >= 1)}"; then upgraded=1; break; fi
  sleep 3
done
[[ "${upgraded}" -eq 1 ]] || fail \
  "node1 and node4 never upgraded their legacy connection to Zakura"
snapshot_timeline "zakura-upgrade-ready"

log "asserting live Zakura peer readiness"
# The upgrade metric above is a historical counter. Wait for the live peer gauge
# before mining so propagation assertions exercise an active Zakura path.
wait_metric_at_least 19002 zakura_p2p_conn_active 1 node2
if strict_upgrade; then
  wait_metric_at_least 19004 zakura_p2p_conn_active 1 node4
fi

# Prove the pure-Zakura node bootstrapped genesis over Zakura.
#
# node2 sets sync.debug_skip_regtest_genesis_self_seed, so it does NOT commit the
# Regtest genesis locally. With no legacy stack, its only way to obtain genesis is
# to download and verify it from node1 over Zakura (the bootstrap_genesis_then_pause
# path). Until that succeeds, native header sync cannot anchor at genesis and the
# node stays stuck at height 0 -- the exact Mainnet Zakura-only bug this guards
# against. This assertion runs before any block is mined, so node1 is still at the
# genesis-only tip and node2 must reach height 0 purely by fetching genesis.
log "asserting pure-Zakura node2 bootstrapped genesis over Zakura (self-seed disabled)"
genesis_hash=$(block_hash 18232 0)
[[ -n "${genesis_hash}" ]] || fail "could not read Regtest genesis hash from node1"
deadline=$((SECONDS + READY_TIMEOUT)); n2_genesis=""
while (( SECONDS < deadline )); do
  n2_genesis=$(block_hash 18332 0)
  printf '  node2 genesis(height 0)=%s (want %s)\n' "${n2_genesis:-<none>}" "${genesis_hash}"
  [[ "${n2_genesis}" == "${genesis_hash}" ]] && break
  sleep 3
done
[[ "${n2_genesis}" == "${genesis_hash}" ]] || fail \
  "pure-Zakura node2 never committed genesis over Zakura (self-seed disabled); genesis bootstrap is broken"
# Confirm genesis arrived via a real download+verify, not a self-seed that silently
# ignored the flag. `request_genesis` only logs the download-start line when genesis
# is ABSENT at startup, so this line cannot appear on a self-seeding node -- it
# proves node2 fetched genesis from a peer over Zakura.
deadline=$((SECONDS + READY_TIMEOUT)); n2_bootstrapped=0
while (( SECONDS < deadline )); do
  # Consume all log output: with pipefail, grep -q can close the pipe early and
  # turn Docker's resulting SIGPIPE into a false-negative pipeline status.
  if docker compose -f "${COMPOSE_FILE}" logs zakura-node-2 2>/dev/null \
    | grep -F "starting genesis block download and verify" >/dev/null; then
    n2_bootstrapped=1; break
  fi
  sleep 2
done
[[ "${n2_bootstrapped}" -eq 1 ]] || fail \
  "node2 committed genesis but never logged a genesis download; the self-seed shortcut may have run instead of a real over-Zakura fetch"
snapshot_timeline "genesis-bootstrap"

log "generating ${GENERATE_BLOCKS} block(s) on node1"
for ((i = 1; i <= GENERATE_BLOCKS; i++)); do
  if strict_upgrade; then
    wait_metric_at_least 19004 zakura_p2p_conn_active 1 "node4 before block ${i}"
  fi

  rpc 18232 generate "[1]" | jq -e '.result | length == 1' >/dev/null \
    || fail "generate RPC failed on node1 (check miner_address / mining config)"
  target=$(block_count 18232)
  printf '  generated block %s/%s; node1 height=%s\n' "${i}" "${GENERATE_BLOCKS}" "${target}"
  [[ "${target}" -ge "${i}" ]] || fail "node1 did not advance after generate"

  # The upgraded path currently learns about mined blocks through block
  # advertisements. Mine them one at a time so a node with one in-flight
  # download from a peer does not intentionally ignore the next advertisement
  # from that same peer before it has accepted the first block.
  if (( i < GENERATE_BLOCKS )) && strict_upgrade; then
    deadline=$((SECONDS + PROPAGATE_TIMEOUT))
    while (( SECONDS < deadline )); do
      h4=$(block_count 18532)
      printf '  node4 height=%s after generated block %s (target %s)\n' \
        "${h4}" "${i}" "${target}"
      [[ "${h4}" -ge "${target}" ]] && break
      sleep 3
    done

    if [[ "${h4}" -lt "${target}" ]]; then
      if strict_upgrade; then
        fail "upgraded dual-stack node4 did not ingest generated block ${i} before the next block (got ${h4}, want ${target})"
      fi

      printf '  known issue: node4 upgraded-Zakura propagation did not complete for generated block %s (got %s, want %s); continuing non-strict run\n' \
        "${i}" "${h4}" "${target}"
    fi
  fi
done
snapshot_timeline "post-generate-mining"

log "asserting block propagation to node2 (pure Zakura), node3 (legacy TCP), and checking node4 (known upgraded-Zakura issue)"
deadline=$((SECONDS + PROPAGATE_TIMEOUT))
while (( SECONDS < deadline )); do
  h2=$(block_count 18332); h3=$(block_count 18432); h4=$(block_count 18532)
  printf '  node2 height=%s  node3 height=%s  node4 height=%s (target %s)\n' \
    "${h2}" "${h3}" "${h4}" "${target}"
  if strict_upgrade; then
    [[ "${h2}" -ge "${target}" && "${h3}" -ge "${target}" && "${h4}" -ge "${target}" ]] && break
  else
    [[ "${h2}" -ge "${target}" && "${h3}" -ge "${target}" ]] && break
  fi
  sleep 3
done
[[ "${h2}" -ge "${target}" ]] || fail \
  "block did not propagate to pure-Zakura node2 (got ${h2}, want ${target}) -- pure-Zakura path broken"
[[ "${h3}" -ge "${target}" ]] || fail \
  "block did not propagate to legacy-only node3 over TCP (got ${h3}, want ${target})"
if [[ "${h4}" -lt "${target}" ]]; then
  if strict_upgrade; then
    fail "block did not propagate to upgraded dual-stack node4 over the Zakura adapter (got ${h4}, want ${target})"
  fi

  printf '  known issue: node4 upgraded-Zakura propagation did not complete (got %s, want %s); see stako/p2p-services/P2_E2E_KNOWN_ISSUES.md\n' \
    "${h4}" "${target}"
else
  printf '  node4 upgraded-Zakura propagation reached height=%s\n' "${h4}"
fi

log "asserting Zakura body frontier reached the header tip after gossip propagation"
wait_zakura_body_frontiers_at_tip "${target}" "post-generate"
assert_block_sync_budget_empty "post-generate"
snapshot_timeline "post-generate"
quiesce_optional_node4_before_extended_work

log "stopping pure-Zakura node2 before the from-scratch catch-up setup"
wait_for_commit_trace_balance node2 "node2 pre-reset"
stop_node2_for_reset "post-reset catch-up"

# ---------------------------------------------------------------------------
# Deepen the chain before the from-scratch reset so the kind-6 catch-up below
# re-downloads many bodies in a burst. Crossing hundreds of blocks is what fills
# the inbound block-sync wire queue and exercises the body-flood path that
# wedged in production (a full queue silently dropping solicited bodies, then a
# checkpoint-range commit waiting forever on the gap). A 3-block catch-up never
# fills that queue, so the earlier topology could not reproduce the stall. node2
# is stopped before this deepening, so it cannot prefetch any of these bodies.
if (( CATCHUP_BLOCKS > 0 )); then
  log "deepening node1 chain by ${CATCHUP_BLOCKS} block(s) before the reset catch-up"
  remaining=${CATCHUP_BLOCKS}
  while (( remaining > 0 )); do
    batch=$(( remaining < 50 ? remaining : 50 ))
    # `generate` mines sequentially (~0.25s/block on a debug build), so a 50-block batch can
    # exceed the default 10s RPC deadline — give it a generous, batch-scaled timeout.
    rpc 18232 generate "[${batch}]" "$(( batch * 4 + 30 ))" \
      | jq -e ".result | length == ${batch}" >/dev/null \
      || fail "bulk generate of ${batch} block(s) failed on node1"
    remaining=$(( remaining - batch ))
    printf '  mined batch of %s; node1 height=%s (%s remaining)\n' \
      "${batch}" "$(block_count 18232)" "${remaining}"
  done
  # Make the deepened tip the working target so the from-scratch catch-up spans
  # the whole chain and the trailing reorg stays a cheap one-block tip reorg
  # rather than unwinding everything mined here.
  target=$(block_count 18232)
  log "waiting for node1 body frontier to settle at the deepened tip ${target}"
  wait_zakura_body_frontier_at_tip 19001 18232 "${target}" "node1 deepened"
  snapshot_timeline "deepened"
fi

# ---------------------------------------------------------------------------
# Exercise the production checkpoint-to-native sync transition.
#
# Reset the pure-Zakura node from scratch.
# Node2 previously reached the initial tip through inbound gossip.
# The reset discards node2's state before node1 deepens the chain.
# Node1 does not advertise the old blocks again.
# Node2 downloads the checkpoint prefix through compatibility requests.
# It then hands ownership to the native header engine.
# Native block sync downloads the post-checkpoint suffix.
log "starting pure-Zakura node2 for a from-scratch kind-6 catch-up"
catchup_target=$(block_count 18232)
[[ "${catchup_target}" -ge 1 ]] || fail \
  "node1 has no chain for node2 to catch up to (height ${catchup_target})"
before_node1_served=$(metric 19001 sync_block_body_served)
snapshot_timeline "pre-reset-catch-up"

# ---------------------------------------------------------------------------
# Derive Regtest checkpoints from node1 after mining the blocks.
# Rewrite node2's config before the restart.
# The checkpoints exercise batch verification that the genesis-only list cannot reach.
# Override only `checkpoints` to preserve the Regtest identity.
# The `configured_regtest_checkpoints_preserve_regtest_identity` test fixes this config shape.
# Keep the highest checkpoint strictly below the tip so the trailing tip reorg later is never
# blocked by a final (immutable) checkpoint.
checkpoint_ceiling=$(( catchup_target - 2 ))
checkpoint_handoff_height=0
if [[ "${ZAKURA_E2E_DISABLE_CHECKPOINTS}" == "1" ]]; then
  log "node2 Regtest checkpoints are disabled; catch-up verifies through the full verifier after genesis"
elif (( CHECKPOINT_INTERVAL > 0 && checkpoint_ceiling >= CHECKPOINT_INTERVAL )); then
  # block::Hash deserializes as a 32-byte array in internal (display-reversed) order, so
  # convert each getblockhash hex into a reversed decimal byte array, e.g.
  # "029f..e327" -> [39, 227, ..., 2].
  hash_to_internal_bytes() {
    local hex="$1" out="" i
    (( ${#hex} == 64 )) || fail "unexpected block hash length for '${hex}'"
    for (( i = 62; i >= 0; i -= 2 )); do
      out+="$(( 16#${hex:i:2} )), "
    done
    printf '[%s]' "${out%, }"
  }

  cp_entries=""
  cp_count=0
  h=0
  while (( h <= checkpoint_ceiling )); do
    cp_hash=$(block_hash 18232 "${h}")
    [[ -n "${cp_hash}" ]] || fail "could not read node1 block hash at checkpoint height ${h}"
    [[ -n "${cp_entries}" ]] && cp_entries+=", "
    cp_entries+="[${h}, $(hash_to_internal_bytes "${cp_hash}")]"
    cp_count=$(( cp_count + 1 ))
    h=$(( h + CHECKPOINT_INTERVAL ))
  done

  # Replace node2's plain `network = "Regtest"` with a ConfiguredRegtest inline table carrying
  # the derived checkpoints (the deserializer matches ConfiguredRegtest by the `params` key).
  sed_in_place \
    "s|^network = \"Regtest\"\$|network = { params = { checkpoints = [${cp_entries}] } }|" \
    "${CONFIG_DIR}/node2.toml"
  grep -q '^network = { params = ' "${CONFIG_DIR}/node2.toml" \
    || fail "failed to inject derived checkpoints into node2 config"
  native_checkpoint_height=$(( h - CHECKPOINT_INTERVAL ))
  log "node2 will hand off to native sync after genesis; native checkpoint verification uses ${cp_count} derived checkpoint(s) through height ${native_checkpoint_height} (selection ceiling ${checkpoint_ceiling})"
else
  log "chain too short (tip ${catchup_target}, interval ${CHECKPOINT_INTERVAL}); node2 catches up with the genesis-only checkpoint list"
fi

node2_legacy_lines_before=$(trace_line_count "${ZAKURA_E2E_TRACE_DIR}/node2/legacy_sync.jsonl")
node2_block_lines_before=$(trace_line_count "${ZAKURA_E2E_TRACE_DIR}/node2/block_sync.jsonl")
node2_header_lines_before=$(trace_line_count "${ZAKURA_E2E_TRACE_DIR}/node2/header_sync.jsonl")
start_node2_after_reset "post-reset catch-up"
snapshot_timeline "post-reset-catch-up-started"

# Do not treat a zero native-request counter as a stall while the checkpoint
# verifier owns the prefix. Observe that phase directly and require its explicit
# handoff before waiting for native block-sync activity.
if [[ "${ZAKURA_E2E_REQUIRE_HANDOFF}" == "1" ]]; then
  (( checkpoint_handoff_height < catchup_target )) \
    || fail "checkpoint handoff height ${checkpoint_handoff_height} leaves no native suffix below target ${catchup_target}"
  wait_for_genesis_handoff \
    "${checkpoint_handoff_height}" "${node2_legacy_lines_before}" "${CATCHUP_TIMEOUT}"
fi

# Node2 restarts its counters at zero.
# Assert absolute native activity instead of a delta.
# With node1 idle, only kind-6 can supply the post-checkpoint gap.
wait_metric_at_least 19002 sync_block_request_sent 1 "node2 catch-up" "${CATCHUP_TIMEOUT}"
wait_metric_at_least 19002 sync_block_body_received 1 "node2 catch-up" "${CATCHUP_TIMEOUT}"
wait_block_count_at_least 18332 "${catchup_target}" "node2 catch-up" "${CATCHUP_TIMEOUT}"
wait_zakura_body_frontier_at_tip 19002 18332 "${catchup_target}" "node2 catch-up" "${CATCHUP_TIMEOUT}"
if [[ "${ZAKURA_E2E_REQUIRE_HANDOFF}" == "1" ]]; then
  wait_for_native_suffix_coverage \
    "$(( checkpoint_handoff_height + 1 ))" \
    "${catchup_target}" \
    "${node2_block_lines_before}" \
    "${node2_header_lines_before}" \
    "${CATCHUP_TIMEOUT}"
fi

# node2 dials only node1, so node1 must have served the catch-up bodies over kind-6.
after_node1_served=$(metric 19001 sync_block_body_served)
printf '  node1 kind-6 bodies served=%s (before reset %s)\n' \
  "${after_node1_served}" "${before_node1_served}"
if ! awk "BEGIN{exit !(${after_node1_served} > ${before_node1_served})}"; then
  fail "node2 caught up from scratch but node1's kind-6 body-served metric did not increase — bodies did not flow over block sync"
fi
assert_block_sync_budget_empty "post-catch-up"
if [[ "${ZAKURA_E2E_REQUIRE_HANDOFF}" == "1" ]]; then
  log "node2 compatibility-fetched genesis, handed off, then downloaded native kind-6 heights 1..${catchup_target}"
else
  log "node2 completed its from-scratch catch-up to height ${catchup_target} with native kind-6 activity"
fi
snapshot_timeline "post-catch-up"

if [[ "${ZAKURA_E2E_RESTART_MATRIX}" == "1" ]]; then
  run_restart_matrix "${catchup_target}"
fi

log "asserting non-finalized reorg survival with no block-sync budget leak"
old_tip_hash=$(block_hash 18232 "${target}")
[[ -n "${old_tip_hash}" ]] || fail "could not read old tip hash at height ${target}"
invalidate_block_if_present 18232 "${target}" "${old_tip_hash}" node1
invalidate_block_if_present 18332 "${target}" "${old_tip_hash}" node2
invalidate_block_if_present 18432 "${target}" "${old_tip_hash}" node3
if [[ "${OPTIONAL_NODE4_QUIESCED}" == "1" ]]; then
  printf '  node4 skipping invalidate: quiesced after upgrade smoke coverage\n'
else
  invalidate_block_if_present 18532 "${target}" "${old_tip_hash}" node4
fi
reorg_base=$((target - 1))
wait_block_count_equal 18232 "${reorg_base}" node1
wait_block_count_equal 18332 "${reorg_base}" node2
if strict_upgrade; then
  wait_block_count_equal 18532 "${reorg_base}" node4
fi
# Mine each replacement only after the legacy peer commits its parent. If both
# are mined together, node3 can receive the child inventory while its parent is
# still downloading. Its next legacy FindBlocks response then has only that one
# remaining hash, which the legacy syncer deliberately ignores, leaving node3
# stuck one block behind despite repeated inventories for the child.
for replacement in 1 2; do
  rpc 18232 generate "[1]" | jq -e '.result | length == 1' >/dev/null \
    || fail "generate RPC failed for replacement block ${replacement} after invalidating old tip"
  target=$(block_count 18232)
  wait_block_count_at_least 18332 "${target}" "node2 replacement ${replacement}"
  wait_block_count_at_least 18432 "${target}" "node3 replacement ${replacement}"
  if strict_upgrade; then
    wait_block_count_at_least 18532 "${target}" "node4 replacement ${replacement}"
  fi
done
wait_zakura_body_frontiers_at_tip "${target}" "post-reorg"
wait_metric_at_least 19001 sync_block_reorg_reset 1 node1
wait_metric_at_least 19002 sync_block_reorg_reset 1 node2
assert_block_sync_budget_empty "post-reorg"
snapshot_timeline "post-reorg"

if [[ "${ZAKURA_E2E_RESTART_MATRIX}" == "1" ]]; then
  log "restart-matrix: restarting node2 after the non-finalized reorg"
  post_reorg_hash=$(block_hash 18332 "${target}")
  [[ -n "${post_reorg_hash}" ]] || fail "could not read node2 post-reorg tip hash before restart"
  post_reorg_log_lines=$(docker_log_line_count)
  restart_node2_preserving_state "restart-matrix post-reorg"
  wait_block_count_at_least 18332 "${target}" "node2 restart-matrix post-reorg" "${CATCHUP_TIMEOUT}"
  wait_zakura_body_frontier_at_tip 19002 18332 "${target}" "node2 restart-matrix post-reorg" "${CATCHUP_TIMEOUT}"
  assert_node2_post_reorg_restore \
    "${post_reorg_log_lines}" "${target}" "${post_reorg_hash}"
  assert_block_sync_budget_empty "restart-matrix post-reorg"
fi

assert_trace_layout
run_trace_oracle

log "PASS (${RUN_LABEL}): Zakura body frontier, peer serving, reorg survival, and legacy compatibility verified"
