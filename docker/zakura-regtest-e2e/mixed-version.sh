#!/usr/bin/env bash
#
# Drive and assert the mixed-version Zakura regtest e2e.
#
# run.sh execs this script for ZAKURA_E2E_MODE=mixed-version; its header
# describes the topology and the assertions. The released binary speaks
# p2p-v2/3 and the binary under test speaks p2p-v2/4, so the two cohorts can
# only meet over legacy TCP (docs/specs/zakura-quic.md, COMPAT-7 and COMPAT-8).
#
# Environment:
#   ZAKURAD_BIN                the binary under test (default: target/debug/zakurad,
#                              built if missing, as in the other modes)
#   ZAKURAD_OLD_BIN            the released binary; when unset, the pinned release
#                              tarball is downloaded, SHA256-verified, and cached
#   ZAKURA_E2E_OLD_BIN_CACHE   download cache (default: target/zakura-e2e-old-zakurad)
#   ZAKURA_E2E_TRACE_DIR       where node logs are saved for inspection
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/../docker-compose.zakura-regtest-e2e-mixed-version.yml"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# shellcheck source=lib.sh source-path=SCRIPTDIR
source "${SCRIPT_DIR}/lib.sh"

READY_TIMEOUT="${READY_TIMEOUT:-120}"
PROPAGATE_TIMEOUT="${PROPAGATE_TIMEOUT:-180}"
# Blocks each miner generates, one at a time.
GENERATE_BLOCKS="${GENERATE_BLOCKS:-3}"
# After propagation, keep watching the cross-version legacy connections at an
# idle tip for longer than the legacy heartbeat (60 s) and the syncer restart
# delay (45 s), so a close from either is caught and explained.
MIXED_SOAK_SECONDS="${MIXED_SOAK_SECONDS:-90}"
RUN_LABEL="${ZAKURA_REGTEST_E2E_LABEL:-zakura-mixed-version}"
EVIDENCE_DIR="${ZAKURA_E2E_TRACE_DIR:-/tmp/zakura-regtest-e2e-traces-${RUN_LABEL}}"

# The latest release before p2p-v2/4. Pin the tarball digests here rather than
# trusting the SHA256SUMS.txt fetched next to them.
OLD_RELEASE="v1.8.9"
declare -A OLD_RELEASE_SHA256=(
  [x86_64]=3661e01e474cb46ad453b93a2c00704a3634eeb169bec037fa15afb916a2ea03
  [aarch64]=3701fbedce4fd9f43bcce23317527d2b65670bf5c0b355793ba38294a9bcb635
)
OLD_RELEASE_URL="https://github.com/zakura-core/zakura/releases/download/${OLD_RELEASE}"

# Every node shares the host network, so any running node can serve the
# harness's loopback requests. old-dual starts first and runs throughout.
E2E_HTTP_CONTAINER=zakura-mixed-old-dual

# name -> "container rpc-port metrics-port"
declare -A NODE=(
  [old-dual]="zakura-mixed-old-dual 28232 29001"
  [old-zakura]="zakura-mixed-old-zakura 28332 29002"
  [new-dual]="zakura-mixed-new-dual 28432 29003"
  [new-zakura]="zakura-mixed-new-zakura 28532 29004"
  [old-dialer]="zakura-mixed-old-dialer 28632 29005"
)
ALL_NODES=(old-dual old-zakura new-dual new-zakura old-dialer)
DUAL_NODES=(old-dual new-dual old-dialer)
container_of() { local c; read -r c _ _ <<< "${NODE[$1]}"; printf '%s' "${c}"; }
rpc_port_of() { local p; read -r _ p _ <<< "${NODE[$1]}"; printf '%s' "${p}"; }
metrics_port_of() { local m; read -r _ _ m <<< "${NODE[$1]}"; printf '%s' "${m}"; }

compose() { docker compose -f "${COMPOSE_FILE}" "$@"; }

command -v docker >/dev/null || fail "docker is required"
command -v jq >/dev/null || fail "jq is required to parse RPC responses"
command -v curl >/dev/null || fail "curl is required to download the released zakurad"

# Prints the user agent a binary advertises, /Zakura:<version>/, from its
# --version output (the user agent drops the build metadata).
user_agent_of() {
  local bin="$1" version
  version=$(docker run --rm -v "${bin}:/usr/local/bin/zakurad:ro" debian:trixie-slim \
      /usr/local/bin/zakurad --version 2>/dev/null \
    | sed -n 's/^zakurad \([^+ ]*\).*$/\1/p' | tail -n 1)
  [[ -n "${version}" ]] || fail "could not read the version of ${bin} in the Linux container"
  printf '/Zakura:%s/' "${version}"
}

# Use ZAKURAD_OLD_BIN, or download, verify, and cache the pinned release.
ensure_old_zakurad_bin() {
  if [[ -n "${ZAKURAD_OLD_BIN:-}" ]]; then
    [[ -x "${ZAKURAD_OLD_BIN}" ]] || fail "ZAKURAD_OLD_BIN is not executable: ${ZAKURAD_OLD_BIN}"
    export ZAKURAD_OLD_BIN
    return 0
  fi

  local arch
  case "$(uname -m)" in
    x86_64|amd64) arch=x86_64 ;;
    arm64|aarch64) arch=aarch64 ;;
    *) fail "no ${OLD_RELEASE} release binary for $(uname -m); set ZAKURAD_OLD_BIN" ;;
  esac
  local cache="${ZAKURA_E2E_OLD_BIN_CACHE:-${REPO_DIR}/target/zakura-e2e-old-zakurad}/${OLD_RELEASE}-linux-${arch}"
  ZAKURAD_OLD_BIN="${cache}/zakurad"
  export ZAKURAD_OLD_BIN
  [[ -x "${ZAKURAD_OLD_BIN}" ]] && return 0

  local tarball="zakurad-${OLD_RELEASE}-linux-${arch}.tar.gz"
  local pinned="${OLD_RELEASE_SHA256[${arch}]}"
  local download listed actual
  log "downloading ${tarball} from the ${OLD_RELEASE} release"
  mkdir -p "${cache}"
  download="$(mktemp -d "${cache}/download.XXXXXX")"
  curl -fsSL --retry 3 -o "${download}/${tarball}" "${OLD_RELEASE_URL}/${tarball}" \
    || fail "could not download ${OLD_RELEASE_URL}/${tarball}"
  curl -fsSL --retry 3 -o "${download}/SHA256SUMS.txt" "${OLD_RELEASE_URL}/SHA256SUMS.txt" \
    || fail "could not download ${OLD_RELEASE_URL}/SHA256SUMS.txt"
  listed=$(awk -v f="./${tarball}" '$2 == f || $2 == substr(f, 3) { print $1 }' \
    "${download}/SHA256SUMS.txt")
  actual=$(sha256sum "${download}/${tarball}" | cut -d' ' -f1)
  printf '  %s sha256 pinned=%s listed=%s actual=%s\n' \
    "${tarball}" "${pinned}" "${listed:-<none>}" "${actual}"
  [[ "${listed}" == "${pinned}" ]] \
    || fail "${OLD_RELEASE} SHA256SUMS.txt does not list the pinned digest for ${tarball}"
  [[ "${actual}" == "${pinned}" ]] \
    || fail "${tarball} digest ${actual} does not match the pinned ${pinned}"
  tar -xzf "${download}/${tarball}" -C "${download}" ./bin/zakurad \
    || fail "${tarball} does not contain ./bin/zakurad"
  mv "${download}/bin/zakurad" "${ZAKURAD_OLD_BIN}"
  rm -rf "${download}"
}

ensure_zakurad_bin
ensure_old_zakurad_bin
NEW_UA=$(user_agent_of "${ZAKURAD_BIN}")
OLD_UA=$(user_agent_of "${ZAKURAD_OLD_BIN}")
log "run mode: mixed-version (${RUN_LABEL})"
printf '  new binary %s (%s)\n  old binary %s (%s)\n' \
  "${ZAKURAD_BIN}" "${NEW_UA}" "${ZAKURAD_OLD_BIN}" "${OLD_UA}"
[[ "${NEW_UA}" != "${OLD_UA}" ]] \
  || fail "the old and new binaries advertise the same user agent ${NEW_UA}"

CONFIG_DIR="$(mktemp -d "${TMPDIR:-/tmp}/zakura-regtest-e2e-mixed-configs.XXXXXX")"
cp "${SCRIPT_DIR}"/mixed-version/*.toml "${CONFIG_DIR}/"
export ZAKURA_MIXED_CONFIG_DIR="${CONFIG_DIR}"

cleanup() {
  local status=$?
  set +e
  log "node logs (tail)"
  compose logs --tail=30
  mkdir -p "${EVIDENCE_DIR}"
  compose logs --no-color --timestamps > "${EVIDENCE_DIR}/docker-compose.log" 2>&1
  log "tearing down (logs saved to ${EVIDENCE_DIR}/docker-compose.log)"
  compose down --volumes --remove-orphans --timeout 5
  rm -rf "${CONFIG_DIR}"
  return "${status}"
}
trap cleanup EXIT

# Prints the node ID a node logs when its v2 endpoint is ready. Each version
# derives the ID from the configured secret key its own way, so the harness
# reads it rather than pinning it.
wait_node_id() {
  local name="$1" container id deadline=$((SECONDS + READY_TIMEOUT))
  container=$(container_of "${name}")
  while (( SECONDS < deadline )); do
    id=$(docker logs "${container}" 2>&1 \
      | sed -n 's/.*Zakura P2P endpoint ready.*node_id=\([0-9a-f]\{64\}\).*/\1/p' \
      | tail -n 1)
    if [[ -n "${id}" ]]; then
      printf '%s' "${id}"
      return 0
    fi
    sleep 2
  done
  fail "${name} did not log its Zakura node ID within ${READY_TIMEOUT}s"
}

start_node() {
  local name="$1"
  compose up -d "${name}" || fail "could not start ${name}"
  wait_ready "$(rpc_port_of "${name}")" "${name}"
}

# metric_sum <name> <metric> -> the sum of every labelled series of <metric>.
metric_sum() {
  container_http "$(metrics_port_of "$1")" "/metrics" "" 5 2>/dev/null \
    | awk -v n="$2" '{ split($1, s, "{") } s[1] == n { v += $2 } END { print v + 0 }'
}

node_metric() { metric "$(metrics_port_of "$1")" "$2"; }

# wait_metric_sum_above <name> <metric> <floor> <timeout> -> waits until the
# sum of <metric>'s series on <name> exceeds <floor>.
wait_metric_sum_above() {
  local name="$1" series="$2" floor="$3" timeout="$4" value
  local deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    value=$(metric_sum "${name}" "${series}")
    printf '  %s %s=%s (want > %s)\n' "${name}" "${series}" "${value}" "${floor}"
    (( value > floor )) && return 0
    sleep 5
  done
  fail "${name} ${series} stayed at or below ${floor} within ${timeout}s"
}

# cross_links <name> <user-agent> -> one "addr inbound|outbound" line for each
# legacy connection to a peer with <user-agent>, sorted. An inbound connection's
# address carries the dialer's ephemeral port, so the same line later proves
# the same TCP connection is still open.
cross_links() {
  rpc "$(rpc_port_of "$1")" getpeerinfo \
    | jq -r --arg ua "$2" '
        .result // [] | .[]
        | select(.subver == $ua)
        | "\(.addr) \(if .inbound then "inbound" else "outbound" end)"
      ' \
    | sort
}

# The legacy connections that cross the version boundary. Each dual node dials
# a dual node of the other cohort, so both roles of the upgrade prelude run.
#   new-dual   -> old-dual   new side initiates, old side rejects
#   old-dialer -> new-dual   old side initiates, new side rejects
declare -A CROSS_LINK_UA=(
  [old-dual]="${NEW_UA}"
  [new-dual]="${OLD_UA}"
  [old-dialer]="${NEW_UA}"
)
# Each node's legacy peers across the boundary, and the dual nodes' legacy ports.
declare -A CROSS_LINK_PEERS=(
  [old-dual]="new-dual"
  [new-dual]="old-dual old-dialer"
  [old-dialer]="new-dual"
)
declare -A LEGACY_LISTEN_ADDR=(
  [old-dual]="127.0.0.1:28233"
  [new-dual]="127.0.0.1:28433"
  [old-dialer]="127.0.0.1:28633"
)
declare -A CROSS_LINKS_AT_START=()
# "<node> <addr> <direction>" -> why the connection closed.
declare -A CLOSED_CROSS_LINKS=()

cross_links_ready() {
  local old_dual new_dual old_dialer
  old_dual=$(cross_links old-dual "${NEW_UA}")
  new_dual=$(cross_links new-dual "${OLD_UA}")
  old_dialer=$(cross_links old-dialer "${NEW_UA}")
  printf '  old-dual: [%s]  new-dual: [%s]  old-dialer: [%s]\n' \
    "${old_dual//$'\n'/, }" "${new_dual//$'\n'/, }" "${old_dialer//$'\n'/, }"
  grep -q ' inbound$' <<< "${old_dual}" \
    && grep -qx '127.0.0.1:28233 outbound' <<< "${new_dual}" \
    && grep -q ' inbound$' <<< "${new_dual}" \
    && grep -qx '127.0.0.1:28433 outbound' <<< "${old_dialer}"
}

# The legacy peer set disconnects a peer after three consecutive empty or
# failed FindBlocks/FindHeaders responses (FIND_RESPONSE_STALL_THRESHOLD in
# peer_set/stall_tracker.rs, in v1.8.9 and in this build). At an idle Regtest
# tip the legacy syncer keeps running rounds of FANOUT = 3 FindBlocks, so a node
# whose only legacy peer has nothing newer drops it in one round, then redials
# it. Neither the address book nor the ban list changes, and the rule ignores
# the peer's version.
#
# stall_dropped <name> <addr> -> whether <name> logged that disconnect for <addr>.
stall_dropped() {
  docker logs "$(container_of "$1")" 2>&1 \
    | grep -F 'dropping stalled peer: exceeded FindBlocks/FindHeaders stall threshold' \
    | grep -E "peer=${2//./\\.}( |\$)" >/dev/null
}

# Records each cross-version connection seen at start that has since closed,
# and fails unless one end of it logged the FindBlocks stall disconnect. For
# the remote end, an outbound connection's peer is the node listening on its
# address, and an inbound connection's peer dialed this node's listener.
check_cross_links() {
  local phase="$1" name peer link addr remote reason
  declare -A now=() closed=()
  for name in "${!CROSS_LINK_UA[@]}"; do
    now[${name}]=$(cross_links "${name}" "${CROSS_LINK_UA[${name}]}")
    while IFS= read -r link; do
      [[ -z "${link}" ]] && continue
      grep -qxF "${link}" <<< "${now[${name}]}" || closed[${name}]+="${link}"$'\n'
    done <<< "${CROSS_LINKS_AT_START[${name}]}"
  done
  for name in "${!closed[@]}"; do
    while IFS= read -r link; do
      [[ -z "${link}" || -n "${CLOSED_CROSS_LINKS["${name} ${link}"]:-}" ]] && continue
      addr="${link%% *}" reason=""
      if stall_dropped "${name}" "${addr}"; then
        reason="${name} dropped it as a FindBlocks staller"
      fi
      for peer in ${CROSS_LINK_PEERS[${name}]}; do
        [[ -n "${reason}" ]] && break
        if [[ "${link}" == *" outbound" ]]; then
          [[ "${LEGACY_LISTEN_ADDR[${peer}]}" == "${addr}" ]] || continue
          while IFS= read -r remote; do
            if [[ "${remote}" == *" inbound" ]] && stall_dropped "${peer}" "${remote%% *}"; then
              reason="${peer} dropped its end (${remote}) as a FindBlocks staller"
            fi
          done <<< "${closed[${peer}]:-}"
        else
          remote="${LEGACY_LISTEN_ADDR[${name}]} outbound"
          if grep -qxF "${remote}" <<< "${closed[${peer}]:-}" \
            && stall_dropped "${peer}" "${remote%% *}"; then
            reason="${peer} dropped its end (${remote}) as a FindBlocks staller"
          fi
        fi
      done
      [[ -n "${reason}" ]] || fail \
        "${phase}: ${name}'s cross-version legacy connection ${link} closed, and neither end logged a FindBlocks stall disconnect (now: [${now[${name}]//$'\n'/, }])"
      CLOSED_CROSS_LINKS["${name} ${link}"]="${reason}"
      printf '  %s: %s cross-version connection %s closed: %s\n' \
        "${phase}" "${name}" "${link}" "${reason}"
    done <<< "${closed[${name}]}"
  done
}

# Waits until every node has at least <target> blocks and the same hash there.
wait_all_nodes_at() {
  local target="$1" label="$2" deadline=$((SECONDS + PROPAGATE_TIMEOUT))
  local name heights behind h hash want_hash
  while (( SECONDS < deadline )); do
    heights="" behind=0
    for name in "${ALL_NODES[@]}"; do
      h=$(block_count "$(rpc_port_of "${name}")")
      heights+=" ${name}=${h}"
      (( h >= target )) || behind=1
    done
    printf '  %s:%s (target %s)\n' "${label}" "${heights}" "${target}"
    (( behind == 0 )) && break
    sleep 3
  done
  (( behind == 0 )) || fail "${label}: block ${target} did not reach every node:${heights}"

  want_hash=$(block_hash "$(rpc_port_of "${ALL_NODES[0]}")" "${target}")
  for name in "${ALL_NODES[@]}"; do
    hash=$(block_hash "$(rpc_port_of "${name}")" "${target}")
    [[ "${hash}" == "${want_hash}" ]] || fail \
      "${label}: ${name} has ${hash:-<none>} at height ${target}, ${ALL_NODES[0]} has ${want_hash}"
  done
}

# mine_and_propagate <miner> -> mines GENERATE_BLOCKS blocks on <miner> one at a
# time, and waits for each to reach every node of both cohorts.
mine_and_propagate() {
  local miner="$1" i target
  log "mining ${GENERATE_BLOCKS} block(s) on ${miner} and waiting for every node"
  for ((i = 1; i <= GENERATE_BLOCKS; i++)); do
    rpc "$(rpc_port_of "${miner}")" generate "[1]" | jq -e '.result | length == 1' >/dev/null \
      || fail "generate RPC failed on ${miner}"
    target=$(block_count "$(rpc_port_of "${miner}")")
    wait_all_nodes_at "${target}" "${miner} block ${i}/${GENERATE_BLOCKS}"
  done
}

# assert_metric <name> <metric> <op> <value>, with op one of == <= >=.
assert_metric() {
  local name="$1" series="$2" op="$3" want="$4" value
  value=$(node_metric "${name}" "${series}")
  printf '  %s %s=%s (want %s %s)\n' "${name}" "${series}" "${value}" "${op}" "${want}"
  awk -v v="${value}" -v w="${want}" -v op="${op}" \
    'BEGIN { exit !((op == "==" && v == w) || (op == "<=" && v <= w) || (op == ">=" && v >= w)) }' \
    || fail "${name} ${series}=${value}, want ${op} ${want}"
}

log "starting the old dual-stack seed"
start_node old-dual
OLD_DUAL_ID=$(wait_node_id old-dual)
log "starting the new dual-stack node (dials old-dual over legacy TCP)"
start_node new-dual
NEW_DUAL_ID=$(wait_node_id new-dual)
printf '  old-dual node ID (p2p-v2/3 derivation) %s\n' "${OLD_DUAL_ID}"
printf '  new-dual node ID (p2p-v2/4 derivation) %s\n' "${NEW_DUAL_ID}"

# Neither v2-only container exists yet, so replacing the config files is safe.
write_bootstrap_peer() {
  local file="${CONFIG_DIR}/$1" placeholder="$2" id="$3"
  sed "s/${placeholder}/${id}/" "${file}" > "${file}.tmp" && mv "${file}.tmp" "${file}" \
    || fail "could not write the bootstrap peer into $1"
}
write_bootstrap_peer old-zakura.toml OLD_DUAL_NODE_ID "${OLD_DUAL_ID}"
write_bootstrap_peer new-zakura.toml NEW_DUAL_NODE_ID "${NEW_DUAL_ID}"
grep -q "${OLD_DUAL_ID}@" "${CONFIG_DIR}/old-zakura.toml" || fail "could not write old-zakura's bootstrap peer"
grep -q "${NEW_DUAL_ID}@" "${CONFIG_DIR}/new-zakura.toml" || fail "could not write new-zakura's bootstrap peer"

log "starting the v2-only nodes and the old dialer"
start_node old-zakura
start_node new-zakura
start_node old-dialer

log "asserting the cross-version legacy connections form and stay on legacy"
deadline=$((SECONDS + READY_TIMEOUT)); links_ready=0
while (( SECONDS < deadline )); do
  if cross_links_ready; then links_ready=1; break; fi
  sleep 3
done
(( links_ready == 1 )) || fail "the cross-version legacy connections did not all form within ${READY_TIMEOUT}s"
for name in "${!CROSS_LINK_UA[@]}"; do
  CROSS_LINKS_AT_START[${name}]=$(cross_links "${name}" "${CROSS_LINK_UA[${name}]}")
done

# Both roles of the prelude ran and fell back to legacy. v1.8.9 runs the same
# prelude code, which counts each fallback, and logs it at debug level.
for name in "${DUAL_NODES[@]}"; do
  deadline=$((SECONDS + READY_TIMEOUT))
  while (( SECONDS < deadline )); do
    rejected=$(metric_sum "${name}" zakura_p2p_upgrade_prelude_rejected)
    printf '  %s zakura_p2p_upgrade_prelude_rejected=%s (want >= 1)\n' "${name}" "${rejected}"
    (( rejected >= 1 )) && break
    sleep 3
  done
  (( rejected >= 1 )) || fail "${name} never fell back to legacy after a rejected upgrade prelude"
done
# new-dual both dialed and was dialed by an old node, so it saw both roles.
rejected_at_start=$(metric_sum new-dual zakura_p2p_upgrade_prelude_rejected)
(( rejected_at_start >= 2 )) \
  || fail "new-dual fell back to legacy fewer than two times (once per prelude role)"
# Release builds compile debug logs out (release_max_level_info), as in the
# downloaded v1.8.9 binary, so only a debug build of new-dual shows the
# fallback for each role: connection_kind "Out" as initiator, "In" as responder.
new_dual_logs=$(docker logs "$(container_of new-dual)" 2>&1)
if ! grep -q ' DEBUG ' <<< "${new_dual_logs}"; then
  printf '  new-dual fallback log lines skipped: the binary is built without debug logs\n'
else
  for kind in Out In; do
    fallbacks=$(grep 'Zakura upgrade not completed; continuing on the legacy connection' \
        <<< "${new_dual_logs}" | grep -c "connection_kind=\"${kind}\"" || true)
    printf '  new-dual fallback log lines with connection_kind=%s: %s (want >= 1)\n' \
      "${kind}" "${fallbacks}"
    (( fallbacks >= 1 )) \
      || fail "new-dual did not log a legacy fallback for connection_kind=${kind}"
  done
fi

log "asserting same-version v2 connections"
wait_metric_at_least "$(metrics_port_of new-zakura)" zakura_p2p_conn_active 1 new-zakura
wait_metric_at_least "$(metrics_port_of new-dual)" zakura_p2p_conn_active 1 new-dual
wait_metric_at_least "$(metrics_port_of old-zakura)" zakura_p2p_conn_active 1 old-zakura
wait_metric_at_least "$(metrics_port_of old-dual)" zakura_p2p_conn_active 1 old-dual

log "asserting the new nodes' v2 handshakes are post-quantum"
assert_post_quantum_handshakes "$(metrics_port_of new-dual)" new-dual
assert_post_quantum_handshakes "$(metrics_port_of new-zakura)" new-zakura

# Mine on a node of each kind in each cohort. Every block crosses the legacy
# bridge once, and the v2-only miners' blocks cross both cohorts' v2 links too.
for miner in new-dual old-dual new-zakura old-zakura; do
  mine_and_propagate "${miner}"
  check_cross_links "after mining on ${miner}"
done

log "watching the cross-version legacy connections for ${MIXED_SOAK_SECONDS}s at an idle tip"
soak_end=$((SECONDS + MIXED_SOAK_SECONDS))
while (( SECONDS < soak_end )); do
  check_cross_links "soak"
  sleep 10
done
check_cross_links "end of soak"
if (( ${#CLOSED_CROSS_LINKS[@]} == 0 )); then
  for name in "${!CROSS_LINK_UA[@]}"; do
    printf '  %s kept [%s] open for the whole run\n' \
      "${name}" "${CROSS_LINKS_AT_START[${name}]//$'\n'/, }"
  done
else
  # A dropped staller is redialed. The redial must run the upgrade prelude
  # again, be rejected again, and continue the legacy handshake. At an idle tip
  # the redialed connection can be stall-dropped again within milliseconds, so
  # the counter, not getpeerinfo, is the evidence.
  log "waiting for a redial across the version boundary to run a fresh, rejected prelude"
  wait_metric_sum_above new-dual zakura_p2p_upgrade_prelude_rejected "${rejected_at_start}" \
    "$(( READY_TIMEOUT + 90 ))"
fi
printf '  stall disconnects logged: %s\n' "$(
  for name in "${DUAL_NODES[@]}"; do
    printf '%s=%s ' "${name}" "$(docker logs "$(container_of "${name}")" 2>&1 \
      | grep -c 'dropping stalled peer: exceeded FindBlocks/FindHeaders stall threshold' || true)"
  done)"

log "asserting no upgrade error and no v2 connection across versions"
for name in "${DUAL_NODES[@]}"; do
  assert_metric "${name}" zakura_p2p_handshake_upgrade_error == 0
  assert_metric "${name}" zakura_p2p_upgrade_prelude_malformed == 0
done
# new-dual's only v2 peer is new-zakura, which has no legacy stack, so no
# legacy upgrade may complete on it and it may hold one v2 connection. The old
# nodes can each reach at most the other old nodes over v2.
# (zakura_p2p_handshake_upgraded counts every authenticated v2 registration,
# native ones too, so the upgrade's own info line is the evidence here.)
upgrades=$(docker logs "$(container_of new-dual)" 2>&1 \
  | grep -c 'upgraded mutually P2P-v2-capable peer to Zakura' || true)
printf '  new-dual completed legacy->v2 upgrades: %s (want 0)\n' "${upgrades}"
(( upgrades == 0 )) || fail "new-dual upgraded a legacy connection to v2, but its only legacy peers are old nodes"
assert_metric new-dual zakura_p2p_conn_active == 1
assert_metric new-zakura zakura_p2p_conn_active == 1
assert_metric old-dual zakura_p2p_conn_active '<=' 2
assert_metric old-zakura zakura_p2p_conn_active '<=' 2
assert_metric old-dialer zakura_p2p_conn_active '<=' 2
for name in new-dual new-zakura; do
  printf '  %s zakura_p2p_discovery_dial_alpn_mismatch=%s\n' \
    "${name}" "$(node_metric "${name}" zakura_p2p_discovery_dial_alpn_mismatch)"
done
# The counters cover every v2 handshake of the run, so this also shows no
# classical handshake happened on a new node.
assert_post_quantum_handshakes "$(metrics_port_of new-dual)" new-dual
assert_post_quantum_handshakes "$(metrics_port_of new-zakura)" new-zakura

log "asserting no node banned or penalized the other cohort"
# Every node shares 127.0.0.1, so any ban would ban the other cohort too.
for name in "${ALL_NODES[@]}"; do
  assert_metric "${name}" address_book_misbehavior_banned_total == 0
  # The ban warning, the refused-inbound debug line, and misbehavior reports.
  # Span fields such as `bans=BannedIps { .. }` are not bans.
  ban_lines=$(docker logs "$(container_of "${name}")" 2>&1 \
    | grep -E 'banned the IP address|banned inbound connection|misbehavior report|misbehaving' \
    || true)
  if [[ -n "${ban_lines}" ]]; then
    head -n 20 <<< "${ban_lines}" >&2
    fail "${name} logged a ban or misbehavior report"
  fi
  printf '  %s ban or misbehavior log lines: none\n' "${name}"
done

log "PASS (${RUN_LABEL}): blocks crossed the version boundary over legacy, rejected upgrades kept every cross-version connection on legacy, same-version v2 worked, and nobody banned the other cohort"
