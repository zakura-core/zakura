# shellcheck shell=bash
#
# Helpers shared by the Zakura regtest e2e drivers (run.sh and mixed-version.sh).
# Source this file; it defines functions and constants only.
#
# Callers set REPO_DIR and READY_TIMEOUT before using the helpers that read
# them. E2E_HTTP_CONTAINER names the container whose network namespace serves
# RPC and metrics requests; it defaults to the four-node stack's node1.

log()  { printf '\n=== %s ===\n' "$*"; }
fail() { printf '\nFAILED: %s\n' "$*" >&2; exit 1; }

# Ensure a container-compatible zakurad binary exists and export it as
# ZAKURAD_BIN. Docker Desktop runs Linux containers on macOS, so its
# bind-mounted executable must be Linux too.
ensure_zakurad_bin() {
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
}

# Fetch localhost-only node endpoints from inside a node's Linux network
# namespace. Docker Desktop does not expose host-network container ports to the
# macOS host unless its optional host-networking feature is enabled.
container_http() {
  local port="$1" path="$2" payload="$3" max_time="$4"
  docker exec "${E2E_HTTP_CONTAINER:-zakura-node-1}" perl -MIO::Socket::INET -e '
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

# metric <port> <series> -> counter value (0 if absent). <series> is a metric
# name, or a name with its labels exactly as /metrics renders them, such as
# 'zakura_quic_tls_key_exchange{group="X25519MLKEM1024"}'.
metric() {
  local port="$1" name="$2"
  container_http "${port}" "/metrics" "" 5 2>/dev/null \
    | awk -v n="${name}" '$1==n {v=$2} END {print (v==""?0:v)}'
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

# The only v2 TLS series a node may export: the hybrid key-exchange group
# (zakura-quic TLS-9) and the composite signature scheme (TLS-4).
PQ_KEY_EXCHANGE_SERIES='zakura_quic_tls_key_exchange{group="X25519MLKEM1024"}'
PQ_SIGNATURE_SERIES='zakura_quic_tls_signature_verified{scheme="ed25519_mldsa65"}'

# assert_post_quantum_handshakes <metrics-port> <label>
# Requires at least one completed X25519MLKEM1024 key exchange and one verified
# Ed25519+ML-DSA-65 handshake signature, and no other group or scheme.
assert_post_quantum_handshakes() {
  local port="$1" label="$2" other
  wait_metric_at_least "${port}" "${PQ_KEY_EXCHANGE_SERIES}" 1 "${label}"
  wait_metric_at_least "${port}" "${PQ_SIGNATURE_SERIES}" 1 "${label}"
  other=$(container_http "${port}" "/metrics" "" 5 \
    | awk -v kx="${PQ_KEY_EXCHANGE_SERIES}" -v sig="${PQ_SIGNATURE_SERIES}" '
        $1 ~ /^zakura_quic_tls_(key_exchange|signature_verified)/ && $1 != kx && $1 != sig {
          printf "%s%s=%s", (n++ ? " " : ""), $1, $2
        }') \
    || fail "could not read ${label} /metrics"
  printf '  %s other TLS groups or schemes: %s (want none)\n' "${label}" "${other:-none}"
  [[ -z "${other}" ]] || fail \
    "${label} ran v2 handshakes outside X25519MLKEM1024 and Ed25519+ML-DSA-65: ${other}"
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

peer_count() { rpc "$1" getpeerinfo | jq -r 'if .result then (.result | length) else 0 end'; }
block_count() { rpc "$1" getblockcount | jq -r '.result // 0'; }
block_hash() { rpc "$1" getblockhash "[$2]" | jq -r '.result // empty'; }
