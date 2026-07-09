#!/usr/bin/env bash
# Install or prepare commands for Zakura's zcashd-compat operating modes.
set -euo pipefail

SCRIPT_SOURCE="${BASH_SOURCE[0]:-}"
if [[ -n "$SCRIPT_SOURCE" && -f "$SCRIPT_SOURCE" ]]; then
  SCRIPT_DIR="$(cd "$(dirname "$SCRIPT_SOURCE")" && pwd)"
else
  SCRIPT_DIR="$PWD"
fi
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
UNITY_ROOT="$(cd "$REPO_ROOT/.." && pwd)"

ZAKURA_RELEASE_TAG="v0.0.1-alpha.1"
ZAKURA_ARCHIVE="zakurad-${ZAKURA_RELEASE_TAG}-linux-x86_64.tar.gz"
ZAKURA_URL="https://github.com/zakura-core/zakura/releases/download/${ZAKURA_RELEASE_TAG}/${ZAKURA_ARCHIVE}"
# sha256 of ZAKURA_ARCHIVE from the release's SHA256SUMS.txt. Pin it once the
# zakurad-named release artifact is published: an unpinned download in a
# `curl | bash` installer is a supply-chain hole. Empty = skip verification.
ZAKURA_ARCHIVE_SHA256=""
ZAKURA_MEMBER="./bin/zakurad"
ZAKURA_DOCKER_IMAGE="valargroup/zakura:0.0.1-alpha.1@sha256:74f76366eed48bdfb15a3386d033a6e3e2d7481f40cb06c5c6ae3c5e9f77e4b5"
ZAKURA_COMPAT_DOCKER_IMAGE="valargroup/zakura:zcashd-compat-0.0.1-alpha.1@sha256:f3f36dc215a15f3724690529244df8527cc0389ccf4bf9348206cb49388ac8c8"
ZAKURA_COMPAT_DOCKER_FALLBACK_IMAGE="valargroup/zakura:zcashd-compat-latest"
ZAKURA_DEFAULT_CACHE_DIR="${XDG_CACHE_HOME:-${HOME}/.cache}/zakura"
ZAKURA_DOCKER_RUNTIME_UID=10001
ZAKURA_DOCKER_RUNTIME_GID=10001

MANIFEST_PATH="$REPO_ROOT/zebrad/zcashd-compat-manifest.json"
TARGET_TRIPLE="x86_64-pc-linux-gnu"
ZCASHD_RUNTIME_ARCHIVE_URL="https://github.com/valargroup/zcashd/releases/download/v0.0.1-compat-alpha.3/zcashd-zebra-compat-v0.0.1-compat-alpha.3-linux-x86_64.tar.gz"
ZCASHD_RUNTIME_ARCHIVE_SHA256="a2deaf9cfb89e8a1b34664ace0393336b7b5095a8fe0b4c7fb67b3715012ef47"
ZCASHD_RUNTIME_ARCHIVE_MEMBER_BINARY_PATH="./bin/zcashd"

MODE=""
NETWORK="Mainnet"
ZCASHD_DEFAULT_DATADIR="${HOME}/.zcash"
ZAKURA_STATE_DIR="$ZAKURA_DEFAULT_CACHE_DIR"
ZCASHD_DATADIR="$ZCASHD_DEFAULT_DATADIR"
INSTALL_DIR="${HOME}/.local/zcashd-compat"
CACHE_DIR="${HOME}/.cache/zcashd-compat"
COOKIE_DIR=""
ZAKURA_P2P_ADDR=""
COMPAT_LISTEN_ADDR="127.0.0.1:28232"
ZCASHD_CONF=""
ZAKURAD_PATH=""
ZCASHD_PATH=""
ZCASH_SRC_DIR=""
ZCASHD_DOCKER_IMAGE=""
DOWNLOAD_BINARIES=1
DOWNLOAD_BINARIES_SET=0
ZAKURA_STATE_DIR_SET=0
ZCASHD_DATADIR_SET=0
DRY_RUN=0
NON_INTERACTIVE=0
UNSAFE_LOW_SPECS=0

ERRORS=()
LOW_SPEC_ERRORS=()
WARNINGS=()
PROMPT_FD=0
PROMPT_INPUT_ERROR_REPORTED=0

if [[ ! -t 0 ]]; then
  if ! { exec {PROMPT_FD}</dev/tty; } 2>/dev/null; then
    PROMPT_FD=-1
  fi
fi

USE_ANSI=0
if [[ -t 1 && -z "${NO_COLOR:-}" ]]; then
  USE_ANSI=1
fi

RESET=""
BOLD=""
DIM=""
RED=""
YELLOW=""
GREEN=""
BLUE=""
CYAN=""

if ((USE_ANSI)); then
  RESET=$'\033[0m'
  BOLD=$'\033[1m'
  DIM=$'\033[2m'
  RED=$'\033[31m'
  YELLOW=$'\033[33m'
  GREEN=$'\033[32m'
  BLUE=$'\033[34m'
  CYAN=$'\033[36m'
fi

style() {
  local color="$1"
  local text="$2"

  if ((USE_ANSI)); then
    printf '%s%s%s' "$color" "$text" "$RESET"
  else
    printf '%s' "$text"
  fi
}

print_section() {
  local marker="$1"
  local title="$2"

  if ((USE_ANSI)); then
    printf '\n%s %s\n' "$(style "$BLUE$BOLD" "$marker")" "$(style "$BOLD" "$title")"
    printf '%s\n' "$(style "$DIM" "----------------------------------------")"
  else
    printf '\n%s\n' "$title"
  fi
}

print_command_block_start() {
  if ((USE_ANSI)); then
    printf '%s\n' "$(style "$CYAN" "> Run the command(s) below:")"
    printf '%s\n' "$(style "$DIM" "----------------------------------------")"
  fi
}

print_command_block_end() {
  if ((USE_ANSI)); then
    printf '%s\n' "$(style "$DIM" "----------------------------------------")"
  fi
}

usage() {
  cat <<'EOF'
Usage: install-zcashd-compat.sh [options]

Interactive by default. Use flags for repeatable, non-interactive runs.

Modes:
  split-binary               Download zakurad and zcashd, print separate commands
  supervised                 Download zakurad and zcashd, print Zakura-supervised command
  docker-split-containers    Pull images, print separate docker run commands
  docker-supervised          Pull compat image, print single supervised docker run command
  build-from-source          Validate source tree paths, print build/start commands

Options:
  --mode MODE
  --network NETWORK
  --zakura-state-dir DIR
  --zcashd-datadir DIR
  --install-dir DIR
  --cache-dir DIR
  --cookie-dir DIR
  --zakura-p2p-addr HOST:PORT Zakura legacy P2P listener; zcashd is pinned to its port
                             (default [::]:8233 mainnet, [::]:18233 testnet/regtest)
  --compat-listen-addr ADDR  Zakura zcashd-compat RPC listener (default 127.0.0.1:28232)
  --zcash-conf FILE
  --zakurad-path PATH
  --zcashd-path PATH
  --zcashd-docker-image IMAGE
  --download-binaries yes|no
  --dry-run                  Do not download archives or pull Docker images
  --unsafe-low-specs         Report hardware/disk failures as warnings
  -y, --yes, --non-interactive
  -h, --help
EOF
}

add_error() {
  ERRORS+=("$1")
}

add_low_spec_error() {
  LOW_SPEC_ERRORS+=("$1")
}

add_warning() {
  WARNINGS+=("$1")
}

print_list() {
  local marker_color="${1:-$YELLOW}"
  local marker="-"
  local item
  shift || true

  if ((USE_ANSI)); then
    marker="$(style "$marker_color" "[!]")"
  fi

  for item in "$@"; do
    printf -- '%s %s\n' "$marker" "$item"
  done
}

finalize_checks() {
  if ((${#LOW_SPEC_ERRORS[@]} > 0)); then
    if ((UNSAFE_LOW_SPECS)); then
      local error
      for error in "${LOW_SPEC_ERRORS[@]}"; do
        WARNINGS+=("${error}. continuing because --unsafe-low-specs was explicitly provided")
      done
      LOW_SPEC_ERRORS=()
    else
      ERRORS+=("${LOW_SPEC_ERRORS[@]}")
      LOW_SPEC_ERRORS=()
    fi
  fi

  if ((${#ERRORS[@]} > 0)); then
    if ((USE_ANSI)); then
      local marker
      marker="$(style "$RED$BOLD" "[x]")"
      printf '\n%s %s\n' "$marker" "$(style "$RED$BOLD" "Installer validation failed:")" >&2
      print_list "$RED" "${ERRORS[@]}" >&2
      exit 1
    fi
      printf '\nInstaller validation failed:\n' >&2
      print_list "$YELLOW" "${ERRORS[@]}" >&2
      exit 1
  fi

  if ((${#WARNINGS[@]} > 0)); then
    if ((USE_ANSI)); then
      local marker
      marker="$(style "$YELLOW$BOLD" "[!]")"
      printf '\n%s %s\n' "$marker" "$(style "$YELLOW$BOLD" "Installer warnings:")"
    else
      printf '\nInstaller warnings:\n'
    fi
    print_list "$YELLOW" "${WARNINGS[@]}"
    printf '\n'
    WARNINGS=()
  fi
}

require_value() {
  local name="$1"
  local value="${2:-}"
  if [[ -z "$value" ]]; then
    echo "$name requires a value" >&2
    usage >&2
    exit 2
  fi
}

abs_path() {
  local path="$1"
  if [[ "$path" = /* ]]; then
    printf '%s\n' "$path"
  else
    printf '%s/%s\n' "$PWD" "$path"
  fi
}

shell_quote() {
  printf '%q' "$1"
}

sanitize_terminal_input() {
  LC_ALL=C sed $'s/\x1B\\[[0-9;?]*[ -\\/]*[@-~]//g' | tr -d '[:cntrl:]'
}

read_prompt() {
  local prompt="$1"
  local var_name="$2"
  local read_opts=(-r)

  if ((PROMPT_FD < 0)); then
    if ((PROMPT_INPUT_ERROR_REPORTED == 0)); then
      add_error "interactive prompts require a terminal when the installer is read from stdin; pass --non-interactive with explicit options"
      PROMPT_INPUT_ERROR_REPORTED=1
    fi
    return 1
  fi

  # readline editing (backspace/delete, cursor movement) requires -e on a tty.
  if [[ -t "$PROMPT_FD" ]]; then
    read_opts=(-e -r)
  fi

  read "${read_opts[@]}" -u "$PROMPT_FD" -p "$prompt" "$var_name"
}

prompt_value() {
  local label="$1"
  local current="$2"
  local reply sanitized_reply

  if ((NON_INTERACTIVE)); then
    printf '%s\n' "$current"
    return
  fi

  if ! read_prompt "$label [$current]: " reply; then
    printf '%s\n' "$current"
    return
  fi
  sanitized_reply="$(printf '%s' "$reply" | sanitize_terminal_input)"
  if [[ -n "$sanitized_reply" ]]; then
    printf '%s\n' "$sanitized_reply"
  else
    printf '%s\n' "$current"
  fi
}

prompt_yes_no() {
  local label="$1"
  local default="$2"
  local reply sanitized_reply

  if ((NON_INTERACTIVE)); then
    printf '%s\n' "$default"
    return
  fi

  if ! read_prompt "$label [$default]: " reply; then
    printf '%s\n' "$default"
    return
  fi
  sanitized_reply="$(printf '%s' "$reply" | sanitize_terminal_input)"
  printf '%s\n' "${sanitized_reply:-$default}"
}

network_name_lowercase() {
  local network="$NETWORK"
  network="${network,,}"

  case "$network" in
    main | mainnet) printf 'mainnet\n' ;;
    test | testnet) printf 'testnet\n' ;;
    regtest) printf 'regtest\n' ;;
    *) printf '%s\n' "$network" ;;
  esac
}

zcashd_network_datadir() {
  local datadir="$1"

  case "$(network_name_lowercase)" in
    testnet) printf '%s/testnet3\n' "$datadir" ;;
    regtest) printf '%s/regtest\n' "$datadir" ;;
    *) printf '%s\n' "$datadir" ;;
  esac
}

# Value for ZAKURA_NETWORK__NETWORK, as zebra-network deserializes it.
network_config_value() {
  case "$(network_name_lowercase)" in
    mainnet) printf 'Mainnet\n' ;;
    testnet) printf 'Testnet\n' ;;
    regtest) printf 'Regtest\n' ;;
    *) printf '%s\n' "$NETWORK" ;;
  esac
}

# zebra-network's Config::default() hardcodes [::]:8233 for every network; the
# network-aware default_port() only applies when a port-less string is
# deserialized. So the P2P listener must be set explicitly per network.
network_default_p2p_port() {
  case "$(network_name_lowercase)" in
    mainnet) printf '8233\n' ;;
    *) printf '18233\n' ;;
  esac
}

# Network selection flags the sidecar zcashd needs, matching what Zakura's
# supervisor passes to its managed child (zcashd_compat/supervisor.rs).
zcashd_network_args() {
  case "$(network_name_lowercase)" in
    testnet) printf -- '-testnet\n' ;;
    # Zakura skips proof-of-work on regtest, so its blocks carry null Equihash
    # solutions that stock zcashd validation would reject with a peer ban.
    regtest) printf -- '-regtest\n-regtestacceptunvalidatedpow\n' ;;
  esac
}

p2p_port_from_addr() {
  printf '%s\n' "${1##*:}"
}

# zcashd dials Zakura on loopback regardless of the interface Zakura binds.
zcashd_connect_addr() {
  printf '127.0.0.1:%s\n' "$(p2p_port_from_addr "$ZAKURA_P2P_ADDR")"
}

# The P2P pinning flags. zcashd peers *only* with the local Zakura node:
# -connect selects the single outbound peer, and the rest disable inbound
# listening, DNS seeding, onion, and discovery. Defense in depth against
# operator zcash.conf values.
zcashd_p2p_pinning_args() {
  printf -- '-connect=%s\n-listen=0\n-dnsseed=0\n-listenonion=0\n-discover=0\n' "$(zcashd_connect_addr)"
}

path_capacity_bytes() {
  local path="$1"
  local ancestor size

  command_exists df || return 1
  command_exists awk || return 1

  ancestor="$(nearest_existing_ancestor "$path")" || return 1
  size="$(df -PB1 "$ancestor" 2>/dev/null | awk 'NR == 2 { gsub(/B$/, "", $2); print $2 }')"

  [[ "$size" =~ ^[0-9]+$ ]] || return 1
  printf '%s\n' "$size"
}

path_has_min_capacity() {
  local path="$1"
  local min_bytes="$2"
  local size

  size="$(path_capacity_bytes "$path")" || return 1
  ((size >= min_bytes))
}

zebra_state_has_expected_files() {
  local dir="$1"
  local net_dir matches match

  [[ -d "$dir" ]] || return 1
  net_dir="$(network_name_lowercase)"

  matches=("$dir"/state/v*/"$net_dir"/version "$dir"/state/v*/"$net_dir"/CURRENT "$dir"/state/v*/"$net_dir"/MANIFEST-*)
  for match in "${matches[@]}"; do
    if [[ -e "$match" ]]; then
      return 0
    fi
  done

  return 1
}

zcashd_datadir_has_expected_files() {
  local datadir="$1"
  local effective_datadir

  [[ -d "$datadir" ]] || return 1

  for effective_datadir in "$datadir" "$(zcashd_network_datadir "$datadir")"; do
    [[ -f "$effective_datadir/zcash.conf" ]] && return 0
    [[ -d "$effective_datadir/blocks" ]] && return 0
    [[ -d "$effective_datadir/blocks/index" ]] && return 0
    [[ -d "$effective_datadir/chainstate" ]] && return 0
  done

  return 1
}

candidate_search_roots() {
  local root seen
  local roots=("$HOME" "${XDG_CACHE_HOME:-$HOME/.cache}" /mnt /media /srv /var/lib /data)
  seen=""

  if command_exists df && command_exists awk; then
    while IFS= read -r root; do
      roots+=("$root")
    done < <(df -P 2>/dev/null | awk 'NR > 1 { print $6 }')
  fi

  for root in "${roots[@]}"; do
    [[ -n "$root" ]] || continue
    case $'\n'"$seen" in
      *$'\n'"$root"$'\n'*) continue ;;
    esac
    seen+="$root"$'\n'
    printf '%s\n' "$root"
  done
}

check_candidate_datadir() {
  local candidate="$1"
  local min_bytes="$2"
  local expected_files_check="$3"

  [[ -d "$candidate" ]] || return 1
  path_has_min_capacity "$candidate" "$min_bytes" || return 1
  "$expected_files_check" "$candidate"
}

candidate_size_kib() {
  local candidate="$1"

  if command_exists du && command_exists awk; then
    du -sk "$candidate" 2>/dev/null | awk 'NR == 1 { print $1 }'
  else
    printf '0\n'
  fi
}

candidate_score() {
  local candidate="$1"
  local size_kib

  size_kib="$(candidate_size_kib "$candidate")"
  if [[ "$size_kib" =~ ^[0-9]+$ ]]; then
    printf '%s\n' "$size_kib"
  else
    printf '0\n'
  fi
}

maybe_select_better_candidate() {
  local candidate="$1"
  local min_bytes="$2"
  local expected_files_check="$3"
  local score

  check_candidate_datadir "$candidate" "$min_bytes" "$expected_files_check" || return 0
  score="$(candidate_score "$candidate")"

  if [[ -z "${BEST_CANDIDATE:-}" || "$score" -gt "${BEST_CANDIDATE_SCORE:-0}" ]]; then
    BEST_CANDIDATE="$candidate"
    BEST_CANDIDATE_SCORE="$score"
  fi
}

maybe_select_better_install_root() {
  local root="$1"
  local min_bytes="$2"
  local score

  [[ -n "$root" && "$root" != "/" && -d "$root" ]] || return 0
  score="$(path_capacity_bytes "$root" 2>/dev/null || printf '0')"
  [[ "$score" =~ ^[0-9]+$ ]] || return 0
  ((score >= min_bytes)) || return 0

  if [[ -z "${BEST_INSTALL_ROOT:-}" || "$score" -gt "${BEST_INSTALL_ROOT_SCORE:-0}" ]]; then
    BEST_INSTALL_ROOT="$root"
    BEST_INSTALL_ROOT_SCORE="$score"
  fi
}

recommend_install_root() {
  local min_bytes="$1"
  local root
  BEST_INSTALL_ROOT=""
  BEST_INSTALL_ROOT_SCORE=0

  while IFS= read -r root; do
    maybe_select_better_install_root "$root" "$min_bytes"
  done < <(candidate_search_roots)

  if [[ -n "$BEST_INSTALL_ROOT" ]]; then
    printf '%s\n' "$BEST_INSTALL_ROOT"
    return 0
  fi

  return 1
}

search_named_candidates() {
  local min_bytes="$1"
  local expected_files_check="$2"
  shift 2

  local root name candidate
  while IFS= read -r root; do
    [[ -n "$root" && "$root" != "/" && -d "$root" ]] || continue

    for name in "$@"; do
      candidate="$root/$name"
      maybe_select_better_candidate "$candidate" "$min_bytes" "$expected_files_check"
    done
  done < <(candidate_search_roots)
}

search_zebra_state_candidates() {
  local min_bytes="$1"
  local root candidate

  search_named_candidates "$min_bytes" zebra_state_has_expected_files \
    ".cache/zakura" "zakura" "zakura-state" "data/zakura" "data/zakura-state" \
    "mnt/data/zakura" "mnt/data/zakura-state"

  command_exists find || return 0
  while IFS= read -r root; do
    [[ -n "$root" && "$root" != "/" && -d "$root" ]] || continue
    path_has_min_capacity "$root" "$min_bytes" || continue

    while IFS= read -r candidate; do
      maybe_select_better_candidate "$candidate" "$min_bytes" zebra_state_has_expected_files
    done < <(find "$root" -xdev -maxdepth 5 -type d \( -name zakura -o -name zakura-state \) -print 2>/dev/null)
  done < <(candidate_search_roots)
}

search_zcashd_datadir_candidates() {
  local min_bytes="$1"
  local root candidate

  search_named_candidates "$min_bytes" zcashd_datadir_has_expected_files \
    ".zcash" "zcash" "zcashd" "zcashd-mainnet" "data/.zcash" "data/zcashd" "data/zcashd-mainnet" \
    "mnt/data/.zcash" "mnt/data/zcashd" "mnt/data/zcashd-mainnet"

  command_exists find || return 0
  while IFS= read -r root; do
    [[ -n "$root" && "$root" != "/" && -d "$root" ]] || continue
    path_has_min_capacity "$root" "$min_bytes" || continue

    while IFS= read -r candidate; do
      maybe_select_better_candidate "$candidate" "$min_bytes" zcashd_datadir_has_expected_files
    done < <(find "$root" -xdev -maxdepth 5 -type d \( -name .zcash -o -name zcash -o -name zcashd -o -name zcashd-mainnet \) -print 2>/dev/null)
  done < <(candidate_search_roots)
}

recommend_zebra_state_dir() {
  local binary_default="$1"
  local min_bytes=$((300 * 1024 * 1024 * 1024))
  local synthetic_min_bytes="${SYNTHETIC_INSTALL_MIN_BYTES:-$min_bytes}"
  local install_root
  BEST_CANDIDATE=""
  BEST_CANDIDATE_SCORE=0

  maybe_select_better_candidate "$binary_default" "$min_bytes" zebra_state_has_expected_files
  search_zebra_state_candidates "$min_bytes"

  if [[ -n "$BEST_CANDIDATE" ]]; then
    printf '%s\n' "$BEST_CANDIDATE"
    return
  fi

  if ! path_has_min_capacity "$binary_default" "$min_bytes"; then
    if install_root="$(recommend_install_root "$synthetic_min_bytes")"; then
      printf '%s/.cache/zakura\n' "$install_root"
      return
    fi
  fi

  printf '%s\n' "$binary_default"
}

recommend_zcashd_datadir() {
  local binary_default="$1"
  local min_bytes=$((300 * 1024 * 1024 * 1024))
  local synthetic_min_bytes="${SYNTHETIC_INSTALL_MIN_BYTES:-$min_bytes}"
  local install_root
  BEST_CANDIDATE=""
  BEST_CANDIDATE_SCORE=0

  maybe_select_better_candidate "$binary_default" "$min_bytes" zcashd_datadir_has_expected_files
  search_zcashd_datadir_candidates "$min_bytes"

  if [[ -n "$BEST_CANDIDATE" ]]; then
    printf '%s\n' "$BEST_CANDIDATE"
    return
  fi

  if ! path_has_min_capacity "$binary_default" "$min_bytes"; then
    if install_root="$(recommend_install_root "$synthetic_min_bytes")"; then
      printf '%s/.zcash\n' "$install_root"
      return
    fi
  fi

  printf '%s\n' "$binary_default"
}

recommend_datadir_defaults() {
  # Empty fallback locations share a filesystem, so size them for both datadirs
  # when both prompt defaults are being selected together.
  if ((ZAKURA_STATE_DIR_SET == 0 && ZCASHD_DATADIR_SET == 0)); then
    SYNTHETIC_INSTALL_MIN_BYTES=$((550 * 1024 * 1024 * 1024))
  else
    SYNTHETIC_INSTALL_MIN_BYTES=$((300 * 1024 * 1024 * 1024))
  fi

  if ((ZAKURA_STATE_DIR_SET == 0)); then
    ZAKURA_STATE_DIR="$(recommend_zebra_state_dir "$ZAKURA_DEFAULT_CACHE_DIR")"
  fi

  if ((ZCASHD_DATADIR_SET == 0)); then
    ZCASHD_DATADIR="$(recommend_zcashd_datadir "$ZCASHD_DEFAULT_DATADIR")"
  fi

  unset SYNTHETIC_INSTALL_MIN_BYTES
}

prompt_mode() {
  local reply

  if [[ -n "$MODE" ]]; then
    return
  fi

  if ((NON_INTERACTIVE)); then
    add_error "--mode is required with --non-interactive"
    return
  fi

  if ((USE_ANSI)); then
    printf '\n%s\n' "$(style "$BOLD" "Choose a zcashd-compat mode:")"
    printf '  %b1)%b %bsplit-binary%b\n' "$CYAN$BOLD" "$RESET" "$GREEN$BOLD" "$RESET"
    printf '     %bStart Zakura and zcashd as two separate processes.%b\n' "$DIM" "$RESET"
    printf '  %b2)%b %bsupervised%b\n' "$CYAN$BOLD" "$RESET" "$GREEN$BOLD" "$RESET"
    printf '     %bStart Zakura, which downloads hash-pinned zcashd and spins it up as a supervised child process.%b\n' "$DIM" "$RESET"
    printf '  %b3)%b %bdocker-split-containers%b\n' "$CYAN$BOLD" "$RESET" "$GREEN$BOLD" "$RESET"
    printf '     %bsplit-binary, but in Docker.%b\n' "$DIM" "$RESET"
    printf '  %b4)%b %bdocker-supervised%b\n' "$CYAN$BOLD" "$RESET" "$GREEN$BOLD" "$RESET"
    printf '     %bsupervised, but in Docker.%b\n' "$DIM" "$RESET"
    printf '  %b5)%b %bbuild-from-source%b\n' "$CYAN$BOLD" "$RESET" "$GREEN$BOLD" "$RESET"
    printf '     %bBuild everything yourself; the installer provides links and health checks.%b\n' "$DIM" "$RESET"
  else
    cat <<'EOF'
Choose a zcashd-compat mode:
  1) split-binary
     Start Zakura and zcashd as two separate processes.
  2) supervised
     Start Zakura, which downloads hash-pinned zcashd and spins it up as a supervised child process.
  3) docker-split-containers
     split-binary, but in Docker.
  4) docker-supervised
     supervised, but in Docker.
  5) build-from-source
     Build everything yourself; the installer provides links and health checks.
EOF
  fi
  printf '\n'
  read_prompt "Mode [split-binary]: " reply || reply=""
  case "${reply:-split-binary}" in
    1 | split-binary) MODE="split-binary" ;;
    2 | supervised) MODE="supervised" ;;
    3 | docker-split-containers) MODE="docker-split-containers" ;;
    4 | docker-supervised) MODE="docker-supervised" ;;
    5 | build-from-source) MODE="build-from-source" ;;
    *) MODE="$reply" ;;
  esac
}

normalize_inputs() {
  prompt_mode

  if [[ "$MODE" == "split-binary" || "$MODE" == "supervised" ]]; then
    if ((DOWNLOAD_BINARIES_SET == 0)); then
      case "$(prompt_yes_no "Download Zakura/zcashd release binaries now?" "yes")" in
        yes | y | Y | YES | Yes) DOWNLOAD_BINARIES=1 ;;
        no | n | N | NO | No) DOWNLOAD_BINARIES=0 ;;
        *) add_error "binary download answer must be yes or no" ;;
      esac
    fi
  fi

  recommend_datadir_defaults

  ZAKURA_STATE_DIR="$(prompt_value "Zakura state directory" "$ZAKURA_STATE_DIR")"
  ZCASHD_DATADIR="$(prompt_value "zcashd datadir" "$ZCASHD_DATADIR")"
  INSTALL_DIR="$(prompt_value "Install directory" "$INSTALL_DIR")"

  if [[ -z "$ZCASHD_CONF" ]]; then
    ZCASHD_CONF="$ZCASHD_DATADIR/zcash.conf"
  fi

  ZAKURA_STATE_DIR="$(printf '%s' "$ZAKURA_STATE_DIR" | sanitize_terminal_input)"
  ZCASHD_DATADIR="$(printf '%s' "$ZCASHD_DATADIR" | sanitize_terminal_input)"
  INSTALL_DIR="$(printf '%s' "$INSTALL_DIR" | sanitize_terminal_input)"
  CACHE_DIR="$(printf '%s' "$CACHE_DIR" | sanitize_terminal_input)"
  if [[ -z "$COOKIE_DIR" ]]; then
    COOKIE_DIR="$ZAKURA_STATE_DIR"
  fi
  COOKIE_DIR="$(printf '%s' "$COOKIE_DIR" | sanitize_terminal_input)"
  ZCASHD_CONF="$(printf '%s' "$ZCASHD_CONF" | sanitize_terminal_input)"
  ZAKURAD_PATH="$(printf '%s' "$ZAKURAD_PATH" | sanitize_terminal_input)"
  ZCASHD_PATH="$(printf '%s' "$ZCASHD_PATH" | sanitize_terminal_input)"

  ZAKURA_STATE_DIR="$(abs_path "$ZAKURA_STATE_DIR")"
  ZCASHD_DATADIR="$(abs_path "$ZCASHD_DATADIR")"
  INSTALL_DIR="$(abs_path "$INSTALL_DIR")"
  CACHE_DIR="$(abs_path "$CACHE_DIR")"
  COOKIE_DIR="$(abs_path "$COOKIE_DIR")"
  ZCASHD_CONF="$(abs_path "$ZCASHD_CONF")"

  case "$MODE" in
    split-binary | supervised | docker-split-containers | docker-supervised | build-from-source) ;;
    "") add_error "mode is required" ;;
    *) add_error "unsupported mode: $MODE" ;;
  esac

  case "$(network_name_lowercase)" in
    mainnet | testnet | regtest) ;;
    *) add_error "unsupported network: $NETWORK (expected Mainnet, Testnet, or Regtest)" ;;
  esac

  if [[ -z "$ZAKURA_P2P_ADDR" ]]; then
    ZAKURA_P2P_ADDR="[::]:$(network_default_p2p_port)"
  fi
  ZAKURA_P2P_ADDR="$(printf '%s' "$ZAKURA_P2P_ADDR" | sanitize_terminal_input)"
  COMPAT_LISTEN_ADDR="$(printf '%s' "$COMPAT_LISTEN_ADDR" | sanitize_terminal_input)"

  if [[ "$ZAKURA_P2P_ADDR" != *:* || -z "$(p2p_port_from_addr "$ZAKURA_P2P_ADDR")" ]]; then
    add_error "--zakura-p2p-addr must be HOST:PORT, got: $ZAKURA_P2P_ADDR"
  fi
}

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

collect_tool_checks() {
  local tools=()

  case "$MODE" in
    split-binary | supervised)
      tools=(curl install tar sha256sum python3)
      ;;
    docker-split-containers | docker-supervised)
      tools=(docker)
      ;;
    build-from-source)
      tools=(cargo make)
      ;;
  esac

  local tool
  for tool in "${tools[@]}"; do
    if ! command_exists "$tool"; then
      add_error "required tool is missing from PATH: $tool"
    fi
  done

  for tool in awk df find getconf stat; do
    if ! command_exists "$tool"; then
      add_error "required preflight tool is missing from PATH: $tool"
    fi
  done
}

nearest_existing_ancestor() {
  local path="$1"
  local current="$path"

  while [[ ! -e "$current" ]]; do
    local parent
    parent="$(dirname "$current")"
    if [[ "$parent" == "$current" ]]; then
      return 1
    fi
    current="$parent"
  done

  printf '%s\n' "$current"
}

check_writable_target() {
  local label="$1"
  local target="$2"
  local ancestor

  if ! ancestor="$(nearest_existing_ancestor "$target")"; then
    add_error "no existing ancestor path found for $target"
    return
  fi

  if [[ ! -d "$ancestor" ]]; then
    add_error "$label path $target requires a directory at $ancestor, which exists but is not a directory"
    return
  fi

  if [[ ! -w "$ancestor" || ! -x "$ancestor" ]]; then
    if [[ "$target" == "$ancestor" ]]; then
      add_error "$label path $target is not writable by the current user"
    else
      add_error "$label path $target cannot be created: nearest existing ancestor $ancestor is not writable by the current user"
    fi
  fi
}

collect_permission_checks() {
  check_writable_target "zakura state directory" "$ZAKURA_STATE_DIR"
  check_writable_target "zcashd datadir" "$ZCASHD_DATADIR"
  check_writable_target "install directory" "$INSTALL_DIR"
  check_writable_target "download/cache directory" "$CACHE_DIR"
  check_writable_target "rpc cookie directory" "$COOKIE_DIR"

  if [[ -e "$ZCASHD_CONF" || -L "$ZCASHD_CONF" ]]; then
    if [[ -L "$ZCASHD_CONF" && ! -e "$ZCASHD_CONF" ]]; then
      add_error "zcashd config $ZCASHD_CONF is a symlink to a missing target"
    elif [[ -d "$ZCASHD_CONF" ]]; then
      add_error "zcashd config path $ZCASHD_CONF is a directory, expected a file"
    elif [[ ! -r "$ZCASHD_CONF" ]]; then
      add_error "zcashd config $ZCASHD_CONF exists but is not readable by the current user"
    fi
  else
    check_writable_target "zcashd config directory" "$(dirname "$ZCASHD_CONF")"
  fi
}

human_gib() {
  local bytes="$1"
  awk -v bytes="$bytes" 'BEGIN { printf "%.1f GiB", bytes / 1024 / 1024 / 1024 }'
}

collect_platform_checks() {
  if [[ "$(uname -s)" != "Linux" ]]; then
    add_low_spec_error "zcashd-compat mode is supported on Linux only"
  fi
}

collect_cpu_checks() {
  local cpu_count
  cpu_count="$(getconf _NPROCESSORS_ONLN 2>/dev/null || printf '0')"

  if ((cpu_count < 4)); then
    add_low_spec_error "detected ${cpu_count} logical CPUs, minimum required is 4"
  elif ((cpu_count < 8)); then
    add_warning "detected ${cpu_count} logical CPUs, recommended is 8"
  fi
}

meminfo_total_bytes() {
  awk '/^MemTotal:/ { print $2 * 1024; exit }' /proc/meminfo
}

cgroup_limit_value() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    return 0
  fi

  local value
  value="$(<"$path")"
  value="${value//$'\n'/}"

  if [[ "$value" == "max" || -z "$value" ]]; then
    return 0
  fi

  if [[ "$value" =~ ^[0-9]+$ && "$value" -lt 9223372036854771712 ]]; then
    printf '%s\n' "$value"
  fi
}

effective_memory_bytes() {
  local mem_total limit best_limit
  mem_total="$(meminfo_total_bytes)"

  while IFS=: read -r _ controllers relpath; do
    if [[ "$controllers" == "" ]]; then
      limit="$(cgroup_limit_value "/sys/fs/cgroup/${relpath#/}/memory.max")"
      if [[ -z "$limit" ]]; then
        limit="$(cgroup_limit_value "/sys/fs/cgroup/memory.max")"
      fi
    elif [[ ",$controllers," == *",memory,"* ]]; then
      limit="$(cgroup_limit_value "/sys/fs/cgroup/memory/${relpath#/}/memory.limit_in_bytes")"
      if [[ -z "$limit" ]]; then
        limit="$(cgroup_limit_value "/sys/fs/cgroup/memory/memory.limit_in_bytes")"
      fi
    else
      limit=""
    fi

    if [[ -n "${limit:-}" && ( -z "${best_limit:-}" || "$limit" -lt "$best_limit" ) ]]; then
      best_limit="$limit"
    fi
  done < /proc/self/cgroup

  if [[ -n "${best_limit:-}" && "$best_limit" -lt "$mem_total" ]]; then
    printf '%s\n' "$best_limit"
  else
    printf '%s\n' "$mem_total"
  fi
}

collect_memory_checks() {
  local memory min recommended
  min=$((16 * 1024 * 1024 * 1024))
  recommended=$((32 * 1024 * 1024 * 1024))
  memory="$(effective_memory_bytes)"

  if ((memory < min)); then
    add_low_spec_error "detected effective memory $(human_gib "$memory"), minimum required is $(human_gib "$min")"
  elif ((memory < recommended)); then
    add_warning "detected effective memory $(human_gib "$memory"), recommended is $(human_gib "$recommended")"
  fi
}

disk_device_and_size() {
  local path="$1"
  local ancestor device size
  ancestor="$(nearest_existing_ancestor "$path")" || return 1
  device="$(stat -c '%d' "$ancestor")"
  size="$(df -PB1 "$ancestor" | awk 'NR == 2 { gsub(/B$/, "", $2); print $2 }')"
  printf '%s %s\n' "$device" "$size"
}

collect_disk_checks() {
  local zebra_info zcashd_info zebra_device zebra_size zcashd_device zcashd_size
  local gib tib required combined recommended

  gib=$((1024 * 1024 * 1024))
  tib=$((1024 * gib))
  recommended="$tib"

  if ! zebra_info="$(disk_device_and_size "$ZAKURA_STATE_DIR")"; then
    add_error "failed to inspect filesystem for zakura state path: $ZAKURA_STATE_DIR"
    return
  fi

  if ! zcashd_info="$(disk_device_and_size "$ZCASHD_DATADIR")"; then
    add_error "failed to inspect filesystem for zcashd datadir path: $ZCASHD_DATADIR"
    return
  fi

  read -r zebra_device zebra_size <<<"$zebra_info"
  read -r zcashd_device zcashd_size <<<"$zcashd_info"

  if [[ "$zebra_device" == "$zcashd_device" ]]; then
    required=$((550 * gib))
    combined="$zebra_size"
    if ((zebra_size < required)); then
      add_low_spec_error "zakura state + zcashd datadir mount (paths: $ZAKURA_STATE_DIR, $ZCASHD_DATADIR) has provisioned capacity $(human_gib "$zebra_size"), minimum required is $(human_gib "$required")"
    fi
  else
    required=$((300 * gib))
    combined=$((zebra_size + zcashd_size))
    if ((zebra_size < required)); then
      add_low_spec_error "zakura state mount (paths: $ZAKURA_STATE_DIR) has provisioned capacity $(human_gib "$zebra_size"), minimum required is $(human_gib "$required")"
    fi
    if ((zcashd_size < required)); then
      add_low_spec_error "zcashd datadir mount (paths: $ZCASHD_DATADIR) has provisioned capacity $(human_gib "$zcashd_size"), minimum required is $(human_gib "$required")"
    fi
  fi

  if ((combined < recommended)); then
    add_warning "combined zcashd-compat filesystem capacity is $(human_gib "$combined"), recommended is $(human_gib "$recommended")"
  fi
}

# The zcashd repo is `valargroup/zcashd`, so a plain `git clone` lands in
# `zcashd/`. Older instructions cloned it as `zcash/`. Accept either.
resolve_zcash_src_dir() {
  local candidate
  for candidate in "$UNITY_ROOT/zcashd" "$UNITY_ROOT/zcash"; do
    if [[ -x "$candidate/zcutil/build.sh" ]]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done
  for candidate in "$UNITY_ROOT/zcashd" "$UNITY_ROOT/zcash"; do
    if [[ -d "$candidate" ]]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done
  printf '%s\n' "$UNITY_ROOT/zcashd"
  return 1
}

collect_source_checks() {
  if [[ "$MODE" != "build-from-source" ]]; then
    return
  fi

  if [[ ! -d "$REPO_ROOT" ]]; then
    add_error "Zakura source tree is missing: $REPO_ROOT"
  elif [[ ! -f "$REPO_ROOT/Cargo.toml" ]]; then
    add_error "Zakura source tree is missing Cargo.toml: $REPO_ROOT"
  fi

  ZCASH_SRC_DIR="$(resolve_zcash_src_dir)" || true

  if [[ ! -d "$ZCASH_SRC_DIR" ]]; then
    add_error "zcashd source tree is missing: expected $UNITY_ROOT/zcashd or $UNITY_ROOT/zcash"
  elif [[ ! -x "$ZCASH_SRC_DIR/zcutil/build.sh" ]]; then
    add_error "zcashd build script is missing or not executable: $ZCASH_SRC_DIR/zcutil/build.sh"
  fi

  ZAKURAD_PATH="${ZAKURAD_PATH:-$REPO_ROOT/target/release/zakurad}"
  ZCASHD_PATH="${ZCASHD_PATH:-$ZCASH_SRC_DIR/src/zcashd}"

  if [[ -e "$ZAKURAD_PATH" && ! -x "$ZAKURAD_PATH" ]]; then
    add_error "zakurad binary $ZAKURAD_PATH exists but is not executable by the current user"
  fi

  if [[ -e "$ZCASHD_PATH" && ! -x "$ZCASHD_PATH" ]]; then
    add_error "zcashd binary $ZCASHD_PATH exists but is not executable by the current user"
  fi
}

collect_checks() {
  collect_platform_checks
  collect_tool_checks
  collect_permission_checks
  collect_cpu_checks
  collect_memory_checks
  collect_disk_checks
  collect_source_checks
}

manifest_field() {
  local field="$1"

  if [[ ! -f "$MANIFEST_PATH" ]]; then
    case "$field" in
      runtime_archive_url) printf '%s\n' "$ZCASHD_RUNTIME_ARCHIVE_URL" ;;
      runtime_archive_sha256) printf '%s\n' "$ZCASHD_RUNTIME_ARCHIVE_SHA256" ;;
      runtime_archive_member_binary_path) printf '%s\n' "$ZCASHD_RUNTIME_ARCHIVE_MEMBER_BINARY_PATH" ;;
      *) return 1 ;;
    esac
    return
  fi

  FIELD="$field" TARGET_TRIPLE="$TARGET_TRIPLE" MANIFEST_PATH="$MANIFEST_PATH" python3 - <<'PY'
import json
import os
from pathlib import Path

manifest = json.loads(Path(os.environ["MANIFEST_PATH"]).read_text(encoding="utf-8"))
target = os.environ["TARGET_TRIPLE"]
field = os.environ["FIELD"]

for artifact in manifest["artifacts"]:
    if artifact["target_triple"] == target:
        print(artifact[field])
        raise SystemExit(0)

raise SystemExit(f"missing target triple in zcashd manifest: {target}")
PY
}

download_and_extract() {
  local name="$1"
  local url="$2"
  local sha256="$3"
  local member="$4"
  local archive_name="$5"
  local destination="$6"
  local archive_path extract_dir source_path

  archive_path="$CACHE_DIR/$archive_name"
  extract_dir="$CACHE_DIR/${archive_name%.tar.gz}"

  if ((DRY_RUN)); then
    if ((USE_ANSI)); then
      printf '%s %s\n' "$(style "$CYAN" "[down]")" "$(style "$DIM" "Dry run: would download $name from $url")"
      printf '%s %s\n' "$(style "$CYAN" "[file]")" "$(style "$DIM" "Dry run: would extract $member to $destination")"
    else
      printf 'Dry run: would download %s from %s\n' "$name" "$url"
      printf 'Dry run: would extract %s to %s\n' "$member" "$destination"
    fi
    return
  fi

  mkdir -p "$CACHE_DIR" "$extract_dir" "$(dirname "$destination")"
  if ((USE_ANSI)); then
    printf '%s Downloading %s from %s\n' "$(style "$CYAN" "[down]")" "$(style "$BOLD" "$name")" "$url"
  else
    printf 'Downloading %s from %s\n' "$name" "$url"
  fi
  curl -fsSL "$url" -o "$archive_path"

  if [[ -n "$sha256" ]]; then
    printf '%s  %s\n' "$sha256" "$archive_path" | sha256sum -c -
  fi

  rm -rf "$extract_dir"
  mkdir -p "$extract_dir"
  tar -xzf "$archive_path" -C "$extract_dir"

  source_path="$extract_dir/${member#./}"
  if [[ ! -x "$source_path" ]]; then
    add_error "expected executable missing from $name archive: $member"
    finalize_checks
  fi

  install -D -m 0755 "$source_path" "$destination"
}

prepare_binary_paths() {
  local zcashd_url zcashd_sha zcashd_member

  ZAKURAD_PATH="${ZAKURAD_PATH:-$INSTALL_DIR/zakura/bin/zakurad}"

  if [[ "$MODE" == "split-binary" ]]; then
    ZCASHD_PATH="${ZCASHD_PATH:-$INSTALL_DIR/zcashd/bin/zcashd}"

    zcashd_url="$(manifest_field runtime_archive_url)"
    zcashd_sha="$(manifest_field runtime_archive_sha256)"
    zcashd_member="$(manifest_field runtime_archive_member_binary_path)"
  fi

  if ((DOWNLOAD_BINARIES == 0)); then
    if ((USE_ANSI)); then
      printf '%s Skipping binary downloads. You must provision the right Zakura and zcashd versions yourself.\n' "$(style "$YELLOW" "[!]")"
    else
      printf 'Skipping binary downloads. You must provision the right Zakura and zcashd versions yourself.\n'
    fi
    printf '\nDownload Zakura:\n%s\n' "$ZAKURA_URL"
    if [[ "$MODE" == "split-binary" ]]; then
      printf '\nDownload zcashd:\n%s\n' "$zcashd_url"
    else
      printf '\nZakura supervised mode will use its hash-pinned managed zcashd download at startup.\n'
    fi
    printf '\n'
    if ((!DRY_RUN)); then
      [[ -x "$ZAKURAD_PATH" ]] || add_error "zakurad binary $ZAKURAD_PATH does not exist or is not executable by the current user"
      if [[ "$MODE" == "split-binary" ]]; then
        [[ -x "$ZCASHD_PATH" ]] || add_error "zcashd binary $ZCASHD_PATH does not exist or is not executable by the current user"
      fi
      finalize_checks
    fi
    return
  fi

  download_and_extract "zakurad" "$ZAKURA_URL" "$ZAKURA_ARCHIVE_SHA256" "$ZAKURA_MEMBER" "$ZAKURA_ARCHIVE" "$ZAKURAD_PATH"

  if [[ "$MODE" == "split-binary" ]]; then
    download_and_extract "zcashd" "$zcashd_url" "$zcashd_sha" "$zcashd_member" "$(basename "$zcashd_url")" "$ZCASHD_PATH"
  fi

  if ((!DRY_RUN)); then
    [[ -x "$ZAKURAD_PATH" ]] || add_error "zakurad binary $ZAKURAD_PATH does not exist or is not executable by the current user"
    if [[ "$MODE" == "split-binary" ]]; then
      [[ -x "$ZCASHD_PATH" ]] || add_error "zcashd binary $ZCASHD_PATH does not exist or is not executable by the current user"
    fi
    finalize_checks
  fi
}

# zcashd refuses to start unless its config file exists ("Before starting
# zcashd, you need to create a configuration file"). A fresh or snapshot-restored
# datadir has none, so bootstrap a minimal one rather than printing a command
# that cannot run. Never overwrite an existing file.
ensure_zcashd_conf() {
  # split-binary/build-from-source run zcashd directly; docker-split-containers
  # runs a standalone zcashd container (nobody bootstraps its conf, unlike
  # docker-supervised where the in-container Zakura node creates it). All three
  # need a zcash.conf to exist, or zcashd refuses to start.
  case "$MODE" in
    split-binary | build-from-source | docker-split-containers) ;;
    *) return ;;
  esac

  if [[ -e "$ZCASHD_CONF" ]]; then
    return
  fi

  if ((DRY_RUN)); then
    if ((USE_ANSI)); then
      printf '%s %s\n' "$(style "$CYAN" "[file]")" "$(style "$DIM" "Dry run: would create minimal zcashd config at $ZCASHD_CONF")"
    else
      printf 'Dry run: would create minimal zcashd config at %s\n' "$ZCASHD_CONF"
    fi
    return
  fi

  if ! mkdir -p "$(dirname "$ZCASHD_CONF")"; then
    add_error "failed to create directory for zcashd config: $(dirname "$ZCASHD_CONF")"
    return
  fi

  {
    printf '# Created by install-zcashd-compat.sh for zcashd-compat P2P sidecar mode.\n'
    printf '# Peer selection is pinned on the zcashd command line; do not add\n'
    printf '# connect=/addnode=/seednode= here -- they accumulate and cannot be overridden.\n'
    printf 'i-am-aware-zcashd-will-be-replaced-by-zebrad-and-zallet-in-2025=1\n'
  } >"$ZCASHD_CONF" || add_error "failed to write zcashd config: $ZCASHD_CONF"

  if ((USE_ANSI)); then
    printf '%s Created minimal zcashd config at %s\n' "$(style "$GREEN" "[ok]")" "$ZCASHD_CONF"
  else
    printf 'Created minimal zcashd config at %s\n' "$ZCASHD_CONF"
  fi
}

data_detection_message() {
  if ((USE_ANSI)); then
    print_section "[*]" "Snapshot data"
  fi

  if [[ -d "$ZAKURA_STATE_DIR" && -n "$(find "$ZAKURA_STATE_DIR" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]] ||
     [[ -d "$ZCASHD_DATADIR" && -n "$(find "$ZCASHD_DATADIR" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]]; then
    if ((USE_ANSI)); then
      printf '%s You already have data configured but feel free to redownload a fresh snapshot\n' "$(style "$GREEN" "[ok]")"
    else
      printf 'You already have data configured but feel free to redownload a fresh snapshot\n'
    fi
  else
    if ((USE_ANSI)); then
      printf '%s Please download the snapshot from the locations\n' "$(style "$CYAN" "[down]")"
    else
      printf 'Please download the snapshot from the locations\n'
    fi
  fi
  printf '\nhttps://zcashd.valargroup.org/\n'
  printf '\nhttps://zakura.valargroup.dev/\n'
  printf '\n'
}

docker_image_available_or_pull() {
  local image="$1"

  if ((DRY_RUN)); then
    if ((USE_ANSI)); then
      printf '%s %s\n' "$(style "$CYAN" "[down]")" "$(style "$DIM" "Dry run: would inspect or pull Docker image $image")"
    else
      printf 'Dry run: would inspect or pull Docker image %s\n' "$image"
    fi
    return 0
  fi

  if docker image inspect "$image" >/dev/null 2>&1; then
    return 0
  fi

  docker pull "$image" >/dev/null 2>&1
}

run_privileged_or_current_user() {
  if ((EUID == 0)); then
    "$@"
    return
  fi

  if command_exists sudo && sudo -n true 2>/dev/null; then
    sudo "$@"
    return
  fi

  "$@"
}

prepare_docker_owned_directory() {
  local label="$1"
  local dir="$2"
  local owner="${ZAKURA_DOCKER_RUNTIME_UID}:${ZAKURA_DOCKER_RUNTIME_GID}"

  if ((DRY_RUN)); then
    if ((USE_ANSI)); then
      printf '%s %s\n' "$(style "$CYAN" "[file]")" "$(style "$DIM" "Dry run: would create $label at $dir and chown it to $owner")"
    else
      printf 'Dry run: would create %s at %s and chown it to %s\n' "$label" "$dir" "$owner"
    fi
    return
  fi

  if ! mkdir -p "$dir"; then
    add_error "failed to create $label for Docker mount: $dir"
    return
  fi

  if ! run_privileged_or_current_user chown -R "$owner" "$dir"; then
    add_error "failed to chown $label $dir to Docker runtime user $owner; run: sudo chown -R $owner $(shell_quote "$dir")"
  fi
}

# Both Docker modes bind-mount these directories into containers that run as
# ZAKURA_DOCKER_RUNTIME_UID:GID, so both need the ownership fixed up -- not just
# docker-supervised.
prepare_docker_mounts() {
  case "$MODE" in
    docker-supervised | docker-split-containers) ;;
    *) return ;;
  esac

  prepare_docker_owned_directory "Zakura state directory" "$ZAKURA_STATE_DIR"
  prepare_docker_owned_directory "zcashd datadir" "$ZCASHD_DATADIR"
  finalize_checks
}

prepare_docker_images() {
  case "$MODE" in
    docker-supervised)
      if docker_image_available_or_pull "$ZAKURA_COMPAT_DOCKER_IMAGE"; then
        ZAKURA_COMPAT_DOCKER_SELECTED="$ZAKURA_COMPAT_DOCKER_IMAGE"
      elif docker_image_available_or_pull "$ZAKURA_COMPAT_DOCKER_FALLBACK_IMAGE"; then
        ZAKURA_COMPAT_DOCKER_SELECTED="$ZAKURA_COMPAT_DOCKER_FALLBACK_IMAGE"
      else
        add_error "Docker image is missing or could not be pulled: $ZAKURA_COMPAT_DOCKER_IMAGE; fallback also failed: $ZAKURA_COMPAT_DOCKER_FALLBACK_IMAGE"
      fi
      ;;
    docker-split-containers)
      docker_image_available_or_pull "$ZAKURA_DOCKER_IMAGE" ||
        add_error "Docker image is missing or could not be pulled: $ZAKURA_DOCKER_IMAGE"

      if [[ -z "$ZCASHD_DOCKER_IMAGE" ]]; then
        ZCASHD_DOCKER_IMAGE="valargroup/zcashd:v0.0.1-compat-alpha.3@sha256:d9c80e8469f99406cc7e51238ae67708421d739a7446f52f941a7ea44b3af354"
      fi

      docker_image_available_or_pull "$ZCASHD_DOCKER_IMAGE" ||
        add_error "docker-split-containers requires a zcashd Docker image; attempted $ZCASHD_DOCKER_IMAGE but it was not present or pullable. Pass --zcashd-docker-image IMAGE to choose a published image."
      ;;
  esac

  finalize_checks
}

# printf '%q' renders [::]:18233 as \[::\]:18233, correct but hostile to
# copy-paste. Single-quote anything outside a conservative safe set instead.
quote_env_value() {
  local value="$1"
  if [[ "$value" =~ ^[A-Za-z0-9_./:@,=+-]+$ ]]; then
    printf '%s' "$value"
  else
    printf "'%s'" "${value//\'/\'\\\'\'}"
  fi
}

# Shared Zakura env block for the binary/source start commands. Trailing
# backslash but no final newline: callers put this on its own heredoc line.
print_zakurad_env_lines() {
  printf 'ZAKURA_NETWORK__NETWORK=%s \\\n' "$(quote_env_value "$(network_config_value)")"
  printf 'ZAKURA_NETWORK__LISTEN_ADDR=%s \\\n' "$(quote_env_value "$ZAKURA_P2P_ADDR")"
  printf 'ZAKURA_STATE__CACHE_DIR=%s \\\n' "$(quote_env_value "$ZAKURA_STATE_DIR")"
  printf 'ZAKURA_ZCASHD_COMPAT__COOKIE_DIR=%s \\\n' "$(quote_env_value "$COOKIE_DIR")"
  printf 'ZAKURA_ZCASHD_COMPAT__LISTEN_ADDR=%s \\' "$(quote_env_value "$COMPAT_LISTEN_ADDR")"
}

# Shared zcashd P2P-sidecar flags for the binary/source start commands.
print_zcashd_flag_lines() {
  local arg
  while IFS= read -r arg; do
    [[ -n "$arg" ]] && printf '  %s \\\n' "$arg"
  done < <(zcashd_network_args)
  printf '  -datadir=%s \\\n' "$(shell_quote "$ZCASHD_DATADIR")"
  printf '  -conf=%s \\\n' "$(shell_quote "$ZCASHD_CONF")"
  while IFS= read -r arg; do
    [[ -n "$arg" ]] && printf '  %s \\\n' "$arg"
  done < <(zcashd_p2p_pinning_args)
  printf '  -printtoconsole\n'
}

# zcashd container flag block (fixed in-container datadir/conf paths). Emits one
# flag per line so no command-substitution/heredoc line-join can splice two
# flags together with an escaped space.
print_docker_zcashd_flag_lines() {
  local arg
  while IFS= read -r arg; do
    [[ -n "$arg" ]] && printf '  %s \\\n' "$arg"
  done < <(zcashd_network_args)
  printf '  -datadir=/home/zcashd/.zcash \\\n'
  printf '  -conf=/home/zcashd/.zcash/zcash.conf \\\n'
  while IFS= read -r arg; do
    [[ -n "$arg" ]] && printf '  %s \\\n' "$arg"
  done < <(zcashd_p2p_pinning_args)
  printf '  -printtoconsole\n'
}

print_split_binary_commands() {
  cat <<EOF
$(style "$GREEN$BOLD" "Start Zakura in terminal 1:")
$(print_zakurad_env_lines)
$(shell_quote "$ZAKURAD_PATH") start --zcashd-compat

$(style "$GREEN$BOLD" "Start zcashd in terminal 2:")
$(shell_quote "$ZCASHD_PATH") \\
$(print_zcashd_flag_lines)
EOF
}

print_supervised_command() {
  cat <<EOF
$(style "$GREEN$BOLD" "Start Zakura. In the background, downloads hash-pinned zcashd and kicks it off as a supervised child process.")
$(print_zakurad_env_lines)
ZAKURA_ZCASHD_COMPAT__MANAGE_ZCASHD=true \\
ZAKURA_ZCASHD_COMPAT__ZCASHD_SOURCE=managed \\
ZAKURA_ZCASHD_COMPAT__ZCASHD_DATADIR=$(shell_quote "$ZCASHD_DATADIR") \\
$(shell_quote "$ZAKURAD_PATH") start --zcashd-compat
EOF
}

print_docker_supervised_command() {
  local image="${ZAKURA_COMPAT_DOCKER_SELECTED:-$ZAKURA_COMPAT_DOCKER_IMAGE}"
  local container_zebra_state_dir="/home/zebra/.cache/zakura"
  local container_zcashd_datadir="/home/zebra/.cache/zcashd"
  local p2p_port
  p2p_port="$(p2p_port_from_addr "$ZAKURA_P2P_ADDR")"
  cat <<EOF
docker run --rm -it \\
  -e ZCASHD_COMPAT_ENABLED=true \\
  -e ZAKURA_NETWORK__NETWORK=$(shell_quote "$(network_config_value)") \\
  -e ZAKURA_NETWORK__LISTEN_ADDR='[::]:${p2p_port}' \\
  -e ZAKURA_NETWORK__MAX_CONNECTIONS_PER_IP=8 \\
  -e ZAKURA_STATE__CACHE_DIR=$container_zebra_state_dir \\
  -e ZAKURA_ZCASHD_COMPAT__MANAGE_ZCASHD=true \\
  -e ZAKURA_ZCASHD_COMPAT__COOKIE_DIR=$container_zebra_state_dir \\
  -e ZAKURA_ZCASHD_COMPAT__ZCASHD_DATADIR=$container_zcashd_datadir \\
  -e ZAKURA_ZCASHD_COMPAT__LISTEN_ADDR=0.0.0.0:28232 \\
  -e ZAKURA_ZCASHD_COMPAT__UNSAFE_ALLOW_REMOTE_HTTP=true \\
  -e ZAKURA_ZCASHD_COMPAT__ZCASHD_EXTRA_ARGS='["-rpcbind=0.0.0.0","-rpcallowip=0.0.0.0/0"]' \\
  --mount type=bind,src=$(shell_quote "$ZAKURA_STATE_DIR"),dst=$container_zebra_state_dir \\
  --mount type=bind,src=$(shell_quote "$ZCASHD_DATADIR"),dst=$container_zcashd_datadir \\
  -p ${p2p_port}:${p2p_port} \\
  -p 127.0.0.1:28232:28232 \\
  $(shell_quote "$image") \\
  zakurad start --zcashd-compat
EOF
}

print_docker_split_commands() {
  local p2p_port arg
  p2p_port="$(p2p_port_from_addr "$ZAKURA_P2P_ADDR")"
  cat <<EOF
$(style "$GREEN$BOLD" "Start Zakura container in terminal 1:")
docker run --rm -it --name zakura-compat \\
  -e ZAKURA_NETWORK__NETWORK=$(shell_quote "$(network_config_value)") \\
  -e ZAKURA_NETWORK__LISTEN_ADDR='[::]:${p2p_port}' \\
  -e ZAKURA_NETWORK__MAX_CONNECTIONS_PER_IP=8 \\
  -e ZAKURA_STATE__CACHE_DIR=/home/zebra/.cache/zakura \\
  -e ZAKURA_ZCASHD_COMPAT__LISTEN_ADDR=0.0.0.0:28232 \\
  -e ZAKURA_ZCASHD_COMPAT__UNSAFE_ALLOW_REMOTE_HTTP=true \\
  --mount type=bind,src=$(shell_quote "$ZAKURA_STATE_DIR"),dst=/home/zebra/.cache/zakura \\
  -p ${p2p_port}:${p2p_port} \\
  -p 127.0.0.1:28232:28232 \\
  $(shell_quote "$ZAKURA_DOCKER_IMAGE") \\
  zakurad start --zcashd-compat

$(style "$GREEN$BOLD" "Start zcashd container in terminal 2:")
docker run --rm -it --name zakura-compat-zcashd --network host \\
  --mount type=bind,src=$(shell_quote "$ZCASHD_DATADIR"),dst=/home/zcashd/.zcash \\
  $(shell_quote "$ZCASHD_DOCKER_IMAGE") \\
$(print_docker_zcashd_flag_lines)
EOF
}

print_source_commands() {
  cat <<EOF
git clone https://github.com/zakura-core/zakura.git
git clone https://github.com/valargroup/zcashd.git

cd $(shell_quote "$REPO_ROOT") && cargo build --release --bin zakurad
cd $(shell_quote "$ZCASH_SRC_DIR") && ./zcutil/build.sh -j"\$(nproc)"

$(style "$GREEN$BOLD" "Start Zakura in terminal 1:")
$(print_zakurad_env_lines)
$(shell_quote "$ZAKURAD_PATH") start --zcashd-compat

$(style "$GREEN$BOLD" "Start zcashd in terminal 2:")
$(shell_quote "$ZCASHD_PATH") \\
$(print_zcashd_flag_lines)
EOF
}

print_ready_commands() {
  if ((USE_ANSI)); then
    print_section "[ok]" "Ready to start"
    print_command_block_start
  else
    printf 'Ready to start\n\n'
  fi

  case "$MODE" in
    split-binary) print_split_binary_commands ;;
    supervised) print_supervised_command ;;
    docker-supervised) print_docker_supervised_command ;;
    docker-split-containers) print_docker_split_commands ;;
    build-from-source) print_source_commands ;;
  esac

  print_command_block_end

  case "$MODE" in
    docker-supervised | docker-split-containers)
      printf '\n%s ZAKURA_ZCASHD_COMPAT__UNSAFE_ALLOW_REMOTE_HTTP=true is set because the compat RPC is published on 0.0.0.0 over plain HTTP,\n' "$(style "$YELLOW" "[!]")"
      printf '    so the cookie crosses the network in cleartext and is safe behind the local container/private-network boundary.\n'
      printf '    To remove it, enable TLS via ZAKURA_ZCASHD_COMPAT__TLS_CERT_FILE/_TLS_KEY_FILE/_TLS_CA_FILE.\n'
      ;;
  esac
}

while (($#)); do
  case "$1" in
    --mode)
      require_value "$1" "${2:-}"
      MODE="$2"
      shift 2
      ;;
    --network)
      require_value "$1" "${2:-}"
      NETWORK="$2"
      shift 2
      ;;
    --zakura-state-dir | --zebra-state-dir)
      require_value "$1" "${2:-}"
      ZAKURA_STATE_DIR="$2"
      ZAKURA_STATE_DIR_SET=1
      shift 2
      ;;
    --zcashd-datadir)
      require_value "$1" "${2:-}"
      ZCASHD_DATADIR="$2"
      ZCASHD_DATADIR_SET=1
      shift 2
      ;;
    --install-dir)
      require_value "$1" "${2:-}"
      INSTALL_DIR="$2"
      shift 2
      ;;
    --cache-dir)
      require_value "$1" "${2:-}"
      CACHE_DIR="$2"
      shift 2
      ;;
    --cookie-dir)
      require_value "$1" "${2:-}"
      COOKIE_DIR="$2"
      shift 2
      ;;
    --zakura-p2p-addr | --zebra-p2p-addr)
      require_value "$1" "${2:-}"
      ZAKURA_P2P_ADDR="$2"
      shift 2
      ;;
    --compat-listen-addr)
      require_value "$1" "${2:-}"
      COMPAT_LISTEN_ADDR="$2"
      shift 2
      ;;
    --zcash-conf)
      require_value "$1" "${2:-}"
      ZCASHD_CONF="$2"
      shift 2
      ;;
    --zakurad-path | --zebrad-path)
      require_value "$1" "${2:-}"
      ZAKURAD_PATH="$2"
      shift 2
      ;;
    --zcashd-path)
      require_value "$1" "${2:-}"
      ZCASHD_PATH="$2"
      shift 2
      ;;
    --zcashd-docker-image)
      require_value "$1" "${2:-}"
      ZCASHD_DOCKER_IMAGE="$2"
      shift 2
      ;;
    --download-binaries)
      require_value "$1" "${2:-}"
      case "$2" in
        yes | y | Y | YES | Yes)
          DOWNLOAD_BINARIES=1
          DOWNLOAD_BINARIES_SET=1
          ;;
        no | n | N | NO | No)
          DOWNLOAD_BINARIES=0
          DOWNLOAD_BINARIES_SET=1
          ;;
        *)
          echo "--download-binaries must be yes or no" >&2
          usage >&2
          exit 2
          ;;
      esac
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      NON_INTERACTIVE=1
      shift
      ;;
    --unsafe-low-specs)
      UNSAFE_LOW_SPECS=1
      shift
      ;;
    -y | --yes | --non-interactive)
      NON_INTERACTIVE=1
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

normalize_inputs
collect_checks
finalize_checks
data_detection_message

case "$MODE" in
  split-binary | supervised)
    prepare_binary_paths
    ensure_zcashd_conf
    ;;
  docker-split-containers)
    # Bootstrap the standalone zcashd container's conf before prepare_docker_mounts
    # so its recursive chown to the container runtime user covers the new file.
    ensure_zcashd_conf
    prepare_docker_mounts
    prepare_docker_images
    ;;
  docker-supervised)
    prepare_docker_mounts
    prepare_docker_images
    ;;
  build-from-source)
    ensure_zcashd_conf
    ;;
esac

finalize_checks

print_ready_commands
