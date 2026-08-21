#!/usr/bin/env bash

set -euo pipefail

# A healthy package phase needs about 20 seconds. A degraded mirror has needed
# 13 minutes. Keep the whole phase below the shortest consuming job timeout.
readonly APT_BUDGET_SECONDS="${APT_BUDGET_SECONDS:-1200}"
# Bound each network attempt so one stalled mirror leaves time for failover.
readonly APT_ATTEMPT_SECONDS="${APT_ATTEMPT_SECONDS:-300}"
readonly APT_MIRRORS_FILE="${APT_MIRRORS_FILE:-/etc/apt/apt-mirrors.txt}"

readonly -a packages=(clang libclang-dev protobuf-compiler librocksdb-dev)
readonly -a apt_options=(
  -o Acquire::Retries=3
  -o Acquire::http::Timeout=30
  -o Acquire::https::Timeout=30
  -o DPkg::Lock::Timeout=120
)

apt_deadline=0

remove_azure_apt_mirror() {
  if [[ -f "$APT_MIRRORS_FILE" ]] && grep -q 'azure\.archive\.ubuntu\.com' "$APT_MIRRORS_FILE"; then
    echo "apt: removing the stalled Azure mirror before retrying"
    sudo sed -i '\|azure\.archive\.ubuntu\.com|d' "$APT_MIRRORS_FILE"
  fi
}

# Retry a network-only apt command under both per-attempt and shared limits.
# `timeout` signals the process group, so a killed attempt cannot keep running.
run_apt_network() {
  local label="$1"
  shift
  local attempt=1
  local status remaining attempt_seconds

  while true; do
    remaining=$((apt_deadline - SECONDS))
    if (( remaining < 60 )); then
      echo "::error::apt ${label}: the ${APT_BUDGET_SECONDS}s package budget ran out after $((attempt - 1)) attempt(s)"
      return 1
    fi

    attempt_seconds=$APT_ATTEMPT_SECONDS
    if (( attempt_seconds > remaining )); then
      attempt_seconds=$remaining
    fi

    echo "apt ${label}: attempt ${attempt}, ${remaining}s of budget left, ${attempt_seconds}s attempt limit"
    status=0
    sudo timeout --kill-after=30 "$attempt_seconds" "$@" || status=$?
    if (( status == 0 )); then
      echo "apt ${label}: attempt ${attempt} succeeded"
      return 0
    fi

    if (( status == 124 || status == 137 )); then
      echo "apt ${label}: attempt ${attempt} reached its ${attempt_seconds}s limit and was killed"
    else
      echo "apt ${label}: attempt ${attempt} exited ${status}"
    fi

    if (( attempt == 1 )); then
      remove_azure_apt_mirror
    fi
    attempt=$((attempt + 1))
    sleep 15
  done
}

# The download phase stores every package file before this function runs. Give
# dpkg the remaining shared budget without retrying package configuration.
run_apt_install() {
  local remaining status=0
  remaining=$((apt_deadline - SECONDS))
  if (( remaining < 60 )); then
    echo "::error::apt install: the ${APT_BUDGET_SECONDS}s package budget ran out before installation"
    return 1
  fi

  echo "apt install: ${remaining}s of budget left"
  sudo timeout --kill-after=30 "$remaining" apt-get "${apt_options[@]}" install \
    -y --no-install-recommends "${packages[@]}" || status=$?
  if (( status != 0 )); then
    echo "::error::apt install exited ${status}"
    return "$status"
  fi
  echo "apt install succeeded"
}

main() {
  apt_deadline=$((SECONDS + APT_BUDGET_SECONDS))

  sudo rm -f /etc/apt/sources.list.d/{microsoft-prod,azure-cli}.{list,sources}
  run_apt_network update apt-get "${apt_options[@]}" update
  # Download before installation. Interrupting a download is safe.
  run_apt_network download apt-get "${apt_options[@]}" install \
    -y --no-install-recommends --download-only "${packages[@]}"
  run_apt_install

  # librocksdb-sys runs bindgen through libclang even with system RocksDB.
  echo "ROCKSDB_LIB_DIR=/usr/lib/" >> "$GITHUB_ENV"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
