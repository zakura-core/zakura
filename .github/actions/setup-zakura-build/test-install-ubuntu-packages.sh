#!/usr/bin/env bash

set -euo pipefail

test_dir=$(mktemp -d)
cleanup() {
  rm -rf "$test_dir"
}
trap cleanup EXIT

export APT_BUDGET_SECONDS=120
export APT_ATTEMPT_SECONDS=5
export APT_MIRRORS_FILE="$test_dir/apt-mirrors.txt"

printf '%s\n' \
  'http://azure.archive.ubuntu.com/ubuntu/ priority:1' \
  'https://archive.ubuntu.com/ubuntu/ priority:2' \
  > "$APT_MIRRORS_FILE"

# shellcheck source=.github/actions/setup-zakura-build/install-ubuntu-packages.sh
source "$(dirname "$0")/install-ubuntu-packages.sh"

attempts=0
declare -a attempt_limits=()

sudo() {
  "$@"
}

# The sudo test double dispatches this function in the current shell.
# shellcheck disable=SC2032
timeout() {
  if [[ "$1" != "--kill-after=30" ]]; then
    echo "unexpected timeout option: $1" >&2
    return 1
  fi
  shift
  attempt_limits+=("$1")
  shift
  "$@"
}

sleep() {
  :
}

fake_apt() {
  attempts=$((attempts + 1))
  if (( attempts == 1 )); then
    return 124
  fi
}

apt_deadline=$((SECONDS + APT_BUDGET_SECONDS))
run_apt_network update fake_apt

if (( attempts != 2 )); then
  echo "expected two apt attempts, got ${attempts}" >&2
  exit 1
fi
if [[ "${attempt_limits[*]}" != "5 5" ]]; then
  echo "expected two five-second limits, got: ${attempt_limits[*]}" >&2
  exit 1
fi
if grep -q 'azure\.archive\.ubuntu\.com' "$APT_MIRRORS_FILE"; then
  echo "expected the retry to remove the Azure mirror" >&2
  exit 1
fi
if ! grep -q 'archive\.ubuntu\.com' "$APT_MIRRORS_FILE"; then
  echo "expected the retry to preserve the Ubuntu archive mirror" >&2
  exit 1
fi

echo "setup-zakura-build retry test passed"
