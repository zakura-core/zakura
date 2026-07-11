#!/usr/bin/env bash

# Entrypoint for running Zakura in Docker.
#
# This script handles privilege dropping and launches zakurad or tests.
# Configuration is managed by config-rs using defaults, optional TOML, and
# environment variables prefixed with ZAKURA_.

set -eo pipefail

# Default cache directories for Zakura components.
# These use the config-rs ZAKURA_SECTION__KEY format and will be picked up
# by zakurad's configuration system automatically.
: "${ZAKURA_STATE__CACHE_DIR:=${ZEBRA_STATE__CACHE_DIR:-${HOME}/.cache/zakura}}"
: "${ZAKURA_RPC__COOKIE_DIR:=${ZEBRA_RPC__COOKIE_DIR:-${HOME}/.cache/zakura}}"
export ZAKURA_STATE__CACHE_DIR ZAKURA_RPC__COOKIE_DIR

# Leave zcashd-compat disabled unless the container runtime explicitly opts in.
# Compat images can set ZCASHD_COMPAT_ENABLED=true to use a vendored
# /usr/local/bin/zcashd, while still allowing ZAKURA_ZCASHD_COMPAT__* overrides.
case "${ZCASHD_COMPAT_ENABLED:-}" in
true | TRUE | 1 | yes | YES | on | ON)
  export ZAKURA_ZCASHD_COMPAT__ENABLED="${ZAKURA_ZCASHD_COMPAT__ENABLED:-${ZEBRA_ZCASHD_COMPAT__ENABLED:-true}}"
  if [[ -x /usr/local/bin/zcashd ]]; then
    export ZAKURA_ZCASHD_COMPAT__ZCASHD_SOURCE="${ZAKURA_ZCASHD_COMPAT__ZCASHD_SOURCE:-${ZEBRA_ZCASHD_COMPAT__ZCASHD_SOURCE:-path}}"
    export ZAKURA_ZCASHD_COMPAT__ZCASHD_PATH="${ZAKURA_ZCASHD_COMPAT__ZCASHD_PATH:-${ZEBRA_ZCASHD_COMPAT__ZCASHD_PATH:-/usr/local/bin/zcashd}}"
  fi
  ;;
false | FALSE | 0 | no | NO | off | OFF | "")
  ;;
*)
  echo "ZCASHD_COMPAT_ENABLED must be true or false" >&2
  exit 1
  ;;
esac

# Use setpriv to drop privileges and execute the given command as the specified UID:GID
exec_as_user() {
  user=$(id -u)
  if [[ ${user} == '0' ]]; then
    exec setpriv --reuid="${UID}" --regid="${GID}" --init-groups "$@"
  else
    exec "$@"
  fi
}

# Helper function
exit_error() {
  echo "$1" >&2
  exit 1
}

# Creates a directory if it doesn't exist and sets ownership to specified UID:GID.
create_owned_directory() {
  local dir="$1"
  # Skip if directory is empty
  [[ -z ${dir} ]] && return

  # Create directory with parents
  mkdir -p "${dir}" || exit_error "Failed to create directory: ${dir}"

  # Set ownership for the created directory
  chown -R "${UID}:${GID}" "${dir}" || exit_error "Failed to secure directory: ${dir}"

  # Set ownership for parent directory (but not if it's root or home)
  local parent_dir
  parent_dir="$(dirname "${dir}")"
  if [[ "${parent_dir}" != "/" && "${parent_dir}" != "${HOME}" ]]; then
    chown "${UID}:${GID}" "${parent_dir}"
  fi
}

# Create and own cache and config directories based on ZEBRA_* environment variables.
[[ -n ${ZAKURA_STATE__CACHE_DIR} ]] && create_owned_directory "${ZAKURA_STATE__CACHE_DIR}"
[[ -n ${ZAKURA_RPC__COOKIE_DIR} ]] && create_owned_directory "${ZAKURA_RPC__COOKIE_DIR}"
[[ -n ${ZAKURA_ZCASHD_COMPAT__ZCASHD_DATADIR:-} ]] && create_owned_directory "${ZAKURA_ZCASHD_COMPAT__ZCASHD_DATADIR}"
[[ -n ${ZAKURA_TRACING__LOG_FILE:-} ]] && create_owned_directory "$(dirname "${ZAKURA_TRACING__LOG_FILE}")"

# Legacy ZEBRA_* variables remain accepted by the config loader.
[[ -n ${ZEBRA_STATE__CACHE_DIR:-} ]] && create_owned_directory "${ZEBRA_STATE__CACHE_DIR}"
[[ -n ${ZEBRA_RPC__COOKIE_DIR:-} ]] && create_owned_directory "${ZEBRA_RPC__COOKIE_DIR}"
[[ -n ${ZEBRA_ZCASHD_COMPAT__ZCASHD_DATADIR:-} ]] && create_owned_directory "${ZEBRA_ZCASHD_COMPAT__ZCASHD_DATADIR}"
[[ -n ${ZEBRA_TRACING__LOG_FILE:-} ]] && create_owned_directory "$(dirname "${ZEBRA_TRACING__LOG_FILE}")"

# --- Optional config file support ---
# If provided, pass a config file path through to zakurad via CONFIG_FILE_PATH.

# If the user provided a config file path we pass it to zakurad.
CONFIG_ARGS=()
if [[ -n ${CONFIG_FILE_PATH} && -f ${CONFIG_FILE_PATH} ]]; then
    echo "INFO: Using config file at ${CONFIG_FILE_PATH}"
    CONFIG_ARGS=(--config "${CONFIG_FILE_PATH}")
fi

# Main Script Logic
# - If "$1" is "--", "-", "zakurad", or "zebrad" (legacy alias), run `zakurad`
#   with the remaining params.
# - If "$1" is "test", handle test execution
# - Otherwise run "$@" directly.
case "$1" in
--* | -* | zakurad | zebrad)
  shift
  exec_as_user zakurad "${CONFIG_ARGS[@]}" "$@"
  ;;
test)
  shift
  if [[ "$1" == "zakurad" || "$1" == "zebrad" ]]; then
    shift
    exec_as_user zakurad "${CONFIG_ARGS[@]}" "$@"
  elif [[ -n "${NEXTEST_PROFILE}" ]]; then
    # All test filtering and scoping logic is handled by .config/nextest.toml
    echo "Running tests with nextest profile: ${NEXTEST_PROFILE}"
    exec_as_user cargo nextest run --locked --release --features "${FEATURES}" --run-ignored=all --hide-progress-bar
  else
    exec_as_user "$@"
  fi
  ;;
*)
  exec_as_user "$@"
  ;;
esac
