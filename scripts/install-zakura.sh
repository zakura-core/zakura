#!/usr/bin/env bash
# Compatibility entry point. The canonical installer is maintained by zakura.com.
set -euo pipefail

INSTALLER_URL="https://zakura.com/install-zakura.sh"
installer="$(mktemp "${TMPDIR:-/tmp}/install-zakura.XXXXXX.sh")"
trap 'rm -f "$installer"' EXIT

curl --fail --silent --show-error --location "$INSTALLER_URL" --output "$installer"
bash "$installer" "$@"
