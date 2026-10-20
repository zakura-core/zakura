#!/usr/bin/env bash
#
# Deploy-time zcashd-compat sync check.
#
# Thin wrapper around `zakura-compat-check check` (deploy/runner/zakura_monitoring/
# compat.py), the same checker the fleet watchdog runs over SSH. It reads the
# same environment variables as before (ZAKURA_RPC_URL, ZAKURA_COOKIE_FILE,
# ZAKURA_RPC_CONF, ZAKURA_RPC_USER, ZAKURA_RPC_PASSWORD, the ZCASHD_* equivalents,
# the process patterns, HEIGHT_MAX_DRIFT, SYNC_CHECK_TIMEOUT and
# SYNC_CHECK_INTERVAL). Exit codes: 0 passed, 1 failed or timed out, 2 invalid
# configuration.

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
checker="${ZAKURA_COMPAT_CHECK:-}"
if [[ -z "$checker" ]]; then
    for candidate in \
        "$script_dir/../runner/zakura-compat-check" \
        /opt/zakura-monitoring/current/zakura-compat-check; do
        if [[ -f "$candidate" ]]; then
            checker="$candidate"
            break
        fi
    done
fi

if [[ -z "$checker" || ! -f "$checker" ]]; then
    echo "zakura-compat-check not found; set ZAKURA_COMPAT_CHECK" >&2
    exit 2
fi

exec python3 "$checker" check "$@"
