#!/usr/bin/env bash
set -euo pipefail

HARNESS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
SPAM_ROOT=/root/ironwood-spam
PID_FILE="${SPAM_ROOT}/logs/round-robin.pid"

if [[ -f "${PID_FILE}" ]]; then
  OLD_PID=$(<"${PID_FILE}")
  if [[ "${OLD_PID}" =~ ^[0-9]+$ ]] && kill -0 "${OLD_PID}" 2>/dev/null; then
    kill "${OLD_PID}"
    for _ in {1..30}; do
      kill -0 "${OLD_PID}" 2>/dev/null || break
      sleep 1
    done
  fi
fi

mkdir -p "${SPAM_ROOT}/logs"
nohup python3 "${HARNESS_DIR}/scripts/round_robin_selfsend.py" \
  --environment testnet \
  "$@" \
  >"${SPAM_ROOT}/logs/round-robin.stdout" 2>&1 &
echo "$!" >"${PID_FILE}"
echo "started mempool spam driver as PID $!"
