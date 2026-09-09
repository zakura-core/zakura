#!/usr/bin/env bash
# Runs A/B on one temporary host; the other matrix host runs B/A.
# The runner copies the untouched baked state separately for each invocation.
set -euo pipefail

[[ "${WORKLOAD}" == historical_sync ]] || exit 1
HOST_LEG="$LEG"
case "$HOST_LEG" in primary|baseline) ;; *) exit 1 ;; esac
jq -e 'length == 2 and ([.[].leg] | sort == ["baseline", "primary"])' \
  <<<"$BENCH_LEGS_JSON" >/dev/null

ORDER=(primary baseline)
[[ "$HOST_LEG" == baseline ]] && ORDER=(baseline primary)
# This wrapper is used only on this replay's new disposable droplet.
cloud-init status --wait >/dev/null 2>&1
systemctl stop apt-daily.timer apt-daily-upgrade.timer
for _ in $(seq 1 180); do
  if ! systemctl is-active --quiet apt-daily.service && ! systemctl is-active --quiet apt-daily-upgrade.service; then break; fi
  sleep 5
done
if systemctl is-active --quiet apt-daily.service || systemctl is-active --quiet apt-daily-upgrade.service; then
  echo "Package maintenance did not finish before the replay" >&2
  exit 1
fi
export NEEDRESTART_MODE=l
mkdir -p /root/out
# shellcheck source=/dev/null
[[ -f "$HOME/.cargo/env" ]] && . "$HOME/.cargo/env"
{ rustc --version; cargo --version; uname -a; } > /root/out/replay-toolchain.txt
dpkg-query -W > /root/out/packages-before.txt
export FRESH_STATE_COPY=true
if [[ "${HISTORICAL_PRUNED:-false}" == true ]]; then
  # Warm up the baseline once, then clone that exact stopped state for both refs.
  CONFIG=$(jq -ec '.[] | select(.leg == "baseline")' <<<"$BENCH_LEGS_JSON")
  LEG=baseline SHA=$(jq -er '.sha' <<<"$CONFIG") \
    P2P_STACK=$(jq -r '.p2p_stack' <<<"$CONFIG") \
    VCT_FAST_SYNC=$(jq -er '.vct_fast_sync' <<<"$CONFIG") \
    ENABLE_TRACES=false PROFILE=off PREPARE_ONLY=true OUT_DIR=/root/out/preparation \
    bash /root/perf-bench-run.sh
  export COMMON_PREPARATION_DIR=/root/out/preparation
  export PREPARED_STATE_DIR=/mnt/snapshots/perf-prepared-common
fi
for LEG in "${ORDER[@]}"; do
  CONFIG=$(jq -ec --arg leg "$LEG" '.[] | select(.leg == $leg)' <<<"$BENCH_LEGS_JSON")
  SHA=$(jq -er '.sha' <<<"$CONFIG")
  P2P_STACK=$(jq -r '.p2p_stack' <<<"$CONFIG")
  VCT_FAST_SYNC=$(jq -er '.vct_fast_sync' <<<"$CONFIG")
  ENABLE_TRACES=$(jq -er '.traces' <<<"$CONFIG")
  OUT_DIR="/root/out/$LEG"
  export LEG SHA P2P_STACK VCT_FAST_SYNC ENABLE_TRACES OUT_DIR
  mkdir -p "$OUT_DIR"
  jq -n --arg host "$HOST_LEG" --arg leg "$LEG" \
    '{host: $host, leg: $leg, first: ($host == $leg), fresh_state_copy: true}' > "$OUT_DIR/order.json"
  bash /root/perf-bench-run.sh
done

dpkg-query -W > /root/out/packages-after.txt
