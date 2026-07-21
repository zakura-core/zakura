#!/usr/bin/env bash
# One A/B bench on a perf-lab droplet, asynchronously:
#   bench.sh start   NAME LABEL BUILD_REF [BASELINE_REF=main] [EXTRA_ENV...]
#   bench.sh status  NAME LABEL          -> RUNNING | DONE:<exit> | ABSENT
#   bench.sh collect NAME LABEL          -> pulls artifacts, prints verdict JSON path
# EXTRA_ENV: KEY=VAL pairs passed to checkpoint-sync-bench.sh (e.g. CKPT_LIMIT=3000).
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$DIR/config.env"
SSH="${SSH_BIN:-ssh}"; SCP="${SCP_BIN:-scp}"
SSH_OPTS=(-i "$SSH_KEY_FILE" -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10)
die() { echo "bench.sh: $*" >&2; exit 1; }

ip_of() { bash "$DIR/droplet.sh" ip "$1"; }

cmd_start() {
  local name="${1:?}" label="${2:?}"
  local build_ref="${3:?usage: bench.sh start NAME LABEL BUILD_REF [BASELINE_REF] [KEY=VAL...]}"
  shift 3
  # optional 4th positional is BASELINE_REF; KEY=VAL args are extra env either
  # way (refs never contain '=', env pairs always do)
  local baseline_ref="main"
  if [ $# -gt 0 ] && [[ "$1" != *=* ]]; then baseline_ref="$1"; shift; fi
  local extra_env=("$@")
  local ip; ip="$(ip_of "$name")"; [ -n "$ip" ] || die "no droplet $name"
  # shellcheck disable=SC2087  # client-side expansion of label/refs is intended
  $SSH "${SSH_OPTS[@]}" "root@$ip" bash -s <<REMOTE
set -euo pipefail
# fresh per-label output: the bench script APPENDS to summary.md, so a stale
# same-label dir would leave two tables in one file
rm -rf ${BENCH_OUT_REMOTE}/${label} ${BENCH_OUT_REMOTE}/${label}.log ${BENCH_OUT_REMOTE}/${label}.pid
mkdir -p ${BENCH_OUT_REMOTE}/${label}
cd ${CTL_CLONE_REMOTE}
nohup env \
  BUILD_REF='${build_ref}' BASELINE_REF='${baseline_ref}' \
  TARGET_P2P_STACK=zakura BASELINE_P2P_STACK=zakura \
  BENCH_HOME='${BENCH_HOME_REMOTE}' \
  OUT_DIR='${BENCH_OUT_REMOTE}/${label}' DASHBOARD=1 ${extra_env[@]+${extra_env[@]}} \
  bash scripts/checkpoint-sync-bench.sh \
  > ${BENCH_OUT_REMOTE}/${label}.log 2>&1 < /dev/null &
echo \$! > ${BENCH_OUT_REMOTE}/${label}.pid
disown
REMOTE
  echo "started bench '$label' on $name (BUILD_REF=$build_ref vs BASELINE_REF=$baseline_ref)"
}

cmd_status() {
  local name="${1:?}" label="${2:?}"
  local ip; ip="$(ip_of "$name")"; [ -n "$ip" ] || die "no droplet $name"
  $SSH "${SSH_OPTS[@]}" "root@$ip" bash -s <<REMOTE
if [ ! -f ${BENCH_OUT_REMOTE}/${label}.pid ]; then echo ABSENT; exit 0; fi
pid=\$(cat ${BENCH_OUT_REMOTE}/${label}.pid)
if kill -0 "\$pid" 2>/dev/null; then echo RUNNING; else
  if [ -f ${BENCH_OUT_REMOTE}/${label}/summary.md ]; then echo DONE:0; else echo DONE:1; fi
fi
REMOTE
}

cmd_collect() {
  local name="${1:?}" label="${2:?}"
  local ip; ip="$(ip_of "$name")"; [ -n "$ip" ] || die "no droplet $name"
  local st; st="$(cmd_status "$name" "$label")"
  [ "${st#DONE}" != "$st" ] || die "bench '$label' is $st, not DONE"
  local dest="$ARTIFACT_ROOT/runs/$label"; mkdir -p "$dest"
  $SCP "${SSH_OPTS[@]}" -r "root@$ip:${BENCH_OUT_REMOTE}/${label}/." "$dest/"
  $SCP "${SSH_OPTS[@]}" "root@$ip:${BENCH_OUT_REMOTE}/${label}.log" "$dest/bench.log" || true
  [ -f "$dest/summary.md" ] || die "no summary.md in $dest — see $dest/bench.log ($st)"
  local band="${NOISE_BAND_PCT:-0}"
  python3 "$DIR/verdict.py" "$dest/summary.md" \
    --threshold-pct "$WIN_THRESHOLD_PCT" --noise-band-pct "${band:-0}" \
    > "$dest/verdict.json"
  echo "$dest/verdict.json"
  cat "$dest/verdict.json"
}

case "${1:-}" in
  start)   shift; cmd_start "$@";;
  status)  shift; cmd_status "$@";;
  collect) shift; cmd_collect "$@";;
  *) die "usage: bench.sh start|status|collect ...";;
esac
