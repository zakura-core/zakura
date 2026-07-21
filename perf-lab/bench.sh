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

# labels render into unquoted remote heredocs and scp paths — same charset
# guard as droplet.sh names
check_label() { case "$1" in *[!A-Za-z0-9._-]*) die "bad label: $1";; esac; }

# Pin symbolic refs to a SHA at start time: origin/main can move mid-run, and
# the harness re-resolves the ref per leg — an unpinned A/B could build two
# different commits (seen live 2026-07-21).
resolve_sha() {
  local ref="$1"
  if [[ "$ref" =~ ^[0-9a-f]{7,40}$ ]]; then printf '%s\n' "$ref"; return; fi
  git ls-remote --exit-code origin "refs/heads/$ref" 2>/dev/null | awk '{print $1}' \
    || git rev-parse --verify --quiet "$ref^{commit}" \
    || die "cannot resolve ref: $ref"
}

ip_of() { bash "$DIR/droplet.sh" ip "$1"; }

cmd_start() {
  local name="${1:?}" label="${2:?}"
  local build_ref="${3:?usage: bench.sh start NAME LABEL BUILD_REF [BASELINE_REF] [KEY=VAL...]}"
  shift 3
  # optional 4th positional is BASELINE_REF; KEY=VAL args are extra env either
  # way (refs never contain '=', env pairs always do — a git-legal ref WITH '='
  # would be misrouted to env, so don't name refs like that)
  local baseline_ref="main"
  if [ $# -gt 0 ] && [[ "$1" != *=* ]]; then baseline_ref="$1"; shift; fi
  check_label "$label"
  # git-legal refs can contain quote chars; restrict to a safe charset before
  # rendering into the remote script
  local r
  for r in "$build_ref" "$baseline_ref"; do
    case "$r" in *[!A-Za-z0-9._/-]*) die "bad ref: $r";; esac
  done
  # pre-render KEY=VAL pairs with single-quoted values so URLs, spaces, and &
  # survive the remote env list intact
  local env_str="" kv k v
  for kv in "$@"; do
    [[ "$kv" == *=* ]] || die "extra env not KEY=VAL: $kv"
    k="${kv%%=*}"; v="${kv#*=}"
    case "$k" in [!A-Za-z_]*|*[!A-Za-z0-9_]*) die "bad env key: $k";; esac
    env_str+=" $k='${v//\'/\'\\\'\'}'"
  done
  build_ref="$(resolve_sha "$build_ref")"
  baseline_ref="$(resolve_sha "$baseline_ref")"
  local ip; ip="$(ip_of "$name")"; [ -n "$ip" ] || die "no droplet $name"
  # shellcheck disable=SC2087  # client-side expansion of label/refs is intended
  $SSH "${SSH_OPTS[@]}" "root@$ip" bash -s <<REMOTE
set -euo pipefail
# fresh per-label output: the bench script APPENDS to summary.md, so a stale
# same-label dir would leave two tables in one file
rm -rf ${BENCH_OUT_REMOTE}/${label} ${BENCH_OUT_REMOTE}/${label}.log ${BENCH_OUT_REMOTE}/${label}.pid ${BENCH_OUT_REMOTE}/${label}.exit
# the harness never deletes its per-leg forks (~25-85 GB each) and a full disk
# stalls RocksDB into the wall cap (seen live 2026-07-21); one bench per
# droplet makes clearing them at start safe
rm -rf ${BENCH_HOME_REMOTE}/forks/*
mkdir -p ${BENCH_OUT_REMOTE}/${label}
cd ${CTL_CLONE_REMOTE}
# the subshell records the true exit code; nohup+disown+detached stdio survive
# ssh teardown. status reads .exit first, so DONE:<code> is honest even after
# crashes or PID reuse.
( nohup env \
    BUILD_REF='${build_ref}' BASELINE_REF='${baseline_ref}' \
    TARGET_P2P_STACK=zakura BASELINE_P2P_STACK=zakura \
    BENCH_HOME='${BENCH_HOME_REMOTE}' \
    STOP_HEIGHT='${BENCH_STOP_HEIGHT}' \
    OUT_DIR='${BENCH_OUT_REMOTE}/${label}' DASHBOARD=1${env_str} \
    bash scripts/checkpoint-sync-bench.sh
  echo \$? > ${BENCH_OUT_REMOTE}/${label}.exit
) > ${BENCH_OUT_REMOTE}/${label}.log 2>&1 < /dev/null &
echo \$! > ${BENCH_OUT_REMOTE}/${label}.pid
disown
REMOTE
  echo "started bench '$label' on $name (BUILD_REF=$build_ref vs BASELINE_REF=$baseline_ref)"
}

cmd_status() {
  local name="${1:?}" label="${2:?}"
  check_label "$label"
  local ip; ip="$(ip_of "$name")"; [ -n "$ip" ] || die "no droplet $name"
  # shellcheck disable=SC2087  # client-side expansion of label is intended
  $SSH "${SSH_OPTS[@]}" "root@$ip" bash -s <<REMOTE
if [ ! -f ${BENCH_OUT_REMOTE}/${label}.pid ]; then echo ABSENT; exit 0; fi
if [ -f ${BENCH_OUT_REMOTE}/${label}.exit ]; then echo "DONE:\$(cat ${BENCH_OUT_REMOTE}/${label}.exit)"; exit 0; fi
# fallback: the wrapper subshell can die at ssh teardown without writing
# .exit (seen live 2026-07-21); the harness's own final log line marks true
# completion, and it prints nothing after it
if tail -3 ${BENCH_OUT_REMOTE}/${label}.log 2>/dev/null | grep -q "] done\. artifacts in"; then echo DONE:0; exit 0; fi
pid=\$(cat ${BENCH_OUT_REMOTE}/${label}.pid)
if kill -0 "\$pid" 2>/dev/null; then echo RUNNING; else echo DONE:1; fi
REMOTE
}

cmd_collect() {
  local name="${1:?}" label="${2:?}"
  check_label "$label"
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
  # artifacts are safely local now; free the droplet's copy (kept on failed
  # collects for forensics, since the die paths above skip this)
  $SSH "${SSH_OPTS[@]}" "root@$ip" "rm -rf ${BENCH_OUT_REMOTE}/${label} ${BENCH_OUT_REMOTE}/${label}.log ${BENCH_OUT_REMOTE}/${label}.pid ${BENCH_OUT_REMOTE}/${label}.exit" || true
}

case "${1:-}" in
  start)   shift; cmd_start "$@";;
  status)  shift; cmd_status "$@";;
  collect) shift; cmd_collect "$@";;
  *) die "usage: bench.sh start|status|collect ...";;
esac
