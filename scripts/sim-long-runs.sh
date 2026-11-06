#!/usr/bin/env bash
# Runs the simulator's long checks on a remote host, which the nightly CI jobs
# only smoke: the whole planted catalogue under qa/planted/, and a long fuzz
# campaign of each simulator fuzz target. See qa/fuzz/sim/README.md, Long runs.
#
# Usage, from a checkout on the host:
#
#   scripts/sim-long-runs.sh --out <dir> [--seconds <s>] [--workers <n>]
#
# The script runs in two phases:
#
# 1. `cargo xtask planted`, then `cargo xtask planted --targets`. Each must
#    pass. Their logs go to <dir>/<commit>/.
# 2. With --seconds, one `cargo xtask sim-campaign` per target in
#    qa/suite.toml's sim rows, all at once, each with --workers workers
#    (default 1) for --seconds seconds and the row's libFuzzer limits. Each
#    target's store is <dir>/campaigns/<target>/, so a later run resumes
#    there or starts a new segment. Findings don't fail the script; each
#    run's end record lists them, and `cargo xtask sim-campaign evidence`
#    sums them.
#
# Without --seconds, the script runs only the first phase. The host needs the
# checkout's toolchain, the nightly that sim-fuzz.yml pins, cargo-fuzz and jq.
# A run takes hours, so start it detached and read the log later:
#
#   ssh <host> 'cd ~/zakura && nohup scripts/sim-long-runs.sh --out ~/sim-runs \
#       --seconds 14400 --workers 2 > ~/sim-runs.log 2>&1 < /dev/null &'

set -euo pipefail

usage() {
    sed -n '/^# Usage/,/^# The script/p' "$0" | sed '$d; s/^# \{0,1\}//' >&2
    exit 2
}

out=""
seconds=""
workers=1
while [ $# -gt 0 ]; do
    case "$1" in
        --out) out="${2:?--out needs a directory}"; shift 2 ;;
        --seconds) seconds="${2:?--seconds needs a number}"; shift 2 ;;
        --workers) workers="${2:?--workers needs a number}"; shift 2 ;;
        *) usage ;;
    esac
done
[ -n "${out}" ] || usage

cd "$(dirname "$0")/.."
export CXXFLAGS="${CXXFLAGS:--include cstdint}"
# The pinned nightly, from the workflow that runs the targets' smoke.
nightly="$(sed -n 's/^ *nightly: *//p' .github/workflows/sim-fuzz.yml)"
[ -n "${nightly}" ] || { echo "sim-fuzz.yml names no nightly" >&2; exit 1; }
commit="$(git rev-parse --short=12 HEAD)"
logs="${out}/${commit}"
mkdir -p "${logs}"

# Runs a named step, logging its output and how long it took.
step() {
    local name="$1"
    shift
    local start
    start="$(date +%s)"
    echo "== ${name}: $*"
    if "$@" > "${logs}/${name}.log" 2>&1; then
        echo "== ${name}: passed in $(( $(date +%s) - start ))s; see ${logs}/${name}.log"
    else
        echo "== ${name}: failed in $(( $(date +%s) - start ))s; see ${logs}/${name}.log"
        return 1
    fi
}

failed=0
step planted env RUSTFLAGS="--cfg zakura_sim" cargo xtask planted || failed=1
step planted-targets env RUSTFLAGS="--cfg zakura_sim" \
    cargo "+${nightly}" xtask planted --targets || failed=1

if [ -n "${seconds}" ]; then
    # Builds each target in turn, since they share the fuzz package's target
    # directory, then runs every campaign at once.
    # The rows that sim-fuzz.yml's smoke runs, one tab-separated line each.
    rows="$(python3 scripts/check-fuzz-campaign.py --matrix sim \
        | jq -r '.include[] | [.target, .harness, .rss_mb, .max_len, .detect_leaks, .extra_args] | @tsv')"
    pids=()
    while IFS=$'\t' read -r target harness rss_mb max_len detect_leaks extra_args; do
        store="${out}/campaigns/${target}"
        mkdir -p "${store}"
        if ! step "build-${target}" cargo xtask sim-campaign build --store "${store}" --target "${target}"; then
            failed=1
            continue
        fi
        build="$(sed -n 's/.* as build \([0-9a-f]\{64\}\)$/\1/p' "${logs}/build-${target}.log" | tail -n 1)"
        args=(--max-len "${max_len}" --rss-limit-mb "${rss_mb}" --libfuzzer-arg "-detect_leaks=${detect_leaks}")
        for arg in ${extra_args}; do
            args+=(--libfuzzer-arg "${arg}")
        done
        ASAN_OPTIONS="detect_leaks=${detect_leaks}" step "campaign-${target}" \
            cargo xtask sim-campaign start --store "${store}" --campaign "${target}-${commit}" \
            --sim "${harness}" --build "${build}" --workers "${workers}" --seconds "${seconds}" \
            "${args[@]}" &
        pids+=("$!")
    done <<< "${rows}"
    for pid in "${pids[@]}"; do
        wait "${pid}" || failed=1
    done
fi

exit "${failed}"
