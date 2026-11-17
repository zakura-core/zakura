#!/usr/bin/env bash
# Runs the simulator's long checks on a remote host, which the nightly CI jobs
# only smoke: the whole planted catalogue under qa/planted/, and a long fuzz
# campaign of each simulator fuzz target. See qa/fuzz/sim/README.md, Long runs.
#
# Usage, from a checkout on the host:
#
#   scripts/sim-long-runs.sh --out <dir> [--seconds <s>] [--workers <n>]
#
# Each run writes under its own directory, <dir>/<UTC time>-<commit>/, so a
# later run never collides with an earlier one. The script runs in two
# phases, and runs the second even if the first fails:
#
# 1. `cargo xtask planted`, then `cargo xtask planted --targets`. Each must
#    pass.
# 2. With --seconds, `cargo xtask sim-campaign build` for each target in
#    qa/suite.toml's sim rows, then one `cargo xtask sim-campaign start` per
#    built target, all at once, each with --workers workers (default 1) for
#    --seconds seconds and the row's libFuzzer limits. Each target's store is
#    campaigns/<target>/ in the run's directory, and its campaign starts from
#    the harness's curated seeds. A campaign that doesn't end `complete`
#    fails the script. Findings alone don't change that status; each run's
#    end record lists them, and `cargo xtask sim-campaign evidence` sums them.
#
# Each step's log goes to <step>.log in the run's directory. Without
# --seconds, the script runs only the first phase. The host needs the
# checkout's toolchain, the nightly that sim-fuzz.yml pins, cargo-fuzz and jq.
# A run takes hours, so start it detached and read the log later:
#
#   ssh <host> 'cd ~/zakura && nohup scripts/sim-long-runs.sh --out ~/sim-runs \
#       --seconds 14400 --workers 2 > ~/sim-runs.log 2>&1 < /dev/null &'

set -euo pipefail

usage() {
    sed -n '/^# Usage/,/^# Each run/p' "$0" | sed '$d; s/^# \{0,1\}//' >&2
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

# Resolve --out from the caller's directory before moving to the checkout.
mkdir -p "${out}"
out="$(cd "${out}" && pwd)"
cd "$(dirname "$0")/.."
export CXXFLAGS="${CXXFLAGS:--include cstdint}"
# The pinned nightly, from the workflow that runs the targets' smoke.
nightly="$(sed -n 's/^ *nightly: *//p' .github/workflows/sim-fuzz.yml)"
[ -n "${nightly}" ] || { echo "sim-fuzz.yml names no nightly" >&2; exit 1; }
commit="$(git rev-parse --short=12 HEAD)"
run="${out}/$(date -u +%Y%m%dT%H%M%SZ)-${commit}"
mkdir "${run}"
echo "== run directory: ${run}"

rows=""
if [ -n "${seconds}" ]; then
    # Read the campaign rows now, so a missing tool fails before the hours of
    # the first phase. The rows are those that sim-fuzz.yml's smoke runs, one
    # tab-separated line each.
    command -v jq > /dev/null || { echo "--seconds needs jq" >&2; exit 1; }
    rows="$(python3 scripts/check-fuzz-campaign.py --matrix sim \
        | jq -r '.include[] | [.target, .harness, .rss_mb, .max_len, .detect_leaks, .extra_args] | @tsv')"
    [ -n "${rows}" ] || { echo "qa/suite.toml has no sim rows" >&2; exit 1; }
fi

# Runs a named step, logging its output and how long it took. The step reads
# no input, so it can't consume the rows that a loop reads.
step() {
    local name="$1"
    shift
    local start
    start="$(date +%s)"
    echo "== ${name}: $*"
    if "$@" > "${run}/${name}.log" 2>&1 < /dev/null; then
        echo "== ${name}: passed in $(( $(date +%s) - start ))s; see ${run}/${name}.log"
    else
        echo "== ${name}: failed in $(( $(date +%s) - start ))s; see ${run}/${name}.log"
        return 1
    fi
}

failed=0
step planted env RUSTFLAGS="--cfg zakura_sim" cargo xtask planted || failed=1
step planted-targets env RUSTFLAGS="--cfg zakura_sim" \
    cargo "+${nightly}" xtask planted --targets || failed=1

if [ -n "${seconds}" ]; then
    # Build every target first, since they share the fuzz package's target
    # directory, so that no build competes with a running campaign.
    built=()
    while IFS=$'\t' read -r target harness rss_mb max_len detect_leaks extra_args; do
        store="${run}/campaigns/${target}"
        mkdir -p "${store}"
        if ! step "build-${target}" cargo xtask sim-campaign build --store "${store}" --target "${target}"; then
            failed=1
            continue
        fi
        build="$(sed -n 's/.* as build \([0-9a-f]\{64\}\)$/\1/p' "${run}/build-${target}.log" | tail -n 1)"
        if [ -z "${build}" ]; then
            echo "== build-${target}: names no build; see ${run}/build-${target}.log"
            failed=1
            continue
        fi
        # Tab is whitespace to `read`, so runs of tabs merge: the args, which
        # can be empty, go last.
        built+=("${target}"$'\t'"${harness}"$'\t'"${build}"$'\t'"${rss_mb}"$'\t'"${max_len}"$'\t'"${detect_leaks}"$'\t'"${extra_args}")
    done <<< "${rows}"

    # Then run every built target's campaign at once.
    pids=()
    for row in "${built[@]}"; do
        IFS=$'\t' read -r target harness build rss_mb max_len detect_leaks extra_args <<< "${row}"
        args=(--max-len "${max_len}" --rss-limit-mb "${rss_mb}" --libfuzzer-arg "-detect_leaks=${detect_leaks}")
        # The suite row's other args, such as `-len_control=0`. None contains a space.
        read -r -a extra <<< "${extra_args}"
        for arg in "${extra[@]}"; do
            args+=(--libfuzzer-arg "${arg}")
        done
        ASAN_OPTIONS="detect_leaks=${detect_leaks}" step "campaign-${target}" \
            cargo xtask sim-campaign start --store "${run}/campaigns/${target}" \
            --campaign "${target}-${commit}" --sim "${harness}" --build "${build}" \
            --workers "${workers}" --seconds "${seconds}" "${args[@]}" &
        pids+=("$!")
    done
    for pid in "${pids[@]}"; do
        wait "${pid}" || failed=1
    done
fi

exit "${failed}"
