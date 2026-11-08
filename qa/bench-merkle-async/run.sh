#!/bin/sh
# Alternate arms for N rounds: run.sh <mode> <rounds> <arm>=<test binary>...
# Each round rotates the starting arm so no arm always runs first.
set -eu
mode=$1 rounds=$2; shift 2
export RAYON_NUM_THREADS=${RAYON_NUM_THREADS:-4} BENCH_MODE=$mode
n=$#
r=0
while [ "$r" -lt "$rounds" ]; do
  i=0
  while [ "$i" -lt "$n" ]; do
    idx=$(( (r + i) % n + 1 ))
    eval "spec=\${$idx}"
    arm=${spec%%=*} bin=${spec#*=}
    BENCH_ARM=$arm "$bin" block::tests::hashing_bench::run --ignored --exact --nocapture \
      --test-threads 1 2>&1 | sed -n "s/^\(MERKLE\|PHASE\|VERIFY\|BREAKDOWN\|CONFIG\),/ROUND$r,\1,/p"
    i=$((i + 1))
  done
  r=$((r + 1))
done
