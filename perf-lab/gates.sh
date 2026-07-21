#!/usr/bin/env bash
# Local gates for an experiment worktree.
#   gates.sh l0 WORKTREE_DIR CRATE [CRATE...]   fmt + clippy + targeted tests
#   gates.sh micro-mockbs WORKTREE_DIR [RUNS]   mock-blocksync throughput samples
# Uses a shared CARGO_TARGET_DIR so experiment worktrees build incrementally.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$DIR/config.env"
export CARGO_TARGET_DIR="$ARTIFACT_ROOT/target"
die() { echo "gates.sh: $*" >&2; exit 1; }

cmd_l0() {
  local wt="${1:?}"; shift; [ $# -ge 1 ] || die "l0 needs at least one crate"
  cd "$wt"
  cargo fmt --all -- --check
  for c in "$@"; do cargo clippy -p "$c" --all-targets -- -D warnings; done
  if command -v cargo-nextest >/dev/null 2>&1; then
    for c in "$@"; do cargo nextest run -p "$c" --no-fail-fast; done
  else
    for c in "$@"; do cargo test -p "$c"; done
  fi
  echo "L0 PASS ($*)"
}

cmd_micro_mockbs() {
  local wt="${1:?}" runs="${2:-3}"
  [[ "$runs" =~ ^[1-9][0-9]*$ ]] || die "runs must be a positive integer: $runs"
  cd "$wt"; mkdir -p "$ARTIFACT_ROOT"
  # stderr goes to a log so an unattended failure is diagnosable; a nonzero
  # exit from any run means: discard every sample from this invocation
  local log; log="$ARTIFACT_ROOT/micro-mockbs-$(date +%Y%m%dT%H%M%S).log"
  local i
  for i in $(seq 1 "$runs"); do
    if ! ZAKURA_MOCK_BS_RUN=1 cargo test -p zakura-network --release \
        zakura_mock_blocksync_throughput -- --ignored --nocapture 2>>"$log" \
      | grep -E "^throughput:" | sed "s/^/run $i /"; then
      die "micro-mockbs run $i: no throughput / cargo failed — see $log"
    fi
  done
}

case "${1:-}" in
  l0)           shift; cmd_l0 "$@";;
  micro-mockbs) shift; cmd_micro_mockbs "$@";;
  *) die "usage: gates.sh l0|micro-mockbs ...";;
esac
