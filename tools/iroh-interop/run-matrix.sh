#!/usr/bin/env bash
# Builds both probe workspaces and runs the COMPAT-1/COMPAT-2 matrix.
#
# Pass 1 runs on the host loopback. Pass 2 runs the cells that make Iroh open a
# second path inside an unprivileged network namespace (`unshare -rn`) whose
# loopback delays packets to 127.0.0.1 by 20 ms each way and that has a dummy
# interface with 10.9.0.1. Set NETEM=0 to skip pass 2.
#
# Extra arguments go to the driver in both passes, for example:
#   ./run-matrix.sh --repeat 3 --only C1.c
# Results land in $OUT (default $TMPDIR/iroh-interop-runs/<time>)/{main,netem}.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
toolchain="${INTEROP_TOOLCHAIN:-+1.99.0}"
target="${CARGO_TARGET_DIR:-${TMPDIR:-$HOME/.tmp}/iroh-interop-target}"
out="${OUT:-${TMPDIR:-$HOME/.tmp}/iroh-interop-runs/$(date +%Y%m%d-%H%M%S)}"

CARGO_TARGET_DIR="$target" cargo "$toolchain" build --release --locked \
  --manifest-path "$here/Cargo.toml" --bins
CARGO_TARGET_DIR="$target/upstream" cargo "$toolchain" build --release --locked \
  --manifest-path "$here/upstream/Cargo.toml"

driver=("$target/release/matrix"
  --node-bin "$target/release/interop-node"
  --latest-bin "$target/upstream/release/iroh-latest-node")

status=0
"${driver[@]}" --out "$out/main" "$@" || status=$?

if [ "${NETEM:-1}" != 0 ]; then
  unshare -rn bash -c '
    set -euo pipefail
    ip link set lo up
    tc qdisc add dev lo root handle 1: prio bands 3 priomap 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1
    tc qdisc add dev lo parent 1:1 handle 10: netem delay 20ms
    tc filter add dev lo parent 1: protocol ip prio 1 u32 match ip dst 127.0.0.1/32 flowid 1:1
    ip link add d0 type dummy
    ip addr add 10.9.0.1/32 dev d0
    ip link set d0 up
    INTEROP_NETEM_V4=1 exec "$@"
  ' netem "${driver[@]}" --out "$out/netem" "$@" || status=$?
fi
echo "results: $out/main/results.md $out/netem/results.md"
exit "$status"
