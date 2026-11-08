#!/bin/sh
# Install the benchmark harness into an arm checkout: install.sh <checkout> <ab|c>
set -eu
here=$(cd "$(dirname "$0")" && pwd)
dst=$1/crates/zakura-consensus/src/block/tests
mkdir -p "$dst/hashing_bench"
cp "$here/hashing_bench.rs" "$dst/hashing_bench.rs"
cp "$here/arm-$2/arm.rs" "$dst/hashing_bench/arm.rs"
grep -q '^mod hashing_bench;' "$1/crates/zakura-consensus/src/block/tests.rs" ||
  sed -i 's/^mod nsm_fees;$/mod nsm_fees;\nmod hashing_bench;/' "$1/crates/zakura-consensus/src/block/tests.rs"
