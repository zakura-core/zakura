#!/usr/bin/env bash
# Run as the operator in a clean pinned source checkout, before node activation.
set -euo pipefail
source_dir=${1:?source checkout}
output_dir=${2:?output directory outside source}
package_dir=$(cd "$(dirname "$0")" && pwd)
[[ "$(uname -s)" == Darwin && "$(uname -m)" == arm64 ]] || { echo 'Native Apple Silicon required' >&2; exit 1; }
[[ "$(git -C "$source_dir" rev-parse HEAD)" == af944f5194ef2e9921bc96af017629450375013c ]] || exit 1
[[ -z "$(git -C "$source_dir" status --porcelain)" ]] || exit 1
if pgrep -x zakurad >/dev/null; then
  echo 'Stop the verifier before native builds/corpus execution' >&2
  exit 1
fi
mkdir -p "$output_dir"
export CARGO_BUILD_JOBS=1 CARGO_TERM_COLOR=never
unset CARGO_BUILD_TARGET RUSTFLAGS CARGO_ENCODED_RUSTFLAGS
unset ROCKSDB_LIB_DIR ROCKSDB_INCLUDE_DIR ROCKSDB_STATIC
export CARGO_TARGET_DIR="$output_dir/target"
# Cargo builds its bundled RocksDB; protobuf is required for generated RPC types.
command -v protoc >/dev/null
rustup toolchain install 1.97.1 --profile minimal
python3 "$package_dir/corpus.py" --source "$source_dir" --output "$output_dir/evidence"
(cd "$source_dir" && cargo +1.97.1 build --locked --release -p zakura --bin zakurad)
cp "$CARGO_TARGET_DIR/release/zakurad" "$output_dir/zakurad"
