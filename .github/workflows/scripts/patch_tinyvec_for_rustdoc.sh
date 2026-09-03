#!/usr/bin/env bash

set -euo pipefail

# tinyvec 1.13.0 omits the alloc::vec macro import in its alloc-only build.
# cargo-semver-checks resolves dependencies outside the workspace lock file, so
# patch both its current and published-baseline builds through an isolated Cargo home.
readonly tinyvec_version=1.13.0
readonly original_cargo_home="${CARGO_HOME:-$HOME/.cargo}"
patch_root=$(mktemp -d "$RUNNER_TEMP/tinyvec-semver.XXXXXX")
readonly patch_root
readonly patched_cargo_home="$patch_root/cargo-home"
readonly patched_source="$patch_root/tinyvec-$tinyvec_version"

cargo info "tinyvec@$tinyvec_version" >/dev/null

mapfile -t source_candidates < <(
  find "$original_cargo_home/registry/src" -mindepth 2 -maxdepth 2 -type d \
    -name "tinyvec-$tinyvec_version"
)
if (( ${#source_candidates[@]} != 1 )); then
  echo "expected one tinyvec $tinyvec_version source directory, found ${#source_candidates[@]}" >&2
  exit 1
fi

cp -a "${source_candidates[0]}" "$patched_source"
readonly source_file="$patched_source/src/tinyvec.rs"
if [[ "$(grep -Fxc 'use alloc::vec::{self, Vec};' "$source_file")" != 1 ]]; then
  echo "tinyvec $tinyvec_version no longer matches the expected broken source" >&2
  exit 1
fi
sed -i 's/use alloc::vec::{self, Vec};/use alloc::{vec, vec::Vec};/' "$source_file"

mkdir -p "$patched_cargo_home"
ln -s "$original_cargo_home/registry" "$patched_cargo_home/registry"
if [[ -d "$original_cargo_home/git" ]]; then
  ln -s "$original_cargo_home/git" "$patched_cargo_home/git"
fi
cat > "$patched_cargo_home/config.toml" <<EOF
[patch.crates-io]
tinyvec = { path = "$patched_source" }
EOF

echo "CARGO_HOME=$patched_cargo_home" >> "$GITHUB_ENV"
