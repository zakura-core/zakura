#!/usr/bin/env bash

set -euo pipefail

# cargo-semver-checks resolves dependencies outside the workspace lock file, so
# registry crates that break under a fresh resolution are patched through an
# isolated Cargo home that both its current and published-baseline builds use.
#
# - tinyvec 1.13.0 omits the alloc::vec macro import in its alloc-only build.
# - librocksdb-sys 0.17.3 builds bindgen without `runtime` unless its
#   `bindgen-runtime` feature is on. A fresh resolution now also picks a second
#   bindgen (0.73+, through libzcash_script's open `>= 0.69.5` requirement)
#   that switches the shared clang-sys into runtime mode, so librocksdb-sys's
#   build script panics with "a `libclang` shared library is not loaded on this
#   thread". The workspace enables `bindgen-runtime`, but published baselines
#   cannot change, so give the crate's bindgen build-dependency the feature.
# - precis-profiles 0.1.14 (published 2026-09-08) moved to precis-core 0.2,
#   but stun-rs 0.1.11 in the iroh 0.92 crates.io baselines still requires
#   precis-core 0.1 and precis-profiles ^0.1.12. A fresh resolution selects
#   both precis-core lines and stun-rs no longer compiles. Hold precis-profiles
#   at the last precis-core 0.1 release. Remove once no stable baseline of
#   zakura-network or its dependents resolves stun-rs, i.e. once a stable
#   zakura-network release ships with Iroh 1.1.
# - The workspace root pins the Iroh family to the zakura-core/iroh
#   compatibility revision through [patch.crates-io]. cargo-semver-checks
#   builds the current crate from a placeholder workspace under target/, where
#   root manifest patches do not apply, so the unpatched crates.io iroh 1.1.0
#   would be selected and its ed25519-dalek 3 / sha2 0.11.0 requirements
#   conflict with bip32's exact sha2 prerelease pin. Mirror the root patches at
#   the config level, which applies to every resolution under this Cargo home.
#   The revision is read from the root manifest so the two cannot drift; remove
#   this block together with the root patch once the fork is consumed from
#   crates.io.
readonly original_cargo_home="${CARGO_HOME:-$HOME/.cargo}"
patch_root=$(mktemp -d "$RUNNER_TEMP/semver-registry.XXXXXX")
readonly patch_root
readonly patched_cargo_home="$patch_root/cargo-home"

# Copy one registry crate's extracted source into the patch root and print the
# copy's path. `version` is the exact registry version, including any build
# metadata used in the extracted directory name.
copy_registry_source() {
  local crate="$1"
  local version="$2"
  local patched_source="$patch_root/$crate-$version"

  cargo info "$crate@${version%%+*}" >/dev/null

  local -a source_candidates
  mapfile -t source_candidates < <(
    find "$original_cargo_home/registry/src" -mindepth 2 -maxdepth 2 -type d \
      -name "$crate-$version"
  )
  if (( ${#source_candidates[@]} != 1 )); then
    echo "expected one $crate $version source directory, found ${#source_candidates[@]}" >&2
    exit 1
  fi

  cp -a "${source_candidates[0]}" "$patched_source"
  echo "$patched_source"
}

readonly tinyvec_version=1.13.0
tinyvec_source=$(copy_registry_source tinyvec "$tinyvec_version")
readonly tinyvec_source
readonly tinyvec_file="$tinyvec_source/src/tinyvec.rs"
if [[ "$(grep -Fxc 'use alloc::vec::{self, Vec};' "$tinyvec_file")" != 1 ]]; then
  echo "tinyvec $tinyvec_version no longer matches the expected broken source" >&2
  exit 1
fi
sed -i 's/use alloc::vec::{self, Vec};/use alloc::{vec, vec::Vec};/' "$tinyvec_file"

readonly librocksdb_sys_version=0.17.3+10.4.2
librocksdb_sys_source=$(copy_registry_source librocksdb-sys "$librocksdb_sys_version")
readonly librocksdb_sys_source
readonly librocksdb_sys_manifest="$librocksdb_sys_source/Cargo.toml"
expected_bindgen_block=$'[build-dependencies.bindgen]\nversion = "0.72"\ndefault-features = false'
if [[ "$(grep -Fx -A2 '[build-dependencies.bindgen]' "$librocksdb_sys_manifest")" != "$expected_bindgen_block" ]]; then
  echo "librocksdb-sys $librocksdb_sys_version no longer matches the expected bindgen dependency" >&2
  exit 1
fi
sed -i '/^\[build-dependencies\.bindgen\]$/,/^default-features = false$/ s/^default-features = false$/default-features = false\nfeatures = ["runtime"]/' \
  "$librocksdb_sys_manifest"

readonly precis_profiles_version=0.1.13
precis_profiles_source=$(copy_registry_source precis-profiles "$precis_profiles_version")
readonly precis_profiles_source

root_manifest="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)/Cargo.toml"
readonly root_manifest
readonly iroh_fork_git="https://github.com/zakura-core/iroh"
iroh_fork_rev=$(sed -n \
  "s|^iroh = { git = \"$iroh_fork_git\", rev = \"\([0-9a-f]\{40\}\)\" }\$|\\1|p" \
  "$root_manifest")
readonly iroh_fork_rev
if [[ -z "$iroh_fork_rev" ]]; then
  echo "the root manifest no longer patches iroh to $iroh_fork_git; remove the Iroh patch block from this script" >&2
  exit 1
fi

mkdir -p "$patched_cargo_home"
ln -s "$original_cargo_home/registry" "$patched_cargo_home/registry"
if [[ -d "$original_cargo_home/git" ]]; then
  ln -s "$original_cargo_home/git" "$patched_cargo_home/git"
fi
cat > "$patched_cargo_home/config.toml" <<EOF
[patch.crates-io]
tinyvec = { path = "$tinyvec_source" }
librocksdb-sys = { path = "$librocksdb_sys_source" }
precis-profiles = { path = "$precis_profiles_source" }
iroh = { git = "$iroh_fork_git", rev = "$iroh_fork_rev" }
iroh-base = { git = "$iroh_fork_git", rev = "$iroh_fork_rev" }
iroh-relay = { git = "$iroh_fork_git", rev = "$iroh_fork_rev" }
iroh-dns = { git = "$iroh_fork_git", rev = "$iroh_fork_rev" }
EOF

echo "CARGO_HOME=$patched_cargo_home" >> "$GITHUB_ENV"
