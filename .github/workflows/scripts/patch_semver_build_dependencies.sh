#!/usr/bin/env bash

set -euo pipefail

# cargo-semver-checks resolves outside the workspace lock file. Repair dependency
# build failures in both current and published-baseline builds through an isolated
# Cargo home; the compared Zakura sources and API checks are unchanged.
readonly tinyvec_version=1.13.0
readonly original_cargo_home="${CARGO_HOME:-$HOME/.cargo}"
patch_root=$(mktemp -d "$RUNNER_TEMP/dependencies-semver.XXXXXX")
readonly patch_root
readonly patched_cargo_home="$patch_root/cargo-home"
readonly patched_source="$patch_root/tinyvec-$tinyvec_version"

copy_registry_source() {
  local package="$1" version="$2"
  cargo info "$package@$version" >/dev/null
  local -a sources
  mapfile -t sources < <(
    find "$original_cargo_home/registry/src" -mindepth 2 -maxdepth 2 -type d \
      -name "$package-$version"
  )
  if (( ${#sources[@]} != 1 )); then
    echo "expected one $package $version source directory, found ${#sources[@]}" >&2
    exit 1
  fi
  cp -a "${sources[0]}" "$patch_root/$package-$version"
}

copy_registry_source tinyvec "$tinyvec_version"
# Published Zakura versions cannot inherit the workspace's RocksDB feature fix.
# Give their bindgen 0.72 instance its own libclang loader, even when another
# bindgen version enables clang-sys runtime loading in the same dependency graph.
readonly rocksdb_sys_version=0.17.3+10.4.2
readonly patched_rocksdb_sys="$patch_root/librocksdb-sys-$rocksdb_sys_version"
copy_registry_source librocksdb-sys "$rocksdb_sys_version"

python3 - "$patched_source" "$patched_rocksdb_sys" <<'PYTHON'
from pathlib import Path
import sys


def replace_once(path, before, after):
    text = path.read_text()
    if text.count(before) != 1:
        raise SystemExit(f"{path} no longer matches the expected dependency source")
    path.write_text(text.replace(before, after))


# tinyvec 1.13.0 omits the alloc::vec macro import in its alloc-only build.
replace_once(
    Path(sys.argv[1]) / "src/tinyvec.rs",
    "use alloc::vec::{self, Vec};",
    "use alloc::{vec, vec::Vec};",
)
replace_once(
    Path(sys.argv[2]) / "Cargo.toml",
    '[build-dependencies.bindgen]\nversion = "0.72"\ndefault-features = false\n',
    '[build-dependencies.bindgen]\nversion = "0.72"\nfeatures = ["runtime"]\ndefault-features = false\n',
)
PYTHON

mkdir -p "$patched_cargo_home"
ln -s "$original_cargo_home/registry" "$patched_cargo_home/registry"
if [[ -d "$original_cargo_home/git" ]]; then
  ln -s "$original_cargo_home/git" "$patched_cargo_home/git"
fi
cat > "$patched_cargo_home/config.toml" <<EOF
[patch.crates-io]
tinyvec = { path = "$patched_source" }
librocksdb-sys = { path = "$patched_rocksdb_sys" }
EOF

echo "CARGO_HOME=$patched_cargo_home" >> "$GITHUB_ENV"
