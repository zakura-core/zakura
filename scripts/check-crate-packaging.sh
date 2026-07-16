#!/usr/bin/env bash
# Preflight for publishing the Zakura crate graph to crates.io.
#
# Packages every publishable workspace crate and checks each .crate archive
# against the crates.io size limit. All crates are packaged in one cargo
# invocation: some zakura-* crates depend on unpublished workspace package
# versions, so per-crate packaging cannot always resolve. Workspace-aware
# `cargo package` (Rust 1.90+) resolves those dependencies against an
# in-memory overlay of the packages being packaged instead.
#
# By default the verify build is skipped: verifying rebuilds the whole
# workspace from the packaged archives, which takes as long as a full
# workspace build. Run with --verify before an actual publish.
#
# Packaging requires a registry-only dependency graph: cargo does not honor
# [patch.crates-io] or git dependencies for consumers of published crates
# (enforced by the check-no-git-dependencies CI job on A-release pull requests).
#
# Usage:
#   ./scripts/check-crate-packaging.sh           # package all, skip verify builds
#   ./scripts/check-crate-packaging.sh --verify  # package all, build each from its archive
set -euo pipefail

VERIFY_FLAG="--no-verify"
if [ $# -gt 0 ]; then
  case "$1" in
    --verify) VERIFY_FLAG="" ;;
    *)
      echo "Usage: $0 [--verify]" >&2
      exit 1
      ;;
  esac
fi

for cmd in cargo jq; do
  command -v "$cmd" >/dev/null || { echo "Missing required tool: $cmd" >&2; exit 1; }
done

repo_root="$(cd "$(dirname "$0")/.." && pwd)"

# The publish order from .github/PULL_REQUEST_TEMPLATE/release-checklist.md
# (dependencies before dependents).
# cargo orders the single packaging invocation itself; this list is what
# gets packaged and size-checked.
PUBLISH_ORDER=(
  zakura-test
  zakura-tower-fallback
  zakura-jsonl-trace
  zakura-chain
  zakura-tower-batch-control
  zakura-node-services
  zakura-script
  zakura-state
  zakura-consensus
  zakura-network
  zakura-rpc
  zakura-utils
  zakura
)

metadata="$(
  cargo metadata --format-version 1 --no-deps --manifest-path "${repo_root}/Cargo.toml"
)"

package_args=()
for crate in "${PUBLISH_ORDER[@]}"; do
  package_args+=(-p "$crate")
done

echo "Packaging ${#PUBLISH_ORDER[@]} crates..."
# shellcheck disable=SC2086 # VERIFY_FLAG is intentionally empty or one flag
cargo package --locked $VERIFY_FLAG "${package_args[@]}"

# crates.io rejects archives over 10 MiB.
max_bytes=$((10 * 1024 * 1024))

echo
echo "Package archive sizes:"
failed=0
for crate in "${PUBLISH_ORDER[@]}"; do
  version="$(jq -r --arg crate "$crate" '.packages[] | select(.name == $crate) | .version' <<<"$metadata")"
  if [ -z "$version" ] || [ "$version" = "null" ]; then
    echo "ERROR: could not find ${crate} in workspace metadata" >&2
    failed=1
    continue
  fi

  archive="${repo_root}/target/package/${crate}-${version}.crate"
  if [ ! -f "$archive" ]; then
    echo "ERROR: expected ${archive} to exist after packaging" >&2
    failed=1
    continue
  fi
  bytes="$(wc -c < "$archive")"
  printf '  %s: %d bytes\n' "$(basename "$archive")" "$bytes"
  if [ "$bytes" -ge "$max_bytes" ]; then
    echo "ERROR: ${crate} exceeds the crates.io 10 MiB package limit" >&2
    failed=1
  fi
done

if [ "$failed" -ne 0 ]; then
  echo "Crate packaging preflight FAILED." >&2
  exit 1
fi

echo
echo "All ${#PUBLISH_ORDER[@]} crates package cleanly."
