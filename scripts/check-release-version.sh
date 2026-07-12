#!/usr/bin/env bash
# Check that a release tag matches the `zakura` package version.
#
# Release binaries are built in Docker without `.git`, so `zakurad --version`
# reports CARGO_PKG_VERSION, not the tag: tagging without bumping the package
# version ships binaries that self-report the wrong release (v1.0.0-rc1 shipped
# reporting 1.0.0-rc0). Run this on the release commit before pushing the tag;
# release-binaries.yml runs the same check and refuses to build or publish
# assets for a mismatched tag.
#
# Usage:
#   ./scripts/check-release-version.sh <release-tag>
#   ./scripts/check-release-version.sh v1.0.0-rc3
set -euo pipefail

if [ $# -ne 1 ] || [ -z "$1" ]; then
  echo "Usage: $0 <release-tag> (for example v1.0.0-rc3)" >&2
  exit 1
fi
RELEASE_TAG="$1"

for cmd in cargo jq; do
  command -v "$cmd" >/dev/null || { echo "Missing required tool: $cmd" >&2; exit 1; }
done

case "$RELEASE_TAG" in
  v*) ;;
  *)
    echo "Release tag '${RELEASE_TAG}' must start with 'v'." >&2
    exit 1
    ;;
esac
tag_version="${RELEASE_TAG#v}"

repo_root="$(cd "$(dirname "$0")/.." && pwd)"

# --no-deps only reads the workspace manifests, so this works offline and
# without a Cargo.lock.
package_version="$(
  cargo metadata --format-version 1 --no-deps --manifest-path "${repo_root}/Cargo.toml" \
    | jq -r '.packages[] | select(.name == "zakura") | .version'
)"

if [ -z "$package_version" ]; then
  echo "Could not find the 'zakura' package in the workspace." >&2
  exit 1
fi

if [ "$package_version" != "$tag_version" ]; then
  cat >&2 <<EOF
Release tag ${RELEASE_TAG} does not match the 'zakura' package version.

  tag version:     ${tag_version}
  package version: ${package_version}

Bump the package version on the release branch before tagging, for example:

  cargo release version --verbose --execute --allow-branch '*' -p zakura <level>

See .github/PULL_REQUEST_TEMPLATE/release-checklist.md ("Update Zakura Version").
EOF
  exit 1
fi

echo "Release tag ${RELEASE_TAG} matches the 'zakura' package version (${package_version})."
