#!/usr/bin/env bash
# Check that a stable release carries no prerelease leftovers in the
# workspace manifests.
#
# De-rc'ing an rc line to its stable version leaves two classes of debris
# that nothing else catches:
#
# - internal dependency requirements like `^1.2.1-rc0`: cargo-release with
#   `dependent-version = "fix"` only rewrites a requirement when it no
#   longer matches, and the stable version still matches the old rc
#   requirement — so the published manifests would carry rc requirements
#   (hand-fixed during the v1.0.3 hotfix);
# - a publishable crate whose own version was never de-rc'd.
#
# Prerelease tags legitimately carry prerelease versions and requirements,
# so this check only gates stable (non-hyphenated) release tags.
#
# Usage:
#   ./scripts/check-release-requirements.sh <release-tag>
#   ./scripts/check-release-requirements.sh v1.0.4
set -euo pipefail

if [ $# -ne 1 ] || [ -z "$1" ]; then
  echo "Usage: $0 <release-tag> (for example v1.0.4)" >&2
  exit 1
fi
RELEASE_TAG="$1"

case "$RELEASE_TAG" in
  v*) ;;
  *)
    echo "Release tag '${RELEASE_TAG}' must start with 'v'." >&2
    exit 1
    ;;
esac

case "$RELEASE_TAG" in
  *-*)
    echo "Prerelease tag ${RELEASE_TAG}: skipping the stable-requirement check."
    exit 0
    ;;
esac

for cmd in cargo jq; do
  command -v "$cmd" >/dev/null || { echo "Missing required tool: $cmd" >&2; exit 1; }
done

repo_root="$(cd "$(dirname "$0")/.." && pwd)"

# --no-deps only reads the workspace manifests, so this works offline and
# without a Cargo.lock.
metadata="$(cargo metadata --format-version 1 --no-deps --manifest-path "${repo_root}/Cargo.toml")"

# Workspace-internal dependencies are the entries with a `path`; a hyphen in
# a version requirement is a prerelease marker (this workspace only uses
# simple caret/exact requirements, which never contain hyphens otherwise).
# Suggest the dependency's current workspace version as the replacement —
# that is what the manual v1.0.3 normalization set.
stale_requirements="$(jq -r '
  ( [ .packages[] | { key: .name, value: .version } ] | from_entries ) as $versions
  | .packages[]
  | . as $pkg
  | .dependencies[]
  | select(.path != null and (.req | test("-")))
  | [ $pkg.manifest_path, .name, .req, (.kind // "normal"),
      ($versions[.name] // "unknown") ]
  | @tsv
' <<<"$metadata")"

# `publish` is null for publishable crates and `[]` for `publish = false`.
stale_versions="$(jq -r '
  .packages[]
  | select((.publish == null or (.publish | length) > 0)
           and (.version | test("-")))
  | [ .manifest_path, .name, .version ] | @tsv
' <<<"$metadata")"

if [ -z "$stale_requirements" ] && [ -z "$stale_versions" ]; then
  echo "Stable release ${RELEASE_TAG}: no prerelease markers in workspace versions or internal dependency requirements."
  exit 0
fi

echo "ERROR: stable release ${RELEASE_TAG} still carries prerelease markers:" >&2
echo >&2

if [ -n "$stale_versions" ]; then
  while IFS=$'\t' read -r manifest name version; do
    printf '  %s: publishable crate %s is still versioned %s\n' \
      "${manifest#"${repo_root}/"}" "$name" "$version" >&2
  done <<<"$stale_versions"
  echo >&2
fi

if [ -n "$stale_requirements" ]; then
  while IFS=$'\t' read -r manifest dep req kind workspace_version; do
    printf '  %s: %s requirement "%s" (%s)\n' \
      "${manifest#"${repo_root}/"}" "$dep" "$req" "$kind" >&2
    printf '      -> set version = "%s" (%s'\''s current workspace version)\n' \
      "$workspace_version" "$dep" >&2
  done <<<"$stale_requirements"
  echo >&2
fi

cat >&2 <<EOF
Fix each requirement to the dependency's current workspace version shown
above, in the same dependency table entry as its \`path = ...\`. cargo-release
with \`dependent-version = "fix"\` does not rewrite these on a de-rc, because
the stable version still matches the old rc requirement. Then re-run:

  make pre-release RELEASE_TAG=${RELEASE_TAG} BASE_TAG=v<previous>
EOF
exit 1
