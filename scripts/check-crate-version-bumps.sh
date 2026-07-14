#!/usr/bin/env bash
# Warn when publishable crates changed since the previous release tag without a
# package version bump.
#
# This check is advisory: documentation-only or test-only crate changes might
# not require publishing, and crate versions intentionally advance on their own
# cadence. Run it locally before release dispatch so missed bumps are visible.
#
# Usage:
#   ./scripts/check-crate-version-bumps.sh
#   BASE_TAG=v1.0.0-rc5 ./scripts/check-crate-version-bumps.sh
set -euo pipefail

for cmd in cargo git jq; do
  command -v "$cmd" >/dev/null || { echo "Missing required tool: $cmd" >&2; exit 1; }
done

repo_root="$(cd "$(dirname "$0")/.." && pwd)"

base_tag="${BASE_TAG:-}"
if [ -z "$base_tag" ]; then
if ! base_tag="$(git -C "$repo_root" describe --tags --match 'v*' --abbrev=0 HEAD 2>/dev/null)"; then
    echo "ERROR: no v* release tag found; pass BASE_TAG explicitly." >&2
    exit 1
  fi
fi

if ! git -C "$repo_root" rev-parse --verify --quiet "${base_tag}^{commit}" >/dev/null; then
  echo "ERROR: base tag '${base_tag}' does not exist or is not a commit." >&2
  exit 1
fi

if ! git -C "$repo_root" merge-base --is-ancestor "$base_tag" HEAD; then
  echo "ERROR: base tag '${base_tag}' is not an ancestor of HEAD." >&2
  exit 1
fi

metadata="$(
  cargo metadata --format-version 1 --no-deps --manifest-path "${repo_root}/Cargo.toml"
)"

warning_count=0
changed_count=0
new_count=0

while IFS=$'\t' read -r crate manifest_path current_version; do
  [ -n "$crate" ] || continue

  manifest_rel="${manifest_path#"$repo_root"/}"
  crate_dir="$(dirname "$manifest_rel")"

  if ! git -C "$repo_root" cat-file -e "${base_tag}:${manifest_rel}" 2>/dev/null; then
    if ! git -C "$repo_root" diff --quiet "$base_tag" -- "$crate_dir"; then
      printf 'Changed publishable crate: %s (new -> %s)\n' "$crate" "$current_version"
      printf 'NOTICE: %s is new since %s; choose an initial publish version intentionally.\n' "$crate" "$base_tag" >&2
      new_count=$((new_count + 1))
    fi
    continue
  fi

  if git -C "$repo_root" diff --quiet "$base_tag" -- "$crate_dir"; then
    continue
  fi

  changed_count=$((changed_count + 1))
# Assumes [package] has a literal version before any other version key;
  # manifests using version.workspace = true would need different parsing.
  base_version="$(
    git -C "$repo_root" show "${base_tag}:${manifest_rel}" \
      | awk -F ' *= *' '$1 == "version" { gsub(/"/, "", $2); print $2; exit }'
  )"

  printf 'Changed publishable crate: %s (%s -> %s)\n' \
    "$crate" "${base_version:-unknown}" "$current_version"

  if [ -z "$base_version" ]; then
    echo "WARNING: could not read ${crate}'s package version at ${base_tag}." >&2
    warning_count=$((warning_count + 1))
    continue
  fi

  if [ "$current_version" = "$base_version" ]; then
    cat >&2 <<EOF
WARNING: ${crate} changed since ${base_tag}, but its package version is still ${current_version}.
         If this crate should be published, bump ${manifest_rel}; otherwise ignore this advisory.
EOF
    warning_count=$((warning_count + 1))
  fi
done < <(
  jq -r '
    .packages[]
    | select(.publish == null or (.publish | length) > 0)
    | [.name, .manifest_path, .version]
    | @tsv
  ' <<<"$metadata"
)

if [ "$warning_count" -gt 0 ]; then
  printf '\nWARNING: Crate version bump advisory found %d warning(s) across %d changed published crate(s).\n' \
    "$warning_count" "$changed_count" >&2
else
  echo "Crate version bump advisory found no unchanged versions across ${changed_count} changed published crate(s)."
fi

if [ "$new_count" -gt 0 ]; then
  printf '\nNOTICE: Crate version bump advisory also found %d new published crate(s).\n' \
    "$new_count" >&2
fi

if [ -n "$(git -C "$repo_root" status --porcelain --untracked-files=normal)" ]; then
  cat >&2 <<EOF

NOTICE: The working tree has local changes.
        This advisory compared those changes against ${base_tag}.
EOF
fi
