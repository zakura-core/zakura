#!/usr/bin/env bash
# Plan or fully dry-run the exact Zakura crate set absent from crates.io.
#
# This proof of concept deliberately has no publish mode and never accepts or
# reads a registry token.
#
# Usage:
#   ./scripts/publish-crates.sh plan [--output PATH]
#   ./scripts/publish-crates.sh verify [--output PATH]
set -euo pipefail

tool_root="$(cd "$(dirname "$0")/.." && pwd)"
repo_root="${ZAKURA_REPO_ROOT:-$tool_root}"
[ -f "${repo_root}/Cargo.toml" ] || {
  echo "ZAKURA_REPO_ROOT has no Cargo.toml: ${repo_root}" >&2
  exit 1
}
cd "$repo_root"

# The library is linted as a standalone shellcheck input in lint.yml.
# shellcheck source=scripts/lib/crates-index.sh disable=SC1091
. "${tool_root}/scripts/lib/crates-index.sh"

mode="${1:-}"
[ $# -gt 0 ] && shift
case "$mode" in
  plan | verify) ;;
  *)
    echo "Usage: $0 <plan|verify> [--output PATH]" >&2
    exit 1
    ;;
esac

output_path=""
while [ $# -gt 0 ]; do
  case "$1" in
    --output)
      output_path="${2:?--output needs a path}"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

for cmd in cargo curl git jq; do
  command -v "$cmd" >/dev/null || {
    echo "Missing required tool: $cmd" >&2
    exit 1
  }
done

metadata="$(cargo metadata --format-version 1 --no-deps --locked)"
publishable="$(jq -r '
  .packages[]
  | select(.publish == null or (.publish | length) > 0)
  | [.name, .version]
  | @tsv
' <<<"$metadata")"

packages_json='[]'
publish_set=()
initial_publish_required=()
exclude_args=()

echo "Crates.io dry-run plan (exact workspace versions):"
while IFS=$'\t' read -r crate version; do
  [ -n "$crate" ] || continue
  published_versions="$(crates_index_versions "$crate")" || {
    echo "Could not query the crates.io index for ${crate}." >&2
    exit 1
  }

  if grep -Fxq -- "$version" <<<"$published_versions"; then
    status="already_published"
    exclude_args+=(--exclude "$crate")
  elif [ -z "$published_versions" ]; then
    status="initial_publish_required"
    publish_set+=("${crate}@${version}")
    initial_publish_required+=("${crate}@${version}")
  else
    status="to_publish"
    publish_set+=("${crate}@${version}")
  fi
  printf '  %-30s %-18s %s\n' "$crate" "$version" "$status"

  packages_json="$(
    jq --arg name "$crate" --arg version "$version" --arg status "$status" \
      '. + [{name: $name, version: $version, status: $status}]' \
      <<<"$packages_json"
  )"
done <<<"$publishable"

plan_json="$(
  jq -n \
    --arg mode "$mode" \
    --argjson packages "$packages_json" \
    '{
      mode: $mode,
      packages: $packages,
      counts: {
        publishable: ($packages | length),
        already_published: ([$packages[] | select(.status == "already_published")] | length),
        to_publish: ([$packages[] | select(.status == "to_publish")] | length),
        initial_publish_required: ([$packages[] | select(.status == "initial_publish_required")] | length)
      }
    }'
)"

if [ -n "$output_path" ]; then
  printf '%s\n' "$plan_json" > "$output_path"
  echo "Wrote dry-run plan to ${output_path}."
fi

echo
jq -r '
  "Summary: \(.counts.to_publish) to publish, " +
  "\(.counts.initial_publish_required) initial publishes required, " +
  "\(.counts.already_published) already published"
' <<<"$plan_json"

if [ "${#initial_publish_required[@]}" -gt 0 ]; then
  echo "NOTICE: bootstrap these crate names manually before production automation: ${initial_publish_required[*]}" >&2
fi

if [ "$mode" = "plan" ]; then
  exit 0
fi

if [ "${#publish_set[@]}" -eq 0 ]; then
  echo "Every publishable workspace version is already on crates.io; verification is a no-op."
  exit 0
fi

if ! git diff --quiet HEAD -- Cargo.lock; then
  echo "Cargo.lock must be clean before dry-run verification." >&2
  exit 1
fi
lock_backup="$(mktemp)"
cp Cargo.lock "$lock_backup"
restore_lock() {
  if ! cmp -s Cargo.lock "$lock_backup"; then
    cp "$lock_backup" Cargo.lock
  fi
  rm -f "$lock_backup"
}
trap restore_lock EXIT

echo
echo "Checking the selected crates against the live crates.io dependency graph..."
ZAKURA_REPO_ROOT="$repo_root" \
ZAKURA_ALLOW_UNPUBLISHABLE_CRATE_GRAPH=0 \
  "${tool_root}/scripts/check-crate-publish-graph.sh"

echo
echo "Building the selected packaged crates with Cargo dry-run publishing..."
cargo publish --workspace --dry-run --locked "${exclude_args[@]}"

echo
echo "Dry-run verification passed: ${publish_set[*]}"
