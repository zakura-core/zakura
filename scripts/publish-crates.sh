#!/usr/bin/env bash
# Plan, dry-run, or publish the exact Zakura crate set absent from crates.io.
#
# The publish set is derived from `cargo metadata`, never a hardcoded list, and
# is filtered against the live sparse index: a crate already published at its
# workspace version is excluded. That is what makes `publish` resumable — a run
# that fails partway through republishes nothing when it is dispatched again.
#
# `publish` uploads to crates.io and is irreversible (versions can only be
# yanked, never replaced). It reads CARGO_REGISTRY_TOKEN from the environment —
# in CI, a short-lived Trusted Publishing token — and never logs it.
#
# Usage:
#   ./scripts/publish-crates.sh plan    [--output PATH]
#   ./scripts/publish-crates.sh verify  [--output PATH]
#   ./scripts/publish-crates.sh publish [--output PATH]
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
  plan | verify | publish) ;;
  *)
    echo "Usage: $0 <plan|verify|publish> [--output PATH]" >&2
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

for cmd in cargo curl git jq python3; do
  command -v "$cmd" >/dev/null || {
    echo "Missing required tool: $cmd" >&2
    exit 1
  }
done

# version_precedes VERSION — read published versions on stdin and succeed when
# VERSION sorts below the highest of them under semver precedence.
#
# `sort -V` cannot answer this: it orders 1.0.0 before 1.0.0-rc1, the reverse of
# semver, which would hide exactly the stale-prerelease case this warns about.
#
# Zakura's `-rcN` tags are a single alphanumeric identifier, which semver (and
# so crates.io) compares lexically: 1.0.0-rc10 ranks below 1.0.0-rc2. Past rc9
# the advisory can fire on a version that is newest by tag order. That matches
# how the registry itself orders them; `-rc.N` would sort numerically.
version_precedes() {
  python3 -c '
import sys


def key(version):
    core, _, pre = version.split("+")[0].partition("-")
    nums = [int(part) for part in core.split(".")]
    if not pre:
        # A release outranks every prerelease sharing its core version.
        return (nums, 1, [])
    ids = [(0, int(p), "") if p.isdigit() else (1, 0, p) for p in pre.split(".")]
    return (nums, 0, ids)


candidate = sys.argv[1]
published = [line.strip() for line in sys.stdin if line.strip()]
try:
    sys.exit(0 if key(candidate) < max(key(v) for v in published) else 1)
except ValueError:
    # An unparsable version on the index is not a reason to fail a publish.
    sys.exit(1)
' "$1"
}

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
superseded=()
exclude_args=()

echo "Crates.io publish plan (exact workspace versions):"
while IFS=$'\t' read -r crate version; do
  [ -n "$crate" ] || continue
  published_versions="$(crates_index_versions "$crate")" || {
    echo "Could not query the crates.io index for ${crate}." >&2
    exit 1
  }

  # Advisory only: a hotfix on an older release line legitimately publishes
  # below the newest version, so this informs the approver rather than failing.
  older=false
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
    if version_precedes "$version" <<<"$published_versions"; then
      older=true
      superseded+=("${crate}@${version}")
    fi
  fi
  printf '  %-30s %-18s %s%s\n' "$crate" "$version" "$status" \
    "$([ "$older" = true ] && echo ' (below the newest published version)')"

  packages_json="$(
    jq --arg name "$crate" --arg version "$version" --arg status "$status" \
      --argjson older "$older" \
      '. + [{name: $name, version: $version, status: $status, below_newest_published: $older}]' \
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
  echo "Wrote the publish plan to ${output_path}."
fi

echo
jq -r '
  "Summary: \(.counts.to_publish) to publish, " +
  "\(.counts.initial_publish_required) initial publishes required, " +
  "\(.counts.already_published) already published"
' <<<"$plan_json"

if [ "${#superseded[@]}" -gt 0 ]; then
  echo "WARNING: selected below the newest version already on crates.io: ${superseded[*]}" >&2
  echo "WARNING: expected for a hotfix on an older release line; otherwise this run is publishing from a stale tag." >&2
fi

if [ "${#initial_publish_required[@]}" -gt 0 ]; then
  if [ "$mode" = "publish" ]; then
    cat >&2 <<EOF
ERROR: these crate names do not exist on crates.io: ${initial_publish_required[*]}

Trusted Publishing cannot create a crate: its configuration lives on an
existing crate, so an unknown name has no publisher to authorize this run and
would fail partway through the publish. Reserve each name manually with a
narrowly scoped token from a trusted maintainer machine, add the Trusted
Publishing entry described in docs/release-tag-protection.md, then re-run.
EOF
    exit 1
  fi
  echo "NOTICE: reserve and configure these crate names before they can be published: ${initial_publish_required[*]}" >&2
fi

if [ "$mode" = "plan" ]; then
  exit 0
fi

if [ "${#publish_set[@]}" -eq 0 ]; then
  echo "Every publishable workspace version is already on crates.io; ${mode} is a no-op."
  exit 0
fi

if ! git diff --quiet HEAD -- Cargo.lock; then
  echo "Cargo.lock must be clean before ${mode}." >&2
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

if [ "$mode" = "verify" ]; then
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
  exit 0
fi

if [ -z "${CARGO_REGISTRY_TOKEN:-}" ]; then
  echo "CARGO_REGISTRY_TOKEN is not set; publishing needs a registry token." >&2
  exit 1
fi

echo
echo "Publishing to crates.io: ${publish_set[*]}"
# --no-verify is required, not a shortcut: the verify mode compiled these exact
# packages minutes earlier in a token-free job, and repeating those builds here
# would outlast the 30-minute Trusted Publishing token. Cargo orders the
# multi-package publish and waits for each crate to reach the index itself.
cargo publish --workspace --no-verify --locked "${exclude_args[@]}"

echo
echo "Published: ${publish_set[*]}"
