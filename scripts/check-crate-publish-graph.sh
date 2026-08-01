#!/usr/bin/env bash
# Verify that publishing the workspace's changed crates leaves a resolvable
# dependency graph on crates.io.
#
# Under changed-crates-only publishing, a crate already published at its
# workspace version is not republished — its dependents resolve against the
# manifest on the index, not the local one. The workspace packaging overlay
# (scripts/check-crate-packaging.sh) always resolves against local
# manifests, so it cannot see that difference. This check can: it computes
# the real publish set (workspace crates whose exact version is absent from
# the index) and dry-run-publishes exactly that set, so every
# already-published crate resolves from the live index.
#
# The failure class this catches, concretely (v1.1.0-rc0): the workspace
# moved zakura-chain to 3.1.0-rc0, but zakura-node-services stayed at its
# published 3.0.0, whose index manifest pins zakura-chain ^3.0.0. A
# requirement without a pre-release tag can never select a pre-release, so
# every crate depending on both stopped resolving — publishing that graph
# would have shipped unresolvable crates. Fix by republishing the pinning
# crates with updated requirements (prepare-release.sh plans these as
# "cascade" bumps) or by making the release candidate GitHub-only.
#
# Requires network access (sparse index queries plus a registry-backed
# dry-run publish). No crates.io token is needed; nothing is uploaded.
#
# Emergency override for a deliberately GitHub-only release: export
# ZAKURA_ALLOW_UNPUBLISHABLE_CRATE_GRAPH=1 to downgrade a resolution
# failure to a warning, and note the override in the release PR.
#
# Usage:
#   ./scripts/check-crate-publish-graph.sh
set -euo pipefail

for cmd in cargo curl git jq; do
  command -v "$cmd" >/dev/null || { echo "Missing required tool: $cmd" >&2; exit 1; }
done

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

# The library is linted as a standalone shellcheck input in lint.yml.
# shellcheck source=scripts/lib/crates-index.sh disable=SC1091
. "${repo_root}/scripts/lib/crates-index.sh"

allow_unpublishable="${ZAKURA_ALLOW_UNPUBLISHABLE_CRATE_GRAPH:-0}"

metadata="$(cargo metadata --format-version 1 --no-deps)"

# `publish` is null for publishable crates and `[]` for `publish = false`.
publishable="$(jq -r '
  .packages[]
  | select(.publish == null or (.publish | length) > 0)
  | [.name, .version] | @tsv
' <<<"$metadata")"

# Workspace-internal requirements by crate, for the divergence advisory.
local_internal_reqs="$(jq -r '
  .packages[]
  | .name as $pkg
  | .dependencies[]
  | select(.path != null and .kind != "dev")
  | [$pkg, .name, .req] | @tsv
' <<<"$metadata")"

publish_set=()
exclude_args=()
divergent=0

echo "Crates.io publish set (exact workspace versions absent from the index):"
while IFS=$'\t' read -r crate version; do
  [ -n "$crate" ] || continue
  rc=0
  crates_index_has_version "$crate" "$version" || rc=$?
  case "$rc" in
    0)
      printf '  %s@%s: already published, will not be republished\n' \
        "$crate" "$version"
      exclude_args+=(--exclude "$crate")

      # Advisory: a published crate whose local requirements differ from the
      # index copy has changed without a version bump. Dependents resolve
      # the index copy, so local-only edits are dead on arrival.
      published_reqs="$(crates_index_deps "$crate" "$version")" \
        || { echo "ERROR: could not read ${crate}@${version} from the index." >&2; exit 1; }
      while IFS=$'\t' read -r pkg dep req; do
        [ "$pkg" = "$crate" ] || continue
        published_req="$(awk -F'\t' -v dep="$dep" \
          '$1 == dep && $3 != "dev" { print $2; exit }' <<<"$published_reqs")"
        if [ -n "$published_req" ] \
          && [ "${published_req#^}" != "${req#^}" ]; then
          printf '      WARNING: local requirement %s = "%s" differs from the published "%s"; the index copy is what dependents resolve\n' \
            "$dep" "${req#^}" "${published_req#^}"
          divergent=1
        fi
      done <<<"$local_internal_reqs"
      ;;
    1)
      printf '  %s@%s: to publish\n' "$crate" "$version"
      publish_set+=("${crate}@${version}")
      ;;
    *)
      echo "ERROR: could not query the crates.io index for ${crate}." >&2
      exit 1
      ;;
  esac
done <<<"$publishable"

echo
if [ "${#publish_set[@]}" -eq 0 ]; then
  echo "Every publishable crate is already on the index; nothing to publish, graph check passes."
  exit 0
fi

echo "Dry-run publishing ${#publish_set[@]} crate(s) against the live index..."
publish_status=0
cargo publish --workspace --dry-run --no-verify --locked \
  "${exclude_args[@]}" || publish_status=$?

echo
if [ "$publish_status" -eq 0 ]; then
  echo "Publish graph resolves: ${publish_set[*]}"
  if [ "$divergent" = 1 ]; then
    echo "WARNING: published crates with locally diverged requirements were skipped above; review the warnings." >&2
  fi
  exit 0
fi

cat >&2 <<'EOF'
ERROR: the crates.io publish graph does not resolve.

Publishing the crates listed as "to publish" would leave unresolvable
crates on the index: at least one crate that stays at an already-published
version pins a requirement that cannot select a version being published
(a requirement without a pre-release tag never matches a pre-release, and
caret requirements never cross a major boundary).

Fix one of these ways:
  - Republish the pinning crates: bump each one (patch + the release's
    prerelease suffix) so its rewritten requirements ship to the index.
    scripts/prepare-release.sh plans these automatically as "cascade" rows.
  - Make the release candidate GitHub-only (no crates.io publishing) and
    export ZAKURA_ALLOW_UNPUBLISHABLE_CRATE_GRAPH=1 (or check the
    allow_unpublishable_crate_graph input when dispatching the Create
    release workflow). Note the override in the release PR.
EOF

if [ "$allow_unpublishable" = 1 ]; then
  echo >&2
  echo "WARNING: ZAKURA_ALLOW_UNPUBLISHABLE_CRATE_GRAPH=1 — continuing despite an unpublishable crate graph; do not publish crates for this release." >&2
  exit 0
fi
exit 1
