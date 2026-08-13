#!/usr/bin/env bash
# Verify that publishing the workspace's changed crates leaves a sound
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
# A skipped crate's index manifest can break the graph two ways, and cargo
# reports only one of them:
#
#   - Unresolvable: the index copy pins a requirement in the same
#     semver-compatible class as a version being published but unable to
#     select it. This is how v1.1.0-rc0 failed — index zakura-node-services
#     3.0.0 pins zakura-chain ^3.0.0, which never selects the prerelease
#     3.1.0-rc0, and no single zakura-chain 3.x satisfies both requirement
#     forms. The dry-run publish fails loudly.
#
#   - Duplicated: the index copy pins a different semver-compatible class
#     (an old major). That is not a resolution failure: cargo selects both
#     majors side by side, the dry-run succeeds, and consumers get two
#     copies of the crate with mismatched types between them. --no-verify
#     never compiles anything, so no build error surfaces either. Caught
#     here by reading the Cargo.lock cargo writes into each packaged
#     archive: every workspace crate appearing in any packaged lock must
#     resolve at its workspace version.
#
# Fix either by republishing the pinning crates with updated requirements
# (scripts/prepare-release.sh plans these as "cascade" rows) or by making
# the release candidate GitHub-only.
#
# Requires network access (sparse index queries plus a registry-backed
# dry-run publish). No crates.io token is needed; nothing is uploaded.
#
# Emergency override for a deliberately GitHub-only release: export
# ZAKURA_ALLOW_UNPUBLISHABLE_CRATE_GRAPH=1 to downgrade either failure to a
# warning, and note the override in the release PR.
#
# Usage:
#   ./scripts/check-crate-publish-graph.sh
set -euo pipefail

for cmd in cargo curl git jq tar; do
  command -v "$cmd" >/dev/null || { echo "Missing required tool: $cmd" >&2; exit 1; }
done

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

allow_unpublishable="${ZAKURA_ALLOW_UNPUBLISHABLE_CRATE_GRAPH:-0}"

# Shared tail for both failure shapes: how to fix, then the documented
# GitHub-only override.
finish_unpublishable() {
  cat >&2 <<'EOF'

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
}

metadata="$(cargo metadata --format-version 1 --no-deps)"

# `publish` is null for publishable crates and `[]` for `publish = false`.
publishable="$(jq -r '
  .packages[]
  | select(.publish == null or (.publish | length) > 0)
  | [.name, .version] | @tsv
' <<<"$metadata")"

# The only versions cargo may resolve for workspace crates in any packaged
# lockfile — the assertion below compares against this.
declare -A workspace_version=()
while IFS=$'\t' read -r crate version; do
  [ -n "$crate" ] || continue
  workspace_version["$crate"]="$version"
done <<<"$publishable"

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

# A dedicated target directory keeps this run's packaged archives apart
# from other checks' output and from earlier failed runs: the lockfile
# assertion below must only ever read archives this dry-run wrote.
graph_target_dir="${repo_root}/target/publish-graph-check"
rm -rf "${graph_target_dir}/package"

echo "Dry-run publishing ${#publish_set[@]} crate(s) against the live index..."
publish_status=0
CARGO_TARGET_DIR="$graph_target_dir" \
  cargo publish --workspace --dry-run --no-verify --locked \
  "${exclude_args[@]}" || publish_status=$?

echo
if [ "$publish_status" -ne 0 ]; then
  cat >&2 <<'EOF'
ERROR: the crates.io publish graph does not resolve.

Publishing the crates listed as "to publish" would leave unresolvable
crates on the index: at least one crate that stays at an already-published
version pins a requirement that cannot select any version being published
or unify with the workspace's own requirements (for example, a requirement
without a pre-release tag never matches a pre-release).
EOF
  finish_unpublishable
fi

# The dry-run resolved each packaged crate against the live index plus an
# overlay of the crates being published, and wrote the result into the
# archive as its Cargo.lock. That lock is cargo's own answer to what
# consumers of this publish will get — read it rather than re-deriving it.
# A skipped crate whose index copy pins an old major does not fail the
# dry-run: cargo resolves the old major next to the new one, and the only
# symptom is a workspace crate at a non-workspace version in a packaged
# lock. Asserting every workspace crate resolves at its workspace version
# catches duplicates, stale selections, and every pinning mistake in
# between, with no model of cargo's resolver.
echo "Publish graph resolves; checking the Cargo.lock in each packaged archive..."
if [ ! -d "${graph_target_dir}/package" ]; then
  echo "ERROR: the dry-run left no ${graph_target_dir}/package directory; adapt this check to cargo's packaging layout before trusting it." >&2
  exit 1
fi
stale=0
for entry in "${publish_set[@]}"; do
  member="${entry%@*}-${entry#*@}"
  # Locating each expected archive by name fails closed: if cargo's
  # packaging layout ever moves, this reports a missing archive instead of
  # silently checking nothing.
  archive="$(find "${graph_target_dir}/package" -name "${member}.crate" -print -quit)"
  if [ -z "$archive" ]; then
    echo "ERROR: the dry-run left no ${member}.crate under ${graph_target_dir}/package; adapt this check to cargo's packaging layout before trusting it." >&2
    exit 1
  fi
  lock="$(tar -xOzf "$archive" "${member}/Cargo.lock")" || {
    echo "ERROR: ${archive} has no readable Cargo.lock; adapt this check to cargo's packaging behavior before trusting it." >&2
    exit 1
  }
  while IFS=$'\t' read -r crate version; do
    [ -n "$crate" ] || continue
    want="${workspace_version[$crate]:-}"
    [ -n "$want" ] || continue
    if [ "$version" != "$want" ]; then
      printf 'ERROR: %s resolves %s %s; this workspace publishes %s %s.\n' \
        "$member" "$crate" "$version" "$crate" "$want" >&2
      stale=1
    fi
  done < <(awk '
    /^\[\[package\]\]/ { name = "" }
    /^name = /    { gsub(/"/, ""); name = $3 }
    /^version = / { gsub(/"/, ""); if (name != "") print name "\t" $3 }
  ' <<<"$lock")
done

if [ "$stale" -ne 0 ]; then
  cat >&2 <<'EOF'

ERROR: the crates.io publish graph resolves, but not to this workspace.

A packaged crate's Cargo.lock selects a workspace crate at a version other
than the workspace's. That happens when a crate skipped at publish time
(already published at its workspace version) pins a requirement only an
old version can satisfy: an old major does not fail resolution — cargo
selects it next to the new one, and consumers get two copies of the crate
with mismatched types between them.
EOF
  finish_unpublishable
fi

echo "Publish graph resolves at workspace versions: ${publish_set[*]}"
if [ "$divergent" = 1 ]; then
  echo "WARNING: published crates with locally diverged requirements were skipped above; review the warnings." >&2
fi
exit 0
