#!/usr/bin/env bash
# Perform the mechanical parts of preparing a release PR:
#
#   1. Bump every changed publishable crate since BASE_TAG (level chosen by
#      cargo-semver-checks: breaking -> major, anything else -> minor).
#   2. Bump the zakura package to the release tag version.
#   3. Refresh Cargo.lock.
#   4. Regenerate the stored config fixture for the new version.
#   5. Update the README install tag (stable releases only).
#   6. Floor ESTIMATED_RELEASE_HEIGHT from the committed checkpoint list.
#   7. Assemble the changelog from pending fragments.
#
# Judgment calls stay with the reviewer: minor is never downgraded to patch,
# new crates are reported but not versioned, and the authoritative
# end-of-support estimate still comes from the release checklist. The
# "Prepare release PR" workflow runs this script and opens a draft PR; it is
# equally runnable locally.
#
# Usage:
#   ./scripts/prepare-release.sh --release-tag v1.2.3-rc0 [--base-tag v1.2.2]
#       [--no-crates] [--dry-run] [--summary-json PATH]
#
#   --base-tag      defaults to the most recent v* tag reachable from HEAD
#   --no-crates     GitHub-only release candidate: bump only the zakura package
#   --dry-run       print the bump plan and intended edits; modify nothing
#   --summary-json  write a machine-readable summary (used for the PR body)
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

release_tag=""
base_tag=""
no_crates=0
dry_run=0
summary_json=""

usage() {
  sed -n '2,26p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --release-tag) release_tag="${2:?--release-tag needs a value}"; shift 2 ;;
    --base-tag) base_tag="${2:?--base-tag needs a value}"; shift 2 ;;
    --no-crates) no_crates=1; shift ;;
    --dry-run) dry_run=1; shift ;;
    --summary-json) summary_json="${2:?--summary-json needs a value}"; shift 2 ;;
    -h|--help) usage ;;
    *) echo "Unknown argument: $1" >&2; usage 1 >&2 ;;
  esac
done

# --- Validation -------------------------------------------------------------

if ! [[ "$release_tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-rc[0-9]+)?$ ]]; then
  echo "ERROR: --release-tag must match vX.Y.Z or vX.Y.Z-rcN, got '${release_tag}'." >&2
  exit 1
fi

version="${release_tag#v}"
# Prerelease suffix every bumped crate inherits ("-rc0" or empty for stable).
suffix=""
case "$version" in
  *-*) suffix="-${version#*-}" ;;
esac

if [ "$no_crates" = 1 ] && [ -z "$suffix" ]; then
  echo "ERROR: --no-crates is only valid for release candidates; stable releases always version the full crate graph." >&2
  exit 1
fi

for cmd in cargo git jq awk perl; do
  command -v "$cmd" >/dev/null || { echo "Missing required tool: $cmd" >&2; exit 1; }
done
cargo release --version >/dev/null 2>&1 \
  || { echo "Missing required tool: cargo-release (cargo install cargo-release)" >&2; exit 1; }
if [ "$no_crates" = 0 ]; then
  cargo semver-checks --version >/dev/null 2>&1 \
    || { echo "Missing required tool: cargo-semver-checks (cargo install cargo-semver-checks)" >&2; exit 1; }
fi

if [ -z "$base_tag" ]; then
  if ! base_tag="$(git describe --tags --match 'v*' --abbrev=0 HEAD 2>/dev/null)"; then
    echo "ERROR: no v* release tag found; pass --base-tag explicitly." >&2
    exit 1
  fi
fi

if ! git rev-parse --verify --quiet "${base_tag}^{commit}" >/dev/null; then
  echo "ERROR: base tag '${base_tag}' does not exist or is not a commit." >&2
  exit 1
fi

if ! git merge-base --is-ancestor "$base_tag" HEAD; then
  echo "ERROR: base tag '${base_tag}' is not an ancestor of HEAD." >&2
  exit 1
fi

if [ "$dry_run" = 0 ] && [ -n "$(git status --porcelain --untracked-files=normal)" ]; then
  echo "ERROR: the working tree is not clean; commit or stash before preparing a release." >&2
  exit 1
fi

echo "Preparing ${release_tag} from base ${base_tag}$( [ "$no_crates" = 1 ] && echo ' (zakura package only)' )"

# --- Version helpers --------------------------------------------------------

strip_pre() { printf '%s' "${1%%-*}"; }

# Set a crate's version, choosing the tool by direction. cargo-release refuses
# semver "downgrades", and attaching a prerelease suffix to an already-bumped
# stable-form version is one (2.0.0-rc1 < 2.0.0). Those versions are
# unpublished, so the change is safe: edit the manifest directly and rewrite
# workspace dependency requirements ourselves. Cargo fails closed if a
# requirement were missed — a "2.0.0" requirement does not match 2.0.0-rc1 —
# and the post-apply assertion below re-checks every target version.
set_crate_version() {
  local crate="$1" manifest_rel="$2" current="$3" target="$4"
  if [ "$(strip_pre "$current")" = "$(strip_pre "$target")" ] \
    && [ "$current" = "$(strip_pre "$current")" ]; then
    echo "(direct manifest edit: cargo-release treats ${current} -> ${target} as a downgrade)"
    OLD="$current" NEW="$target" perl -pi -e \
      's/^version = "\Q$ENV{OLD}\E"$/version = "$ENV{NEW}"/ && $done++ unless $done' \
      "$manifest_rel"
    local dep_manifest
    while IFS= read -r dep_manifest; do
      CRATE="$crate" OLD="$current" NEW="$target" perl -0777 -pi -e \
        's/(\Q$ENV{CRATE}\E\s*=\s*\{[^}]*?version\s*=\s*")\Q$ENV{OLD}\E(")/$1$ENV{NEW}$2/gs' \
        "$dep_manifest"
    done < <(jq -r '.packages[].manifest_path' <<<"$metadata")
  else
    cargo release version "$target" \
      --verbose --execute --no-confirm --allow-branch '*' -p "$crate"
  fi
}

bump_level() {
  # bump_level X.Y.Z major|minor
  local core="$1" level="$2" x y
  x="${core%%.*}"
  y="${core#*.}"; y="${y%%.*}"
  case "$level" in
    major) printf '%d.0.0' "$((x + 1))" ;;
    minor) printf '%d.%d.0' "$x" "$((y + 1))" ;;
    *) echo "internal error: unknown bump level '$level'" >&2; return 1 ;;
  esac
}

# --- Phase A: analyse changed publishable crates ----------------------------
#
# Analysis is fully separated from mutation so --dry-run is exact and
# cargo-semver-checks always sees unmodified manifests.

crates_json='[]'
plan_names=()
plan_targets=()
plan_currents=()
plan_manifests=()

metadata="$(cargo metadata --format-version 1 --no-deps)"

add_to_plan() {
  # add_to_plan name target current manifest_rel
  plan_names+=("$1")
  plan_targets+=("$2")
  plan_currents+=("$3")
  plan_manifests+=("$4")
}

add_crate_row() {
  # add_crate_row name base current target level reason
  crates_json="$(jq \
    --arg name "$1" --arg base "$2" --arg current "$3" \
    --arg target "$4" --arg level "$5" --arg reason "$6" \
    '. + [{name: $name, base: $base, current: $current,
           target: $target, level: $level, reason: $reason}]' \
    <<<"$crates_json")"
}

if [ "$no_crates" = 0 ]; then
  while IFS=$'\t' read -r crate manifest_path current_version; do
    [ -n "$crate" ] || continue
    # The zakura package itself is versioned from the release tag below.
    [ "$crate" != "zakura" ] || continue

    manifest_rel="${manifest_path#"$repo_root"/}"
    crate_dir="$(dirname "$manifest_rel")"

    if ! git cat-file -e "${base_tag}:${manifest_rel}" 2>/dev/null; then
      if ! git diff --quiet "$base_tag" -- "$crate_dir"; then
        echo "NOTICE: ${crate} is new since ${base_tag}; choose its initial publish version manually." >&2
        add_crate_row "$crate" "" "$current_version" "" "manual" \
          "new crate since ${base_tag}; choose the initial version manually"
      fi
      continue
    fi

    if git diff --quiet "$base_tag" -- "$crate_dir"; then
      continue
    fi

    # Assumes [package] has a literal version before any other version key;
    # manifests using version.workspace = true would need different parsing.
    base_version="$(
      git show "${base_tag}:${manifest_rel}" \
        | awk -F ' *= *' '$1 == "version" { gsub(/"/, "", $2); print $2; exit }'
    )"
    if [ -z "$base_version" ]; then
      echo "ERROR: could not read ${crate}'s package version at ${base_tag}." >&2
      exit 1
    fi

    if [ "$current_version" != "$base_version" ]; then
      # Already bumped on main (e.g. by the semver-checks merge gate): keep
      # the chosen level and only align the prerelease suffix. This handles
      # rc -> rc and rc -> stable uniformly.
      target="$(strip_pre "$current_version")${suffix}"
      if [ "$target" = "$current_version" ]; then
        add_crate_row "$crate" "$base_version" "$current_version" "$current_version" \
          "kept" "already bumped on main; suffix already matches"
      else
        add_crate_row "$crate" "$base_version" "$current_version" "$target" \
          "kept" "already bumped on main; prerelease suffix aligned"
        add_to_plan "$crate" "$target" "$current_version" "$manifest_rel"
      fi
      continue
    fi

    # Changed but unbumped: let cargo-semver-checks pick the level against the
    # base tag (a git baseline, unlike semver-checks.yml's crates.io baseline,
    # because a base-tag crate version may never have been published).
    echo "Running cargo-semver-checks for ${crate} against ${base_tag}..."
    semver_output=""
    semver_status=0
    semver_output="$(
      cargo semver-checks --package "$crate" --default-features \
        --baseline-rev "$base_tag" 2>&1
    )" || semver_status=$?

    if [ "$semver_status" -eq 0 ]; then
      level="minor"
      reason="semver-checks clean; crate changed, minor is the conservative floor"
    elif grep -qi 'requires new major version' <<<"$semver_output"; then
      level="major"
      reason="semver-checks detected a breaking change"
    elif grep -qi 'requires new minor version' <<<"$semver_output"; then
      level="minor"
      reason="semver-checks requires a minor bump"
    else
      echo "ERROR: cargo-semver-checks failed for ${crate} without a bump verdict:" >&2
      echo "$semver_output" >&2
      exit 1
    fi

    base_core="$(strip_pre "$base_version")"
    if [ "$level" = "major" ]; then
      target_core="$(bump_level "$base_core" major)"
    elif [ "$base_version" != "$base_core" ]; then
      # The base version is an unpublished prerelease: compatible changes fold
      # into the pending bump instead of stacking another one.
      target_core="$base_core"
    else
      target_core="$(bump_level "$base_core" minor)"
    fi
    target="${target_core}${suffix}"

    if [ "$target" = "$current_version" ]; then
      add_crate_row "$crate" "$base_version" "$current_version" "$current_version" \
        "$level" "${reason}; version already at target"
      continue
    fi

    add_crate_row "$crate" "$base_version" "$current_version" "$target" "$level" "$reason"
    add_to_plan "$crate" "$target" "$current_version" "$manifest_rel"
  done < <(
    jq -r '
      .packages[]
      | select(.publish == null or (.publish | length) > 0)
      | [.name, .manifest_path, .version]
      | @tsv
    ' <<<"$metadata"
  )
fi

zakura_old="$(
  cargo metadata --format-version 1 --no-deps \
    | jq -r '.packages[] | select(.name == "zakura") | .version'
)"

# End-of-support floor: committed checkpoint height plus ~3 days of blocks.
# The release checklist's manual estimate from the live chain tip stays
# authoritative; this only keeps a skipped step from shipping a stale value.
eos_file="zakurad/src/components/sync/end_of_support.rs"
checkpoint_file="zakura-chain/src/parameters/checkpoint/main-checkpoints.txt"
committed_height="$(tail -1 "$checkpoint_file" | cut -d' ' -f1)"
eos_floor=$((committed_height + 3456))
eos_old="$(
  grep -oE 'ESTIMATED_RELEASE_HEIGHT: u32 = [0-9_]+' "$eos_file" \
    | grep -oE '[0-9_]+$' | tr -d '_'
)"
eos_floored=0
[ "$eos_old" -lt "$eos_floor" ] && eos_floored=1

fragment_count="$(find changelog-unreleased -name '*.md' ! -name 'README.md' | wc -l | tr -d ' ')"

# --- Report the plan --------------------------------------------------------

echo
echo "Bump plan:"
jq -r '.[] | "  \(.name): \(.base // "-") -> \(.target // "?") [\(.level)] (\(.reason))"' \
  <<<"$crates_json"
[ "$(jq 'length' <<<"$crates_json")" != 0 ] || echo "  (no publishable crate changes)"
echo "  zakura: ${zakura_old} -> ${version} (release tag)"
if [ "$eos_floored" = 1 ]; then
  echo "  ESTIMATED_RELEASE_HEIGHT: ${eos_old} -> ${eos_floor} (checkpoint ${committed_height} + 3456)"
else
  echo "  ESTIMATED_RELEASE_HEIGHT: ${eos_old} already at or above the ${eos_floor} floor"
fi
echo "  Changelog fragments to consume: ${fragment_count}"
if [ -z "$suffix" ]; then
  echo "  README install tag -> ${release_tag}"
else
  echo "  README install tag: unchanged (release candidate)"
fi

write_summary() {
  [ -n "$summary_json" ] || return 0
  jq -n \
    --arg release_tag "$release_tag" \
    --arg base_tag "$base_tag" \
    --argjson no_crates "$([ "$no_crates" = 1 ] && echo true || echo false)" \
    --argjson dry_run "$([ "$dry_run" = 1 ] && echo true || echo false)" \
    --argjson crates "$crates_json" \
    --arg zakura_old "$zakura_old" \
    --arg zakura_new "$version" \
    --argjson eos_old "$eos_old" \
    --argjson eos_floor "$eos_floor" \
    --argjson eos_floored "$([ "$eos_floored" = 1 ] && echo true || echo false)" \
    --argjson readme_updated "$([ -z "$suffix" ] && echo true || echo false)" \
    --argjson fragments_consumed "$fragment_count" \
    '{release_tag: $release_tag, base_tag: $base_tag, no_crates: $no_crates,
      dry_run: $dry_run, crates: $crates,
      zakura: {old: $zakura_old, new: $zakura_new},
      eos: {old: $eos_old, floor: $eos_floor, floored: $eos_floored},
      fixture: ("zakurad/tests/common/configs/v" + $zakura_new + ".toml"),
      readme_updated: $readme_updated,
      fragments_consumed: $fragments_consumed}' \
    > "$summary_json"
  echo "Wrote summary to ${summary_json}"
}

if [ "$dry_run" = 1 ]; then
  echo
  echo "Dry run: no files were modified."
  write_summary
  exit 0
fi

# --- Phase B: apply ---------------------------------------------------------

# --allow-branch '*' because CI runs on a detached HEAD (and local prep may
# run on a scratch branch); release.toml's dependent-version = "fix" rewrites
# workspace-internal requirements alongside each cargo-release bump.
if [ "${#plan_names[@]}" -gt 0 ]; then
  for i in "${!plan_names[@]}"; do
    echo
    echo "==> Bumping ${plan_names[$i]} to ${plan_targets[$i]}"
    set_crate_version "${plan_names[$i]}" "${plan_manifests[$i]}" \
      "${plan_currents[$i]}" "${plan_targets[$i]}"
  done
fi

# The zakura bump must precede the changelog step: prepare-release-changelog
# asserts the tag matches the zakura package version.
if [ "$zakura_old" != "$version" ]; then
  echo
  echo "==> Bumping zakura to ${version}"
  set_crate_version zakura zakurad/Cargo.toml "$zakura_old" "$version"
else
  echo "zakura package is already at ${version}."
fi

# Every planned version must have landed exactly; a missed manifest edit
# would otherwise surface much later (or not at all for a kept requirement).
echo
echo "==> Checking applied versions"
applied="$(cargo metadata --format-version 1 --no-deps)"
assert_version() {
  local crate="$1" want="$2" got
  got="$(jq -r --arg name "$crate" \
    '.packages[] | select(.name == $name) | .version' <<<"$applied")"
  if [ "$got" != "$want" ]; then
    echo "ERROR: ${crate} is at ${got} after the bump, expected ${want}." >&2
    exit 1
  fi
}
if [ "${#plan_names[@]}" -gt 0 ]; then
  for i in "${!plan_names[@]}"; do
    assert_version "${plan_names[$i]}" "${plan_targets[$i]}"
  done
fi
assert_version zakura "$version"
echo "All applied versions match the plan."

echo
echo "==> Refreshing Cargo.lock"
cargo update --workspace
cargo metadata --format-version 1 --locked >/dev/null

# Regenerate the stored config fixture after the version bump so the binary
# self-reports the new version. Generated fixtures are exactly the v*.toml
# files; custom test configurations are kept. The default cache and identity
# paths are read back from the generated config (they are platform-dependent:
# XDG on Linux, ~/Library/Caches on macOS) and replaced globally, matching
# the acceptance test's normalization — the cache path also appears in other
# fields such as cookie_dir.
echo
echo "==> Regenerating the stored config fixture"
rm -f zakurad/tests/common/configs/v*.toml
cargo build --bin zakurad
generated="$(./target/debug/zakurad generate)"
default_cache_dir="$(printf '%s\n' "$generated" | awk -F'"' '/^cache_dir = "/ { print $2; exit }')"
default_identity_dir="$(printf '%s\n' "$generated" | awk -F'"' '/^identity_dir = "/ { print $2; exit }')"
if [ -z "$default_cache_dir" ] || [ -z "$default_identity_dir" ]; then
  echo "ERROR: could not read default cache/identity paths from 'zakurad generate' output." >&2
  exit 1
fi
printf '%s\n' "$generated" \
  | sed "s#${default_cache_dir}#cache_dir#g" \
  | sed "s#${default_identity_dir}#identity_dir#g" \
  > "zakurad/tests/common/configs/v${version}.toml"

if [ -z "$suffix" ]; then
  # Stable releases move the README source-install example to the new tag via
  # the committed pre-release-replacements rule in zakurad/Cargo.toml
  # (cargo release version deliberately skips the replace step).
  echo
  echo "==> Updating the README install tag"
  cargo release replace --verbose --execute --no-confirm --allow-branch '*' -p zakura
fi

if [ "$eos_floored" = 1 ]; then
  echo
  echo "==> Flooring ESTIMATED_RELEASE_HEIGHT at ${eos_floor}"
  formatted="$(echo "$eos_floor" | awk '{n=$0; r=""; for(i=length(n);i>0;i--) { r=substr(n,i,1) r; if((length(n)-i)%3==2 && i>1) r="_" r }; print r}')"
  # sed -i needs a suffix argument on macOS; keep the script runnable locally.
  sed -i.prepare-release.bak \
    "s/ESTIMATED_RELEASE_HEIGHT: u32 = [0-9_]*/ESTIMATED_RELEASE_HEIGHT: u32 = ${formatted}/" \
    "$eos_file"
  rm -f "${eos_file}.prepare-release.bak"
fi

echo
echo "==> Assembling the changelog"
make prepare-release-changelog RELEASE_TAG="$release_tag"

echo
echo "Release preparation for ${release_tag} is complete; review with 'git diff'."
write_summary
