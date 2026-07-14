#!/usr/bin/env bash
# Publish the Zakura crate graph to crates.io from the create-release workflow.
#
# Modes:
#   plan     Compute which publishable workspace crates still need their
#            current version published and write the selection to
#            target/publish-crates-plan.txt. Emits `count=` and `crates=` to
#            $GITHUB_OUTPUT when running under GitHub Actions. Needs no
#            registry token, so it is safe to run locally.
#   verify   Package the selected crates, including their verify builds,
#            without touching the registry. This runs before the registry
#            token is minted: the verify builds take longer than the
#            30-minute Trusted Publishing token lifetime.
#   publish  Publish the selected crates in one dependency-ordered cargo
#            invocation with --no-verify (verification already happened in
#            `verify`). Requires CARGO_REGISTRY_TOKEN.
#
# The selection is derived from `cargo metadata` — every workspace crate
# without `publish = false` — so new crates are picked up automatically. A
# crate name that does not exist on crates.io fails the plan with setup
# instructions instead of failing the publish halfway through the graph:
# every publishable name must already be reserved and have a Trusted
# Publishing configuration (see docs/release-tag-protection.md).
#
# Already-published versions are skipped, so a partially-published graph is
# resumed by re-running the workflow. Versions are checked against the
# sparse index, which lists yanked versions too: a yanked version can never
# be re-published, so it counts as published here.
set -euo pipefail

MODE="${1:-}"
case "$MODE" in
  plan | verify | publish) ;;
  *)
    echo "Usage: $0 <plan|verify|publish>" >&2
    exit 1
    ;;
esac

for cmd in cargo curl jq; do
  command -v "$cmd" >/dev/null || { echo "Missing required tool: $cmd" >&2; exit 1; }
done

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
plan_file="${repo_root}/target/publish-crates-plan.txt"

# The release version is the `zakura` package version: the create-release
# workflow only runs this script on a tag that check-release-version.sh has
# matched against that version, and every crate being published at a given
# release must carry it (unchanged crates stay at their already-published
# older version and are skipped by the index check).
release_version() {
  cargo metadata --format-version 1 --no-deps --manifest-path "${repo_root}/Cargo.toml" \
    | jq -r '.packages[] | select(.name == "zakura") | .version'
}

read_plan() {
  if [ ! -f "$plan_file" ]; then
    echo "Missing ${plan_file}; run '$0 plan' first." >&2
    exit 1
  fi
  mapfile -t selected < "$plan_file"
}

plan() {
  local metadata version index_json http_code index_url selected=()
  metadata="$(cargo metadata --format-version 1 --no-deps --manifest-path "${repo_root}/Cargo.toml")"
  version="$(echo "$metadata" | jq -r '.packages[] | select(.name == "zakura") | .version')"
  if [ -z "$version" ] || [ "$version" = "null" ]; then
    echo "Could not find the 'zakura' package in the workspace." >&2
    exit 1
  fi

  local crate crate_version
  while IFS=$'\t' read -r crate crate_version; do
    # Sparse index path for names of four or more characters; every Zakura
    # crate name is longer.
    index_url="https://index.crates.io/${crate:0:2}/${crate:2:2}/${crate}"
    index_json="$(mktemp)"
    http_code="$(curl -sS --retry 3 -o "$index_json" -w '%{http_code}' "$index_url")"
    case "$http_code" in
      200) ;;
      404)
        echo "ERROR: ${crate} does not exist on crates.io." >&2
        echo "Reserve the name and add its Trusted Publishing configuration" >&2
        echo "before releasing (see docs/release-tag-protection.md)." >&2
        exit 1
        ;;
      *)
        echo "ERROR: sparse index lookup for ${crate} returned HTTP ${http_code}." >&2
        exit 1
        ;;
    esac
    if jq -r '.vers' "$index_json" | grep -Fxq "$crate_version"; then
      echo "  ${crate} ${crate_version}: already published, skipping."
    else
      if [ "$crate_version" != "$version" ]; then
        echo "ERROR: ${crate} is at ${crate_version}, but this release publishes ${version}." >&2
        echo "Crates published at a release must carry the release version;" >&2
        echo "unchanged crates keep their already-published older version." >&2
        exit 1
      fi
      echo "  ${crate} ${crate_version}: selected for publish."
      selected+=("$crate")
    fi
    rm -f "$index_json"
  done < <(
    echo "$metadata" \
      | jq -r '.packages[] | select(.publish == null) | [.name, .version] | @tsv' \
      | sort
  )

  mkdir -p "$(dirname "$plan_file")"
  printf '%s\n' "${selected[@]:-}" | sed '/^$/d' > "$plan_file"
  echo "Planned ${#selected[@]} crate(s) at version ${version}."
  if [ -n "${GITHUB_OUTPUT:-}" ]; then
    {
      echo "count=${#selected[@]}"
      echo "crates=${selected[*]:-}"
    } >> "$GITHUB_OUTPUT"
  fi
}

verify() {
  local selected package_args=()
  read_plan
  if [ "${#selected[@]}" -eq 0 ]; then
    echo "Nothing selected; skipping packaging."
    return
  fi
  local crate
  for crate in "${selected[@]}"; do
    package_args+=(-p "$crate")
  done
  echo "Packaging and verifying ${#selected[@]} crate(s): ${selected[*]}"
  cargo package --locked "${package_args[@]}"
}

publish() {
  local selected version publish_args=()
  read_plan
  if [ "${#selected[@]}" -eq 0 ]; then
    echo "Nothing selected; skipping publish."
    return
  fi
  version="$(release_version)"
  case "$version" in
    *-*)
      echo "ERROR: refusing to publish pre-release version ${version}:" >&2
      echo "pre-releases never reach crates.io (docs/release-tag-protection.md)." >&2
      exit 1
      ;;
  esac
  if [ -z "${CARGO_REGISTRY_TOKEN:-}" ]; then
    echo "ERROR: CARGO_REGISTRY_TOKEN is not set." >&2
    exit 1
  fi
  local crate
  for crate in "${selected[@]}"; do
    publish_args+=(-p "$crate")
  done
  # cargo publishes the selected packages in dependency order, waiting for
  # each to be available in the index before publishing its dependents.
  echo "Publishing ${#selected[@]} crate(s) at version ${version}: ${selected[*]}"
  cargo publish --locked --no-verify "${publish_args[@]}"
}

"$MODE"
