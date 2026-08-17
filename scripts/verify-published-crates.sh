#!/usr/bin/env bash
# Confirm that a publish plan actually landed on crates.io, and that each
# version was uploaded by the workflow run that claims it.
#
# crates.io records the GitHub repository, run ID, and commit behind every
# Trusted Publishing upload and serves them publicly as a version's
# `trustpub_data`. Asserting those match this run is what distinguishes "the
# release pipeline published this" from "a version with the right number
# exists" — the latter is also true of a hand-published crate, or of a version
# an attacker with a stolen classic token uploaded first.
#
# Usage:
#   ./scripts/verify-published-crates.sh --plan PATH
#   ./scripts/verify-published-crates.sh --plan PATH \
#     --repository OWNER/NAME --run-id ID --sha COMMIT
#
# --timeout SECONDS bounds the wait for each version to reach the index
# (default 300).
#
# Without the attestation triple, this checks only that each selected version
# exists and is not yanked, which is what an operator wants after a manual
# publish.
set -euo pipefail

tool_root="$(cd "$(dirname "$0")/.." && pwd)"

# The library is linted as a standalone shellcheck input in lint.yml.
# shellcheck source=scripts/lib/crates-index.sh disable=SC1091
. "${tool_root}/scripts/lib/crates-index.sh"

CRATES_API_URL="${CRATES_API_URL:-https://crates.io/api/v1}"

plan_path=""
repository=""
run_id=""
sha=""
timeout_seconds=300

while [ $# -gt 0 ]; do
  case "$1" in
    --plan) plan_path="${2:?--plan needs a path}"; shift 2 ;;
    --repository) repository="${2:?--repository needs OWNER/NAME}"; shift 2 ;;
    --run-id) run_id="${2:?--run-id needs an ID}"; shift 2 ;;
    --sha) sha="${2:?--sha needs a commit}"; shift 2 ;;
    --timeout) timeout_seconds="${2:?--timeout needs seconds}"; shift 2 ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

[ -n "$plan_path" ] || { echo "Usage: $0 --plan PATH [--repository OWNER/NAME --run-id ID --sha COMMIT] [--timeout SECONDS]" >&2; exit 1; }
[ -f "$plan_path" ] || { echo "No publish plan at ${plan_path}." >&2; exit 1; }

attest=false
if [ -n "$run_id" ] || [ -n "$sha" ] || [ -n "$repository" ]; then
  if [ -z "$run_id" ] || [ -z "$sha" ] || [ -z "$repository" ]; then
    echo "--repository, --run-id, and --sha must be given together." >&2
    exit 1
  fi
  attest=true
fi

for cmd in curl jq; do
  command -v "$cmd" >/dev/null || { echo "Missing required tool: $cmd" >&2; exit 1; }
done

selected="$(jq -r '
  .packages[]
  | select(.status != "already_published")
  | [.name, .version]
  | @tsv
' "$plan_path")"

if [ -z "$selected" ]; then
  echo "The plan selected no crates; nothing to verify."
  exit 0
fi

failed=0
while IFS=$'\t' read -r crate version; do
  [ -n "$crate" ] || continue

  # cargo already waits for each crate to reach the index before publishing its
  # dependents, so this normally passes first try; the poll only covers the
  # window where a final crate's index write has not propagated yet.
  waited=0
  until crates_index_has_version "$crate" "$version"; do
    if [ "$waited" -ge "$timeout_seconds" ]; then
      echo "ERROR: ${crate}@${version} never appeared on the crates.io index after ${timeout_seconds}s." >&2
      failed=1
      break
    fi
    sleep 10
    waited=$((waited + 10))
    crates_index_forget "$crate"
  done
  crates_index_has_version "$crate" "$version" || continue

  # --fail turns an HTTP error into a non-zero exit instead of an error body
  # that would parse to a null `yanked` and be reported as a yank. This runs
  # right after an irreversible publish, where a wrong diagnosis is expensive.
  version_json="$(
    curl -sS --fail --retry 4 --retry-connrefused \
      -H 'User-Agent: zakura-release-tooling (https://github.com/zakura-core/zakura)' \
      "${CRATES_API_URL}/crates/${crate}/${version}"
  )" || {
    echo "ERROR: could not read ${crate}@${version} from the crates.io API." >&2
    failed=1
    continue
  }

  if [ "$(jq -r '.version.yanked' <<<"$version_json")" != "false" ]; then
    echo "ERROR: ${crate}@${version} is yanked on crates.io." >&2
    failed=1
    continue
  fi

  if [ "$attest" != true ]; then
    printf '  %-30s %-18s published\n' "$crate" "$version"
    continue
  fi

  actual="$(jq -r '
    .version.trustpub_data
    | if . == null then "none"
      else "\(.provider) \(.repository) \(.run_id) \(.sha)"
      end
  ' <<<"$version_json")"
  expected="github ${repository} ${run_id} ${sha}"
  if [ "$actual" != "$expected" ]; then
    echo "ERROR: ${crate}@${version} was not published by this run." >&2
    echo "         expected: ${expected}" >&2
    echo "         recorded: ${actual}" >&2
    failed=1
    continue
  fi
  printf '  %-30s %-18s published by this run\n' "$crate" "$version"
done <<<"$selected"

if [ "$failed" -ne 0 ]; then
  echo >&2
  echo "ERROR: the publish did not land as planned. crates.io versions cannot be replaced," >&2
  echo "so recover by re-running the publish (already-published crates are skipped), and" >&2
  echo "yank anything uploaded in error." >&2
  exit 1
fi

echo
echo "Every selected crate version is live on crates.io."
