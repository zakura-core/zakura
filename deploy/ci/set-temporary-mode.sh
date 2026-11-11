#!/usr/bin/env bash
# Toggle reduced CI and retain the original workflow states for restoration.
set -euo pipefail
readonly repo=zakura-core/zakura
readonly state="${XDG_STATE_HOME:-$HOME/.local/state}/zakura-temporary-ci"
readonly snapshot="$state/workflows.json"
readonly -a paused=(
  benchmarks.yml coverage.yml docs-check.yml test-crates.yml test-docker.yml
  zcashd-compat-regtest.yml zakura-e2e.yml history-growth.yml semver-checks.yml
  cross-platform.yml zakura-vct-handoff-canary.yml header-fuzz.yml rpc-fuzz.yml
  p2p-fuzz.yml sim-fuzz.yml fuzz-campaign-check.yml fuzz-campaign.yml
  zakura-mac-cranelift.yml
)
case "${1:-}" in
  on)
    [[ ! -e "$snapshot" ]] || { echo "Existing snapshot: $snapshot; restore it before enabling another interval." >&2; exit 1; }
    mkdir -p "$state"
    gh api "repos/$repo/actions/workflows?per_page=100" > "$snapshot"
    gh variable set TEMPORARY_PR_CI --repo "$repo" --body true
    for workflow in "${paused[@]}"; do
      gh workflow disable "$workflow" --repo "$repo"
    done
    echo "Reduced CI enabled. Original workflow states: $snapshot"
    ;;
  off)
    [[ -f "$snapshot" ]] || { echo "Original workflow state is missing: $snapshot" >&2; exit 1; }
    for workflow in "${paused[@]}"; do
      if jq -e --arg path ".github/workflows/$workflow" \
        '.workflows[] | select(.path == $path and .state == "active")' "$snapshot" >/dev/null; then
        gh workflow enable "$workflow" --repo "$repo"
      fi
    done
    gh variable set TEMPORARY_PR_CI --repo "$repo" --body false
    mv "$snapshot" "$state/restored-workflows.json"
    echo 'Normal CI restored. Drain and remove the temporary runner separately.'
    ;;
  *) echo "Usage: $0 on|off" >&2; exit 2 ;;
esac
