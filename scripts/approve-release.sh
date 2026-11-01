#!/usr/bin/env bash
# approve-release.sh — approve a waiting `Create release` run with your
# security key. This replaces the GitHub "Approve and deploy" button, which on
# its own makes the publish job fail before anything is tagged.
#
# It shows what the run will publish (tag, commit, job results, and the staged
# assets, whose checksums it re-checks locally), asks you to type the tag, signs
# the approval with your YubiKey (PIN, then touch), and approves the `release`
# environment with the signature as the approval comment.
#
# See docs/release-approval.md for one-time setup and troubleshooting.
#
# Usage:
#   scripts/approve-release.sh <run-id or run URL> [options]
#
# Options:
#   --key PATH         security-key handle (default ~/.ssh/id_ed25519_sk_zakura_release)
#   --repo OWNER/NAME  default zakura-core/zakura (or $REPOSITORY)
set -euo pipefail

REPO="${REPOSITORY:-zakura-core/zakura}"
KEY="${HOME}/.ssh/id_ed25519_sk_zakura_release"
RUN_ID=""
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
approval="${repo_root}/.github/scripts/release_approval.py"

die() { printf 'ERROR   %s\n' "$*" >&2; exit 1; }
info() { printf 'INFO    %s\n' "$*"; }

# sha256 <file...>: prints `<hex>  <file>` on both Linux and macOS.
sha256() {
  if command -v sha256sum >/dev/null; then sha256sum "$@"; else shasum -a 256 "$@"; fi
}

while [ $# -gt 0 ]; do
  case "$1" in
    --key) KEY="$2"; shift 2 ;;
    --repo) REPO="$2"; shift 2 ;;
    -h | --help) sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    -*) die "unknown option $1 (see --help)" ;;
    *) RUN_ID="${1##*/runs/}"; RUN_ID="${RUN_ID%%/*}"; shift ;;
  esac
done
[ -n "$RUN_ID" ] || die "pass the Create release run id or URL (see --help)"
[[ "$RUN_ID" =~ ^[0-9]+$ ]] || die "not a run id: ${RUN_ID}"
for cmd in gh jq python3; do
  command -v "$cmd" >/dev/null || die "missing required tool: $cmd"
done
[ -f "$KEY" ] || die "no security-key handle at ${KEY}; see docs/release-approval.md (or pass --key)"
gh auth status >/dev/null 2>&1 || die "gh is not signed in; run: gh auth login"

run=$(gh api "repos/${REPO}/actions/runs/${RUN_ID}") || die "cannot read run ${RUN_ID} in ${REPO}"
[ "$(jq -r '.path' <<<"$run")" = ".github/workflows/create-release.yml" ] \
  || die "run ${RUN_ID} is not a Create release run"
[ "$(jq -r '.status' <<<"$run")" = "waiting" ] \
  || die "run ${RUN_ID} is $(jq -r '.status' <<<"$run"), not waiting for approval"
title=$(jq -r '.display_title' <<<"$run")
tag="${title#Create release }"
[ "$tag" != "$title" ] || die "cannot read the tag from the run title '${title}'"
commit=$(jq -r '.head_sha' <<<"$run")

env_id=$(gh api "repos/${REPO}/actions/runs/${RUN_ID}/pending_deployments" \
  --jq '.[] | select(.environment.name == "release") | .environment.id')
[ -n "$env_id" ] || die "run ${RUN_ID} is not waiting on the release environment"
can_approve=$(gh api "repos/${REPO}/actions/runs/${RUN_ID}/pending_deployments" \
  --jq '.[] | select(.environment.name == "release") | .current_user_can_approve')
[ "$can_approve" = "true" ] \
  || die "your GitHub account is not a reviewer on the release environment"

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

sha256sums=none
if gh api "repos/${REPO}/actions/runs/${RUN_ID}/artifacts" \
    --jq '.artifacts[].name' | grep -qx staged-release-assets; then
  info "downloading the staged release assets to re-check their checksums..."
  gh run download "$RUN_ID" --repo "$REPO" --name staged-release-assets --dir "${workdir}/assets"
  (cd "${workdir}/assets" && sha256 -c SHA256SUMS.txt >/dev/null) \
    || die "the staged assets do not match their SHA256SUMS.txt; do not approve"
  sha256sums=$(sha256 "${workdir}/assets/SHA256SUMS.txt" | cut -d' ' -f1)
fi

cat <<EOF

Create release run ${RUN_ID}: $(jq -r '.html_url' <<<"$run")
  started by  $(jq -r '.actor.login' <<<"$run") from $(jq -r '.head_branch' <<<"$run")
  tag         ${tag}
  commit      ${commit}
              $(gh api "repos/${REPO}/commits/${commit}" --jq '.commit.message | split("\n")[0]')

Jobs:
$(gh api "repos/${REPO}/actions/runs/${RUN_ID}/jobs" --jq '.jobs[] | "  \(.conclusion // .status)\t\(.name)"')

EOF
if [ "$sha256sums" = none ]; then
  printf 'No staged assets: this is a source-first release, so you approve the tag and commit only.\n\n'
else
  printf 'Staged assets (checksums re-checked locally):\n'
  sed 's/^/  /' "${workdir}/assets/SHA256SUMS.txt"
  printf '\n'
fi

printf 'You are signing:\n'
python3 -I "$approval" message --repository "$REPO" --tag "$tag" --commit "$commit" \
  --sha256sums "$sha256sums" | sed 's/^/  /'
printf '\nType the tag to approve it, or anything else to stop: '
read -r typed
[ "$typed" = "$tag" ] || die "not approved"

info "signing: enter your security-key PIN, then touch the key (it times out after ~30s)"
signature=$(python3 -I "$approval" sign --key "$KEY" --repository "$REPO" --tag "$tag" \
  --commit "$commit" --sha256sums "$sha256sums") \
  || die "signing failed; check the key is plugged in and re-run (nothing was approved)"

gh api --method POST "repos/${REPO}/actions/runs/${RUN_ID}/pending_deployments" \
  -F "environment_ids[]=${env_id}" -f state=approved \
  -f "comment=zakura-release-approval ${signature}" >/dev/null
info "approved. The publish job verifies your signature, then tags and publishes ${tag}."
info "watch: gh run watch ${RUN_ID} --repo ${REPO}"
