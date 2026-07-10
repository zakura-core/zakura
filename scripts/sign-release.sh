#!/usr/bin/env bash
# Sign a published release's SHA256SUMS.txt with the maintainer's minisign key
# and upload the detached signature as a release asset.
#
# Run on a trusted machine that holds the minisign secret key:
#   ./scripts/sign-release.sh v1.0.0
#
# Environment:
#   REPOSITORY    GitHub repository (default: zakura-core/zakura)
#   MINISIGN_KEY  Path to the minisign secret key (default: minisign's default)
set -euo pipefail

# Must match the key published in VERIFY.md; the signature is verified against
# this before upload so a signature from the wrong key never ships.
MAINTAINER_PUBKEY="RWTZkHOmfhxdQf43RZJyOawUNvMSlbPH539O9Y2Sir/ZHTihqnSO1RZn"
REPOSITORY="${REPOSITORY:-zakura-core/zakura}"

if [ $# -ne 1 ]; then
  echo "Usage: $0 <release-tag>" >&2
  exit 1
fi
RELEASE_TAG="$1"

for cmd in gh minisign; do
  command -v "$cmd" >/dev/null || { echo "Missing required tool: $cmd" >&2; exit 1; }
done

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

echo "Fetching asset list for ${RELEASE_TAG}..."
gh api "repos/${REPOSITORY}/releases/tags/${RELEASE_TAG}" \
  --jq '.assets[] | "\(.name) \(.digest // "-")"' > "${workdir}/assets.txt"

gh release download "$RELEASE_TAG" --repo "$REPOSITORY" \
  --pattern SHA256SUMS.txt --dir "$workdir"

# Cross-check SHA256SUMS.txt against the digests GitHub reports for the
# uploaded assets, so we never sign sums that don't match what users download.
fail=0
while read -r name digest; do
  case "$name" in
    SHA256SUMS.txt | *.minisig) continue ;;
  esac
  expected="$(awk -v f="./${name}" '$2 == f {print $1}' "${workdir}/SHA256SUMS.txt")"
  if [ -z "$expected" ]; then
    echo "ERROR: asset ${name} is not listed in SHA256SUMS.txt" >&2
    fail=1
  elif [ "$digest" = "-" ]; then
    echo "WARNING: GitHub reports no digest for ${name}; cannot cross-check" >&2
  elif [ "sha256:${expected}" != "$digest" ]; then
    echo "ERROR: digest mismatch for ${name}: sums=${expected} github=${digest#sha256:}" >&2
    fail=1
  fi
done < "${workdir}/assets.txt"

# Entries listed in the sums file that were never uploaded to the release.
while read -r _ path; do
  name="${path#./}"
  if ! awk -v n="$name" '$1 == n {found = 1} END {exit !found}' "${workdir}/assets.txt"; then
    echo "WARNING: SHA256SUMS.txt lists ${name}, which is not an uploaded asset" >&2
  fi
done < "${workdir}/SHA256SUMS.txt"

if [ "$fail" -ne 0 ]; then
  echo "Refusing to sign ${RELEASE_TAG}: fix the release assets first (workflow_dispatch with publish_assets_to_release)." >&2
  exit 1
fi

sign_args=(-Sm "${workdir}/SHA256SUMS.txt" -t "zakura ${RELEASE_TAG} SHA256SUMS.txt")
if [ -n "${MINISIGN_KEY:-}" ]; then
  sign_args+=(-s "$MINISIGN_KEY")
fi
minisign "${sign_args[@]}"

# Refuse to upload anything the published maintainer key can't verify.
minisign -Vm "${workdir}/SHA256SUMS.txt" -P "$MAINTAINER_PUBKEY"

gh release upload "$RELEASE_TAG" "${workdir}/SHA256SUMS.txt.minisig" \
  --repo "$REPOSITORY" --clobber

echo
echo "Uploaded SHA256SUMS.txt.minisig to ${RELEASE_TAG}."
echo "Users verify with:"
echo "  minisign -Vm SHA256SUMS.txt -P ${MAINTAINER_PUBKEY}"
