#!/usr/bin/env bash
# enroll-release-key.sh — create your release-approval key on a YubiKey and add
# it to .github/release-signers together with the YubiKey's attestation, which
# proves the key was generated inside genuine Yubico hardware.
#
# Asks for the YubiKey's FIDO2 PIN and a touch. Plug in only the YubiKey you
# are enrolling. Run it once per YubiKey: a backup key gets its own handle
# path. Afterwards, commit the changed files and open a PR. See
# docs/release-approval.md.
#
# Usage:
#   scripts/enroll-release-key.sh <github-username> [--key PATH]
#
#   For example: scripts/enroll-release-key.sh ebfull
#   Backup key:  scripts/enroll-release-key.sh ebfull --key ~/.ssh/id_ed25519_sk_zakura_release_backup
#
# Options:
#   --key PATH  where to write the key handle
#               (default ~/.ssh/id_ed25519_sk_zakura_release; must not exist)
set -euo pipefail

KEY="${HOME}/.ssh/id_ed25519_sk_zakura_release"
LOGIN=""
repo_root="$(cd "$(dirname "$0")/.." && pwd)"

die() { printf 'ERROR   %s\n' "$*" >&2; exit 1; }

while [ $# -gt 0 ]; do
  case "$1" in
    --key) KEY="$2"; shift 2 ;;
    -h | --help) sed -n '2,19p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    -*) die "unknown option $1 (see --help)" ;;
    *) LOGIN="$1"; shift ;;
  esac
done
[[ "$LOGIN" =~ ^[A-Za-z0-9-]+$ ]] || die "pass your GitHub username, for example: $0 ebfull"
[ ! -e "$KEY" ] || die "${KEY} already exists; for a backup YubiKey pass --key ${KEY}_backup, or move it aside to replace it"

# macOS's bundled ssh-keygen has no FIDO support.
keygen=ssh-keygen
[ -x /opt/homebrew/opt/openssh/bin/ssh-keygen ] && keygen=/opt/homebrew/opt/openssh/bin/ssh-keygen

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT
# The challenge only has to be reproducible so CI can check the attestation.
printf 'zakura-release-attestation:%s' "$LOGIN" > "${workdir}/challenge"

echo "Creating your release key: enter the YubiKey PIN, then touch the key."
"$keygen" -t ed25519-sk -O resident -O verify-required \
  -O application=ssh:zakura-release -O challenge="${workdir}/challenge" \
  -O write-attestation="${workdir}/attestation" \
  -C "$LOGIN" -N '' -f "$KEY"

signers="${repo_root}/.github/release-signers"
line="${LOGIN} namespaces=\"zakura-release\" $(cut -d' ' -f1,2 "${KEY}.pub")"
attestation_dir="${repo_root}/.github/release-attestations"
attestation_name=$(python3 -I "${repo_root}/.github/scripts/release_approval.py" attestation-name \
  --signer-line "$line")
mkdir -p "$attestation_dir"
cp "${workdir}/attestation" "${attestation_dir}/${attestation_name}"
# Keep the new key on its own line even if the file lacks a final newline.
[ -z "$(tail -c 1 "$signers")" ] || printf '\n' >> "$signers"
printf '%s\n' "$line" >> "$signers"

python3 -I "${repo_root}/.github/scripts/release_approval.py" check-signers \
  --signers "$signers" --attestations "$attestation_dir" \
  --yubico-ca "${repo_root}/.github/yubico-ca" \
  || die "the new key failed the YubiKey attestation check; is this a genuine YubiKey?"

cat <<EOF

Done. Your key handle is ${KEY}.
Commit and open a PR with:
  .github/release-signers
  .github/release-attestations/${attestation_name}
EOF
