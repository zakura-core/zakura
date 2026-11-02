# Verifying Zakura Releases

Release artifacts can be authenticated with independent checks. The security-key
approval and the maintainer signature are the stronger claims, since a human
signed off on the release. The build provenance attestation proves the
artifacts were built unmodified by this repository's CI.

## Security-key approval

Every release is approved by a maintainer's hardware security key before it is
published. The approval is attached to the release as `release-approval.txt`
(what was approved) and `release-approval.sig` (the signature). The allowed
keys are listed in [`.github/release-signers`](../.github/release-signers) at
the release tag, and CI only accepts keys whose Yubico attestation, in
`.github/release-attestations/`, proves they
live on a YubiKey.

You need `ssh-keygen` from OpenSSH 8.2 or newer and `git`, which macOS and
current Linux distributions already include. To verify an archive (example: linux-x86_64
for `v1.0.0`):

```sh
TAG=v1.0.0
# The commit the protected tag points to. Stop if this does not print one.
COMMIT="$(git ls-remote --tags --refs https://github.com/zakura-core/zakura | awk -v ref="refs/tags/${TAG}" '$2 == ref {print $1}')"
echo "$COMMIT" | grep -Ex '[0-9a-f]{40}'

BASE="https://github.com/zakura-core/zakura/releases/download/${TAG}"
curl -fsSLO "${BASE}/zakurad-${TAG}-linux-x86_64.tar.gz"
curl -fsSLO "${BASE}/SHA256SUMS.txt"
curl -fsSLO "${BASE}/release-approval.txt"
curl -fsSLO "${BASE}/release-approval.sig"
curl -fsSL -o release-signers "https://raw.githubusercontent.com/zakura-core/zakura/${COMMIT}/.github/release-signers"

# 1. A listed maintainer key signed the approval. Prints who signed it.
SIGNER="$(ssh-keygen -Y find-principals -f release-signers -s release-approval.sig)"
ssh-keygen -Y verify -f release-signers -I "$SIGNER" -n zakura-release \
  -s release-approval.sig < release-approval.txt

# 2. The approval names this repository, tag, commit, and checksums file.
printf 'zakura-release-approval v1\nrepository zakura-core/zakura\ntag %s\ncommit %s\nsha256sums %s\n' \
  "$TAG" "$COMMIT" "$(sha256sum SHA256SUMS.txt | cut -d' ' -f1)" \
  | diff - release-approval.txt && echo "Approval matches this release"

# 3. The archive matches the approved checksums.
sha256sum -c --ignore-missing SHA256SUMS.txt
```

Each step must succeed. The commit check prints the 40-character commit; if it
prints nothing, the tag lookup failed, so stop. Step 1 prints `Good "zakura-release" signature for
<signer>`, step 2 prints `Approval matches this release`, and step 3 prints
`OK`. On macOS, replace `sha256sum` with `shasum -a 256`.

Releases made in emergency source-first mode are approved before their
binaries exist, so their approval reads `sha256sums none` and covers only the
tag and commit. For those, use `none` in place of the checksum in step 2, and
check the archives with the build provenance command below, which pins the
same commit.

Only releases published after this check was added carry these files.

## Maintainer signature (minisign)

`SHA256SUMS.txt` on every full release is signed by the Zakura lead
maintainer's [minisign](https://jedisct1.github.io/minisign/) key:

```
RWTZkHOmfhxdQf43RZJyOawUNvMSlbPH539O9Y2Sir/ZHTihqnSO1RZn
```

Pre-releases (tags containing a hyphen, such as `v1.0.0-rc0`) may not carry a
maintainer signature.

To verify an archive (example: linux-x86_64 for `v1.0.0`):

```sh
TAG=v1.0.0
BASE="https://github.com/zakura-core/zakura/releases/download/${TAG}"
curl -fsSLO "${BASE}/zakurad-${TAG}-linux-x86_64.tar.gz"
curl -fsSLO "${BASE}/SHA256SUMS.txt"
curl -fsSLO "${BASE}/SHA256SUMS.txt.minisig"

# 1. The checksums file is signed by the maintainer key.
minisign -Vm SHA256SUMS.txt -P RWTZkHOmfhxdQf43RZJyOawUNvMSlbPH539O9Y2Sir/ZHTihqnSO1RZn

# 2. The archive matches the signed checksums.
sha256sum -c --ignore-missing SHA256SUMS.txt
```

`--ignore-missing` is needed because `SHA256SUMS.txt` covers every release
asset and you typically download only one platform's archive. On macOS, use
`shasum -a 256 -c --ignore-missing SHA256SUMS.txt`.

The signature's trusted comment names the release tag (for example
`zakura v1.0.0 SHA256SUMS.txt`); check that it matches the tag you
downloaded so a valid signature from one release cannot be replayed on
another.

## Build provenance (GitHub attestation)

CI attests every release asset with
[GitHub artifact attestations](https://docs.github.com/en/actions/security-for-github-actions/using-artifact-attestations).
With the [GitHub CLI](https://cli.github.com/) installed:

```sh
TAG=v1.0.0
COMMIT="$(git ls-remote --tags --refs https://github.com/zakura-core/zakura | awk -v ref="refs/tags/${TAG}" '$2 == ref {print $1}')"
echo "$COMMIT" | grep -Ex '[0-9a-f]{40}' &&
gh attestation verify "zakurad-${TAG}-linux-x86_64.tar.gz" --repo zakura-core/zakura \
  --source-digest "$COMMIT" \
  --signer-workflow zakura-core/zakura/.github/workflows/release-binaries.yml \
  && echo "Provenance OK"
```

This proves the file was built by the `release-binaries` workflow in this
repository at the tagged commit, and has not been modified since. Without
`--source-digest` and `--signer-workflow`, the command only checks the
repository. For releases from before the security-key approval, the workflow
checked out the tag by name, so a branch with the same name could have been
built instead; their attestation does not rule that out.

## Docker images

The Docker images on Docker Hub (`zakuracore/zakura`) are not yet signed;
verify the binary archives above, or build images from source. Image signing
is planned follow-up work.
