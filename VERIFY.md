# Verifying Zakura Releases

Release artifacts can be authenticated with two independent checks. The
maintainer signature is the stronger claim — a human signed off on the
release. The build provenance attestation proves the artifacts were built
unmodified by this repository's CI.

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
gh attestation verify zakurad-v1.0.0-linux-x86_64.tar.gz --repo zakura-core/zakura
```

This proves the file was built by the `release-binaries` workflow in this
repository at the tagged commit, and has not been modified since.

## Docker images

The Docker images on Docker Hub (`zakuracore/zakura`) are not yet signed;
verify the binary archives above, or build images from source. Image signing
is planned follow-up work.
