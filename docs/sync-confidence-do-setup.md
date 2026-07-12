# Sync-confidence on DigitalOcean — one-time setup

The `Sync confidence` workflows run on ephemeral DigitalOcean droplets in `nyc3`
and store cached chain state in DO Spaces. Configure these once (and update the
state objects after a DB format-version bump by re-running the snapshots workflow).

## Secrets (Settings -> Secrets and variables -> Actions -> Secrets)

- `DIGITALOCEAN_ACCESS_TOKEN` - a DO API token with read/write scope.
- `SPACES_ACCESS_KEY` / `SPACES_SECRET_KEY` - a Spaces access keypair.
- `DO_SSH_PRIVATE_KEY` - private key of a dedicated CI SSH keypair (PEM).

## Variables (... -> Variables)

- `SPACES_BUCKET` - the Space (bucket) name, created in `nyc3`.
- `DO_SSH_KEY_FINGERPRINT` - fingerprint of the public key, after registering it
  in DO (`doctl compute ssh-key import ci-key --public-key-file ci-key.pub`, then
  `doctl compute ssh-key list`).

## Seed the snapshots (one-time, and after a DB format-version bump)

1. On a host with ~750 GB-1 TB of free disk and the Zebra build deps, run
   `.github/workflows/scripts/make-sync-confidence-snapshots.sh` (point `SNAP_URL`
   at a recent full mainnet snapshot, `REPO` at this checkout, and configure
   `s3cmd` for the destination Space). It rewinds the snapshot to each window
   start, prunes, and uploads `pre-nu62`/`post-nu62` tarballs to the Space.
2. Make the GHCR image public: GitHub -> the org's **Packages** -> `zakura-tests`
   -> Package settings -> Change visibility -> **Public** (created on the first
   `Sync confidence` run; droplets pull it anonymously).

## Running

> **Currently disabled.** The `push` and `schedule` triggers below are commented out in
> `sync-confidence.yml` until `main` ships DB format 28.3.0 and matching pruned
> snapshots (the snapshots are 28.3.0, so a `main` image built before then would
> `FormatMismatch` on open). Only manual dispatch is active. Re-enable by restoring the
> `push` and `schedule` blocks in `sync-confidence.yml`.

Once the snapshots exist and the package is public, **Sync confidence**:

- **on merge to `main`** rebuilds and publishes the `main` test image
  to GHCR — no sync test runs, so merges stay cheap;
- **every 12 hours (schedule)** reuses that `main` image (no rebuild) and syncs
  both windows;
- **on manual dispatch** builds the image from the dispatched ref and syncs both windows
  by default; uncheck `build_image` to reuse the `main` image instead.

Each window restores its pruned tarball and syncs its 5k-block range. A red means a
sync/consensus regression or a broken DB-format migration — the consumer runs the image's
migration when opening the pruned state, and the error distinguishes the two
(`FormatMismatch`/migration panic vs. a verification failure).

## Cost note

Droplets are deleted after each run (`if: always()` + a 1h orphan sweep). If a run
is force-killed, check `doctl compute droplet list --tag-name sync-confidence-ci`.
