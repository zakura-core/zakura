# Production snapshot-host publisher

## Purpose and topology

The production release-state publisher generates a trusted, coupled Mainnet
checkpoint list and VCT frontier from Zakura's finalized pruned database. It
runs on the production snapshot host against:

- container: `zakura-pruned`
- cache: `/mnt/zec_snapshot/zakura-cache-pruned`
- R2 endpoint:
  `https://152e2a8834283136c2f0575782b1b7aa.r2.cloudflarestorage.com`
- bucket: `zakura-release-state`
- public prefix:
  `https://zakura-release.valargroup.dev/release-state`

The exporter is a stopped-node phase of `zakura-snapshot-pruned.service`, after
that service stops `zakura-pruned` and before it starts the `tar | zstd`
archive pipeline. It is not a separate timer. This ordering gives the exporter
a consistent RocksDB view and keeps the existing parent lock,
`/run/zakura-snapshot-pruned-publish.lock`, held for the entire operation.
The exporter also retains its own
`/run/zakura-release-state-publish.lock`, which rejects duplicate hook or
manual invocations. It never takes the independent archive publisher lock.

The existing automation remains in place:

- `zakura-snapshot-pruned-check.timer` runs at minute `:17` with up to 120
  seconds of randomized delay.
- `zakura-snapshot-pruned-check.service` performs the age and alert check.
- `zakura-snapshot-pruned.service` publishes the pruned snapshot.
- `zakura-snapshot-check.timer`, `zakura-snapshot-check.service`, and
  `zakura-snapshot.service` continue to manage archive snapshots independently.

## Install and configure

From a trusted checkout of this repository:

```sh
deploy/release-state/deploy-snapshot-host.sh <snapshot-host-ssh-target>
```

The script builds `zakura-checkpoints` with
`zakura-checkpoints-offline` from pinned Mainnet commit
`d1fed3e6e0e420571ecacb9e1984dea6353cc7a3`, installs the binary and
publisher scripts under `/opt/zakura-release-state`, installs the
`zakura-snapshot-pruned.service` drop-in, runs `systemctl daemon-reload`, and
verifies the unit. It refuses to install while either snapshot publisher is
active and does not restart either live node.

The host's existing Infisical Universal Auth identity uses project
`c57a6889-6a7c-4d05-a54a-e4a4c0b14ee7`, environment `prod`.
`/usr/local/bin/with-secrets.sh` already injects `R2_ACCESS_KEY_ID` and
`R2_SECRET_ACCESS_KEY` for snapshot publication. The hook reuses that pair,
which has been verified against `zakura-release-state`, and converts it to an
environment-only rclone remote; there is no rclone configuration file. Do not
install the broad `CF_VALARGROUP_ZAKURA_STATE_PROV` control-plane token on
this host.

Verify presence without printing either value:

```sh
/usr/local/bin/with-secrets.sh bash -c '
  test -n "${R2_ACCESS_KEY_ID:-}"
  test -n "${R2_SECRET_ACCESS_KEY:-}"
'
```

## Operation and verification

Normal and manual publication both use the existing service:

```sh
systemctl start zakura-snapshot-pruned.service
journalctl -fu zakura-snapshot-pruned.service
```

Expected journal order is: stop `zakura-pruned`, run and complete the
release-state hook, archive the pruned state, restart `zakura-pruned`, then
verify and upload the snapshot. A hook failure or 30-minute timeout emits a
Sentry error tagged `alert:zakura_release_state` and `check:release_state`,
but snapshot creation continues and the parent EXIT trap still restarts the
container.

After a run:

```sh
systemctl is-active zakura-snapshot-pruned.service
docker inspect --format '{{.State.Running}}' zakura-pruned
curl -fsS https://zakura-release.valargroup.dev/release-state/latest.json | jq .
```

Confirm the pointer height advanced, fetch its `meta_url`, verify
`meta_sha256`, and verify the listed size and SHA-256 for
`main-checkpoints.txt` and `mainnet-frontier.bin`. The publisher keeps the
newest four immutable `release-state/v1/<height>/` bundles by default.

Direct hook invocation is an incident diagnostic only. First ensure both
snapshot publishers are inactive, stop `zakura-pruned`, invoke the hook
through `with-secrets.sh`, and always restart and verify the container:

```sh
/usr/local/bin/with-secrets.sh \
  /opt/zakura-release-state/bin/publish-from-snapshot-host.sh \
  /mnt/zec_snapshot/zakura-cache-pruned
```

## CI consumers

In `zakura-core/zakura`:

- `.github/workflows/update-release-state.yml` performs weekly or manual
  digest-verified fetch, append-only checkpoint validation, the release-state
  gate, frontier and Sprout tests, a strict diff allowlist, then mints a
  short-lived GitHub App token and opens a signed draft
  `adam/update-release-state` PR.
- `.github/workflows/tests-unit.yml` includes the checkpoint, frontier, and
  provenance paths, so generated update PRs run Unit Tests.
- `.github/workflows/create-release.yml` and `make/release.mk` validate only
  committed release state before creating a tag.
- `scripts/check-release-state.sh` is the non-Cargo release gate and rejects
  bootstrap provenance unless the documented emergency override is explicit.

## Credential rotation

Rotate the existing snapshot R2 credential in the same Infisical project and
environment, and run the injected-presence check above. Verify the replacement
can access both snapshot buckets and `zakura-release-state`, then trigger one
service run and verify both publication paths before revoking the old
credential. Never print, journal, or persist either secret.

## Rollback

Remove only the hook drop-in and reload systemd:

```sh
rm /etc/systemd/system/zakura-snapshot-pruned.service.d/release-state.conf
systemctl daemon-reload
systemd-analyze verify zakura-snapshot-pruned.service
```

Do not remove or disable either snapshot timer and do not delete the publisher
script during an incident. Keep bootstrap and current R2 objects intact, so
GitHub safely takes the no-op path when no newer bundle is available. If a run
was interrupted, verify `zakura-pruned` is running and syncing before closing
the incident. Preserve the service journal and dedicated Sentry event.
