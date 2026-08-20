# Production release-state publisher host

## Purpose and topology

The production release-state publisher generates a trusted, coupled Mainnet checkpoint list, VCT
frontier, completed-subtree artifact, and historical frontier grid from Zakura's finalized
**archive** database. It runs on the production snapshot host against:

- container: `zakura` (the archive node, not `zakura-pruned`)
- cache: the archive container's cache directory, supplied as
  `RELEASE_STATE_ARCHIVE_CACHE` in `/etc/zakura-release-state.env`
- R2 endpoint:
  `https://152e2a8834283136c2f0575782b1b7aa.r2.cloudflarestorage.com`
- bucket: `zakura-release-state`
- public prefix:
  `https://zakura-release.valargroup.dev/release-state`

The exporter is its own `zakura-release-state.timer`, not a phase of any snapshot job. Two
changes made that possible, and one made it necessary:

- The exporter opens the state database as a read-only RocksDB **secondary**, so it reads a
  consistent view of a running node. It never needed the stopped-node window for correctness,
  only for convenience.
- It therefore takes no snapshot lock and cannot delay a snapshot. Its own
  `/run/zakura-release-state-publish.lock` still rejects overlapping timer or manual runs.
- The frontier grid covers the heights below the checkpoint, which the pruned database no
  longer holds. That is why the source moved from `zakura-pruned` to `zakura`.

Grid generation cost depends on which kind of archive node the cache belongs to. A **legacy**
archive node — one that never fast-synced — has per-height trees everywhere, so every entry is a
read and no note commitment is ever re-appended. A fast-synced archive node has to replay its
absent band instead, which is hours on Mainnet.

Neither is quick. The default cost-weighted spacing decides where entries go by scanning every
block body from genesis for its commitment counts, so both kinds of node read the whole chain
once per run; the legacy node just skips the append work on top. `TimeoutStartSec=6h` on the unit
covers the slower case. Measure one run before trusting the daily schedule, and consider
`RELEASE_STATE_GRID_COST_MS` or a longer `OnCalendar` interval if it does not comfortably fit.

The existing automation remains in place and is untouched by this publisher:

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

The script builds `zakura-checkpoints` with `zakura-checkpoints-offline` at the commit checked out
in the trusted source tree. It verifies that commit is on `origin/main`, then installs the binary
and publisher scripts under `/opt/zakura-release-state`, removes any leftover
`zakura-snapshot-pruned.service` drop-in from the previous topology, installs
`zakura-release-state.{service,timer}`, runs `systemctl daemon-reload`, and verifies the units. It
refuses to install while any publisher is active, does not restart either live node, and does not
enable the timer.

Then supply the archive cache path and enable the timer:

```sh
printf 'RELEASE_STATE_ARCHIVE_CACHE=%s\n' /path/to/archive-cache \
  > /etc/zakura-release-state.env
chmod 0644 /etc/zakura-release-state.env
systemctl enable --now zakura-release-state.timer
```

The publisher refuses to run without `RELEASE_STATE_ARCHIVE_CACHE`. There is deliberately no
default: falling back to the pruned cache would publish a bundle the grid export cannot complete.

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

Normal and manual publication both use the publisher service:

```sh
systemctl start zakura-release-state.service
journalctl -fu zakura-release-state.service
```

The run reads the archive cache through a secondary and touches no container, so nothing has to
be stopped or restarted around it. Expected journal order is: checkpoint selection, the frontier
and subtree artifacts, the frontier grid (with periodic entry progress), then the bundle upload
and pointer replacement.

After a run:

```sh
systemctl is-active zakura-release-state.service
docker inspect --format '{{.State.Running}}' zakura
curl -fsS https://zakura-release.valargroup.dev/release-state/latest.json | jq .
```

Confirm the pointer height advanced, fetch its `meta_url`, verify
`meta_sha256`, and verify the listed size and SHA-256 for
`main-checkpoints.txt`, `mainnet-frontier.bin`, `mainnet-treestate-subtrees.bin`, and
`mainnet-frontier-grid.bin`. The publisher keeps the newest four immutable
`release-state/v1/<height>/` bundles by default.

Direct invocation is available for diagnostics. It takes the same publisher lock as the timer,
so it is safe alongside a scheduled run, and it leaves both containers alone:

```sh
/usr/local/bin/with-secrets.sh \
  /opt/zakura-release-state/bin/publish-from-archive-host.sh \
  /path/to/archive-cache
```

## CI consumers

In `zakura-core/zakura`:

- `.github/workflows/update-release-state.yml` performs weekly or manual
  digest-verified fetch, append-only checkpoint validation, the release-state
  gate, frontier and Sprout tests, a strict diff allowlist, then mints a
  short-lived GitHub App token and opens a signed draft
  `adam/update-release-state` PR.
- `.github/workflows/tests-unit.yml` includes the checkpoint, frontier, and
  provenance paths, so generated update PRs run Unit Tests. A follow-up adds the
  frontier-grid path once the binary is committed.
- `.github/workflows/create-release.yml` and `scripts/make/release.mk` validate only
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

Disable only the publisher timer:

```sh
systemctl disable --now zakura-release-state.timer
systemctl daemon-reload
```

Do not remove or disable either snapshot timer, and do not delete the publisher script during an
incident. Keep bootstrap and current R2 objects intact, so GitHub safely takes the no-op path
when no newer bundle is available. A publisher run mutates nothing on the host beyond its own
lock file and a temporary staging directory, so an interrupted run needs no container recovery.
Preserve the service journal.
