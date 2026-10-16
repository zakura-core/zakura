# Release-state publisher

Publishes Mainnet release-state bundles — the coupled checkpoint list, VCT
frontier, completed-subtree roots, and historical frontier grid — from an
archive host to R2. The daily `update-release-state.yml` workflow publishes
complete sets; `adopt-release-state.yml` imports them into reviewable draft PRs.
Design: `docs/design/verified-commitment-trees.md`, section 16.
Production host wiring, operations, and rollback:
[`SNAPSHOT_HOST.md`](SNAPSHOT_HOST.md).

## Where the publisher runs

Selected by `RELEASE_STATE_HOST_PROFILE` when running `deploy-snapshot-host.sh`:
`snapshot` (default, the containerised host) or `archive`
(`roman-zakura-archive-vct-off`, the supported generator). The archive node runs
`vct_fast_sync = false`, so the frontier grid comes from reads rather than replaying
an absent band, and it shares its host with no snapshot job.

Nothing in the publisher needs a container runtime. `publish-release-state.sh` has no
docker in it at all: the exporter is a plain binary reading the cache as a read-only
RocksDB secondary. Docker appears only as a liveness hint on the `snapshot` profile,
because that host happens to run its node in a container; the `archive` profile checks
`zakurad.service` instead, and the unit only `Wants=docker.service` so it starts on a
host with none.

## What runs where

- **This host (archive node):** `publish-release-state.sh <archive-cache-dir>`
  runs the offline export and uploads one immutable bundle
  (`meta.json`, `main-checkpoints.txt`, `mainnet-frontier.bin`,
  `mainnet-treestate-subtrees.bin`, and `mainnet-frontier-grid.bin`),
  then atomically replaces `release-state/latest.json`. Bundles are retained
  newest-4 by default (`RELEASE_STATE_KEEP`).
- **GitHub (repository):** daily publication verifies one candidate and its
  matching grid crate before advertising a complete ready set. A separate
  weekly/manual workflow imports ready sets into draft PRs. Humans review
  and merge; releases build committed source and exact locked dependencies.

  The grid is published rather than committed because it is regenerated on
  every refresh at ~2.1 MB, which the repository would carry in its history
  forever. Cargo can only resolve a version that already exists, so the
  publish necessarily precedes the PR that pins it. Unadopted versions remain
  available so published sets keep resolving. See
  `docs/design/historical-treestate-serving.md` §5.

## Daily publication and reviewed adoption

`update-release-state.yml` keeps the existing crates.io publishing identity and
runs daily at 07:00 UTC or by manual dispatch. It verifies one candidate,
publishes or verifies the matching grid crate, confirms Cargo resolution,
and promotes the complete set into `release-state/published/v1/<height>/`.
Only then does it conditionally advance `release-state/published/latest.json`.
Existing immutable objects must match byte for byte. Concurrent or delayed
runs cannot replace a newer pointer with an older one. Identical retries also
verify that the public ready pointer is readable and identifies the selected set.

The published prefix is retained without automatic expiry. The archive
exporter's four-candidate cleanup only visits `release-state/v1/`; it cannot
remove a ready set. Publication failures before the pointer commit leave the
previous ready set intact. A lost acknowledgement after the final commit can
leave the new complete set selected; retrying verifies and reuses it.

The ready pointer uses schema version 2: the original bundle identity plus an
`asset` record containing `name`, `version`, `checksum` (the crate archive), and
`grid_sha256`. Candidate pointers remain schema version 1. Consumers use
`MAINNET_RELEASE_STATE_READY_URL`, never fall back to the candidate pointer,
and verify the bundle and crate together. Checkpoint metadata must match the
final checkpoint height and hash before publication or consumption; checkpoint
format checks run before publication. Registry archives are bounded before and
after decompression, including their metadata headers. A set older than 48 hours is
rejected, including waived imports. Republishing does not reset generation
time. This age check does not independently measure the archive node's lag
behind the live chain.

`adopt-release-state.yml` runs Monday at 06:00 UTC or manually. It imports a
ready set, updates the exact dependency and lockfile, validates the result,
and opens a draft PR. Review and merge remain human decisions. Release
preparation consumes the same ready sets and no longer dispatches publication.
Branch dry runs are read-only with respect to publishing, storage, and PRs.

### Production qualification and activation

The schedules are controlled by repository activation variables. Complete
storage setup and qualification before enabling them. They may be configured
before merge after an authorized branch qualification; the new schedules only
run once these workflows reach `main`.

1. Use a dedicated object read/write credential from the generator's existing
   Infisical project/environment; do not copy its host credential. Restrict it
   to the existing `zakura-release-state` bucket where possible. Reusing an
   existing credential with broader bucket access needs explicit operator
   authorization.
2. Configure the `release-state-storage` GitHub environment with no per-run
   reviewer gate, deployment restricted to `main`, and only
   `RELEASE_STATE_R2_ACCESS_KEY_ID` and `RELEASE_STATE_R2_SECRET_ACCESS_KEY`.
   Keep crates.io authentication isolated in `crates-io-assets`.
3. Audit bucket lifecycle rules to exclude `release-state/published/` from
   expiry. Configure the custom domain to bypass caching for the published
   pointer and to avoid caching missing objects under the published prefix.
4. Qualify publication, registry availability, Cargo resolution, every public
   bundle file, and pointer advancement. Rerun to verify reuse. Authorized
   branch qualification keeps its temporary publishing exceptions separate
   from the PR and removes them afterward. After merge, inspect the first
   publication from `main` to confirm the merged configuration.
5. Set `MAINNET_RELEASE_STATE_READY_URL` to
   `https://zakura-release.valargroup.dev/release-state/published/latest.json`.
   Keep `MAINNET_RELEASE_STATE_LATEST_URL` pointing at the candidate pointer.
6. Run adoption and release-preparation dry runs against the ready set. Set
   `MAINNET_RELEASE_STATE_PUBLICATION_ENABLED=true` and
   `MAINNET_RELEASE_STATE_ADOPTION_ENABLED=true` only after qualification.

Local validation uses Python 3.12, the pinned client requirements in
`.github/scripts/requirements-release-state.txt`, and
`python -m unittest discover -s .github/scripts -p 'test_*release_state*.py'`.
The integration tests run actual Cargo publication, resolution, and lockfile
updates against a loopback registry. Promotion runs against an in-memory store
with S3 conditional-write semantics, and stubbed SDK calls check how storage
responses map onto those semantics. The tests require no production
credentials or uploads.
Real R2/CDN behavior, crates.io authentication, and the node's cryptographic
subtree verifier still require their separate live/CI qualification.

## Why an archive node, and why it need not be stopped

The exporter opens the state database as a read-only RocksDB secondary, so it
reads a consistent view of a _running_ node and does not need the stopped-node
window the pruned snapshot job used to provide.

It must be an archive node. Checkpoints, the frontier, and the subtree roots
come out of pruned state, but the frontier grid covers the heights below the
checkpoint, which a pruned database no longer holds. A **legacy** archive node
(one that never fast-synced) is the best generator: it has per-height trees
everywhere, so the grid is built from reads with no replay at all, and its
coverage follows its tip. A fast-synced archive node also works, but pays one
whole-band replay — hours on Mainnet.

## Exporter compatibility

The importer requires all four data files, including the historical frontier
grid. Older candidates without the grid are rejected before publication.
The existing archive exporter already produces this format; this change does
not require redeploying it. Follow the production qualification and activation
steps above for the new ready-set path.

## One-time host setup

1. Install the export tool from the release the fleet runs:

   ```sh
   cargo install --locked --features zakura-checkpoints-offline \
     --git https://github.com/zakura-core/zakura zakura-utils
   ```

2. Install `rclone` and `flock` (normally provided by `util-linux`), then
   configure an rclone remote with R2 credentials that can write the bucket
   (for example remote `r2`, bucket `zakura-artifacts`), and make sure the
   bucket's `release-state/` prefix is served on a public HTTPS domain that is
   in the fetch script's allowed-host list
   (`.github/scripts/fetch-release-state.py`).

3. Run the script from a timer with its environment, e.g.:

   ```sh
   RELEASE_STATE_R2_REMOTE=r2:zakura-artifacts \
   RELEASE_STATE_PUBLIC_BASE=https://zakura-release.valargroup.dev/release-state \
   /opt/zakura/publish-release-state.sh /var/lib/zakura/archive-cache
   ```

4. Set the repository variable `MAINNET_RELEASE_STATE_LATEST_URL` to
   `<RELEASE_STATE_PUBLIC_BASE>/latest.json`.

## Invariants the script maintains

- Bundle directories are immutable: an existing height is only ever reused
  when its data-file digests match the fresh export byte-for-byte (a same-state
  re-run); different contents at the same height abort loudly. Two runs that
  select the same checkpoint read the same chain prefix, so this holds even
  though the node keeps syncing underneath the secondary.
- Data files upload before `meta.json`, and `latest.json` moves last, so a
  partial upload is never resolvable.
- A host-local lock serializes export, upload, pointer replacement, and
  retention. Run exactly one publisher host; multiple hosts require
  object-store conditional writes rather than the local lock.
- Each run resumes the frontier grid from the previous bundle's copy, so it scans only the
  blocks above that grid's last entry. Carried entries are re-checked against the database
  before they are accepted, and are written out byte-for-byte, so a bundle is a prefix-extension
  of its predecessor by construction. If the previous bundle has no grid, the run falls back to
  a full walk from genesis and says so.
- Exports continue the deterministic checkpoint selection grid from the
  binary's embedded list. Never hand-edit the Mainnet checkpoint file or
  publish RPC-mode Mainnet output: off-grid lines make every later bundle fail
  the workflow's byte-for-byte prefix check.
- Every artifact in a bundle describes the checkpoint that run selected. The
  frontier grid in particular must not lag: a node whose fast-sync handoff is
  above the grid's checkpoint refuses to start with derivation enabled.

## Failure modes

- `state tip ... is not above the last checkpoint`: the exported state
  predates the embedded checkpoint list of the installed tool; sync further
  or update the tool.
- `cannot derive the note commitment tree at ...: block body ... is not
  retained`: the grid export ran against a pruned database. Point it at an
  archive one.
- `Sprout note commitments were appended at ...`: a v4 JoinSplit landed just
  below the state tip; the next day's export self-heals once the checkpoint
  grid passes that block.
- `existing bundle at this height has different contents`: determinism broke
  (or the bucket was tampered with) — investigate before deleting anything.
- `another release-state publisher is already running`: wait for the active
  timer or manual publication to finish before retrying.

## Updating the archive publisher

The mainnet fleet deployer marks `archive-vct-off` with
`release_state_publisher = true`. A deployment builds `zakurad` and the offline
`zakura-checkpoints` exporter at the same resolved commit. The exporter revision
must be on `origin/main`. Deployment tooling comes from the workflow revision,
so selecting an older release does not select an older deployment procedure.
Before changing the host, the deployer checks that the exporter supports the
publisher's required CLI options, including grid output, resume, and cost settings.
Older exporters that lack any of these options are rejected before installation.

The deployer pauses the timer, waits up to ten minutes for publication to finish,
and installs both binaries before restarting the node. It checks RPC and a
90-second settle window, runs publication, then verifies the public bundle's
height and digests. The existing publisher scripts, unit, profile, and credentials
remain host-managed. Use `deploy-snapshot-host.sh` for initial installation or
changes to those components.

`--no-restart` is rejected when the selection includes this host. Deploy the pair
together, or select another node for staging. A node startup failure leaves the
publisher timer stopped and fails the deployment. There is no automatic binary
rollback after startup because the database may already have migrated. Repair
with a compatible revision and retry the paired deployment. A marker under
`/opt/zakura-release-state/deploy.resume-timer` lets a successful retry resume a
timer that the failed deployment stopped. A persistent `deploy.paused` marker in
the same directory and a systemd service condition keep publication blocked across
reboots until the paired deployment validates the node. Remove the resume marker only if deliberately
leaving publication paused. A publication-only failure keeps the compatible pair
installed and restores the previous timer state so the next scheduled run can retry.
