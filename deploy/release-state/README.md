# Release-state publisher

Publishes Mainnet release-state bundles — the coupled checkpoint list, VCT
frontier, completed-subtree roots, and historical frontier grid — from an
archive host to R2, where the `update-release-state.yml` workflow imports them
into reviewable draft PRs.
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
- **GitHub (repository):** the workflow resolves `latest.json` over a pinned
  HTTPS host, verifies every digest, publishes the bundle's frontier grid to
  crates.io, and opens a draft PR that imports the other three artifacts and
  repins the grid. Humans review and merge; releases build only committed
  source and the exact versions the lockfile pins.

  The grid is published rather than committed because it is regenerated on
  every refresh at ~2.1 MB, which the repository would carry in its history
  forever. Cargo can only resolve a version that already exists, so the
  publish necessarily precedes the PR that pins it; a candidate the reviewer
  rejects is a yanked, unreferenced version. See
  `docs/design/historical-treestate-serving.md` §5.

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

## Cutover order

The importer requires every bundle to carry all four files and fails closed on one that does not,
so the publisher has to be updated around the same time as the repository. It cannot be updated
_first_: `deploy-snapshot-host.sh` refuses to install an exporter whose revision is not already an
ancestor of `origin/main`, which is a supply-chain control worth keeping.

So the order is:

1. Merge the repository change.
2. Run `deploy-snapshot-host.sh`, now buildable from `main`.
3. Set `RELEASE_STATE_ARCHIVE_CACHE` and enable the timer.
4. Dispatch `update-release-state.yml` once the first new bundle exists.

Between 1 and 3 a scheduled import will fail with `meta.files is missing keys:
mainnet-frontier-grid.bin`. Nothing is committed and nothing in production changes — it is a red
scheduled job that clears as soon as the new publisher publishes — but prefer merging and
deploying the same day over leaving it red for a week.

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
