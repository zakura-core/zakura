# Release-state publisher

Publishes Mainnet release-state bundles — the coupled checkpoint list, VCT
frontier, completed-subtree roots, and historical frontier grid — from an
archive host to R2, where the `update-release-state.yml` workflow imports them
into reviewable draft PRs.
Design: `docs/design/verified-commitment-trees.md`, section 16.
Production host wiring, operations, and rollback:
[`SNAPSHOT_HOST.md`](SNAPSHOT_HOST.md).

## What runs where

- **This host (archive node):** `publish-release-state.sh <archive-cache-dir>`
  runs the offline export and uploads one immutable bundle
  (`meta.json`, `main-checkpoints.txt`, `mainnet-frontier.bin`,
  `mainnet-treestate-subtrees.bin`, and `mainnet-frontier-grid.bin`),
  then atomically replaces `release-state/latest.json`. Bundles are retained
  newest-4 by default (`RELEASE_STATE_KEEP`).
- **GitHub (repository):** the workflow resolves `latest.json` over a pinned
  HTTPS host, verifies every digest, and opens a draft PR. Humans review and
  merge; releases build only committed source.

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

This change makes the publisher emit `mainnet-frontier-grid.bin`. Import and provenance checks that
_require_ the fourth file land in a follow-up, so today's importer still accepts a three-file
bundle and a four-file one alike. Deploy when convenient after merge:

1. Merge this repository change.
2. Run `deploy-snapshot-host.sh`, now buildable from `main`.
3. Set `RELEASE_STATE_ARCHIVE_CACHE` and enable the timer.

Until the import follow-up merges, scheduled imports ignore the new grid file. Once that follow-up
lands, prefer deploying the publisher the same day so the first required four-file bundle already
exists.

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
