# Release-state publisher

Publishes Mainnet release-state bundles — the coupled checkpoint list and VCT
frontier — from the snapshot host to R2, where the
`update-release-state.yml` workflow imports them into reviewable draft PRs.
Design: `docs/design/verified-commitment-trees.md`, section 16.
Production host wiring, operations, and rollback:
[`SNAPSHOT_HOST.md`](SNAPSHOT_HOST.md).

## What runs where

- **This host (snapshot service):** while the snapshot job holds its lock and
  has stopped its synced Mainnet node,
  `publish-release-state.sh <stopped-node-cache-dir>` runs
  the offline export and uploads one immutable bundle
  (`release-state/v1/<height>/{meta.json, main-checkpoints.txt, mainnet-frontier.bin}`),
  then atomically replaces `release-state/latest.json`. Bundles are retained
  newest-4 by default (`RELEASE_STATE_KEEP`).
- **GitHub (repository):** the workflow resolves `latest.json` over a pinned
  HTTPS host, verifies every digest, and opens a draft PR. Humans review and
  merge; releases build only committed source.

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

3. Hook the script into the snapshot job with its environment, e.g.:

   ```sh
   RELEASE_STATE_R2_REMOTE=r2:zakura-artifacts \
   RELEASE_STATE_PUBLIC_BASE=https://zakura-release.valargroup.dev/release-state \
   /opt/zakura/publish-release-state.sh /mnt/data/stopped-node-zakura-cache
   ```

4. Set the repository variable `MAINNET_RELEASE_STATE_LATEST_URL` to
   `<RELEASE_STATE_PUBLIC_BASE>/latest.json`.

## Invariants the script maintains

- Bundle directories are immutable: an existing height is only ever reused
  when its data-file digests match the fresh export byte-for-byte (a same-state
  re-run); different contents at the same height abort loudly.
- Data files upload before `meta.json`, and `latest.json` moves last, so a
  partial upload is never resolvable.
- A host-local lock serializes export, upload, pointer replacement, and
  retention. Run exactly one publisher host; multiple hosts require
  object-store conditional writes rather than the local lock.
- Exports continue the deterministic checkpoint selection grid from the
  binary's embedded list. Never hand-edit the Mainnet checkpoint file or
  publish RPC-mode Mainnet output: off-grid lines make every later bundle fail
  the workflow's byte-for-byte prefix check.

## Failure modes

- `state tip ... is not above the last checkpoint`: the stopped-node state
  predates the embedded checkpoint list of the installed tool; sync further
  or update the tool.
- `Sprout note commitments were appended at ...`: a v4 JoinSplit landed just
  below the state tip; the next day's export self-heals once the checkpoint
  grid passes that block.
- `existing bundle at this height has different contents`: determinism broke
  (or the bucket was tampered with) — investigate before deleting anything.
- `another release-state publisher is already running`: wait for the active
  snapshot/manual publication to finish before retrying.
