# Release-state publisher

Publishes Mainnet release-state bundles — the coupled checkpoint list and VCT
frontier — from the snapshot host to R2, where the
`update-release-state.yml` workflow imports them into reviewable draft PRs.
Design: `docs/design/verified-commitment-trees.md`, section 16.

## What runs where

- **This host (snapshot service):** after the snapshot job quiesces its copy of
  a synced Mainnet state, `publish-release-state.sh <quiesced-cache-dir>` runs
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

2. Configure an rclone remote with R2 credentials that can write the bucket
   (for example remote `r2`, bucket `zakura-artifacts`), and make sure the
   bucket's `release-state/` prefix is served on a public HTTPS domain that is
   in the fetch script's allowed-host list
   (`.github/scripts/fetch-release-state.py`).

3. Hook the script into the snapshot job with its environment, e.g.:

   ```sh
   RELEASE_STATE_R2_REMOTE=r2:zakura-artifacts \
   RELEASE_STATE_PUBLIC_BASE=https://zakura-release.valargroup.dev/release-state \
   /opt/zakura/publish-release-state.sh /mnt/data/quiesced-zakura-cache
   ```

4. Set the repository variable `MAINNET_RELEASE_STATE_LATEST_URL` to
   `<RELEASE_STATE_PUBLIC_BASE>/latest.json`.

## Invariants the script maintains

- Bundle directories are immutable: an existing height is only ever reused
  when its data-file digests match the fresh export byte-for-byte (a same-state
  re-run); different contents at the same height abort loudly.
- Data files upload before `meta.json`, and `latest.json` moves last, so a
  partial upload is never resolvable.
- Exports continue the deterministic checkpoint selection grid from the
  binary's embedded list. Never hand-edit the Mainnet checkpoint file or
  publish RPC-mode Mainnet output: off-grid lines make every later bundle fail
  the workflow's byte-for-byte prefix check.

## Pre-merge rehearsal

`rehearsal.sh <quiesced-cache-dir>` exercises the whole pipeline against a
real synced state with no R2 or GitHub side effects: export smoke test
(prefix, gaps, determinism, frontier pairing), the publisher against a local
rclone backend with digest verification and an idempotency re-run, and a
replay of the workflow's import/validate block in the checkout (restored on
exit). Easiest host: dispatch `zakura-pr-node.yml` for this branch with
`network=mainnet snapshot_mode=tip`, SSH in, stop `zakurad`, and run it
against the mounted state clone.

## Failure modes

- `state tip ... is not above the last checkpoint`: the quiesced state predates
  the embedded checkpoint list of the installed tool; sync further or update
  the tool.
- `Sprout note commitments were appended at ...`: a v4 JoinSplit landed just
  below the state tip; the next day's export self-heals once the checkpoint
  grid passes that block.
- `existing bundle at this height has different contents`: determinism broke
  (or the bucket was tampered with) — investigate before deleting anything.
