# PR node on DigitalOcean — ephemeral 1-hour real-node PR testing

The `PR node` workflow (`zakura-pr-node.yml`) boots a droplet in `nyc3` from a
pre-baked image (build deps + warm cargo release cache + a repo clone), attaches
a clone of a pre-baked chain-state volume, builds `zakurad` from a PR branch
incrementally, runs it for ~1 hour, and posts a metrics summary as a PR comment.
The droplet stays up afterwards for SSH inspection and is auto-deleted by the
reaper within 24 hours.

## Workflows

- **`zakura-pr-node-bake.yml`** (weekly + manual) — bakes the golden assets:
  one `zakura-pr-node-<stamp>` droplet image and one
  `zakura-pr-state-{mainnet,testnet}-<stamp>` volume snapshot per network.
  The mainnet volume holds `tip/` (daily pruned snapshot) and `sandblast/`
  (archive pinned at height 1,707,210 — just before the 2022 "sandblasting"
  spam region, so a run stress-syncs through high-density blocks). The testnet
  volume holds `tip/`. Keeps the newest 2 of each asset.
- **`zakura-pr-node.yml`** (manual) — the entrypoint; see Running below.
- **`zakura-pr-node-reaper.yml`** (hourly + manual) — deletes run droplets
  older than 24h, failed bake droplets older than 6h, detached `zakura-pr-*`
  volumes older than 2h, and prunes images/volume snapshots beyond the
  newest 2.

## Prerequisites

- Secrets: `DIGITALOCEAN_ACCESS_TOKEN`, `DO_SSH_PRIVATE_KEY`.
- Variables: `DO_SSH_KEY_FINGERPRINT`.
- Optional variable overrides for the bake's snapshot sources:
  `ZAKURA_PR_NODE_TIP_LATEST_JSON`, `ZAKURA_PR_NODE_SANDBLAST_URL` /
  `ZAKURA_PR_NODE_SANDBLAST_SHA256`, `ZAKURA_TESTNET_SNAPSHOTS_URL`.

Run the bake workflow once before the first PR-node run.

## Running

From the Actions tab or the CLI:

```bash
# Test PR #123 from a tip snapshot on mainnet for an hour (the default):
gh workflow run zakura-pr-node.yml -f pr_number=123

# Stress-sync PR #123 through the sandblast region for 30 minutes:
gh workflow run zakura-pr-node.yml -f pr_number=123 -f snapshot_mode=sandblast -f duration_minutes=30

# Checkpoint-sync a branch from genesis on testnet, delete the droplet after:
gh workflow run zakura-pr-node.yml -f ref=my-branch -f snapshot_mode=genesis \
  -f network=testnet -f teardown_after_run=true
```

Inputs: exactly one of `pr_number` / `ref`; `snapshot_mode` ∈ `tip` (start near
the chain tip and follow it), `sandblast` (mainnet-only archive at 1,707,210),
`genesis` (fresh checkpoint sync, no volume); `network` ∈ `mainnet`/`testnet`;
`duration_minutes` (default 60); `droplet_size` (default `c-8`; its disk must be
≥ the baked image's disk, so ≥ the bake `droplet_size`); `teardown_after_run`.

The summary lands in the job step summary, as an upserted PR comment (one per
mode×network, matched by a hidden marker), and in a `zakura-pr-node-<run id>`
artifact together with `summary.json` and the node logs. Runs for a bare `ref`
without an open PR skip the comment.

## Inspecting a kept node

The step summary prints the droplet IP. On the droplet:

```bash
systemctl status zakurad
tail -f /var/log/zakura/zakura.log
cd /root/zakura && python3 deploy/deployer/deploy.py status --config /root/fleet.toml
```

The node keeps running (and syncing) until the reaper deletes the droplet ~24h
after creation. Re-dispatching for the same PR updates the existing comment.

## How it stays fast

The bake pre-installs apt/rustup toolchains, pre-clones the repo, warms
`CARGO_TARGET_DIR=/root/cargo-target` with a release build of `main`, and bakes
a `root@localhost` SSH identity so `deploy/deployer/deploy.py` is reused
unmodified on the droplet (worktree build keyed on the PR SHA, systemd install
with rollback, status/log commands). A PR run only pays: volume clone + droplet
boot (~2 min), incremental `cargo build` of the diff vs `main`, then the
monitored run itself.

## DB format-version note

Snapshots store `state/v<N>/`. zakurad restores a state one major version back
(a reusable-format upgrade runs in place); anything older is ignored and the
node syncs from scratch — the run summary calls this out. After a DB format
bump lands on `main`, re-run the bake so the volume snapshots catch up.

## Cost note

Per run: `c-8` droplet ~$0.25/h (~$6 if kept the full 24h) + the state volume
(300 GiB ≈ $30/mo pro-rated, so ≪$1/day). Standing: 2 images + 2×2 volume
snapshots ≈ $30–40/mo. If runs were force-killed, check:

```bash
doctl compute droplet list --tag-name zakura-pr-node
doctl compute volume list | grep zakura-pr-
```

or just dispatch the reaper workflow.
