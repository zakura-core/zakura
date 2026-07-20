# Golden DigitalOcean PR nodes

The PR-node workflows provide the supported path for real node tests on
DigitalOcean. A run starts from a Golden image, clones the required state
fixtures, builds the requested PR or ref, and monitors the node. Normal runs do
not download or unpack chain snapshots.

Do not replace this path with an ad hoc Droplet. Only
`zakura-pr-node-bake.yml` creates or refreshes Golden assets.

## Golden asset architecture

| Asset family | Role | Profiles |
| --- | --- | --- |
| `zakura-pr-node-*` | Build image | All |
| `zakura-pr-state-mainnet-*` | Zakura mainnet state | Mainnet |
| `zakura-pr-state-testnet-*` | Zakura testnet state | Testnet `zakura` |
| `zakura-pr-state-zcashd-mainnet-*` | zcashd state | `zcashd-compat` |

The image contains Ubuntu, build dependencies, a repository clone, and a warm
release build cache. The mainnet Zakura fixture contains `tip/` and
`sandblast/`; the testnet fixture contains `tip/`. The clean zcashd fixture
contains `blocks/`, `chainstate/`, `unity/`, and its `fixture-manifest.json`
provenance manifest.

State assets are volume snapshots rather than part of the Droplet image. Each
run clones only what its profile needs:

```text
zakura          = base image + one Zakura state volume
zcashd-compat   = base image + Zakura mainnet state + zcashd mainnet state
genesis         = base image only
```

This keeps ordinary Zakura tests small while making zcashd compatibility tests
fast and reproducible.

## Workflows

- **`zakura-pr-node-bake.yml`** runs twice weekly and on demand. It bakes one
  `zakura-pr-node-<stamp>` image, Zakura state snapshots for mainnet and
  testnet, and a `zakura-pr-state-zcashd-mainnet-<stamp>` snapshot when a
  zcashd fixture exists. Mainnet Zakura state contains `tip/` and `sandblast/`.
  The latter starts at height 1,707,210, immediately before the 2022
  sandblasting region. Testnet state contains `tip/`.
- **`zakura-pr-node.yml`** is the test entrypoint. It clones the selected
  assets, builds the requested target, runs it, and writes a job summary and
  artifact. A PR target also receives one upserted summary comment per profile,
  mode, and network.
- **`zakura-pr-node-reaper.yml`** runs hourly. It deletes run Droplets older
  than 24 hours, failed bake Droplets older than 6 hours, and detached run or
  bake volumes older than 2 hours. It retains the newest two images and the
  newest two snapshots in every state family.

## Prerequisites

- Secrets: `DIGITALOCEAN_ACCESS_TOKEN`, `DO_SSH_PRIVATE_KEY`.
- Variable: `DO_SSH_KEY_FINGERPRINT`.
- Optional Zakura source overrides:
  `ZAKURA_PR_NODE_TIP_LATEST_JSON`, `ZAKURA_PR_NODE_SANDBLAST_URL`,
  `ZAKURA_PR_NODE_SANDBLAST_SHA256`, `ZAKURA_PR_NODE_SANDBLAST_HEIGHT`,
  `ZAKURA_PR_NODE_SANDBLAST_DB_FORMAT`, and
  `ZAKURA_TESTNET_SNAPSHOTS_URL`.
- Optional zcashd retention override:
  `ZAKURA_PR_NODE_ZCASHD_TX_RETENTION` (default `10000`).

Run the bake workflow before the first PR-node test.

## Choose a test profile

| Profile | Purpose | Valid state modes |
| --- | --- | --- |
| `zakura` | Node tests | `tip`, mainnet `sandblast`, or `genesis` |
| `zcashd-compat` | Managed wrapper | Mainnet `tip` only |

Use `zakura` for ordinary sync, validation, performance, and regression tests.
The compat profile covers the managed zcashd wrapper's download, lifecycle,
restart, and pruned state behavior. It starts Zakura from
`/mnt/snapshots/tip` and zcashd from `/mnt/zcashd`, then removes only the
ephemeral `/mnt/snapshots/tip/zcashd-compat/bin` cache before the first start.
The run verifies a cold wrapper installation, exactly one direct zcashd child,
both heights advancing, clean managed child shutdown, and a warm restart with
the same cached binary hash and modification time. Monitoring also rejects the
pruned block sidecar failure.

## Run a test

Dispatch from the Actions tab or with `gh`:

```bash
# Test PR 123 near the mainnet tip for one hour.
gh workflow run zakura-pr-node.yml \
  -f pr_number=123 \
  -f test_profile=zakura

# Test PR 123 with the managed zcashd wrapper.
gh workflow run zakura-pr-node.yml \
  -f pr_number=123 \
  -f test_profile=zcashd-compat \
  -f network=mainnet \
  -f snapshot_mode=tip

# Stress-sync a ref through the sandblasting region for 30 minutes.
gh workflow run zakura-pr-node.yml \
  -f ref=my-branch \
  -f test_profile=zakura \
  -f snapshot_mode=sandblast \
  -f duration_minutes=30

# Explicitly checkpoint-sync from genesis on testnet, then tear down.
gh workflow run zakura-pr-node.yml \
  -f ref=my-branch \
  -f test_profile=zakura \
  -f snapshot_mode=genesis \
  -f network=testnet \
  -f teardown_after_run=true
```

Provide exactly one of `pr_number` or `ref`. Other inputs are
`duration_minutes` (default `60`), `droplet_size` (default `c-8`), and
`teardown_after_run`. The run Droplet's disk must be at least as large as the
baked image's disk.

The summary is available in the Actions job summary and in a
`zakura-pr-node-<profile>-<run id>` artifact with `summary.json` and node logs.
Bare refs without an open PR skip the PR comment. Summaries identify the
selected image and snapshots, their creation times and ages, and their fixture
manifests.

## Inspect a retained run

The job summary prints the Droplet IP. On the Droplet:

```bash
systemctl status zakurad
tail -f /var/log/zakura/zakura.log
cd /root/zakura && python3 deploy/deployer/deploy.py status --config /root/fleet.toml
```

Unless `teardown_after_run=true`, the node remains available for SSH inspection
until the reaper deletes its Droplet and cloned volumes. Re-dispatching the same
PR updates the existing comment for that profile.

## Refresh the zcashd fixture

After the first seed, each scheduled bake clones the latest zcashd snapshot,
syncs it forward, shuts zcashd down cleanly, and publishes a new snapshot. The
Monday and Thursday schedule leaves enough margin for one missed refresh within
the default 10,000-block retention window. The bake requires the zcashd lag to
remain nonnegative and below
`ZAKURA_PR_NODE_ZCASHD_TX_RETENTION` before and after sync, then requires it to
catch up. This keeps the sidecar within Zakura's retained transaction window.

To create the first fixture, use an active seed Droplet in the same region that
the CI SSH key can reach. Both Zakura and zcashd must have stopped cleanly, and
no process may have the datadir open. Supply the recorded seed tip and the
explicit copy confirmation:

```bash
gh workflow run zakura-pr-node-bake.yml \
  -f zcashd_seed_droplet_id=DROPLET_ID \
  -f zcashd_seed_confirm=COPY_CLEAN_ZCASHD_STATE \
  -f zcashd_seed_datadir=/root/.zcashd \
  -f zcashd_seed_height=HEIGHT \
  -f zcashd_seed_hash=BLOCK_HASH \
  -f zcashd_seed_checksum_verified=false
```

The seed contributes only `blocks/`, `chainstate/`, and `unity/`. Runtime
cookies, locks, wallets, peer history, logs, and identities are not part of the
fixture. `fixture-manifest.json` records the source, tip, checksum status,
refresh lag, clean shutdown, node versions, and zcashd binary hash. A seed with
`zcashd_seed_checksum_verified=false` is marked `candidate`, and that status is
preserved by later refreshes. The expected archive URL and SHA256 may still be
supplied as provenance for a candidate seed. A verified seed must provide both
`zcashd_seed_archive_url` and `zcashd_seed_archive_sha256`. Do not manually
download a zcashd archive onto a PR-node run.

## Missing, stale, or incompatible assets

- If an asset is missing or too stale for the intended test, dispatch the bake
  workflow, wait for it to finish, and retry the test.
- If a Zakura state database is too old, rebake after the needed format lands
  on `main`. Zakura can upgrade one state major version in place; older state is
  intentionally rejected. If the target PR needs a newer unsupported format,
  stop and report it rather than implying that the branch can be baked.
- If no first zcashd fixture exists, use the documented seed inputs with an
  existing cleanly stopped datadir.
- If a bake guard fails, stop and report the failed prerequisite. Do not fall
  back to an untracked Droplet, archive download, renamed snapshot, or patched
  fixture.
- Use `snapshot_mode=genesis` only for a test that explicitly requires genesis,
  not as a fallback for unavailable Golden state.

## Why runs stay fast

The base image pre-installs the toolchain, pre-clones the repository, warms
`CARGO_TARGET_DIR=/root/cargo-target` with a release build of `main`, and bakes
a loopback SSH identity so `deploy/deployer/deploy.py` can install the selected
worktree. State volume clones are created from maintained snapshots. A normal
run pays only for volume cloning, Droplet boot, an incremental build, and the
monitored test.

## Cost and cleanup

Run Droplets and cloned volumes continue billing until deletion. Prefer
`teardown_after_run=true` when no SSH investigation is needed. Otherwise the
reaper removes them within 24 hours. After a force cancelled workflow, dispatch
`zakura-pr-node-reaper.yml` or inspect tagged resources:

```bash
doctl compute droplet list --tag-name zakura-pr-node
doctl compute volume list | grep zakura-pr-
```
