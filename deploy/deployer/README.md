# zakurad deploy tool

A small, dependency-free operator tool to build `zakurad` from a per-node commit,
distribute it to a fleet over SSH, run it as a systemd service that logs to a
deterministic file, and pull those logs back by node name.

It uses a build → scp → install-with-`.bak`-backup → `systemctl restart` →
rollback pattern generalized to a dynamic multi-node config.

## Requirements

- Python 3.11+ (uses the stdlib `tomllib`; no third-party packages).
- A working SSH key for every node's `ssh_string` (key-based auth; the tool runs
  ssh in `BatchMode`, so password prompts are not supported).
- A local Rust toolchain + `protoc` to build `zakurad` (same as a normal workspace
  build). Builds run on this host; the resulting binary is copied to every node,
  so nodes must share the build host's architecture and a compatible glibc
  (DigitalOcean Ubuntu x86_64 droplets do).

## Config

Copy `nodes.example.toml` to `nodes.toml` and edit. Each `[[nodes]]` entry needs:

- `name` — used for `--node` selection and `logs/<name>.log`.
- `ssh_string` — the ssh/scp destination, e.g. `root@167.99.162.47`.
- `commit` — branch, tag, or SHA to build from (must be fetched locally).

`[defaults]` supplies fleet-wide values (service name, paths, network, ssh
`port`); any field can be overridden per node. `nodes.toml` is gitignored.

## Commands

```bash
cd deploy/deployer

# Build each unique commit into .build-cache/zakurad-<sha> (reused if present).
python3 deploy.py build  --config nodes.toml

# Build-if-needed, distribute, install the unit, restart. Parallel; rolls back
# a node to <bin_path>.bak if its restart fails. Non-zero exit if any node fails.
python3 deploy.py deploy --config nodes.toml
python3 deploy.py deploy --config nodes.toml --node node-a   # one node
python3 deploy.py deploy --config nodes.toml --no-restart    # stage only

# Service state + version per node.
python3 deploy.py status --config nodes.toml

# Pull logs (deterministic log_file from the rendered config).
python3 deploy.py logs fetch  --config nodes.toml              # -> logs/<name>.log
python3 deploy.py logs fetch  --config nodes.toml --lines 2000 # last N lines only
python3 deploy.py logs follow --config nodes.toml --node node-a
```

## GitHub Actions testnet fleet deploy

`.github/workflows/zakura-testnet-deploy.yml` runs this deployer on a Linux x86_64
self-hosted runner, expected to be `zakura-testnet-1` with the
`zakura-testnet-deployer` label. The runner builds the native `zakurad` binary and
then deploys it to:

- `zakura-testnet-1` — `root@167.99.103.111`
- `zakura-testnet-2` — `root@167.99.110.145`
- `zakura-testnet-3` — `root@138.68.229.254`
- `zakura-testnet-eu` — `root@164.92.209.78`
- `zakura-testnet-as` — `root@206.189.148.0`
- `zakura-compat` — `root@206.189.208.228`

The first five nodes are systemd-managed `zakurad.service` nodes. `zakura-compat`
is process-managed because it shares the compat host with a manually supervised
`zcashd` sidecar; the deployer updates `/root/unity/zakura/target/release/zakurad`,
rewrites `/root/unity/zakura-testnet.toml`, restarts only the Zakura process, and
then the workflow verifies the sidecar with `deploy/zcashd-compat/sync-check.sh`.

One-time runner bootstrap from an operator machine with SSH access and CI
credentials in `~/agents-env`:

```bash
cd deploy/deployer
./testnet/bootstrap-zakura-testnet-runner.sh
```

Useful overrides:

```bash
ENV_FILE=~/agents-env ./testnet/bootstrap-zakura-testnet-runner.sh
FORCE_REGISTER=1 ./testnet/bootstrap-zakura-testnet-runner.sh
RUNNER_SSH=root@167.99.103.111 ./testnet/bootstrap-zakura-testnet-runner.sh
```

The workflow is manual (`workflow_dispatch`). Inputs:

- `ref` — branch, tag, or SHA to build and deploy, default `main`.
- `force_rebuild` — pass `--force` to rebuild the cached binary.
- `no_restart` — stage binary/config/unit without restarting, default `false`.
- `node` — optional deployer node name; blank deploys the whole fleet.

The generated CI config uses Testnet ports, public RPC at `0.0.0.0:18232`, and
explicitly sets `vct_fast_sync = false`, which keeps checkpoint sync available
while forcing the legacy non-VCT path. Fleet nodes use `p2p_stack = "dual"`;
`zakura-compat` uses `p2p_stack = "legacy"` (legacy TCP only). It also writes
`/etc/zakura/zakura.toml` and uses each node's existing
`/mnt/data/zakura-cache` snapshot directory, so CI restarts the current
`zakurad.service` against the existing state instead of creating a fresh
database. Volume-backed fleet hosts mount their attached DigitalOcean block
volume at `/mnt/data`; legacy `/mnt/<node-name>-data` paths are compatibility
symlinks only. The compat Zakura process uses the same snapshot layout on its
host.

The workflow also refreshes a simple fleet status dashboard on
`zakura-testnet-1`:

- service: `zakura-testnet-dashboard.service`
- URL: `http://167.99.103.111:8090/`
- install dir: `/opt/zakura-testnet-dashboard`

The dashboard reads the generated deployer node config and polls each node over
SSH. It shows the running commit from the node log, last restart time, current
RPC height, whether the height advanced in the last five minutes, and an upgrade
ETA for Ironwood testnet activation height `4134000`. The ETA uses observed
cluster block movement when enough samples are available, otherwise it falls back
to `--target-spacing 7.5`.

The workflow also refreshes a static Zakura Ironwood testnet snapshots website on
`zakura-testnet-1`:

- service: `zakura-testnet-snapshots.service`
- URL: `http://167.99.103.111:8091/`
- install dir: `/opt/zakura-testnet-snapshots/site`
- upload dir: `/opt/zakura-testnet-snapshots/site/files`
- metadata: `/opt/zakura-testnet-snapshots/site/snapshots.json`

The deploy refreshes `index.html` but does not delete uploaded snapshot files or
overwrite an existing host-side `snapshots.json`. To publish a snapshot manually,
upload the archive and then edit the metadata on the runner host:

```bash
scp zakura-ironwood-testnet-archive-YYYYMMDD-height.tar.zst \
  root@167.99.103.111:/opt/zakura-testnet-snapshots/site/files/

ssh root@167.99.103.111 \
  '$EDITOR /opt/zakura-testnet-snapshots/site/snapshots.json'
```

Each enabled metadata entry needs `kind` (`archive` or `pruned`), `group`
(`daily`, `monthly`, or `historical`), `file`, `published`, and `sha256`.
Optional display fields include `name`, `size`, `height`, `zebraVersion`, and
`dbFormat`. Entries with `"enabled": false` are kept as hidden examples.

Manual run from a host with SSH access to every node:

```bash
python3 deploy/runner/zakura-cluster-status.py \
  --config deploy/deployer/nodes.toml \
  --host 0.0.0.0 \
  --port 8090 \
  --upgrade-height 4134000 \
  --target-spacing 7.5
```

## GitHub Actions mainnet fleet deploy

`.github/workflows/zakura-mainnet-deploy.yml` runs the same deployer for the
mainnet fleet on a Linux x86_64 self-hosted runner, expected to be `us-east-0`
with the `zakura-mainnet-deployer` label. It builds the native `zakurad` binary
and deploys it to:

- `asia-0` — `root@165.22.54.66`
- `us-0` — `root@104.131.184.123`
- `us-east-0` — `root@159.65.183.89`
- `us-west-0` — `root@143.244.184.176`
- `canada-0` — `root@159.203.38.10`
- `europe-west-0` — `root@64.227.44.93`
- `europe-central-0` — `root@161.35.156.226`
- `asia-south-0` — `root@139.59.64.115`
- `asia-pacific-0` — `root@168.144.173.250`

All nine run a hand-provisioned `zakurad` systemd service. One-time runner
bootstrap from an operator machine with SSH access and CI credentials in
`~/agents-env`:

```bash
cd deploy/deployer
./mainnet/bootstrap-zakura-mainnet-runner.sh
```

The workflow is manual (`workflow_dispatch`) with the same inputs as testnet
(`ref` defaults to `main`, plus `force_rebuild`, `no_restart`, `node`).

**Binary-only deploy (`manage_config = false`).** The mainnet nodes were
provisioned by hand with rich, per-node configs — `external_addr`, custom peers,
mempool/sync tuning, and an inline `zakura_node_secret_key` that pins each node's
iroh identity (the node ids hardcoded as bootstrap peers in
`zakura-network/src/zakura/handler.rs`) — and their state DB lives at
`/root/.cache/zebra`. Rendering the deployer's managed config over that would
change every node id and drop the tuning. So the generated CI config sets
`manage_config = false`: the deployer swaps `/usr/local/bin/zakurad` and restarts
the existing `zakurad` service, leaving the config, unit, and cache untouched. The
`rpc_listen_addr` / `log_file` / `p2p_stack` /
`[defaults.zakura] bootstrap_peers` in that config are read-only inputs for the
dashboard's SSH probe, not deployed to nodes. On-node configs should use
`network.p2p_stack` (not the deprecated `v2_p2p` /
`legacy_p2p` bools). Reproducing these configs in the deployer's managed model
is separate future work.

The workflow refreshes a fleet status dashboard on `us-east-0`:

- service: `zakura-mainnet-dashboard.service`
- URL: `http://159.65.183.89:8090/`
- install dir: `/opt/zakura-mainnet-dashboard`

It is the same `zakura-cluster-status.py` as testnet, launched with
`--upgrade-height 0`, which hides the upgrade-ETA cards (mainnet has no pending
Zakura activation to count down to). Manual run:

```bash
python3 deploy/runner/zakura-cluster-status.py \
  --config deploy/deployer/nodes.toml \
  --host 0.0.0.0 \
  --port 8090 \
  --upgrade-height 0
```

The mainnet workflow also installs a Slack watchdog on `us-east-0`:

- service: `zakura-fleet-watchdog.service`
- install dir: `/opt/zakura-fleet-watchdog`
- state file: `/var/lib/zakura-fleet-watchdog/state.json`
- env file: `/etc/zakura-fleet-watchdog/env`
- suppression file: `/run/zakura-fleet-watchdog/deploy-suppressed-until`

The watchdog polls the mainnet dashboard locally at
`http://127.0.0.1:8090/data` and the testnet dashboard at
`http://167.99.103.111:8090/data`. It posts transition alerts to Slack
`#zakura-alerts` via an incoming webhook in `SLACK_WEB_HOOK`. A node alert fires
when either of these conditions stays true for at least 10 minutes:

- `health` is `down` or `rpc_error`
- `seconds_since_advanced` is at least 600 seconds

Down alerts take precedence over stalled alerts, so a node only produces one
active alert at a time. The watchdog also alerts if a dashboard endpoint is
unreachable for at least 10 minutes, and posts one recovery message when a node
or dashboard recovers. Persistent failures do not post on every poll cycle.

Restart deploys write a 20-minute suppression marker before touching the fleet.
The mainnet workflow writes it locally on `us-east-0`; the testnet workflow
refreshes it on `us-east-0` over SSH on a best-effort basis. While the marker is
in the future, new failure alerts are logged but not posted to Slack.

Manual dry run from `us-east-0`:

```bash
python3 /opt/zakura-fleet-watchdog/zakura-cluster-watchdog.py \
  --config /opt/zakura-fleet-watchdog/fleets.toml \
  --state-file /tmp/zakura-fleet-watchdog-state.json \
  --once \
  --dry-run
```

Local status checks:

```bash
systemctl status zakura-fleet-watchdog
journalctl -u zakura-fleet-watchdog -f
```

## Continuous genesis sync fleet

The permanent six-node genesis sync canary is managed separately under
`deploy/continuous-sync/`. It repeatedly builds latest `origin/main`, wipes only
its dedicated disposable state, syncs from genesis to tip, posts Slack
completion/failure alerts through its codified monitor timer, and retains five
days of logs and traces.

See `deploy/continuous-sync/README.md` for the inventory, workflow, safety
invariants, replacement-node bootstrap, and manual `status` / `resume` commands.

## Ephemeral PR test nodes

`.github/workflows/zakura-pr-node.yml` reuses this deployer on a throwaway
DigitalOcean droplet to test a single PR against a real node for ~1 hour: the
droplet image bakes a repo clone, a warm `CARGO_TARGET_DIR`, and a
`root@localhost` SSH identity, so the workflow just writes a one-node config and
runs `deploy.py build` / `deploy` / `status` on the droplet itself. See
`docs/pr-node-do-setup.md`.

## How the build cache works

`commit` is resolved to a full SHA (`git rev-parse`). The binary is cached at
`.build-cache/zakurad-<sha>` by default, or under
`$ZAKURA_DEPLOYER_BUILD_CACHE_DIR/zakurad-<sha>` when that environment variable
is set. A cached binary is reused only if `zakurad --version` runs successfully;
the SHA-named cache file ties the binary to its commit. Two nodes on the same
commit build once. Each build happens in a throwaway detached `git worktree`, so
your dirty working tree is never touched. Use `--force` to rebuild the exact-SHA
binary unconditionally while still allowing Cargo to reuse `$CARGO_TARGET_DIR`.

The GitHub Actions deploy workflows set persistent cache locations outside the
checked-out workspace:

- Testnet: `CARGO_TARGET_DIR=/mnt/data/zakura-deployer/cargo-target` and
  `ZAKURA_DEPLOYER_BUILD_CACHE_DIR=/mnt/data/zakura-deployer/binaries`.
- Mainnet: `CARGO_TARGET_DIR=/root/.cache/zakura-deployer-target` and
  `ZAKURA_DEPLOYER_BUILD_CACHE_DIR=/root/.cache/zakura-deployer-binaries`.

Old exact-SHA binaries are pruned after successful builds. The current binary is
always kept, and `ZAKURA_DEPLOYER_BUILD_CACHE_RETAIN` controls total retained
binaries (default `12`). Remove the cache directory manually only when no deploy
job is running.

When a cache path is under `/mnt/data`, the deployer verifies that `/mnt/data` is
a real mount point before building. Managed `zakurad` units rendered by the
deployer also require `/mnt/data` before starting, preventing a missing volume
from creating chain state on the root disk.

For new DigitalOcean volume-backed fleet hosts, format and mount the attached
volume at `/mnt/data` in `/etc/fstab`; do not use droplet-specific mount names
for new hosts.

## What gets installed on a node

- Binary at `bin_path` (default `/usr/local/bin/zakurad`), previous kept as `.bak`.
- Rendered config at `config_path` (default `/etc/zakura/zakura.toml`) with
  `[tracing] log_file` pointed at `log_file`.
- Unit at `/etc/systemd/system/<service_name>.service` running
  `zakurad -c <config_path> start` with `Restart=always`.

The deterministic `log_file` is the single source of truth shared by the running
node (writer) and `logs fetch`/`logs follow` (reader).
