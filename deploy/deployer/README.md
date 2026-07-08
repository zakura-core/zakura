# zebrad deploy tool

A small, dependency-free operator tool to build `zebrad` from a per-node commit,
distribute it to a fleet over SSH, run it as a systemd service that logs to a
deterministic file, and pull those logs back by node name.

It reuses the build → scp → install-with-`.bak`-backup → `systemctl restart` →
rollback pattern from `.github/workflows/deploy-zcashd-compat.yml`, generalized to
a dynamic multi-node config.

## Requirements

- Python 3.11+ (uses the stdlib `tomllib`; no third-party packages).
- A working SSH key for every node's `ssh_string` (key-based auth; the tool runs
  ssh in `BatchMode`, so password prompts are not supported).
- A local Rust toolchain + `protoc` to build `zebrad` (same as a normal workspace
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

# Build each unique commit into .build-cache/zebrad-<sha> (reused if present).
python3 deploy.py build  --config nodes.toml

# Build-if-needed, distribute, install the unit, restart. Parallel; rolls back
# a node to <bin_path>.bak if its restart fails. Non-zero exit if any node fails.
python3 deploy.py deploy --config nodes.toml
python3 deploy.py deploy --config nodes.toml --node node-a   # one node
python3 deploy.py deploy --config nodes.toml --no-restart    # stage only

# Service state + version per node.
python3 deploy.py status --config nodes.toml

# Pull logs (deterministic log_file from the rendered config).
python3 deploy.py logs fetch  --config nodes.toml                 # -> logs/<name>.log
python3 deploy.py logs fetch  --config nodes.toml --lines 2000    # last N lines only
python3 deploy.py logs follow --config nodes.toml --node node-a   # live tail -F
```

## GitHub Actions testnet fleet deploy

`.github/workflows/zakura-testnet-deploy.yml` runs this deployer on a Linux x86_64
self-hosted runner, expected to be `zakura-testnet-1` with the
`zakura-testnet-deployer` label. The runner builds the native `zebrad` binary and
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

- `ref` — branch, tag, or SHA to build and deploy, default `ironwood-main`.
- `force_rebuild` — pass `--force` to rebuild the cached binary.
- `no_restart` — stage binary/config/unit without restarting, default `false`.
- `node` — optional deployer node name; blank deploys the whole fleet.

The generated CI config uses Testnet ports, public RPC at `0.0.0.0:18232`, and
explicitly sets `vct_fast_sync = false`, which keeps checkpoint sync available
while forcing the legacy non-VCT path. It also writes `/etc/zakura/zakura.toml`
and uses each node's existing `/mnt/<node-name>-data/zakura-cache` snapshot
directory, so CI restarts the current `zakurad.service` against the existing state
instead of creating a fresh database. If an older `/mnt/<node-name>-data/zebra-cache`
directory exists and the `zakura-cache` target does not, the deployer stops that
node and moves the directory before starting with the new config. The compat
Zakura process uses the same snapshot layout on its host.

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
python3 deploy/runner/zebra-cluster-status.py \
  --config deploy/deployer/nodes.toml \
  --host 0.0.0.0 \
  --port 8090 \
  --upgrade-height 4134000 \
  --target-spacing 7.5
```

## How the build cache works

`commit` is resolved to a full SHA (`git rev-parse`). The binary is cached at
`.build-cache/zakurad-<sha>`. A cached binary is reused only if its embedded
`zakurad --version` matches the SHA, otherwise it is rebuilt. Two nodes on the same
commit build once. Each build happens in a throwaway detached `git worktree`, so
your dirty working tree is never touched. Use `--force` to rebuild unconditionally.

## What gets installed on a node

- Binary at `bin_path` (default `/usr/local/bin/zakurad`), previous kept as `.bak`.
- Rendered config at `config_path` (default `/etc/zakura/zakura.toml`) with
  `[tracing] log_file` pointed at `log_file`.
- Unit at `/etc/systemd/system/<service_name>.service` running
  `zakurad -c <config_path> start` with `Restart=always`.

The deterministic `log_file` is the single source of truth shared by the running
node (writer) and `logs fetch`/`logs follow` (reader).
