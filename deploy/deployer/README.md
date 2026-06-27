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

## How the build cache works

`commit` is resolved to a full SHA (`git rev-parse`). The binary is cached at
`.build-cache/zebrad-<sha>`. A cached binary is reused only if its embedded
`zebrad --version` matches the SHA, otherwise it is rebuilt. Two nodes on the same
commit build once. Each build happens in a throwaway detached `git worktree`, so
your dirty working tree is never touched. Use `--force` to rebuild unconditionally.

## What gets installed on a node

- Binary at `bin_path` (default `/usr/local/bin/zebrad`), previous kept as `.bak`.
- Rendered config at `config_path` (default `/etc/zebrad/zebrad.toml`) with
  `[tracing] log_file` pointed at `log_file`.
- Unit at `/etc/systemd/system/<service_name>.service` running
  `zebrad -c <config_path> start` with `Restart=always`.

The deterministic `log_file` is the single source of truth shared by the running
node (writer) and `logs fetch`/`logs follow` (reader).
