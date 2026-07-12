---
name: deploy-nodes
description: >-
  Deploy the Zakura mainnet or testnet fleet through GitHub Actions
  (zakura-mainnet-deploy.yml / zakura-testnet-deploy.yml). Use when the user
  asks to deploy nodes, roll out zakurad to the fleet, deploy all mainnet,
  deploy all testnet, or deploy a single node by name.
---

# Deploy Zakura Nodes

## Scope

Dispatch the manual fleet deploy workflows in `zakura-core/zakura`:

| Network | Workflow | GitHub Environment | Default `ref` |
| --- | --- | --- | --- |
| testnet | `zakura-testnet-deploy.yml` | `zakura-testnet` | `main` |
| mainnet | `zakura-mainnet-deploy.yml` | `zakura-mainnet` | `main` |

Both workflows build a native `zakurad` binary on a self-hosted runner, then use
`deploy/deployer/deploy.py` to install it across the fleet over SSH.

Workflow URLs:

- [Deploy Zakura testnet fleet](https://github.com/zakura-core/zakura/actions/workflows/zakura-testnet-deploy.yml)
- [Deploy Zakura mainnet fleet](https://github.com/zakura-core/zakura/actions/workflows/zakura-mainnet-deploy.yml)

## Inputs

Require or confirm these before dispatch:

| Input | Required | Notes |
| --- | --- | --- |
| `ref` | yes | Git branch, tag, or SHA to build and deploy |
| `node` | no | Blank deploys the whole fleet; set to one name below for a single node |
| `force_rebuild` | no | Rebuild even if a cached binary for this commit exists |
| `no_restart` | no | Stage binary/config/unit without restarting `zakurad` |

For mainnet deploys, confirm the `ref` explicitly with the user before dispatch.

## Fleet Nodes

### Testnet (6 nodes)

| Node name | Host | Notes |
| --- | --- | --- |
| `zakura-testnet-1` | `167.99.103.111` | systemd `zakurad.service`; also hosts the testnet deploy runner |
| `zakura-testnet-2` | `167.99.110.145` | systemd |
| `zakura-testnet-3` | `138.68.229.254` | systemd |
| `zakura-testnet-eu` | `164.92.209.78` | systemd |
| `zakura-testnet-as` | `206.189.148.0` | systemd |
| `zakura-compat` | `206.189.208.228` | process-managed; shares host with `zcashd` sidecar |

### Mainnet (11 nodes)

| Node name | Host | Notes |
| --- | --- | --- |
| `asia-0` | `165.22.54.66` | systemd |
| `us-0` | `104.131.184.123` | systemd |
| `us-east-0` | `159.65.183.89` | systemd |
| `us-west-0` | `143.244.184.176` | systemd |
| `canada-0` | `159.203.38.10` | systemd |
| `europe-west-0` | `64.227.44.93` | systemd |
| `europe-central-0` | `161.35.156.226` | systemd |
| `asia-south-0` | `139.59.64.115` | systemd |
| `asia-pacific-0` | `168.144.173.250` | systemd |
| `zakura-compat` | `159.203.113.196` | systemd `zebrad-compat`; shares host with `zcashd` |
| `zakura-compat-docker` | `178.128.66.48` | Docker container `zakura-compat`; shares host with `zakura-compat-zcashd` |

## Preflight

From a checkout with `gh` authenticated for `zakura-core/zakura`:

```bash
cd /Users/roman/projects/zakura
git fetch origin
gh workflow view zakura-testnet-deploy.yml
gh workflow view zakura-mainnet-deploy.yml
```

When deploying a specific `ref`, verify it exists:

```bash
git rev-parse --verify <ref>
```

## Dispatch — All Testnet

```bash
gh workflow run zakura-testnet-deploy.yml \
  --repo zakura-core/zakura \
  -f ref=<ref>
```

## Dispatch — All Mainnet

```bash
gh workflow run zakura-mainnet-deploy.yml \
  --repo zakura-core/zakura \
  -f ref=<ref>
```

## Dispatch — Single Testnet Node

Use the exact node name from the table above:

```bash
gh workflow run zakura-testnet-deploy.yml \
  --repo zakura-core/zakura \
  -f ref=<ref> \
  -f node=<node-name>
```

Examples:

```bash
gh workflow run zakura-testnet-deploy.yml --repo zakura-core/zakura -f ref=main -f node=zakura-testnet-1
gh workflow run zakura-testnet-deploy.yml --repo zakura-core/zakura -f ref=main -f node=zakura-compat
```

## Dispatch — Single Mainnet Node

```bash
gh workflow run zakura-mainnet-deploy.yml \
  --repo zakura-core/zakura \
  -f ref=<ref> \
  -f node=<node-name>
```

Examples:

```bash
gh workflow run zakura-mainnet-deploy.yml --repo zakura-core/zakura -f ref=main -f node=us-east-0
gh workflow run zakura-mainnet-deploy.yml --repo zakura-core/zakura -f ref=main -f node=asia-pacific-0
```

## Optional Flags

Append when needed:

```bash
-f force_rebuild=true    # ignore cached binary for this commit
-f no_restart=true       # stage only; do not restart zakurad
```

## Watch the Run

```bash
gh run list --repo zakura-core/zakura --workflow zakura-testnet-deploy.yml --limit 1
gh run list --repo zakura-core/zakura --workflow zakura-mainnet-deploy.yml --limit 1
gh run watch <run-id> --repo zakura-core/zakura --exit-status
```

On failure, the workflow uploads deploy logs as a GitHub Actions artifact.

## Post-Run Checks

- Confirm the run completed successfully.
- For full-fleet or `zakura-compat` testnet deploys with restart, the workflow
  verifies the `zcashd` sidecar sync on `206.189.208.228`.
- The workflow always prints fleet status at the end via `deploy.py status`.
- Mainnet restarts suppress fleet Slack watchdog alerts for 20 minutes.

## Hard Guards

- Do not dispatch mainnet without explicit user confirmation of the `ref`.
- Do not change cache paths or resync mainnet archive nodes. The workflow pins
  existing cache and identity paths so nodes keep their state DB and iroh node id.
- `node` must match a deployer config name exactly (case-sensitive).
- Only one deploy per network runs at a time (`cancel-in-progress: false`).

## References

- Deploy tool: `deploy/deployer/README.md`
- Testnet workflow: `.github/workflows/zakura-testnet-deploy.yml`
- Mainnet workflow: `.github/workflows/zakura-mainnet-deploy.yml`
- Fleet watchdog: `deploy/runner/README.md`
