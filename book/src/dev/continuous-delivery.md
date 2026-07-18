# Zakura Continuous Delivery

Zakura node deployments are manual, SSH-based, and driven by GitHub Actions workflows running on self-hosted deployer runners. There are no cloud-managed instance groups: each fleet is a fixed set of DigitalOcean hosts, and a deploy builds a `zakurad` binary once on the deployer runner and installs it host-by-host with `deploy/deployer/deploy.py`.

## Fleet deploys

Both deploys are `workflow_dispatch`-only. The dispatcher picks the git ref to build and can target a single node or the whole fleet.

- [Deploy Zakura mainnet fleet](https://github.com/zakura-core/zakura/blob/main/.github/workflows/zakura-mainnet-deploy.yml) runs on the `zakura-mainnet-deployer` runner. It is a **binary-only** deploy: mainnet nodes keep their hand-provisioned configs, iroh identities, and chain state; the workflow only swaps `/usr/local/bin/zakurad` and restarts the service. The previous binary is kept at `<bin_path>.bak`.
- [Deploy Zakura testnet fleet](https://github.com/zakura-core/zakura/blob/main/.github/workflows/zakura-testnet-deploy.yml) runs on the `zakura-testnet-deployer` runner and manages binary, config, and systemd unit. The zcashd-compat node is deployed process-managed because it shares its host with a sidecar `zcashd`.

## Rollback

[Rollback Zakura mainnet node](https://github.com/zakura-core/zakura/blob/main/.github/workflows/zakura-mainnet-rollback.yml) is the emergency path for a single node: it captures diagnostics, restores `<bin_path>.bak` (kept by the deploy workflow), and restarts the service.

## Ephemeral PR nodes

For testing a PR on a real node before merge, the PR-node system runs one-hour ephemeral droplets on DigitalOcean:

- `zakura-pr-node-bake.yml` bakes a golden droplet image (build dependencies, warm cargo cache) and per-network chain-state volume snapshots, weekly.
- `zakura-pr-node.yml` boots a droplet from that image, attaches a clone of the chosen state snapshot, builds the PR branch incrementally, runs it for the requested duration, and posts a metrics summary as a PR comment. The droplet stays up for SSH inspection.
- `zakura-pr-node-reaper.yml` deletes run droplets and any leaked volumes, images, or snapshots on a TTL, hourly.

## Continuous genesis sync

[Zakura continuous genesis sync fleet](https://github.com/zakura-core/zakura/blob/main/.github/workflows/zakura-continuous-sync.yml) audits (twice hourly) a small fleet of nodes that permanently re-sync from genesis, to catch sync regressions against the real network; its manual actions deploy and resume the sync controllers.

## Releases

Release binaries and Docker images are built by `release-binaries.yml` when a `v*` tag is created through the protected [Create release](https://github.com/zakura-core/zakura/blob/main/.github/workflows/create-release.yml) workflow. See [Zakura versioning and releases](release-process.md) for the release process.

## History

Zakura inherited a GCP-based continuous-delivery pipeline from upstream Zebra (zonal stateful Managed Instance Groups, cached state disks, cache images). That system was decommissioned along with the rest of the GCP CI infrastructure. [ADR 0006](../../../docs/decisions/devops/0006-gcp-deployment-naming.md) records its design and is retained as a historical record.
