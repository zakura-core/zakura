# Zakura CI/CD Architecture

This document describes Zakura's GitHub Actions automation: what each workflow does, when it runs, and how the pieces fit together.

All automation runs on GitHub Actions. It falls into four domains:

1. **Merge-gating CI** on GitHub-hosted runners (lint, tests, docs).
2. **The release pipeline** (protected tag creation, asset builds, Docker images).
3. **Fleet operations**: deploying and testing real nodes on DigitalOcean droplets via self-hosted deployer runners.
4. **Fork maintenance**: triaging upstream Zebra PRs.

> **History**: Zakura inherited an extensive Google-Cloud-based CI/CD system from upstream Zebra
> (GCP integration-test VMs, cached state disks, MIG-based continuous delivery). That system has
> been fully decommissioned; the last remnants were removed along with this document's rewrite.
> No workflow uses GCP. Node-fleet automation targets DigitalOcean and a fixed set of
> SSH-reachable hosts instead.

## Merge-gating CI

These workflows run on pull requests, pushes to `main` / `feat/**` / `release/**`, and (where noted) in the merge queue. `merge_group` triggers ignore path filters, so queued merges are always revalidated against the latest `main`.

| Workflow | What it does | Triggers |
| --- | --- | --- |
| `lint.yml` | Clippy, rustfmt, `cargo deny`, feature checks. A nightly scheduled run adds the expensive non-gating lints (unused deps, docs build). | PR/push on Rust-relevant paths, merge queue, nightly, manual |
| `tests-unit.yml` | Unit-test suite via `cargo nextest` on an OS matrix. Nightly run covers release mode. | PR/push on Rust-relevant paths, merge queue, nightly, manual |
| `test-crates.yml` | Builds each workspace crate standalone under its feature combinations. | PR/push on Rust-relevant paths |
| `test-docker.yml` | Builds the production runtime image once, then smoke-tests its packaged binaries, privilege drop, default startup, and combined config overrides. | PR/push on Cargo, Docker, `zakurad`, or runtime-config paths; weekly; manual |
| `zakura-e2e.yml` | The heaviest PR-path job, isolated in its own workflow: regtest docker-compose end-to-end gate, multi-node testkit test, block-sync fuzz on every push to `main`, and long four-node modes nightly. PR runs are gated by a `changes` job or the `run-zakura-e2e` label. | PR/push (self-gated), merge queue, nightly, manual |
| `status-checks.patch.yml` | Empty jobs with the same names as required checks, so branch protection passes when path filters skip `lint.yml` / `tests-unit.yml` / `test-crates.yml`. Its `paths-ignore` list **must stay the exact inverse** of those workflows' `paths`. | PR on non-Rust paths only |
| `docs-check.yml` | markdownlint, codespell, and lychee link checking over all Markdown. | PR/push on Markdown paths |
| `changelog.yml` | Requires one fragment for Rust/Cargo.toml PRs and tests release assembly. | Every PR/push/merge group |
| `coverage.yml` | llvm-cov + nextest coverage uploaded to Codecov. A 120-minute instrumented build, kept off the PR path. | Push to `main`/`release/**`, nightly, manual |
| `benchmarks.yml` | Criterion benchmarks. Runs on PRs carrying the `C-benchmark` label; results publish to the dashboard data on `gh-pages/dev/bench`. | Labeled PRs, manual |
| `zcashd-compat-regtest.yml` | zcashd interoperability regtest suite (spawns fresh `zakurad` + `zcashd`, no external infrastructure). **Temporarily manual-only**: see the workflow header for the sidecar-zcashd re-enable condition. | Manual |

## Release pipeline

- **`create-release.yml`** — the only supported path for creating `v*` release tags. Calls `release-binaries.yml` (as a reusable workflow) to build and verify every asset, then a protected environment lets the release GitHub App publish the draft and create the immutable tag. See the release runbook before using it.
- **`release-binaries.yml`** — builds and publishes `zakurad` release assets and Docker images when a `v*` tag is pushed. Also callable from `create-release.yml` for pre-tag staging, and manually dispatchable to repair assets on an existing tag. Gated on the tag matching the `zakura` package version.
- **`release-drafter.yml`** — manual: compiles PR titles since the last release
  into a draft GitHub release note.
- **Changelog assembly** — `make prepare-release-changelog` consumes reviewed
  PR fragments into the versioned root changelog before the release PR runs
  the protected release gate.
- **`update-release-state.yml`** — manual + weekly: imports the newest Mainnet checkpoint/VCT-frontier bundle from the release-state publisher (digest-verified, append-only over the committed list) and opens a draft PR for human review. Release creation itself never fetches from R2; `make pre-release` validates only the committed state.

## Fleet operations (DigitalOcean)

Deploys are manual, SSH-based, and run from self-hosted deployer runners; there are no cloud-managed instance groups.

- **`zakura-mainnet-deploy.yml`** — manual, binary-only deploy across the mainnet fleet. Builds `zakurad` natively on the `zakura-mainnet-deployer` runner, then installs it host-by-host with `deploy/deployer/deploy.py`. Node configs, identities, and chain state are deliberately left untouched; the previous binary is kept as `.bak`.
- **`zakura-testnet-deploy.yml`** — the same for the testnet fleet, from the `zakura-testnet-deployer` runner. Includes the zcashd-compat host, where Zakura runs alongside a sidecar `zcashd`.
- **`zakura-mainnet-rollback.yml`** — emergency rollback for a single mainnet node: captures diagnostics, restores `<bin_path>.bak`, restarts the service.
- **`zakura-continuous-sync.yml`** — twice-hourly audit (plus manual deploy/status/resume actions) of the continuous genesis-sync fleet, which permanently re-syncs from genesis to catch sync regressions.
- **`zakura-pr-node.yml`** — ephemeral one-hour real-node test of a PR or ref: boots a droplet from the pre-baked image, attaches a chain-state snapshot clone (`tip`, `sandblast`, or `genesis`), builds the branch incrementally, runs it, and posts a metrics summary as a PR comment. The droplet stays up for SSH inspection until the reaper removes it.
- **`zakura-pr-node-bake.yml`** — weekly bake of the golden PR-node droplet image (build deps, warm cargo cache) and per-network chain-state volume snapshots.
- **`zakura-pr-node-reaper.yml`** — hourly TTL cleanup backstop for PR-node droplets, volumes, images, and snapshots.
- **`checkpoint-sync-bench.yml`** — manual checkpoint-zone sync benchmark on the `zakura-bench` self-hosted runner, with a persistent metrics dashboard. See the workflow header for one-time runner setup.

These workflows use the helper scripts in `.github/workflows/scripts/` (`pr-node-bake.sh`, `pr-node-run.sh`, `pr-node-monitor.py`).

## Fork maintenance

- **`upstream-sync.yml`** — scheduled and manual discovery/triage of upstream `ZcashFoundation/zebra` PRs, with conservative adaptation; opens at most one downstream draft PR per run.

## Conventions

- **Merge queue, not Mergify.** PRs land through GitHub's native merge queue; `lint.yml`, `tests-unit.yml`, and `zakura-e2e.yml` re-run on every queued entry via `merge_group`.
- **Patch workflows.** When a required check is skipped by path filters, GitHub leaves it "Expected" forever. The `.patch.yml` pattern provides an empty job with the same name on the inverse path set. If you change `paths` in a gating workflow, update `status-checks.patch.yml` to match.
- **Label-gated heavy jobs.** `C-benchmark` runs benchmarks on a PR; `run-zakura-e2e` forces the e2e suite on a PR that wouldn't otherwise trigger it.
- **Fork PRs.** Repository secrets and variables are not available to workflows on PRs from forks, so fleet and PR-node workflows are dispatch-only from this repository.
- **Checkpoints are updated manually.** The upstream automated checkpoint pipeline depended on the removed GCP integration tests. Until a replacement exists, follow the [`zakura-checkpoints` instructions](../../zakura-utils/README.md#zakura-checkpoints); checkpoint PRs remain consensus-critical and need careful review.
