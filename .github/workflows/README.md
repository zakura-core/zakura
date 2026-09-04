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
| `lint.yml` | Clippy, rustfmt, `cargo deny`, feature checks. A nightly scheduled run adds the expensive non-gating lints (unused deps, docs build). | PR/push on lint-relevant paths; merge queue; nightly; manual |
| `tests-unit.yml` | Unit-test suite via `cargo nextest` on an OS matrix. Nightly run covers release mode. Also runs `scripts/check-crate-publish-graph.sh` so a skipped dependent whose crates.io index manifest still pins an old major cannot merge. | Every PR (self-gated by a `changes` job); push on Rust-relevant paths; merge queue; nightly; manual |
| `test-crates.yml` | Builds each workspace crate standalone under its feature combinations. | PR/push on crate-relevant paths; merge queue; manual |
| `semver-checks.yml` | Runs semver checks for affected publishable crates. | PR/push on semver-relevant paths; merge queue; manual |
| `test-docker.yml` | Builds the production runtime image once, then smoke-tests its packaged binaries, privilege drop, default startup, and combined config overrides. | PR/push on Cargo, Docker, `zakurad`, or runtime-config paths; weekly; manual |
| `zakura-e2e.yml` | The heaviest PR-path job, isolated in its own workflow: regtest docker-compose end-to-end gate, multi-node testkit test, block-sync fuzz on every push to `main`, and long four-node modes nightly. PR runs are gated by a `changes` job or the `run-zakura-e2e` label. | PR/push (self-gated), merge queue, nightly, manual |
| `docs-check.yml` | markdownlint, codespell, and lychee link checking over all Markdown. | PR/push on Markdown paths |
| `changelog.yml` | Requires one fragment for Rust/Cargo.toml PRs and tests release assembly. | Every PR/push/merge group |
| `coverage.yml` | llvm-cov + nextest coverage uploaded to Codecov. A 120-minute instrumented build, kept off the PR path. | Push to `main`/`release/**`, nightly, manual |
| `benchmarks.yml` | Criterion benchmarks. Runs on PRs carrying the `C-benchmark` label; results publish to the dashboard data on `gh-pages/dev/bench`. | Labeled PRs, manual |
| `zcashd-compat-regtest.yml` | zcashd interoperability regtest suite (spawns fresh `zakurad` + `zcashd`, no external infrastructure). **Temporarily manual-only**: see the workflow header for the sidecar-zcashd re-enable condition. | Manual |

## Release pipeline

- **`create-release.yml`** — the only supported path for creating `v*` release tags. For a normal release, it dispatches the advisory Mainnet VCT handoff canary, deploys the release commit to `us-east-0`, and calls `release-binaries.yml` to build and verify every asset. The protected-environment approval waits only for validation and asset staging, so the start canary remains visible in the same workflow without delaying approval. Start-canary infrastructure or reliability failures are advisory; a failed canary rolls `us-east-0` back to its previous binary first. Emergency source-first releases skip the canaries and pre-tag asset build. See the release runbook before using it.
- **`release-binaries.yml`** — builds and publishes `zakurad` release assets and Docker images when a `v*` tag is pushed. Also callable from `create-release.yml` for pre-tag staging, and manually dispatchable to repair assets on an existing tag. The canonical repository uses Depot builders by default; set the `RELEASE_BUILD_BACKEND` repository variable to `github` for an immediate fallback. Gated on the tag matching the `zakura` package version.
- **`release-drafter.yml`** — manual: compiles PR titles since the last release
  into a draft GitHub release note.
- **Changelog assembly** — `make prepare-release-changelog` consumes reviewed
  PR fragments into the versioned root changelog before the release PR runs
  the protected release gate.
- **`prepare-release-pr.yml`** — manual: mechanically prepares a release PR for a given tag (crate bumps chosen by `cargo-semver-checks` against the base tag, zakura version, lockfile, config fixture, end-of-support floor, changelog assembly via `scripts/prepare-release.sh`), verifies with `make pre-release`, and opens a draft PR through the release GitHub App. Judgment items (bump-level review, authoritative end-of-support height, changelog curation) stay with the reviewer; a `dry_run` input uploads the diff as an artifact instead of opening a PR.
- **`update-release-state.yml`** — manual + weekly: imports the newest Mainnet release-state bundle from the release-state publisher — checkpoint list, VCT frontier, subtree roots, and historical frontier grid, digest-verified and append-only over the committed files — and opens a draft PR for human review. Release creation itself never fetches from R2; `make pre-release` validates only the committed state.

## Fleet operations (DigitalOcean)

Deploys are manual, SSH-based, and run from self-hosted deployer runners; there are no cloud-managed instance groups.

- **`zakura-mainnet-deploy.yml`** — manual, binary-only deploy across the mainnet fleet. Builds `zakurad` natively on the `zakura-mainnet-deployer` runner, then installs it host-by-host with `deploy/deployer/deploy.py`. Node configs, identities, and chain state are deliberately left untouched; the previous binary is kept as `.bak`.
- **`zakura-testnet-deploy.yml`** — the same for the testnet fleet, from the `zakura-testnet-deployer` runner.
- **`zakura-mainnet-rollback.yml`** — emergency rollback for a single mainnet node: captures diagnostics, restores `<bin_path>.bak`, restarts the service.
- **`zakura-continuous-sync.yml`** — twice-hourly audit (plus manual deploy/status/resume actions) of the continuous genesis-sync fleet, which permanently re-syncs from genesis to catch sync regressions.
- **`zakura-pr-node.yml`** — reusable ephemeral real-node test of a PR or ref: boots a droplet from the pre-baked image, attaches a chain-state snapshot clone (`tip`, `pre-checkpoint`, `sandblast`, or `genesis`), builds the branch incrementally, runs it, and posts a metrics summary as a PR comment. `pre-checkpoint` picks the highest retained snapshot below the branch's max checkpoint, then independently reads the restored database tip before networking starts. It fails unless C is finalized, the best chain reaches C+1, and `state.vct.fast.block.count` confirms the Zakura `tree_aux` fast path processed blocks.
- **`zakura-pr-node-bake.yml`** — bake in `nyc1` and `sfo3`, weekly and when provisioning changes merge to `main`, of the golden PR-node droplet image (build deps, warm cargo cache), per-network chain-state volume snapshots, and an optional dedicated Mainnet pruned snapshot 100 blocks below the current VCT handoff. Set `rebuild_approach_from_sandblast` on a manual dispatch to build that rare fixture forward from the retained historical archive; ordinary weekly bakes copy a disposable clone of the closest retained dedicated approach fixture into each region and verify its height with the new binary. Each region can complete independently. Failed bakes alert Slack; the reaper sends one daily alert when a regional image is missing or older than 14 days.
- **`zakura-pr-node-reaper.yml`** — hourly TTL cleanup backstop for PR-node resources. In each region it keeps two images, six Mainnet state generations, three Testnet generations, and two dedicated approach generations. It also pins the highest ordinary and dedicated Mainnet fixture below the current handoff. Fresh artifacts get a one-day grace period.
- **`zakura-vct-handoff-canary.yml`** — daily, manual, reusable, and release-state-PR Mainnet test that forces the Zakura P2P `tree_aux` path from the pinned pre-checkpoint state and exits after C is finalized and the best chain reaches C+1 with VCT fast-path activity. Failures alert `#zakura-alerts` with their phase; the release producer adds only missing labels, and unrelated label additions do not start another canary; trusted same-repository `A-release-state` PRs run the crossing as a PR check, while normal releases dispatch it as an advisory check.
- **`checkpoint-sync-bench.yml`** — manual fixed-height sync benchmark with checkpoint or semantic verification on an ephemeral DigitalOcean droplet (baked image + Mainnet sandblast state). Uploads metrics series, bottleneck verdicts, and logs as artifacts for local dashboard replay; tears down the droplet and volume when the run ends (hourly reaper as backstop).
- **`zakura-perf-bench.yml`** — CPU profiles mainnet workloads on ephemeral droplets. Historical mode restores `sandblast` state for fixed-height throughput and optional parallel A/B; live-head mode uses the production default P2P stack, catches up from the baked pruned tip, and requires head health throughout one observational profile window. Both produce flamegraphs, CPU counters, metrics, available traces, and block-latency digests (see `docs/cpu-profiling.md`).

These workflows use the helper scripts in `.github/workflows/scripts/` (`pr-node-bake.sh`, `pr-node-run.sh`, `pr-node-monitor.py`, `perf-bench-run.sh`, `perf-bench-compare.py`, `checkpoint-sync-bench-run.sh`).

After initially deploying this automation, manually dispatch the PR-node bake
with `rebuild_approach_from_sandblast` enabled. This creates the first
`zakura-vct-approach-mainnet-*` snapshot without depending on rollback data
that pruned state snapshots do not retain.

Droplet lifecycle is shared, not copy-pasted, through the composite actions in `.github/actions/`:

- **`do-cli`** — installs the pinned `doctl`, authenticates it for the rest of the job, and optionally writes the fleet SSH key to `/tmp/do_ssh`.
- **`do-droplet`** — resolves the newest `zakura-pr-node-*` baked image (or an exact `image_id`) and optionally clones either the newest network state or an exact volume snapshot, then creates the tagged droplet. Outputs `id`, `ip`, `region`, `size`, `image_id`, `state_snapshot_id`, `snapshot_height`, `volume_id`, and `volume_name`. Selection and lifecycle code is shared with image baking in `scripts/do_provision.py`. The exact selected resources are recorded in `provisioning.json`.
- **`do-wait-ssh`** — polls a new droplet until it accepts SSH. Give the step an `id` if teardown needs to distinguish "unreachable" from "ran".
- **`do-teardown`** — best-effort delete of a droplet and its volumes, recovering missing IDs from deterministic names and retrying volumes while the detach settles. Never fails a job; `enabled: false` keeps the resources and says so.

`do-droplet` has three explicit policies:

- `fixed` uses exactly the requested size. Benchmarks also pin `nyc1` so their legs cannot silently change hardware class or region.
- `correctness` prefers the requested size, then chooses catalog sizes with at least its CPU and RAM, enough image disk, and an hourly price at most $0.50. This permits shared CPUs for correctness tests; the existing time limit still applies.
- `bake` uses stock Ubuntu and blank state volumes, with the same resource rules plus an exact 100 GB root disk. This prevents a fallback bake from making the image incompatible with 100 GB runtime machines.

Runtime regions are tried in order (`nyc1,sfo3,nyc3`). Only regions with an available compatible image and the requested state are eligible. An explicit artifact ID is never substituted. Pre-checkpoint selection requires a known height strictly below C; the restored database is still checked independently before networking starts. Existing date-only states remain usable for tip and sandblast tests.

Within each region, the newest image is tried first. Older retained images add fallback sizes when their smaller disk requirement permits machines that the newer image cannot use. Each size is tried once per region with its newest compatible image.

The provisioner only retries an explicitly rejected capacity request. Ambiguous creation errors recover deterministic resource names and clean up instead of issuing another create. Workflow teardown and the hourly reaper remain cleanup backstops. Regional retention is essential: a global newest-two policy could delete the only image or approach fixture in the fallback region.

Bakes on a branch write `zakura-pr-validation-*` artifacts, which scheduled jobs never select. The workflow uploads their exact IDs for explicit `image_id`/`state_snapshot_id` PR-node validation. Delete those temporary images and snapshots after validation. Only a bake on `main` publishes the ordinary artifact names. Manual `notify=false` and PR-node `post_comment=false` allow quiet validation.

State downloads resume across bounded connections until one shared deadline, four hours after the bake script step starts. Compilation and earlier downloads consume that same budget. Each connection lasts at most ten minutes, with its timeout and retry delay shortened near the deadline. The seven-hour job reserves the remaining three hours for extraction, fixture copying, snapshots, and cleanup. This permits slow but viable downloads without an attempt-count cap; it cannot make an archive that needs days fit into the job.

To validate a change, run the provisioning regression tests and a read-only catalog plan, then bake both regions on the branch. Run a pre-checkpoint PR-node test against each region's exact artifact IDs with the Zakura P2P stack and immediate teardown. Verify finalized C, best height C+1, VCT activity, and cleanup before promoting the code. A successful fallback catalog plan alone is not a successful handoff test.

## Fork maintenance

- **`upstream-sync.yml`** — scheduled and manual discovery/triage of upstream `ZcashFoundation/zebra` PRs, with conservative adaptation; opens at most one downstream draft PR per run.

## Conventions

- **Merge queue, not Mergify.** PRs land through GitHub's native merge queue; `lint.yml`, `tests-unit.yml`, `test-crates.yml`, `semver-checks.yml`, and `zakura-e2e.yml` re-run on every queued entry via `merge_group`.
- **Required unit-test check.** Only `test success` is intended to be globally required. `tests-unit.yml` therefore triggers on every PR: its lightweight `changes` job detects test-relevant paths, the heavy jobs skip on irrelevant PRs, and its always-running `re-actors/alls-green` summary reports exactly once. `lint.yml`, `test-crates.yml`, and `semver-checks.yml` retain workflow-level PR path filters and are advisory checks, avoiding any runner use when they are irrelevant. `zakura-e2e.yml` remains independently self-gated. When changing the unit workflow's relevant paths, update both its `push.paths` globs and its `changes` filter. Keep `allowed-skips` limited to jobs whose documented `if:` conditions deliberately skip them; update the detector, job conditions, and summary together. There is no separate patch workflow to keep in sync. The `crates.io publish graph` job is in this workflow for that reason: semver-checks cannot see a dependency major underneath unchanged type names, and an advisory workflow would not keep `main` publishable.
- **Crate inputs are broader than Rust source.** Keep `crates/**` in the lint, unit-test, and crate-build relevant-path sets. Crates consume non-Rust build and test inputs including verifying keys, protobufs, snapshots, checkpoints, and test vectors; narrowing this to `*.rs` and Cargo manifests would let those inputs change without running the affected checks. Keep `rust-toolchain.toml` in the lint paths as well because the supply-chain job's Rust setup consumes the repository toolchain file.
- **Label-gated heavy jobs.** `C-benchmark` runs benchmarks on a PR; `run-zakura-e2e` forces the e2e suite on a PR that wouldn't otherwise trigger it.
- **Fork PRs.** Repository secrets and variables are not available to workflows on PRs from forks, so fleet and PR-node workflows are dispatch-only from this repository.
- **Mainnet checkpoints arrive through the release-state pipeline.** `update-release-state.yml` imports a publisher-produced bundle and opens a draft PR; nothing merges or ships from it automatically, and checkpoint PRs remain consensus-critical and need careful review. The manual [`zakura-checkpoints` instructions](../../crates/zakura-utils/README.md#zakura-checkpoints) cover Testnet updates and diagnostics, and are what the publisher itself runs. Do not hand-append RPC-mode output to the Mainnet list: it would land off the deterministic selection grid the import checks against.
