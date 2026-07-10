# DO PR-node tester — zcashd-compat extension (stage 2)

Status: **planned** (not yet implemented). Extends the ephemeral per-PR
DigitalOcean test nodes shipped in
[#70](https://github.com/zakura-core/zakura/pull/70) (`zakura-pr-node.yml` /
`zakura-pr-node-bake.yml` / `zakura-pr-node-reaper.yml`, docs in
`docs/pr-node-do-setup.md`) with two **zcashd-compat flavors**: a run mode that
boots `zebrad` in zcashd-compat mode alongside a real `zcashd` hard-locked to
it, and reports height-drift metrics in the PR summary.

## Motivation

The compat deployment (`deploy-zcashd-compat.yml`, sidecar layout documented in
`deploy/zcashd-compat/`) is one of the riskiest surfaces to change: it couples
zebrad's zcashd-compat RPC/P2P behavior to a live zcashd whose sync depends on
it. Today the only way to test a PR against it is the single hand-provisioned
compat host per network — mutually exclusive with everything else the host
does. The stage-1 PR-node system gives us ephemeral, self-cleaning nodes with
PR summaries; this extension makes the compat pairing testable the same way:
dispatch a PR, get a droplet running `zebrad --zcashd-compat` + `zcashd
-connect`-locked to it, get drift metrics posted to the PR, auto-delete in 24h.

## Design

Reuses the stage-1 architecture unchanged: one baked droplet image (deps + warm
cargo cache + loopback `deploy.py` identity), per-flavor DO **volume
snapshots** holding pre-extracted state, hourly reaper. The compat flavor adds
a zcashd binary + datadir to the volume and a second supervised process to the
run.

### New snapshot inputs: published zcashd datadir archives

Decision (Roman): seed compat state from **published archives**, not by
rsyncing the live compat hosts and not by syncing from scratch.

1. Add a small publishing flow (systemd timer or manual runbook step on each
   compat host) that tars the zcashd datadir at a quiesced moment and uploads
   it next to the existing chain snapshots:
   - testnet: the snapshots site already hosts exactly this artifact
     (`zcashd-pre-ironwood-h4129124-*.tar.zst`, ~16 GiB, published 2026-07-08
     in `snapshots.json` with `zebraVersion: "zcashd"`) — formalize the
     cadence rather than invent a new mechanism.
   - mainnet: publish `zcashd-mainnet-<stamp>-<height>.tar.zst` to the
     `zebra.valargroup.org` Space under a new `zcashd-mainnet/` prefix with a
     `latest.json` pointer, mirroring the existing zebra-snapshots layout.
   - Each publish records `sha256`, `height`, and the zcashd version string.
2. Publish the **zcashd binary** (and `fetch-params` output if not vendored)
   alongside, so the bake never builds zcashd. The compat hosts' binary is the
   reference build; upload it on the same cadence as the datadir.

### Bake changes (`zakura-pr-node-bake.yml` + `pr-node-bake.sh`)

Two additional volume snapshots per bake, sized per the earlier capacity
decision:

| flavor | volume size | contents |
|---|---|---|
| `compat-mainnet` | 650 GiB | `zebra/` (archive-mode zebrad state — compat serves headers/blocks to zcashd, pruned is not sufficient), `zcashd/` (datadir from the published archive), `bin/zcashd` |
| `compat-testnet` | 150 GiB | same layout, testnet artifacts |

Bake steps mirror the existing `fetch_state` flow: stream-extract with
sha256 verification, assert the expected `state/v*/<network>` (zebra) and
`blocks/`+`chainstate/` (zcashd) trees, record the seed heights into a
`seed-meta.json` on the volume for the run summary. Compat volumes are baked
by the same weekly job; failures of the compat legs must not fail the
zakura-only legs (bake them in independent steps so stage-1 assets always
refresh).

Reaper: extend the keep-last-2 volume-snapshot pruning loop with the two new
`zakura-pr-state-compat-{mainnet,testnet}-` prefixes. No other reaper changes —
droplet/volume TTLs are flavor-agnostic.

### Run changes (`zakura-pr-node.yml` + `pr-node-run.sh`)

- New `snapshot_mode: compat` (valid with both networks). Selects the
  `compat-<network>` volume snapshot; disk sizing means compat-mainnet needs a
  larger `droplet_size` default (the 650 GiB volume is attached storage, but
  memory matters: zcashd + archive zebrad want ≥32 GiB — `g-8vcpu-32gb`).
- `pr-node-run.sh` compat path:
  1. Mount volume; fleet.toml points `state_cache_dir` at `/mnt/snapshots/zebra`
     with `storage_mode = "archive"`, and enables the zcashd-compat surface the
     same way the compat hosts' config does (compat listen/RPC ports matching
     what the baked zcashd datadir's config expects).
  2. Build + deploy zebrad via `deploy.py` exactly as today.
  3. Start zcashd from the volume (`/mnt/snapshots/bin/zcashd
     -datadir=/mnt/snapshots/zcashd -connect=127.0.0.1:<compat p2p port> ...`),
     process-supervised the same way the compat host runs it (nohup + pattern,
     mirroring `deploy_kind = "process"` semantics; systemd unit optional
     later). The `-connect` hard-lock is the point: zcashd can only sync
     through the PR's zebrad.
  4. Monitor: run `deploy/zcashd-compat/sync-check.sh` (already parameterized
     via `ZEBRA_RPC_URL`/`ZCASHD_RPC_URL`/cookie paths/`HEIGHT_MAX_DRIFT`) once
     at startup as a gate, then extend `pr-node-monitor.py` to sample both RPCs
     each interval and add to `summary.json`/`summary.md`: zcashd height,
     zebrad→zcashd drift (max/last), zcashd RSS, and a `failed` verdict if
     drift exceeds `HEIGHT_MAX_DRIFT` for N consecutive samples or zcashd
     exits.
- PR comment marker becomes `<!-- zakura-pr-node:compat:<network> -->` — the
  existing per-mode×network upsert logic needs no change.

### Validation / DoD for the implementation PR

Same incremental pattern that landed stage 1:

1. Publish one testnet zcashd archive through the new flow (or bless the
   existing site artifact) + one mainnet archive; record sha256s as repo vars.
2. Bake with the two compat legs; verify 4 volume snapshots exist.
3. `compat`/`testnet` 15-min smoke vs a real PR: zcashd follows zebrad within
   drift bounds; PR comment shows both heights.
4. `compat`/`mainnet` 30-min run; verify drift metrics and teardown.
5. Reaper `max_age_hours=0` sweep leaves nothing (droplets, 650 GiB volume).
6. Evidence links in the PR description.

## Costs

- Standing: 2 extra volume snapshots ≈ 800 GiB ≈ $40/mo (snapshot storage is
  billed at ~$0.05/GiB-mo) on top of stage 1's ~$30–40/mo.
- Per compat-mainnet run: 650 GiB volume ≈ $2.2/day pro-rated + g-8vcpu-32gb
  droplet ≈ $0.38/h (~$9 if kept the full 24h).

## Risks / open questions

1. **zcashd datadir portability** — the datadir embeds paths/settings from the
   compat host (`zcash.conf`, cookie paths). The bake must rewrite a minimal
   `zcash.conf` (datadir-relative, `-connect` target, RPC cookie) rather than
   trust the archived one. Verify a restored datadir opens cleanly on first
   boot before wiring the full flow.
2. **Compat zebrad config drift** — the compat hosts' zebrad configs are
   hand-tuned (`deploy-zcashd-compat.yml` deliberately does binary-only
   swaps). The PR-node compat config is a fresh render, so it must be reviewed
   once against a live compat host's config to catch load-bearing settings
   (ports, indexes) the template doesn't cover.
3. **Seed freshness** — a stale zcashd seed makes the first minutes all
   catch-up through zebrad, which is signal too (it exercises compat serving),
   but bounded staleness (< ~1 week) keeps the hour meaningful. Weekly publish
   cadence matches the weekly bake.
4. **Ironwood activation on mainnet** (height 3,428,143) — pick seeds on the
   compatible side of activation for whatever the compat pairing is expected
   to validate at the time of implementation; pre/post-activation seed pairs
   may both be worth publishing around the upgrade window.
