# Zakura sync perf runbook — deterministic isolated env

Measure/debug local sync performance from the 1.8M snapshot, reproducibly. The
bench node syncs a fixed range (≈1.80M→1.83M) peering **only** with two frozen
serving nodes we control, over a private Zakura cohort (the `dev_network`
feature, PR #262), so other engineers churning the public fleet can't perturb a
run. Background: `book/src/dev/private-zakura-network.md`.

Everything lives in `/root/zakura/deploy/runner/` (untracked local tooling).
`perf.sh` is the entry point — it wraps the deployer and the run/analyze scripts.
Repo-relative paths below are from the repo root `/root/zakura/`.

## Layout

- `perf.sh` — the one entry point (setup + run loop).
- `cohort.env` — single source of truth: cohort tag, the two serving hosts, their
  captured `node_id@ip:8234`, serve commit, seed height, bench binary path.
- `zebra-bench-config.toml` — the bench node config (tokenized; `perf.sh` fills
  the cohort tag + peers, `feed_run.sh` fills the per-run tokens).
- `nodes.toml.tmpl` — deployer config for the two serving nodes.
- `feed_run.sh` / `feed_analyze.py` / `zebra-metrics-dashboard.py` —
  the run engine, the analyzer, and the live dashboard (all called by `perf.sh`).
- All host-specific paths (snapshot source, fork dir, work dir, bench binary) are
  the `BENCH_*` vars in `cohort.env` — nothing is hard-coded in the scripts.
  Defaults put the instrumented binary + CSVs under `/root/wal-bench/` and forks
  under `/mnt/roman-dev-2-data/`.

## One-time setup — the two frozen serving nodes

Edit `cohort.env` (`COHORT_TAG`, the two `NODE_*_SSH`/`IP`, `SERVE_COMMIT` = a
branch containing #262), then:

```bash
cd deploy/runner
./perf.sh seed-serving     # deploy both nodes (legacy_p2p=true) → sync from public mainnet
./perf.sh status           # repeat until both heights >= SEED_HEIGHT
./perf.sh peers            # capture each node_id@ip:8234 from logs into cohort.env
./perf.sh freeze-serving   # redeploy legacy_p2p=false → static cohort servers
```

Frozen = seeded once past the bench range, then cut off from public so the nodes
serve a byte-identical range every run (no public dependency, no self-sync
jitter). Each node's iroh identity persists in its state cache dir — **never wipe
it**, or the captured peer ids change.

The deployer renders the cohort config itself: `deploy/deployer/deploy.py` reads a
fleet-wide `[defaults.zakura]` table plus `storage_mode` / `v2_p2p` /
`legacy_p2p`, so there is no hand-editing of `/etc/zakura` on the nodes.

## Run loop (the repeatable part)

```bash
./perf.sh build-local                 # once per code change: rebuild the commit-metrics binary
./perf.sh run r1 1822000              # fork the snapshot, sync isolated to the 2 nodes → CSV
./perf.sh analyze r1 1806000 1820000  # steady-state window: throughput + commit breakdown + verdict
./perf.sh dashboard                   # optional live panels (auto-detects the run's metrics port)
./perf.sh verify-isolation 19980      # confirm only the 2 cohort peers, no rejects
```

`run` renders the cohort tag + peers into the bench config and hands it to
`feed_run.sh` (args: `LABEL BIN [stop] [met] [maxsec]`); CSVs land at
`$BENCH_WORK_DIR/feedrun-<label>.csv`. Re-running is just `run` + `analyze`.

- **Determinism check:** two back-to-back labels should match within noise.
- **Isolation check:** `verify-isolation` (or the `zakura.p2p.*` metrics / the
  Zakura `conn`/`handshake` trace tables) shows a stable 2-peer set with no
  growing `wrong_network`/`wrong_chain` rejects; kill one serving node and the
  bench peer count drops to 1.

## Bench config knobs that matter (`zebra-bench-config.toml`)

- `[network]` `v2_p2p=true`, `legacy_p2p=false` — v2-only, so the bench node's
  only peers are the cohort.
- `[network.zakura]` `listen_addr="0.0.0.0:8234"` — **not loopback** (iroh uses one
  UDP socket for send+recv, so a loopback bind breaks outbound). `dev_network` and
  `bootstrap_peers` are token-filled by `perf.sh` from `cohort.env`.
- `[network.zakura.block_sync] replace_legacy_syncer=true` — Zakura owns block
  download; the legacy syncer does tip discovery only.
- `[consensus] checkpoint_sync=true` — selects the VCT peer-source fast path.
- Binary: release + `commit-metrics` → `/root/wal-bench/zakurad-notecommit-instr`
  (built by `perf.sh build-local`).

## Snapshot

- Pristine source: `/mnt/roman-dev-2-data/zebra-ckpt-1800000` (archive, tip
  1,800,000, format 27.2.0). Never run against directly.
- Warm master: `/mnt/roman-dev-2-data/zebra-ckpt-1800000-warm` (tip 1,802,000).
  `feed_run.sh` makes a hard-link fork per run and breaks links on the
  RocksDB metadata + `version`. If the branch DB format changes, re-check
  `zebra-state/src/constants.rs` and relabel the warm master `version`.

## Gotchas

- **Kill nodes by PID, not name** — `comm` truncates to `zebrad-notecomm`, so
  `pkill -x zebrad` misses it and `pkill -f zebrad-notecommit-instr` kills your
  own shell. Scan `/proc/*/exe`, `kill -9`.
- **Unique metrics/listen port per relaunch** — ports linger in TIME_WAIT.

---

> The `deploy/deployer/` changes (deploy.py + template + nodes.example.toml) are
> tracked deployer improvements — commit them separately, **not** inside the
> PR #262 zakura-feature PR. The `deploy/runner/` files are untracked local tooling.
