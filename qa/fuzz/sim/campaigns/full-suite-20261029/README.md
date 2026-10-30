# Full-suite campaign 2026-10-29

This configuration starts an intentionally incomplete campaign over every
mapped fuzz target. The manifest records the exact input PR heads. The
deployment record adds the integration commit, toolchain output, version
reports, seed digest, corpus digest, and start time.

## Validate and populate

Run these commands from the repository root. Set `CAMPAIGN_ROOT` to persistent
disk. Do not put the campaign under `/tmp` or `/dev/shm`.

```console
export CAMPAIGN_ROOT=/srv/zakura-fuzz/campaigns/full-suite-20261029
export RUSTFLAGS="--cfg zakura_sim"
cargo +1.97.0 test -p zakura-sim --locked
cargo +1.97.0 xtask sim-seeds --report "$CAMPAIGN_ROOT/seed-report.json"
cargo +1.97.0 xtask sim-seeds --sim block-sync/storage --out "$CAMPAIGN_ROOT/corpus/block_sync_storage"
cargo +1.97.0 xtask sim-seeds --sim header-sync/wire --out "$CAMPAIGN_ROOT/corpus/header_sync_wire"
cargo +1.97.0 xtask sim-seeds --sim header-sync/storage --out "$CAMPAIGN_ROOT/corpus/header_sync_storage"
cargo +1.97.0 xtask sim-seeds --sim discovery/wait-complete --out "$CAMPAIGN_ROOT/corpus/discovery_wait_complete"
```

Build and smoke every target from `qa/fuzz/sim`:

```console
export RUSTFLAGS="--cfg zakura_sim"
for target in block_sync_storage header_sync_wire header_sync_storage discovery_wait_complete race_fixtures feedback_probe; do
  cargo +nightly-2026-07-15 fuzz build --fuzz-dir . "$target"
done
cargo +1.97.0 xtask sim-feedback
```

Run each correctness target for 60 seconds before the unbounded campaign.
Use its populated corpus and a distinct artifact directory.

## Launch and resume

The launch command and the resume command are identical. libFuzzer resumes
from each target's persistent corpus. Run one service per target. Set
`ZAKURA_SIM_ARTIFACTS` to preserve classified inputs and traces outside the
cargo-fuzz artifact directory.

```console
cd qa/fuzz/sim
export RUSTFLAGS="--cfg zakura_sim"
export ZAKURA_SIM_ARTIFACTS="$CAMPAIGN_ROOT/evidence/block_sync_storage"
cargo +nightly-2026-07-15 fuzz run --fuzz-dir . block_sync_storage "$CAMPAIGN_ROOT/corpus/block_sync_storage" -- -jobs=3 -workers=3 -max_len=4096 -timeout=600 -rss_limit_mb=4096 -artifact_prefix="$CAMPAIGN_ROOT/artifacts/block_sync_storage/"
```

Repeat that command with these substitutions:

| Target | Jobs and workers | Timeout seconds |
| --- | ---: | ---: |
| `header_sync_wire` | 2 | 180 |
| `header_sync_storage` | 1 | 120 |
| `discovery_wait_complete` | 1 | 60 |
| `race_fixtures` | 1 | 60 |

The first deployment uses AddressSanitizer. A later throughput rotation may
add `-s none --no-default-features`, but it must use separate binaries, logs,
and service names so the deployment record identifies each mode.

## Reporting

Treat every crash, timeout, abandonment, nondeterministic result, and property
violation as evidence. Preserve the input before triage. Replay ordinary
property failures exactly. Report correctness findings separately from C1-C4
completeness. Missing H1, H2, H4, H5, P8, and typed protocol-state collectors
keep this campaign incomplete even if every running target stays clean.
