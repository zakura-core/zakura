# Mutation readiness

This directory records mut-1 of the
[mutation plan](https://valargroup.getoutline.com/doc/6af54d3b-7ca5-4f05-975d-77893109e7ad).
Each section names a pinned commit, the commands that reproduce its checks,
and the expected outcome of each check.
Dated logs, traces and reports live in an evidence bundle outside the
repository. The mutation plan in Outline holds each bundle as an attachment.
Each section records its bundle's SHA-256 and file list.

## mut-1.1: baseline and one-case sensitivity smoke

Pinned source: `870678be1739ea799e59b4e0bdfe6cf512792710`.
Host: Linux x86_64, Rust 1.99.0, the committed lockfile.
This run starts the baseline inventory. It does not close the full
retained-suite readiness gate.

### Build

Disable debug information and incremental compilation to bound disk use.
The simulator keeps the dev/test profile's debug assertions and overflow
checks. Rust 1.94.0 refuses this commit, which requires Rust 1.97 or later.

```sh
export CARGO_TARGET_DIR="$TMPDIR/mut-1-baseline/build"
export CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
export RUSTFLAGS='--cfg zakura_sim'
cargo +1.99.0 test --locked -j 8 -p zakura-sim \
  --test witness --test block_semantic_properties \
  --test header_wire_lifecycle --no-run
```

Set `SIM` to `$CARGO_TARGET_DIR/debug/zakura-sim`. Copy that executable
before building the mutant. Set `OUT` to a fresh disk-backed directory.

### Sensitivity smoke

The smoke uses one existing production mutation,
`qa/planted/header/reactor-policy/locator-call-settles-without-removal.patch`,
and its committed witness `crates/zakura-sim/tests/fixtures/header-reactor-policy.bin`.
The patch leaves a settled storage call in the pending-call map.
The `header/reactor-policy` model must reject that state.

```sh
FIXTURE=crates/zakura-sim/tests/fixtures/header-reactor-policy.bin
timeout 30 "$SIM" record --sim header/reactor-policy --input "$FIXTURE" \
  --out "$OUT/header-baseline" --witness header/reactor-policy --expect holds
timeout 30 "$SIM" replay "$OUT/header-baseline"
```

Apply the patch in a separate worktree at the pinned commit. Build it with
the same environment and
`cargo +1.99.0 build --locked -j 8 -p zakura-sim --bin zakura-sim`.
Set `MUTANT_SIM` to that executable.

```sh
timeout 30 "$MUTANT_SIM" record --sim header/reactor-policy --input "$FIXTURE" \
  --out "$OUT/header-mutant" --witness header/reactor-policy --expect violated
timeout 30 "$MUTANT_SIM" replay "$OUT/header-mutant"
timeout 30 "$MUTANT_SIM" confirm --sim header/reactor-policy --input "$FIXTURE" \
  --property header/reactor-policy --out "$OUT/header-confirm"
```

Then repeat the unchanged recording with the saved executable and a fresh
output directory.

### Reported failures

Run each test executable from the build above under `timeout 60`.

| Executable | Arguments |
| --- | --- |
| `witness` | `--test-threads=1` |
| `block_semantic_properties` | `capacity_release_wakes --include-ignored --test-threads=1` |
| `header_wire_lifecycle` | `saved_plans_replay_their_end_records --exact --test-threads=1` |

Check the reported test counts. A zero exit with only ignored tests is not a
passing baseline.

### Expected outcome

| Check | Expected outcome at the pinned commit |
| --- | --- |
| Unchanged header-reactor witness | `holds` passes after 26 steps with 23 applicable checks; Exact replay matches |
| Mutated header-reactor witness | `header/reactor-policy` is violated at step 7; Exact replay matches |
| Fresh-process confirmation | Confirms `header/reactor-policy` |
| Unchanged witness after the mutant run | `holds` passes again |
| `witness` | 9 pass; `discovery_wait_complete_lost_wakeup` and `recorded_witness_judges_its_own_trace` fail with lost trigger history |
| `saved_plans_replay_their_end_records` | Fails on `honest-sync`: 30,049 steps against a saved 30,062 |
| `capacity_release_wakes` without `--include-ignored` | 0 run, 2 ignored; no baseline evidence |
| `capacity_release_wakes` with `--include-ignored` | Frontier-first exceeds the 60-second limit in the debug profile; reply-first does not start |

The smoke caught its single mutation. That result establishes sensitivity
for this committed witness only. It establishes no discovery rate.
The lost-trigger and header-wire failures reproduce on Linux at the pinned
commit. The capacity timeout leaves that baseline unresolved: it neither
passes nor reproduces a property failure. The test's own ignore reason says a
debug run takes about eight minutes. A remote droplet runs it later with
`--profile planted`.

The baseline build took 57.25 seconds and the mutant build took 41.95
seconds. Each completed test process took less than three seconds.

### Evidence bundle

`2037-mut-1.1-2026-11-05.tar.zst`, SHA-256
`c489d5d2f6ed9dff1b3a9a5481a4f96481990d0bb0a09b433e7b1b1b513d62c2`.
It holds the original run README, `host.txt`, the process outcomes in
`results.json`, a 34-case planted inventory with patch and input hashes, the
four trace directories, the confirmation record and the logs.
`SHA256SUMS` inside the bundle covers every other file.

```text
2037-mut-1.1-2026-11-05/README.md
2037-mut-1.1-2026-11-05/SHA256SUMS
2037-mut-1.1-2026-11-05/header-baseline-after/{input.bin,trace.jsonl,witness.json}
2037-mut-1.1-2026-11-05/header-baseline/{input.bin,trace.jsonl,witness.json}
2037-mut-1.1-2026-11-05/header-confirm/confirmation.json
2037-mut-1.1-2026-11-05/header-confirm/traces/header/reactor-policy/51646a106d69/{input.bin,trace.jsonl}
2037-mut-1.1-2026-11-05/header-mutant/{input.bin,trace.jsonl,witness.json}
2037-mut-1.1-2026-11-05/host.txt
2037-mut-1.1-2026-11-05/inventory.json
2037-mut-1.1-2026-11-05/logs/{baseline-after,baseline-replay,capacity,check-0,check-1,check-2,check-3}.log
2037-mut-1.1-2026-11-05/logs/{mutant-confirm,mutant-record,mutant-replay}.log
2037-mut-1.1-2026-11-05/results.json
```

### Scope left open

Other retained witnesses and planted cases remain unverified at this commit.
mut-1.2 owns the native mutation inventory. mut-1.3 owns missing witnesses
and fuzz adapters. mut-2 owns the case catalogue and frozen discovery inputs.
sim-9 stays deferred.
