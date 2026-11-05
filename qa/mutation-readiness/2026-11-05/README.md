# mut-1.1: bounded baseline evidence

This run starts the baseline inventory at
`870678be1739ea799e59b4e0bdfe6cf512792710`.
It does not close the full retained-suite readiness gate.

## Frozen scope

The short pilot uses one existing production mutation:
`qa/planted/header/reactor-policy/locator-call-settles-without-removal.patch`.
Its intended check is `header/reactor-policy`.
Its input is `crates/zakura-sim/tests/fixtures/header-reactor-policy.bin`.
The patch leaves a settled storage call in the pending-call map.
The existing model must reject that state after the witness reaches settlement.

Run the unchanged witness first, then the patched witness, then fresh-process
confirmation and Exact replay. Stop the pilot if the unchanged witness fails.
Limit each child to 30 seconds. Do not run a discovery campaign or repair a
witness during this run. This is a sensitivity smoke, not the planned
8–12-case campaign or evidence of generator discovery.

Separately run the `witness` integration test, the two capacity-release
tests in `block_semantic_properties`, and
`header_wire_lifecycle::saved_plans_replay_their_end_records`.
Limit each test process to 60 seconds. Keep baseline failures as open gaps.
These checks investigate reported failures without making them pilot cases.

## Build

Use Linux x86_64, Rust 1.99.0, the committed lockfile, and
`RUSTFLAGS='--cfg zakura_sim'`. Disable debug information and incremental
compilation to bound disk use. The simulator still uses the dev/test profile's
debug assertions and overflow checks. Build time is separate from test time.

```sh
export CARGO_TARGET_DIR="$TMPDIR/mut-1-baseline/build"
export CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
export RUSTFLAGS='--cfg zakura_sim'
cargo +1.99.0 test --locked -j 8 -p zakura-sim \
  --test witness --test block_semantic_properties \
  --test header_wire_lifecycle --no-run
```

The host's default `stable` toolchain was Rust 1.94.0. Cargo rejected it
before compilation because this commit requires Rust 1.97 or later.

## Scope left open

Other retained witnesses and planted cases remain unverified at this revision.
The native mutation inventory belongs to mut-1.2. Missing witnesses and fuzz
adapters belong to mut-1.3. The full catalogue and frozen discovery inputs
belong to mut-2. sim-9 remains deferred.

See the [mutation plan](https://valargroup.getoutline.com/doc/6af54d3b-7ca5-4f05-975d-77893109e7ad).

## Results

| Check | Result |
| --- | --- |
| Header-reactor unchanged witness | Passed: 23 applicable checks, 26 steps, trigger present, Exact replay |
| Header-reactor mutation | Intended `header/reactor-policy` violation at step 7; trigger present |
| Fresh-process confirmation | Confirmed; Exact replay passed |
| Unchanged witness after mutation run | Passed with the saved unchanged executable |
| `witness` integration test | 9 passed, 2 failed with lost trigger history |
| Header-wire saved end records | Failed on `honest-sync`; the end record differs |
| Capacity-release tests, default selection | 0 passed, 2 ignored; no baseline evidence |
| Capacity-release tests, explicitly enabled | Frontier-first exceeded 60 seconds; reply-first did not run |

The short pilot caught its single mutation. It establishes sensitivity for
this committed witness only. It establishes no discovery rate.
The discovery and header-wire failures reproduced on Linux at the pinned
revision. The capacity timeout leaves that baseline unresolved; it does not
reproduce a property failure. Run that test with the optimized `planted`
profile in a separate, longer run.

`inventory.json` enumerates all 34 retained planted cases and hashes their
patches and inputs. It records unexecuted cases as `not-run`.
`results.json` records process outcomes. The trace directories retain build
provenance, observations, property outcomes, and witness results.
`header-confirm/confirmation.json` records fresh-process confirmation.
`logs/` preserves test failures and replay output. `SHA256SUMS` covers these
artifacts. Log copies omit trailing whitespace. The baseline build took
57.25 seconds; the mutation build took
41.95 seconds. Each completed test process took less than three seconds.

The experiment used two source worktrees and a serial build cache. It saved
the unchanged executable before building the mutant. The trace metadata
records distinct source and executable digests. No tests, properties,
generators, or inputs changed during the experiment.

## Reproduce the checks

Run the build above from the pinned revision. Set `SIM` to the resulting
`$CARGO_TARGET_DIR/debug/zakura-sim`. Keep a copy of that executable before
building the mutation. Set `OUT` to a fresh directory on disk-backed storage.

```sh
timeout 30 "$SIM" record --sim header/reactor-policy \
  --input crates/zakura-sim/tests/fixtures/header-reactor-policy.bin \
  --out "$OUT/header-baseline" --witness header/reactor-policy --expect holds
timeout 30 "$SIM" replay "$OUT/header-baseline"
```

Apply the retained patch in a separate worktree at the pinned revision.
Build the mutant with the same environment and
`cargo +1.99.0 build --locked -j 8 -p zakura-sim --bin zakura-sim`.
Set `MUTANT_SIM` to that executable.

```sh
timeout 30 "$MUTANT_SIM" record --sim header/reactor-policy \
  --input crates/zakura-sim/tests/fixtures/header-reactor-policy.bin \
  --out "$OUT/header-mutant" --witness header/reactor-policy --expect violated
timeout 30 "$MUTANT_SIM" replay "$OUT/header-mutant"
timeout 30 "$MUTANT_SIM" confirm --sim header/reactor-policy \
  --input crates/zakura-sim/tests/fixtures/header-reactor-policy.bin \
  --property header/reactor-policy --out "$OUT/header-confirm"
```

Repeat the unchanged recording with the saved executable and a fresh output
directory. For the diagnostic tests, run the test executables printed by
the baseline build with these arguments. Apply `timeout 60` to each process.

| Executable | Arguments |
| --- | --- |
| `witness` | `--test-threads=1` |
| `block_semantic_properties` | `capacity_release_wakes --include-ignored --test-threads=1` |
| `header_wire_lifecycle` | `saved_plans_replay_their_end_records --exact --test-threads=1` |

Check the reported test counts. Exit status zero with only ignored tests
does not establish a passing baseline.
