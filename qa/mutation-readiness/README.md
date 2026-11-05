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

## mut-1.2: patch and test bindings

Pinned source: `870678be1739ea799e59b4e0bdfe6cf512792710`, with the
metadata from this PR.
This section adds binding metadata and one fuzz-target declaration fix.
It does not complete mut-1.2's compilation and sensitivity gate.

### What stays in the repository

- `qa/native-mutations/cases.json` gives all 22 native patches a package,
  an exact test selector, a source file and an intended assertion.
  The handoff-floor case uses a `zakura-sim` integration test and needs
  `RUSTFLAGS='--cfg zakura_sim'`. Its new README names that test.
  These mappings name native assertions, not registered simulator IDs.
- `scripts/mutation_inventory.py` inventories all 34 planted and 22 native
  patches without applying them. It runs `git apply --check` on each patch.
  For planted cases it checks the input hash, the sidecar's `holds` witness
  for the intended property and the simulator target binaries.
  For native cases it checks the package and the selector. A library
  selector must start with its file's module path, and an integration
  selector's file must be `tests/<test_target>.rs`. Otherwise `--exact`
  would select zero tests.
- `qa/fuzz/p2p/Cargo.toml` now lists `discovery/answer-settlement` for
  `discovery_query`. That target already replays every input through
  `replay_discovery_settlement_bytes`; only the metadata omitted the ID.

### Reproduce

```sh
python3 scripts/mutation_inventory.py --output "$TMPDIR/mutations.json"
python3 -m unittest discover -s scripts/tests -p test_mutation_inventory.py -v
python3 scripts/check-fuzz-campaign.py
```

For one native case, run the command that the inventory emits with the
recorded flags. First append `--list` and confirm the exact selector.
Then require one executed test on the unchanged source, the intended
assertion on the patched source, and a pass after restoring the source.

### Expected outcome

| Check | Expected outcome |
| --- | --- |
| Inventory | 56 patches; 0 metadata or application failures. 22 planted cases map to simulator fuzz targets. 12 use `block-sync/storage`, which has no fuzz target. |
| Inventory tests | 8 pass |
| `check-fuzz-campaign.py` | Passes |
| `decode_admission_precedes_materialization_and_survives_final_holders` | 1 passed |
| `standalone_transaction_reservation_precedes_decode_and_outlives_clones`, unchanged | 1 passed |
| Same test with `state/transaction-results/early-transaction-release.patch` | Fails with `a live standalone allocation retains its charge`: 0 against 435 |
| Same test after restoring the source | 1 passed |

The native run used Rust 1.99.0 on Linux, `--cfg zakura_sim`, the locked
dependencies and the test profile without debug information. It applied
only `early-transaction-release.patch`. The other 21 native selectors are
checked statically only. No discovery campaign ran.

### Slot reconciliation

| Slot | Evidence at the pinned source | Remaining gap |
| --- | --- | --- |
| own-1, header reactor | The `header/reactor-policy` planted case passed mut-1.1 sensitivity. | H2 wrong-owner release, retention bypass, missed wakeup and stale-completion mutations are absent. Reuse the adopted-state work in #2032/#2033 where its checks apply. |
| own-4, block reactor | Three `block/reactor-policy` patches and their coverage row exist. | The `block-sync/reactor-query-lifecycle` row still marks the bypass pending: no committed input reaches action-channel refusal. |
| own-5, block fill and peers | The handoff-floor patch maps to an integration test. The p2p machine table maps block fill, request lifecycle and peer-registry properties. | The handoff mapping has no registered replay property. Broader B3/B4 mutations and integrated observations remain open. |
| own-8, discovery | The planted discovery cases have simulator mappings. Four discovery machine targets exist. | The two `qa/mutations/discovery/worker-lifetime/` patches are outside both retained runners and have no case sidecars. |
| own-9, sessions and credit | `held_credit_bounds_a_greedy_peer`, `releasing_a_credit_hold_unblocks_the_sender` and `grants_match_an_independent_ledger` are candidate checks. | No retained held-credit mutation exists. |
| own-10, backend reads | All five read-worker patches apply. Both private-pool patches already use `ReadWorkerPool`. | Compilation, intended assertion failure and deterministic replay remain open. |
| own-11, backend results | Disk-result and transaction-result patches have exact selectors. `early-transaction-release` has baseline, mutant and restored results. | The other backend cases need executable selection and sensitivity runs. |

### Evidence bundle

`2039-mut-1.2-2026-11-05.tar.zst`, SHA-256
`ba5edbd8b776c64d9a94eb91c8659a9277af1d5f2888c025962768ae711d020f`.
It holds the original run README, the static inventory output, the native
run record and its logs. That inventory output was generated from an
uncommitted worktree, so its `revision` field names the parent commit.

```text
2039-mut-1.2-2026-11-05/README.md
2039-mut-1.2-2026-11-05/SHA256SUMS
2039-mut-1.2-2026-11-05/inventory.json
2039-mut-1.2-2026-11-05/logs/{decode-baseline,native-baseline,native-mutant,native-restored,node-tests}.log
2039-mut-1.2-2026-11-05/native-result.json
```
