# mut-1.2: patch and test bindings

This inventory builds on [mut-1.1](../2026-11-05/README.md) at
`aa98e054eac239817fa54a4b2687cbe71fb07612`.
That commit contains the baseline evidence for production source
`870678be1739ea799e59b4e0bdfe6cf512792710`.
This PR adds binding metadata and fixes one fuzz-target declaration.
It does not complete mut-1.2's compilation and sensitivity gate.

## Changes and checks

`qa/native-mutations/cases.json` gives all 22 native patches exact test
selectors and intended assertions. The handoff patch lacked adjacent test
instructions. Its new README identifies the existing simulator integration
test. The other 21 mappings come from their existing README tables.
These mappings identify native assertions, not registered simulator IDs.

`scripts/mutation_inventory.py` inventories all 34 planted and 22 native
patches. It checks application without modifying source. It validates planted
input hashes, sidecar witness IDs, dependency paths, and simulator target
binary paths. It checks native source symbols and package names. It records
the revision and worktree status because application checks read the worktree.
Malformed input fails the command. Missing or stale mappings remain failures.

The command reports all 56 patches applying and no metadata failures.
Of the 34 planted cases, 22 map to simulator fuzz targets and 12 use
`block-sync/storage`, which has no simulator fuzz target in this revision.
This does not establish that those 22 targets pass target readiness.

The `qa/fuzz/p2p` machine target `discovery_query` already invokes both
`replay_discovery_query_bytes` and `replay_discovery_settlement_bytes`.
Its metadata omitted `discovery/answer-settlement`. This PR adds that ID.
The fix does not add a simulator target or a `harness/coverage.rs` row.

Six checker tests reject missing/duplicate/orphan mappings, wrong source
symbols, stale patches, and a witness for the wrong property. They also
check that source lookup never claims executable selection. The existing
`scripts/check-fuzz-campaign.py` check passes.

## Bounded executable evidence

Rust 1.99.0 on Linux compiled the `zakura-node-services` library tests with
`RUSTFLAGS='--cfg zakura_sim'`, the locked dependencies, and the dev/test
profile. Debug information and incremental compilation were disabled.
The compiled list contains both exact selectors below. Each baseline
executed one test and passed:

- `block_memory::tests::decode_admission_precedes_materialization_and_survives_final_holders`
- `block_memory::tests::standalone_transaction_reservation_precedes_decode_and_outlives_clones`

Only `state/transaction-results/early-transaction-release.patch` was applied
for this native check. Its exact test compiled and failed with
`a live standalone allocation retains its charge`: actual charge 0, expected
435. After restoring the patch, the rebuilt exact test passed. The mutation
and restored builds each took about one second. No discovery campaign ran.

`logs/` retains the compiled list and the baseline, mutant, and restored
results. Log copies omit trailing whitespace. `native-result.json` records
the patch hash, flags, exact selector, and exit statuses. `inventory.json`
contains static results only; it deliberately does not import runtime
claims. The earlier header-reactor sensitivity result remains in mut-1.1.
All other mutation compilations and sensitivity runs remain unverified here.

## Reconciliation of the reported slot gaps

| Slot | Evidence at the pinned source | Remaining gap |
| --- | --- | --- |
| own-1, header reactor | The H1 `header/reactor-policy` planted case passed mut-1.1 sensitivity. | H2 wrong-owner release, retention bypass, missed wakeup, and stale-completion mutations remain absent from this retained inventory. H1 evidence does not close H2. Reuse the adopted-state work in #2032/#2033 where its checks apply. |
| own-4, block reactor | Three `block/reactor-policy` patches and its coverage row exist. `domain/block_sync_semantic/reactor_queries.rs` documents the runtime observation binding. | The `block-sync/reactor-query-lifecycle` coverage row still marks the bypass pending because no committed input reaches action-channel refusal. |
| own-5, block fill and peers | The handoff-floor patch maps to an existing integration test. `qa/fuzz/p2p/Cargo.toml` maps block fill, request lifecycle, and peer-registry machine properties. | The native handoff mapping does not supply a registered replay property. Broader B3/B4 fault-class mutations and integrated observation evidence remain open. |
| own-8, discovery | Existing planted discovery cases have simulator mappings. Four discovery machine targets exist. This PR fixes the answer-settlement ID on the query target. | The two patches under `qa/mutations/discovery/worker-lifetime/` are outside both retained runners and have no case sidecars. The machine-property IDs lack simulator coverage rows. Do not assign them to an unrelated harness. |
| own-9, sessions and credit | The existing tests `held_credit_bounds_a_greedy_peer`, `releasing_a_credit_hold_unblocks_the_sender`, and `grants_match_an_independent_ledger` provide candidate checks. | No retained held-credit mutation appears in these directories. Prove sensitivity with those tests before requiring sim-9. |
| own-10, backend reads | All five read-worker patches apply. Both private-pool patches already name `ReadWorkerPool`. Their native tests have explicit selectors. | Native selectors other than the two node-services cases above remain source-checked only. Compilation, exact assertion failure, and deterministic backend replay remain open. |
| own-11, backend results | Disk-result and transaction-result patches have explicit selectors. The early-release mutation now has baseline/mutant/restored proof. | Other backend cases still need executable selection and sensitivity. Native assertion evidence does not establish simulator observation bindings. |

The slot inventory records concrete existing coverage rather than treating
every earlier missing-binding claim as current. It does not replace property
definitions, observation bindings, or the existing harness coverage manifest.

## Reproduce

```sh
python3 scripts/mutation_inventory.py --output "$TMPDIR/mutations.json"
python3 -m unittest discover -s scripts -p test_mutation_inventory.py -v
python3 scripts/check-fuzz-campaign.py
```

The inventory exits nonzero on metadata or application failures. A zero exit
does not establish runtime readiness. For native runs, use the emitted
command with the recorded toolchain and flags. First append `--list` and
verify the exact selector. Then require one executed test and its intended
assertion on the mutant. Never count zero selection, ignored tests, compiler
failures, or unrelated panics as detection.
