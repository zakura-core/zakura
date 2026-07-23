# Header-chain fuzz coverage

This report maps executable fuzz domains to production boundaries and
specification rules. “Covered” means a checked-in deterministic corpus test
asserts the named outcome. “Partial” identifies the exact evidence present and
the remaining gap. Fuzzer reachability alone is not requirement coverage.

## Transition and engine domain

| Operation or scenario | Production boundary | Rule IDs | Evidence | Status |
| --- | --- | --- | --- | --- |
| Extend / fork / named branch resume | `TransitionEvent::InsertHeaders` and exhaustive DAG oracle | LC-SELECT-01..04, LC-REORG-01, LC-INT-01 | `linear_growth`, `fork_replacement`, `aud_01_losing_branch_promotion` | Covered |
| Harder target class | Canonical target and recomputed `block_work` | LC-WORK-01, LC-SELECT-01 | `aud_02_shorter_higher_work` | Covered |
| Equal-height permutations | Work/raw-hash comparator and retained-state oracle | LC-SELECT-02..03 | `aud_03_same_height_permutations` | Covered in the engine; DF-01 full-state comparison remains open |
| Verified grow/reset | `TransitionEvent::VerifiedChainChanged` | LC-FRONTIER-02, LC-INT-01..02 | `verified_and_finality`, `aud_04_consecutive_resets` | Covered |
| Body mismatch | `BodyEvidence::PayloadMismatch` | LC-BODY-02, LC-ERR-01 | `body_mismatch` | Covered |
| Body invalid / transient / verified | Typed `BodyEvidence` variants | LC-BODY-01..04, LC-AVAIL-01 | `body_invalid`, `body_unavailable`, `verified_and_finality` | Covered for one rule/class each; the complete IN-02/DF-02 class matrix remains open |
| Operator invalidate / reconsider | Unified planner and exact reason IDs | LC-INT-04, LC-OP-01..02 | structured operations and AUD-10..12 | Covered |
| Finalize verified | `TransitionEvent::FullStateFinalized` | LC-FINAL-01..02, LC-TXN-01 | `verified_and_finality`, AUD-13 | Covered |
| Deferred header / clock / reevaluation | `DeferredUntil` and `ReevaluateDeferred` | LC-VAL-08, LC-INT-01 | `deferred_header` | Covered |
| Candidate-tip pressure | Planner retention and independent eviction oracle | LC-RETAIN-01..04 | `evict_pressure` | Covered at the tip cap; 65,536 nodes remain a deterministic retention test |
| Fixed-anchor 999/1,000/1,001 replacement | Planner in both arrival orders | LC-REORG-01 | `fixed_anchor_999_1000_1001` | Covered |
| Logical crash/reopen | Retained digest and snapshot clone | LC-RECOVER-02, LC-FRONTIER-04 | `crash_reopen`, `aud_incident_late_a_after_b_promotion` | Partial: production disk reopen is a separate target, not periodic inside `fork_transitions` |
| Block specification mutations | Production `prepare_headers` plus generated planner fixtures | LC-VAL-02..08 | `block_spec_mutations`, hard-work, and deferred-time classes | Partial: valid/future/nonmonotonic time plus parent/version/commitment/target single-field cases are covered; byte-parameterized mutation combinations remain open |
| Page partition equivalence | Target-bound staging/admission | LC-WIRE-03, LC-WIRE-05 | `page_partitions` plus requester reactor tests | Covered |

## Pursuit and ownership domain

| Operation or scenario | Production boundary | Rule IDs | Evidence | Status |
| --- | --- | --- | --- | --- |
| Advertise / start / page / prepare / complete | `PeerWorkQueue`, response predicates, `CompletionGate` | LC-WIRE-03..05, LC-WORK-01..03 | exact/wrong-target/wrong-ancestry corpus | Covered |
| Disconnect | Exact source/owner retirement | LC-WORK-02..03 | `disconnected_held_completion` | Covered |
| Hold/release network completion | Pending-owner retirement and gate | LC-WORK-02, LC-GEN-03 | held/stale corpus and AUD-05 | Covered |
| Hold/release state success/failure | Applying phase and gate | LC-TXN-01, LC-GEN-03 | AUD-06, AUD-07 | Covered at the queue/gate boundary; direct reactor assertions remain open |
| Corrupt advisory | Discovery/removal without local authority mutation | LC-WIRE-04, LC-ERR-02 | `corrupt_advisory` | Covered |
| Seventeen pursuits | Staged-target cap and priority replacement | LC-RETAIN-04, LC-WIRE-04 | `seventeen_pursuits` | Covered |
| Coverage after reset | Production `CoverageMap` | LC-GEN-03, LC-WORK-03 | AUD-08 | Covered outside the byte target |
| VCT repair retirement | Production `VctRepairQueue` | LC-AUX-03, LC-GEN-03 | AUD-09 | Covered outside the byte target |

## Wire and recovery domains

| Target | Production boundary | Rule IDs | Evidence | Status |
| --- | --- | --- | --- | --- |
| `header_codec` | Sole codec and four fixed discriminants | LC-SCOPE-09, LC-WIRE-01, LC-WIRE-06 | golden/truncation/discriminant corpus | Covered |
| `recovery_rows` | Twelve RocksDB families and startup audit | LC-RECOVER-01..03, LC-TXN-01 | row mutation, migration, and refutation seeds | Covered for bounded mutations and mode transitions |
| State-writer crash harness | `FaultPoint::ALL` durable/memory/publication handoffs | LC-TXN-01, LC-FRONTIER-04, LC-RECOVER-02 | durable AUD-14 | Partial: every transition variant and external response/reactor boundaries remain open |

## Named audit status

AUD-01 through AUD-13 and AUD-INCIDENT have named deterministic
orchestrations. The durable state-writer portion of AUD-14 is covered, but its
full transition/response/reactor matrix is open. The shared AUD-15 next-child
helper currently covers AUD-10 through AUD-13; the remaining call sites are
open.
