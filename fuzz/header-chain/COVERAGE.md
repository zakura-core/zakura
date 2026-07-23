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
| Equal-height permutations | Work/raw-hash comparator and retained-state oracle | LC-SELECT-02..03 | `aud_03_same_height_permutations`, `df_01_body_valid_fork_graph_matches_full_state_before_finalization` | Engine insertion-order coverage is complete; the integrated Mainnet/Testnet differential now matches full-state work, raw-hash ordering, and selected tips across equal-work and greater-work branches |
| Verified grow/reset | `TransitionEvent::VerifiedChainChanged` | LC-FRONTIER-02, LC-INT-01..02 | `verified_and_finality`, `aud_04_consecutive_resets` | Covered |
| Body mismatch | `BodyEvidence::PayloadMismatch` | LC-BODY-02, LC-ERR-01 | `body_mismatch` | Covered |
| Body invalid / transient / verified | Typed `BodyEvidence` variants and verifier/driver/reactor/state handoff | LC-BODY-01..04, LC-AVAIL-01 | `body_evidence_matrix`, `body_invalid`, `body_unavailable`, `verified_and_finality`, IN-02 | Covered: all mismatch/transient kinds, unknown-header refusal, invalid-descendant propagation, exact supplier gating, and durable production persistence/reselection of commitment-matching invalidity |
| Operator invalidate / reconsider | Unified planner and exact reason IDs | LC-INT-04, LC-OP-01..02 | structured operations and AUD-10..12 | Covered |
| Finalize verified | `TransitionEvent::FullStateFinalized` | LC-FINAL-01..02, LC-TXN-01 | `verified_and_finality`, AUD-13 | Covered |
| Deferred header / clock / reevaluation | `DeferredUntil` and `ReevaluateDeferred` | LC-VAL-08, LC-INT-01 | `deferred_header` | Covered |
| Candidate-tip pressure | Planner retention and independent eviction oracle | LC-RETAIN-01..04 | `evict_pressure` | Covered at the tip cap; 65,536 nodes remain a deterministic retention test |
| Fixed-anchor 999/1,000/1,001 replacement | Planner in both arrival orders | LC-REORG-01 | `fixed_anchor_999_1000_1001` | Covered |
| Logical and production crash/reopen | Retained digest and snapshot clone plus RocksDB writer/startup and reconstructed reactor/session harnesses | LC-RECOVER-02, LC-FRONTIER-04 | `crash_reopen`, `aud_incident_late_a_after_b_promotion`, AUD-14 | Covered: the byte target checks deterministic logical identity, while AUD-14 separately exercises every production transaction boundary, startup repair boundary, and stale completion/stream delivery after process-state loss |
| Block specification mutations | Production `prepare_headers` plus generated planner fixtures | LC-VAL-02..08 | `block_spec_mutations`, hard-work, and deferred-time classes | Covered for the structured operation: subsequent input bytes parameterize isolated invalid parent/version/commitment/signed-target/nonmonotonic-time fields and valid future deferral |
| Page partition equivalence | Target-bound staging/admission | LC-WIRE-03, LC-WIRE-05 | `page_partitions` plus requester reactor tests | Covered |

## Pursuit and ownership domain

| Operation or scenario | Production boundary | Rule IDs | Evidence | Status |
| --- | --- | --- | --- | --- |
| Advertise / start / page / prepare / complete | `PeerWorkQueue`, response predicates, `CompletionGate` | LC-WIRE-03..05, LC-WORK-01..03 | exact/wrong-target/wrong-ancestry corpus | Covered |
| Disconnect | Exact source/owner retirement | LC-WORK-02..03 | `disconnected_held_completion` | Covered |
| Hold/release network completion | Pending-owner retirement and gate | LC-WORK-02, LC-GEN-03 | held/stale corpus and AUD-05 | Covered |
| Hold/release state success/failure | Applying phase, gate, and live reactor snapshot retirement | LC-TXN-01, LC-GEN-03 | AUD-06, AUD-07 | Covered: modeled probes and the actual reactor both reject delayed success/failure after the committed replacement snapshot with no action or publication |
| Corrupt advisory | Discovery/removal without local authority mutation | LC-WIRE-04, LC-ERR-02 | `corrupt_advisory` | Covered |
| Seventeen pursuits | Staged-target cap and priority replacement | LC-RETAIN-04, LC-WIRE-04 | `seventeen_pursuits` | Covered |
| Coverage after reset | Production `CoverageMap` | LC-GEN-03, LC-WORK-03 | AUD-08 | Covered outside the byte target |
| VCT repair retirement | Production `VctRepairQueue` | LC-AUX-03, LC-GEN-03 | AUD-09 | Covered outside the byte target |

## Wire and recovery domains

| Target | Production boundary | Rule IDs | Evidence | Status |
| --- | --- | --- | --- | --- |
| `header_codec` | Sole codec and four fixed discriminants | LC-SCOPE-09, LC-WIRE-01, LC-WIRE-06 | golden/truncation/discriminant corpus | Covered |
| `recovery_rows` | Twelve RocksDB families and startup audit | LC-RECOVER-01..03, LC-TXN-01 | row mutation, migration, and refutation seeds | Covered for bounded mutations and mode transitions |
| State-writer and restart crash harnesses | Durable/memory/publication handoffs plus fresh-reactor delivery of pre-crash preparation/admission completions and ordered-stream responses | LC-TXN-01, LC-FRONTIER-04, LC-RECOVER-02 | AUD-14 | Complete: every real semantic transition/completion shape, repeated multi-row auxiliary writes, fail-closed migrated-pin commit, private startup recovery, unchanged/advanced-snapshot completion delivery, and old-stream response delivery to a replacement peer session are covered |
| Header/full-state fork differential | Shared observable rules plus combined full-state/header writer before finalization | LC-PARITY-01..03, LC-SELECT-01..03 | `df_01_observable_activation_headers_pass_shared_rules`, `df_01_body_valid_fork_graph_matches_full_state_before_finalization` | Partial DF-01: exact Overwinter, Sapling, Blossom, and Heartwood activation headers pass the complete shared pipeline on both production networks, and body-valid fork graphs compare work, equal-work raw-hash order, and selected tips; Canopy/NU5 history-root activation graphs remain open |
| Header/body intentional differences | Header-only selection paired with concrete verifier and contextual error classifiers | LC-SCOPE-01..03, LC-BODY-01..04, LC-PARITY-02 | DF-02 classifier matrices plus `in_02_header_valid_body_invalid_reselects_after_exact_authenticated_evidence` | Covered: coinbase/Merkle/ZIP-244 payload mismatches; transaction, script, proof, nullifier, anchor, value-pool, and state-rule invalidity; and local time/header/storage/resource failures all retain exact typed explanations without treating header acceptance as body validity |

## Named audit status

AUD-01 through AUD-13 and AUD-INCIDENT have named deterministic
orchestrations. The durable state-writer and startup-recovery portions of
AUD-14 are covered, and fresh-reactor tests cover stale preparation/admission
delivery after process-state loss plus an old ordered-stream response delivered
after the same peer reconnects with a new session generation. The driver has no
independent durable or ownership state: before a result event is sent, a lost
read has no effects, a completed atomic write is recovered from its committed
snapshot, and any delayed event is subject to the tested reactor owner/session
gates. AUD-15 exact-parent checks now run on a clone of the real post-transition
store after every changed header or verified frontier, as well as at both final
frontiers. The same helper runs after each consecutive reset and permutation
winner; the incident fixture and AUD-10 through AUD-13 retain their focused
checks. AUD-05 through AUD-09 are snapshot consumers rather than frontier
mutation sites, so their conformance obligation is the already-tested
retirement of work owned by the producer's old exact frontier.

IN-02 is covered across the planner matrix and the production
verifier/driver/sequencer/state path. Exact embedded supplier attribution is
required before either durable invalidity or peer scoring. DF-02 pairs a
header-only selected child with typed full-state failure vectors and proves the
intentional boundary: payload mismatches retain the header, deterministic
commitment-matching invalidity can make it ineligible, and local/transient
failures cannot.
