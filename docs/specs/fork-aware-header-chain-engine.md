# Fork-aware headers-only chain engine specification

Status: normative design oracle for the replacement of PR #229  
Version: 1.2  
Date: 2026-07-20  
Scope: Zakura native header sync and its integration with Zebra/Zakura full state

## 1. Scope, language, and authority

The key words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** are normative as described by BCP 14. Each normative rule has a stable identifier and one authority class:

- **ZC — Zcash consensus:** mandatory production Mainnet/Testnet behavior required to accept the same observable header rules as Zcash full consensus. Exceptions for custom networks or disabled proof of work are never included in a ZC rule.
- **ZP — Zcash deployment security:** production-network trust pins or operational requirements imposed by the Zcash specification on full validators, but not derivable from header validity alone.
- **ZF — Zebra fork choice:** deterministic local policy used by Zebra when consensus does not prescribe a deterministic choice.
- **ZW — Zakura wire/integration:** native protocol or full-node integration behavior.
- **LS — local safety/liveness:** crash consistency, resource, scheduling, and operator policy.
- **OPT — optimization:** non-normative implementation advice. An optimization may not change externally observable selection.

When this document and the current PR #229 implementation disagree, this document is the redesign oracle. Zcash consensus rules remain superior authority; the cited local code is the required parity target, not permission to preserve a known implementation bug.

### 1.1 Definitions

A **headers-only client** downloads and validates the observable block-header chain. It does not validate transaction bodies or the state transition they express.

An **integrated engine** is that same reusable header-chain engine embedded in a full node and augmented by authenticated body-validation feedback. It is not a second overlay with different fork choice.

An **anchor** is a trusted `(height, hash)`: configured genesis, a mandatory settled-upgrade activation pin, a configured local sync checkpoint, or the finalized frontier. The **work anchor** is the current finalized frontier from which retained suffix work is summed. Settled-upgrade pins and local sync checkpoints are separate configuration namespaces even when they contain the same tuple.

**Integrated mode** embeds the engine in a full validator and advances finality only from fully verified state. **Headers-only mode** has no body-validity oracle and advances a disclosed local finality pin from sufficiently deep `header_best` history under LC-FINAL-03.

An **eligible chain** is a continuous path from `finalized` whose nodes pass all applicable header checks, have no ineligible ancestor, and do not conflict with an anchor. The rollback horizon is expressed by advancing `finalized`, not by comparing a candidate with the incumbent tip.

The **correct tip** for this engine is the deterministic greatest-work eligible header chain visible to the client, subject to trusted finality and the bounded-resource rules in this specification. “Visible” means that the client has downloaded enough linked headers to recompute all work and validation locally; status claims alone are not visible candidates.

### 1.2 Security and impossibility boundary

**LC-SCOPE-01 [LS].** The implementation MUST document and expose that `header_best` means “best eligible header chain,” not “fully valid Zcash chain.” A pure header client cannot detect a body-validity failure that is neither committed by the header nor accompanied by a sound proof.

**LC-SCOPE-02 [LS].** Correctness assumes collision resistance of the header and commitment hash functions, correct local network parameters and activation heights, authentic trusted checkpoints, and at least one useful non-eclipsing peer from which every relevant retained fork can eventually be discovered.

**LC-SCOPE-03 [LS].** The engine MUST NOT infer transaction validity, coinbase height validity, note/nullifier state, value-pool conservation, script/proof validity, or state-transition validity from header acceptance.

**LC-SCOPE-08 [LS].** A headers-only deployment MUST disclose that its automatic 1,000-deep local finality is an irreversible local trust decision: an eclipsed or incomplete view can pin the wrong header-valid branch, and a later greater-work fork conflicting with that pin will be rejected. Its “correct tip” guarantee is therefore relative to its durable finality history as well as the assumptions in LC-SCOPE-02. The mandatory settled-upgrade pin in LC-ANCHOR-04 applies to headers-only deployments as well, so even a wrongly pinned branch must contain the settled activation hash; and if such a store is later migrated to integrated mode, deterministic body validation can refute the pin, after which recovery requires discarding the migrated header store under LC-FINAL-04.

### 1.3 Included and excluded work

Version 1 includes linear download and verification of all headers, a bounded fork DAG, full-state feedback, durable recovery, and the native v8 discovery protocol.

**LC-SCOPE-04 [LS].** Version 1 MUST NOT implement ZIP 307 wallet payment discovery, compact-block scanning, trial decryption, note witnesses, or wallet state.

**LC-SCOPE-05 [LS].** Version 1 MUST NOT claim or implement ZIP 221 FlyClient logarithmic sampling or succinct chain proofs. It validates every retained header on a candidate suffix. ZIP 221 history-tree data is covered only as auxiliary metadata used by the integrated node.

**LC-SCOPE-06 [LS].** Block-sync token-bucket connection eviction and readiness/accounting defects unrelated to header-chain selection are outside this engine and MUST NOT be coupled to its fork-choice state.

**LC-SCOPE-07 [ZW].** ZIP 221/ZIP 244 commitments and VCT metadata MAY be consumed by the integrated node only under the authentication and generation rules in section 4.7; they MUST NOT become unauthenticated header-validity or fork-choice evidence.

## 2. Canonical chain model and durable representation

### 2.1 Header DAG

The durable model is a directed acyclic graph keyed by the consensus block hash. Parent edges point through `previous_block_hash`. Heights are not supplied by peers as consensus evidence; they are inferred as parent height plus one.

**LC-DAG-01 [LS].** Every retained node MUST durably contain:

1. the canonical serialized header and locally computed hash;
2. parent hash and inferred height;
3. per-block work and suffix cumulative work relative to the current work anchor;
4. header validation state, including deferred-until time when applicable;
5. eligibility state and a set of durable reasons;
6. body-validation state, if known; and
7. body-size and tree-aux metadata keyed by this header hash, with source peer and authentication provenance.

**LC-DAG-02 [LS].** The store MUST maintain bijective or reconstructible indexes for hash to node, parent to children, height to all retained hashes, selected-header height to hash, eligibility roots/reasons, and durable frontier/version metadata. A height index MUST allow multiple hashes.

**LC-DAG-03 [LS].** A node whose parent is unknown MAY exist only in a bounded staging area—at most one in-flight response’s headers per peer and at most 4,096 staged headers in total—and MUST NOT be durable, eligible, counted as candidate work, or published. Staging overflow is a temporary resource refusal, not peer misbehavior. Admission to the DAG occurs only after the parent or trusted context is known and height is inferred.

### 2.2 Independent frontiers

The engine maintains three named frontiers:

- `finalized`: immutable trusted `(height, hash)` and the common root of both selected paths;
- `header_best`: tip of the deterministic best eligible header path;
- `verified_best`: tip selected by full state among fully body-verified paths; in headers-only mode it equals `finalized` because no non-finalized body-verified suffix exists.

**LC-FRONTIER-01 [LS].** `header_best` and `verified_best` MUST each walk continuously by parent hash to the same `finalized` frontier. Neither selected path is required to be a prefix of the other above `finalized`.

**LC-FRONTIER-02 [ZW].** A verified grow or reset MUST update `verified_best` and feed any resulting eligibility evidence into the header engine, but MUST NOT select `verified_best` over a different higher-work eligible `header_best` merely because bodies are available. A later full-state finalization is different evidence: LC-FINAL-02 advances the common trust anchor and makes every conflicting candidate ineligible.

**LC-FRONTIER-03 [ZW].** Selecting a header candidate MUST NOT mark its body verified. `verified_best` changes only through full-state acceptance.

**LC-FRONTIER-04 [LS].** Every published frontier MUST identify a node present in the committed durable state and continuously walkable to `finalized`. Publication includes watches, exchange status, scheduling anchors, RPC/read views, and peer status.

### 2.3 Versions, generations, and branch identity

The durable metadata contains monotonically increasing unsigned counters. Counter exhaustion is a fail-closed storage error; counters never wrap.

**LC-GEN-01 [LS].** `state_version` MUST increase on every committed transaction that can affect a frontier, selected projection, eligibility, or finality.

**LC-GEN-02 [LS].** `header_generation` MUST increase in the same transaction whenever `finalized`, any header eligibility affecting selection, or the selected header path changes. `verified_generation` MUST increase whenever the selected fully verified path changes.

**LC-GEN-03 [LS].** Every branch-sensitive network request, staged target, buffer, coverage interval, pending commit, retry/avoidance record, body task, auxiliary repair, and completion event MUST carry the relevant generation and an explicit branch identity. A branch identity is at least `(anchor_hash, target_tip_hash)`; height alone is never sufficient.

**LC-GEN-04 [LS].** Before any effect, completion handling MUST compare generation, branch identity, request identity, and pending owner. A stale or unowned result MUST have no frontier, coverage, retry, repair, scheduling, publication, body-task, or peer-score effect.

**LC-GEN-05 [ZW].** The header engine MUST be the sole publisher of accepted header-frontier transitions. A driver or state-response task MUST NOT independently publish the raw result of a range commit.

### 2.4 Transactions and restart

**LC-TXN-01 [LS].** A frontier-affecting mutation MUST atomically update DAG rows, reverse indexes, height/child indexes, selected projections, eligibility, auxiliary provenance, generations, and frontier metadata before any observer is notified.

**LC-TXN-02 [LS].** Invalidation, reconsideration, body feedback, finalization, checkpoint advancement, direct growth, fork replacement, VCT repair, and full-state grow/reset MUST use the same durable transition API. A direct-extension fast path is allowed only if it produces the same transaction and selection result.

**LC-RECOVER-01 [LS].** Startup MUST audit canonical hashes, inferred heights, parent linkage, index bijections, anchor/checkpoint ancestry, both selected projections, eligibility roots, generation metadata, and the ability to walk each published frontier to `finalized`.

**LC-RECOVER-02 [LS].** Startup MUST recompute eligibility and deterministic selection from the retained DAG rather than trust cached selected rows. It MUST either repair to the last complete atomic transaction or fail closed; it MUST NOT publish an incoherent or discontinuous frontier.

**LC-RECOVER-03 [LS].** If cached projections differ from recomputation but all source DAG rows are coherent, startup MUST atomically rewrite the projections and increment the appropriate version/generation before publication. If source rows or anchor ancestry are incoherent, startup MUST surface a local storage incident and stop header publication.

## 3. Header validation, selection, and retention

### 3.1 Ordered validation pipeline

Validation proceeds in the following order so cheap bounds precede CPU work and no contextual result can be detached from its branch:

**LC-VAL-01 [ZW].** An inbound response MUST pass framing, negotiated version, nonzero request correlation, message and count bounds, allocation bounds, parallel-vector length checks, and exact payload consumption before header admission. No allocation may be derived from an unbounded peer count.

**LC-VAL-02 [ZC].** Each header MUST canonically deserialize using the network’s Equihash solution encoding. Its version, interpreted with the high bit as the signed bit used by zcashd, MUST have the high bit clear and be at least 4. The canonical hash MUST be computed from the entire serialized header.

**LC-VAL-03 [ZC].** Every non-genesis header MUST name its validated parent hash, and every following header in a supplied sequence MUST name the immediately preceding computed hash.

**LC-HEIGHT-01 [ZW].** The first supplied header MUST name an authenticated retained common ancestor or known parent. The engine MUST infer height by checked parent-height increment, reject overflow, and treat any peer-supplied height only as framing that must equal the locally inferred value, never as consensus evidence.

**LC-COMMIT-01 [ZC].** After height inference, a production Mainnet/Testnet header’s 32-byte commitment field—the slot interpreted across upgrades as `hashFinalSaplingRoot`, `hashLightClientRoot`, or `hashBlockCommitments`—MUST successfully parse as the height- and network-upgrade-specific `Header::commitment(network, height)` variant. This check includes canonical Sapling-root encoding and the all-zero chain-history activation reserved value at the Heartwood activation height. It validates the field’s observable structure only; it does not authenticate a claimed history root, authorization-data root, or body/state contents.

**LC-COMMIT-02 [ZW].** Regtest and configured custom networks MUST run the same commitment parser using their configured activation schedule, including overlapping activation-height behavior. A custom activation schedule MUST NOT bypass or guess the commitment variant.

**LC-VAL-05 [ZC].** On production Mainnet/Testnet, compact `nBits` MUST decode to a positive, non-overflowing target no easier than the network PoW limit, and the little-endian integer value of the header hash MUST be less than or equal to that target. LC-VAL-05 runs before LC-VAL-04 so the free target checks reject a candidate before any Equihash CPU cost, matching the check order in `zakura-consensus::block::check`.

**LC-VAL-04 [ZC].** On production Mainnet/Testnet, the Equihash solution size and parameters MUST match the network and height, and the Equihash proof MUST verify. Production headers MUST NOT use the short Regtest proof shape.

**LC-VAL-06 [ZC].** For each non-genesis production Mainnet/Testnet candidate, `nBits` MUST equal the branch-local `ThresholdBits(height)` computed by the same algorithm and network-upgrade parameters as `zakura-state`. Context consists of up to the preceding 28 linked headers: a 17-block averaging window plus the 11-block median span. Early-chain PoW-limit and Testnet minimum-difficulty rules, including ZIP 205/208 behavior, MUST match full state exactly.

**LC-POW-01 [ZW].** Regtest or a configured custom network MAY explicitly disable proof of work only through its authenticated local network parameters. In that mode the engine MUST still enforce the configured solution encoding and parameters and the positive, non-overflowing PoW-limit-bounded target, but MUST mirror full state by waiving exactly the Equihash-proof check, header-hash-to-target filter, and contextual `ThresholdBits` equality. This exception MUST NOT be reachable for production Mainnet/Testnet identifiers.

**LC-VAL-07 [ZC].** For every non-genesis production Mainnet/Testnet candidate, `nTime` MUST be strictly greater than median-time-past of up to the preceding 11 linked headers. At every non-genesis Mainnet height, and on Testnet where `Network::is_max_block_time_enforced(height)` becomes active (currently height 653,606), `nTime` MUST also be no greater than MTP plus 90 minutes. zcashd deployed the Mainnet bound as a soft fork from height 2; the shared `is_max_block_time_enforced` parity function enforces it at every Mainnet height, the two are observably identical on the real chain, and the shared function is authoritative for LC-PARITY-01.

**LC-TIME-01 [ZW].** Regtest and configured custom networks MUST apply the same MTP algorithm and the exact height-dependent maximum-time policy returned by their authenticated local network parameters and full state. They MUST NOT inherit Mainnet/Testnet activation heights by name or bypass MTP merely because proof of work is disabled.

**LC-VAL-08 [LS].** A header more than two hours ahead of the validating node’s local clock MUST enter `DeferredUntil(nTime - 2 hours)`, not permanent invalidity. Deferred nodes and descendants MUST be excluded from selection until reevaluation succeeds, and reevaluation MUST occur without retransmission. This local-clock rule is nondeterministic and is not Zcash consensus.

**LC-VAL-09 [LS].** At every configured local sync-checkpoint height present in a candidate path, the computed hash MUST exactly equal the configured checkpoint hash. The candidate’s ancestry MUST be consistent with genesis, every applicable settled-upgrade pin and local checkpoint, and `finalized`. This is trusted local selection policy, not a context-free Zcash header rule.

**LC-VAL-10 [ZC].** Per-block work MUST be computed from the validated compact target as `floor(2^256 / (target + 1))`, using the same target conversion as `zakura-chain`.

**LC-WORKCALC-01 [LS].** Candidate suffix cumulative work MUST be the checked sum of per-block work from the current work anchor using the same integer ordering as `zakura-chain`. Conversion or sum overflow is a fail-closed local error and MUST NOT produce a selectable candidate or wrapped advertised work.

**LC-VAL-11 [LS].** Only after every applicable preceding pipeline rule, including LC-COMMIT-01/02 and LC-POW-01, passes MAY the engine apply resource admission, atomically insert nodes and indexes, recompute eligibility, and evaluate fork choice. `DeferredUntil` is the sole non-passing result admitted by LC-VAL-08; all other deterministic checks MUST still pass before that node is stored as deferred. Response partitioning MUST NOT create intermediate selection semantics different from insertion of the complete validated sequence.

### 3.2 Trusted anchors and checkpoint context

**LC-ANCHOR-01 [LS].** Genesis, every mode-applicable mandatory settled-upgrade pin, configured local sync checkpoints, and `finalized` MUST be absolute pins. A node conflicting at or below any applicable anchor is permanently anchor-ineligible, as are its descendants. No operator action may override this reason.

**LC-ANCHOR-02 [LS].** Supplied trusted-anchor headers MUST still pass every applicable context-free or directly observable pipeline rule: canonical deserialization, version, hash, commitment-field structure, target encoding/limit and filter, solution size, and Equihash, with custom-network exceptions limited to LC-POW-01. Trusting an anchor permits the client to trust consensus history before it; it does not exempt post-anchor headers from normal validation.

**LC-ANCHOR-03 [LS].** A client starting at a later checkpoint MUST acquire the checkpoint header and enough linked predecessors to validate the first post-anchor header—up to 27 predecessors before the anchor, for 28 total context headers ending at the anchor. It MUST authenticate the anchor by exact configured hash and authenticate predecessor context by the backward hash links ending in that anchor. This context is immutable validation context below `finalized`, not a selectable fork.

**LC-ANCHOR-04 [ZP].** Before a deployment in either mode—integrated/full-validator or headers-only—publishes any header or verified frontier for Mainnet or Testnet, its release-authenticated network manifest MUST independently contain the exact `(upgrade, activation_height, activation_hash)` pin for that network’s most recent settled network upgrade. “Release-authenticated” means immutable data compiled into, or cryptographically authenticated with, the installed release artifact; an unsigned runtime file or peer response is insufficient. The engine MUST fail closed if that pin is missing, malformed, duplicated inconsistently, or unavailable for the selected network; enabling or disabling optional sync checkpoints MUST NOT remove or replace it. Every candidate reaching or passing the activation height MUST contain that exact hash in its ancestry. For version 1.2, the settled tuples, with hashes written in the protocol specification’s RPC display order and canonically parsed into `block::Hash`, are:

| Network | Upgrade | Activation height | Activation hash (RPC display order) |
| --- | --- | ---: | --- |
| Mainnet | NU6.2 | 3,364,600 | `0000000000806344c408a4cfdf472f4132c632edbdc24cf2f3f672061da8b865` |
| Testnet | NU6.2 | 4,052,000 | `0010cb912b0188da5bc055ee67e3f77d30cd27611369d865974a5bf0b1ec2912` |

These values MUST come from the release-authenticated manifest rather than peer status or the optional checkpoint files.

Non-normative implementation warning: at version 1.2 publication, `main-checkpoints.txt` ends at height 3,358,006 and `test-checkpoints.txt` ends at 4,023,200, both below their NU6.2 activation height. Those files therefore cannot satisfy LC-ANCHOR-04 without the independent settled-upgrade manifest.

**LC-ANCHOR-05 [ZP].** A release that changes which upgrade is most recently settled or changes a settled activation tuple MUST update the manifest and both-network conformance vectors atomically. Runtime peer claims, candidate-upgrade configuration such as NU6.3/NU7, and mere passage of an activation height MUST NOT create, supersede, or mutate a settled-upgrade pin.

### 3.3 Eligibility and deterministic fork choice

Eligibility reasons are a set, not a single overwritable flag. Permanent reasons include trusted-anchor conflict (settled pin, local checkpoint, or finality) and intrinsic body invalidity. Reversible reasons include operator invalidation. Temporary states include future-time deferral and resource staging.

**LC-SELECT-01 [LS].** Selection MUST consider only complete paths connected to `finalized`, contextually valid at every node, locally time-admissible, and free of an ineligible ancestor. No eligibility decision may depend on distance from the current incumbent `header_best`.

**LC-SELECT-02 [ZC].** The primary comparison MUST be cumulative work over complete suffixes from the shared work anchor. Greater cumulative work wins. Only locally validated targets and LC-WORKCALC-01 sums are inputs to this comparison.

**LC-SELECT-03 [ZF].** If cumulative work is equal, the engine MUST compare the tips’ raw internal `block::Hash.0` byte arrays lexicographically and select the greater array, exactly as `zakura-state::service::non_finalized_state::Chain::cmp`. Display-order hex, first-seen time, arrival order, peer identity, response range, and response partitioning MUST NOT break the tie.

**LC-SELECT-04 [LS].** For a fixed finalized anchor, selection MUST be a pure deterministic function of the admitted DAG, eligibility set, and the comparator in LC-SELECT-02/03. Replaying any permutation of equivalent insertions and completions before the same durable finalization event MUST yield the same `header_best`. A headers-only finality event deliberately makes its chosen ancestor a new trust pin under LC-FINAL-03; discovery after that event cannot revise history at or below the pin.

### 3.4 Reorg boundary, finality, and retention

**LC-REORG-01 [LS].** Candidate eligibility and replacement MUST be derived from the durable `finalized` anchor: every eligible candidate must descend from it, and no candidate conflicting at or below it may be selected. Among descendants of the same `finalized` anchor, replacement depth relative to the current `header_best` MUST NOT affect eligibility or comparison. The 1,000-block rollback horizon is implemented by the mode-specific finalization rules below, never by incumbent-relative admission.

**LC-FINAL-01 [LS].** Finalization MUST atomically advance the immutable anchor, prove that the new anchor lies on both required selected histories or explicitly transition each history, prune all non-descendants, rebase retained suffix cumulative work, update projections and generations, and publish only after commit.

**LC-FINAL-02 [ZW].** In integrated mode, only a durable finalization decision from fully verified full state MAY advance `finalized`. The new anchor MUST be the exact height/hash finalized by full state. In the same transition, the engine MUST retire every header candidate that does not descend from that anchor, including a conflicting former `header_best`, and then recompute `header_best` among the surviving descendants. Header depth, header-peer agreement, resource pressure, and body unavailability MUST NOT independently advance integrated-mode finality.

**LC-FINAL-03 [LS].** In headers-only mode, after each atomic insertion or reselection, if `header_best.height - finalized.height > 1000`, the same serialized transition MUST advance `finalized` to the unique ancestor of `header_best` at `header_best.height - 1000`, apply LC-FINAL-01, and only then publish. Thus no published headers-only state retains more than 1,000 selected descendants above its local finality pin. This rule is a bounded-resource local trust policy, not proof of body validity or a Zcash consensus rule, and the deployment MUST expose the disclosure in LC-SCOPE-08.

**LC-FINAL-04 [LS].** The durable store MUST record whether it is integrated or headers-only and the finality source for every advancement. Startup MUST fail closed on a mode mismatch or a finality record without the required full-state evidence or headers-only 1,000-deep ancestor proof. Switching modes requires an explicit migration that preserves existing pins; it MUST NOT roll finality back. A headers-only finality record is local trust, never body-verification evidence: a migration to integrated mode MUST import headers-only pins as header trust anchors only, MUST NOT count them as full-state finalization, and integrated mode MUST still body-verify the imported history from its own last full-state-verified anchor. If deterministic body validation refutes an imported headers-only pin, the node MUST fail closed with an explicit incident naming that pin; the only supported recovery is deleting the migrated header store and its finality records and resynchronizing, which discards a local trust artifact rather than rolling back integrated finality, because integrated finality was never granted to the imported pin.

**LC-RETAIN-01 [LS].** The engine MUST retain no more than `MAX_NON_FINALIZED_CHAIN_FORKS` eligible candidate tips—the same shared constant that caps full-state non-finalized chains, currently 10—and 65,536 non-finalized DAG nodes. The tip cap MUST be consumed from the same shared `zakura-chain`-level definition as full state, so the header engine can never retain an eligible fork that integrated full state cannot represent. It MUST protect every node on `header_best` and `verified_best` from resource eviction. If integrated-mode verification/finalization stalls and admitting another node would exceed the node cap after all permitted eviction, the engine MUST refuse or stage that admission, retain the current frontiers, and raise an explicit resource-stalled alarm; it MUST NOT evict either protected path or synthesize finality to make room.

**LC-RETAIN-02 [LS].** On pressure, the engine MUST remove permanently ineligible subtrees first. It MUST then evict unprotected candidate tips in ascending order of cumulative work, breaking equal-work eviction ties by the smallest raw tip hash. Shared ancestors MUST be removed only when no retained path or validation-context window references them.

**LC-RETAIN-03 [LS].** Resource eviction MUST NOT be recorded as consensus or body invalidity. A later advertisement MAY reacquire and revalidate the branch.

**LC-RETAIN-04 [LS].** Each peer MUST have at most one staged unknown fork target and the engine MUST have at most 16 globally. Exceeding these limits is a temporary resource refusal, not proof of peer misbehavior.

## 4. Full-state integration and failure semantics

### 4.1 One transition interface

All calls below submit evidence to one serialized durable transition. The transition reads an expected `state_version`; commit uses compare-and-swap semantics and retries from the new durable state on conflict. It computes both frontiers independently, commits all changes, and then emits one ordered observation containing the committed version and generations.

**LC-INT-01 [ZW].** The following events MUST use that transition interface: header-range insertion, full-block grow, full-block reset at any height, deterministic body-invalid feedback, operator invalidate, operator reconsider, finalization, configured-checkpoint advancement, VCT metadata repair, and restart reconstruction.

**LC-INT-02 [LS].** Same-height, lower-height, and forward-height resets MUST be detected by branch identity and processed identically. Height monotonicity MUST NOT be used as evidence that a transition is direct growth.

**LC-INT-03 [ZW].** A full-block commit MUST ensure its exact header node exists in the DAG with correct parent linkage, update body-validation state, update `verified_best` from full state, apply any body-derived eligibility evidence, and then independently reevaluate `header_best`.

**LC-INT-04 [ZW].** Invalidate or reconsider MUST complete its durable eligibility and both-frontier transition before returning externally complete or publishing watches. If full state has no non-finalized chain, `verified_best` MUST become `finalized`; the header DAG remains independently selected subject to its eligibility set.

### 4.2 Body evidence

Body feedback is keyed by requested header hash and verifier rule identity. The integrated engine distinguishes a bad delivery from an intrinsic property of every body validly committed by that header.

**LC-BODY-01 [ZW].** If a delivered body has a different header hash, fails to reproduce the requested header’s transaction Merkle root, ZIP 244 authorization-data commitment, or other applicable body-derived header commitment, the engine MUST classify that delivery as `BodyPayloadMismatch`, attribute it only to the body supplier, discard it, and retry another source without changing header eligibility.

**LC-BODY-02 [ZW].** If a body matching all applicable header/body commitments deterministically fails a body or state consensus rule, the engine MUST durably add `ConsensusBodyInvalid(evidence)` to that block and propagate body-ineligibility to its descendants. It MUST then independently rerun header selection. Header suppliers MUST NOT be blamed for this result.

**LC-BODY-03 [ZW].** Missing state context, canceled or superseded work, local storage failure, verifier unavailability, timeout, and transient resource exhaustion MUST NOT disqualify a header or branch. These outcomes remain retryable local/body-fetch state.

**LC-BODY-04 [ZW].** Peer scoring MUST attach to the peer and evidence that proved the malformed header, mismatched body payload, or invalid body delivery. It MUST NOT attach to a peer that merely advertised or supplied a header-valid branch later shown body-invalid.

### 4.3 Operator eligibility

**LC-OP-01 [LS].** Operator invalidation MUST add a durable, reversible `OperatorInvalid` reason at the selected hash and make descendants ineligible by ancestry. It MUST use the same atomic selection transition as consensus body feedback.

**LC-OP-02 [LS].** Reconsider MUST remove only the specified operator reason. It MUST NOT remove trusted-anchor conflict, intrinsic body invalidity, another operator invalidation, or any unrelated reason, and MUST rerun deterministic selection atomically.

### 4.4 Body unavailability

**LC-AVAIL-01 [LS].** If bodies for `header_best` are unavailable, the engine MUST keep that eligible greatest-work chain selected, retry body acquisition across eligible peers with exponential delays of 1, 2, 4, 8, 16, 32, then at most 60 seconds with ±10% jitter, and MAY prefetch retained alternatives without selecting them. Tests use a seeded jitter source.

**LC-AVAIL-02 [LS].** After every currently eligible supplier has been tried and either ten deliveries have failed or ten minutes have elapsed, the retry episode MUST raise a persistent, externally visible `header_best_body_unavailable` alarm and metrics containing hash, height, age, attempts, and available suppliers. While alarmed, the engine MUST probe no more than once per ten minutes; discovery of a new supplier or an operator retry MUST start a new episode. The episode MUST NOT become an infinite silent hot loop.

**LC-AVAIL-03 [LS].** Timeout, peer absence, or resource eviction MUST NOT imply body invalidity or cause selection of a lower-work chain.

### 4.5 Coverage, retries, and stale work

**LC-WORK-01 [LS].** Coverage and retry avoidance above `finalized` MUST be keyed by `header_generation` and branch identity. A generation change MUST retire all forward coverage, retry avoidance, staged targets, buffers, pending commits, and body tasks that are not valid for the new branch.

**LC-WORK-02 [LS].** Finalized/backward coverage MAY survive a header-generation change only if every retained interval is at or below `finalized` and authenticated by the unchanged finalized hash ancestry.

**LC-WORK-03 [LS].** A stale `UnknownAnchor` from a local v7-style commit MUST trigger a bounded durable reload/reanchor and status refresh. It MUST NOT cause a hot retry of the same impossible anchor or peer punishment.

### 4.6 Error taxonomy and attribution

Every terminal or retry result is one of the following typed categories:

| Category | Meaning | Eligibility effect | Automatic header-peer score |
| --- | --- | --- | --- |
| `MalformedProtocol` | framing, correlation, bounds, codec, or explicit ancestry contract violated | none unless header also proved invalid | yes |
| `InvalidHeader` | a specific supplied header fails a deterministic header rule | header and descendants ineligible | yes, for its supplier |
| `ValidLosingFork` | valid candidate loses deterministic comparison | none | no |
| `DeferredHeader` | local future-time rule not yet satisfied | temporarily excluded | no |
| `BodyPayloadMismatch` | delivered body does not match requested header commitments | none | body supplier only |
| `ConsensusBodyInvalid` | commitment-matching body deterministically fails full consensus | block/descendants body-ineligible | proving body supplier as appropriate; never header-only suppliers |
| `OperatorIneligible` | reversible local operator choice | block/descendants temporarily ineligible | no |
| `StaleTargetOrGeneration` | target snapshot disappeared or local work was superseded | none | no |
| `LocalAnchorOrIncoherence` | local anchor missing, index broken, or durable view incoherent | fail closed/recover locally | no |
| `LocalResourceOrStorage` | capacity, cancellation, I/O, verifier, or storage failure | none | no |

**LC-ERR-01 [ZW].** Implementations MUST preserve these distinctions across service, driver, reactor, metrics, and peer-scoring boundaries. A generic error conversion MUST NOT turn a local or stale category into peer misbehavior.

**LC-ERR-02 [ZW].** Only `MalformedProtocol` and `InvalidHeader` automatically justify header-peer misbehavior. `UnknownAnchor` is local except when a v8 response violates the explicit locator/common-ancestor contract sent by that peer.

### 4.7 VCT and tree-aux metadata

VCT/tree-aux records are execution assistance. They can help reconstruct and authenticate ZIP 221 history-tree and ZIP 244 block-commitment inputs, but peer claims are not self-authenticating.

**LC-AUX-01 [ZW].** Body-size and tree-aux data MUST be keyed by header hash, not height alone, and MUST record supplying peer, request, branch, generation, and authentication status.

**LC-AUX-02 [ZW].** Unauthenticated auxiliary data MUST NOT affect header validity, cumulative work, or fork choice. Missing or invalid auxiliary data invalidates only that metadata delivery unless a separately verified header commitment or body supplies intrinsic consensus evidence.

**LC-AUX-03 [ZW].** VCT repair MUST be scoped to a header generation and exact branch. Any branch/generation change MUST retire scheduled, outstanding, buffered, commit-waiting, and state-dispatched repair ownership before forward work for the new branch is scheduled; late results are discarded under LC-GEN-04.

**LC-AUX-04 [ZW].** Auxiliary roots MAY be marked authenticated only after the existing integrated verifier reconstructs the relevant ZIP 221 history-tree inputs and checks them against the appropriate checkpoint/header commitment, including the one-header-later authentication boundary. Proven bad metadata MAY score its supplier but MUST NOT invalidate the header.

## 5. Native fork-discovery protocol v8

### 5.1 Negotiation and common codec

Native stream version 8 is a breaking successor to v7. Negotiation advertises supported versions and selects exactly one codec for a stream. Integers below are unsigned little-endian. Hashes are 32 raw internal bytes. Heights are `u32` constrained to `block::Height::MAX`; request IDs are nonzero `u64`; work is `u128` encoded as exactly 16 bytes. All messages retain the negotiated application frame limit and the local 2 MiB hard message cap.

**LC-V8-01 [ZW].** A v8 decoder MUST reject unknown discriminants, zero request IDs, invalid booleans, out-of-range heights, count/vector disagreement, trailing bytes, arithmetic overflow, and messages exceeding either negotiated or hard byte/count limits before allocation or CPU-heavy validation.

### 5.2 Messages

The v8 message discriminants are:

| Code | Message |
| ---: | --- |
| `1` | `StatusV8` |
| `2` | `GetHeadersV8` |
| `3` | `HeadersV8` |
| `4` | `HeadersOutcomeV8` |

`StatusV8` is encoded in this order:

```text
work_anchor_height        u32
work_anchor_hash          [u8; 32]
selected_tip_height       u32
selected_tip_hash         [u8; 32]
suffix_cumulative_work    u128 (16-byte little-endian)
oldest_retained_height    u32
max_headers_per_response  u32
max_inflight_requests     u16
max_message_bytes         u32
tree_aux_schema_mask      u32
```

`GetHeadersV8` is encoded as:

```text
request_id                nonzero u64
target_tip_hash           [u8; 32]
locator_count             u8 (1..=13)
locator_hashes            locator_count * [u8; 32]
max_header_count          u32 (>0 and locally/peer capped)
tree_aux_schema           u8 (0, or 1..=32 advertised by the server)
```

`HeadersV8` is encoded as:

```text
request_id                nonzero u64
target_tip_hash           [u8; 32]
common_ancestor_height    u32
common_ancestor_hash      [u8; 32]
header_count              u32
complete                  bool byte
tree_aux_schema           u8 (0, or the exact requested schema)
headers                   header_count canonical headers
body_size_hints           header_count * u32
tree_aux_roots            absent, or header_count records of the selected schema
```

`header_count` is positive except that it may be zero when `complete = 1` and the common-ancestor hash already equals `target_tip_hash`. Otherwise, `complete = 1` means the last returned header hash equals `target_tip_hash`. `complete = 0` means the response reached its count/byte limit; the requester continues with a fresh locator whose first entry is the returned suffix tip and the same snapshot target. All other empty success responses are forbidden.

Each `body_size_hints` value is the advisory total canonical serialized block size. `0` means unknown; the only other legal values are `1..=2,000,000` (`block::MAX_BLOCK_BYTES`). A decoder rejects larger values. A receiver may use a legal nonzero hint for scheduling or byte-budget estimation, but it may not allocate a block buffer, grant admission credit, or relax an independent frame/body bound solely from the hint.

`tree_aux_schema_mask` uses bit `n - 1` to advertise schema `n`; zero advertises no auxiliary schema. A requester sends `tree_aux_schema = 0` for no records, or one schema advertised in the status snapshot. A server may answer with schema `0` when the requested metadata is unavailable; otherwise the response schema MUST equal the request. Schema `0` has no records. Schema `1` has exactly one 156-byte record per header, encoded in this order:

```text
height                    u32
sapling_root              [u8; 32] (canonical Zcash serialization)
orchard_root              [u8; 32] (canonical Zcash serialization)
ironwood_root             [u8; 32] (canonical Zcash serialization)
sapling_tx_count          u64
orchard_tx_count          u64
ironwood_tx_count         u64
auth_data_root            [u8; 32]
```

Each schema-1 height must equal its parallel header’s inferred height. Root decoders must apply their canonical field encodings. Orchard fields use the empty/default root and count zero below NU5; Ironwood fields use the empty/default root and count zero before the configured NU7 activation; `auth_data_root` is all zero below NU5. These defaults are syntactic schema values only and do not authenticate the metadata.

`HeadersOutcomeV8` contains `request_id`, `target_tip_hash`, and one outcome byte:

| Code | Outcome | Meaning |
| ---: | --- | --- |
| `1` | `TargetNotRetained` | target was absent or evicted before the server acquired its snapshot |
| `2` | `NoLocatorIntersection` | none of the sent locator hashes is on the target path |
| `3` | `HistoryPruned` | an intersection may exist below the server’s retained boundary but cannot be served |
| `4` | `Busy` | bounded temporary concurrency/resource refusal |

**LC-V8-02 [ZW].** `StatusV8` MUST describe one atomic durable snapshot: the selected `header_best`, the finalized/work anchor it descends from, locally recomputed suffix work exclusive of the anchor and inclusive of the selected tip, retention floor, and effective serving caps. A receiver MUST treat height and work as advisory until it downloads and validates headers.

**LC-V8-17 [ZW].** `StatusV8` is unsolicited and v8 defines no status request. Each side MUST send its current status immediately after v8 negotiation completes, MUST send an updated snapshot after any committed transition that changes its selected tip, work anchor, retention floor, or advertised serving caps, and MUST refresh at least once per configured status-refresh interval (default 30 seconds, matching the existing v7 `status_refresh_interval`) as a liveness signal. Rapid successive changes MAY be coalesced, but the newest snapshot MUST be sent within 2 seconds of its commit, and a peer is never required to send more than one snapshot per second. Status silence or staleness is grounds for bounded rescheduling and deprioritization, never automatic misbehavior. Header-tip announcement inside a v8 session is carried entirely by these status updates; full-block relay remains on the paths in LC-V8-16.

**LC-V8-03 [ZW].** The requester MUST build its locator from the current local selected tip, then ancestors at offsets `1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1000`, and finally `finalized`; it MUST preserve that order, deduplicate hashes, and cap the list at 13. A continuation of an incomplete response instead uses the continuation locator defined in section 5.2, whose first entry is the returned suffix tip; every fresh target pursuit MUST use the locator above.

**LC-V8-04 [ZW].** `target_tip_hash` MUST come from the status snapshot being pursued. The requester MUST enforce the per-peer and global staged-target limits in LC-RETAIN-04. A newer status MAY supersede an older unstarted target from that peer.

**LC-V8-13 [ZW].** `HeadersV8` body-size hints MUST obey the zero sentinel, 2,000,000-byte maximum, parallel count, and no-hint-based-allocation rules above. Invalid hints make the response malformed protocol data and prevent admission from that response, but do not make any independently obtained header invalid. Legal hints remain unauthenticated scheduling metadata and MUST NOT affect header validity or fork choice.

**LC-V8-14 [ZW].** Tree-aux negotiation and schema-1 records MUST use the exact mask, selector, length, field order, encoding, height correspondence, and activation-dependent defaults above. An unknown, unadvertised, mismatched, malformed, or wrong-length schema is `MalformedProtocol`: the response MUST be rejected before any of its headers or records are admitted, but no independently obtained header becomes invalid. A structurally valid record that later fails cryptographic authentication invalidates only that metadata delivery under LC-AUX-02.

**LC-V8-15 [ZW].** Auxiliary schema meanings are immutable. NU6.3, NU7, or any later upgrade MUST be handled by the existing schema’s explicitly defined activation semantics or by assigning a new advertised schema bit; an implementation MUST NOT reinterpret schema 1 based on a candidate-upgrade name. Adding, removing, resizing, or reordering fields requires a new schema, and exhausting the 32-bit mask or changing message framing requires a successor stream version.

**LC-V8-16 [ZW].** v8 removes the v7 `NewBlock` message. Discriminant `4` means `HeadersOutcomeV8` only on a negotiated v8 stream; it continues to mean `NewBlock` only on a negotiated v7 stream. A v8 session MUST use the ordinary Zcash block-announcement/download path or the separately negotiated Zakura block-sync facility for full blocks, and MUST reject a v7 `NewBlock` payload presented as v8. Version negotiation occurs before discriminant decoding, so no decoder may guess a version from payload shape.

### 5.3 Server snapshot contract

**LC-V8-05 [ZW].** On accepting a request, the server MUST acquire a retained-path snapshot for the exact target hash. It MUST select the first locator entry, in requester order, that lies on that target’s ancestor path, and serve only the contiguous path after that ancestor toward that target.

**LC-V8-06 [ZW].** The accepted target MUST remain snapshot-bound even if the server’s selected tip changes concurrently. The server MUST either complete that exact path through one or more responses/continuations or return one explicit `HeadersOutcomeV8`; it MUST NOT silently substitute its new selected path.

**LC-V8-07 [ZW].** If the target is not retained, no locator intersects, required history was pruned, or resources are temporarily unavailable, the server MUST return the corresponding explicit outcome and no partial ambiguous success.

### 5.4 Requester validation and discovery

**LC-V8-08 [ZW].** The requester MUST correlate request and target, verify that the returned common ancestor was in the exact sent locator, verify its local height/hash, validate every parent link and header rule, and require a completed sequence to end at the requested target hash.

**LC-V8-09 [ZW].** Continuations MUST remain bound to the same target. The requester MUST NOT count advertised target work, unvalidated partial work, or a claimed common-ancestor height in fork choice.

**LC-V8-10 [ZW].** `TargetNotRetained`, `HistoryPruned`, and `Busy` MUST cause bounded status refresh/rescheduling without peer punishment. `NoLocatorIntersection` is non-punitive unless the response contradicts a locator membership fact cryptographically known from the same peer snapshot.

**LC-V8-11 [ZW].** Unsolicited/mismatched request IDs, a common ancestor not in the sent locator, broken returned ancestry, a completed sequence with the wrong target hash, malformed bounds, or an invalid header MUST be peer-attributable evidence.

**LC-V8-12 [ZW].** A same-height/different-hash status, a shorter tip claiming more work, or any unknown target hash MUST be eligible for discovery scheduling. Locally validated complete suffixes, never advertised heights or work, determine whether selection changes. Advertised suffix work is commensurable with local work only when both snapshots name the same work anchor or one anchor is a retained ancestor of the other, in which case the requester MAY rebase the claim across the locally known per-block work between the anchors. When the anchors are not comparable, an unknown target remains discovery-eligible on hash evidence alone, and the incomparable work claim MUST NOT be used to suppress or deprioritize discovery below its normal scheduling.

### 5.5 v7 compatibility

**LC-V7-01 [ZW].** A dual-version node SHOULD continue serving v7’s selected height projection and MAY accept a v7 range only when its first parent hash is already retained, its implicit anchor still identifies the current branch, and every header validates.

**LC-V7-02 [ZW].** A v7 range whose implicit height anchor no longer matches MUST be classified as stale discovery, not peer misbehavior. v7-only operation MUST NOT claim deterministic unknown-fork discovery or same-height convergence.

**LC-V7-03 [ZW].** When an advertised fork cannot connect through retained v7 ancestry, the scheduler SHOULD prefer v8 or legacy locator-based Zcash `getheaders`; it MUST NOT overwrite the selected projection by height to manufacture connectivity.

## 6. Consensus parity and intentional limits

| Rule or evidence | Headers-only engine | Full Zebra/Zakura state | Authority / intentional difference |
| --- | --- | --- | --- |
| Canonical header encoding and version | Exact same parser; version high bit clear and value ≥4 | Same | Zcash consensus |
| Header linkage and inferred height | Parent hash plus checked increment | Parent hash plus body-proven coinbase height | Header height is inferred; body coinbase height can still invalidate |
| Header commitment-field structure | Same height/upgrade-specific parser; cannot prove the committed body/state value | Same parser plus later contextual commitment verification | Observable structure is Zcash consensus; commitment contents remain an intentional limit |
| Equihash and PoW target/filter on Mainnet/Testnet | Exact same network parameters, proof, and target conversion | Same | Zcash consensus |
| Disabled PoW on Regtest/custom networks | Exactly the configured LC-POW-01 waivers | Same configured waivers | Zakura custom-network integration policy, never a Mainnet/Testnet exception |
| Contextual difficulty | Same branch-local 28-header context and upgrade rules | Same algorithm over full blocks | Zcash consensus |
| Median time | Same preceding 11 headers and MTP bounds | Same | Zcash consensus |
| Local two-hour future rule | Deferred and reevaluated | Temporarily rejected/retried | Nondeterministic local-clock policy |
| Most recent settled upgrade | Mandatory manifest pin in every mode | Mandatory manifest pin | Zcash deployment-security requirement, independent of sync checkpoints |
| Local sync checkpoints | Exact configured hash plus absolute trust pin | Checkpoint verifier / finalized state | Trusted local policy; normal post-anchor validation |
| Finality | Automatic 1,000-deep disclosed local pin | Only fully verified full-state finalization | Deliberate mode-specific trust policy |
| Work | Same target-derived formula | Same | Zcash consensus ordering input |
| Equal-work tie | Greater raw `block::Hash.0` | Greater raw `block::Hash.0` | Zebra deterministic policy; differs from protocol first-seen guidance |
| Transaction Merkle/body matching | Cannot validate without body/proof | Validated | Deliberate impossibility boundary |
| Coinbase height and subsidy | Cannot validate | Validated | Deliberate difference |
| Transactions, signatures, scripts, proofs | Cannot validate | Validated | Deliberate difference |
| Nullifiers, anchors, note trees, value pools | Cannot validate | Validated against state | Deliberate difference |
| ZIP 221/244 commitment semantics | Only with authenticated auxiliary data; never bare fork-choice evidence | Validated from bodies/state | Execution assistance is distinct from headers-only validity |
| Header-valid/body-invalid fork | Eligible until deterministic body feedback | Rejected by full validation | Separate frontiers and evidence feedback reconcile the views |

**LC-PARITY-01 [LS].** Every row identified as Zcash consensus MUST be implemented through shared functions or differential vectors that prove bit-for-bit equivalent observable-header acceptance and work across all configured networks and activation boundaries. Local/custom exceptions MUST be exercised separately and MUST NOT weaken production-network vectors.

**LC-PARITY-02 [LS].** Intentional differences MUST be represented as typed states and explicit differential tests; they MUST NOT appear as unexplained mismatches or be papered over by selecting `verified_best`.

**LC-PARITY-03 [ZF].** Equal-work ordering MUST be differential-tested separately against Zebra’s raw-tip-hash comparator; neither this Zebra policy nor any v8 discovery behavior may be described or tested as a Zcash header-validity rule.

## 7. Executable conformance and acceptance matrix

### 7.1 Harness contract

Each named test below is a deterministic test target or parameterized suite. Model tests use a small reference DAG with exact integer work. Async tests use paused channels and explicit release barriers at database commit, response delivery, frontier publication, and reactor observation; wall-clock races are not accepted as coverage.

**LC-TEST-01 [LS].** CI MUST execute every suite mapped from a normative rule on Mainnet and Testnet where network behavior differs, and on Regtest/custom-network fixtures where solution size or disabled-PoW behavior differs.

**LC-TEST-02 [LS].** Every future normative rule added to this document MUST receive a stable rule ID and at least one entry in the rule-to-test matrix in section 7.4 in the same change.

### 7.2 Required suites

#### Header validation

- **HV-01 `codec_bounds`:** canonical/truncated/trailing encodings, bad counts, vector mismatch, overflow, allocation caps, request correlation, and fuzz corpus.
- **HV-02 `version_hash_link_height`:** versions 3, 4, historical non-4 values, high-bit values, full-header hashes, unknown parents, internal breaks, and height overflow.
- **HV-03 `equihash_solution_and_target`:** Mainnet/Testnet/Regtest solution shapes, Equihash vectors, compact-target negative/zero/overflow/noncanonical cases, PoW limits, hash equality at the target boundary, exact disabled-PoW waivers on custom networks, and proof that those waivers are unreachable for Mainnet/Testnet.
- **HV-04 `difficulty_differential`:** every upgrade boundary, first 28 heights, 17/28-header window edges, damping/bounds, ZIP 205/208 Testnet minimum difficulty, response partition permutations, and differential calls against `AdjustedDifficulty`.
- **HV-05 `mtp_and_future_time`:** 1–11 predecessor medians, equality/one-second boundaries, Mainnet height 1/2, Testnet height 653,605/653,606, Regtest/custom activation schedules with PoW enabled and disabled, two-hour equality, deferred reevaluation, clock advancement, and restart while deferred.
- **HV-06 `checkpoint_anchor_context`:** genesis, each configured checkpoint fixture, conflicting ancestry, observable checkpoint checks, trusted later-start context of 28 headers, bad backward linkage, and normal first post-anchor validation.
- **HV-07 `work_vectors`:** compact target to work vectors, suffix sums, overflow fail-close, locally recomputed versus advertised work, and raw-hash equal-work comparator.
- **HV-08 `commitment_field_structure`:** every production activation boundary, malformed and canonical Sapling-root encodings, zero/nonzero Heartwood activation reserved fields, Canopy-at-Heartwood overlapping activation, NU5-and-later opaque commitment structure, and Regtest/custom activation schedules differential-tested against `Header::commitment(network, height)`.
- **HV-09 `settled_upgrade_pins`:** Mainnet NU6.2 at 3,364,600 and Testnet NU6.2 at 4,052,000 with correct/wrong/missing hashes; absent, malformed, duplicate-conflicting, stale, and peer-supplied-only manifests; optional checkpoints disabled or ending below activation; candidate-upgrade non-promotion; and fail-closed startup before publication on both networks and in both engine modes.

#### DAG, durability, and integration

- **DG-01 `dag_model_properties`:** arbitrary insertion order, competing suffixes, multiple hashes per height, selection purity, descendant eligibility, and continuous selected paths.
- **DG-02 `frontier_independence`:** divergent `header_best`/`verified_best`, verified lower-work and header-only higher-work branches, and body feedback without forced verified selection.
- **DG-03 `atomic_transition_crash`:** crash injection before/after every database write, version CAS, response, publication, and observation boundary; reopen after every injection.
- **DG-04 `startup_audit_reconstruction`:** corrupt hash/height/parent/index/projection/generation/checkpoint cases, last-complete-transaction repair, deterministic recomputation, and fail-closed cases.
- **DG-05 `finality_and_reorg`:** fixed-anchor competitors replacing 999, 1,000, 1,001, and longer incumbent descendants in every insertion order; conflicts at/above/below finality; atomic finalization, pruning, work rebasing, and both selected paths.
- **DG-06 `bounded_retention`:** one more eligible candidate tip than the shared fork cap, more than 65,536 nodes, staging-area overflow refusal, permanent-invalid-first deletion, deterministic work/hash eviction, protected paths, shared ancestors, 17 staged targets, evicted-branch reacquisition, and integrated-mode refusal/alarm when protected paths fill the node cap.
- **DG-07 `mode_specific_finality`:** integrated finality from exact fully verified evidence with conflicting `header_best` retirement; rejection of header-depth/resource/body-unavailable synthetic finality; headers-only advancement to exactly tip minus 1,000 before publication; deep-fork rejection only after that pin; mode/finality-source restart audit; non-rollback mode migration; and fail-closed incident plus destroy-and-resync recovery when body validation refutes a migrated headers-only pin.
- **IN-01 `uniform_full_state_transitions`:** direct grow and same/lower/forward reset, full header DAG insertion, empty non-finalized fallback, invalidate, reconsider, finalization, and next-header commit.
- **IN-02 `body_feedback`:** wrong header, bad Merkle/ZIP 244 commitment, commitment-matching deterministic invalidity, transient verifier/state/storage failure, correct supplier attribution, and descendant propagation.
- **IN-03 `body_unavailable`:** multi-peer bounded retry/backoff, retained selection, alternative prefetch, persistent alarm, metrics, resume, and no lower-work failover.
- **IN-04 `operator_reasons`:** nested operator invalidations, reconsider of one reason, permanent reason preservation, atomic reselection, and restart.
- **IN-05 `generation_permutations`:** stale success/failure after reset, generation CAS conflict, request/pending-owner mismatch, old coverage/retry/body task, sole publisher, and absence of peer-score effects.
- **IN-06 `vct_branch_phases`:** reset while VCT repair is scheduled, on-wire, buffered, capacity-waiting, state-dispatched, and completed late; metadata authentication boundary and bad supplier provenance.
- **IN-07 `unknown_anchor_recovery`:** local v7 stale anchor reload/reanchor, bounded retry, no score, coherent publication, and explicit v8 ancestry-contract violation.

#### Protocol and differential behavior

- **P8-01 `v8_codec_and_fuzz`:** every message/outcome, exact integer/hash encoding, zero IDs, booleans, bounds, trailing bytes, oversized payloads, vector lengths, version-selected discriminant 4, v7 `NewBlock` rejection as v8, and mutation fuzzing.
- **P8-02 `locator_vectors`:** tips from heights 0 through 2,000, exact exponential offsets, 1,000 offset, final anchor, deduplication, order, and 13-entry cap.
- **P8-03 `target_snapshot_and_continuation`:** target changes during serve, selection changes during serve, count/byte continuations, same target across pages, exact completed tip, and no silent substitution.
- **P8-04 `outcomes_and_attribution`:** all four explicit outcomes, stale refresh, busy backoff, malformed ancestor, mismatched target/ID, invalid header, and score/no-score assertions.
- **P8-05 `multipeer_fork_discovery`:** shorter-higher-work, longer-lower-work, same-height equal-work, same-height greater-work, unknown fork, same-height status trigger, and permutation-independent convergence.
- **P8-06 `aux_schema_and_body_hints`:** schema mask/selector negotiation, exact schema-1 156-byte golden vectors, every field and root encoding, height mismatch, preactivation defaults, NU5/NU6.3/NU7 boundaries, all-or-none parallel counts, unavailable metadata fallback, unknown/future schemas, `0`/`1`/`2,000,000`/`2,000,001` body hints, and proof that hints cannot drive allocation or admission credit.
- **P8-07 `status_propagation`:** initial status immediately after negotiation, change-driven updates for tip/anchor/retention/cap changes, burst coalescing with the two-second freshness and one-per-second floor, configured periodic refresh, snapshot atomicity against concurrent selection changes, and non-punitive handling of silent or stale-status peers.
- **P7-01 `v7_compatibility`:** selected projection serving, retained-parent acceptance, stale implicit anchor, no false score, unknown fork limitation, locator fallback, and v7-only `NewBlock` discriminant behavior.
- **DF-01 `header_full_state_parity`:** body-valid generated fork graphs fed to the integrated header engine and full state from the same finalized anchor; require identical observable-header acceptance, work, raw-hash tie order, and selected tip before a full-state finalization event.
- **DF-02 `intentional_difference_vectors`:** coinbase height, Merkle/body mismatch, transaction/proof/script failure, nullifier/anchor/value-pool/state-transition failure, local future time, and header-valid/body-invalid outcomes, each with an asserted typed explanation.

### 7.3 Audit closure and incident scenarios

The following matrix supersedes the old audit invariant that the verified chain must always be a prefix of one provisional suffix. The replacement invariant is LC-FRONTIER-01: both selected paths are independently continuous to the same finalized anchor.

| Audit finding | Normative prevention | Deterministic test |
| --- | --- | --- |
| Obsolete commit completion can undo reset | generation/branch/pending-owner gate; version CAS; sole publisher | IN-05/AUD-06, AUD-07 |
| Covered heights are branch-blind | generation- and branch-keyed coverage retired on change | IN-05/AUD-08 |
| Invalidate/reconsider bypass reconciliation | one durable transition for every eligibility/selection mutation | IN-01, IN-04/AUD-10..12 |
| VCT repair survives reset | branch/generation-scoped repair ownership and retirement | IN-06/AUD-09 |
| Equal-work header/full-state mismatch | exact raw `block::Hash.0` comparator | HV-07, DF-01/AUD-03 |
| Single-chain overlay cannot retain candidates | hash-addressed bounded DAG plus selected projection | DG-01, DG-06/AUD-01..04 |
| Headers cannot prove full validity | independent frontiers and evidence-based body feedback | DG-02, IN-02, DF-02 |
| Missing durable anchor / `UnknownAnchor` incident | atomic hash DAG/projection, durable-before-publish, local bounded recovery | DG-03, IN-07/AUD-INCIDENT |
| Same-height and forward resets mishandled | branch identity, never height monotonicity | IN-01/AUD-04 |
| Restart can select inconsistent overlay | atomic metadata plus startup audit/reconstruction | DG-03, DG-04/AUD-14 |
| Latest settled activation is only an optional/stale checkpoint | independent release-authenticated settled-upgrade pin and fail-closed startup | HV-09 |
| Incumbent-relative 1,000-block rule makes arrival order select an unreplaceable fork | eligibility relative only to `finalized`; explicit integrated and headers-only finality sources | DG-05, DG-07 |
| Header commitment field is not structurally interpreted at inferred height | mandatory `Header::commitment(network, height)` parity | HV-08, DF-01 |
| v8 auxiliary records, body hints, and discriminant 4 are underspecified | exact schema/hint codec, immutable schema evolution, and explicit removal of v8 `NewBlock` | P8-01, P8-06, P7-01 |
| Consensus authority is mixed with custom/checkpoint/Zebra policy | separate ZC, ZP, ZF, ZW, and LS rules and parity assertions | HV-03, HV-06, HV-09, DF-01, conformance-manifest self-test |

The 15 named audit scenarios are executable cases, not prose-only acceptance examples:

1. **AUD-01 `losing_branch_later_longer`:** retain branch B while losing; extend it until greatest work; select B; commit the next child of B.
2. **AUD-02 `shorter_branch_greater_work`:** select a shorter but greater-work suffix after full contextual validation.
3. **AUD-03 `same_height_competitors`:** test equal-work greater-raw-hash and unequal-work competitors in both arrival orders.
4. **AUD-04 `consecutive_resets`:** perform lower, same, and forward branch resets, including two before any new forward commit.
5. **AUD-05 `old_network_response`:** release an old-branch wire response after reset and assert no effects.
6. **AUD-06 `late_state_success`:** durably commit/hold old completion, reset and reconcile B, release success, and assert B remains the only published/covered/scheduled branch.
7. **AUD-07 `late_state_failure`:** release a stale old-branch failure and assert no retry, repair, publication, or score.
8. **AUD-08 `old_coverage`:** mark old heights covered, reset at lower/same/forward height, and require a request from the new exact hash.
9. **AUD-09 `repair_every_phase`:** reset during every VCT repair phase and assert old results cannot block or mutate new work.
10. **AUD-10 `invalidate_promotes_alternate`:** invalidate current best, promote retained alternate, verify durable continuity and next child.
11. **AUD-11 `invalidate_to_finalized`:** invalidate the only non-finalized full chain and publish exact finalized `verified_best` without disturbing eligible header candidates.
12. **AUD-12 `reconsider_promotes`:** reconsider same-height and shorter-greater-work branches; preserve non-operator reasons.
13. **AUD-13 `finalize_during_replacement`:** permute finalization with candidate suffix replacement and require one serializable result.
14. **AUD-14 `restart_boundaries`:** restart between database commit, response, publication, and reactor observation for every transition kind.
15. **AUD-15 `next_header_every_tip`:** after every newly selected header or verified tip in AUD-01..14, commit its next linked header successfully.

**AUD-INCIDENT `branch_A_B_late_completion`:** reproduce the production shape: select A, retain losing B, promote B, reconcile and publish B, release a late A completion, then commit the next header anchored to B. Assert no `UnknownAnchor`, no A publication/coverage/score, and durable continuity after reopen.

### 7.4 Normative rule-to-test mapping

Every normative rule is mapped here. A range such as `LC-SCOPE-01..03` includes each individual stable rule in that inclusive range.

| Normative rule IDs | Required test IDs |
| --- | --- |
| LC-SCOPE-01..03 | DF-02, DG-02 |
| LC-SCOPE-04..06 | DF-02, architecture dependency check |
| LC-SCOPE-07 | IN-06, DF-02 |
| LC-SCOPE-08 | DG-07, DF-02 |
| LC-DAG-01..03 | DG-01, DG-04, DG-06 |
| LC-FRONTIER-01..04 | DG-02, DG-03, IN-01, AUD-15 |
| LC-GEN-01..05 | DG-03, IN-05, AUD-05..09, AUD-13..14 |
| LC-TXN-01..02 | DG-03, IN-01, IN-04, IN-06, AUD-10..14 |
| LC-RECOVER-01..03 | DG-04, AUD-14 |
| LC-VAL-01 | HV-01, P8-01 |
| LC-VAL-02..03 | HV-02 |
| LC-HEIGHT-01 | HV-02, P8-03 |
| LC-COMMIT-01..02 | HV-08, DF-01 |
| LC-VAL-04..05 | HV-03 |
| LC-VAL-06 | HV-04, DF-01 |
| LC-POW-01 | HV-03, DF-01 |
| LC-VAL-07..08 | HV-05, DF-02 |
| LC-TIME-01 | HV-05, DF-01 |
| LC-VAL-09 | HV-06 |
| LC-VAL-10 | HV-07 |
| LC-WORKCALC-01 | HV-07, DG-04 |
| LC-VAL-11 | DG-01, HV-04, IN-05 |
| LC-ANCHOR-01..03 | HV-06, DG-05 |
| LC-ANCHOR-04..05 | HV-09, DG-04 |
| LC-SELECT-01..04 | DG-01, HV-07, DF-01, AUD-01..03 |
| LC-REORG-01, LC-FINAL-01..04 | DG-05, DG-07, AUD-13 |
| LC-RETAIN-01..04 | DG-06, P8-05 |
| LC-INT-01..04 | IN-01, IN-04, IN-06, AUD-04, AUD-10..15 |
| LC-BODY-01..04 | IN-02, DF-02 |
| LC-OP-01..02 | IN-04, AUD-10..12 |
| LC-AVAIL-01..03 | IN-03 |
| LC-WORK-01..03 | IN-05, IN-07, AUD-04..09, AUD-INCIDENT |
| LC-ERR-01..02 | IN-02, IN-05, IN-07, P8-04 |
| LC-AUX-01..04 | IN-06 |
| LC-V8-01 | P8-01 |
| LC-V8-02 | P8-01, P8-05, HV-07 |
| LC-V8-03..04 | P8-02, P8-05 |
| LC-V8-05..07 | P8-03, P8-04 |
| LC-V8-08..12 | P8-03, P8-04, P8-05 |
| LC-V8-13..16 | P8-01, P8-06, P7-01 |
| LC-V8-17 | P8-05, P8-07 |
| LC-V7-01..03 | P7-01, IN-07 |
| LC-PARITY-01..03 | HV-03, HV-08, DF-01, DF-02 |
| LC-TEST-01..02 | conformance-manifest self-test |
| LC-ACCEPT-01..05 | conformance-manifest self-test, all suites in sections 7.2 and 7.3 |

The “architecture dependency check” asserts that wallet scanning, FlyClient sampling, block-sync eviction, and unrelated readiness state are absent from the header-engine crate dependency/API surface. The “conformance-manifest self-test” parses stable rule IDs from this document and fails if an ID is duplicated or absent from the machine-readable test manifest maintained beside the implementation.

### 7.5 Acceptance criteria

**LC-ACCEPT-01 [LS].** The redesign MUST NOT be accepted until every normative rule maps to a passing deterministic test, every audit finding maps to a prevention rule and regression, and all 15 audit scenarios plus `AUD-INCIDENT` pass.

**LC-ACCEPT-02 [LS].** In every test and crash point, each published frontier MUST be durable and walkable to `finalized`. For the same finalized anchor and admitted candidates, event order and response partitioning MUST NOT change fork choice. Headers-only finality changes that anchor irreversibly and is accepted only with the explicit policy, proof, and disclosure in LC-FINAL-03, LC-FINAL-04, and LC-SCOPE-08.

**LC-ACCEPT-03 [LS].** Stale generations MUST have zero frontier, coverage, retry, repair, scheduling, publication, body-task, and peer-score effects.

**LC-ACCEPT-04 [LS].** Body-invalid and body-unavailable cases MUST terminate each retry episode in either deterministic reselection or an explicit persistent alarm; neither may produce an infinite silent retry.

**LC-ACCEPT-05 [LS].** The full-state/header differential suite MUST enumerate and explain every intentional difference. Version 1.2 acceptance MUST contain no unresolved design placeholders.

## 8. Implementation oracle and source authority

### 8.1 Authoritative local behavior

The implementation must share code with or remain differential-test equivalent to these repository sources:

| Concern | Local source |
| --- | --- |
| Canonical header/version encoding and local future-time rule | `zakura-chain/src/block/serialize.rs`, `zakura-chain/src/block/header.rs` |
| Height-dependent header commitment-field interpretation | `zakura-chain/src/block/header.rs` (`Header::commitment`), `zakura-chain/src/block/commitment.rs` (`Commitment::from_bytes`) |
| Compact target, work formula, and integer ordering | `zakura-chain/src/work/difficulty.rs` |
| Equihash and context-free PoW checks | `zakura-consensus/src/block/check.rs` |
| Contextual 28-header difficulty, 11-header MTP, and MTP+90-minute rule | `zakura-state/src/service/check/difficulty.rs`, `zakura-state/src/service/check.rs` |
| Full-state greatest-work/raw-tip-hash ordering | `zakura-state/src/service/non_finalized_state/chain.rs` (`impl Ord for Chain`) |
| Local 1,000-block finality horizon | `zakura-chain/src/parameters/constants.rs`, `zakura-state/src/constants.rs` |
| Shared non-finalized fork cap | `MAX_NON_FINALIZED_CHAIN_FORKS` (today `zakura-state/src/constants.rs`; the redesign MUST hoist one shared definition into `zakura-chain::parameters` consumed by both full state and the header engine) |
| Checkpoint hashes and verification | `zakura-chain/src/parameters/checkpoint/`, `zakura-consensus/src/checkpoint.rs` |
| Existing v7 framing/bounds/correlation | `zakura-network/src/zakura/header_sync/wire.rs`, `config.rs`, `validation.rs` |
| Existing reactor, scheduling, coverage, and repair ownership | `zakura-network/src/zakura/header_sync/reactor.rs`, `state.rs`, `work_queue.rs`, `events.rs` |
| Current provisional persistence/startup audit | `zakura-state/src/service/finalized_state/zakura_db/block.rs`, `.../block/tests/header_store_coherence/` |
| ZIP 221/244 auxiliary authentication | `zakura-chain/src/parallel/commitment_aux.rs`, `commitment_aux_verify.rs`, `block/commitment.rs` |

Non-normative provenance: the production failure evidence that motivated the audit scenarios (the latest-v2-fails root-cause and audit notes) lives outside this repository. No normative requirement depends on those files; sections 7.2–7.4 fully specify every behavior and race extracted from them.

### 8.2 Official protocol sources

- [Zcash Protocol Specification](https://zips.z.cash/protocol/protocol.pdf), version `v2026.7.0-33-gc55edc [NU6.2]` dated 2026-07-12 for the version 1.2 settled hashes, and especially its block-header, difficulty-adjustment, work, and best-chain rules.
- [ZIP 204: Zcash P2P Network Protocol](https://zips.z.cash/zip-0204), for bounded framing, locators, and contiguous header responses. Native v8 remains Zakura-specific.
- [ZIP 205](https://zips.z.cash/zip-0205) and [ZIP 208](https://zips.z.cash/zip-0208), for Testnet difficulty and Blossom target-spacing behavior.
- [ZIP 221: FlyClient consensus-layer changes](https://zips.z.cash/zip-0221), for history-tree commitments and work metadata. Its logarithmic proof protocol is explicitly excluded from v1.
- [ZIP 244: transaction identifier non-malleability](https://zips.z.cash/zip-0244), for `hashAuthDataRoot` and `hashBlockCommitments` semantics.
- [ZIP 307: light-client payment detection](https://zips.z.cash/zip-0307), explicitly excluded because this specification is a header-chain engine, not a wallet scanning protocol.
- [Official ZIP index](https://zips.z.cash/), for upgrade proposal status. Candidate status alone is never evidence that an upgrade is settled and never changes LC-ANCHOR-04/05 or an auxiliary schema.

### 8.3 Fixed design decisions

This version fixes the following choices: one integrated reusable engine; linear verification of all candidate headers; native v8 fork discovery with limited v7 compatibility; independent `header_best`, `verified_best`, and `finalized`; Zebra’s greater-raw-tip-hash equal-work policy; integrated finality sourced only from fully verified state; headers-only automatic local finality 1,000 descendants behind `header_best`; an eligible-candidate-tip cap equal to the shared non-finalized fork-cap constant, currently 10; 65,536 non-finalized DAG nodes; body-invalid branch disqualification; body-unavailable selection plus alarm; independent mandatory settled-upgrade pins in every deployment mode; absolute local checkpoint pins with observable header checks; v8 auxiliary schema 1 and no v8 `NewBlock`; and authenticated-only use of VCT/tree-aux metadata.

Any future change to one of these choices requires a new specification version, explicit migration and compatibility rules, and corresponding updates to the conformance manifest. It is not an implementation detail.
