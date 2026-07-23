# NU6.3 Ironwood Zakura–Zebra consensus-parity audit

> Real-code audit results:
> [`docs/audits/nu6_3_ironwood_zakura_zebra.md`](../audits/nu6_3_ironwood_zakura_zebra.md)

Status: in progress — source audit complete; differential execution pending
Priority: stop-ship for the first Zakura release used at Mainnet NU6.3
Scope owner: consensus maintainer
Final approvers: one Zakura consensus maintainer and one independent reviewer

## Objective

Produce reproducible evidence that a pinned Zakura release candidate and a
pinned Zebra release candidate:

1. make the same accept/reject decision for every transaction and block in the
   audit corpus;
2. compute the same consensus identifiers and commitments;
3. reach the same consensus state after every accepted block; and
4. implement every NU6.3 consensus requirement in the pinned specification
   set.

The core executable property is:

```text
accept_zakura(S, B, N, H) == accept_zebra(S, B, N, H)

and, when both accept:

consensus_state_zakura(S, B) == consensus_state_zebra(S, B)
```

Here `S` is an identical logical pre-state, `B` is identical candidate block
bytes, `N` is the network, and `H` is the candidate height. Logical state is
compared instead of implementation-specific database bytes.

The claim at the end of this audit is deliberately bounded:

> No consensus divergence was found for the pinned sources, binaries, state
> prefixes, and generated corpus recorded in the audit manifest.

Neither Zebra nor an AI reviewer is a specification oracle. Zebra is a
comparison implementation, and both implementations can share code,
dependencies, vectors, or the same misunderstanding. The audit therefore uses
four independent evidence classes: specifications, independently generated
vectors, implementation comparison, and execution against real and synthetic
chains.

## Urgency

At the time this plan was created, Testnet NU6.3 had activated at height
4,134,000. Mainnet activation is configured for height 3,428,143, estimated by
the project at approximately 2026-07-28. Run the short critical path first:

1. freeze sources and binaries;
2. replay the Testnet activation range in both nodes;
3. close the known anchor and end-to-end Ironwood coverage gaps;
4. run the activation-boundary and malformed-input differential corpus;
5. issue a go/no-go report no later than 24 hours before Mainnet activation.

The short path produces an early risk report only. Final `GO` requires G0–G7;
an incomplete gate at the release deadline is `NO-GO`. Continue live parity
monitoring after the release decision.

## Kickoff board

All gates start red. Checking in this plan does not constitute an audit pass.
The coordinator assigns named people/agents and timestamps when execution
starts.

| Task | Owner role | Deliverable | Dependency | Initial state |
| --- | --- | --- | --- | --- |
| A0 | Coordinator | Frozen sources, release binaries, and `source-lock.json` | Release candidate selection | Ready |
| A1 | Specification agent | Reviewed normative requirement ledger | A0 | Blocked |
| A2 | Zakura agent | Zakura implementation and alternate-path map | A0, A1 IDs | Blocked |
| A3 | Zebra agent | Zebra-candidate map plus v6.0.0-to-candidate fix map | A0, A1 IDs | Blocked |
| A4 | Harness agent | Common-schema pure and validator adapters | A0 | Blocked |
| A5 | Vector agent | Final-ID v6, ZIP 221 v3, and independent vectors | A0, A1 | Blocked |
| A6 | Adversarial agent | Boundary matrix, mutations, and killed mutants | A1, A4, A5 | Blocked |
| A7 | State agent | Mode/reorg/migration/VCT comparison | A2, A3, A4 | Blocked |
| A8 | Differential runner | Testnet full sync and activation replay | A0, A4 | Blocked |
| A9 | Independent verifier | Reproduction and signed go/no-go report | A1–A8 | Blocked |

Start A8 as soon as A0 and the observation adapter are available; it is the
longest-running task. Run A1, A2, and A3 independently before merging their
conclusions.

## Seed source lock

These revisions are discovery inputs, not permanent floating references. The
audit coordinator must replace each reference with the exact candidate used by
the release and record the resolved commit, binary digest, build command, Rust
toolchain, target triple, Cargo.lock digest, features, and environment.

| Input | Seed revision observed on 2026-07-23 |
| --- | --- |
| Zakura candidate | `zakura-core/zakura` `origin/main` at `bc5a61dc0e2dfdb339aa28b8d0c615b33b298ab7` |
| Zebra candidate | `ZcashFoundation/zebra` `v6.2.1` at `f3edc40601b4a377693a32c982d4cddf1795fb6f` |
| Zebra historical Ironwood baseline | `ZcashFoundation/zebra` `v6.0.0` at `bb41d69013edbfa8594bb097fa751f47eeb31445` |
| Common Git ancestor | `1e6519ea91e2d3035c20aadd4d9a40dcac2eed3a` |
| Zcash ZIPs and protocol source | `zcash/zips` `main` at `9112f7595be9252fe43986a9938e210750997e9d` |
| Canonical test vectors | `zcash/zcash-test-vectors` `master` at `78321beacb0e0477e33cd002b56585a107c2708c` |
| Ironwood transaction vectors currently copied into Zakura | `valargroup/zcash-test-vectors` at `16b5d0cee253e9947cbd9860f2ec2be1633d6484` |

Resolve and hash the exact sources and package checksums for `orchard`,
`zcash_protocol`, `zcash_primitives`, `zcash_history`, Halo 2, RedPallas, and
the script verifier. Semver versions alone are insufficient. Until ZIP 2006
has a specification body, the exact Orchard circuit implementation and
verification-key derivation are especially important consensus inputs.

Only the Zebra candidate participates in parity results and the run ID. The
historical v6.0.0 baseline is used to discover and classify every later
Ironwood fix; it is not a second oracle.

The current Zcash protocol PDF identifies itself as
`v2026.7.0-61-g9112f7 [NU6.2]`, while the NU6.3 ZIPs refer to a future
`2026.8.0 [NU6.3]` specification. At the pinned source revision,
`protocol.tex` still contains `0x????????` for the v6 version group ID, ZIP
2006 is a Reserved header with no specification body, and ZIP 258 and ZIP 229
are Draft. The candidate protocol follow-up
[zcash/zips#1336](https://github.com/zcash/zips/pull/1336) is itself a
conflicting draft with unresolved inline TODOs.

This is an input-stability and completeness risk, not permission to infer
missing rules. G0 is red at plan creation. Before the final gate, either:

- pin and review the published NU6.3 protocol specification; or
- pin an immutable addendum from the protocol owners that supplies every
  missing rule and constant and resolves every conflict in the exact ZIP,
  circuit, dependency, and Zebra revisions used as the interim normative set.

A blanket waiver cannot resolve ambiguous consensus behavior. Any source
change after the lock invalidates the affected audit results. A bot may
identify the changed requirement families, but a human consensus maintainer
decides the rerun scope.

### Normative input set

The requirements ledger must include, at minimum:

- the Zcash Protocol Specification;
- ZIP 200, network upgrade mechanics;
- ZIP 201, network peer-management changes;
- ZIP 204, P2P protocol versions;
- ZIP 209, chain value pool balances;
- ZIP 213, shielded coinbase requirements;
- ZIP 221, chain history;
- ZIP 229, version 6 transaction format and commitments;
- ZIP 253, network protocol version enforcement;
- ZIP 255, NU6.3 advertised network protocol version;
- ZIP 244, transaction identifiers and signature hashes;
- ZIP 258, NU6.3 deployment and activation rules;
- ZIP 317, kept in a separate policy/mining section;
- ZIP 2005, Ironwood quantum-recoverable notes; and
- ZIP 2006, the Orchard cross-address restriction.

Wallet migration behavior from ZIP 318 and ZIP 326 is out of validator
consensus scope unless it changes bytes produced by Zakura mining or RPC code.

## Threat model

The audit looks for:

- one node accepting a block or transaction that the other rejects;
- both nodes accepting a block but computing different transaction IDs,
  authorization digests, signature hashes, block commitments, history roots,
  note commitment roots, nullifier sets, or value pool balances;
- a rule applied at the wrong height, on the wrong network, to the wrong
  transaction version, or only in the mempool rather than block validation;
- a checkpoint, fast-sync, pruned-state, migration, rollback, or restart path
  bypassing a rule that the normal semantic path enforces;
- malformed or adversarial input causing a panic, hang, resource-dependent
  result, or fail-open behavior in only one implementation;
- Zakura and Zebra sharing an erroneous dependency or vector;
- a specification or upstream implementation change landing after review; and
- correlated AI review errors, especially where one agent's conclusion
  anchors later reviewers.

Mempool policy differences are not consensus divergence. They are recorded in
a separate result class because they can still prevent propagation or mining.
Different error messages are also not divergence when both nodes reject at the
same boundary.

## Consensus surface

Each family below gets a stable requirement ID and a class of `consensus`,
`policy`, or `operational`. Consensus IDs must map to a specification location,
Zakura code, Zebra code, a positive test, a negative test, and a differential
case. Policy and operational IDs instead define their applicable comparison
semantics; any omitted field needs a reviewed `not_applicable_reason`.

| Family | Required coverage |
| --- | --- |
| `ACT` | Branch ID `0x37a5165b`; Testnet height 4,134,000; Mainnet height 3,428,143; activation at `H`, not `H + 1` |
| `VER` | Valid transaction versions before and after activation; v6 version group ID; consensus branch checks; expiry and version gating |
| `WIRE` | Exact v6 field order and conditional presence; CompactSize canonicality; action/proof/signature counts; flag reserved bits; truncation and trailing data |
| `HASH` | v6 txid, auth digest, component digests, signature hashes, personalizations, empty components, anchor movement into authorizing data, and auth-data block root |
| `ORC` | No Orchard actions in coinbase after activation; Orchard value balance nonnegative; cross-address disabled; rules apply to v5 and v6; correct circuit key routing |
| `IRN` | Ironwood action semantics; spend/output flags; coinbase spend prohibition; proof size and Halo 2 verification; binding and spend authorization signatures |
| `NCT` | Separate Ironwood note commitment tree, anchors, subtree handling, capacity, and root updates |
| `NULL` | Separate Ironwood nullifier set; duplicates within a transaction, block, non-finalized chain, finalized state, and across reorgs |
| `POOL` | Separate Ironwood chain value pool; per-pool nonnegativity; total `MAX_MONEY` bound; checked overflow and rollback behavior |
| `CB` | Ironwood coinbase note plaintext lead byte `0x03` and all applicable shielded coinbase checks |
| `HIST` | ZIP 221 v3 activation; earliest/latest Ironwood roots; Ironwood transaction count; serialization; aggregation; history and block commitment roots |
| `STATE` | Commit ordering, duplicate handling, reorg, rollback, restart, database upgrade, pruning, checkpoint verification, full verification, and Zakura VCT fast sync |
| `MINE` | Block template construction and proposal/submit paths cannot create or admit a block that the validators disagree on; include a NU6.3 Unified Address Orchard-receiver reward that is remapped to Ironwood |
| `NET` | Advertised protocol version 170160, upgrade-aware accepted peer floors, IBD floors, and transaction relay behavior, classified separately unless it changes the accepted chain |

The audit must search for repeated protocol literals and terminology drift
after building the ledger. A value appearing in more than one implementation
location must either use a shared constant or have a test that proves the
copies agree.

## AI review topology

Use one coordinator plus three workers per wave. Agents in the first pass get
the same source lock and output schema, but do not receive other agents'
conclusions. This reduces anchoring and makes omissions visible.

### Roles

1. **Coordinator**
   - freezes inputs, assigns requirement IDs, owns the evidence manifest, and
     prevents agents from silently expanding the normative source set;
   - does not approve code or findings it authored.
2. **Specification agent**
   - extracts every normative NU6.3 validator rule and all activation
     conditions;
   - labels ambiguous, draft, or conflicting text without resolving it by
     guesswork.
3. **Zakura implementation agent**
   - maps each requirement to the narrowest production path and all alternate
     validation/state paths;
   - identifies missing, duplicated, dead, feature-gated, or policy-only
     checks.
4. **Zebra implementation agent**
   - independently produces the same map at the pinned Zebra revision;
   - records Ironwood PR and follow-up provenance.
5. **Differential-harness agent**
   - builds common-schema adapters and a shared serialized corpus;
   - cannot generate the expected result with code imported from either node.
6. **State-transition agent**
   - audits note trees, nullifiers, value pools, history, migrations, reorgs,
     restarts, pruning, checkpoints, and VCT.
7. **Adversarial agent**
   - mutates valid cases at the wire and semantic levels and minimizes every
     mismatch;
   - treats panic, timeout, or nondeterminism as a finding.
8. **Independent verifier**
   - starts with fresh context, reruns evidence from manifests, challenges
     waived cases, and signs or rejects the final report.

An agent that writes a fix or expected vector cannot be its final reviewer.
AI-authored findings require either a reproducer or precise source evidence;
confidence language alone is not evidence.

### Common agent prompt contract

Every task prompt must include:

```text
Use only the pinned source revisions in source-lock.json.
Do not treat Zebra, Zakura, or a shared dependency as the specification.
Separate facts, inferences, and unresolved questions.
For every claim, cite a specification section or commit:path:line range.
Report absence only after listing the exact searches performed.
Mapping agents: do not edit production code during the audit pass.
Emit results in the required schema, including commands and artifact hashes.
Stop and file a finding for ambiguity; never invent the intended rule.
```

## Evidence layout and schemas

Each run gets an immutable ID:

```text
nu6.3-YYYYMMDD-HHMM-<zakura12>-<zebra12>
```

CI stores the full evidence bundle in access-controlled immutable storage. The
small manifest, requirement ledger, non-sensitive finding summaries, and final
report are retained in the repository or another reviewable immutable store.

Critical and High reproducers, raw inputs, and exploit details go only to
access-controlled storage using the repository's `SECURITY.md` process.
Coordinate disclosure with Zebra when a finding is shared. Public artifacts
contain opaque finding IDs and non-sensitive hashes, not weaponizable inputs.

```text
audit/
  source-lock.json
  rename-map.toml
  tools.json
  requirements.csv
  implementation-map.csv
  corpus-manifest.json
  results/
    zakura.jsonl
    zebra.jsonl
    diff.jsonl
  findings/
    NU63-0001.md
  command-log/
  final-report.md
  SHA256SUMS
```

`source-lock.json` records repository commits, dirty-state checks, dependency
lock digests, build flags, compiler, binary SHA-256 digests, configuration,
activation schedule, and container image digests. Any candidate, specification,
vector, or dependency change creates a new run ID and source lock. Reusing an
artifact requires a recorded dependency and impact proof.

Every A0–A9 task has a machine-readable task record with:

```text
schema_version,run_id,task_id,argv,cwd,environment_digest,config_digest,
input_artifacts,output_artifacts,timeout_seconds,retry_policy,
expected_exit_codes,pass_predicate,started_at,completed_at
```

Each requirements-ledger row contains:

```text
schema_version,run_id,requirement_id,requirement_class,source_revision,
source_location,normalized_rule,activation_condition,transaction_versions,
applicability,comparison_semantics,not_applicable_reason,zakura_locations,
zebra_locations,positive_cases,negative_cases,differential_cases,status,
reviewers
```

Each executable result is one JSON object:

```json
{
  "schema_version": 1,
  "run_id": "nu6.3-...",
  "case_id": "ORC-COINBASE-H+0-V5-001",
  "requirement_ids": ["ORC-COINBASE"],
  "input_sha256": "...",
  "state_fixture_id": "...",
  "generator_revision": "...",
  "generator_seed": 0,
  "network": "regtest",
  "network_config_sha256": "...",
  "height": 100,
  "activation_schedule_sha256": "...",
  "active_upgrade": "NU6.3",
  "active_branch_id": "37a5165b",
  "chain_prefix_sha256": "...",
  "logical_prestate_sha256": "...",
  "implementation": "zakura",
  "binary_sha256": "...",
  "config_sha256": "...",
  "adapter_patch_sha256": null,
  "stage": "contextual_block",
  "observed_boundary": "orchard_coinbase_rule",
  "decision": "reject",
  "normalized_class": "consensus_invalid",
  "txid": null,
  "auth_digest": null,
  "block_commitment": null,
  "sapling_root": null,
  "orchard_root": null,
  "ironwood_root": null,
  "history_root": null,
  "value_pools": null,
  "tip_hash": null,
  "logical_poststate_sha256": null,
  "attempt": 1,
  "repetition": 1,
  "duration_ms": 0,
  "process_exit": 0,
  "signal": null,
  "timed_out": false,
  "panicked": false,
  "stderr_sha256": "..."
}
```

Compare accept/reject, identifiers, commitments, and post-state. Preserve raw
errors for diagnosis but do not require error enum or text equality. Every
nullable field has explicit applicability metadata so “not produced” cannot be
confused with “not compared.”

The implementation map records requirement ID, implementation revision,
production locations, entry points, shared dependencies, alternate paths, and
review disposition. The corpus manifest records each serialized input, state
fixture, generator, seed, intended validation boundary, and artifact digest.
The diff schema records both result IDs, field-level comparison semantics,
mismatches, and disposition. Command logs record the task contract plus exit,
signal, timeout, stdout, and stderr digests. Version these schemas and validate
every artifact before a gate consumes it.

The logical-state hash is over a versioned canonical manifest, never raw
RocksDB bytes. For synthetic chains it includes sorted UTXOs, all known
Sapling/Orchard/Ironwood nullifiers and anchors, tree roots and positions,
history state, value pools, and the tip. For the real Testnet replay, where a
complete set may not be exportable, retain the common block prefix and compare
all exported commitments and balances, then probe every shielded spend and
anchor present in the replay range through test-only read adapters.

Each finding records:

- stable ID and severity;
- affected requirement IDs and revisions;
- fact, impact, and root cause;
- minimal serialized reproducer and prior-state manifest;
- exact commands and observed Zakura/Zebra results;
- whether the specification resolves the expected behavior;
- proposed fix and regression test;
- author, independent reviewer, disposition, and rerun evidence.

## Execution waves

### Wave 0 — freeze and reproduce

1. Create clean detached worktrees for the two pinned revisions.
2. Build release binaries with locked dependencies and the exact production
   features.
3. Record all source, tool, binary, configuration, and image hashes.
4. Run each repository's existing formatting, lint, unit, property, and
   integration gates. Record pre-existing failures; do not relabel them as
   irrelevant without reproducing them on the pinned base.
5. Confirm all consensus-affecting experimental `cfg` and feature gates are
   identical to the release build.

Exit criterion: the exact release-artifact digest is authoritative. A second
machine produces byte-identical binaries or enumerates known nondeterministic
sections and passes a predefined section-aware comparison and attestation
method.

### Wave 1 — requirements and independent code maps

Run the specification, Zakura, and Zebra agents in parallel. The coordinator
then joins their outputs by requirement ID.

For each row:

- verify the activation condition and transaction versions before inspecting
  whether the rule body looks similar;
- trace network bytes through deserialization, semantic verification,
  contextual verification, state commit, and rollback;
- inspect checkpoint and mempool paths separately;
- distinguish shared librustzcash/orchard behavior from node-owned checks; and
- map every Zebra Ironwood follow-up after its initial implementation, not just
  the original feature PR.

Use semantic comparison rather than a raw repository diff. At plan creation,
541 commits were reachable only from the Zakura seed and 165 only from the
Zebra seed. Crate/path renames make a whole-tree diff too noisy to be a useful
oracle. `rename-map.toml` may normalize only reviewed mechanical renames; every
remaining consensus-trusted-code-base hunk needs a disposition.

Start the Zebra delta inventory with initial Ironwood support `15e30b343` and
follow-ups `138a830ff`, `9a903dd11`, `27471e84f`, `c0858c79a`, `63b0dab9a`,
and `ee288df73`. In the v6.0.0-to-v6.2.1 range, explicitly disposition
`b36a3ed59`, `b23dfeacd`, `ffb09bd4c`, `b96c586b0`, `9502e78ae`,
`fe8639e9c`, and `fdb55f422`. This is a priority seed list, not an allowlist:
classify every candidate-range change that can affect parsing, validation,
state, template construction, peer-supplied block handling, or resource bounds.

Exit criterion: 100% of normative rows have two implementation maps and at
least one independent reviewer; every unmapped or ambiguous row is a finding.

### Wave 2 — canonical vectors and pure-function parity

Create test-only adapters in the detached worktrees. They read the same JSONL
requests and expose:

- structural parse and canonical reserialization;
- transaction ID and authorization digest;
- all v6 component digests and transparent signature hash modes;
- history leaf/internal-node bytes and root;
- note commitment tree append/root behavior; and
- stateless transaction verification outcome.

The adapters must call production code. Keep them out of published crate APIs.
Do not add `pub` or `pub(crate)` solely for the harness; place adapters where
private APIs are already visible or exercise the public validator boundary.
Record adapter patches and adapter-binary digests separately from the locked
release binaries. Pure-function results may use those adapters; end-to-end
release gates must run the unmodified locked binaries. An adapter result is
invalid unless its patch is reviewed for semantic pass-through behavior.

Run:

1. canonical `zcash-test-vectors` ZIP 221 v3 and ZIP 244 vectors;
2. the copied Valar v6 vectors;
3. independent ZIP 2005 recoverable-Ironwood-note known answers for the `0x03`
   plaintext form, `0x0B`-domain `rcm` derivation, commitment reconstruction,
   and one-at-a-time wrong derivation inputs;
4. vectors generated independently from the pinned specification; and
5. cross-generated cases: serialize in each implementation and parse/hash in
   the other.

The copied Ironwood vectors are not independent evidence until they agree with
an upstream canonical source or a separately implemented generator reviewed
against ZIP 229. Keep provenance on every vector.

Build the expected proof-size table independently from the pinned normative
inputs for every tested action count. Computing `expected`, `expected - 1`, and
`expected + 1` through either node or their shared Orchard dependency is not an
independent boundary oracle.

Exit criterion: zero byte, hash, decision, or round-trip mismatches. Any vector
whose expected value comes only from one node remains `unproven`.

### Wave 3 — differential validator corpus

Use a shared serialized corpus with stable seeds. Cover the Cartesian boundary
matrix first, then property-based mutations.

#### Boundary matrix

- Mainnet, Testnet, and configured Regtest schedules with NU6.3 after NU6.2,
  NU6.3 without NU6.2, NU6.2/NU6.3 co-activation, and NU6.3/NU7
  co-activation;
- `H - 2`, `H - 1`, `H`, `H + 1`, `H + 39`, and `H + 40`;
- v4, v5, and v6;
- no shielded component, Orchard only, Ironwood only, and both;
- coinbase and non-coinbase;
- empty, one-action, multi-action, and maximum-size boundary cases; and
- mempool, semantic block, contextual block, checkpoint, proposal, and
  `submitblock` entry points where applicable.

`H + 39` and `H + 40` straddle the end of Zakura's mempool branch-ID
misbehavior grace behavior. The consensus decision must remain strict for every
height.

Run checkpoint cases as their own route matrix. For semantic proposal and
mutation cases, configure `max_checkpoint_height < H` and record the observed
validation boundary; otherwise a below-checkpoint rejection or checkpoint
route can mask the transaction rule under test.

#### Required mutations

- transaction version, version group ID, branch ID, expiry, field order,
  noncanonical CompactSize, truncation, and trailing bytes;
- every Orchard and Ironwood flag combination, including all reserved bits;
- declared action/proof/signature count disagreement and proof sizes at
  `expected - 1`, `expected`, and `expected + 1`;
- empty-component digest domains and one-field-at-a-time mutations;
- Sapling, Orchard, and Ironwood anchor mutations proving that v6 anchors affect
  authorization data but not the transaction ID as specified;
- correct and incorrect circuit-key routing for v5 Orchard at NU6.2 and NU6.3,
  v6 Orchard, and v6 Ironwood;
- Orchard deposit, withdrawal, zero balance, and minimum/maximum amount edges;
- Ironwood pool underflow, total-pool overflow, and arithmetic boundaries;
- duplicate nullifiers within one bundle, transaction, block, non-finalized
  chain, finalized state, and after rollback; also confirm Orchard and Ironwood
  namespaces remain separate;
- unknown, prior, current, and future anchors, including attempts to substitute
  an Orchard root as an Ironwood root;
- coinbase Orchard actions, Ironwood spends, and decryptable/non-decryptable
  outputs with note lead bytes around `0x03`;
- malformed, identity, small-order, and noncanonical group encodings;
- invalid proofs, binding signatures, and spend authorization signatures;
- note commitment tree capacity at `2^32 - 1`, `2^32`, and overflow through a
  synthetic frontier fixture;
- ZIP 221 v2/v3 history boundary, root endianness, CompactSize counts, parent
  aggregation, and count overflow; and
- block auth-data and history commitment mutations.

Every unmutated seed must first pass full validation with correct transaction
Merkle, history, and authorization commitments. Run at least 100,000
deterministic mutation cases after the bounded matrix.
Partition and report cheap parse/hash mutations separately from valid
cryptographic and contextual cases. Require coverage per requirement and
intended rejection boundary, and record evidence that each case reached that
boundary; a large corpus that fails only during early parsing is insufficient.
Retain seeds and minimize every mismatch before triage.

Also maintain a curated source-mutation suite. It must fail if a test build:

- changes an activation comparison from `<` to `<=` or the reverse;
- routes v5 Orchard at NU6.3 to the NU6.2 key;
- swaps Orchard and Ironwood bundle, root, anchor, nullifier, or value fields;
- omits Ironwood from value-pool or ZIP 221 history calculations;
- accepts any reserved flag bit;
- lets the mempool branch-ID grace affect mined-block validation; or
- returns an empty Ironwood tree for a missing postactivation state row.

Every curated mutant must compile, be demonstrably non-equivalent, and be
killed by a named assertion. An uncompilable or equivalent mutation is an
invalid suite entry, not a kill or survivor. A valid surviving mutant is a High
evidence gap even if the ordinary differential corpus is green.

Exit criterion: zero unexplained decision or consensus-output mismatches; no
panic, timeout, nondeterministic result, or unbounded allocation.

### Wave 4 — state-transition and mode parity

Execute accepted and rejected sequences, not just isolated transactions:

- full semantic sync across `H - 1` to `H + 1`;
- checkpoint sync followed by semantic sync;
- Zakura VCT fast sync followed by normal commits;
- archive and pruned state;
- fresh state and every supported pre-Ironwood database upgrade;
- clean and crash-like restart before, at, and after activation;
- competing forks that cross activation, become best, and roll back;
- rollback below activation followed by a different Ironwood branch;
- duplicated nullifier and old-anchor attempts after reorg;
- blocks with no Ironwood actions between blocks that have actions; and
- batch/concurrent verification completed in different orders.

After every accepted height compare:

- tip height and block hash;
- Sapling, Orchard, and Ironwood note commitment roots;
- history root and header commitment;
- all chain value pool balances;
- accepted nullifier membership probes; and
- restart/reopen results.

Zakura-only storage and sync optimizations do not need matching internal rows in
Zebra, but their externally derived consensus state must match Zebra's.

Exit criterion: the same sequence is accepted, the same fork wins, and the
listed consensus state agrees after every transition and restart.

### Wave 5 — real Testnet replay

Testnet activation has already produced the strongest positive corpus.

Prerequisite: build a read-only per-height observation adapter for each
implementation and record its source, patch, and binary hashes. Release RPCs do
not expose every required authorization, history, and Ironwood-root field. The
adapter must not affect validation or state, and release acceptance decisions
still come from the unmodified locked binaries.

1. Pin a confirmed tip height, hash, contiguous block sequence, observation
   timestamp, and required confirmation depth in the run manifest.
2. Perform at least one clean full sync with each pinned binary. Compare stable
   consensus observations at shared checkpoints, then establish a common
   preactivation height using their own clean states.
3. Prove from configuration and validation logs that blocks under comparison
   traverse semantic validation rather than being trusted solely through a
   postactivation checkpoint.
4. Replay from at least height 4,133,500 through the pinned Testnet tip.
5. Compare the accepted hash, roots, history commitment, and value pools at
   every height.
6. Extract every distinct v4/v5/v6 and Orchard/Ironwood transaction shape as a
   permanent regression seed.
7. Resync the same range through Zakura's production fast/checkpoint path and
   compare its final state with the semantic run.

Testnet agreement proves positive-chain compatibility, not rejection
compatibility; Wave 3 remains mandatory.

Exit criterion: exact per-height agreement through a tip observed by at least
two independent network sources.

### Wave 6 — independent verification and release decision

The independent verifier:

1. validates `SHA256SUMS` and reconstructs the run from `source-lock.json`;
2. samples at least one case from every requirement family;
3. fully reruns all prior mismatches, waivers, activation-boundary cases, and
   real-chain replay;
4. searches the final candidate diff for consensus-affecting changes after the
   source lock; and
5. signs a report that lists limitations and residual risks.

## Release gates

All gates are fail-closed.

| Gate | Pass condition |
| --- | --- |
| G0 — inputs | Exact candidate/spec/vector/dependency revisions and binaries are locked; a final NU6.3 spec or protocol-owner addendum resolves all placeholders, stubs, TODOs, and conflicts |
| G1 — requirements | Every consensus rule has reviewed Zakura and Zebra mappings plus positive and negative cases; every policy/operational row has applicable comparison semantics or a reviewed exclusion |
| G2 — implementation delta | Every consensus-relevant Zebra follow-up and every Zakura-only consensus path has a reviewed semantic disposition |
| G3 — vectors | All canonical and independently produced vectors agree; no expected value relies on a single implementation |
| G4 — differential corpus | Zero unexplained accept/reject, digest, commitment, state, panic, timeout, or nondeterminism differences |
| G5 — state modes | Full, checkpoint, VCT, prune, migration, restart, reorg, and rollback cases reach the same consensus state |
| G6 — Testnet | Both candidates accept the pinned chain and exported post-state agrees at every height from the preactivation window through the locked tip |
| G7 — review | Independent rerun is reproducible; no open critical or high finding; required approvers sign |

Severity and disposition:

- **Critical:** demonstrated block-consensus accept/reject or post-state
  divergence, or a panic on an accepted real-chain input. Stop ship
  immediately.
- **High:** missing or wrong consensus rule, untested alternate validation path,
  unstable normative input, or a likely divergence without a complete live
  reproducer. Stop ship until fixed and rerun.
- **Medium:** policy, mining, or availability behavior that does not currently
  split block consensus. Fix or receive explicit owner acceptance.
- **Low:** evidence quality, diagnostics, or maintainability issue with no
  plausible consensus impact. Track normally.

Waivers cannot turn a Critical or High result green. A disputed expected result
stays red until the normative source owner resolves it.

## Initial evidence gaps to close

These are audit gaps discovered while writing the plan, not confirmed
consensus bugs:

1. `zakura-state/src/service/check/tests/anchors.rs` contains an explicit TODO
   for Orchard and Ironwood anchor tests.
2. The isolated mempool-load harness currently defaults to NU6.1 and documents
   that Ironwood coverage is waiting on a transaction-builder dependency
   update. Existing end-to-end coverage therefore must not be credited as
   Ironwood coverage.
3. The large v6 transaction/hash vector file is copied from a Valar fork of
   `zcash-test-vectors`. It was generated before the final v6 version group and
   NU6.3 branch IDs; current tests patch those bytes at runtime and compare the
   result to the shared librustzcash lineage rather than final published
   expected hashes. Add final canonical or independently generated known-answer
   vectors.
4. The checked-in block-vector inventory has no obvious NU6.3 activation block
   corpus. Import real Testnet activation-range seeds with provenance.
5. The current protocol PDF is still labelled NU6.2, its source contains a v6
   version-group placeholder, ZIP 2006 has no specification body, and the
   NU6.3 deployment and transaction-format ZIPs are Draft. The open protocol
   follow-up is conflicting and contains unresolved NU6.3 TODOs. G0 cannot pass
   until an immutable normative set resolves these gaps.
6. Zakura and Zebra have substantial independent history after their June 2026
   common ancestor. Recent Zebra Ironwood follow-ups and Zakura ports must be
   mapped one by one rather than assumed equivalent by title.
7. Zakura's VCT fast-sync, pruning, rollback, and database-repair paths are
   broader than Zebra's corresponding storage paths and need explicit
   post-state parity evidence.
8. `docs/specs/fork-aware-header-chain-engine.md` says Ironwood auxiliary fields
   remain empty until NU7, while the VCT embedded-frontier provenance test uses
   NU6.3 to decide when an empty Ironwood frontier is required. Determine
   whether the design document's NU7 label is stale or describes a different
   schema boundary before relying on VCT evidence.
9. NU6.3 block-template construction remaps a Unified Address Orchard receiver
   to an Ironwood miner reward, but current template fixtures are anchored at
   NU5. Add an end-to-end template case and validate and submit its serialized
   block to both candidates.
10. Network code advertises protocol version 170160 but still labels the NU6.3
    mapping provisional, while initial-IBD peer floors are selected from NU6.2.
    Disposition advertised version, near-tip floor, and IBD floor separately
    under the `NET` policy class.
11. Zakura's native ZIP 244 implementation and cached v6 empty-bundle digest
    constants are consensus-critical optimized paths. Recompute every constant
    independently and compare cached and uncached results for all component
    combinations.

Close each item with a finding/disposition and artifact links. Deleting a TODO
or changing a comment is not closure.

If execution commits Rust source or `Cargo.toml`, including a Rust harness or
fix, follow the repository policy: open the draft PR first, add exactly one
PR-numbered changelog fragment, and run the required version-bump process for
deliberately published API changes. The documentation-only setup of this plan
does not require a fragment.

## CI and monitoring after the audit

Add three levels after the harness has been reviewed:

1. **Pull request gate**
   - source-lock drift check;
   - bounded activation/version/flag matrix;
   - canonical vectors;
   - targeted rerun selected from changed requirement IDs.
2. **Nightly**
   - deterministic 100,000-case mutation corpus;
   - state-transition matrix;
   - trailing Testnet replay and live-tip comparison.
3. **Release gate**
   - full clean rebuild and evidence bundle;
   - complete corpus and Testnet activation replay;
   - independent reviewer signature.

From 48 hours before Mainnet activation until at least 48 hours after it,
compare production Zakura and Zebra observers at every block. Alert on tip hash,
history commitment, treestate root, or value-pool mismatch. Monitoring detects
a failure; it does not substitute for the release gates.

## Completion report

The final report must state:

- exact sources, dependency locks, binaries, configurations, and chain ranges;
- requirement coverage counts and all excluded or unresolved rules;
- corpus counts by family and accepted/rejected result;
- every mismatch, including those fixed during the audit;
- real Testnet per-height comparison result;
- mode/state comparison result;
- open Medium/Low findings and explicit owners;
- independent reproduction result;
- final go/no-go decision and signatures; and
- all API-surface changes made by fixes or harness work, including
  `pub(crate)` changes.

Do not use “no divergence” without the bounded claim from the Objective.
