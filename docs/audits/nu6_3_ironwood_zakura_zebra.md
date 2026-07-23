# NU6.3 Ironwood Zakura–Zebra code audit

Date: 2026-07-23

## Result

No reachable canonical-block accept/reject divergence was found between the
audited Zakura and Zebra revisions.

The review did find two medium operational/security differences, two confirmed
low-severity state-service defects, and several low-severity assurance or API
differences. None currently permits Zakura to commit a block Zebra rejects, or
to derive different NU6.3 consensus state from the same accepted chain.

This is a source-audit result, not a proof of equivalence. Release approval
should remain conditional on the differential activation and state-root tests
listed under [Release gates](#release-gates).

## Audited revisions

- Zakura: `bc5a61dc0e2dfdb339aa28b8d0c615b33b298ab7`
- Zebra v6.2.1: `f3edc40601b4a377693a32c982d4cddf1795fb6f`
- Common ancestor: `1e6519ea91e2d3035c20aadd4d9a40dcac2eed3a`

The audit inspected the real source trees at those revisions, including Zebra's
Ironwood implementation and its post-v6.0 security fixes. It covered:

- activation constants, branch IDs, and network versions;
- v6 parsing, serialization, txids, auth digests, and sighashes;
- Orchard and Ironwood flag, value-balance, coinbase, and proof rules;
- Halo2 verifier-key selection and failure propagation;
- semantic, checkpoint, proposal, and `submitblock` routes;
- note trees, history-tree V3, anchors, nullifiers, value pools, and reorgs;
- mempool admission, peer scoring, mining templates, and recent Zebra fixes;
- Zakura's VCT fast-sync path, which has no direct Zebra equivalent.

## Findings

### F-01 — Medium: native P2Pv2 loses invalid-transaction peer attribution

Zakura locally rejects invalid Ironwood proofs, but a transaction received over
the native P2Pv2 path is converted to a source with no `PeerSocketAddr`:

- Zakura:
  `zakurad/src/components/mempool/downloads.rs:75-86,424-432,500-528`
- Native push path:
  `zakura-network/src/zakura/legacy_gossip.rs:438-441` and
  `zakurad/src/components/inbound.rs:177-183,676-690`
- Scoring is skipped without an address:
  `zakurad/src/components/mempool.rs:119-131,739-743`

Zebra's legacy pipeline retains the advertiser and applies the verifier's
misbehavior score:

- `zebrad/src/components/mempool/downloads.rs:373-380,402-435,455-458`
- `zebrad/src/components/mempool.rs:684-695`

An attributable invalid shielded proof scores 100 in both verifiers. On
Zakura's native path, however, that score cannot be sent to a peer. Testnet and
other non-main networks enable dual networking by default at
`zakura-network/src/config.rs:126-137`.

Impact: repeated expensive invalid v6 proofs can consume verification work
without contributing to a native peer ban. Per-peer pending downloads bound
concurrency, but not repeated work over time. This is a DoS-resistance
difference, not a consensus-rule difference. Mainnet's default legacy path is
unaffected unless P2Pv2 is enabled.

Recommended action: carry a stable native peer identity through the mempool
source type and add an invalid-Ironwood-proof scoring integration test.

### F-02 — Medium: Zakura Testnet GBT selects minimum difficulty early

Zebra uses the consensus Testnet minimum-difficulty boundary directly:

- `zebra-state/src/service/read/difficulty.rs:323-357`
- regression test:
  `zebra-state/src/service/read/difficulty.rs:429-505`

Zakura subtracts an extra 150-second mining allowance and then future-dates the
template to `previous_time + 451s`:

- allowance:
  `zakura-state/src/service/read/difficulty.rs:35-38`
- early threshold:
  `zakura-state/src/service/read/difficulty.rs:320-342`
- timestamp clamp:
  `zakura-state/src/service/read/difficulty.rs:352-369`

For candidate times in `(previous_time + 300s, previous_time + 450s]`, Zakura
can return a future-dated minimum-difficulty template while Zebra retains the
requested time and standard target.

The resulting block can still satisfy consensus. This is mining-template policy,
but it can unnecessarily depress Testnet difficulty and makes cross-client
templates differ.

Recommended action: port Zebra's
`eager_window_stays_standard_difficulty_and_is_not_future_dated` regression and
remove the early transition unless the difference is an explicit Zakura policy.

### F-03 — Low: rollback leaves stale VCT upgrade and handoff metadata

This is a confirmed, reachable Zakura-only storage defect.

The first VCT-aware commit records upgrade height `U` only if no marker exists:

- `zakura-state/src/service/finalized_state/zakura_db/shielded.rs:786-794`

The supported rollback path truncates per-height trees and the commitment-root
index, but does not clear or move `VCT_UPGRADE_METADATA` or
`VCT_SYNC_METADATA`:

- public rollback:
  `zakura-state/src/service/finalized_state/zakura_db/rollback.rs:271-312`
- pruning:
  `zakura-state/src/service/finalized_state/zakura_db/rollback.rs:991-1048`
- CLI:
  `zakurad/src/commands/rollback_state.rs:48-99`

After rollback below `U`, reopening below handoff `H` restores frozen VCT mode
at `zakura-state/src/service/finalized_state.rs:419-425`. A fast-resynced height
below stale `U` has no per-height tree, but `vct_tree_absent` returns false
because it recognizes only `[U, H)`:

- `zakura-state/src/service/finalized_state/zakura_db/block.rs:917-933`

`serve_block_roots` then chooses backward-searched trees below `U` rather than
the correct root index:

- `zakura-state/src/service/finalized_state/commitment_aux.rs:570-600`

A temporary regression on the pinned revision constructed a finalized chain,
seeded realistic `U = 3, H = 10`, rolled back to height 1 through the production
API, and reopened the database. It failed with:

```text
rollback left stale U=Some(Height(3)); a VCT resync at Height(2) skips its tree
but vct_tree_absent(Height(2)) is false
```

Impact: historical tree and `tree_aux` responses can disagree with the root
actually committed in the serving index. A receiving VCT client checks roots
against successor headers and fails closed, so this does not create accepted
bad consensus state. It can disrupt root serving or recovery.

Recommended action: atomically reset or move `U/H` when rolling back below
`U`, and land the failing regression.

### F-04 — Low: stale rejected-block UTXO can escape `AwaitUtxo`

Zebra drains rejected hashes immediately before reading pending UTXOs:

- `zebra-state/src/service.rs:1165-1180`

Zakura reads the sent UTXO directly:

- `zakura-state/src/service.rs:1323-1336`

Both implementations drain before `KnownBlock`, and Zakura contains the core
same-hash retry fix. Until another drain-triggering request runs, however,
`AwaitUtxo` can return an output produced by a contextually rejected queued
block.

The contextual verifier re-fetches and validates spends at
`zakura-state/src/service/check/utxo.rs:56-79`, so the stale value does not let
an invalid block commit. The impact is transient state-service behavior and
availability.

Recommended action: port Zebra's drain immediately before `AwaitUtxo` and add a
queue–reject–await regression.

### F-05 — Low: v6 wire validation happens at different stages

Zebra round-trips parsed v5/v6 transactions through librustzcash before
returning them:

- `zebra-chain/src/transaction/serialize.rs:1123-1140,1187-1218`

Zakura returns its parsed representation directly:

- `zakura-chain/src/transaction/serialize.rs:1146-1162,1205-1232`

Therefore, a v6 Orchard or Ironwood proof with trailing padding passes Zakura's
wire parser but fails Zebra's parser. Zakura rejects it later through canonical
proof-size checks and librustzcash conversion:

- `zakura-consensus/src/transaction.rs:425-471,567-569`
- regressions:
  `zakura-chain/src/transaction/tests/vectors.rs:2061-2217`

The checkpoint route does not create a bypass. Zakura validates the block's
auth-data commitment on normal finalized insertion at
`zakura-state/src/service/finalized_state.rs:1035-1060`, and the VCT route binds
the actual body's auth-data root at lines 779-882. Padding changes that root, so
the body fails before it is committed.

Impact: different rejection stage and parsing cost only. No block acceptance
difference was found.

Follow-up status: parser hardening is implemented in the working tree. Zakura
now rejects noncanonical Orchard proof sizes while parsing every V5 transaction
and always enforces canonical Orchard and Ironwood proof sizes for V6. This is
intentionally stricter than Zebra and the pre-NU6.2 consensus rules for
historical V5 transactions. It relies on the accepted compatibility assumption
that deployed historical chains contain no noncanonical Orchard proofs.
Because transaction parsing has no network context, the policy also applies to
Testnet and custom networks. Regressions cover canonical, one-byte-short, and
one-byte-padded proofs and explicitly document the pre-NU6.2 librustzcash
difference.

The official ZIP-244 digest vectors contain synthetic 32-to-287-byte Orchard
proofs, whose exact bytes are part of their expected hashes. A `#[cfg(test)]`
`pub(super)` decoder keeps those vectors usable without weakening any
production deserialization path.

### F-06 — Low assurance gap: Testnet checkpoint coverage differs

- Zakura Testnet checkpoint tip: height `4,023,200`
- Zebra Testnet checkpoint tip: height `4,180,800`
- NU6.3 Testnet activation in both: height `4,134,000`

Zebra checkpoint-verifies activation through height `4,180,800`; Zakura
semantically verifies that range. All 10,059 shared Testnet checkpoint entries
and all 14,155 shared Mainnet entries have identical hashes. Both Mainnet
checkpoint tips are before NU6.3.

Checkpoint verification binds height, work, Equihash, transaction Merkle root,
branch ID, and checkpoint ancestry. Finalized-state commitment validation binds
authorizing data. No canonical acceptance difference follows from the coverage
delta, but the clients exercise different validation routes on historical
Testnet activation blocks.

Recommended action: run an instrumented fresh Testnet sync and retain evidence
that Zakura's semantic path accepts every Zebra-checkpointed activation block.

### F-07 — Low assurance gap: Ironwood types are aliases in Zakura

Zakura re-exports Orchard's tree, nullifier, and shielded-data types:

- `zakura-chain/src/ironwood.rs:8-11`

Zebra uses distinct newtypes:

- `zebra-chain/src/ironwood.rs:18-47`

The current Zakura production paths keep the pools separate:

- v6 field accessors:
  `zakura-chain/src/transaction.rs:1231-1330`
- parallel tree update:
  `zakura-chain/src/parallel/tree.rs:77-143`
- mixed-pool regression:
  `zakura-chain/src/parallel/tree.rs:326-440`
- separate finalized nullifier column families:
  `zakura-state/src/service/finalized_state/zakura_db/shielded.rs:721-744`
- separate anchor checks:
  `zakura-state/src/service/check/anchors.rs:124-155`

No current pool swap was found. The aliases nevertheless allow a future
Orchard/Ironwood positional swap to compile.

Recommended action: introduce distinct Ironwood newtypes, or replace every
positional multi-pool API with a named struct and retain distinct-root tests.

### F-08 — Low assurance gap: consensus-crypto lock closures differ

Both revisions use the same direct Ironwood dependencies, including:

- `orchard 0.15.0`
- `halo2_proofs 0.3.2`
- `zcash_history 0.5.0`
- `zcash_primitives 0.29.0`
- `zcash_protocol 0.10.0`

Their complete lock closures are not identical. Notably, Zakura locks
`reddsa 0.5.2`, while Zebra locks `reddsa 0.5.1`. Other patch-level differences
include `bitvec`, `arrayvec`, and `zeroize`.

No differing RedDSA verification behavior was found between the locked patch
versions, and no signature-result divergence was reproduced. This remains a
reproducibility and assurance gap.

Recommended action: diff the complete locked consensus-crypto closure in CI and
require an explicit waiver for each version delta.

## Other non-consensus differences

- For the first 40 NU6.3 blocks, Zakura and Zebra both reject a mempool v5
  transaction carrying the NU6.2 branch ID. Zakura assigns score 0 during its
  grace period; Zebra assigns 100. Block verification rejects immediately in
  both.
- Zakura rejects invalid v6 Orchard flags and pre-NU6.3 v6 objects during
  serialization. Zebra can serialize those in-memory values, but both parsers
  and both semantic verifiers reject the resulting bytes.
- A direct invalid v6 `SIGHASH_SINGLE` library call can assert in Zakura and
  return a digest in Zebra. Both consensus script callbacks reject the missing
  corresponding output before hashing.
- Both clients reserve enough block-template space to avoid Zebra's oversized
  GBT vulnerability. Zakura additionally reserves maximum coinbase-tag growth,
  so it can select fewer transactions near the size limit.

## Proven NU6.3 equivalences

| Surface | Evidence |
| --- | --- |
| Activation | Testnet `4,134,000`; Mainnet `3,428,143` in both |
| Branch ID | `0x37a5165b` in both |
| Network protocol | minimum and advertised version `170160` in both |
| V6 activation | both reject v6 before NU6.3 and accept it at NU6.3+ |
| V6 group ID | `0xD884_B698` in both |
| Orchard transition | both forbid cross-address Orchard, net additions to Orchard, and coinbase Orchard bundles |
| Ironwood flags | both require spend or output when actions exist; cross-address alone is insufficient |
| Canonical proofs | both enforce Orchard and Ironwood expected proof sizes before acceptance |
| Halo2 keys | both use the NU6.3-onward verifier for v6 Orchard and Ironwood |
| Digests | Zakura's native ZIP-244 txid/auth/sighash results match librustzcash over the tested v5/v6 shapes |
| Note encryption | both route v6 Ironwood recovery through `IronwoodDomain` |
| Note trees | current Zakura paths preserve Orchard/Ironwood order and state separation |
| History V3 | both commit Ironwood start/end roots and transaction count through `zcash_history 0.5.0` |
| State | separate anchors, nullifiers, value pools, subtree state, and reorg updates are present in both |
| Failure behavior | proof, signature, conversion, timeout, proposal, and `submitblock` errors fail closed |
| Zebra fixes | oversized GBT, quadratic UTXO checking, same-hash replacement, early ZIP-317 checks, and invalid-proof scoring are present, subject to F-01 and F-04 |

## VCT-specific conclusion

Zebra always recomputes the note-commitment frontiers. Zakura can instead fold
peer-supplied roots through its VCT fast path. The source review found the path
fail-closed:

- supplied roots require a linking successor header;
- actual block transaction counts are folded into the history leaf;
- pre-NU6.3 Ironwood roots are pinned to the empty root;
- a frozen frontier with no verified root refuses the commit;
- handoff frontier roots must equal authenticated supplied roots.

Mainnet's embedded VCT handoff is height `3,418,406`, while NU6.3 activates at
`3,428,143`. VCT therefore resumes ordinary frontier computation before
Ironwood activates. Testnet has no embedded VCT handoff frontier.

No current VCT/Ironwood consensus difference was found. The embedded Mainnet
frontier is still a Zakura-specific trust artifact and must be compared with an
independent Zebra/legacy reconstruction at the handoff and its successor.

## Executed verification

All clean-pass commands ran against the pinned audit worktrees.

| Command or check | Result |
| --- | --- |
| `cargo test -p zakura-chain --lib v6_` | 11 passed |
| Zakura ZIP-244 differential property test with 1,024 cases | 1 passed |
| `cargo test -p zakura-chain --lib parallel::tree::tests` | 4 passed |
| `cargo test -p zakura-chain --lib history_tree::tests::vectors` | 13 passed |
| Zakura `primitives::zcash_history` vector tests | 3 passed |
| `cargo test -p zakura-consensus --lib ironwood` | 6 passed |
| Zebra v6 transaction vector filter | 3 passed |
| Zebra Ironwood consensus filter | 2 passed |
| Zebra history-tree vector filter | 2 passed |
| Shared Mainnet/Testnet checkpoint hash comparison | no mismatches |
| Temporary VCT rollback regression | failed as expected, confirming F-03 |

## Release gates

Before declaring consensus equivalence for NU6.3:

1. Fix or explicitly waive F-01 and F-02.
2. Fix F-03 and F-04 and retain their regressions.
3. Run a shared transaction-byte corpus through both parsers and semantic
   verifiers, including padded proofs, reserved flags, branch-ID boundaries,
   every sighash mode, and malformed point/signature encodings.
4. Feed both nodes the same valid and mutant blocks at activation `H-1`, `H`,
   and `H+1`; compare accept/reject results, errors, txids, auth digests,
   history roots, note roots, nullifier sets, and value pools.
5. Run the Zakura block/state differential twice, with VCT enabled and disabled.
6. Independently reconstruct and compare the Mainnet frontiers at VCT handoff
   `3,418,406` and after appending `3,418,407`.
7. Record and review the complete locked consensus-crypto dependency diff.

## Repository impact

The original audit added documentation only. The F-05 follow-up changes the
transaction parser, parser tests, and related comments. It makes no public or
`pub(crate)` API-surface changes. The existing Sprout-verification edits in
`zakura-consensus/src/transaction.rs` and
`zakura-consensus/src/transaction/tests.rs` were preserved.
