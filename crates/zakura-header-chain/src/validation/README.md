# Header validation

This module validates observable Zcash block-header rules. Validation is split
into two stages:

1. **Prepare** checks facts that depend only on the supplied headers,
   authenticated network policy, the supplied parent height, and local time.
   It returns a sealed `PreparedHeaderBatch`; it does not read or mutate the
   retained header graph.
2. **Contextual admission** binds that prepared result to the live retained
   parent and predecessor history. Only planner admission can turn prepared
   evidence into retained graph state.

This keeps parallel proof-of-work checks independent of storage while ensuring
that branch-sensitive results cannot be detached from the branch that justified
them. A prepared batch is validation evidence, not a commit.

## Context-free preparation

[`prepare_headers`](prepare/pipeline.rs) performs:

| Check | Authority |
| --- | --- |
| Nonempty batch and per-transition size bound | Zakura resource policy |
| Supported signed header version and locally computed full-header hash | [Zcash §7.6](https://zips.z.cash/protocol/protocol.pdf#blockheader) and [§7.7.2](https://zips.z.cash/protocol/protocol.pdf#difficulty) |
| Checked height inference from the supplied parent height | Local trust rule required by height-dependent consensus checks |
| Height- and network-specific commitment-field interpretation | [Zcash §7.6](https://zips.z.cash/protocol/protocol.pdf#blockheader) structure; value authentication requires the full block |
| Valid compact target within the network proof-of-work limit | [Zcash §§7.7.3–7.7.4](https://zips.z.cash/protocol/protocol.pdf#diffadjustment) |
| Little-endian header hash at or below its target | [Zcash §7.7.2](https://zips.z.cash/protocol/protocol.pdf#difficulty) |
| Equihash solution shape and proof | [Zcash §7.7.1](https://zips.z.cash/protocol/protocol.pdf#equihash) |
| Accept now or assign `DeferredUntil(nTime - 2 hours)` | [Zcash §7.6 full-validator rule](https://zips.z.cash/protocol/protocol.pdf#blockheader); explicitly nondeterministic, not consensus |
| Exact per-block work | [Zcash §7.7.5](https://zips.z.cash/protocol/protocol.pdf#workdef) |
| Receipt binding the parent, network, and trust-anchor digest | Zakura stale-work protection |

Authenticated custom networks may waive the proof, hash-filter, and contextual
`ThresholdBits` equality checks when proof of work is explicitly disabled.
Mainnet and the default public Testnet cannot receive that waiver.

## Retained-branch contextual admission

The transition planner's
[`admit_prepared_headers`](../transition/planner/event_effects/header_admission.rs)
uses the retained graph and up to 28 predecessor headers to check:

| Check | Authority |
| --- | --- |
| Receipt still matches the live parent, network, and trust anchors | Zakura stale-work protection |
| First-parent and internal batch linkage by `hashPrevBlock` | [Zcash §7.6](https://zips.z.cash/protocol/protocol.pdf#blockheader) |
| Prepared hash, height, and work recompute exactly | Zakura evidence-integrity rule |
| `nBits == ThresholdBits(height)`, including the 17-block averaging window, damping, and Testnet ZIP 205/208 behavior | [Zcash §§7.6 and 7.7.3](https://zips.z.cash/protocol/protocol.pdf#diffadjustment) |
| `nTime` is strictly greater than median-time-past of up to 11 predecessors | [Zcash §7.6](https://zips.z.cash/protocol/protocol.pdf#blockheader) |
| Active 90-minute median-time upper bound, including the Mainnet height-1 exception | [Zcash §7.6](https://zips.z.cash/protocol/protocol.pdf#blockheader) |
| Finality, settled-upgrade pins, checkpoints, completion shape, ownership, and auxiliary provenance | Zakura local chain policy |

The difficulty and time calculations are implemented in
[`contextual/`](contextual/).

## Full-block boundary

Preparation validates only the **structure and height-dependent interpretation**
of the header commitment field. Authenticating `hashMerkleRoot`, the final
Sapling root, ZIP-221 history roots, or NU5+ ZIP-244 block commitments requires
the block body and state.

The full-block consensus path also checks the two-million-byte block limit,
requires at least one transaction, and enforces that the first transaction is
coinbase and later transactions are not. Those rules remain in body sync and
are intentionally outside this header-validation module.

For the atomic transition and commit model around admission, see
[`transition/README.md`](../transition/README.md).
