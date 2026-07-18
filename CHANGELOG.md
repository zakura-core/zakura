# CHANGELOG

All notable changes to Zakura are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org).

## [Unreleased]

### Fixed

- Database format upgrades now finish before startup exposes the finalized
  state database; only configured periodic format checks continue in the
  background.
- Preserve Sprout note-commitment history during fresh verified-commitment-tree
  fast sync, so later JoinSplit spends can use historical anchors. Affected
  Mainnet databases that previously ran v2 p2p + fast mode require repair at
  startup from a reviewed trusted artifact, snapshot redownload, or genesis
  resync.

### Changed

- Update the embedded zcashd-compat binary and default split-container image to
  valargroup/zcashd v1.0.1.
- Header sync now schedules only forward ranges from the durable verified block
  tip. Startup rejects configured anchors above that base, and no longer
  backfills headers below a checkpoint anchor.

### Fixed

- Deliver mined/submitted block gossip to peers that were momentarily unready
  when the block was advertised. A block broadcast via `AdvertiseBlockToAll`
  queued a re-send for unready peers, but the queued send future was dropped
  before the connection wrote the `inv`, so the connection treated the request
  as canceled and silently skipped it. Because a zcashd-compat sidecar follows a
  single upstream and learns the tip only from block `inv`s, it could then stall.
  The queued send now runs to completion. Only local mining paths (regtest, e2e,
  and local-mining deployments) exercise `AdvertiseBlockToAll`; standard
  following nodes advertise network blocks via `AdvertiseBlock` and are
  unaffected.
- Deliver committed-tip block gossip to configured zcashd-compat sidecar peers
  even when they are momentarily unready. The "always include sidecars" carve-out
  in block broadcasts only covered ready peers, so a sidecar that was unready when
  a block was gossiped was skipped; because it follows a single upstream and
  learns the tip only from block `inv`s, it then stalled until a later gossip
  coincided with a ready service. The latest hash is now queued for an unready
  sidecar and delivered once it is ready again, bounding the stall to one
  readiness cycle.

## [1.0.1] - 2026-07-17

### Added

- Deterministic attributed-memory accounting for decoded blocks in the
  block-sync pipeline, with per-decode histograms and active-pipeline gauges
  ([#159](https://github.com/zakura-core/zakura/pull/159)).

### Changed

- Zakura v1.0.1 remains supported through the expected Ironwood activation
  (height 3,428,143, ~2026-07-28) and halts one week after it: the
  end-of-support window widens from 7 to 18 days after the estimated release
  height ([#234](https://github.com/zakura-core/zakura/pull/234)).
- Block-sync now keeps its apply backlog in serialized wire form and decodes
  bodies only for the verifier submission window, so decoded memory is bounded
  regardless of backlog depth. Admission accounting charges serialized pools at
  wire size, and the default look-ahead budget is a 1.5 GiB memory target:
  initial-sync memory no longer grows with block era
  ([#190](https://github.com/zakura-core/zakura/pull/190)). The
  `MALLOC_ARENA_MAX` mitigation from
  [#148](https://github.com/zakura-core/zakura/pull/148) remains as the
  complementary allocator-retention layer.

### Fixed

- Prevent initial sync from stalling at checkpoint boundaries by refilling the
  verifier submission window after stale apply completions
  ([#215](https://github.com/zakura-core/zakura/pull/215)).
- Header sync now keeps timed-out ranges in a bounded, single-owner work queue,
  retries them indefinitely with short peer-local avoidance, and commits
  pipelined responses in height order
  ([#138](https://github.com/zakura-core/zakura/pull/138)).
- Stop header-sync maintenance from repeatedly waking on a stale VCT repair
  retry deadline while the repair request is still in flight, and honor the
  configured status refresh interval from startup
  ([#218](https://github.com/zakura-core/zakura/pull/218)).
- Pruned finalized blocks remain visible to chain-identity queries, including peer
  block-hash responses and RPC confirmation lookups, after their bodies are removed
  ([#133](https://github.com/zakura-core/zakura/pull/133)).
- Stop pruned nodes from serving fabricated zero transaction counts and auth-data
  roots when a historical block body is unavailable during Zakura header sync
  ([#133](https://github.com/zakura-core/zakura/pull/133)).
- Bind VCT prevalidation reuse to the block's height, canonical hash, and
  authorizing-data root, and reject cached same-block bodies with altered
  authorizing data as permanently invalid instead of parking them for retry
  ([#208](https://github.com/zakura-core/zakura/pull/208)).
- Source VCT successor witnesses only from contextually validated headers and
  their persisted authorizing-data roots, so a buffered block body with altered
  authorizing data can no longer evict a valid supplied root and stall header
  sync ([#212](https://github.com/zakura-core/zakura/pull/212)).
- Serve `BlockRoots` responses for every requested finalized height, including
  heights whose blocks added no Sapling commitments, so header sync no longer
  stalls on false coverage gaps
  ([#202](https://github.com/zakura-core/zakura/pull/202)).
- Invalidating a block at a chain's non-finalized root now removes every fork
  built on that block, not just the chain with the matching tip
  ([#202](https://github.com/zakura-core/zakura/pull/202)).
- Reject Mainnet-shaped Equihash solutions on Regtest: each network now accepts
  only its own Equihash parameter variant, matching zcashd
  ([#202](https://github.com/zakura-core/zakura/pull/202)).
- Generated local-genesis networks activating NU6.1 or later now satisfy the
  one-time lockbox disbursement rule instead of rejecting every possible
  activation block ([#202](https://github.com/zakura-core/zakura/pull/202)).
- Reject oversized `FindBlocks` responses before they enter the syncer's
  discovered-hash reserve ([#207](https://github.com/zakura-core/zakura/pull/207)).
- Build the verbose `getrawmempool` transaction-ID index once per response
  instead of once per mempool transaction, removing quadratic work from large
  mempools ([#203](https://github.com/zakura-core/zakura/pull/203)).
- Weight Sapling batch verification by spend and output proof count rather than
  bundle count, so batch limits bound the actual Groth16 verification work
  submitted to one blocking task
  ([#150](https://github.com/zakura-core/zakura/pull/150)).

### Security

- Validate transparent spends without cloning the block's spent UTXO set for
  every transaction, removing quadratic work that let a specially crafted
  block stall block validation for nearly a minute on fast hardware
  ([GHSA-4g24-549m-hp75](https://github.com/zakura-core/zakura/security/advisories/GHSA-4g24-549m-hp75)).
- Attribute transactions pushed directly by a peer to that peer when they fail
  verification, so peers sending consensus-invalid transactions — including
  transactions with invalid proofs that poison batched proof verification and
  force repeated, expensive fallback verification — are now misbehavior-scored
  and banned instead of degrading block validation unidentified
  ([GHSA-g7c4-2w6c-cr3r](https://github.com/zakura-core/zakura/security/advisories/GHSA-g7c4-2w6c-cr3r)).
- Reserve the serialized block header, transaction count, and maximum
  pool-modified coinbase size when selecting mempool transactions for
  `getblocktemplate`, so an adversary can no longer provoke templates that
  violate the consensus block size limit and stall mining on a targeted node
  ([GHSA-95m2-vx53-v2jw](https://github.com/zakura-core/zakura/security/advisories/GHSA-95m2-vx53-v2jw)).

## [1.0.0] - 2026-07-15

Initial release of Zakura.

Zakura is a fork of the Zcash Foundation's
[Zebra](https://github.com/ZcashFoundation/zebra), forked at Zebra v5.0.0. For
the history of this codebase before the fork, see
[upstream's CHANGELOG](https://github.com/ZcashFoundation/zebra/blob/main/CHANGELOG.md).
