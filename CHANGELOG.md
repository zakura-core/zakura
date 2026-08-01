# CHANGELOG

All notable changes to Zakura are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org).

## [Unreleased]

## [1.0.5] - 2026-07-27

### Changed

- Improved address-book metric collection performance, which was surprisingly
  active in CPU profiles
  ([#451](https://github.com/zakura-core/zakura/pull/451)).
- Increased the number of peers returned in refreshed `GetAddr` responses from
  approximately one quarter to one half of eligible address-book entries
  ([#458](https://github.com/zakura-core/zakura/pull/458)).
- Restored the legacy `FindBlocks` compatibility behavior while additional
  peer-response and chain-head edge cases are validated
  ([#459](https://github.com/zakura-core/zakura/pull/459)).

### Fixed

- Prevent banned peer addresses from blocking outbound reconnections
  ([#439](https://github.com/zakura-core/zakura/pull/439)).
- Fixed legacy genesis sync livelocking at height 0. Reverted the pruned-peer
  block-routing filter from
  [#440](https://github.com/zakura-core/zakura/pull/440), which failed
  historical block requests with a `NoReadyPeers` error whenever no ready peer
  advertised `NODE_NETWORK` — including under ordinary peer-set saturation. The
  syncer treats that error as fatal, so every sync round was aborted about a
  millisecond after dispatch and its in-flight downloads discarded
  ([#448](https://github.com/zakura-core/zakura/pull/448)).
- Avoid duplicate block advertisements when a mined-block notification and the
  committed-tip fallback become ready together
  ([#450](https://github.com/zakura-core/zakura/pull/450)).
- Fixed legacy block sync stalling when a peer advertises only one new block
  at the chain head
  ([#452](https://github.com/zakura-core/zakura/pull/452)).
- Replenish legacy outbound peers during periodic crawls when fewer than 27%
  of the configured outbound connection slots are active
  ([#462](https://github.com/zakura-core/zakura/pull/462)).
- Fixed Mainnet genesis VCT sync failing closed at the handoff height by serving
  embedded frontier roots from `PeerSource` when no authenticated DB root exists
  at that height
  ([#464](https://github.com/zakura-core/zakura/pull/464)).
- Fixed Prometheus cardinality growth for `zakura.net.connection.state` by
  aggregating live connections by command instead of retaining historical
  per-peer address series
  ([#467](https://github.com/zakura-core/zakura/pull/467)).
- Extended the legacy syncer's near-tip stall grace from approximately 6
  minutes to 20 minutes, reducing unnecessary peer disconnects during ordinary
  block-time variance and propagation delay
  ([#469](https://github.com/zakura-core/zakura/pull/469)).
- Credit outbound peer replenishment demand when a failed dial restores
  channel demand, so poor connectivity does not double-spend the timer budget
  ([#474](https://github.com/zakura-core/zakura/pull/474)).

## [1.0.4] - 2026-07-25

### Added

- Added `checkpoint.duplicate.newer_request` to count in-queue same-hash
  replacements at the checkpoint verifier
  ([#423](https://github.com/zakura-core/zakura/pull/423)).

### Changed

- Flush partial cryptographic verification batches at semantic block boundaries,
  avoiding the global 100 ms batch delay during block execution when the
  boundary signal succeeds
  ([#417](https://github.com/zakura-core/zakura/pull/417)).

### Removed

- Removed the obsolete `block-template-to-proposal` and `search-issue-refs`
  binaries and the `search-issue-refs`, `regex`, and `reqwest` Cargo feature
  flags in `zakura-utils` 2.0.0
  ([#406](https://github.com/zakura-core/zakura/pull/406)).
- Removed the experimental embedded Elasticsearch exporter. If you use this
  feature, please contact the Zakura maintainers so we can prioritize a safer,
  external indexing API around your requirements
  ([#409](https://github.com/zakura-core/zakura/pull/409)).

### Fixed

- Re-admit block-sync peers when a no-progress park expires instead of leaving
  the connection without a block-sync session until reconnect, and keep a
  healthy connection owned across the transport's stream-reopen backoff so a
  finished discovery exchange cannot close it mid-gap
  ([#166](https://github.com/zakura-core/zakura/pull/166)).
- Harden the stream-reopen paths against teardown races: legacy-gossip session
  removal is scoped to the exact stream session (and gossip connections now stay
  owned across reopen gaps), block-sync park admission is atomic with the
  routine's park write so an in-flight cooldown is never silently bypassed and a
  dead connection can no longer record one, and header-sync gap claims survive
  the reactor's stale post-disconnect peer snapshot
  ([#166](https://github.com/zakura-core/zakura/pull/166)).
- Bound the stream-reopen loops against unresponsive peers: a block-sync stream
  EOF now parks or disconnects the peer like a liveness stall whenever
  block-progress liveness is still armed — including after per-request timeouts
  already drained the outstanding set — discovery sessions retire after
  repeated silent or broken exchanges instead of reopening forever,
  legacy-gossip reopen churn from zero-frame sessions is capped, and header-sync
  reopen waits target the reactor's real advisory-backoff expiry
  ([#166](https://github.com/zakura-core/zakura/pull/166)).
- Let native Zakura peer sets converge beyond bootstrap nodes by advertising
  reachable addresses, safely dialing valid gossiped discovery records, and
  applying discovery IP safety and connection limits consistently across IPv4
  transition encodings
  ([#373](https://github.com/zakura-core/zakura/pull/373)).
- Scope discovery cleanup and shared-connection ownership to the exact admitted
  peer session to prevent stale teardown and unintended service disconnects
  ([#374](https://github.com/zakura-core/zakura/pull/374)).
- Refresh long-lived discovery sessions before service summaries expire and
  remove service membership derived from expired signed records
  ([#375](https://github.com/zakura-core/zakura/pull/375)).
- Fixed VCT fast-sync handoffs after a restart from writing duplicate note
  commitment trees that prevented the database from reopening
  ([#401](https://github.com/zakura-core/zakura/pull/401)).
- Fixed a checkpoint-sync stall where benign duplicate block resubmits
  (`NewerRequest`) rewound verifier progress behind an already-verified
  checkpoint and left a permanent queue gap
  ([#423](https://github.com/zakura-core/zakura/pull/423)).
- Prevent configured local zcashd-compat sidecars from starving behind a
  saturated public legacy accept backlog by draining queued connections in
  bounded, paced bursts and immediately handshaking accepted sidecars
  ([#426](https://github.com/zakura-core/zakura/pull/426)).
- Prevented Zakura header sync from stalling when a verified full-block tip is
  not yet available as a durable header-range anchor
  ([#427](https://github.com/zakura-core/zakura/pull/427)).
- Fixed VCT checkpoint sync stalling immediately below the final checkpoint
  by automatically recovering a missing terminal header witness at runtime
  ([#432](https://github.com/zakura-core/zakura/pull/432)).
- Prevented Zakura block sync from permanently stalling when a needed height
  left the work queue while a higher height stayed claimed, which pinned the
  block download floor and grew the reorder buffer without bound
  ([#435](https://github.com/zakura-core/zakura/pull/435)).
- Prevented Zakura header sync from livelocking or stalling root authentication
  when durable reanchors discarded in-flight work or left fallback ranges
  disconnected from the authenticated frontier
  ([#438](https://github.com/zakura-core/zakura/pull/438)).
- Avoided initial-sync delays from requesting historical block bodies from
  pruned peers that do not advertise `NODE_NETWORK`
  ([#440](https://github.com/zakura-core/zakura/pull/440)).

### Security

- Authenticate peer-supplied VCT commitment roots against checkpoint-covered
  canonical headers before persisting or serving them, and delete unauthenticated
  roots from existing databases during the state format upgrade
  ([#352](https://github.com/zakura-core/zakura/pull/352)).
- Reject locator hashes echoed inside legacy tip-extension responses
  ([#372](https://github.com/zakura-core/zakura/pull/372)).
- Harden the native discovery candidate dialer against untrusted gossip: charge
  each dialed connection to the network path iroh actually confirms (so a record
  cannot escape the per-IP connection cap by listing a decoy address first),
  scope dial-failure backoff to `(node_id, ip)` instead of IP alone (so a signed
  record cannot back an honest peer's IP off for every candidate that shares it),
  and reject RFC 5737 / RFC 3849 documentation ranges as dial targets
  ([#373](https://github.com/zakura-core/zakura/pull/373)).

## [1.0.3] - 2026-07-22

### Added

- Add an offline Mainnet checkpoint and VCT frontier export mode to
  `zakura-checkpoints`, and a committed provenance record
  (`vct/mainnet-frontier.json`) that CI verifies against the embedded
  checkpoint list and frontier on every PR. Groundwork for automated
  release-state updates
  ([#261](https://github.com/zakura-core/zakura/pull/261)).
- Automate Mainnet checkpoint and VCT frontier refreshes: the
  `update-release-state.yml` workflow imports digest-verified publisher bundles
  from R2 into reviewable draft PRs, and `make pre-release` now verifies the
  committed checkpoint/frontier/provenance coupling (rejecting pre-pipeline
  bootstrap state unless explicitly overridden)
  ([#262](https://github.com/zakura-core/zakura/pull/262)).
- Add `mempool::Request::TakePendingGossipTransactionIds` for bounded,
  atomic draining of transaction IDs awaiting peer advertisement
  ([#64](https://github.com/zakura-core/zakura/pull/64)).

### Changed

- Update the embedded zcashd-compat binary and default split-container image to
  valargroup/zcashd v1.1.0
  ([#319](https://github.com/zakura-core/zakura/pull/319)).
- Hardened the hotfix release checklist and process documentation from the
  first hotfix-path release's findings
  (see [docs/security-hotfix-release.md](docs/security-hotfix-release.md)).

### Removed

- Removed unused public `zakura-chain` errors, constants, and helper methods
  ([#361](https://github.com/zakura-core/zakura/pull/361)).

### Fixed

- Re-admit deferred or ended ordered service streams on existing healthy
  connections. Block sync resumes after no-progress cooldowns when body work
  exists, header sync wakes when advisory backoff expires, retry scheduling is
  bounded, and negotiated streams respect aggregate inbound queue limits
  ([#166](https://github.com/zakura-core/zakura/pull/166)).
- Treat IPv4 and IPv4-mapped IPv6 peer addresses as the same address when
  enforcing bans, inbound rate limits, and per-IP connection limits, preventing
  peers from bypassing them through the alternate representation
  ([#238](https://github.com/zakura-core/zakura/pull/238),
  [#314](https://github.com/zakura-core/zakura/pull/314)).
- Prevent RPC read-secondary synchronization races, stale stream retries, and
  finalized-state gaps from interrupting RPC and indexer availability
  ([#118](https://github.com/zakura-core/zakura/pull/118)).
- Keep valid internal-miner work running across mempool-only block template
  updates ([#226](https://github.com/zakura-core/zakura/pull/226)).
- Honor `disable_pow = true` during native header sync on configured Testnets,
  matching semantic and checkpoint block verification
  ([#289](https://github.com/zakura-core/zakura/pull/289)).
- Make retained peer-ban insertion and eviction O(1) rather than O(N),
  preventing ban-list maintenance from slowing as the 20,000-IP bound fills
  ([#286](https://github.com/zakura-core/zakura/pull/286)).
- Stop the experimental dummy CPU miner from continuing to use a stale block
  template after template generation fails
  ([#333](https://github.com/zakura-core/zakura/pull/333)).
- Replace outdated Zebra branding in Zakura logs, errors, RPC responses, CLI
  help, and operator tooling
  ([#335](https://github.com/zakura-core/zakura/pull/335)).
- Stop advertising dependent transactions after their expired parent is removed
  from the mempool
  ([#342](https://github.com/zakura-core/zakura/pull/342)).
- Fixed test log capture racing the process-wide tracing subscriber, which
  could corrupt span bookkeeping in `zakurad` test binaries; `zakura-test` now
  provides a `log_capture` module that captures messages from its shared
  subscriber ([#332](https://github.com/zakura-core/zakura/pull/332)).
- Kept verbose block-header metadata bound to the resolved block across chain
  reorganizations
  ([#328](https://github.com/zakura-core/zakura/pull/328)).
- Preserve transaction advertisements when the mempool gossip task lags its
  notification channel
  ([#64](https://github.com/zakura-core/zakura/pull/64)).

### Security

- Reject malformed legacy block-discovery responses instead of allowing them
  to disrupt the active sync attempt
  ([#355](https://github.com/zakura-core/zakura/pull/355)).
- Allow chain synchronization to immediately retry an honest block body after
  rejecting a body with the same header hash, without waiting for another state
  request to trigger cleanup
  ([GHSA-8gxx-hc65-vv82](https://github.com/ZcashFoundation/zebra/security/advisories/GHSA-8gxx-hc65-vv82)).

## [1.0.2] - 2026-07-20

### Added

- Add an opt-in `network.expose_peer_addresses` setting for unredacted legacy
  peer address labels in peer activity logs and metrics
  ([#258](https://github.com/zakura-core/zakura/pull/258)).
- Add structured legacy peer request traces that attribute `FindBlocks` hash
  announcements and block download outcomes to privacy-preserving peer IDs,
  including exact-inventory versus speculative routing and the peer's
  self-reported handshake height
  ([#275](https://github.com/zakura-core/zakura/pull/275)).
- Diagnose requests for pruned block bodies with the block height, hash, and
  configured retention, rate limited to once per minute
  ([#279](https://github.com/zakura-core/zakura/pull/279)).
- Add a configurable 250,000-byte default maximum for individual mempool
  transactions. Larger transactions are rejected before semantic and contextual
  verification without penalizing peers, and the policy does not affect block
  validation ([#255](https://github.com/zakura-core/zakura/pull/255)).
- Add `zakurad validate-vct-sprout-history` to audit repaired historical
  Sprout anchors in archive or pruned Mainnet state databases
  ([#247](https://github.com/zakura-core/zakura/pull/247),
  [#251](https://github.com/zakura-core/zakura/pull/251)).

### Changed

- Reuse transaction-wide transparent signature hash components across input
  checks instead of hashing them again for every signature
  ([#281](https://github.com/zakura-core/zakura/pull/281)).
- Reject transactions that do not meet ZIP-317 mempool fee policy before
  running script and proof checks. Block validation is unchanged
  ([#263](https://github.com/zakura-core/zakura/pull/263)).
- Parse the bundled Sapling proving parameters once per process and reuse the
  shared prover, instead of re-parsing the parameters on every
  `getblocktemplate` refresh
  ([#291](https://github.com/zakura-core/zakura/pull/291)).
- Maintain mempool metric totals incrementally instead of rescanning the full
  mempool after every insertion or removal
  ([#268](https://github.com/zakura-core/zakura/pull/268)).
- Point snapshot links and benchmark defaults at the Zakura snapshot service
  ([#276](https://github.com/zakura-core/zakura/pull/276)).
- Source the embedded Mainnet VCT Sprout-history repair artifact from
  exact-versioned crates.io packages instead of storing its large source
  bytes in the Zakura repository, and reuse one validated decode throughout
  startup repair ([#259](https://github.com/zakura-core/zakura/pull/259)).
- Update the embedded zcashd-compat binary and default split-container image to
  valargroup/zcashd v1.0.1
  ([#245](https://github.com/zakura-core/zakura/pull/245)).
- Header sync now schedules only forward ranges from the durable verified block
  tip. Startup rejects configured anchors above that base, and no longer
  backfills headers below a checkpoint anchor
  ([#227](https://github.com/zakura-core/zakura/pull/227)).

### Fixed

- Use the consensus proof-of-work limit for early-chain header validation at
  height 17 ([#220](https://github.com/zakura-core/zakura/pull/220)).
- Advertise `NODE_NETWORK` to a supervised zcashd-compat sidecar even when
  Zakura uses pruned storage
  ([#270](https://github.com/zakura-core/zakura/pull/270)).
- Report invalid mempool scripts as script verification errors
  ([#265](https://github.com/zakura-core/zakura/pull/265)).
- Honor an explicit embedded zcashd-compat source selection even when a stale
  local binary path remains configured
  ([#271](https://github.com/zakura-core/zakura/pull/271)).
- Shut down a managed zcashd-compat process before Zakura exits on SIGINT or
  SIGTERM ([#274](https://github.com/zakura-core/zakura/pull/274)).
- Stop pruned nodes from returning retained chain-index hashes through legacy
  `getblocks` when the corresponding block bodies are no longer serveable
  ([#275](https://github.com/zakura-core/zakura/pull/275)).
- Enable all legacy wallet features by default for supervised zcashd-compat
  processes, while allowing `-allowdeprecated=none` to disable them all
  ([#278](https://github.com/zakura-core/zakura/pull/278)).
- Avoid penalizing peers that relay NU6.2 branch-ID transactions during the
  first 40 heights after NU6.3 activation, while keeping consensus validation
  strict ([#273](https://github.com/zakura-core/zakura/pull/273)).
- Preserve failed shielded proof and signature verification errors so invalid
  transactions receive the existing mempool peer misbehavior score
  ([#283](https://github.com/zakura-core/zakura/pull/283)).
- Ban peers that send mempool transactions with invalid Orchard or Ironwood
  proof sizes ([#285](https://github.com/zakura-core/zakura/pull/285)).
- Database format upgrades now finish before startup exposes the finalized
  state database; only configured periodic format checks continue in the
  background ([#240](https://github.com/zakura-core/zakura/pull/240)).
- Preserve Sprout note-commitment history during fresh verified-commitment-tree
  fast sync, so later JoinSplit spends can use historical anchors. Affected
  Mainnet databases that previously ran v2 p2p + fast mode require repair at
  startup from a reviewed trusted artifact, snapshot redownload, or genesis
  resync ([#239](https://github.com/zakura-core/zakura/pull/239),
  [#244](https://github.com/zakura-core/zakura/pull/244),
  [#259](https://github.com/zakura-core/zakura/pull/259)).
- Deliver mined/submitted block gossip to peers that were momentarily unready
  when the block was advertised. A block broadcast via `AdvertiseBlockToAll`
  queued a re-send for unready peers, but the queued send future was dropped
  before the connection wrote the `inv`, so the connection treated the request
  as canceled and silently skipped it. Because a zcashd-compat sidecar follows a
  single upstream and learns the tip only from block `inv`s, it could then stall.
  The queued send now runs to completion. Only local mining paths (regtest, e2e,
  and local-mining deployments) exercise `AdvertiseBlockToAll`; standard
  following nodes advertise network blocks via `AdvertiseBlock` and are
  unaffected ([#236](https://github.com/zakura-core/zakura/pull/236)).
- Deliver committed-tip block gossip to configured zcashd-compat sidecar peers
  even when they are momentarily unready. The "always include sidecars" carve-out
  in block broadcasts only covered ready peers, so a sidecar that was unready when
  a block was gossiped was skipped; because it follows a single upstream and
  learns the tip only from block `inv`s, it then stalled until a later gossip
  coincided with a ready service. The latest hash is now queued for an unready
  sidecar and delivered once it is ready again, bounding the stall to one
  readiness cycle ([#231](https://github.com/zakura-core/zakura/pull/231)).
- The inbound-overload protection no longer disconnects operator-configured
  block-gossip / zcashd-compat sidecar peers. When such a peer's own getdata /
  getheaders overloaded or timed out the inbound service, the random
  connection-drop (probability 0.05→0.5) could sever the very peer this node
  feeds, and the one-connection-per-IP reconnect refusal could stretch that into
  a multi-second blackout. Configured sidecars are now exempt from the drop —
  their requests are still shed for backpressure, but the connection is not
  closed. Every other peer's denial-of-service protection is unchanged
  ([#242](https://github.com/zakura-core/zakura/pull/242)).

### Security

- Prevent a peer from stalling chain synchronization by delivering a rejected
  block body that shares its header hash with a later valid block
  ([GHSA-8gxx-hc65-vv82](https://github.com/ZcashFoundation/zebra/security/advisories/GHSA-8gxx-hc65-vv82)).

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
