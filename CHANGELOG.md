# CHANGELOG

All notable changes to Zakura are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
As a binary distribution, the `zakura` package uses
[Semantic Versioning](https://semver.org) as a baseline, with documented
release-policy exceptions for backwards-compatible additions. These exceptions
do not apply to published library crates, whose API versions are evaluated
independently.

## [Unreleased]

## [1.3.0-rc4] - 2026-08-27

### Changed

- Zakura now routes transaction-verifier and mempool state reads around the
  serialized read-write state buffer
  ([#784](https://github.com/zakura-core/zakura/pull/784)).

### Fixed

- Allowed Windows tools to check out and package `zakura-header-chain`
  ([#829](https://github.com/zakura-core/zakura/pull/829)).
- Fixed native sync stalls caused by header retention evicting a full-state fork. The node now
  completes affected descendant operations and exits if legacy fallback waits 30 minutes for
  native applies to drain ([#831](https://github.com/zakura-core/zakura/pull/831)).

## [1.3.0-rc3] - 2026-08-27

### Added

- Added the `getchaintips` RPC method to `zakurad`
  ([#796](https://github.com/zakura-core/zakura/pull/796)). Operators no longer
  have to send the call to the zcashd-compat sidecar, where it scans the whole
  block index under `cs_main` and stalls every other RPC for seconds. Zakura
  reads only the chains it holds in memory, so the cost is bounded by the number
  of tracked forks rather than by the height of the chain.

  The two nodes report different tips. zcashd never prunes its block index, so it
  lists every stale tip it has ever seen. Zakura lists the tips that are still
  live: the best chain, the non-finalized forks, recently invalidated branches,
  and the selected header chain when some block bodies are unavailable. Zakura
  does not return zcashd's `valid-headers` or `unknown` statuses. `branchlen` for
  an `invalid` tip can be short, because Zakura tracks a limited number of forks
  and can drop the chain that the branch forked from.

### Changed

- Reduced non-finalized chain snapshot latency by sharing immutable contextual
  UTXO maps ([#780](https://github.com/zakura-core/zakura/pull/780)).
- A commitment-root repair that no connected peer can supply now reports why each peer was
  excluded and escalates after 60 seconds
  ([#821](https://github.com/zakura-core/zakura/pull/821)).
- Updated the `zakura-core/libraries` crates to `1.0.0-rc.4`
  ([#824](https://github.com/zakura-core/zakura/pull/824)).
- Sped up Orchard note commitment tree updates during sync by about 2x by
  evaluating `MerkleCRH^Orchard` with the libraries' new weighted fixed-length
  Sinsemilla evaluator, for a one-time 3.75 MiB in-memory table
  ([#824](https://github.com/zakura-core/zakura/pull/824)).
- Shortened this release's end-of-support window from 40 to 33 days, so
  Mainnet `zakurad` nodes running it halt at block 3,500,552, estimated
  2026-09-30 — the day before October 1st. End-of-support warnings begin
  3 days earlier ([#825](https://github.com/zakura-core/zakura/pull/825)).

### Fixed

- Fixed block sync peer routines panicking when concurrent retry state changes
  emptied a selected work range. The routine now evaluates retry state once per
  item, keeps a nonempty contiguous range, and carries the checked retry
  deadline into its next wait
  ([#818](https://github.com/zakura-core/zakura/pull/818)).
- Fixed the legacy block syncer restarting an entire sync round after one transient peer or
  transport failure. The syncer now retries the affected block hash with a bounded budget and
  preserves the other download and checkpoint verification tasks. The syncer limits each transient
  hash to eight peer requests per sync round. A replacement download also stops waiting and
  restarts the round if the network stays unready past the download timeout
  ([#819](https://github.com/zakura-core/zakura/pull/819)).
- Fixed a sync halt when a checkpoint commit needed verified commitment-tree metadata from peers
  that had advanced past the stalled node. Repair now accepts any peer that can reach the exact
  height. Serving resolves finalized targets through a reserved one-header path in finalized
  indexes
  ([#821](https://github.com/zakura-core/zakura/pull/821)).
- Fixed commitment-root repair retries that could restart on every missing-root poll, discard
  scheduler state, penalize the wrong supplier, duplicate local work, or miss connected suppliers.
  Repair now preserves exact ownership and attribution while it rotates through bounded supplier
  cycles
  ([#821](https://github.com/zakura-core/zakura/pull/821)).
- Fixed authentication-sweep repair masking a blocked checkpoint committer repair. The committer
  now takes priority until its checkpoint commits, then the sweep resumes
  ([#821](https://github.com/zakura-core/zakura/pull/821)).
- Fixed authentication-sweep repair stopping after it rejected a replacement at the same height.
  Rejection now starts a new repair generation, while repeated missing-root observations remain in
  the current generation
  ([#821](https://github.com/zakura-core/zakura/pull/821)).
- Fixed checkpoint-handoff repair stopping after it rejected roots without a successor witness.
  Each rejected delivery now starts one deduplicated replacement generation
  ([#821](https://github.com/zakura-core/zakura/pull/821)).
- Fixed an indefinite node stall when local preparation or application of commitment-root repair
  remained pending. The node now exits with an error after 30 minutes so its supervisor can restart
  it
  ([#821](https://github.com/zakura-core/zakura/pull/821)).
- Fixed dual-stack nodes waiting forever to start legacy fallback when native block sync holds an
  incomplete checkpoint range. Legacy fallback can now supply the missing bodies and resume block
  verification
  ([#823](https://github.com/zakura-core/zakura/pull/823)).

## [1.3.0-rc2] - 2026-08-24

### Added

- Mainnet now ships a reviewed historical frontier grid, so a verified-commitment-trees
  fast-synced archive node serves `z_gettreestate` and the `getblock`/`getblockheader` tree
  sizes across its absent band with no deployment-time configuration. Serving previously
  required pointing `state.historical_frontier_artifact` at a grid; that setting remains, now
  as an override for tests and custom networks. The grid carries no trust weight — every entry
  is still checked against the authenticated root this node already stores before it anchors
  anything — but the bytes a build embeds are bound to the committed provenance manifest, so a
  grid that has fallen out of step with the embedded checkpoint list fails CI rather than
  leaving the band silently unavailable
  ([#703](https://github.com/zakura-core/zakura/pull/703),
  [#739](https://github.com/zakura-core/zakura/pull/739)).
- Added opt-in historical treestate serving for verified-commitment-tree fast-synced archive nodes.
  When `state.historical_frontier_artifact` names a sparse frontier grid, the node replays retained
  block bodies from the nearest root-verified anchor and serves the result only after reproducing
  its authenticated root. Missing or unusable grids and pruned nodes continue to report the typed
  historical-tree-unavailable error
  ([#775](https://github.com/zakura-core/zakura/pull/775)).

### Changed

- The Mainnet release-state bundle now carries the historical frontier grid alongside the
  checkpoint list, VCT frontier, and completed-subtree roots, and the four are one required
  set, so a replacement checkpoint list can never ship without its coupled release state.
  Bundle fetching requires the grid and verifies its digest, the importer requires each update
  to extend the previously published grid rather than rewrite it, and the provenance manifest
  records its digest, size, and entry count
  ([#703](https://github.com/zakura-core/zakura/pull/703),
  [#735](https://github.com/zakura-core/zakura/pull/735)).

- The grid is distributed as an exact-pinned crates.io package built from
  `crates/zakura-assets/` rather than committed to this repository. At ~2.1 MB regenerated on
  every weekly refresh, committing it would add that much to git history each time; the payload
  is generated at publish time and never enters git. The version is
  `0.<last_checkpoint>.<revision>`, so the pin states which checkpoint the payload covers, and
  the release-state workflow publishes the grid before opening the pull request that pins it,
  because cargo can only resolve a version that already exists.
  `scripts/check-release-state.sh` holds the pin against the provenance manifest without cargo
  and now runs on every pull request, while `embedded_mainnet_final_frontiers_parse` hashes the
  bytes the dependency supplies and holds them against the manifest's recorded digest
  ([#703](https://github.com/zakura-core/zakura/pull/703),
  [#763](https://github.com/zakura-core/zakura/pull/763)).
- Reduced contextual block verification latency by removing redundant UTXO
  copies ([#779](https://github.com/zakura-core/zakura/pull/779)).
- The Mainnet release-state asset package is now published as `zakura-assets` rather than
  `valargroup-zakura-assets`. The prefixed name was a stand-in taken while `zakura-assets` was
  reserved but unpublishable, and the workspace pin carried a `package` alias to translate
  between the dependency name and the published one. Both now agree, so the pin, the packer, and
  the release-state coupling gate all refer to `zakura-assets` and nothing else. Consumers that
  build `zakura-state` from crates.io resolve the renamed package; the payload, the
  `0.<last_checkpoint>.<revision>` version scheme, and the digest the provenance manifest binds it
  to are unchanged
  ([#793](https://github.com/zakura-core/zakura/pull/793)).

## [1.3.0-rc1] - 2026-08-21

### Fixed

- Fixed a sync halt on nodes running the Zakura header chain. Checkpoint finality
  evidence is bound to the state version the state writer read, but the combined
  auxiliary-then-checkpoint path installed the auxiliary transition before it
  planned finality. The auxiliary transition advanced the version and caused the
  planner to persist mismatched checkpoint provenance. The combined path now
  validates and records provenance against the pre-auxiliary snapshot. The
  combined path returns `Stale` before staging the auxiliary transition when the
  checkpoint request names an old state version
  ([#746](https://github.com/zakura-core/zakura/pull/746)).
- Fixed checkpoint recovery after a hard state commit error. Sibling checkpoint
  commits could queue late reset requests that rewound the recovered verifier and
  reopened a permanent block gap. The verifier now coalesces resets by commit
  generation and ignores late resets from an earlier generation
  ([#746](https://github.com/zakura-core/zakura/pull/746)).
- Fixed header-chain startup when a release migrates an older disk format and
  extends the checkpoint list in the same binary, so nodes can upgrade without
  a resync
  ([#765](https://github.com/zakura-core/zakura/pull/765)).

## [1.3.0-rc0] - 2026-08-20

### Added

- Added fork-aware, bounded header and block synchronization with crash-atomic
  header-chain persistence, contextual validation, resumable startup
  reconstruction, and header-time authentication of peer-supplied
  verified-commitment-tree metadata
  ([#586](https://github.com/zakura-core/zakura/pull/586)).
- Embedded published completed subtree roots alongside the Mainnet last checkpoint, letting a
  verified-commitment-trees fast-synced node serve `z_getsubtreesbyindex` through that last
  checkpoint. Published roots never displace the node's own rows. A node that fast-synced at an
  older checkpoint still uses a newer artifact, but only for history below its original handoff
  ([#593](https://github.com/zakura-core/zakura/pull/593)).
- Published subtree roots are proven against the note commitment frontier that pins them, so an
  artifact with wrong or missing roots is rejected rather than served. The check runs at
  generation, on the embedded artifact in CI, and on a candidate bundle before the release-state
  update workflow imports it ([#593](https://github.com/zakura-core/zakura/pull/593)).
- Mainnet checkpoint export now extends the completed subtree roots embedded in the binary using
  retained database rows, allowing a pruned VCT node to publish the next coupled checkpoint,
  frontier, and subtree artifacts. Added `verify-historical-treestates` to prove an artifact
  against a frontier without a state database
  ([#593](https://github.com/zakura-core/zakura/pull/593)).
- The release-state update workflow now imports the subtree-root artifact alongside the checkpoint
  list and frontier, enforcing that published roots are only ever appended to
  ([#593](https://github.com/zakura-core/zakura/pull/593)).
- Applications can now embed `zakurad` to register custom Zakura p2p services and advertise them through discovery
  ([#594](https://github.com/zakura-core/zakura/pull/594)).
- `zakura_consensus::clear_shielded_verification_caches` forgets every cached
  shielded bundle verification. It is hidden from the documentation and exists
  for benchmarks, which must start each iteration with cold caches to measure
  verification rather than cache hits. Clearing a cache only costs a
  re-verification ([#600](https://github.com/zakura-core/zakura/pull/600)).
- Added peer software identifiers (`subver`) and advertised protocol versions
  to the `getpeerinfo` RPC response ([#730](https://github.com/zakura-core/zakura/pull/730)).

### Changed

- Changed native/legacy sync handoff, header capability readiness, and ordered
  service demand to use one explicit lifecycle coordinator, preventing legacy
  fallback from applying blocks while an accepted native apply is still live
  ([#586](https://github.com/zakura-core/zakura/pull/586)).
- Sapling bundle verification is now cached, so a transaction's Sapling proofs
  and signatures are not verified a second time when the block that mines it
  arrives. This extends the Orchard and Ironwood cache added in #597 to the
  remaining shielded pool, under the same transaction-ID key
  ([#600](https://github.com/zakura-core/zakura/pull/600)).
- The shielded verification cache metrics moved from
  `zakura.consensus.halo2.cache.{hit,miss,insert,evict,size}` to
  `zakura.consensus.cache.{hit,miss,insert,evict,size}`, each carrying a
  `verifier` label whose values are `halo2_pre_nu6_2`, `halo2_nu6_2`,
  `halo2_nu6_3_onward` and `groth16_sapling`. Each Orchard circuit era now
  reports its own hit rate instead of the three sharing one series. The
  explicit-flush log for the Sapling batch also reports `groth16_sapling`
  instead of `sapling`, matching that verifier's other metrics
  ([#600](https://github.com/zakura-core/zakura/pull/600)).
- Zakura derives auxiliary outcomes from exact state observations. It removes the caller-selected
  verdict API. It validates untrusted durable outcomes before recovery promotes them. It requests
  replacement auxiliary data when a retained successor lacks a usable witness. Version 2 changes
  the auxiliary outcome encoding. Zakura atomically migrates version-1 header-chain databases at
  startup without requiring a resync
  ([#667](https://github.com/zakura-core/zakura/pull/667)).
- Zakura bounds every header-chain startup collection before RocksDB decodes durable rows
  ([#667](https://github.com/zakura-core/zakura/pull/667)).
- Block and checkpoint verification now reject a block whose header carries an
  invalid version or an unrepresentable timestamp before computing its hash.
  Canonical deserialization already enforced the version rule, so this closes
  the gap for in-memory headers that never went through the parser, such as
  block proposals and locally constructed blocks
  ([#674](https://github.com/zakura-core/zakura/pull/674)).
- The provisional header-chain durable format moves from version 2 to version 3 to store the
  complete network policy. Startup still migrates a released Mainnet version-1 database. Startup
  rejects version-1 Testnet and Regtest databases because that format cannot authenticate their
  configurable policy. Version 2 is unreleased and has no migration path, so a header-chain
  database on that version must be deleted and resynchronized. The startup error names the
  required version ([#692](https://github.com/zakura-core/zakura/pull/692)).
- The public header-chain transition API now includes shared `FinalityWitnessProof` values,
  structured full-state provenance, and authenticated `DiskMigration` finality sources. This
  change updates the provisional database schema and Rust types. It does not change the wire
  protocol or user configuration
  ([#707](https://github.com/zakura-core/zakura/pull/707)).
- Changed the published dependency graph to use the coordinated Zakura
  cryptography forks, so crate consumers resolve the Zakura implementations
  instead of their upstream equivalents
  ([#749](https://github.com/zakura-core/zakura/pull/749)).

### Removed

- Removed the height-keyed header-root authentication lane and its durable
  frontier; the committer now verifies supplied roots before persistence
  ([#586](https://github.com/zakura-core/zakura/pull/586)).
- Removed the unsupported `zakurad copy-state` debugging command and its
  custom environment-prefix configuration loader
  ([#716](https://github.com/zakura-core/zakura/pull/716)).
- Removed the deprecated Zebra-era Rust type aliases in `zakura-rpc` 8.0.0
  ([#723](https://github.com/zakura-core/zakura/pull/723)). Crate consumers
  should replace them as follows:

  - `GetInfo` with `GetInfoResponse`;
  - `AddressStrings` with `GetAddressBalanceRequest`;
  - `AddressBalance` with `GetAddressBalanceResponse`;
  - `SentTransactionHash` with `SendRawTransactionResponse`;
  - `GetBlock` with `GetBlockResponse`;
  - `GetBlockHeader` with `GetBlockHeaderResponse`;
  - `GetBlockHeaderObject` with `BlockHeaderObject`;
  - `GetBlockHash` with `GetBlockHashResponse`;
  - `GetBestBlockHeightAndHash` with `GetBlockHeightAndHashResponse`;
  - `GetRawTransaction` with `GetRawTransactionResponse`;
  - `GetAddressUtxos` with `Utxo`; and
  - `BlockSubsidy` with `GetBlockSubsidyResponse`.

  RPC endpoints and wire formats are unchanged.
- Removed five deprecated Rust compatibility methods from `zakura-rpc`
  ([#726](https://github.com/zakura-core/zakura/pull/726)). Crate consumers
  should replace them as follows:

  - `GetInfoResponse::from_parts` with `GetInfoResponse::new`;
  - `GetAddressBalanceRequest::new_valid` with
    `GetAddressBalanceRequest::new` and rely on server-side validation;
  - `SendRawTransactionResponse::inner` with
    `SendRawTransactionResponse::hash`;
  - `Utxo::from_parts` with `Utxo::new`; and
  - `GetAddressTxIdsRequest::from_parts(addresses, start, end)` with
    `GetAddressTxIdsRequest::new(addresses, Some(start), Some(end))`.

  The deprecated treestate compatibility methods remain available. RPC
  endpoints and wire formats are unchanged.

### Fixed

- Fixed header-sync work and memory retention across body commits, finality
  advances, peer terminal paths, process restarts, and committed resource-limit
  refusals
  ([#586](https://github.com/zakura-core/zakura/pull/586)).
- Improved native scratch-sync commit throughput by caching immutable trust
  data, using bounded exact-key lookups, parallelizing independent header
  checks, and refilling the bounded header window before it is exhausted
  ([#586](https://github.com/zakura-core/zakura/pull/586)).
- Fixed `zakurad` silently accepting an expired or not-yet-valid RPC TLS
  certificate at startup. It now logs a warning naming the certificate file and
  the date the certificate fails on before opening the listener, so operators
  can diagnose rejected client handshakes
  ([#627](https://github.com/zakura-core/zakura/pull/627)).
- Refused header-chain transitions before commit when protected retained paths
  exceed configured bounds. Retention uses bounded candidate passes only under
  pressure, recovers stalled headers-only state, and reports exact structural
  work budgets
  ([#665](https://github.com/zakura-core/zakura/pull/665)).
- Improved checkpoint-sync performance by applying header graph updates incrementally
  ([#679](https://github.com/zakura-core/zakura/pull/679)).
- Fixed fork-aware header-chain upgrades so nodes atomically discard obsolete
  header-overlay indexes instead of requiring a state resync
  ([#688](https://github.com/zakura-core/zakura/pull/688)).
- Fixed initial header DAG migration startup so it does not rescan every
  historical finalized header
  ([#689](https://github.com/zakura-core/zakura/pull/689)).
- Stopped labeling network Prometheus counters by peer address, so `/metrics`
  stays bounded on long-lived public listeners
  ([#697](https://github.com/zakura-core/zakura/pull/697)).
- Fixed block sync so selected-header extensions preserve in-flight body work
  instead of downloading the bounded checkpoint window again
  ([#700](https://github.com/zakura-core/zakura/pull/700)).
- Accepted header batches that finalize past headers the same transition
  inserted. A batch longer than the local finality depth advanced the finalized
  frontier above a header it had just inserted, and the graph transition rejected
  it, so header sync stalled on every retry of that batch
  ([#706](https://github.com/zakura-core/zakura/pull/706)).
- Stopped leaking one header child-index entry on every finality advance. The
  advance deletes the new finalized frontier's parent, and the transition then
  restored a child edge under that deleted parent
  ([#706](https://github.com/zakura-core/zakura/pull/706)).
- Fixed startup of version-one header-chain databases so the supported migration runs without
  requiring a resync
  ([#710](https://github.com/zakura-core/zakura/pull/710)).
- Fixed startup of version-two header-chain databases so they migrate to the current
  format without a resync
  ([#715](https://github.com/zakura-core/zakura/pull/715)).
- Fixed a panic in the batch verification services. `Batch::poll_ready` polled the shared
  worker task handle after that task had finished, which Tokio panics on, and propagated a
  worker panic while holding the handle's mutex, which poisoned it. Once a batch worker
  exited, the next readiness check on any clone of that verifier panicked instead of
  returning the worker's error. The finished handle is now taken out of the shared slot, and
  the lock is released before the panic is propagated, so every later caller receives the
  worker error ([#728](https://github.com/zakura-core/zakura/pull/728)).
- Rejected configured network parameters that made block validation panic at a later height:
  funding stream numerators that overflow, a receiver configured twice in one funding stream,
  funding stream addresses that are not P2SH, lockbox disbursement addresses that do not parse
  or are not P2SH, lockbox disbursement amounts that do not sum to a valid amount, and slow
  start intervals that make the founders reward inexact
  ([#729](https://github.com/zakura-core/zakura/pull/729)).
- Clamped the batch verifier's concurrent batch limit to at least one, so the batch worker can
  no longer be built in a state where it panics on its first poll and fails every request, and
  saturated request weight accumulation so an overflowing weight can no longer strand its
  callers' responses
  ([#729](https://github.com/zakura-core/zakura/pull/729)).
- Fixed a sync throughput collapse on nodes running the Zakura header chain.
  Trimming the bounded finality history located the record to evict by walking
  from the start of its column family, and because each eviction deletes the
  lowest key there, that walk stepped over one more tombstone per eviction ever
  performed. The window is too small to trigger the compaction that would collect
  them, so per-block commit time grew without bound once the ring filled: a
  dual-stack Mainnet sync went from 254 to 8.6 blocks per second between height
  9.6k and 928k and could no longer reach the tip. Eviction now seeks past the
  published checkpoint epoch, which is O(1) and independent of how many evictions
  preceded it. An existing state directory gets the fix on its next eviction
  without a migration
  ([#731](https://github.com/zakura-core/zakura/pull/731)).
- Fixed startup of version-three header-chain databases so they migrate to the current
  format without a resync, including nodes whose work origin is below the migrated
  finalized frontier
  ([#736](https://github.com/zakura-core/zakura/pull/736)).
- Backed off repeated bootstrap dials when a peer closes short-lived Zakura
  connections ([#741](https://github.com/zakura-core/zakura/pull/741)).

### Security

- Updated dependencies to address published security advisories and expanded
  supply-chain review coverage
  ([#619](https://github.com/zakura-core/zakura/pull/619)).
- Disabling proof of work now requires an authenticated custom network
  configuration. A configuration claiming `disable_pow` for Mainnet or the
  default public Testnet is refused with an error instead of silently waiving
  Equihash verification. The waiver path still validates solution shape, so a
  short Regtest-shaped solution cannot be accepted on a larger network
  ([#674](https://github.com/zakura-core/zakura/pull/674)).
- Fixed nine header-chain authority, recovery, finality, and replay flaws found by the V12
  security review. Recovery now binds durable state to the complete network policy and validates
  independent authority before it accepts full-state paths and finality witnesses
  ([#692](https://github.com/zakura-core/zakura/pull/692)).
- Header-chain disk format 4 now migrates formats 1–3 in one atomic batch. Formats 1 and 2 require
  the fixed Mainnet policy. Format 3 requires the exact configured policy digest. Migration
  authenticates the active frontier, replaces unverifiable history with one authenticated
  migration record, and preserves monotonic counters. Migration deletes orphaned version-1 body
  authority and tombstone rows
  ([#707](https://github.com/zakura-core/zakura/pull/707)).
- Header-chain finality now stores headers-only proofs in a bounded, content-addressed witness DAG.
  Each node uses its exact height and hash. Shared immutable nodes and reference counts preserve
  historical branches without rewriting the complete 1,000-header proof on each advance
  ([#707](https://github.com/zakura-core/zakura/pull/707)).
- Header-chain finality now retains the original full-state transition ID, event kind, and source
  state version. Recovery checks that provenance against the independently canonical full-state
  path. A replaced historical transition ID now fails startup
  ([#707](https://github.com/zakura-core/zakura/pull/707)).
- Header-chain recovery treats durable auxiliary outcomes as unauthenticated provenance. The VCT
  window waits for the current process to authenticate an outcome before selection or peer-failure
  attribution ([#707](https://github.com/zakura-core/zakura/pull/707)).
- Bounded the allocation for a peer-declared byte vector length, so a short message that
  declares a near-maximal length no longer forces a multi-megabyte allocation before the
  message is rejected
  ([#729](https://github.com/zakura-core/zakura/pull/729)).

## [1.2.0] - 2026-08-14

### Added

- Added the `GetBlockRange` method to the indexer gRPC service, streaming a
  height range of finalized blocks in ascending order. Blocks are served from
  the stored raw bytes instead of the per-block deserialize/re-serialize round
  trip `GetBlock` performs, so indexers can backfill finalized history without
  a request round trip per block. Serving block ranges requires building with
  the `indexer` compile-time feature; without it the method returns
  `UNIMPLEMENTED` ([#612](https://github.com/zakura-core/zakura/pull/612)).

### Changed

- Orchard Halo2 proofs verified from the mempool are no longer verified a second
  time when the block that mines them arrives. The result is cached per Orchard
  circuit era under a key that hashes the bundle's consensus encoding, its bundle
  version and the sighash, so a reused result is bit-identical to the computation
  it replaces; every other consensus check still runs at the block's own height.
  New `zakura.consensus.halo2.cache.{hit,miss,insert,evict}` counters and a
  `zakura.consensus.halo2.cache.size` gauge report the cache's behaviour
  ([#597](https://github.com/zakura-core/zakura/pull/597)).
- The generated `Indexer` gRPC service trait in the `zakura-rpc` crate has a
  new required `get_block_range` method and `GetBlockRangeStream` associated
  type, a breaking change for external implementers of the trait; `zakura-rpc`
  takes its major version bump to 7.0.0 in this PR
  ([#612](https://github.com/zakura-core/zakura/pull/612)).
- Block work and cumulative chain work are now stored at their exact 256-bit
  width, so valid hard targets are representable instead of failing conversion.
  Fork choice and the committed chain history root are unchanged for Mainnet and
  Testnet, and no database migration is required
  ([#637](https://github.com/zakura-core/zakura/pull/637)).
- The maximum-block-time activation height is now a per-network parameter.
  Configured networks and Regtest default to activating the MTP-plus-90-minutes
  rule at height 2 instead of inheriting the public Testnet height, generated
  local networks activate it after their seed chain, and Regtest accepts a
  `max_block_time_start_height` config field. Mainnet and public Testnet
  behaviour is unchanged
  ([#640](https://github.com/zakura-core/zakura/pull/640)).

### Removed

- Removed `Work::as_u128` and `PartialCumulativeWork::as_u128` from
  `zakura-chain` in favour of `as_u256`
  ([#637](https://github.com/zakura-core/zakura/pull/637)).
- Removed internal-miner stale-template cancellation until its CPU Equihash
  solver dependency is ready for release
  ([#657](https://github.com/zakura-core/zakura/pull/657)).

### Fixed

- Zakura protocol services now enforce their declared frame caps for inbound and
  outbound application frames
  ([#592](https://github.com/zakura-core/zakura/pull/592)).
- Stopped the internal miner from submitting solutions for stale templates and
  reduced stale work by cancelling between Equihash digit rounds
  ([#643](https://github.com/zakura-core/zakura/pull/643)).

### Security

- Required mutual TLS for indexer RPC listeners on non-loopback addresses,
  bounded indexer connections and streams, and rejected blocks without a
  coinbase height before state preparation
  ([#596](https://github.com/zakura-core/zakura/pull/596)).

## [1.1.1] - 2026-08-10

### Added

<!-- release-readiness: allow-patch; reason:
The added APIs and audit command are backwards compatible, and the ZIP 317
cap increase is a compatible policy adjustment.
-->

- Added the `audit-historical-treestates` subcommand, which reports whether a state
  database can rebuild the note commitment trees a verified-commitment-trees fast-synced
  node is missing, and can walk that range to rebuild and check every tree against the
  authenticated roots the node already stores
  ([#552](https://github.com/zakura-core/zakura/pull/552)).
- Added the public `NoteCommitmentTree::from_frontier` constructor to the Sapling
  and Orchard note commitment trees (Ironwood inherits it via the `ironwood::tree`
  re-export), building a tree from an already-validated frontier without
  re-appending its leaves. `parallel::batch_frontier` already returns bare
  frontiers publicly; this adds the way back in
  ([#602](https://github.com/zakura-core/zakura/pull/602)).

### Changed

- Raised Zakura's ZIP 317 block-production weight ratio cap from 4 to 10
  ([#595](https://github.com/zakura-core/zakura/pull/595)).

### Fixed

- `zakurad` no longer bans a peer for serving a block whose height is above the
  sync lookahead limit. The ban used the address of the peer that answered the
  block request, but a far-ahead height comes from whichever peer supplied that
  hash in an earlier `FindBlocks` response — usually a different peer — so an
  honest peer could be banned for correctly answering a request this node made.
  Such blocks are still dropped without restarting sync, and peers that serve
  blocks with no valid height or that fail consensus verification are still
  scored
  ([GHSA-qhr3-cvch-5fh2](https://github.com/ZcashFoundation/zebra/security/advisories/GHSA-qhr3-cvch-5fh2)).
- Fixed read-only state opens failing against databases written by an older version, which
  made read-only tooling unable to inspect them at all. Opening read-only no longer
  requires column families the database does not have
  ([#552](https://github.com/zakura-core/zakura/pull/552)).
- Chain synchronization now downloads a peer's only unknown block hash from a short
  `FindBlocks` response, allowing nodes near the chain tip to continue advancing
  ([#576](https://github.com/zakura-core/zakura/pull/576)).

### Security

- Score and ban peers that gossip invalid blocks. `zakurad` verifies gossiped
  blocks with the block verifier router, but inbound cleanup only recognized
  errors from the semantic verifier, so every rejection from the production
  path left the advertising peer unscored and it could keep supplying invalid
  blocks over the same connection
  ([GHSA-8hh2-hrf2-cqf4](https://github.com/ZcashFoundation/zebra/security/advisories/GHSA-8hh2-hrf2-cqf4)).
- Reject a downloaded block that builds on `zakurad`'s own chain tip but claims
  a coinbase height other than one above the tip. A V5+ coinbase `scriptSig` is
  authorizing data, so it is excluded from the block hash. That makes the height
  malleable: a peer can rewrite it in an otherwise canonical body and still
  answer the request for that hash. Such a poisoned body was previously
  discarded as a benign old block — unattributed, unscored, and not requeued —
  delaying the newest block until the next discovery round and leaving a mining
  backend issuing work on an obsolete tip. The check now runs on both block
  download paths, the syncer and block gossip. On each, the supplying peer is
  scored for a ban and the block hash is re-requested straight away under its own
  bounded retry budget, rather than waiting for the next discovery round
  ([GHSA-g95h-hw6g-pvgv](https://github.com/ZcashFoundation/zebra/security/advisories/GHSA-g95h-hw6g-pvgv)).

## [1.1.0] - 2026-08-05

### Added

- Added the `pruneheight` field to `getblockchaininfo`, reporting the lowest
  height at and above which every block body is retained. As in `zcashd`, it is
  only present when `pruned` is true. Zakura prunes raw transaction data rather
  than whole blocks, so blocks below this height keep their headers, transaction
  IDs, and consensus state, and the genesis block is never pruned
  ([#470](https://github.com/zakura-core/zakura/pull/470)).
- Added the `getdeprecationinfo` RPC, which reports Zakura's Mainnet
  end-of-support height and an estimated halt time with a 24-hour safety
  margin. When the chain tip is unavailable, the estimate starts from the
  latest network checkpoint
  ([#494](https://github.com/zakura-core/zakura/pull/494)).

### Changed

- Changed the read-request metric label `is_pruned` to `pruning_info`, because
  the request now reports the prune height alongside the pruned flag. Dashboards
  and alerts keyed on the old label need updating
  ([#470](https://github.com/zakura-core/zakura/pull/470)).
- Changed the public `GetBlockchainInfoResponse::new()` in `zakura-rpc` to take
  a `prune_height` argument, so that downstream callers can set the new field.
  This breaks existing callers of that constructor
  ([#470](https://github.com/zakura-core/zakura/pull/470)).
- Increased the default inbound connection limit from 100 to 300 connections,
  and reduced the default outbound connection limit from 300 to 150
  connections, so peers that cannot accept inbound connections can still reach
  `zakurad` nodes
  ([#478](https://github.com/zakura-core/zakura/pull/478)).
- Increased the peer address disk cache from 75 to 300 addresses, so restarted
  nodes have more dialable peer candidates
  ([#478](https://github.com/zakura-core/zakura/pull/478)).
- Updated `rocksdb` to 0.24 (RocksDB 10.4.2), porting
  [ZcashFoundation/zebra#10922](https://github.com/ZcashFoundation/zebra/pull/10922).
  Zakura now builds with GCC 15/16 without the `CXXFLAGS="-include cstdint"`
  workaround. The bundled `librocksdb-sys` now always runs `bindgen` to
  generate its FFI bindings, so **`libclang` is required at build time** (in
  addition to `protoc` and a C++ compiler) even when linking a system RocksDB
  via `ROCKSDB_LIB_DIR`. Install `libclang-dev` (Debian/Ubuntu), `clang`
  (Arch), or the equivalent for your platform
  ([#480](https://github.com/zakura-core/zakura/pull/480)).
- `zakurad-log-filter` no longer requires GNU sed, and no longer reads the
  `GNU_SED` environment variable. Backslashes in log lines are now printed
  as-is ([#481](https://github.com/zakura-core/zakura/pull/481)).
- The first peer disk-cache write is now retried every 20 seconds until it
  succeeds, instead of waiting the full 5-minute update interval, so a
  cold-started node caches its peers soon after finding them
  ([#484](https://github.com/zakura-core/zakura/pull/484)).
- Raised `zakura-rpc` to 6.0.0 for its breaking public API changes, including
  the new required `Rpc::get_deprecation_info` trait method and the
  `GetBlockchainInfoResponse::new` signature change from #470
  ([#494](https://github.com/zakura-core/zakura/pull/494)).
- Documented the public `zakura-state` pruning API changes from #470:
  `ReadRequest::IsPruned` and `ReadResponse::IsPruned(bool)` were replaced by
  `ReadRequest::PruningInfo` and a structured `ReadResponse::PruningInfo`.
  Downstream callers must update their request and response matching
  ([#494](https://github.com/zakura-core/zakura/pull/494)).
- Disabled the unused non-TOML format backends (YAML, JSON, JSON5, RON, INI)
  of the `config` crate, removing their dependency trees from the build.
  Configuration file and environment-variable handling are unaffected
  ([#504](https://github.com/zakura-core/zakura/pull/504)).
- Rustdoc pages for the library crates now show the Zakura logo and favicon
  instead of upstream Zebra branding
  ([#510](https://github.com/zakura-core/zakura/pull/510)).
- Apply batched peer misbehavior scores to the address book every five seconds
  instead of every 30 seconds, so threshold bans become visible to the
  listener and peer set promptly
  ([#428](https://github.com/zakura-core/zakura/pull/428)).
- A Mainnet database written by the original verified-commitment-trees fast sync (VCT-synced and
  below database format version 28.0.1) is now rejected at startup instead of being repaired in
  place. Such a database is missing the historical Sprout anchors needed to verify JoinSplits
  that spend a historical Sprout root, and the repair that used to backfill them has been
  removed. Discard the database and resync, or restore a snapshot taken with a current release
  ([#533](https://github.com/zakura-core/zakura/pull/533)).
- Replaced the `StateInitError::VctSproutHistoryRepairRequired` and
  `::VctSproutHistoryRepairInvalid` variants with a single `::VctSproutHistoryUnrepairable`
  ([#533](https://github.com/zakura-core/zakura/pull/533)).
- Logged the outbound replenishment target, connection limit, and remaining dial
  budget at info once per peer crawl interval. Release builds compile debug
  records out, so this accounting could not be read on a deployed node, and a
  node that has stopped replenishing was indistinguishable from one that has
  enough peers ([#506](https://github.com/zakura-core/zakura/pull/506)).
- `zakurad` now shares a native discovery record with other peers only after it
  confirms that record's owner directly, either through a first-party discovery
  exchange with the owner or a successful dial. Records learned second-hand from
  another peer still serve as local dial hints, so bootstrap reach is unchanged
  ([#558](https://github.com/zakura-core/zakura/pull/558)).
- Native discovery answers a peer's `GetPeers` request from a bounded per-peer
  record set that is stable for a ten-minute window. Repeated requests from one
  peer, however they vary their limits, service filters, or exclusion lists, no
  longer walk further through the local discovery book
  ([#558](https://github.com/zakura-core/zakura/pull/558)).
- Extended the end-of-support window from 18 to 40 days after the estimated
  release height, so Mainnet nodes from this release halt at height 3,484,507
  (~2026-09-15) instead of ~2026-08-24. Upgrade warnings keep their three-day
  lead and begin ~2026-09-12
  ([#575](https://github.com/zakura-core/zakura/pull/575)).

### Removed

- Removed the `zakurad validate-vct-sprout-history` subcommand and the embedded VCT
  Sprout-history repair artifact it audited. The forward fix that stops new databases from
  missing historical Sprout anchors shipped long ago, so the 71 MB embedded artifact and its
  replay migration no longer earn their place
  ([#533](https://github.com/zakura-core/zakura/pull/533)).
- Removed the `zakura-state` public API that existed only to build and audit that artifact:
  `generate_mainnet_from_archive`, `GeneratorError`, `validate_vct_sprout_history`,
  `VctSproutHistoryValidationSummary`, and `VctSproutHistoryValidationError`
  ([#533](https://github.com/zakura-core/zakura/pull/533)).

### Fixed

- Fixed `getblockchaininfo` reporting `pruned: false` on a node configured with
  `storage_mode.pruned` until it first deleted block data, which took at least
  10,000 blocks on Mainnet and Testnet and longer when pruning was enabled on an
  existing archive database. As in `zcashd`, the field now reports whether blocks
  are subject to pruning, not whether any block has been deleted yet. Databases
  that had already pruned were unaffected
  ([#470](https://github.com/zakura-core/zakura/pull/470)).
- Fixed a queued-block index leak where dequeuing one fork sibling un-indexed
  another block at the same height, preventing it from being pruned and leaking
  entries in the block queue and its known-UTXO cache
  ([#483](https://github.com/zakura-core/zakura/pull/483)).
- Reject noncanonical Orchard and Ironwood proof sizes while parsing V5 and V6
  transactions, and ban peers that send these malformed transactions
  ([#410](https://github.com/zakura-core/zakura/pull/410)).
- Indexer gRPC tip and mempool subscriptions now apply backpressure with a
  60-second send timeout instead of being dropped the moment their buffer
  fills, so a briefly slow consumer no longer triggers rapid re-subscribe
  cycles ([#486](https://github.com/zakura-core/zakura/pull/486)).
- Prevent mined-block broadcasts from waiting for peers that disconnected while unavailable
  ([#497](https://github.com/zakura-core/zakura/pull/497)).
- Unknown peer message commands are now escaped before they are written to the
  log, so control characters sent by a peer can no longer alter log output
  ([#513](https://github.com/zakura-core/zakura/pull/513)).
- Fixed Zakura's P2P user agent so every transport mode advertises the
  `zakurad` release version without an internal networking crate version
  ([#524](https://github.com/zakura-core/zakura/pull/524)).
- Fixed `z_gettreestate` returning a JSON `null` treestate, and `z_getsubtreesbyindex`
  returning an empty list, for heights a verified-commitment-trees fast-synced node has no
  data for. Clients following the lightwalletd contract read an absent treestate as the
  _empty_ tree, so a wallet could derive a birthday anchor asserting an empty commitment
  tree deep in the chain with no error raised. Both now return a typed archive-mode error,
  and verbose `getblock`/`getblockheader` preserve that explanation rather than reporting
  bare "missing Orchard tree" and "missing Sapling tree" errors, respectively. Subtree errors
  also report whether this node has determined that it will never hold the data, or has not yet
  reached the handoff needed to decide
  ([#534](https://github.com/zakura-core/zakura/pull/534)).
- Restored outbound peer replenishment to the configured peer set size. The
  crawler's per-interval dial budget targeted a share of the outbound
  connection limit, so halving that limit in
  [#478](https://github.com/zakura-core/zakura/pull/478) also halved the
  steady-state outbound peer count. Nodes now dial toward 80% of
  `peerset_initial_target_size`, bounded by the outbound connection limit
  ([#506](https://github.com/zakura-core/zakura/pull/506)).

### Security

- Ban peers that directly serve blocks with contextual consensus violations,
  without blaming peers for invalid ancestors
  ([#330](https://github.com/zakura-core/zakura/pull/330)).
- `zakurad-log-filter` no longer runs log text as a shell command. Previously a
  log line containing a single quote could execute arbitrary commands as the
  user running the filter
  ([#481](https://github.com/zakura-core/zakura/pull/481)).
- Removed the unmaintained `structopt` (RUSTSEC-2022-0104) and
  `rustls-pemfile` (RUSTSEC-2025-0134) dependencies and their advisory-flagged
  transitive trees (`ansi_term`, `atty`, `proc-macro-error`), migrating the
  `zakura-checkpoints` CLI to clap 4 and RPC TLS PEM parsing to
  `rustls-pki-types`. Command-line flags and TLS certificate/key file handling
  are unchanged
  ([#504](https://github.com/zakura-core/zakura/pull/504)).
- CI now verifies the `doctl` and `rustup-init` binaries it downloads against
  pinned upstream SHA-256 checksums before executing them
  ([#504](https://github.com/zakura-core/zakura/pull/504)).

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
  (`vct/mainnet-vct-manifest.json`) that CI verifies against the embedded
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
