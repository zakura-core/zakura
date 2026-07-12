# CHANGELOG

All notable changes to Zakura are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org).

## [Unreleased]

### Security

- Fixed a remotely triggerable panic in the `getblock` RPC with verbosity 2 on a
  side-chain block (GHSA-x6v8-c2xp-928m). Transaction `confirmations` are now a
  signed `i64`, matching zcashd, so the `-1` sentinel returned for side-chain
  blocks no longer overflows the previous `u32` conversion and aborts the node.
  Backported from upstream Zebra PR #10889.
- Reject mempool transactions with non-standard transparent inputs _before_ script
  verification, avoiding the more expensive script checks and reducing DoS surface.
  Script verification now runs on the shared Rayon thread pool so it no longer blocks
  the runtime. Fixes GHSA-84j3-rw4c-gqmj (thanks to @ouicate for reporting).
  Backported from upstream Zebra PR #10936.

### Fixed

- Nodes running the Zakura or dual P2P stack no longer advertise a doubled
  `/Zakura:<version>/Zakura:<version>/` user agent on legacy connections: the
  Zakura token is only prepended when the configured user agent doesn't
  already carry it.
- Repair evicted verified-commitment-trees supplied roots instead of stalling
  sync forever. When the finalized writer rejects and evicts a well-formed but
  invalid supplied root, header sync now re-requests the canonical `H`/`H+1`
  header/root tuple through a bounded repair lane (one episode per stalled
  height: six serial peer attempts within four minutes, pinned to the node's
  own canonical hashes). The state writer remains the final verifier, and the
  node stays fail-closed with an operator signal if repair exhausts.
- Prevent verified-commitment-trees checkpoint sync from stalling at checkpoint
  boundaries by authenticating a block's supplied roots with its already-validated
  successor header, without waiting for the successor block body to finish checkpoint
  verification.
- Handle the `invalidateblock` and `reconsiderblock` state-control RPC edge
  cases (invalidating a non-finalized chain root, invalidating same-height
  sibling fork tips, and repeated reconsideration of the same block) without
  panicking the non-finalized state write task. Backported from upstream Zebra
  PR #10592.
- Settle `zakura-checkpoints`-generated checkpoints past the full 1000-block
  rollback window (`MAX_BLOCK_REORG_HEIGHT`) instead of only the coinbase
  maturity, so a shipped checkpoint cannot be orphaned by a reorg Zakura would
  still follow. `MAX_BLOCK_REORG_HEIGHT` now lives in `zakura-chain` as the single
  source of truth. Backported from upstream Zebra PR #10719.
- Added a default 180-second request timeout to `RpcRequestClient`, so RPC calls
  no longer hang indefinitely when a server accepts the connection but never
  sends a response. A new `new_with_timeout` constructor lets callers with
  legitimately long-running calls (such as `generate`) opt into a longer bound.
  Backported from upstream Zebra PR #10468.

### Changed

- Prepared the Zakura crate graph for publication at `1.0.0-rc2`. Install the
  `zakura` package with Cargo to obtain the `zakurad` executable; Zakura-owned
  libraries now share the release version, including the renamed
  `zakura-tower-batch-control` and `zakura-tower-fallback` packages.
- Corrected Zakura's release identity and build provenance. `zakurad --version`
  now reports the `1.0.0-rc0` release with source-commit build metadata, and
  Docker images expose OCI version, revision, source, and title labels. The
  Cargo packages are now named `zakura` and `zakura-watchdog`; the watchdog
  binary, systemd unit, runtime paths, and Sentry identity use
  `zakura-watchdog`.
- Renamed Zakura-owned crates, binaries, tools, deployment helpers, and current
  documentation from Zebra/Zebrad names to Zakura/Zakurad names. This is a
  breaking migration: use `zakura.toml`, `ZAKURA_*`, the `zakurad` binary, and
  `p2p_stack = "legacy"`; the old Zakura-owned aliases and automatic cache
  migration have been removed. `zebrad.toml` remains a deprecated fallback when
  `zakura.toml` is absent. The config loader accepts legacy `ZEBRA_*` environment
  variables with lower precedence than `ZAKURA_*`, and the Docker entrypoint
  translates them to `ZAKURA_*`. The deprecated `legacy_p2p` and `v2_p2p`
  booleans still map to `p2p_stack`. External zcashd and protobuf contracts
  retain their upstream names.
- zcashd-compat no longer adds or requires the obsolete zcashd deprecation
  acknowledgement in `zcash.conf`.
- Removed the deprecated zcashd-compat dedicated RPC listener (`:28232`) and its
  cookie/TLS configuration. The P2P sidecar syncs over legacy Zcash P2P only;
  use the standard `rpc.listen_addr` endpoint for operator JSON-RPC queries.

### Added

- Set the NU6.3 (Ironwood) network upgrade activation height on Mainnet to
  `3_428_143`, matching `zcash_protocol`'s Mainnet parameters. NU6.3 was already
  scheduled on Testnet (`4_134_000`); this schedules it on Mainnet as well.
- Zakura now tags the coinbase input of every block it mines with a `🌸`. The
  `mining.extra_coinbase_data` option is now limited to 86 bytes (was 94);
  Zakura refuses to start if it is exceeded.

### Changed

- Replaced the `network.legacy_p2p` and `network.v2_p2p` booleans with a single
  `network.p2p_stack` setting, which selects `"legacy"` for the legacy TCP
  Zcash P2P stack, `"zakura"` for the native Zakura P2P v2 stack, or `"dual"`
  for both. It defaults to `"default"`, which follows Zakura's binary default
  for the configured network so it can change during upgrades: currently
  `"legacy"` on Mainnet, and `"dual"` on Testnet and Regtest.
  The old booleans are deprecated but still parsed, so existing configs keep
  working. Setting them alongside `network.p2p_stack` is an error, and setting
  both to `false` — which used to start a node with no peer-to-peer networking
  at all — is now rejected.
- Verified-commitment-trees fast sync is now enabled by default when checkpoint
  sync is enabled. Operators can keep checkpoint sync but opt out of the new
  path by setting `consensus.vct_fast_sync = false`.
- Refreshed the default Testnet Zakura bootstrap peer identities
  (`DEFAULT_TESTNET_ZAKURA_BOOTSTRAP_PEERS`) after the Testnet fleet's iroh node
  keys were rotated. The previous hardcoded node IDs were stale, so a fresh node
  using the default config could not discover the Testnet fleet over Zakura. The
  peer IP addresses and Mainnet bootstrap peers are unchanged.
- Changed Zakura's default cache and cookie directories from `zebra` paths to
  `zakura` paths (for example, Linux defaults now use `~/.cache/zakura` instead
  of `~/.cache/zebra`). Docker, install scripts, and deployment helpers now use
  matching Zakura cache paths by default.
- Renamed operator-facing defaults from `zebrad.toml`, `zebrad.log`, and
  `ZEBRA_*` to `zakura.toml`, `zakura.log`, and `ZAKURA_*`. Legacy config
  filenames, environment prefixes, and binary aliases remain accepted with
  deprecation warnings for compatibility.

### Removed

- Removed two Zakura block-sync config fields that never needed operator
  tuning: `max_reorder_lookahead_blocks` (the defense-in-depth block-count cap
  is now a fixed internal constant; the resident-memory budget remains the
  primary bound) and `fanout` (a legacy reservation multiplier with no effect
  on scheduling). Configs that still set either key will fail to parse and
  should drop the lines.

### Fixed

- Made opening a read-only secondary state database safe: a read-only open
  against a missing, unreadable, or ephemeral database now returns an explicit
  error instead of panicking, and the co-located read-state syncer backs off
  instead of spinning while the primary node's database is unavailable.
  Backported from upstream Zebra PR #10741.
- The read-only secondary database used by the co-located read-state service no
  longer flushes RocksDB on shutdown. Only the primary owns its files, so a
  secondary flush on exit could race the primary's writes; read-only secondaries
  now close without flushing. Backported from upstream Zebra PR #10784.
- Fixed two zcashd-compat sidecar stalls under production connection patterns.
  Sidecar IP matching now treats native IPv4 and IPv4-mapped IPv6 addresses as
  the same peer, so the configured sidecar is always included in block inventory
  gossip. The legacy listener also reserves one inbound slot for the sidecar and
  exempts its reconnects from the ordinary recent-IP throttle, preventing public
  peers from filling every slot while zcashd loads its block index. Accepted
  socket addresses, ban checks, and the total inbound connection limit retain
  their existing behavior.
- Fixed the mainnet status dashboard's Zakura trace path.
- Fixed a Zakura block-sync stall after header reanchors / tip resets: the
  post-reset producer refill no longer skips when peer-registry outstanding is
  still briefly inflated, so downloads resume instead of leaving an empty work
  queue (`max_outstanding = 0`).
- Fixed mined block gossip suppressing the committed-tip fallback before
  `AdvertiseBlockToAll` completed. The gossip task now marks a submitted block
  hash as seen only after the all-peers broadcast returns successfully within
  the broadcast timeout, and preserves that mark across spawned tip-change
  futures. If the mined broadcast times out, the committed-tip path still sends
  `AdvertiseBlock` as a fallback.
- Fixed misleading Zakura connectivity telemetry: header-sync now records
  record-only peer violations as `header_peer_violation_recorded` instead of
  `header_peer_disconnect_requested`, header-sync exposes the authoritative
  `zakura.p2p.connected_peers` gauge and freshness-derived
  `zakura.p2p.healthy_peers` gauge, header- and block-sync expose active
  reactor connection counts, discovery dials classify already-connected peers
  separately from genuinely short-lived registrations, and the metrics
  dashboard now reads the authoritative connected-peer gauge.
- Zakura header-store consensus reads now verify store invariants as they
  walk: the difficulty-context walk checks that every stored header is the
  block its hash row names and links to the row below it, and the anchor
  lookup distinguishes a corrupted index round-trip from a genuinely unknown
  anchor. A corrupted store now surfaces as an explicit local
  `StoreIncoherent` error (never scored against peers) instead of poisoning
  difficulty validation and rejecting honest headers with
  `InvalidDifficultyThreshold`.
- Fixed three Zakura header-store write paths that could leave the on-disk
  header store internally incoherent after chain forks, causing nodes to
  reject valid headers from honest peers (`InvalidDifficultyThreshold` /
  `UnknownAnchor`) and wedge below the network tip until manual intervention.
  Header ranges must now link to their anchor and be internally contiguous
  (rejected with the new `UnlinkedRange` error otherwise, which is classified
  as a local, non-peer-scoring failure), header ranges re-delivered over
  heights that already have committed block bodies no longer re-insert
  provisional header rows below the body tip, and the header row seeded from
  a committed best-chain block is skipped when it does not link to the stored
  row below it (header-range sync converges the store instead).
- Zebra can now audit the Zakura header store when the state database opens and
  self-repair any incoherence (broken linkage, hash↔height index mismatches,
  gaps with stranded rows above them, stale rows at committed heights) by
  truncating the Zakura column families to the last coherent height in bounded
  batches, then letting header sync re-download the truncated suffix. Operators
  can opt in with `state.repair_zakura_header_store_on_startup = true`; each
  repair emits a warning and the `state.zakura.header_store.incoherent` metric.
- Fixed dual-stack Zakura fallback shutting down the Zakura serving layer. When
  the stall watchdog resumes legacy `ChainSync`, Zakura now keeps its header-
  and block-sync reactors alive as a serving/advertising bridge while an apply
  gate drains in-flight Zakura body applies before legacy sync drives commits.
- Fixed legacy peers being disconnected for returning empty `FindBlocks` or
  `FindHeaders` responses when Zebra is at or near the network tip.
- Kept an already-active mempool and `getblocktemplate` mining RPCs running
  when the legacy sync status temporarily reports Zebra is far from the tip.
  Initial mempool activation still waits until Zebra is within 100 blocks of
  the tip.
- Fixed a near-tip sync restart loop when a timed-out `AwaitUtxo` lookup in the
  transaction verifier was converted to `InternalDowncastError` instead of a
  missing transparent input.
- Fixed Zakura nodes gossiping and following non-best-chain blocks. An inbound
  `NewBlock` that did not land on the best chain (for example a testnet
  min-difficulty branch) still advanced the node's Zakura header and verified
  frontiers and was forwarded to peers, so a whole fleet could advertise and
  propagate a losing branch over Zakura while each node's own chain stayed on
  the best one — stranding zakura-only peers that followed the gossip. An
  accepted `NewBlock` is now checked against the best chain before it advances
  any frontier or is forwarded; non-best-chain accepts are remembered for dedup
  only and counted in `sync.header.tip.new_block.non_best_chain`.
- Fixed dual-stack nodes (`v2_p2p` and `legacy_p2p` both enabled) permanently
  shutting down their own Zakura header- and block-sync drivers when a legacy
  peer on a foreign fork answered the body-sync stall watchdog's cross-check
  probe. The probe counted peer-offered block hashes that were missing from
  our state as "blocks ahead" without checking they extend our chain, so a
  canonical-network peer connected to a fork node tripped the fallback
  threshold while the node was exactly at its own network tip. The probe now
  requests headers and only counts a run that anchors at a block we already
  have and links parent-to-parent; each probe decision is logged. Also added
  header-sync reactor liveness metrics (`sync.header.reactor.iterations`,
  per-event `started`/`finished` counters) and a loud log if the reactor loop
  ever exits, since a stopped reactor previously left no trace at default log
  levels.
- Fixed healthy but quiet Zakura connections being closed by the application
  idle reaper every idle window. The reaper only counts inbound application
  messages and the periodic header-sync status refresh suppressed unchanged
  statuses, so two peers idle at the same tip went mutually silent and reaped
  their connection each idle timeout, then redialed — constant connection
  churn between synced peers. Header sync now sends a redundant status as an
  application keepalive on a spam-safe budget, so healthy connections stay
  fresh while unused connections are still reaped. Service park decisions
  (admission rejections and no-demand ordered streams) are now logged at info
  and counted in metrics, since a parked peer is indistinguishable from a
  wedged remote from the other side.
- Moved the auto-generated Zakura iroh node identity key out of Zebra's cache
  tree and into `network.identity_dir` (defaulting to
  `~/.zakura/<network>.zakura-iroh-secret-key`), so cache or state snapshots do
  not clone a node's long-term P2P identity.
- Fixed Regtest Zakura defaults so they no longer inherit Mainnet bootstrap
  peers. Regtest nodes now start with an empty Zakura bootstrap peer list and
  log a separate warning when no Zakura bootstrap peers are configured.
- Fixed a restarted or resyncing Zakura peer being locked out of block sync for
  up to ~150s (occasionally longer) when it redialed the fleet from its stable
  IP. The receiving node kept the peer's previous, now-dead connection as the
  incumbent and rejected every redial as a duplicate until the incumbent aged
  past the 300s eviction gate or was reaped by the QUIC idle timeout. A same-IP
  duplicate (a restarted peer reclaiming its own slot) now evicts the stale
  incumbent on a short gate so the redial reconnects within seconds, while a
  just-registered incumbent is still kept so simultaneous-open races do not flap.
- Fixed Zakura header-sync and block-sync peers getting stuck unable to serve
  requests when an initial `Status` advertisement was dropped by a full outbound
  queue. Status send bookkeeping now only records queued frames, header sync
  retries unsent status advertisements, and block sync replies to the first
  inbound status so peers converge after dropped connect-time advertisements.
- Raised the default Zakura per-IP admission cap from 1 to 16 so NATed or
  co-hosted v2 peers are not rejected while the legacy TCP per-IP default
  remains 1.
- Fixed a block-sync busy-spin under sustained byte-budget backpressure. The
  sequencer re-published its progress view (waking the reactor and every per-peer
  routine) even when no schedulable field had changed, which combined with the
  per-attempt floor-funding request to spin a routine's refill loop with no timer
  while the budget was pinned. The sequencer now wakes watchers only when a
  scheduling-relevant field actually changes.
- Fixed a transient `getinfo` RPC panic while Zebra is committing early synced
  blocks and concurrently calculating display-only chain-tip difficulty.
- Fixed an out-of-memory crash during Zakura block sync when the header chain
  runs far ahead of the commit tip. The block-sync applying buffer holds decoded
  block bodies ahead of the in-order committer; its look-ahead budget counted
  wire bytes (not the ~4× larger decoded footprint) and the floor-rescue path
  bypassed the budget entirely and advanced with the _download_ floor, so the
  buffer grew unbounded (~569k blocks, ~26 GiB RSS) until the kernel killed the
  node. The budget now bounds estimated resident memory (retained and in-flight
  wire bytes at the decoded multiple) and gates the floor lane, exempting one
  checkpoint range above the verified tip (the commit window) so a pinned
  checkpoint range can always assemble and the committer can always drain the
  pipeline (no deadlock). Resident memory now plateaus near the configured budget
  plus at most one worst-case commit window (~3.2 GB), with only bounded transient
  overshoot from floor-rescue requests.

### Performance

- Reduce Zakura block-sync CPU overhead in the BBR-lite fill loop and trace path.
  The byte-mode cold-start check now tests for fresh BDP samples without scanning
  the BBR windows, and per-view trace rows skip expensive peer/work-queue
  diagnostics while still reporting commit pipeline progress.
- Improve Zakura block-sync download scheduling for checkpoint sync. A
  byte-denominated BBR-lite congestion controller (`block_sync/bbr.rs`) and
  per-peer admission control (`block_sync/admission.rs`) replace the previous
  request pacing, a floor-rescue path keeps the lowest missing body height
  fundable even under a full byte budget, and the checkpoint-frontier refresh
  interval is shortened (5s → 200ms) so the checkpoint apply window recycles
  promptly instead of leaving the finalized writer idle between refreshes. Also
  adds an offline block-sync `Sequencer` benchmark helper
  (`zakura::spawn_bench_sequencer`) behind a new `internal-bench` feature.
- Remove O(n) scans from the Zakura block-sync sequencer's per-event hot path,
  which stalled the checkpoint-sync commit pipeline for tens of seconds. During
  checkpoint sync, headers race far ahead of the body tip, so the sequencer's
  `WorkQueue` holds the entire lag (100k+ pending heights) in mutex-guarded
  `BTreeMap`s and the `applying` map holds thousands of buffered bodies. Three
  operations scanned these on every body/control event or floor advance, going
  quadratic as the backlog grew and serializing the work-queue lock (freezing
  both commit and download): (1) `WorkQueue::reserved_bytes()` re-summed reserved
  request bytes across `pending` + `in_flight` on every `publish_view`;
  (2) `advance_floor`/`reset_above` ran a full-map `retain` to drop committed
  heights; and (3) `publish_view`'s `applying_buffered_bytes` /
  `submitted_applying_count` / `submitted_applying_bytes` /
  `unsubmitted_applying_count` each folded over the whole `applying` map. All are
  now O(1) or O(removed·log n): `reserved_bytes` and the applying totals are
  incrementally-maintained counters (cross-checked against the independent byte
  budget by the existing `publish_view` audit, and asserted drift-free by new unit
  tests), and `advance_floor`/`reset_above` pop only the committed prefix/suffix
  instead of scanning the whole map.
- Precompute checkpoint-zone auth data roots before finalized-state commitment,
  and reuse the shared txid/auth-digest conversion while preparing semantic block
  data. This moves ZIP-244 authorizing-data commitment work off the finalized
  committer's critical path when available, while preserving the existing
  recompute fallback.
- Compute the v5 ZIP-244 txid and authorizing-data digest natively. Both
  previously routed through `Transaction::to_librustzcash`, which re-serializes
  and reparses the whole transaction — decompressing every Jubjub and Pallas
  curve point — purely to feed the same canonical bytes into the BLAKE2b digest
  tree. A new `zakura-chain` `transaction::zip244` module builds the txid and
  auth-commitment digests directly from Zebra's already-parsed transaction
  fields, removing that reparse on the checkpoint path where no point is ever
  needed. v6 transactions (the unstable `tx_v6` feature) still route through
  `librustzcash`. The output is byte-identical: a differential property test
  (`native_zip244_matches_librustzcash`) asserts the native txid and auth digest
  match the `librustzcash` conversion across thousands of random v5 transactions.
- Parallelize per-block serialization in the finalized block writer. On heavy
  shielded blocks, serializing the raw transaction bytes (`tx_by_loc`) and
  computing the block size for `BlockInfo` dominate the per-block write cost. Both
  are now done across the rayon pool — `par_iter` over the block's transactions —
  inside the dedicated `COMMIT_COMPUTE_POOL` so the workers don't contend with the
  download/verification pipeline. The raw-bytes path is byte-identical (`RawBytes`
  is stored verbatim) and the size path is byte-count-identical (header +
  CompactSize(tx_count) + sum of transaction sizes). Both fork-joins are gated on a
  transaction-count threshold (`PARALLEL_BLOCK_TX_THRESHOLD = 16`) so small
  early-chain blocks, where the fork-join overhead would outweigh the work, run
  sequentially.
- Parallelize the finalized writer's spent-UTXO and address-balance reads. In
  transparent-heavy checkpoint ranges these cache-served point lookups were
  issued serially on the writer thread; blocks with at least 16 reads now fan
  them across the rayon pool and reuse each spent output location for the UTXO
  lookup, reducing the serial read overhead without changing the committed batch.
- Cache the `MerkleCRH^Orchard` Sinsemilla hash domain. The Orchard
  note-commitment Merkle hash previously rebuilt the Sinsemilla `HashDomain` —
  including a full `hash_to_curve` for its `Q` generator — on every node hash,
  even though the domain (`z.cash:Orchard-MerkleCRH`) is constant for the whole
  tree. The domain is now derived once and reused, speeding up every Orchard
  note-commitment tree hash, including the irreducibly-serial per-block `root()`
  chain (`orchard_combine` microbench ~−15%). The output is byte-identical.
- Parallelize note-commitment tree updates during checkpoint-zone sync. Sapling
  and Orchard note commitments for each block are now appended to the incremental
  Merkle frontier using a parallel divide-and-conquer reduction across the rayon
  pool (`parallel_append` in `zakura-chain`), and the note-commitment tree update
  runs concurrently with the block-commitment validity check on the rayon pool.
  Combined, these changes roughly double checkpoint-zone throughput on multi-core
  hosts (~20 → ~42 blk/s on an 8-core machine at 1.7M height). A new
  default-off `commit-metrics` feature emits per-block timing histograms
  (`zebra.state.write.*`) for future profiling.
- Limit RocksDB write-ahead logs to 4 GiB in the finalized state database. Heavy
  sync could otherwise accumulate tens of GiB of WAL files, making restarts spend
  minutes replaying logs before Zebra could resume syncing.

### Changed

- Use network-specific default Zakura bootstrap peers: Mainnet keeps the existing
  native-P2P bootstrap list, while Testnet defaults to the Zakura testnet fleet.
- Increased Zakura's default connection, handshake, stream-open, and QUIC
  window limits, and configured default native Zakura bootstrap peers. The
  larger defaults are intended for the production native-P2P sync path rather
  than the earlier conservative test-network envelope.
- Bound Zakura block-sync requests to non-responsive peers with a probe-first
  no-progress policy: peers receive only `initial_block_probe_requests` before
  their first accepted block body, then `max_requests_without_block_progress`
  becomes the hard cap before liveness disconnects them.
- Extended finalized-state value-pool disk serialization with an Ironwood slot
  after the deferred pool, keeping older value-pool records readable.
- Use V3 chain-history entries from NU6.3 onward, including Ironwood note
  commitment roots and transaction counts.
- Reject transactions that add net value to the Orchard pool after NU6.3
  activation.
- V6 transactions with supported NU6.3-or-later consensus branch IDs now
  serialize and deserialize successfully, while unsupported later placeholders
  are rejected.
- Route post-NU6.3 coinbase rewards for Orchard receivers in unified miner
  addresses to the Ironwood pool instead of rejecting them or falling back to a
  lower-priority receiver.
- Unified the workspace Minimum Supported Rust Version (MSRV) at 1.91, matching
  the `zebrad` binary. The library crates previously declared 1.85.1, but the
  dependency tree (via `iroh`/`sentry` → `time 0.3.47`, plus the
  `tonic`/`darling`/`serde_with` chain) now requires Rust 1.88, and keeping
  those current versions is required to stay clear of RUSTSEC-2026-0009, a
  stack-exhaustion DoS fixed in `time 0.3.47`.

### Added

- Added Zakura header-sync commitment roots to ranged header responses. Stream
  version 5 requests and serves one tree-aux root payload per header, persists
  received roots with header-only ranges, and caps rootless header tips until
  matching roots are available.
- Added Ironwood value pool entries to `getblockchaininfo` and verbose
  `getblock` RPC output.
- Report `pruned: true` in `getblockchaininfo` after Zebra has pruned
  historical raw transaction data, matching the node's storage mode instead of
  always reporting archive behavior.
- Pruned storage mode (`state.storage_mode`). When set to `pruned`, Zebra deletes
  historical raw transaction bytes (`tx_by_loc`) outside a configurable retention
  window (`tx_retention`), reducing disk usage while keeping all consensus-critical
  state and the indexes needed to validate future blocks. The retention floor is
  10_000 blocks on Mainnet/Testnet, and the reorg window + 1 on Regtest so tests can
  exercise pruning without a 10_000-block chain. This is a one-way mode: a pruned
  database cannot be reopened in archive mode.
  Historical RPC queries such as `getrawtransaction` for pruned transactions
  return a not-found error. During checkpoint bootstrap, pruned nodes can also be
  missing raw transaction data near their current tip until sync passes the
  retention floor, so `getrawtransaction` and verbose `getblock` can fail for
  recent-looking heights. The default remains `archive` (keep all data).
- Offline pruning tooling (`zebrad prune-state` and the standalone
  `zakura-prune-state` binary) to reclaim historical raw transaction data from an
  existing database in a single pass, including data left intact when pruning is
  first enabled on an archive database. Defaults to a preview; pass `--confirm`
  to apply.
- Added the default-off Zakura P2P iroh dependency scaffold, including a
  reserved persistent iroh node secret-key config surface and relay/discovery-off
  endpoint construction helper.
- Added default-on `v2_p2p` and `legacy_p2p` network config flags for the
  Zakura P2P v2 endpoint and legacy Zcash P2P networking paths.
- Added bounded Zakura P2P v2 upgrade prelude and control-handshake wire types,
  including transcript binding, native-vs-upgraded control validation, and
  duplicate-peer handling scaffolding.
- Added bounded Zakura header-sync stream-5 wire messages, stateless header
  validation, and the default `network.zakura.header_sync` config surface.
- Include the `zakura-rollback-state` and `zakura-prune-state` utilities alongside
  `zebrad` in release Docker images and Docker CI builds.
- Use the `5.0.0-rc.3` release identity for this fork's v5 rollback build.
- zcashd-compat mode for managing zcashd as a wallet while leveraging zebra for p2p.
- zcashd-compat RPC can serve HTTPS with configured TLS certificate and private
  key files, including an explicit TLS-only mode that disables cookie auth for
  externally protected deployments.
- zcashd-compat preflight now validates filesystem permissions for the zcashd
  datadir, `zcash.conf`, zcashd binary, Zakura state directory, and RPC cookie
  directory before creating directories or config files, reporting all problems
  in one aggregated error. Failures can be bypassed with `--unsafe-low-specs`.
- `zcashd_compat.unsafe_allow_remote_http` allows a non-loopback zcashd-compat
  RPC listener without TLS for deployments where another boundary secures the
  listener, such as a private container network. Non-loopback listeners
  otherwise require TLS.
- Added Ironwood RPC output for `getblock`, `getrawtransaction`,
  `z_gettreestate`, and `z_getsubtreesbyindex`.

### Changed

- Parallelize NU5-onward block auth-data-root computation across transactions,
  reducing contextual validation time for blocks with many or large shielded
  transactions.
- Tune public-fork sync defaults for faster block sync: retry sync rounds after
  10 seconds, allow 30 seconds for tip acquisition, increase default block
  download concurrency to 100, increase the default peer target size to 100, and
  cap inbound peers at the peer target size.
- Increase the default zcashd-compat supervised shutdown grace period to 5
  minutes, giving `zcashd` more time to flush wallet and chainstate data before
  force-kill.
- Make zcashd-compat supervision retry indefinitely with capped exponential
  restart backoff, reset the backoff ramp after healthy child uptime, and expose
  active/disabled/exhausted supervisor state through metrics.
- zcashd-compat supervision now also retries `zcashd` spawn failures with the
  same capped backoff instead of permanently ending supervision when the binary
  is briefly missing or unspawnable.
- zcashd-compat now defaults to externally managed `zcashd` path mode, so Zebra
  starts the compat RPC endpoint without spawning `zcashd` unless supervision is
  explicitly enabled.
- Update `zakura-rollback-state` and `zebrad rollback-state` to run rollback by
  default and use `--dry-run` for rollback-plan previews (replacing the old
  `--force` gate).
- Increased Zebra's local rollback window (`MAX_BLOCK_REORG_HEIGHT`) from 99 to
  1000 blocks as a defence-in-depth measure against sustained consensus splits.
- Decouple Zakura block-sync downloads from commit speed. The body-download
  refill mark now counts only the download pipeline (queued and in-flight
  requests), not the commit pipeline (reorder and applying buffers), so a slow
  commit/verify no longer throttles downloads. Download depth is bounded by the
  in-flight byte budget (`network.zakura.block_sync.max_inflight_block_bytes`)
  and per-peer slots, letting downloads run ahead of commit.
- Open a Zakura block-sync stream from both ends of a connection regardless of
  who dialed, so a node can serve and download over every peer instead of only
  peers that proactively opened the stream toward it. A block-sync stream opened
  by both sides is resolved to one survivor by a deterministic node-id tiebreak,
  never by dropping the connection. Other ordered services are unchanged.

### Fixed

- Use network protocol version 170160 as the NU6.3 minimum on Mainnet, Testnet,
  and Regtest, matching Zebra's advertised current protocol version.
- Avoid panics in the block write task when RPC users invalidate a non-finalized
  root block or reconsider the same invalidated block twice.
- Compare RPC authentication cookies in constant time after checking their
  length.
- Make the Zakura body-sync stall watchdog use verified-tip progress only,
  ignoring the best-header gap so fast header sync cannot trigger a false
  fallback while block verification is still advancing.
- Stop the Zakura body-sync watchdog from running two commit pipelines at once.
  When Zakura block sync stalled, the watchdog reactivated the legacy ChainSync
  body downloader but left the Zakura block- and header-sync drivers running, so
  both fed the state-commit pipeline concurrently — breaking its accounting and
  deadlocking the node. The watchdog now cancels the Zakura sync drivers (via the
  endpoint shutdown token) before handing off to legacy ChainSync. The fallback is
  also limited to dual-stack nodes (`v2_p2p` and `legacy_p2p` both enabled); a
  Zakura-only node, which has no legacy peers to fall back to, instead keeps
  waiting for Zakura and logs a warning once per stall window.
- Treat missing transaction inventory responses during mempool download as a
  recoverable download failure, avoiding a panic when public peers no longer
  have a gossiped transaction available.
- Roll back the Zakura header store together with finalized block data, so
  databases produced by `zakura-rollback-state` can resume Zakura body sync from
  the new body tip instead of stalling behind stale headers and falling back to
  legacy sync.
- Report `pruned: true` in `getblockchaininfo` after Zebra has pruned
  historical raw transaction data, matching the node's storage mode instead of
  always reporting archive behavior.
- The zcashd-compat supervisor no longer force-kills `zcashd` outside its own
  SIGTERM → grace period → SIGKILL sequence. The child is spawned without
  `kill_on_drop` and in its own process group, so zebrad panics, supervisor
  task aborts, and group-wide terminal signals can no longer SIGKILL `zcashd`
  mid-flush, and Zebra now waits the shutdown grace period plus a fixed margin
  for the supervisor task before abandoning it. An interrupted shutdown was
  able to silently discard hours of ingested chainstate and force a long
  replay on the next start.
- Always include configured `zcashd_compat.block_gossip_peer_ips` in block
  inventory gossip, preventing pinned zcashd-compat peers from stalling when
  normal fractional peer sampling misses them.
- Make `zakura-rollback-state` rollback existing v5 databases without replaying
  note commitment trees from genesis for modern rollback targets whose removed
  blocks did not change the Sprout tree. If rolled-back blocks contain Sprout
  commitments, rollback keeps the full rebuild path so the Sprout tip is reset
  correctly.
- Retry missing block downloads inside the active sync round, avoiding long
  stalls when a peer reports `notfound` for a required block hash.
- Give the head-of-line block priority during checkpoint sync. When a required
  block is missing from all current peers, Zebra now pauses _new_ speculative
  look-ahead downloads and retries the block on a non-blocking backoff timer,
  so the in-flight pipeline drains and frees peers for the critical block
  instead of saturating them with future-block downloads. This removes minutes-
  long head-of-line freezes on thin-peer draws. New `pool.route_inv.*` metrics
  break down single-block routing outcomes (advertiser / maybe / synthetic
  not-found by cause) to diagnose peer-scarcity stalls.
- Report a finalized block as known for `Request::KnownBlock` even after its body
  has been pruned, deciding membership from the retained hash index rather than
  body availability. Without this, a pruned-mode node treated already-finalized
  historical blocks whose bodies were pruned as unknown and re-downloaded them over
  Zakura sync and inbound gossip only to reject them as behind the finalized tip.
- Stop a legitimately quiet Zakura ordered stream from tearing down the whole
  connection it shares with an actively-transferring stream. The per-frame read
  deadline was the connection idle timeout and was treated as fatal, so a stream
  that is normally quiet during catch-up sync (e.g. gossip while far below the
  tip) disconnected the peer every idle-timeout window even while block sync was
  downloading on the same connection, causing constant re-dial and
  duplicate-connection churn. Inter-frame quiet on a persistent ordered stream is
  now tolerated; connection-level idleness remains owned by the freshness reaper
  and the QUIC idle timeout.
- Reclaim overdue Zakura block-sync requests on the scheduling hot path instead
  of only on the periodic timeout tick, and bias a timed-out range's retry away
  from the peer that just timed it out (falling back to that peer only when no
  other peer can serve the range). A single slow peer holding the contiguous
  floor block can no longer stall the commit pipeline for far longer than the
  request timeout.

### Security

- Reject invalid Sapling `cv` and `epk` point encodings during the fast semantic
  precheck for V6 transactions, matching the existing V4/V5 behavior and keeping
  small-order Sapling outputs out of the expensive batch verifier.
- Write RPC authentication cookies through a freshly created private temporary
  file before replacing `.cookie`, so pre-existing permissive cookie files cannot
  expose the generated RPC authentication secret.
- The Zakura body-sync fallback watchdog now distinguishes genuine block-sync
  progress — the verified tip closing the gap to the best-header network frontier —
  from inbound gossip that only nudges the tip. A peer trickling occasional
  next-height blocks can no longer keep the node from falling back to legacy sync
  while it stays materially behind the network tip.
- Zakura header sync now compares cumulative chain work before replacing an
  existing header chain: a conflicting header range is rejected unless it carries
  strictly more work than the chain it would overwrite. Previously a shorter or
  lower-work conflicting range (for example a low-difficulty header flood with
  manipulated timestamps past the last checkpoint) could replace a longer,
  higher-work header chain by height alone and steer body-gap discovery off the
  real chain.

## Pre-fork history

Zakura is a fork of the Zcash Foundation's [Zebra](https://github.com/ZcashFoundation/zebra),
forked at Zebra v5.0.0. For the history of this codebase before the fork
(Zebra 1.0.0 through 5.0.0), see [upstream's CHANGELOG](https://github.com/ZcashFoundation/zebra/blob/main/CHANGELOG.md).
