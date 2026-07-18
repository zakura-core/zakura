# Verified commitment trees — fast checkpoint sync

## Overview (start here)

**What it is.** Below the last checkpoint, Zebra normally rebuilds the Sapling, Orchard, and
Ironwood
note-commitment trees for every block just to learn each block's treestate root — the single
biggest CPU cost of checkpoint sync. Verified commitment trees (VCT) instead **fetch the
per-block roots from peers**, **verify each one against the headers the node already trusts**,
fold them straight into the anchor set and history tree, and **skip the rebuild**. At the
last checkpoint height an **embedded final frontier** (verified against that block's proven root) is
written so normal per-block verification resumes above the checkpoint. Result: same consensus
state as the legacy committer, far less work — and no new cryptography. Sprout is different:
VCT roots do not carry it, so the fast path still appends every JoinSplit commitment locally
and persists each changed historical Sprout frontier.

**The one invariant that makes it safe:** _no root influences consensus state until it has been
authenticated against a header commitment._ Everything else (the transport, the cache, the peer
policy) is plumbing around that invariant. A root that cannot be obtained or verified is refused,
never guessed: while VCT fast sync is using verified roots below the last checkpoint, the committer
stops and retries rather than recomputing from a stale frontier (§8).

**Data flow (fetch + commit path):**

```text
header sync (runs ahead of bodies)
   │ GetHeaders { want_tree_aux_roots } ─▶ peer ─▶ Headers { headers, body_sizes, tree_aux_roots }
   │ (roots carried in-band, all-or-nothing, finalized ranges only; §4.2)
   ▼
header-sync reactor (zakura-network): validate root count + per-height alignment; reject
   │ unrequested or non-finalized roots as MalformedMessage (§8.1)
   ▼
CommitHeaderRange (zakura-state): persist provisional roots into
   │ commitment_roots_by_height, ahead of body commit (§4.2)
   ▼
PeerSource (DB-backed reader) ── vct_root(height) ──▶ finalized committer
   │
   ▼
finalized committer: verify-before-commit (§6) ──fold roots, skip recompute──▶ DB
   │ at the last checkpoint height: verify + write the embedded final frontier ──▶ resume legacy recompute
```

**Serving path (how a node answers other nodes' fetches):**

```text
peer GetHeaders { want_tree_aux_roots } ─▶ header-sync reactor ─▶ header-sync driver (zakurad)
   ─▶ ReadRequest::BlockRoots ─▶ committed commitment_roots_by_height index, then provisional
      entries in the same index for header-ahead heights (all-or-nothing; §9)
```

**Lifecycle of one fast sync.**

(1) Node starts under `consensus.checkpoint_sync = true` on
Mainnet → the committer is built in peer mode.
(2) Header sync requests the per-height roots in-band with the finalized header ranges it already fetches (`want_tree_aux_roots`) and persists the received roots provisionally into the database ahead of the committer (§4.2). (3) Each checkpoint block: look up its root; verify it (own header now, successor header next block, plus
the direct below-Heartwood/below-NU5/below-Nu6_3 checks); fold it in; freeze the frontier (§6, §7).
(4) At the last checkpoint height, verify and write the embedded frontier and unfreeze.
(5) Above the last checkpoint height, ordinary semantic verification resumes from the real frontier. A bad/missing root anywhere in the frozen window parks the block and retries in place; it never writes wrong state. Roots are not individually re-requested, so a hole that no in-flight re-delivery of the same header range fills is a fail-closed stall, surfaced loudly by the §8 metrics.

**Glossary.**

| Term | Meaning |
| --- | --- |
| **Checkpoint sync** | `consensus.checkpoint_sync = true`: trust the embedded checkpoint list for headers/PoW up to the max checkpoint. Precondition for VCT. |
| **last checkpoint height** | The network's max checkpoint height; the boundary where the fast path ends and the embedded final frontier is written. |
| **Fast root** | A peer-supplied `(sapling_root, orchard_root, ironwood_root)` for one height, folded in after verification instead of being recomputed. |
| **Final frontier** | The real Sapling/Orchard/Sprout/Ironwood note-commitment trees at the last checkpoint height, embedded in the binary (§5.2) and written as the tip treestate at last checkpoint height. |
| **Frozen frontier** | During VCT fast sync below the last checkpoint, Zebra folds verified modern-pool roots into the root indexes but does not advance the full Sapling/Orchard/Ironwood frontiers for every block. Sprout continues to advance locally. If a required modern-pool root is missing, the committer must stop and retry later, because recomputing from a stale modern frontier would write invalid state (§8). |
| **Verify-before-commit** | Authenticating each root against the node's header commitments (ZIP-221 MMR one-block-lag + direct sub-Heartwood/sub-NU5/sub-Nu6_3 checks) before it affects state (§6). |
| **Fail closed** | Stop and retry without writing state when a required root is missing or invalid (§8). |
| **Provisional roots** | Peer-supplied roots carried in the header-sync `Headers` message and persisted to `commitment_roots_by_height` ahead of body commit. Advisory until verify-before-commit authenticates them (§4.2, §6). |
| **All-or-nothing** | A `Headers` message carries roots for _every_ header in the range or none; a partial root set is rejected on the wire and never served (§5.4). |
| **Kill switch** | `consensus.vct_fast_sync = false`: keep checkpoint sync but force the legacy committer (§4.4). |

For where each piece lives in the tree, see the file map (§15).

## 1. Goal

Let a node sync the chain up to the last checkpoint **without recomputing the Sapling,
Orchard, and Ironwood note-commitment frontiers per block** — the dominant CPU cost of checkpoint sync
(the per-block `update_trees_parallel` recompute, ~70% of per-block commit time).

Instead of rebuilding the trees, the committer consumes:

1. **per-block commitment roots** (the Sapling and Orchard treestate roots as of the end of
   each block), each **verified against the node's own checkpoint-committed block headers**
   before it is allowed to influence consensus state; and
2. a **final note-commitment frontier** at the checkpoint last checkpoint height, so post-checkpoint
   semantic verification resumes from a correct frontier.

This is **one fast verified path with its data source factored out behind a seam**, not a
new consensus mode. Every supplied root is verified before commit; a node that cannot obtain
or verify a root falls back to the legacy recompute, bit-identical to today.

## 2. Scope and non-goals

- **In scope:** the consensus-critical commit path (verify-before-commit, the frozen-frontier
  failure policy, the checkpoint last checkpoint height), the header-sync transport that carries
  roots in-band, the provisional-root persistence and serving read path, and the persistent
  fast-synced database format.
- **Not a consensus change.** There are exactly two enduring code paths: the standard local
  tree rebuild (legacy) and the fast verified path. Which one runs is config-driven by
  `consensus.checkpoint_sync` plus the rollout fast-sync knob
  (`consensus.vct_fast_sync`; §4.4); the `state.storage_mode` axis (Archive vs. Pruned)
  is orthogonal — it controls raw-tx/index pruning, not the tree path, so both storage modes
  use the fast path under checkpoint sync unless fast sync is disabled. The network `PeerSource` and
  crate-local test fixtures are _sources_ behind one seam (§5.3) — not modes.
- **No new cryptography.** Verification reuses the existing consensus checks
  (`block_commitment_is_valid_for_chain_history`, `HistoryTree::push`); see §6.
- **Out of scope for the fast lane:** historical tree/subtree RPCs (`z_gettreestate`,
  `GetSubtreeRoots`) below the last checkpoint height. A fast-synced node deliberately never built the
  per-height trees those need; they return a typed archive-mode error below the last checkpoint height and
  are restored only by the archive follower (§12, increments 7–8).

## 3. Background: the cost being eliminated

On checkpoint sync, header and PoW validity are already attested by the checkpoint list, so
the committer's remaining per-block work is dominated by advancing the Sapling and Orchard
note-commitment trees (`update_trees_parallel`) to recompute each block's treestate root.
The roots themselves are small and, from Heartwood onward, are **already committed to by the
block headers** via the ZIP-221 ChainHistory MMR: a block's header commitment binds the
history tree as of its parent, and each history-tree leaf is built from the block body plus
that block's Sapling/Orchard roots.

That is the lever: if a node is _handed_ the per-block roots, it can fold them straight into
the anchor set and history MMR and **confirm them against the headers it already trusts**,
skipping the frontier recompute entirely — without weakening any consensus check.

## 4. Design decisions

### 4.1 Roots travel on the wire; the frontier is embedded

The fast path needs two things, and they are sourced differently:

- **Per-block roots travel over the network**, carried in-band on the header-sync `Headers`
  message (§4.2, §5.4). `BlockCommitmentRoots { height, sapling_root, orchard_root,
  ironwood_root, .. }` (§5.1) is the wire payload.
- **The final frontier is embedded in the binary** (§5.2), refreshed per release like a
  checkpoint, _not_ sent on the wire. There is no `GetFinalFrontiers`/`FinalFrontiers` message
  and no frontier-serving path to attack or keep available.

### 4.2 Roots ride the header-sync message

Commitment roots are header-adjacent verified metadata, not body data: tiny, verified against
the header chain, servable only by a node holding the validated headers, and needed _buffered
ahead of_ the committer. So they are **carried in-band on the header-sync `Headers` message**
rather than over a separate stream. `GetHeaders` gains a `want_tree_aux_roots` flag, and a
`Headers` response carries an **all-or-nothing** `tree_aux_roots` vector parallel to `headers`
(§5.4). The same `Headers` response also carries a `body_sizes` vector, one advisory
serialized-body-size hint per header. These size hints are not commitment roots and are used
only to schedule block downloads (§5.4). The header-sync stream version is bumped (2 → 4) for
the new field.

Header sync sets `want_tree_aux_roots` on all of its range requests — the finalized
(checkpoint-verified) ranges below the last checkpoint and the non-finalized forward range
alike. The wire rejects roots a request opted out of, a root count that does not match the
header count, and per-height misalignment as `MalformedMessage` (§8.1). When a header range
commits via `CommitHeaderRange`, its roots are **persisted into the
`commitment_roots_by_height` column family ahead of body commit** (§5.3). Only roots below
the last checkpoint are ever _consumed_ by the committer, and only after verify-before-commit
(§6); roots for header-ahead heights above it are provisional serving data only (§9). The committer then reads them per height through the `PeerSource` seam.
The same header commit stores non-zero advertised body-size hints in
`zakura_header_body_size_by_height`, so block sync can later request realistic ranges even
before the corresponding bodies are committed. Headers, body-size hints, and roots arrive
together, so a range's root coverage is known before any of its roots can trigger the fast path.

The one coupling to bodies: verifying a root via the ZIP-221 MMR leaf needs the block's
tx-counts (from the body), so roots are **consumed** at commit time with bodies even though they
are **delivered** early with headers.

### 4.3 Roots follow the header-sync window

Because roots ride the header-sync `Headers` message, they are fetched exactly where header sync
already is — for the finalized ranges between the verified tip and the last checkpoint height —
with no separate fetch cursor, fetch-ahead cap, or eviction watermark to manage. The committer
only ever looks up a root for a block it is about to commit, and persisted provisional roots are
naturally bounded above by the header tip and settled below it: each provisional root is
**replaced by the verified serving-index row when its block body commits** (the same atomic
batch deletes the provisional entry and writes the committed row), and header-store rollback
also trims provisional roots above the rollback target. Advertised body-size hints follow the same header-store
lifecycle: header reorgs and rollbacks drop stale hints, and committed block sizes take
precedence once the corresponding body is durable.

### 4.4 Mode selection: fast under checkpoint sync

The fast-vs-legacy choice is driven by user-facing config, not by env vars. The axes are
`consensus.checkpoint_sync` (full checkpoint trust), `consensus.vct_fast_sync` (initial
rollout fast-sync knob for VCT fast sync), and `state.storage_mode` (Archive vs. Pruned, an
orthogonal pruning axis). The resulting modes:

| Mode | Config | Tree behavior |
| --- | --- | --- |
| **Archive** (default) | `consensus.checkpoint_sync = true`, `consensus.vct_fast_sync = true`, `storage_mode = archive` | Fast — verified roots folded in, recompute skipped. Unpruned (raw tx + indexes kept). No per-height tree history below the last checkpoint height _for now_ (§7, §10). |
| **Pruning** | `consensus.checkpoint_sync = true`, `consensus.vct_fast_sync = true`, `storage_mode.pruned` | Fast — same as Archive, **plus** raw-tx/index pruning outside the retention window. |
| **Force-disabled VCT** | `consensus.checkpoint_sync = true`, `consensus.vct_fast_sync = false` (any storage mode) | Legacy — keeps checkpoint sync enabled but fully reconstructs the Sapling/Orchard/Ironwood trees per block. |
| **Checkpoint sync disabled** | `consensus.checkpoint_sync = false` (any storage mode) | Legacy — fully reconstructs the Sapling/Orchard/Ironwood trees per block, using only mandatory checkpoints. |

Gating fast on `checkpoint_sync` is also a correctness precondition: the embedded last checkpoint height
frontier is pinned to the network's **full** max checkpoint height (§5.2), which only applies
when `checkpoint_sync = true` (with it `false`, the effective max checkpoint drops to the
Canopy mandatory checkpoint, so there is no valid last checkpoint height to resume from). zakurad mirrors
`consensus.checkpoint_sync` into the state config at startup
(`state_config.checkpoint_sync`), so the state makes the decision without depending on
`zakura-consensus`.

In the config file, `consensus.vct_fast_sync` is tri-state: unset (the default) means enabled,
and the generated default config does not write the key, so configs stay readable by older
zakurad versions. Explicitly setting `vct_fast_sync = true` together with
`checkpoint_sync = false` is rejected at zakurad startup as a contradiction; leaving it unset
with checkpoint sync disabled is fine (the node runs legacy either way), so pre-VCT configs
that disable checkpoint sync keep working unchanged.

Precedence is resolved by a pure, unit-tested `select_source_mode` (no process env, no embedded
files in the decision — `consensus.checkpoint_sync`, `consensus.vct_fast_sync`, and the
embedded-frontier presence are passed in as plain inputs):

1. `consensus.checkpoint_sync = false`, `consensus.vct_fast_sync = false`, or a network
   with **no embedded frontier** → **legacy** (no VCT state, zero overhead);
2. else → **peer** (the default under checkpoint sync where embedded frontiers exist).

The earlier file-backed checkpoint/fixture root source (`VCT_FAST`/`VCT_FIXTURE`) and capture
mode (`VCT_CAPTURE`) were transient integration scaffolding before peer delivery existed and
have been removed. `VCT_REGTEST_FRONTIER` remains as a Regtest final-frontier test hook.
`consensus.vct_fast_sync = false` is the supported user-facing way to force the legacy
committer without disabling checkpoint sync (the deliberate opt-out for the default-on path; see
the status note at the top of this document).

## 5. Payload, wire, and the source seam

### 5.1 Per-block commitment roots (the wire payload)

`zakura_chain::parallel::commitment_aux::BlockCommitmentRoots` holds `{ height, sapling_root,
orchard_root, ironwood_root, sapling_tx, orchard_tx, ironwood_tx, auth_data_root }` with
`ZcashSerialize`/`ZcashDeserialize`. It lives in `zakura-chain` so `zakura-network` and
`zakura-state` share one type without a dependency cycle. `orchard_root` is the empty/default
root below NU5, and `ironwood_root` is the empty/default root below `Nu6_3` (§6.1). The
deserializer treats `height` as an unvalidated `u32`: a wrong or out-of-range height simply
fails to match any local header during verification (§6), so it is harmless; malformed root
bytes are rejected by the root parsers.

The payload carries **no trust**: a recipient re-verifies every root against its own
checkpoint-committed headers (§6) before folding it in, so a forwarding/serving node is
exactly as trustworthy as an originating one.

### 5.2 The final frontier last checkpoint height (embedded)

Fast mode never advances the running Sapling/Orchard/Ironwood frontiers below the checkpoint,
so the real frontiers at the checkpoint must be supplied for the resume. `FinalFrontiers {
height, sapling, orchard, sprout, ironwood }` is embedded in the binary
(`zakura-state/src/service/finalized_state/vct/mainnet-frontier.bin`, via `include_bytes!`),
tied to the network's max checkpoint height (validated on load:
`embedded VCT final frontier height must match the network's max checkpoint height`). When the
Mainnet checkpoint list advances, this file is regenerated alongside the checkpoint artifacts
by the maintenance tool described in §16.

- **Sprout is reconstructed locally throughout fast sync.** JoinSplits remain valid in
  historical blocks after Sprout's introduction, and their anchors can be referenced by later
  transactions. VCT therefore appends each block's Sprout commitments after all retryable
  peer-root checks pass, writes every changed `root → frontier` entry, and verifies the
  locally reconstructed root against the embedded frontier at handoff.
- **Ironwood** is carried the same way as Sapling/Orchard, and is authenticated at the
  handoff (§7) against the supplied Ironwood root before it is written as the tip treestate.
  The on-disk byte format is backward compatible: the Ironwood tree is a 4th length-prefixed
  blob appended after Sprout, and bytes written before Ironwood existed (no 4th blob) parse
  with the Ironwood frontier defaulted to the empty tree — the existing embedded
  `mainnet-frontier.bin` needs no regeneration for this.
- **Subtree tips are not carried**: the resuming chain recomputes them from the frontier
  position.
- **Regtest** has no fixed checkpoint (its list is derived at runtime), so there is no constant
  to embed; for deterministic e2e testing the frontier is loaded from the file named by
  `VCT_REGTEST_FRONTIER` and validated against the Regtest checkpoint height. This is scoped to
  Regtest only — Mainnet always uses the embedded constant and never reads the env.

### 5.3 The `CommitmentRootSource` seam

`CommitmentRootSource` (`zakura-state/.../finalized_state/commitment_aux.rs`) abstracts _where_
the fast path's roots and last checkpoint height frontier come from. The committer (`VctState.source`) reads
through this one seam regardless of source:

```rust
fn vct_root(&self, height) -> Option<(sapling::Root, orchard::Root)>;
fn vct_last_checkpoint_height(&self) -> Option<block::Height>;
fn final_frontiers(&self) -> Option<&FinalFrontiers>;
fn invalidate(&self, height);              // drop a rejected root so a replacement can be re-delivered
fn evict_committed_through(&self, height); // drop roots for already-committed heights
```

Implementations:

- `PeerSource` — the production default, a **DB-backed reader** (`PeerSource::new(db,
  frontiers)`). Each `vct_root(height)` reads the provisional root for that height from the
  `commitment_roots_by_height` column family that header sync persisted (§4.2). The
  last checkpoint height frontier is held immutably from the embedded constant, so only roots come
  from the network. `invalidate` **deletes** a rejected root from that column family so the next
  read misses instead of re-reading the same rejected root forever; a verifiable replacement only
  arrives if the same header range is re-delivered (§8.1). The earlier in-memory
  cache variant and its `PeerSourceWriter` are removed; proptests fill roots by writing to an
  ephemeral database through the same header-sync persistence path production uses.
- `FixtureSource` — a crate-local `#[cfg(test)]` source over the same height→roots map, used only
  to isolate committer behavior and DB-produced payload round trips without networking.

The **producer** half (`produce_block_roots(db, range)` / `produce_final_frontiers(db,
height)`) derives the same payload from a database's per-height trees — the serving read path
(§9), minus the network. The producer→`PeerSource`→committer round-trip proving producer and
consumer agree is `vct_db_produced_payload_round_trips`.

Because the production `PeerSource` reads straight from the database, peer mode no longer
exports a root-writer handle. Header sync writes provisional roots through `CommitHeaderRange`
on the normal state write path, and the committer reads them back through the same database. The
old per-state `TreeAuxRootsWriter` / `PeerSourceHandle` / targeted-refetch signal are removed.
The persisted roots store no peer identity; peer accountability for bad roots is the header-sync
reactor's misbehavior reporting (§8.1), preserving the `zakura-state` / `zakura-network` crate
boundary.

### 5.4 Roots on the header-sync message

There is no separate roots stream. The header-sync `HeaderSyncMessage` carries roots in two
places (`zakura-network/src/zakura/header_sync/wire.rs`):

- `GetHeaders { start_height, count, want_tree_aux_roots }` — header sync sets
  `want_tree_aux_roots` on its range requests (finalized and non-finalized alike; only roots
  below the last checkpoint are ever consumed by the committer, §4.2).
- `Headers { headers, body_sizes, tree_aux_roots }` — `tree_aux_roots` is **all-or-nothing**:
  either empty, or exactly one `BlockCommitmentRoots` per header, in ascending height order
  aligned to `start_height`. A one-byte `has_roots` marker precedes the roots on the wire.
  `body_sizes` is always parallel to `headers`; each entry is an advisory serialized body size,
  with `0` meaning unknown.

Body-size hints are scheduling data, not consensus data. `CommitHeaderRange` persists non-zero
advertised hints for header-ahead heights, preserving the maximum non-zero hint for the same
header and clearing hints when a competing higher-work header chain replaces the range.
`ReadRequest::BlockSizeHints` returns the durable committed block size when available, otherwise
the advertised hint, otherwise `None`. Block sync uses those hints to pack contiguous
`GetBlocks` ranges by estimated bytes and to set receive-path size-mismatch tolerance; the
downloaded body still has to hash to the committed header, and the actual serialized size is
settled when the body is received.

Wire and DoS bounds:

- The `body_sizes` count must exactly match the header count (`BodySizeCountMismatch`); there is
  no independent untrusted body-size length to preallocate from.
- The byte budget that bounds a `Headers` message accounts for the per-header root
  (`HEADER_SYNC_BLOCK_COMMITMENT_ROOTS_BYTES = 4 + 32·3 + 8·3 + 32` — height, the three
  note-commitment roots, the three shielded tx-counts, and the auth-data root), and the static
  range-fits-budget assertion includes it, so requesting roots reduces the per-message header
  count accordingly (`inbound_get_headers_count_limit(.., want_tree_aux_roots)`).
- Decoding validates: the `has_roots` marker must be 0 or 1 (`InvalidBoolMarker`); roots are
  present only when the request wanted them (`UnrequestedTreeAuxRoots`); the root count equals
  the header count (`TreeAuxRootCountMismatch`); and the root vector is preallocated only with
  the already-bounded header count, never an independent untrusted length.
- The reactor additionally checks each root's height is `start_height + offset`
  (`TreeAuxRootHeightMismatch` / `validate_tree_aux_root_heights`) before the roots reach
  state. State re-checks the count and alignment invariants in `CommitHeaderRange`
  (`prepare_header_range_batch_with_roots`) as defense in depth, and never
  writes peer-supplied roots for a height whose body is already committed — a re-delivered header range over committed heights cannot overwrite the verified serving-index rows.

`BlockCommitmentRoots` still carries no trust: a recipient re-verifies every root against its
own checkpoint-committed headers (§6) before folding it in, so a forwarding/serving node is
exactly as trustworthy as an originating one.

## 6. Verification — verify-before-commit

Before a supplied root influences consensus state, the committer confirms it against the
node's own checkpoint-committed headers. The logic lives in
`finalized_state/commitment_aux_verify.rs` and reuses the existing consensus check
`block_commitment_is_valid_for_chain_history` plus `HistoryTree::push` — **no new crypto**.

A block's header commitment binds the history tree _as of its parent_, so the root supplied
for height `H` is folded into a candidate history tree and confirmed when `H+1`'s commitment
is checked against that candidate. A wrong root makes that check fail and the block is
**rejected, not recomputed** (§8). The standalone `verify_commitment_roots` returns the first
offending height; over `[start..=end]` it confirms `[start..=end-1]`, and `end+1` confirms
`end`.

### 6.1 Direct header checks below Heartwood, NU5, and Nu6_3

The ZIP-221 MMR does not authenticate everything, so three gaps are closed by direct comparison
(no one-block lag — a wrong root is rejected at the block's own commit):

- **Sapling below Heartwood** (`verify_supplied_sapling_root_below_heartwood`): there is no MMR
  yet, so the header's `FinalSaplingRoot` is compared directly; pre-Sapling the root must be
  the empty-tree root. At/above Heartwood the MMR path authenticates it.
- **Orchard below NU5** (`verify_supplied_orchard_root_below_nu5`): the V1 history leaf
  (Heartwood..Canopy) _ignores_ the Orchard root and there is no MMR below Heartwood, so no
  header commits to an Orchard root below NU5 — yet the fast path folds the supplied Orchard
  root into the anchor set for every block. The Orchard tree is provably empty there (no
  Orchard actions are allowed), so the supplied root is pinned to the empty-tree root. Without
  this, an untrusted source could inject an Orchard anchor the legacy recompute never produces,
  breaking the §11 trust boundary and consensus equivalence. This was a real hole, masked only
  while the source was a trusted fixture; the in-flight peer source would have armed it
  (fix in commit #190).
- **Ironwood below Nu6_3** (`verify_supplied_ironwood_root_below_nu6_3`): `Nu6_3` is the first
  upgrade whose history leaf (`IronwoodOnward`/V3) commits to an Ironwood root; below it, no
  header commits to one and the Ironwood tree is provably empty (no Ironwood actions are
  allowed), so the supplied root is pinned to the empty-tree root — the same pattern as the
  below-NU5 Orchard pin, and closing the same class of hole. At/above `Nu6_3` the MMR path
  authenticates it.

### 6.2 The one-block lag and the dedup

A block's own commitment check `C(X, T_{X-1})` is the _identical_ computation the previous
fast block already ran as its look-ahead one commit earlier. The committer caches the
look-ahead result as `(next_height, next_hash, next_auth_data_root)` and skips a block's own
check when the prior look-ahead validated exactly it. Below NU5 the auth-data-root component is
unused because it is not an input to the header commitment. At NU5 and later it binds a
header-only successor witness to the later body, so a same-header body with different
authorizing data cannot reuse the earlier prevalidation. Steady state drops from two
commitment checks per block to one (legacy parity) while still attesting every root before it
is persisted. A non-last checkpoint height fast block with no buffered successor is deferred by the write
worker until the successor arrives; the checkpoint last checkpoint height is the only no-successor fast commit
because the embedded final frontier independently authenticates that height's roots. The cache
is cleared on last checkpoint height and on legacy blocks. The dedup is observable
(`state.vct.prevalidated.block.count`) so it cannot silently regress.

### 6.3 The auth-data-root cache lock

The NU5+ commitment check trusts a precomputed `AuthDataRoot` carried on
`CheckpointVerifiedBlock` (so the single-threaded committer does not recompute it). Every
cached value is computed from the block by the constructors, so it is correct _by
construction_ — but the public API previously let it be desynced after construction
(`pub auth_data_root`, `DerefMut`, both re-exported). A holder could swap the block while
keeping a stale root, and a header matching the stale root would finalize a block without
proving the header binds the block's actual authorizing data. The (block, auth-data-root) pair
is locked together: `CheckpointVerifiedBlock` drops `DerefMut`, and the checkpoint verifier
can only fill the optional cache through `with_precomputed_auth_data_root`, which computes the
value from that same wrapped block rather than accepting arbitrary bytes.

## 7. The fast commit path and checkpoint last checkpoint height

The commit-path hook lives in `finalized_state.rs`; everything about _where data comes from_
lives in the `vct` and `commitment_aux` submodules, so the commit path holds only the last checkpoint height
logic. For a checkpoint-verified block at `height`:

1. **Fast-root lookup.** `vct.vct_root(height)` returns the supplied `(sapling, orchard,
   ironwood)` roots, or `None`.
2. **If supplied (fast path):**
   - run the own-commitment check unless the dedup (§6.2) already validated it;
   - apply the direct below-Heartwood/below-NU5/below-Nu6_3 checks (§6.1);
   - build a candidate history tree with the roots folded in (`HistoryTree::push`);
   - **verify-before-commit:** either check the buffered successor's commitment against the
     candidate (the one-block-lag confirmation) and cache
     `(height+1, next_hash, next_auth_data_root)` as
     pre-validated, or, at the checkpoint last checkpoint height only, verify the embedded final
     frontiers — including Ironwood — against this height's roots; a failure means _this_
     height's root is bad → reject and evict (§8);
   - after all retryable root/successor checks pass, append this block's Sprout commitments
     locally, so retrying a deferred block cannot double-append them;
   - fold the roots (Sapling, Orchard, and Ironwood) into their anchor sets, skip the modern frontier
     recompute, and **freeze** the note-commitment frontier (`vct_frontier_frozen = true`) for
     non-last checkpoint height fast blocks.
3. **Checkpoint last checkpoint height** (when `height` is the last checkpoint height): verify the embedded
   Sapling/Orchard/Ironwood frontiers against this block's verified roots (`frontier.root() ==
   verified root` for each pool; collision resistance makes each root a binding commitment to
   its frontier), write them as the real tip treestate via the normal write path, and
   **unfreeze** — heights at/above the last checkpoint height resume legacy recompute from a
   correct frontier. The embedded Sprout root must equal the locally reconstructed root, but
   the local Sprout frontier is retained rather than replaced.
4. **If not supplied:** §8.

The write worker enforces the successor side of this contract before calling the committer: if
a queued checkpoint block would take the fast path, is not the last checkpoint height, and has no
buffered successor yet, it is parked locally and retried when another checkpoint block arrives.
It is not reported through the invalid-block reset path, because no verification failure has
occurred — the needed `H+1` witness is merely not buffered yet.

**Persistent fast-synced databases.** A persistent fast sync marks the database with a
`fast_sync_metadata` column family recording the last checkpoint height (DB format minor bump to
**27.3.0**, consolidated with the roots serving index and history-tree repair). This is a sibling
to `pruning_metadata`, not a reuse — pruning drops tx bytes and keeps trees, fast-sync drops the
per-height trees; a DB can be both. Because fast sync deletes nothing, a **completed** fast-synced
DB (tip at/above the last checkpoint height) **reopens in any storage mode** — a reopen loses no servable data,
and `consensus.vct_fast_sync = false` or `consensus.checkpoint_sync = false` simply resumes
the legacy recompute from the real tip frontier.

The one reopen that _is_ refused is an **interrupted** fast sync (frozen frontier, tip below the
last checkpoint height) reopened with the fast path disabled (legacy mode —
`consensus.vct_fast_sync = false`, `consensus.checkpoint_sync = false`, or no embedded
frontier). The on-disk frontier is stale and no source can supply the verified roots, so the
fail-closed policy (§8) would refuse every below-last checkpoint height block forever. The open guard refuses
with a clear recovery path (finish the fast sync under `consensus.checkpoint_sync = true` and
`consensus.vct_fast_sync = true`, or re-sync from genesis) instead of stalling silently.
Guards: per-height tree reads return `None` below the last checkpoint height (before the backward search, so no
stale tree and no panic); `z_gettreestate` returns a typed archive-mode error below the last checkpoint height;
genesis-root and subtree format-validity checks skip fast-synced DBs.

## 8. Failure policy — fail closed on a frozen frontier

While the frontier is frozen (a fast sync has folded roots but the last checkpoint height has not yet written
the real frontier), the on-disk frontier is **stale**. A legacy recompute in that window would
extend the stale frontier and fold a _wrong_ root into the MMR — corrupting consensus state.
So the committer **fails closed** rather than falling back to recompute (commit #211):

- A supplied root that fails _any_ verification step is **evicted** from its source (so the
  same rejected root is never re-read) and the commit is **refused** with the typed,
  **retryable** `VctSuppliedRootUnavailable { height }` error — not retried against the same
  rejected root forever, and not recomputed locally.
- A frozen-frontier height with **no** valid supplied root (never delivered, or just evicted)
  refuses with the same retryable error and leaves the database untouched. The block commits
  only if a verifiable root arrives via a re-delivery of its header range (§8.1).
- A non-last checkpoint height fast block with a valid supplied root but **no buffered successor** is not a
  root failure: the write worker defers it locally until `H+1` is available to authenticate
  the candidate history tree. If a direct committer caller bypasses that deferral, the
  committer still fails closed before writing.
- The frozen flag is **seeded from the durable fast-sync marker on open**, not just tracked
  in-session: a fast sync interrupted by a restart (frozen frontier persisted, tip below the
  last checkpoint height) still refuses on the first post-restart height with a missing root. The frozen
  region is exactly `tip < last_checkpoint_height` (the last checkpoint height itself carries the real frontier).

Outside the frozen window (legacy), a missing root is
simply the ordinary legacy recompute — bit-identical to today. Inside the frozen window, a
missing root parks the current checkpoint block and retries the same commit **in place** —
**without resetting the block queue**. Nothing re-requests an individual root: the retry is
satisfied only if the same header range is re-delivered while still in flight (header sync
fans each range across several peers, so another peer's response for that range may still
land and re-persist its roots). If no re-delivery fills the hole, the node stays parked
fail-closed at that height (§8.1). A peer-supplied root that has no buffered successor to
confirm it against the header
chain (the one-block lag) is likewise **deferred, not committed on faith**: an untrusted tip
root is rejected before it is persisted, rather than one block too late (when it would be
irreversibly on disk and could wedge the sync). Test-only trusted local sources are exempt and
commit a tip root on the in-arrears check. This is the safety contract: **a bad, slow, or
withholding peer cannot publish a root that influences state without authentication; after
freeze, a later bad or missing re-delivery never writes wrong state and does not reset the block
queue for root availability.** A height that stays stuck on a retryable stall past a threshold escalates
to an error-level log and the `state.vct.root.stalled.height` gauge, so a genuinely unservable
root surfaces loudly instead of a silent stall. Because roots are delivered in-band with the
finalized header range and persisted before commit (§4.2), the common case is that the frozen
window is never entered without its roots in hand. Counters:
`state.vct.root.rejected.count` (evicted after failing verification),
`state.vct.root.unavailable.count` (frozen-frontier hole refused),
`state.vct.root.await_successor.count` (deferred for a missing successor),
`state.vct.root.retry.count` (park-and-retry attempts), and the
`state.vct.root.stalled.height` gauge (raised once a height is stuck past the warn threshold).

### 8.1 Adversarial peer handling

With roots carried in-band on header sync, there is no separate `tree_aux` driver and no bespoke
provenance/cooldown/demotion/hedging policy. Bad roots are handled in two layers:

- **At the wire/reactor boundary**, a peer that sends a malformed root set — wrong count,
  misaligned height, roots on a non-finalized range, roots that were not requested, or an
  invalid marker byte — is reported through header sync's existing misbehavior path
  (`report_misbehavior(.., MalformedMessage)`), and the range is retried. None of those roots
  reach state.
- **At verify-before-commit**, a well-formed but _wrong_ root fails authentication against the
  header commitment (§6). The committer evicts it (`PeerSource::invalidate` **deletes** it from
  `commitment_roots_by_height`) and refuses the commit with the retryable
  `VctSuppliedRootUnavailable` error (§8). Header sync does **not** re-request that range —
  its headers are already committed and covered — so the hole is filled only if another
  response for the same range is still in flight from the request fanout; the block then
  commits in place, without resetting the block queue.

Safety is unconditional, liveness is not: a lying peer can never corrupt state, but a
well-formed wrong root (or a rootless serve) that ends up as the settled delivery for its
height halts the fast sync at that height — fail-closed, surfaced by the §8 stall
metrics/logs, and persisting across restarts (header sync resumes from the durable header tip
and does not re-fetch committed ranges). This is a deliberate simplicity trade-off in the
current increment: there is no roots-specific refetch, cooldown, or provenance machinery.
Restoring liveness after a settled bad root requires re-delivering the affected finalized
range (a possible follow-up mechanism) or a fresh sync. Peer accountability rides header
sync's general misbehavior scoring rather than a roots-specific cooldown table, so the
committer still attributes nothing to peers itself and `zakura-state` keeps no dependency on
`zakura-network` peer types.

## 9. The serving read path (`BlockRoots`)

A node serves roots from local state via `ReadRequest::BlockRoots { start_height, count }` →
`ReadResponse::BlockRoots(Vec<BlockCommitmentRoots>)`. The read handler:

- clamps the range to the best **header** tip (which may run ahead of committed bodies);
- serves **committed** verified roots first, from the compact `commitment_roots_by_height` index
  (so a fast-synced node lacking historical per-height trees can still serve), falling back to
  `produce_block_roots` over per-height trees only on a pre-index archive database;
- then appends **provisional** header-ahead roots from `commitment_roots_by_height`
  for the contiguous heights that have headers but no committed body yet — committed roots win on
  any overlap because they are already verified;
- returns an empty vec for out-of-range/empty requests.

When this read backs a header-sync serve, the header-sync driver attaches roots only when it has
a **complete aligned set** for the served header range
(`tree_aux_roots_for_served_header_range`). A partial set is served as rootless headers, never as
a partial root vector — which the all-or-nothing wire format (§5.4) would reject anyway. The
driver maps read errors and wrong responses to a rootless serve, never wrong data.

## 10. Serving availability (open design concern)

Fast-synced nodes serve roots from `commitment_roots_by_height`, while older archive-produced
nodes can still derive roots from per-height trees. This keeps the root-serving fleet available
as more nodes fast-sync. A client that finds no serving peer degrades to legacy speed before
freeze; in the frozen window it parks fail-closed on the missing roots (§8) rather than
corrupting state. Two mechanisms address it, in order of cost:

- **Roots-index CF (lightweight, preferred).** A fast node already verified every root it
  folded in. Persisting them into a compact column family (~160 bytes/block, ~550 MB for all of
  Mainnet before compression) lets it serve them without per-height trees, at near-zero extra
  cost. A background
  task can backfill missing lower ranges by fetching _roots_ (not bodies), so even a
  snapshot-started node becomes a full-range roots server cheaply. This is the targeted fix for
  the §10 serving-availability gap.
- **Indexing-follower resync (heavyweight, opt-in).** Rebuild the per-height trees off the
  consensus critical path (re-downloading bodies if pruned), turning a fast node into a full
  archive node. This pays back the cost fast-sync avoided, so it is the archive/RPC path
  (increments 7–8), not a default.

Protocol hygiene that reduces the failure surface meanwhile: header sync fans each range
request across several peers, so a peer that cannot serve roots and yields rootless headers
does not preclude another fanout response for the same range delivering the roots — though
once a range settles rootless, it is not re-requested (§8.1).
Serving provisional header-ahead roots in addition to committed ones (§9) widens the servable
range to the header tip without per-height trees.

## 11. Trust boundary and security

The trust boundary is sharp: **every peer-provided root must be authenticated against a header
commitment before it influences the anchor set or the history MMR.** Consequences:

- The wire payload (§5.1) and the source seam (§5.3) carry no trust; a serving/forwarding node
  is exactly as trustworthy as an originating one.
- The below-NU5 Orchard pin and below-Heartwood Sapling check (§6.1) close the only ranges the
  MMR cannot vouch for. Skipping either would let an untrusted source inject an anchor the
  legacy recompute never produces — a consensus-equivalence break, not just a slowdown.
- The frozen-frontier fail-closed policy (§8) means a hostile root never corrupts state: it is
  deleted and refused. A malformed root set is rejected at the header-sync reactor before it
  reaches state and is scored through header sync's misbehavior path; a well-formed wrong root
  is evicted on verify-before-commit and the commit stays parked (§8.1). The trade-off is
  availability, not integrity: a settled bad root stalls the fast sync at that height instead
  of writing wrong state (§8.1).
- DoS bounds on the header-sync roots fields (§5.4) — the all-or-nothing count check, the
  per-height alignment check, the bounded preallocation, and the message byte budget — protect
  the serving and client paths from unbounded memory growth.
- The auth-data-root cache lock (§6.3) closes a cross-crate API hole that could otherwise
  finalize a block without binding its authorizing data.

## 12. Increment roadmap

- **Increments 0–5 (done):** the fast path proven end-to-end from a local test source — the
  source seam, verify-before-commit against headers, the frontier-recompute skip, and the
  verified checkpoint last checkpoint height with persistent fast-synced databases.
- **Increment 6a — peer source: fetch + serve (happy-path POC).** The first peer transport for
  roots: originally a standalone roots-only `tree_aux` stream with its own serving side, driver,
  and in-memory `PeerSource` cache — the first point at which real nodes obtained roots over the
  network.
- **Increment 6b — adversarial peer policy.** A `zakurad` driver recorded height→peer provenance
  and ran a roots-specific cooldown/demotion/disconnect policy over the `tree_aux` stream.
- **Increment 6c — fold roots into header sync (current).** The standalone `tree_aux` stream,
  its driver, in-memory cache writer, and bespoke peer policy are **removed**. Roots now ride the
  header-sync `Headers` message as all-or-nothing metadata (§4.2, §5.4), are
  persisted provisionally to `commitment_roots_by_height` ahead of body commit, and
  are read back by a DB-backed `PeerSource`. Recovery from a bad/missing root is an in-place
  commit retry fed only by an in-flight fanout re-delivery of the same header range — roots
  are not individually re-requested, so a settled hole is a fail-closed stall (§8.1); peer
  accountability rides header sync's existing misbehavior scoring.
- **Increment 7 — indexing follower lane (archive only).** Relocate `tx_by_loc` + address
  indexes and the per-height trees + subtree CFs onto an async follower, so archive mode regains
  historical RPC without re-adding the frontier recompute to the consensus path.
- **Increment 8 — archive mode via the follower.** Run the full per-block recompute off the
  critical path to restore `z_gettreestate` / `GetSubtreeRoots`, while the consensus lane uses
  verified roots.
- **Increment 9 — spec / ZIP.** Publish the cross-client payload schema and verification
  algorithm so other clients (zcashd, zaino, …) can serve and verify identically.

### Supporting fix: Zakura header-store rollback

Independent of the fast path but on the same branch, `rollback_finalized_state` now also rolls
back the Zakura header store (`delete_zakura_headers_above`). The header store races ahead of
the body chain and is keyed independently; leaving it untouched on a rollback kept a
`BestHeaderTip` above the new body tip, which stalled body sync (the contiguous floor body was
never requestable) until the 5-minute timeout fell back to legacy ChainSync.
(Commits #198 and #202.)

## 13. Observability

Live commit-path counters distinguish the fast and legacy paths and the failure modes:

| Metric | Meaning |
| --- | --- |
| `state.vct.fast.block.count` | block folded supplied roots, skipped the recompute |
| `state.vct.legacy.block.count` | block recomputed the frontier (`consensus.vct_fast_sync = false`, `consensus.checkpoint_sync = false`, or fell back outside the frozen window) |
| `state.vct.prevalidated.block.count` | dedup sub-case: the previous fast block's look-ahead already validated this header |
| `state.vct.root.rejected.count` | supplied root failed verification and was deleted so it is never re-read |
| `state.vct.root.unavailable.count` | frozen-frontier height with no valid root; commit refused (retryable) |
| `state.vct.root.retry.count` | park-and-retry attempts on a retryable VCT root stall |
| `state.vct.fast_path.hit` | a finalized commit consumed header-carried roots to skip the recompute |
| `state.vct.fast_path.miss` | a finalized commit did not take the fast path |
| `state.vct.root.stalled.height` (gauge) | a height stuck on a retryable stall past the warn threshold |

The header-sync `headers_received` / `headers_served` / commit-state trace rows also carry
`want_tree_aux_roots` and `tree_aux_roots_len`, so root delivery is visible per range. The
fast-vs-legacy ratio (`state.vct.fast_path.hit` vs `miss`) is the signal an integration test
asserts to prove roots actually came over the wire rather than a silent legacy sync.

## 14. Testing strategy

- **Unit:** the `BlockCommitmentRoots` wire round-trip; the header-sync `Headers`/`GetHeaders`
  round-trip carrying roots, plus the all-or-nothing / count-mismatch / height-misalignment /
  invalid-marker / unrequested-roots rejections
  (`decode_rejects_tree_aux_roots_when_not_requested`) and the byte-budget clamp with
  roots requested; `select_source_mode` precedence (`consensus.vct_fast_sync = false` or
  `consensus.checkpoint_sync = false` ⇒ legacy regardless of storage mode or embedded frontier;
  checkpoint sync + enabled VCT + embedded frontier ⇒ peer); a completed fast-synced DB reopens
  in archive mode (`reopening_fast_synced_database_in_archive_mode_succeeds`) while an interrupted
  one reopened with the fast path off is refused
  (`reopening_interrupted_fast_sync_without_a_root_source_panics`); the below-NU5 Orchard pin and
  below-Heartwood Sapling check; the `verify_commitment_roots` lag (wrong root rejected at H+1);
  the dedup (second consecutive fast block skips its check; a stale cache entry does not cause a
  false skip); the all-or-nothing serving helper
  (`served_header_tree_aux_roots_require_complete_coverage`); provisional-root persistence and
  cleanup on body commit (`write_block_deletes_matching_provisional_zakura_roots`);
  `PeerSource::invalidate` eviction; and the in-process producer → `PeerSource` → committer
  byte-identical equivalence.
- **Frozen-frontier proptests:** a frozen-frontier hole returns the retryable
  `VctSuppliedRootUnavailable` and leaves the DB untouched; a reopened committer (frozen marker
  persisted) still refuses on the first post-restart missing root.
- **Header-sync transport:** the header-sync driver tests (`zakura_header_sync_driver_tests`)
  exercise serving and committing finalized ranges with roots end-to-end, including the
  all-or-nothing serving helper (roots attached only on complete coverage, otherwise rootless
  headers) and routing received roots into `CommitHeaderRange`.
- **State persistence:** `CommitHeaderRange` persists provisional roots into
  `commitment_roots_by_height`, rejects count/height mismatches, refuses to overwrite the
  verified row of an already-committed height
  (`header_range_roots_do_not_overwrite_committed_serving_index_rows`), replaces a provisional
  root with the verified row when its body commits
  (`write_block_replaces_matching_provisional_zakura_roots_with_verified_row`), and trims
  provisional roots above a header-store rollback target.
- **Real-data manual runs (`#[ignore]`, env-gated):** `verifies_real_nu5_range_over_synced_forks`
  verifies the real NU5/V2 range against synced archive forks (corrupted root rejected at H+1).
- **Headline end-to-end (manual, follow-up):** a fresh node fast-syncing
  `verified_tip + 1` → checkpoint from a peer and reaching byte-identical consensus state, with
  `state.vct.fast.block.count > 0`. The full two-process Regtest docker e2e is unblocked by the
  `VCT_REGTEST_FRONTIER` override but crosses crate boundaries that cannot be wired into CI
  without a dependency cycle, so it stays manual.

## 15. File map

| Area | File |
| --- | --- |
| Wire payload (`BlockCommitmentRoots`) | `zakura-chain/src/parallel/commitment_aux.rs` |
| Source seam, `PeerSource`, producers, bulk root invalidation | `zakura-state/src/service/finalized_state/commitment_aux.rs` |
| Verify-before-commit logic | `zakura-state/src/service/finalized_state/commitment_aux_verify.rs` |
| Embedded frontier plumbing, `select_source_mode`, counters | `zakura-state/src/service/finalized_state/vct.rs` |
| `checkpoint_sync` mirror field (mode input) | `zakura-state/src/config.rs`; set in `zakurad/src/commands/start.rs` |
| Embedded Mainnet frontier | `zakura-state/src/service/finalized_state/vct/mainnet-frontier.bin` |
| Commit-path hook, last checkpoint height, frozen-frontier policy | `zakura-state/src/service/finalized_state.rs` |
| `BlockRoots` serving read (committed + provisional) | `zakura-state/src/service.rs` |
| Provisional roots in `commitment_roots_by_height`, persistence, body-commit/rollback cleanup | `zakura-state/src/service/finalized_state/zakura_db/block.rs`, `.../rollback.rs` |
| `CommitHeaderRange` with roots, fast-path hit/miss metrics | `zakura-state/src/service/write.rs` |
| Header-sync wire (`GetHeaders`/`Headers` roots, markers, byte budget) | `zakura-network/src/zakura/header_sync/wire.rs` |
| Header-sync root validation (count, height alignment, markers) | `zakura-network/src/zakura/header_sync/validation.rs`, `.../error.rs` |
| Header-sync reactor (request/serve/receive roots, misbehavior) | `zakura-network/src/zakura/header_sync/reactor.rs` |
| Header-sync driver: serve `BlockRoots`, all-or-nothing helper, route received roots | `zakurad/src/commands/start/zakura/header_sync_driver.rs` |

## 16. Frontier regeneration tool

The embedded Mainnet frontier is a release artifact coupled to the last Mainnet checkpoint.
Whenever the checkpoint list's max height changes, the matching
`zakura-state/src/service/finalized_state/vct/mainnet-frontier.bin` must be regenerated from a
synced Zebra state at that same height.

This belongs in the checkpoint-maintenance flow rather than in node runtime configuration. The
`zakura-checkpoints` utility runs against a synced node and produces the `HEIGHT HASH`
checkpoint artifact consumed by `.github/workflows/checkpoint-update.yml`. It also has an
explicit Mainnet frontier-artifact output:

```text
zakura-checkpoints \
  --addr 127.0.0.1:8232 \
  --last-checkpoint <old-height> \
  --mainnet-frontier-output /tmp/mainnet-frontier.bin \
  --state-cache-dir <synced-zakura-state-cache-dir> \
  --frontier-height auto
```

The checkpoint stdout format stays unchanged. The frontier is written only when
`--mainnet-frontier-output` is supplied, and status details go to stderr so the existing
checkpoint log scraper remains stable. `--frontier-height auto` means "use the final Mainnet
checkpoint height generated by this run"; an explicit height is useful for local validation and
debugging. `--state-cache-dir` is required whenever `--mainnet-frontier-output` is supplied.
With `--frontier-height auto`, the utility fails if the run did not emit any checkpoint above
genesis, because there is no updated last checkpoint height to pair with the frontier artifact.

The frontier generator must read Zebra's finalized state, not reconstruct trees from RPC block
data. Checkpoint generation only needs block hashes and sizes, but frontier generation needs the
exact Sapling, Orchard, Sprout, and Ironwood note-commitment trees. The utility therefore opens
Zebra state read-only and calls `zakura-state` helpers that:

- opens the finalized DB read-only from the supplied state cache directory;
- reads the Sapling, Orchard, and Ironwood trees at the requested height;
- reads the tip Sprout tree (Sprout is frozen far below modern checkpoints);
- serializes `FinalFrontiers { height, sapling, orchard, sprout, ironwood }` using the same byte
  format parsed by node startup: `height` as `u32` little-endian, followed by length-prefixed
  `IntoDisk` blobs for Sapling, Orchard, Sprout, and Ironwood. The Ironwood blob is a
  backward-compatible tail: bytes written before Ironwood existed (3 blobs, no trailing bytes
  after Sprout) still parse, with the Ironwood frontier defaulted to the empty tree;
- immediately validates the generated bytes by parsing them through the same height-checking
  path used for the embedded frontier (`produce_final_frontiers_bytes` followed by
  `validate_final_frontiers_bytes`).

The GCP checkpoint-generation workflow copies `/tmp/mainnet-frontier.bin` out of the Mainnet
checkpoint-generation container and uploads it as a separate artifact named
`generate-checkpoints-mainnet-frontier`. `checkpoint-update.yml` replaces the embedded frontier
only when it appends new Mainnet checkpoints, and fails closed if Mainnet checkpoints advance
but the frontier artifact is missing, empty, or has an embedded height that does not match the
updated checkpoint max height.

Local testing proves byte compatibility with the node loader:

- build a small legacy `FinalizedState` over a generated valid chain;
- produce frontier bytes from that DB at a chosen height;
- write the bytes to a temporary file;
- load the file through the same loader/parser path used by `VCT_REGTEST_FRONTIER` and the
  embedded Mainnet frontier;
- assert the parsed height matches, the parsed Sapling/Orchard/Sprout/Ironwood roots match the
  DB, and parsing with a different expected height fails.

That test is the compatibility contract: if the local tool writes bytes that pass this path, the
node will parse the artifact in the same way at startup.

The focused local checks are:

```text
cargo test -p zakura-state final_frontier
cargo test -p zakura-utils --features zakura-checkpoints
cargo test -p zakura --features zakura-checkpoints checkpoints
```

## 17. VCT Sprout-history repair artifact

### 17.1 Purpose and trust boundary

The original VCT fast path advanced Sprout locally but omitted the historical
`Sprout root → frontier` anchor entries. A later JoinSplit can spend one of those roots, so an
affected database must be repaired before it is used for validation. The repair artifact is a
canonical, Mainnet-only history of **only the blocks that change Sprout**; it is unrelated to
peer-delivered VCT roots and is never accepted from peers.

The SHA-256 digest in the artifact detects accidental or malicious byte changes, but does not
establish provenance. Provenance comes from release review and embedding the approved bytes in
the binary. The loader must never substitute downloaded, locally generated, or guessed bytes.
Each record is bound to its canonical block hash, and the header is bound to the canonical
handoff block hash. The terminal root is independently pinned to the separately embedded VCT
handoff frontier, so both artifacts must agree before replay.

### 17.2 Binary format (version 1)

All integers are little-endian. The fixed 115-byte header is:

| Bytes | Field |
| --- | --- |
| 0–7 | ASCII magic `ZKVCTSP1` |
| 8–9 | `u16` format version (`1`) |
| 10 | network tag (`1`, Mainnet) |
| 11–14 | `u32` handoff height |
| 15–46 | canonical handoff block hash |
| 47–50 | `u32` record count |
| 51–82 | 32-byte terminal Sprout root |
| 83–114 | SHA-256 digest of the remaining payload |

The payload contains exactly `record_count` records. Each record is `height_delta: u32`,
the canonical 32-byte block hash, `commitment_count: u16`, `commitment_count` 32-byte Sprout
commitments, and one 32-byte resulting Sprout root. The digest covers these canonical hashes.
Heights are delta-coded from an initial height of zero. The current implementation permits at
most 1M records and 65,535 commitments per record. Because no bytes using the earlier
unshipped layout were published, this corrected layout remains version 1 and has no compatibility
decoder.

### 17.3 Validation and generation

The decoder rejects a wrong magic, version, or network; truncation; digest mismatches; excess
counts; zero or overflowing/non-increasing height deltas; heights above the handoff; empty
records; record-root mismatches while replaying commitments from an empty Sprout tree; a
terminal-root mismatch; and trailing bytes. The generated artifact is authenticated against the
current build's own identity: its handoff must equal the embedded final frontier's height and its
terminal Sprout root must equal the embedded final frontier's Sprout root.

The offline generator opens a complete current-format Mainnet archive read-only, scans canonical
block bodies from genesis through the embedded handoff, emits only Sprout-changing blocks with
their canonical hashes, and then decodes, replays, canonical-index-validates, and handoff-validates
its own output:

```console
cargo run -p zakura-state --bin generate-vct-sprout-artifact -- \
  /path/to/zakura-cache /path/to/vct-sprout-history.bin
```

Reviewers must reproduce and compare the emitted bytes, SHA-256 digest, terminal root, and
handoff identity before changing `MAINNET_ARTIFACT`. Running the tool never installs or enables
its output.

### 17.4 Embedding, availability, and replay

`vct/artifact.rs` currently has no embedded Mainnet bytes (`MAINNET_ARTIFACT` is `None`).
When reviewed bytes are available, they are compiled into that module and loaded only through
`embedded_mainnet()`. Until then, opening a pre-28.0.1 Mainnet VCT-synced database is rejected.
The guard also rejects affected read-only databases and writable opens with upgrades disabled,
even after artifact bytes become available: operators must reopen writable to repair or
discard/resync. Non-Mainnet databases never load or replay this Mainnet artifact. Normally synced
databases and databases already marked at the repair format are unaffected.

The initial startup format change now runs synchronously before `ZakuraDb` or `FinalizedState`
is exposed; only periodic current-format checks remain in the background. Therefore no block
commit can race the migration or observe partially repaired anchors. The 28.0.1 format upgrade
validates artifact records against both retained canonical indexes only through the local
finalized tip. If the tip has reached the database marker, it also requires the local block hash
at that marker to equal the checkpoint list's canonical hash; a prefix database below the marker
does not yet need that local entry. It then replays records through
`min(finalized_tip, database_marker)` into one
`DiskWriteBatch`, first inserting the empty Sprout anchor and then every recorded resulting
anchor. If the tip is below the database marker, it also replaces the stale Sprout **tip** with
the replayed prefix frontier. If the tip is at or above the marker, it deliberately leaves the
tip unchanged: post-marker commits may have advanced truthful state, while the replay only
reconstructs the originally broken fast region. Artifact handoff and database marker equality is
never required.

Cancellation is checked before work, between records, and before the write. The anchor inserts
and any prefix-tip update commit atomically. The database format version is marked complete
only after the upgrade succeeds, so a crash or cancellation leaves the old version and safely
replays the same deterministic batch on the next startup. This makes the migration
crash-safe and idempotent at the format-upgrade boundary; it does not alter post-marker state.
