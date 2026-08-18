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

> **Root persistence is authenticated.** Peer-supplied roots are never written to the
> authoritative `commitment_roots_by_height` index unauthenticated. On the fork-aware header
> chain each root arrives as a hash-keyed auxiliary delivery on its header's DAG node, is
> authenticated against the one-header-later commitment that proves it (§6.0 as headers
> arrive, and again at commit in §6), and only reaches the index through a committed body.
> The superseded ascending-frontier lane (`AuthenticateHeaderRoots`), which keyed roots by
> height instead of by header hash, is **removed**; it is specified for historical reference in
> [header-sync-vct-root-authentication.md](header-sync-vct-root-authentication.md).

**Data flow (fetch + commit path):**

```text
header sync (runs ahead of bodies)
   │ GetHeaders { want_tree_aux_roots } ─▶ peer ─▶ Headers { headers, body_sizes, tree_aux_roots }
   │ (roots carried in-band, all-or-nothing, finalized ranges only; §4.2)
   ▼
header-sync reactor (zakura-network): validate root count + per-height alignment; reject
   │ unrequested or non-finalized roots as MalformedMessage (§8.1)
   ▼
InsertHeaders (zakura-state): commit the headers and their hash-keyed auxiliary deliveries
   │ into the header-chain DAG, then authenticate each delivery against its successor
   │ header as soon as that successor is selected (§6.0), evicting a failing one at once
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
   ─▶ ReadRequest::BlockRoots ─▶ the authoritative commitment_roots_by_height index:
      body-derived rows only, and never above the finalized body tip (all-or-nothing; §9)
```

**Lifecycle of one fast sync.**

(1) Node starts under `consensus.checkpoint_sync = true` on
Mainnet → the committer is built in peer mode.
(2) Header sync requests the per-height roots in-band with the finalized header ranges it already fetches (`want_tree_aux_roots`); each root is stored as an auxiliary delivery on its header's DAG node and authenticated there as soon as the successor header that proves it arrives, far ahead of the committer (§4.2, §6.0). (3) Each checkpoint block: look up its root; verify it (own header now, successor header next block, plus
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
| **Header-authenticated roots** | Peer-supplied roots carried in the header-sync `Headers` message, attached to their header's DAG node as auxiliary deliveries and authenticated against the one-header-later commitment as soon as that header arrives, ahead of body commit (§4.2, §6.0). Verify-before-commit re-checks them at body time (§6). |
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
  roots in-band, the authenticated-root persistence and serving read path, and the persistent
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
is inserted via `InsertHeaders`, its roots attach to their header's DAG node as auxiliary
deliveries; state then **authenticates each delivery against the one-header-later commitment
that proves it, as soon as that successor header arrives** (§6.0), and records the verdict as
header-chain evidence. Only roots below the last checkpoint are ever _consumed_ by the
committer, and only after verify-before-commit (§6). The committer then reads them per height
through the `PeerSource` seam, and writes the `commitment_roots_by_height` row itself once the
body commits — the index holds no height the node has not committed.
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
only ever looks up a root for a block it is about to commit, and persisted authenticated roots
are naturally bounded above by the header tip and settled below it: each header-authenticated
root is **replaced by the body-derived serving-index row when its block body commits** (the
same atomic batch), and header-store rollback
also trims header-supplied roots above the rollback target. Advertised body-size hints follow the same header-store
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
corresponding transaction counts are pinned to zero before those activations, and
`sapling_tx` is pinned to zero before Sapling. Below Heartwood, `sapling_tx` is
also body-verified-only (ZIP-221 does not exist yet); `auth_data_root` below NU5
is body-verified-only because the applicable headers do not commit to that field.
On promotion, those body-verified-only fields are cleared to canonical zeros so
authenticated rows never retain peer-controlled values for unbound slots. The
deserializer treats `height` as an unvalidated `u32`: a wrong or out-of-range height simply
fails to match any local header during verification (§6), so it is harmless; malformed root
bytes are rejected by the root parsers.

The payload carries **no trust**: a recipient applies the epoch-specific direct pins and
re-verifies applicable fields against its own checkpoint-committed headers (§6) before
folding them in. The pre-Heartwood Sapling transaction count becomes authoritative only when
the downloaded body is verified, as does the pre-NU5 auth-data root.

### 5.2 The final frontier last checkpoint height (embedded)

Fast mode never advances the running Sapling/Orchard/Ironwood frontiers below the checkpoint,
so the real frontiers at the checkpoint must be supplied for the resume. `FinalFrontiers {
height, sapling, orchard, sprout, ironwood }` is embedded in the binary
(`crates/zakura-state/src/service/finalized_state/vct/mainnet-frontier.bin`, via `include_bytes!`),
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

`CommitmentRootSource` (`crates/zakura-state/.../finalized_state/commitment_aux.rs`) abstracts _where_
the fast path's roots and last checkpoint height frontier come from. The committer (`VctState.source`) reads
through this one seam regardless of source:

```rust
fn vct_root(&self, height) -> Option<(sapling::Root, orchard::Root, ironwood::Root)>;
fn vct_last_checkpoint_height(&self) -> block::Height;
fn final_frontiers(&self) -> &FinalFrontiers;
```

The seam is read-only: authenticated rows are durable state, so the committer has no way to
delete one (a rejected root parks the commit instead; §8). Committed rows are cleaned up by
the database's own retention, not through this seam.

Implementations:

- `PeerSource` — the production default, a **DB-backed reader** (`PeerSource::new(db,
  frontiers)`). Each `vct_root(height)` reads the authenticated root for that height from the
  `commitment_roots_by_height` column family that the root-authentication lane persisted
  (§4.2). The
  last checkpoint height frontier is held immutably from the embedded constant, so only roots come
  from the network. A root that fails body-time verification stays on disk (deleting it would
  create a gap below the durable authentication frontier); the commit parks and state requests
  a bounded re-delivery instead (§8.1). The earlier in-memory
  cache variant and its `PeerSourceWriter` are removed; proptests fill roots by writing to an
  ephemeral database through the same write path.
- `FixtureSource` — a crate-local `#[cfg(test)]` source over the same height→roots map, used only
  to isolate committer behavior and DB-produced payload round trips without networking.

The **producer** half (`produce_block_roots(db, range)` / `produce_final_frontiers(db,
height)`) derives the same payload from a database's per-height trees — the serving read path
(§9), minus the network. The producer→`PeerSource`→committer round-trip proving producer and
consumer agree is `vct_db_produced_payload_round_trips`.

Because the production `PeerSource` reads straight from the database, peer mode no longer
exports a root-writer handle. Header sync delivers roots with header ranges, state stores and
authenticates them as header-chain auxiliary deliveries on the normal write path (§6.0), and
the committer reads the authenticated delivery back for the height it is committing. The
old per-state `TreeAuxRootsWriter` / `PeerSourceHandle` / targeted-refetch signal are removed.
The persisted roots store no peer identity; peer accountability for bad roots is the header-sync
reactor's misbehavior reporting (§8.1), preserving the `zakura-state` / `zakura-network` crate
boundary.

### 5.4 Roots on the header-sync message

There is no separate roots stream. The header-sync `HeaderSyncMessage` carries roots in two
places (`crates/zakura-network/src/zakura/header_sync/wire.rs`):

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
`ReadRequest::MissingBlockBodyMetadata` returns the durable committed block size when available,
otherwise the advertised hint, otherwise `None`. Block sync uses those hints to pack contiguous
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

### 6.0 Header-time authentication (the early check)

The committer is the trust boundary. It reaches a height only when it commits that
height's block body. During fast sync, the header chain runs far ahead of the block bodies. An
invalid root can therefore remain in the DAG until the committer reaches it.

`service/write/vct_authentication_sweep.rs` detects invalid roots earlier. The sweep walks the
selected projection above the committed body tip. It maintains a ZIP-221 MMR anchored at that tip.
At each height `H`, the sweep verifies the selected delivery against the delivery at `H+1`. This
check uses the same successor boundary as the committer. The sweep calls
`verify_supplied_roots_from_parts` because it has headers instead of block bodies. The sweep records
a successful delivery as `Authenticated`. The sweep rejects an attributable delivery. The sweep
disputes both deliveries when the boundary cannot identify which delivery is invalid. Either
failure starts metadata repair immediately.

Three properties keep the committer authoritative:

- **The committer still verifies every committed delivery.** The sweep adds an early check.
  It does not move the trust boundary.
- **Failure attribution matches the committer.** The header-chain writer disputes both deliveries
  when the boundary cannot identify the invalid delivery. A replacement can later authenticate the
  honest delivery and reject the invalid delivery. A failure on a delivery's pre-activation fields
  rejects only that delivery.
- **Authentication pins selection.** `select_vct_auxiliary_delivery` prefers an authenticated
  delivery. Authentication state cannot return to `Unauthenticated`. A later delivery cannot
  displace roots that the running MMR already folded.

The sweep and committer track repair requests independently. The repair channel publishes the
lower height. A successful block commit clears only the committer repair request.

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
     height's root is bad → reject and park for repair (§8);
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

- A supplied root that fails _any_ verification step **refuses** the commit with the typed,
  **retryable** `VctSuppliedRootUnavailable { height }` error — never recomputed locally. The
  stored row is authenticated durable state and stays on disk (deleting it individually would
  create a gap below the durable authentication frontier); the parked commit is unblocked by a
  bounded repair re-delivery that replaces the row (§8.1).
- A frozen-frontier height with **no** valid supplied root (not yet authenticated, or rejected)
  refuses with the same retryable error and leaves the database untouched. The block commits
  once the root-authentication lane stores a verifiable row for it (§8.1).
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
**without resetting the block queue**. The write loop also publishes a bounded repair request
(`VctRootRepairRequested`) back to header sync, which re-fetches the covered range and runs it
through the root-authentication lane; the retry is satisfied once a verifiable row is stored.
If no repair delivery fills the hole, the node stays parked
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
`state.vct.root.rejected.count` (rejected by verification),
`state.vct.root.unavailable.count` (frozen-frontier hole refused),
`state.vct.root.await_successor.count` (deferred for a missing successor),
`state.vct.root.retry.count` (park-and-retry attempts),
`state.vct.root.repair.requested` (bounded repair requests published to header sync), and the
`state.vct.root.stalled.height` gauge (raised once a height is stuck past the warn threshold).

### 8.1 Adversarial peer handling

With roots carried in-band on header sync, there is no separate `tree_aux` driver and no bespoke
provenance/cooldown/demotion/hedging policy. Bad roots are handled in three layers:

- **At the wire/reactor boundary**, a peer that sends a malformed root set — wrong count,
  misaligned height, roots on a non-finalized range, roots that were not requested, or an
  invalid marker byte — is reported through header sync's existing misbehavior path
  (`report_misbehavior(.., MalformedMessage)`), and the range is retried. None of those roots
  reach state.
- **At header-time authentication**, as each successor header arrives and long before the
  committer reaches the height, state verifies the delivery against the commitment that proves
  it; an invalid delivery is evicted, attributed to the exact supplying peer, and re-fetched
  from another peer (§6.0). This is where a well-formed wrong root is normally caught.
- **At verify-before-commit**, as defense in depth, a stored root that still fails
  authentication against the header commitment (§6) refuses the commit with the retryable
  `VctSuppliedRootUnavailable` error (§8). The row is not deleted (it is below the durable
  authentication frontier); state publishes a bounded `VctRootRepairRequested` so header sync
  re-fetches the covered range and re-authenticates it, and the block then commits in place,
  without resetting the block queue.

Safety is unconditional: a lying peer can never corrupt state, and a wrong root that reaches
the commit path halts the fast sync at that height — fail-closed, surfaced by the §8 stall
metrics/logs — until a repair delivery replaces it. Peer accountability rides header
sync's misbehavior scoring plus the root-authentication lane's exact per-request attribution,
so the committer still attributes nothing to peers itself and `zakura-state` keeps no
dependency on `zakura-network` peer types.

## 9. The serving read path (`BlockRoots`)

A node serves roots from local state via `ReadRequest::BlockRoots { start_height, count }` →
`ReadResponse::BlockRoots(Vec<BlockCommitmentRoots>)`. The read handler:

- refuses any range reaching above the finalized body tip;
- serves the contiguous prefix of the authoritative `commitment_roots_by_height` index — all
  body-derived (so a fast-synced node lacking historical per-height trees can still serve),
  falling back to `produce_block_roots` over per-height trees only on a pre-index archive
  database;
- returns an empty vec for out-of-range/empty requests.

Every served row is body-derived, and the serve refuses any range reaching above the finalized
body tip; there is no provisional tier and a forwarding node introduces no additional trust.

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
Serving header-authenticated roots ahead of committed bodies (§9) widens the servable
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
  refused. A malformed root set is rejected at the header-sync reactor before it
  reaches state and is scored through header sync's misbehavior path; a well-formed wrong root
  is normally rejected by header-root authentication before it is ever persisted, and one that
  still reaches verify-before-commit parks the commit until repaired (§8.1). The trade-off is
  availability, not integrity: a bad root stalls the fast sync at that height instead
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
- **Increment 6c — fold roots into header sync (done, superseded by 6d for persistence).**
  The standalone `tree_aux` stream,
  its driver, in-memory cache writer, and bespoke peer policy are **removed**. Roots now ride the
  header-sync `Headers` message as all-or-nothing metadata (§4.2, §5.4) and
  are read back by a DB-backed `PeerSource`. This increment persisted them provisionally at
  header commit; that behavior was replaced by increment 6d.
- **Increment 6d — header-root authentication (done, superseded by 6e).** Peer-supplied roots are
  no longer persisted unauthenticated. State authenticated each range against the canonical
  header chain behind a durable ascending frontier (`AuthenticateHeaderRoots`), and only
  verified prefixes entered `commitment_roots_by_height`; a one-time database format upgrade
  truncates pre-existing header-ahead rows. Adds the bounded `VctRootRepair` re-delivery path
  for missing or rejected covered heights. Specified in
  [header-sync-vct-root-authentication.md](header-sync-vct-root-authentication.md).
- **Increment 6e — fork-aware header-time authentication (done).** The height-keyed lane assumed
  one canonical chain, so it could not express a root on a competing fork. Each root now attaches
  to its header's DAG node as a hash-keyed auxiliary delivery and is authenticated there (§6.0),
  reusing 6d's verification kernel unchanged. The `AuthenticateHeaderRoots` request, its durable
  ascending frontier, and the `header_root_auth_frontier` column family are **removed**; the
  index is written only by committed bodies.
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
  (`served_header_tree_aux_roots_require_complete_coverage`); body-commit replacement of
  header-supplied rows
  (`write_block_replaces_matching_header_supplied_roots_with_verified_row`);
  read stability of authenticated rows on body mismatch
  (`peer_source_keeps_authenticated_roots_on_body_mismatch`); and the in-process
  producer → `PeerSource` → committer byte-identical equivalence.
- **Frozen-frontier proptests:** a frozen-frontier hole returns the retryable
  `VctSuppliedRootUnavailable` and leaves the DB untouched; a reopened committer (frozen marker
  persisted) still refuses on the first post-restart missing root.
- **Header-sync transport:** the header-sync driver tests (`zakura_header_sync_driver_tests`)
  exercise serving and committing finalized ranges with roots end-to-end, including the
  all-or-nothing serving helper (roots attached only on complete coverage, otherwise rootless
  headers) and routing received roots into `CommitHeaderRange`.
- **State persistence:** `InsertHeaders` validates root shapes and stores the roots as auxiliary
  deliveries. It does not write a root row. `service/write/vct_authentication_sweep.rs`
  authenticates each delivery against its successor commitment. The sweep records the result as
  header-chain evidence. The sweep starts repair after a rejection or dispute. Only a committed
  body writes a `commitment_roots_by_height` row. Rollback truncates rows above its target height.
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
| Wire payload (`BlockCommitmentRoots`) | `crates/zakura-chain/src/parallel/commitment_aux.rs` |
| Source seam, `PeerSource`, producers, bulk root invalidation | `crates/zakura-state/src/service/finalized_state/commitment_aux.rs` |
| Verify-before-commit logic | `crates/zakura-state/src/service/finalized_state/commitment_aux_verify.rs` |
| Embedded frontier plumbing, `select_source_mode`, counters | `crates/zakura-state/src/service/finalized_state/vct.rs` |
| `checkpoint_sync` mirror field (mode input) | `crates/zakura-state/src/config.rs`; set in `crates/zakurad/src/commands/start.rs` |
| Embedded Mainnet VCT state and manifest | `crates/zakura-state/src/service/finalized_state/vct/mainnet-frontier.bin`, `.../mainnet-subtrees.bin`, `.../mainnet-vct-manifest.json` |
| Commit-path hook, last checkpoint height, frozen-frontier policy | `crates/zakura-state/src/service/finalized_state.rs` |
| `BlockRoots` serving read (authoritative index) | `crates/zakura-state/src/service.rs` |
| Root-index lifecycle (`commitment_roots_by_height`, authentication frontier, body-commit/rollback policies) | `crates/zakura-state/src/service/finalized_state/zakura_db/commitment_roots_db.rs`, `.../block.rs`, `.../rollback.rs` |
| `CommitHeaderRange` with roots, fast-path hit/miss metrics | `crates/zakura-state/src/service/write.rs` |
| Header-sync wire (`GetHeaders`/`Headers` roots, markers, byte budget) | `crates/zakura-network/src/zakura/header_sync/wire.rs` |
| Header-sync root validation (count, height alignment, markers) | `crates/zakura-network/src/zakura/header_sync/validation.rs`, `.../error.rs` |
| Header-sync reactor (request/serve/receive roots, misbehavior) | `crates/zakura-network/src/zakura/header_sync/reactor.rs` |
| Header-sync driver: serve `BlockRoots`, all-or-nothing helper, route received roots | `crates/zakurad/src/commands/start/zakura/header_sync_driver.rs` |

## 16. Mainnet release-state pipeline

The embedded Mainnet frontier and completed-subtree roots are release artifacts coupled to the
terminal Mainnet checkpoint. Whenever `main-checkpoints.txt`'s max height advances, the matching
`mainnet-frontier.bin` and `mainnet-subtrees.bin` must advance to the same height, and all three
must land in the same PR. The offline exporter below produces the coupled set; the publisher,
refresh workflow, and release gate below consume it.

### 16.1 Offline export (`zakura-checkpoints --state-cache-dir`)

The publisher host runs the `zakura-checkpoints` utility (built with
`--features zakura-checkpoints-offline`) against a quiesced copy of a synced Mainnet state:

```text
zakura-checkpoints \
  --state-cache-dir /path/to/quiesced-zakura-cache \
  --full-list \
  --mainnet-frontier-output /out/mainnet-frontier.bin \
  --mainnet-subtree-output /out/mainnet-treestate-subtrees.bin \
  > /out/main-checkpoints.txt
```

- Offline mode reads canonical hashes and `BlockInfo` sizes straight from the finalized
  database (read-only), so it works on pruned state and needs no RPC or running node. The
  quiesced database tip is by construction `MAX_BLOCK_REORG_HEIGHT` (1000) blocks behind the
  network tip, which keeps every emitted checkpoint reorg-safe.
- New checkpoints continue the same cumulative byte-count / 400-block height-gap selection as
  the RPC mode, starting from the embedded Mainnet max checkpoint. Selection state fully
  resets at every selected checkpoint, so exports taken at different tips are byte-for-byte
  prefix-compatible — the **grid contract** that lets the pipeline's import verify updates as
  pure appends. (The selection constants are part of that contract; changing them means
  regenerating the committed suffix wholesale in a reviewed PR. Never hand-append RPC-mode
  Mainnet checkpoints: they would land off-grid.)
- `--full-list` prints the embedded list verbatim before the new checkpoints, so stdout is a
  complete replacement `main-checkpoints.txt`.
- The frontier artifact is captured at the **last emitted checkpoint**, which sits below the
  database tip. Sprout stores only its tip frontier, so
  `produce_settled_final_frontiers_bytes` proves the pairing is sound: it scans every
  retained block body above the requested height and fails closed if any appended Sprout
  note commitments (v4 JoinSplits still can) or is missing. A failed scan self-heals on a
  later export once the checkpoint sequence passes the Sprout-changing block. The bytes are
  validated through the same parser used for the embedded frontier before being written.
- Frontier correctness inherits the finalized database's trust boundary: the exporter reads
  trees produced by Zakura's validated, atomic finalized-state commits rather than replaying
  historical transaction bodies. The parser validates framing and height, not the tree roots'
  provenance. This deliberate trust model keeps the exporter compatible with pruned databases;
  operators must only export from a quiesced state produced by a trusted Zakura node.
- The subtree artifact starts with the reviewed roots already embedded in the binary, then appends
  subtree rows the database retained after that checkpoint. Any overlapping database row must
  match the embedded record. The new frontier supplies the exact number of roots needed, and the
  complete result is proven against it before either artifact is returned. This one path works for
  legacy and VCT databases and does not need old block bodies or skipped per-height frontiers.
- Checkpoint lines go to stdout; all status goes to stderr. RPC mode remains for Testnet
  updates and diagnostics.

### 16.2 Bundle, pointer, and refresh workflow

The publisher (`deploy/release-state/`) uploads each export to R2 as an immutable bundle containing
`meta.json`, `main-checkpoints.txt`, `mainnet-frontier.bin`, and
`mainnet-treestate-subtrees.bin`. `meta.json` binds the network, terminal height/hash, an RFC 3339
generation time, and each file's size and SHA-256. The publisher then atomically replaces the
mutable `release-state/latest.json` pointer (height, hash, `meta_url`, `meta_sha256`), keeping the
newest few bundles.

The `update-release-state.yml` workflow (manual dispatch plus a weekly cron) and
`prepare-release-pr.yml` both resolve the pointer once over a pinned HTTPS host with no
redirects, bounded reads, digest verification at every hop, and a maximum bundle age. They
exit green without release-state changes when the bundle does not advance the committed
list. Otherwise their shared importer verifies the committed `main-checkpoints.txt` is a
byte-identical prefix of the bundle's list, requires each pool's subtree bytes to retain the
committed prefix, replaces all three artifacts, and writes `vct/mainnet-vct-manifest.json`
provenance (source `release-state-bundle`, heights, digests, bundle binding).

The standalone update workflow also floors `ESTIMATED_RELEASE_HEIGHT`, validates everything
— including proving the candidate subtree roots against its frontier — restricts the diff to
exactly those five files, and opens a signed **draft PR** for human review. During release
preparation, the importer leaves that constant unchanged so `prepare-release.sh` remains the
sole owner of its projected value and summary. The release workflow re-proves the imported
checkpoint/frontier pairing and includes the import in its draft release PR. Checkpoints are
consensus-critical: the reviewed, committed files are the trust root, and the pipeline's
digests only prove faithful transport from our own publisher.

### 16.3 Committed provenance and the release gate

`vct/mainnet-vct-manifest.json` is committed next to the frontier and subtree artifact. The
`embedded_mainnet_final_frontiers_parse` unit test re-derives its digests from the embedded
checkpoint list and frontier bytes on every PR, so a desynced checkpoint/frontier/provenance
combination fails ordinary CI. The current frontier predates the pipeline and is recorded
honestly as `source: legacy-bootstrap`.

`make pre-release` runs `scripts/check-release-state.sh`, which re-verifies the pairing
without cargo and rejects `legacy-bootstrap` provenance so a release cannot ship bootstrap
state once the pipeline is live; `ZAKURA_ALLOW_BOOTSTRAP_RELEASE_STATE=1` (or the
`allow_bootstrap_release_state` input on `create-release.yml`) is the documented emergency
override. Release creation never consults the moving R2 pointer — it validates only
committed source.

### 16.4 Local test contract

Byte compatibility with the node loader is proven by the `zakura-state` frontier tests: they
produce frontier bytes from a small generated `FinalizedState`, load them through the same
parser used for `VCT_REGTEST_FRONTIER` and the embedded Mainnet frontier, check the height
and all four roots, and reject a different expected height. The settled-Sprout scan is
pinned against real Mainnet blocks 395/396 (the first JoinSplit). The focused local checks
are:

```text
cargo test -p zakura-state --lib -- frontier sprout_change
cargo test -p zakura-state --lib treestate_export
cargo test -p zakura-utils --features zakura-checkpoints-offline
```

## 17. Historical note: the VCT Sprout-history repair artifact

The original VCT fast path advanced Sprout locally but omitted the historical
`Sprout root -> frontier` anchor entries, so an affected Mainnet database could not verify a
later JoinSplit that spends one of those roots. Two things addressed this:

- The forward fix (section 7): the fast commit path now updates the Sprout tree for every block,
  so databases built by current releases are never affected.
- A temporary repair: database format version 28.0.1 replayed a reviewed, embedded, Mainnet-only
  artifact of every Sprout-changing block to backfill the missing anchors.

**The repair artifact has been removed.** With the forward fix long shipped, carrying a 71 MB
embedded blob and its replay machinery was pure cost. Version 28.0.1 is now a no-op format bump.

Databases still missing that history are no longer repairable, so they fail closed instead: a
Mainnet database that is VCT-synced and below format version 28.0.1 is rejected at startup with
`StateInitError::VctSproutHistoryUnrepairable`, in every open mode. Operators must discard and
resync, or restore a snapshot taken with a current release. See `is_unrepairable_vct_database` in
`crates/zakura-state/src/service/finalized_state/zakura_db.rs`.
