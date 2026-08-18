# Historical treestate serving on fast-synced nodes

A verified-commitment-trees (VCT) fast-synced node never builds the per-height
Sapling/Orchard/Ironwood note-commitment trees below the last checkpoint. That is
deliberate and consensus-safe (see
[verified-commitment-trees.md](verified-commitment-trees.md)), but it removes the data
behind `z_gettreestate`, `z_getsubtreesbyindex`, and the `trees` field of `getblock` for
that band. Wallets that sync against a fast-synced archive snapshot cannot start.

This design restores all three without adding a trust surface. The mechanism: publish an
artifact of per-pool frontiers at a sparse height grid, derive any other height on demand
by replaying retained block bodies forward from the nearest entry, and accept nothing that
fails to reproduce the root the node already authenticated. Completed subtree roots ship
separately, pinned by the same final frontier the commit path already verifies.

## Status (2026-08-17)

| Component | State |
| --- | --- |
| Frontier derivation, root check, bounded cache | On `main` |
| Subtree-root artifact and `z_getsubtreesbyindex` serving | On `main` |
| Typed `HistoricalTreeUnavailable` in place of `null` / empty list | On `main` |
| Frontier grid artifact, config gating, `export-historical-treestates` | PR [#703](https://github.com/zakura-core/zakura/pull/703) |
| Client-side removal of the per-block `trees` dependency (§6) | Not started |

## 1. Problem: the absent band

The fast commit path writes anchors, the history tree, and the
`commitment_roots_by_height` serving index, then returns before writing per-height trees
or subtrees (`zakura-state/.../zakura_db/shielded.rs`, the `fast_write.anchor_roots`
branch). The result is a half-open band `[U, H)` where those column families are empty:

- `U` is `vct_upgrade_height`, the first height this binary vct-committed. For a snapshot
  synced from scratch it is effectively genesis.
- `H` is `vct_synced_below`, the last checkpoint. The currently embedded Mainnet frontier
  puts it at 3,418,406. Shipped snapshots can carry an older marker, so `H` is read from
  the database and never assumed.

Heights at or above `H` get trees again from semantic sync, so the gap is exactly the
checkpointed band. Three datasets are missing in it:

| Missing dataset | Column family | Consumer | Behavior before this design |
| --- | --- | --- | --- |
| Per-height pool frontiers | `{pool}_note_commitment_tree` | `z_gettreestate` | JSON `null` |
| Completed subtree roots | `{pool}_note_commitment_subtree` | `z_getsubtreesbyindex` | empty list |
| Per-height tree sizes | derived from the trees | `getblock` v1, `getblockheader` | hard RPC error |

The `null` treestate is the one that motivates the work. Clients following the
lightwalletd contract treat an absent tree as the _empty_ tree, so a wallet derives a
birthday anchor asserting an empty commitment tree at a height deep in the chain, with no
error raised at the point of corruption. The hard RPC error is louder but less dangerous,
and it is what stops a wallet in practice, since lightwalletd-style clients fetch the
`trees` sizes once per block.

## 2. Goals and non-goals

Goals:

- Restore `z_gettreestate`, `z_getsubtreesbyindex`, and verbose
  `getblock`/`getblockheader` across the absent band on an archive-mode fast-synced node.
- Add no **new** trust surface: every served frontier and subtree root is cryptographically
  pinned by something the node already authenticated.
- Keep the artifact small enough to distribute through the existing release-state pipeline,
  without embedding it in the binary.
- Keep the consensus commit path untouched.

Non-goals:

- **Pruned mode.** Derivation needs retained block bodies. A pruned node still cannot serve
  historical treestates, which is unchanged from today.
- **Historical Sprout treestates.** `z_gettreestate` already returns no Sprout data at any
  height on every node, because Sprout stores only its tip frontier. Excluding Sprout
  removes the hardest part of artifact generation and regresses nothing.
- **Eliminating the recompute globally.** The ordered pass still happens once, on the
  publisher host. This design removes it from every consuming node, not from the world.

## 3. Design

### 3.1 The invariant: roots pin frontiers

This is the property the whole design rests on.

A note-commitment root is a binding commitment to its frontier. The VCT commit path
already exploits this at the last checkpoint, where it accepts the embedded final frontier
only if `frontier.root()` equals the verified root for each pool (VCT design §7, step 3).
Every height in the band already has a root in `commitment_roots_by_height`, and that index is
gap-free from `U` and authenticated against the node's own header chain (VCT design §9,
and [header-sync-vct-root-authentication.md](header-sync-vct-root-authentication.md)).

The same check generalises: **a frontier claimed at height `h`, from any source, is
accepted only if its root equals the stored authenticated root at `h`.** A corrupt,
truncated, or hostile input is rejected rather than absorbed.

Two consequences shape everything below:

1. The frontier artifact carries no trust weight. It needs no review-based trust, no place
   in committed source, and no embedding in the binary. It can be fetched at runtime from
   any source, including eventually a peer, which is a natural fit for the cross-client
   spec work in roadmap increment 9.
2. Locally derived frontiers get the same check for free, so derivation is self-validating
   and a derived frontier is safe to reuse as an anchor for later derivations.

### 3.2 The frontier grid artifact

Sapling, Orchard, and Ironwood frontiers at a sparse grid of heights across the absent
band. Entries below a pool's activation height are empty and cost nothing. A partial cell
at the current tip is omitted rather than clamped, so a later export is a prefix-append of
an earlier one, matching the append-only grid contract the checkpoint list already
follows.

Grid spacing is a tunable, not part of the format, and it is weighted by cost rather than
uniform. Replay costs ~1.9 ms/block on average but varies ~50x by height, so a uniform
grid cannot bound the worst case at a sane size: a 2 s worst case needs ~110-block
spacing, 30,527 entries, ~21.8 MB. Spacing entries by estimated replay cost instead
concentrates them where blocks are expensive, and at a 2 s per-entry budget that is 3,380
entries and 3.25 MB. See [Appendix A](#appendix-a-measurements-2026-08-03).

Format follows the conventions established by the Sprout history artifact (VCT design
§17): magic, version, network byte, explicit record count, a SHA-256 over the payload, and
a parser that validates framing before any record is used.

```text
              U                                                  H = vct_synced_below
  genesis ────┼───────── absent band: no per-height trees ───────┼──── semantic sync ──▶
              │                                                  │
  grid        │  ●        ●         ●            ●         ●     │  cost-weighted,
  entries     │  g0       g1        g2           g3        g4    │  ~3,380 entries
              │                     └── replay bodies ──▶ h      │
              │                         (nearest anchor ≤ h,     │
              │                          cache or grid)          │
              ▼                                 │
  commitment_roots_by_height                    │
  (authenticated, gap-free from U) ─────────────┤
                                                ▼
                                 root(frontier@h) == stored root(h)?
                                     yes ─▶ cache, then serve
                                     no  ─▶ HistoricalTreeUnavailable
```

### 3.3 Derivation and caching

To serve an arbitrary height `h` in the band: take the nearest verified frontier at or
below `h`, which is the higher of the bounded cache and the published grid, replay the note
commitments from the retained block bodies in `(anchor, h]`, verify the resulting root
against the stored root at `h`, cache it, and serve.

Two properties make a coarse grid viable:

**Wallet access is sequential.** Scan batches are contiguous: a wallet's next request is
for the block immediately preceding the range it just finished. A node that caches its most
recently derived frontier replays _forward by one batch_, not from a distant grid point.

**Therefore spacing only costs the first request of a sweep.** Steady-state replay cost is
bounded by the client's batch size, independent of grid spacing. That is why the grid is
sized to bound the cold request and the cache carries the rest.

Cache entries double as anchors, which is what makes the two properties compose, and by
§3.1 only root-checked frontiers ever enter the cache.

`state.max_historical_tree_replay_blocks` bounds what a single request may cost from its
nearest anchor. It is a serving backstop, not a correctness bound.

### 3.4 The subtree-root artifact

A completed subtree root is an interior node rather than a complete frontier, so it cannot
be checked by comparing it directly with `commitment_roots_by_height`. It is nevertheless
pinned by the final frontier. The frontier already holds the pairwise hashes of completed
subtrees at every level at or above the tracked subtree height, so folding the candidate
roots in index order must reproduce those stored hashes, and the frontier's own
subtree-level root checks the boundary case where it ends exactly at a subtree
completion. A wrong, missing, or extra root therefore fails verification without
replaying any of the subtree's 65,536 leaves.

The set is small and static: ~1,895 records at the last checkpoint (3,418,406), roughly 70 KB
framed, growing append-only by a handful of subtrees per month at current usage. It ships
embedded in the committed bundle rather than in the runtime-fetched frontier artifact
because it is a small append-only serving index coupled to the release checkpoint. That
location is an operational choice, not a source of trust: a consuming node verifies the
embedded artifact once per process against its embedded frontier before exposing it to the
read service, and RPC requests do not repeat the proof.

### 3.5 Serving the three RPCs

- **`z_gettreestate`** is served directly by §3.3.
- **`getblock` / `getblockheader` sizes** need only a count, not a frontier. A frontier
  knows its own position, so the size at `h` is the nearest anchor's `tree_size()` plus the
  commitments added in between, which requires no hashing at all.
- **`z_getsubtreesbyindex`** is served from the subtree-root artifact, never by replay.
  Wallets doing spend-before-sync fetch the full subtree list in one request at startup,
  from index 0 to the tip, so a replay-backed cold response would approach a full-band
  replay rather than one batch forward. The sequential-access argument that makes §3.3
  cheap does not apply here.

### 3.6 Failure policy

Every way this can fail returns the typed `HistoricalTreeUnavailable` error, never a
`null` treestate or an empty subtree list: derivation disabled, artifact absent or
unreadable, no entry covering the requested height, replay exceeding its bound, or a
derived root failing its check. A pruned node returns it immediately rather than walking
the range until the first missing body.

Startup fails closed if `state.derive_historical_trees` is on without a configured grid,
so a cold request can never replay the entire absent band.

## 4. Component map

```text
z_gettreestate, getblock, getblockheader              (zakura-rpc)
  └─▶ ReadRequest::{Sapling,Orchard,Ironwood}Tree
        └─▶ read::historical_tree                     (zakura-state/src/service/read/historical_tree.rs)
              ├─ nearest anchor: max(cache, grid entry) ≤ h
              ├─ replay retained block bodies in (anchor, h]
              ├─ root check against commitment_roots_by_height
              └─ HistoricalTreeCache (bounded; entries double as anchors)

z_getsubtreesbyindex                                  (zakura-rpc)
  └─▶ subtree-root artifact                           (zakura-state/.../finalized_state/treestate_artifact.rs)
        verified once per process against the embedded final frontier
        (fold proof in zakura-chain/src/subtree_verify.rs)
```

Configuration (`zakura-state/src/config.rs`):

| Key | Role |
| --- | --- |
| `state.derive_historical_trees` | Enables derivation. Off by default. |
| `state.historical_frontier_artifact` | Path to the grid. Required when derivation is on. |
| `state.max_historical_tree_replay_blocks` | Per-request replay backstop. |

## 5. Generation

The two artifacts have independent generators. Neither needs the other, and neither
touches the consensus commit path.

**Frontier grid** — `zakurad export-historical-treestates`, run read-only against a
quiesced archive database on the publisher host. It replays block bodies and checks each
grid entry against the authenticated roots in `commitment_roots_by_height`, rather than
reading stored per-height trees, so any **archive** node can generate it, fast-synced
included. That is how the current Mainnet artifacts were produced. A pruned node still
cannot, because replay needs retained bodies. Where a database upgraded to VCT above
genesis, generation anchors on the stored per-pool trees at `U - 1` and replays only the
absent band `[U, H)`. The command defaults to a cost-weighted grid at a 2 s per-entry
budget (`--target-cost-ms 2000`); `--spacing` produces a uniform grid, which cannot bound
the worst-case cold request at a sane size and is not recommended.

**Subtree roots** — the release-state pipeline, where `zakura-checkpoints` extends the
embedded artifact with the rows the database retained after the previous checkpoint and
proves the result against the newly produced final frontier before returning it. It needs
neither historical block bodies nor a shared pass with the grid.
`zakurad verify-historical-treestates` repeats that proof offline for a candidate bundle.

This remains the design's central trade: the ordered pass happens once, on one host, and is
verified independently by every consumer.

## 6. Related work: remove the per-block size dependency

Independent of everything above, the per-block `trees` size dependency should be removed
from the client side. A client that fetches raw blocks already has the per-block output and
action counts, and the `ChainState` it fetches at the start of each batch already carries
each pool's starting size, so sizes can be accumulated locally.

This halves per-block RPC volume against any backend and removes the single largest source
of requests into the absent band. It is worth doing on its own merits and it is the
cheapest first step.

One trade-off to weigh: the node's sizes currently act as a loose cross-check on the
client's tree position. It is a weak check, since those sizes are unauthenticated on every
backend, but dropping it should be a deliberate decision rather than a silent side effect.

## Appendix A: measurements (2026-08-03)

Measured on a Mainnet fast-synced archive snapshot.

**Replay cost.** ~1.9 ms/block mean across the band, varying ~50x by height: 0.28 ms
median below 400k, 3.6 ms median through 1.6M–2.0M.

**Entry size.** 715 B measured. Earlier sizing used 1,489 B, the size of
`mainnet-frontier.bin`, as a conservative upper bound; that figure includes Sprout, which
this artifact omits. Adjacent entries share nearly all of their stored sibling hashes, so
delta encoding would shrink the artifact further if it ever mattered.

**Grid sizing at 3.42M heights.**

| Grid | Entries | Artifact | Cold request |
| --- | --- | --- | --- |
| Uniform, 50,000 blocks | ~68 | ~100 KB | minutes |
| Uniform, ~110 blocks (2 s worst case) | 30,527 | ~21.8 MB | ≤2 s |
| Cost-weighted, 2 s budget (**default**) | 3,380 | 3.25 MB | median 1.07 s, p90 2.50 s, max 4.71 s |
| Cost-weighted, max under 2 s | — | ~7.7 MB | ≤2 s |

The residual tail above the budget is variance rather than a missing model term. It does
not correlate with block bytes (r = -0.10) or with replay length, so tightening it costs
entries, not modelling.
