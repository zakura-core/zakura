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
| Frontier grid artifact and config gating | PR [#703](https://github.com/zakura-core/zakura/pull/703) |
| Grid generation and distribution through the release-state pipeline | PR [#735](https://github.com/zakura-core/zakura/pull/735) |
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
- Keep the artifact small enough to distribute through the existing release-state pipeline and
  embed in the binary. Mainnet uses the reviewed embedded grid by default; an explicit
  `state.historical_frontier_artifact` path overrides it for tests and custom deployments.
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
§17): magic, version, network byte, explicit record count, a SHA-256 over the non-digest
header fields and payload, and a parser that validates framing before any record is used.

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

`MAX_HISTORICAL_TREE_REPLAY_BLOCKS` (`zakura-state/src/constants.rs`) bounds what a single
request may cost from its nearest anchor. It is a serving backstop, not a correctness bound,
and not a setting: the grid an operator configures is what decides replay cost.

### 3.4 The subtree-root artifact

A completed subtree root is an interior node rather than a complete frontier, so it cannot
be checked by comparing it directly with `commitment_roots_by_height`. It is nevertheless
pinned by the final frontier. Ommers at levels at or above the tracked subtree height are
pairwise hashes of the completed subtrees they span, so folding the candidate roots in
index order must reproduce every corresponding ommer, and the frontier's own subtree-level
root checks the boundary case where it ends exactly at a subtree completion. A wrong,
missing, or extra root therefore fails verification without replaying any of the subtree's
65,536 leaves.

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
`null` treestate or an empty subtree list: no grid available for the network, no entry covering
the requested height, replay exceeding its bound, or a derived root failing its check. A pruned
node returns it immediately rather than walking the range until the first missing body.

Mainnet always has the embedded grid. Serving remains idle on a network with neither an embedded
grid nor a configured override, so a cold request can never replay the entire absent band. An
override that cannot be read, ends below this database's own fast-sync handoff, or whose entries
do not tile genesis through `last_checkpoint` at gaps of at most
`MAX_HISTORICAL_TREE_REPLAY_BLOCKS` fails at startup. Coverage is always from genesis, so a
mid-chain-only file is refused even on a node whose `U` matches that first entry. A grid entry that
fails its root check is skipped; derivation tries the next-lower cell rather than restarting at
genesis.

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
| `state.historical_frontier_artifact` | Optional path overriding the embedded Mainnet grid. |

Derivation itself is not configured. It is active when an archive node either currently runs
the VCT fast path or has a durable `vct_synced_below` marker from an earlier fast sync. The
marker keeps an existing absent band serviceable after sync settings change. Pruned mode and
legacy per-block recompute without a marker are false. The per-request replay backstop is the
`MAX_HISTORICAL_TREE_REPLAY_BLOCKS` constant.

## 5. Generation

Both artifacts come out of one `zakura-checkpoints` run, alongside the checkpoint list and
the final frontier, and none of it touches the consensus commit path. The coupling is not
cosmetic. A node refuses to start when the grid's checkpoint is below its own fast-sync
handoff, and a node's handoff comes from the embedded checkpoint list, so a grid that
advanced independently of that list would eventually fail closed on upgrade. Generating
every artifact for the checkpoint the same run selects makes that impossible by
construction.

**Frontier grid** — `zakura-checkpoints --mainnet-frontier-grid-output`, run against an
archive database. The database is opened as a read-only RocksDB secondary, so the node
does not have to be stopped. Each entry's frontiers come from whichever source the
database holds at that height: stored per-height trees where they exist, and a replay of
retained block bodies only across a fast-synced database's absent band `[U, H)`. Every
entry is then checked against the authenticated roots the database already holds, and a
failed check stops generation rather than publishing the entry.

A **legacy archive** node is therefore the natural generator: it has trees at every
height, so it reads the whole grid with no replay at all, and its coverage tracks its tip
rather than freezing at a handoff it no longer has. A fast-synced archive node can also
generate, at the cost of one whole-band replay (Appendix A: ~1.9 h on Mainnet). A pruned
node cannot, because it holds neither the trees nor the bodies below the checkpoint.

Spacing defaults to cost-weighted at a 2 s per-entry budget
(`--frontier-grid-target-cost-ms`); `--frontier-grid-spacing` produces a uniform grid,
which cannot bound the worst-case cold request at a sane size and is not recommended.

Cost-weighted spacing has a generation cost of its own: entry placement is decided by
each block's commitment count, so the generator reads every block body from genesis
once, whatever source its entries come from. That is why a legacy archive node is faster
than a fast-synced one but not fast in absolute terms — it skips the appends, not the
scan. The alternative would be a cheaper proxy for commitment counts, but the placement
function is part of the artifact's prefix contract, so changing it invalidates every
published grid.

The grid walk starts at genesis regardless of where the generating database's own
boundaries fall. That is what makes entry heights a function of the chain alone, so
exports from different databases, and from the same database at different tips, are
byte-prefix extensions of one another. The release pipeline checks exactly that property
before committing a new grid.

**Subtree roots** — the same run, where `zakura-checkpoints` extends the embedded artifact
with the rows the database retained after the previous checkpoint and proves the result
against the newly produced final frontier before returning it. It needs neither historical
block bodies nor the grid's pass. `zakurad verify-historical-treestates` repeats that proof
offline for a candidate bundle, and also checks the bundle's grid for framing and for
covering the same checkpoint as the rest of the bundle.

**Resuming.** `--mainnet-frontier-grid-input` carries a published grid's entries forward instead
of recomputing them. The cost accumulator resets at every emitted entry, so continuing at
`last carried entry + 1` places the remainder exactly where a walk from genesis would — the same
reset property that makes the checkpoint list resumable. Carried entries are re-checked against
the generating database's authenticated roots before they are accepted, so resuming inherits no
trust from the file. Two consequences: a routine run costs only the new tail rather than a scan
of the whole chain, and the output is a prefix-extension of the input by construction rather than
because two runs happened to agree on a cost budget.

A grid for a checkpoint already committed is produced by
`--mainnet-frontier-grid-checkpoint`, which emits nothing else. That is how the first grid is
introduced for a release state that predates it, without a checkpoint advance riding along.

**Distribution** — the release-state publisher uploads all four files as one immutable
bundle, and `update-release-state.yml` imports them into one reviewable draft PR
(verified-commitment-trees design §16). The grid is committed like the other artifacts,
with its digest, size, and entry count recorded in `mainnet-vct-manifest.json`. It carries
no trust weight either way: a consuming node re-checks every entry it anchors on.

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
this artifact omits. Adjacent entries share nearly all their ommers, so delta encoding
would shrink the artifact further if it ever mattered.

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

## Appendix B: the first full generation run (2026-08-18)

Appendix A's grid sizing is a projection. The first complete run, on the Mainnet legacy
archive node at checkpoint 3,449,371, measured:

| | Projected (Appendix A) | Measured |
| --- | --- | --- |
| Entries | 3,380 | 5,472 |
| Artifact | 3.25 MB | 4,763,633 B |
| Entry size | 715 B | 870 B |
| Blocks replayed | — | 0 |
| Wall clock | — | 4,873 s |

Both figures run about 60% above the projection, so an implementation planning against
Appendix A should use these instead. The 2 s per-entry budget is unchanged; what moved is
how many entries that budget buys.

`0 blocks replayed` is the legacy-archive path in §5 confirmed end to end: every entry came
from a stored per-height tree, and the 81 minutes is the cost-model scan reading each block
body once, not replay. That is the floor for a run that walks from genesis, and it is why such
a run is long even where no commitment is ever re-appended.

That figure is therefore a one-off. A resumed run scans only the blocks above the previous
grid's last entry, so the routine cost tracks how far the chain advanced since the last export
rather than the length of the chain.
