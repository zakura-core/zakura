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

## Status (2026-08-22)

| Component | State |
| --- | --- |
| Frontier derivation, root check, bounded cache | On `main` |
| Subtree-root artifact and `z_getsubtreesbyindex` serving | On `main` |
| Typed `HistoricalTreeUnavailable` in place of `null` / empty list | On `main` |
| Serving from a configured grid, bounded and root-verified | On `main` ([#775](https://github.com/zakura-core/zakura/pull/775)) |
| Grid generation in `zakura-checkpoints` | On `main` ([#751](https://github.com/zakura-core/zakura/pull/751)) |
| Embedded Mainnet grid, its provenance, and the release-state pipeline | PR [#703](https://github.com/zakura-core/zakura/pull/703) |
| Publishing the grid as a pinned asset crate instead of committing it | PR [#763](https://github.com/zakura-core/zakura/pull/763) |
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
(verified-commitment-trees design §16). Three of the four are committed. The grid is not: at
~2.1 MB regenerated on every refresh, committing it would add that much to the repository's
history every week, permanently. It is published instead as an exact-pinned crates.io package
built from `crates/zakura-assets/`, whose payload is generated at publish time and never enters
git. Its digest, size, and entry count are still recorded in `mainnet-vct-manifest.json`; the
pin and the record are held together by the checks in the next section.

Cargo can only resolve a version that already exists, so the package is published _before_ the
pull request that pins it is opened, and the refresh workflow does both in one run. A candidate
a reviewer rejects is a yanked, unreferenced version. That publish is unattended, which is
acceptable for the same reason everything else about this artifact is: the grid carries no trust
weight, and a consuming node re-checks every entry it anchors on.

### 5.1 Keeping the pin and the checkpoint together

Nothing here trusts the pin; these checks stop it from drifting away from the checkpoint the
rest of the release state describes.

- The version is `0.<last_checkpoint>.<revision>`, so the pin itself states which checkpoint the
  payload covers. `scripts/check-release-state.sh` requires that height to equal the manifest's
  `finalized_height`, requires the requirement to be exact rather than a range, and requires
  `Cargo.lock` to resolve it from crates.io with a checksum, which rules out a committed path or
  git override. All of that runs without cargo, on every PR, through `lint.yml`.
- `embedded_mainnet_final_frontiers_parse` hashes the bytes the dependency actually supplies and
  holds them against the manifest's `frontier_grid_sha256`, size, and entry count, and holds the
  payload's own declared checkpoint against the embedded checkpoint list. The crate's constants
  are checked too, so a crate that misdeclares its payload cannot pass by agreeing with itself.
- `import-release-state.py` proves each new grid is a byte prefix-extension of the one the
  repository currently pins, which the workflow materialises from the registry rather than the
  tree.
- Finally, none of the above is what makes a wrong grid safe. Every entry is checked against the
  node's own authenticated root for that height before it anchors anything, and a failed entry is
  skipped rather than fatal.

### 5.2 Verifying a published grid

Four levels, in increasing strength. The first three are available to anyone.

**A. The same bytes at every hop**, no node required:

```sh
V=<pinned version>; N=<published crate name>
curl -sLO "https://static.crates.io/crates/$N/$N-$V.crate"
sha256sum "$N-$V.crate"            # equals Cargo.lock's checksum for $N
tar xzf "$N-$V.crate"
sha256sum "$N-$V/src/mainnet-frontier-grid.bin"   # equals frontier_grid_sha256
cat "$N-$V/.cargo_vcs_info.json"   # the Zakura commit CI packaged from
```

Then re-resolve the bundle the manifest names and compare the same digest:

```sh
python3 .github/scripts/fetch-release-state.py \
    --meta-url <manifest bundle meta URL> \
    --meta-sha256 <manifest meta_sha256> \
    --output-dir /tmp/bundle \
    --metadata-out /tmp/resolution.json
```

`.cargo_vcs_info.json` names the Zakura commit the package was built from, and marks the tree
dirty: the payload is generated and gitignored, so cargo sees uncommitted files and the publish
passes `--allow-dirty`. The commit is what identifies the reviewed packaging code; the payload's
own identity comes from the digests above.

This proves byte identity across repository, registry, and bundle. It proves nothing about
whether the bytes are right.

**B. Independent reproduction** — the strongest check, and it trusts nobody. Entry placement is a
deterministic function of the chain rather than of timing, so a run against any Mainnet archive
node produces byte-identical output:

```sh
cargo run --release -p zakura-utils --bin zakura-checkpoints -- \
    --state-cache-dir <archive cache> \
    --mainnet-frontier-grid-checkpoint <height> \
    --mainnet-frontier-grid-output /tmp/regenerated.bin
cmp /tmp/regenerated.bin /tmp/bundle/mainnet-frontier-grid.bin
```

**C. Offline bundle consistency** — `zakurad verify-historical-treestates` checks the grid's
framing and that it covers the same checkpoint as the rest of the bundle, and proves the subtree
roots against the frontier.

**D. Continuous and automatic** — every entry is root-checked on every node, forever. This is the
only one of the four that cannot be skipped.

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

These are the numbers that motivated refitting the cost model: the 1,500 µs/block and
47 µs/commitment constants this run used overestimated replay cost and oversized the grid.
Refitting them to the measured 249 / 30 (see `docs/changelog/params.md`) and regenerating at
checkpoint 3,453,771 gives the committed artifact — 2,281 entries and 2,167,503 B — so the
sizes in the table above are superseded as a projection even though they remain the honest
record of this run.

`0 blocks replayed` is the legacy-archive path in §5 confirmed end to end: every entry came
from a stored per-height tree, and the 81 minutes is the cost-model scan reading each block
body once, not replay. That is the floor for a run that walks from genesis, and it is why such
a run is long even where no commitment is ever re-appended.

That figure is therefore a one-off. A resumed run scans only the blocks above the previous
grid's last entry, so the routine cost tracks how far the chain advanced since the last export
rather than the length of the chain.
