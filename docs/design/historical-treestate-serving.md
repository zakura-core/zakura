# Historical treestate serving on fast-synced nodes

Status: proposed

## 1. Summary

A verified-commitment-trees (VCT) fast-synced node never builds the per-height
Sapling/Orchard/Ironwood note-commitment trees below the checkpoint handoff. That is
deliberate and consensus-safe (see
[verified-commitment-trees.md](verified-commitment-trees.md) §2, §7), but it removes the
data behind `z_gettreestate`, `z_getsubtreesbyindex`, and the `trees` field of `getblock`
for that band. Wallets that sync against a fast-synced archive snapshot cannot start.

The design in one paragraph: publish a **coarse artifact of per-pool frontiers** at a
sparse height grid alongside the existing release-state bundle, and have the node derive
any other height on demand by replaying retained block bodies forward from the nearest
entry, memoizing what it derives. Every frontier the node accepts, whether read from the
artifact or derived locally, is checked by comparing its root against the already
authenticated root in `commitment_roots_by_height`. That check makes the frontier
artifact carry **no trust weight**, which is what lets it be coarse, small, and
distributed outside the binary. Completed **subtree roots** cannot ride the same check
(§4.6), so they ship separately, as a small reviewed artifact in the committed bundle at
the same trust level as the checkpoint list.

This replaces increments 7 and 8 of the VCT roadmap (an indexing follower that reruns the
full per-block recompute off the critical path) with something roughly two orders of
magnitude smaller in both artifact size and node-side work.

## 2. Problem

### 2.1 The absent band

The fast commit path writes anchors, the history tree, and the
`commitment_roots_by_height` serving index, then returns before writing per-height trees
or subtrees (`zakura-state/.../zakura_db/shielded.rs`, the `fast_write.anchor_roots`
branch). The result is a half-open band `[U, H)` where those column families are empty:

- `U` is `vct_upgrade_height`, the first height this binary committed. For a snapshot
  synced from scratch it is effectively genesis.
- `H` is `vct_synced_below`, the checkpoint handoff. The currently embedded Mainnet
  frontier puts it at 3,418,406, which is also the last entry in
  `main-checkpoints.txt`. Shipped snapshots can carry an older marker: the
  2026-08-02 Mainnet archive snapshot records `H = 3,358,006`, because it was built
  by a binary whose handoff was earlier. `H` is read from the database, never
  assumed, so this only matters when reasoning about a particular snapshot — as it
  does for the §4.6 table below.

Heights at or above `H` get trees again from semantic sync, so the gap is exactly the
checkpointed band.

Three datasets are missing in that band:

| Missing | Column family | Consumer |
| --- | --- | --- |
| Per-height pool frontiers | `{pool}_note_commitment_tree` | `z_gettreestate` |
| Completed subtree roots | `{pool}_note_commitment_subtree` | `z_getsubtreesbyindex` |
| Per-height tree sizes | derived from the trees | `getblock` verbosity 1, `getblockheader` |

### 2.2 Observed failure modes

Two of the three fail in ways worth calling out separately, because one is loud and one
is not.

**Loud.** `getblockheader` and `getblock` map an absent tree to
`ok_or_misc_error("missing Sapling tree")` / `"missing Orchard tree"`. Any verbose block
query inside the band is a hard RPC error. This is what actually stops a wallet, because
lightwalletd-style clients fetch the `trees` sizes once per block.

**Silent, and worse.** `z_gettreestate` maps an absent tree to a JSON `null` rather than
an error. Clients following the lightwalletd contract treat an absent tree as the _empty_
tree, so a wallet derives a birthday anchor asserting an empty commitment tree at a height
deep in the chain. No error is raised at the point of corruption. The typed guard for this
already exists (`ZakuraDb::vct_historical_tree_unavailable`) but has no non-test callers.

`z_getsubtreesbyindex` sits in between: the read path returns an empty list when the
starting index is absent, so a client seeds nothing and fails later, without a clear cause.

### 2.3 Why the indexing-follower lane is the wrong size

Roadmap increments 7 and 8 propose relocating the per-height trees and subtree column
families onto an async follower that reruns the full per-block recompute off the consensus
path. That restores everything, but it pays back in full the cost fast sync was built to
avoid, on every node, and it does so before knowing which heights anyone will ask for.

Measured against actual demand it is heavily oversized. A wallet issues one treestate
request per scan batch. At librustzcash-driven batch sizes that is a few hundred requests
across an entire from-activation Mainnet scan, at heights that are sparse and, critically,
sequential.

## 3. Goals and non-goals

Goals:

- Restore `z_gettreestate`, `z_getsubtreesbyindex`, and verbose `getblock`/`getblockheader`
  across the absent band on an archive-mode fast-synced node.
- Add no **new** trust surface. Everything served is either verified against
  `commitment_roots_by_height` or rests on the review-based trust the node already
  extends to the committed bundle (checkpoint list, embedded frontier). Nothing rests on
  an unverified runtime artifact.
- Keep the artifact small enough to distribute through the existing release-state
  pipeline, without embedding it in the binary.
- Keep the consensus commit path untouched.

Non-goals:

- **Pruned mode.** On-demand derivation needs retained block bodies. A pruned node still
  cannot serve historical treestates, which is unchanged from today.
- **Historical Sprout treestates.** `z_gettreestate` already returns no Sprout data at any
  height on every node, because Sprout stores only its tip frontier. Excluding Sprout
  removes the hardest part of artifact generation and regresses nothing. Sprout consensus
  state is unaffected: the fast path already reconstructs it locally.
- **Eliminating the recompute globally.** The ordered pass still happens once, on the
  publisher host. This design removes it from every consuming node, not from the world.

## 4. Design

### 4.1 The frontier anchor artifact

A new artifact in the release-state bundle: Sapling, Orchard, and Ironwood frontiers at a
sparse grid of heights spanning `[genesis, H]`, ending exactly on the bundle's checkpoint
height so it abuts the existing embedded final frontier.

Grid spacing is a tunable, not a fixed part of the format. Sizing, using the measured
1,489-byte `mainnet-frontier.bin` as a conservative per-entry upper bound (it includes
Sprout, which this artifact omits):

| Spacing | Entries at 3.42M | Artifact | Worst-case cold replay |
| --- | --- | --- | --- |
| Checkpoint grid (avg ~240 blocks) | 14,162 | ~21 MB | ≤400 blocks |
| 10,000 blocks | ~342 | ~500 KB | ≤10,000 blocks |
| 50,000 blocks | ~68 | ~100 KB | ≤50,000 blocks |

Entries below a pool's activation height are empty and cost nothing. Adjacent entries
share nearly all their ommers, so delta encoding would shrink this further if it ever
mattered.

**Measured, 2026-08-03 (supersedes the recommendation this section originally made).**
The table above assumed a coarse grid, on the §4.3 argument that spacing only costs the
first request of a sweep. That argument holds; its magnitude does not. Replay costs
~1.9 ms/block on average across Mainnet and varies ~50x by height, so a 50,000-block
cold replay runs into minutes — far outside any RPC budget. Measured entry size is also
715 B, not the 1,489 B used above as a conservative bound, because that figure includes
Sprout.

A _uniform_ grid cannot bound the worst case at a sane size: 2 s worst-case needs
~110-block spacing, 30,527 entries, ~21.8 MB. Spacing entries by estimated replay cost
instead concentrates them where blocks are expensive. At a 2 s per-entry budget that is
3,380 entries and 3.25 MB, measuring median 1.07 s, p90 2.50 s, max 4.71 s per cold
request.

The residual tail is variance rather than a missing model term — it does not correlate
with block bytes (r = -0.10) or replay length — so tightening it costs entries, not
modelling: ~7.7 MB would be needed to hold max under 2 s.

Format should follow the conventions already established by the Sprout history artifact
(§17 of the VCT design): magic, version, network byte, explicit record count, a SHA-256
over the payload, and a parser that validates framing before any record is used.

### 4.2 Verification: the artifact carries no trust

This is the property the whole design rests on.

A note-commitment root is a binding commitment to its frontier. The VCT commit path
already exploits this once, at the handoff, where it accepts the embedded final frontier
only if `frontier.root() == verified root` for each pool (VCT design §7, step 3).

Every height in the band already has a root in `commitment_roots_by_height`, and that
index is gap-free from `U` and authenticated against the node's own header chain
(VCT design §9, and
[header-sync-vct-root-authentication.md](header-sync-vct-root-authentication.md)).

So the same check generalises: **any frontier claimed at height `h`, from any source, is
accepted only if its root equals the stored authenticated root at `h`.** A corrupt,
truncated, or hostile artifact is rejected rather than absorbed.

Three consequences:

1. The frontier artifact needs no review-based trust, unlike the checkpoint list, the
   Sprout history artifact, and the subtree-root artifact (§4.6). It does not need to be
   committed source or embedded in the binary. This is also why the two artifacts are
   split: only the entries that can be root-checked belong in the unverified one.
2. It can be fetched at runtime from any source, including eventually a peer, without
   weakening anything. That is a natural fit for the cross-client spec work in roadmap
   increment 9.
3. Locally derived frontiers get the same check for free, so §4.3 is self-validating too.

### 4.3 On-demand derivation and memoization

To serve an arbitrary height `h` in the band: take the nearest artifact entry at or below
`h`, replay the note commitments from the retained block bodies in `(entry, h]`, verify
the resulting root against the stored root at `h`, and serve.

Two properties make a coarse grid viable:

**Wallet access is sequential.** Scan batches are contiguous: a wallet's next request is
for the block immediately preceding the range it just finished, which is the block it just
scanned to. A node that memoizes its most recently derived frontier replays _forward by
one batch_, not from a distant grid point.

**Therefore spacing only costs the first request of a sweep.** Steady-state replay cost is
bounded by the client's batch size, independent of grid spacing. This is why the
recommendation is to go coarse on the artifact and spend the effort on the cache.

Derived frontiers should be memoized in a bounded cache, and may optionally be persisted.
A node that persists them accumulates a demand-shaped index over time, with no upfront
pass and no follower lane.

### 4.4 Serving the three RPCs

- **`z_gettreestate`** is served directly by §4.3, or reconstructed client-side from
  `z_gettreestateanchor` (§6a) on a node that would rather not replay.
- **`z_getsubtreesbyindex`** is served from the embedded subtree-root artifact (§4.6),
  never by replay. Replay is the wrong tool here: wallets doing spend-before-sync fetch
  the full subtree list in one request at startup, from index 0 to the tip, so a
  replay-backed cold response would be close to a full-band replay rather than one batch
  forward. The sequential-access argument that makes §4.3 cheap does not apply.
- **`getblock` / `getblockheader` sizes** need only a count, not a frontier. A frontier
  knows its own position, so the size at `h` is the nearest entry's `tree_size()` plus the
  commitments added in between, which requires no hashing at all.

### 4.5 Failure policy

When the artifact is absent, or an entry covering the requested height is missing, or a
derived root fails its check, the RPC must return the typed archive-mode error via
`vct_historical_tree_unavailable`, never a `null` treestate or an empty subtree list. This
should be wired up regardless of whether the rest of this design proceeds, since it
converts today's silent-corruption path into a diagnosable one (§2.2).

### 4.6 The subtree-root artifact

Subtree roots need their own artifact and their own trust story, because the §4.2 check
does not extend to them. A completed subtree root is an interior node of the tree: every
authenticated root in `commitment_roots_by_height` commits to it, but extracting or
confirming it from a root requires replaying the subtree's 65,536 leaves, which is the
work being avoided. Nor is there a supplied-and-verified channel as there is for
per-block roots: headers commit to tree roots through the chain history tree, but nothing
in the protocol commits to subtree roots. They exist only as a local by-product of the
tree maintenance the fast path deliberately skips, which is why they cannot be recorded
during fast sync at all.

**Qualified (2026-08-03).** The first sentence holds for a _serving_ node, which is why the
artifact still ships at review-level trust. It does not hold for the **publisher**, which
replays the band regardless: subtree roots fall out of that replay as interior nodes, pinned
between two root-checked grid entries. Verified against ground truth — above a fast-synced
node's handoff the database does store subtree rows, and replaying `(3,358,006, 3,432,538]`
reproduces all 5 of them exactly. So the generator can verify what it publishes, which is
stronger than the reproducibility gate below; what is unchanged is that a _consumer_ still
cannot check a record cheaply, except opportunistically as §4.6's second bullet describes.

The set is small and static. Decoding the embedded final frontier at the handoff
(3,418,406) gives the pool positions directly:

| Pool | Commitments at handoff | Completed subtrees |
| --- | --- | --- |
| Sapling | 73,934,658 | 1,128 |
| Orchard | 50,268,970 | 767 |
| Ironwood | 0 (activates at/after handoff) | 0 |

At a 32-byte root plus a completion height per record, ~1,895 records is roughly 70 KB
framed, an order of magnitude smaller than even the coarse frontier grid. Measured against
the 2026-08-02 snapshot, whose handoff is the earlier 3,358,006: 1,127 Sapling and 763
Orchard records, 71,879 bytes framed — close enough to confirm the estimate. It grows
append-only by a handful of subtrees per month at current usage.

Because it cannot be self-verified, this artifact ships in the **reviewed, committed
bundle**, at the same trust level as the checkpoint list and the embedded frontier, not
in the runtime-fetched frontier artifact. Two qualifications on that trust:

- At ~1,900 records, "review" cannot mean eyeballing hashes. The real gate is
  reproducibility: two independent exporter runs must agree byte-for-byte, the same
  determinism gate phase C applies to the frontier artifact.
- Replay crosses subtree boundaries anyway. Whenever an on-demand derivation (§4.3)
  passes a completion position, the derived root is compared against the embedded one and
  a mismatch is treated as artifact corruption (§4.5). Wallet sweeps cross every boundary
  in the band, so the list converges from trusted to verified over a node's lifetime at
  no dedicated cost.

## 5. Generation

Generation extends the existing offline exporter (VCT design §16.1), which already runs
read-only against a quiesced database on the publisher host and already emits the
checkpoint list and final frontier as one coupled bundle.

One caveat on reuse: `produce_final_frontiers` requires the requested height to equal the
database tip and errors with `RequestedHeightIsNotTip` otherwise, and
`produce_settled_final_frontiers` handles below-tip heights only through a Sprout settling
scan. Because this artifact excludes Sprout, it needs a new and simpler below-tip producer
that reads the three per-pool trees at a height and skips the Sprout pairing entirely.

~~The publisher host must run an **archive, legacy-synced** node, since the exporter reads
per-height trees from the finalized database.~~ **Superseded (2026-08-03):** the exporter
replays block bodies and checks each grid entry against the authenticated roots in
`commitment_roots_by_height`, rather than reading stored per-height trees. Any **archive**
node can therefore generate the artifact, fast-synced included — which is how the current
Mainnet artifacts were produced. A pruned node still cannot, because replay needs retained
bodies.

This remains the design's central trade: the ordered pass happens once, on one host, and is
verified independently by every consumer.

Artifact entries should follow the same append-only grid contract as the checkpoint list,
so successive bundles remain byte-for-byte prefix-compatible and the import workflow can
verify updates as pure appends.

The same exporter pass emits the subtree-root artifact (§4.6). On the legacy-synced
publisher host this is a plain read of the existing `{pool}_note_commitment_subtree`
column families, no new computation. It follows the same framing conventions and the
same append-only contract, but lands in the committed bundle rather than the
runtime-distributed one.

## 6. Client-side change

Independent of everything above, the per-block `trees` size dependency should be removed
from the client side. A client that fetches raw blocks already has the per-block output
and action counts, and the `ChainState` it fetches at the start of each batch already
carries each pool's starting size. Sizes can therefore be accumulated locally.

This halves per-block RPC volume against any backend, and removes the single largest
source of requests into the absent band. It is worth doing on its own merits and it is the
cheapest first step.

The trade-off to weigh: the node's sizes currently act as a loose cross-check on the
client's tree position. It is a weak check, since those sizes are unauthenticated on every
backend, but dropping it should be a deliberate decision rather than a silent side effect.

**Prototyped (2026-08-05).** zecd derives sizes locally _only_ inside the band — that is, only
when the node cannot serve the tree state just below a scan range, which is exactly when its
per-block `trees` query would fail anyway. Everywhere else the node's sizes are still used, so the
cross-check is kept where it costs nothing and dropped only where it does not exist.

## 6a. Wallet-side reconstruction

A second consumer of §4.2's verification property, and the one that makes the absent band
serviceable without the node paying for §4.3 at all: **the client can run the replay.**

The node exposes a Zakura-specific `z_gettreestateanchor(hash | height)` returning, in one state
read so the pieces cannot straddle a reorg:

- the canonical target height and hash, its header time, and the three authenticated roots;
- the highest frontier at or below the target that the node has already verified — from its
  memo, from the published grid, or from the last stored tree below `U` — with its own canonical
  hash and `finalState` in `z_gettreestate`'s encoding;
- `replayFrom`, the first height the client must replay.

`z_gettreestate` is untouched: same response, same errors, same codes. A client tries it first and
falls back here only on the typed historical-tree-unavailable message from §4.5.

The client then replays the canonical blocks from `replayFrom` to the target and accepts the result
only if all three roots match. **This is the same check as §4.2, run on the other side of the RPC**,
so it carries the same weight: a frontier that reproduces an authenticated root is the frontier,
and a wrong or stale anchor cannot survive it. The trust boundary therefore does not move. The
node's roots are authenticated against its own header chain and the client already relies on that
node for its view of the chain; what it does _not_ have to do is believe an unverified frontier.

Three properties follow, and they are why this is worth having alongside §4.3 rather than instead
of it:

1. **The node does no replay.** Answering costs one root lookup plus, at most, one root check of a
   published grid entry. The measured per-request cost of §4.3 (median 1.07 s, p90 2.50 s) does not
   apply, so an operator can serve the band without a replay budget or an RPC-timeout risk.
2. **The client's cache is self-invalidating.** A wallet memoizes each verified end state and
   anchors the next batch on it. A stale entry — from an abandoned fork, or from before a reorg —
   cannot corrupt anything, because it would have to replay into the _new_ chain's authenticated
   roots. It can only make a request fail, never succeed wrongly. No explicit reorg invalidation is
   needed.
3. **It composes with the anchor grid.** The client's anchor is whichever is higher: its own
   previous batch end, or whatever the node offers. Sequential wallet access (§4.3) makes the first
   the common case, so grid spacing again only costs the first request of a sweep.

The cost the client pays is the replay itself, and the block bodies it needs for it. A pruned node
still cannot supply those, so this widens _who can serve_ the band but not _which nodes_ can.
Subtree roots (§4.6) are unaffected: they cannot be checked this way on either side, so they still
ship as a reviewed artifact.

## 7. Open questions

~~**Replay cost is unmeasured.**~~ **Answered (2026-08-03).** Measured on a Mainnet
fast-synced archive snapshot: ~1.9 ms/block mean across the band, varying ~50x by height
(0.28 ms median below 400k, 3.6 ms median through 1.6M-2.0M). Slow enough that the §4.1
recommendation inverts — see the measured note there.

**Subtree boundary alignment** is resolved by §4.6: subtree roots are published, not
derived on demand, so no replay ever needs to stop at a mid-block position. The only
remaining boundary work is the opportunistic check, which compares against the frontier's
level-16 node as a derivation happens to cross a completion position.

**Cache sizing and persistence** are unspecified. Whether derived frontiers should be
persisted or held in a bounded in-memory cache depends on the same benchmark.

## 8. Phases

Two structural points shape the plan:

- **Phase A and phase C are the same pass.** The experiment that validates the design is
  also the generator prototype. One read-only walk from genesis produces every
  intermediate frontier, checks every root, and times itself.
- **Regtest cannot currently produce an absent band.** `FixtureSource` is a `#[cfg(test)]`
  crate-local source (VCT design §5.3), and the production path needs an embedded frontier
  and a checkpoint list, so it engages on Mainnet only. This is a decision point in
  phase D.

### 8.1 Phase A — derisk

Read-only, no changes to the consensus path. Ordered by cost.

**A1 — Snapshot inventory.** On a real fast-synced archive snapshot, dump `U` and `H`,
confirm `commitment_roots_by_height` is gap-free across `[U, H)`, and confirm block bodies
are retained across the band.

- Answers: are the two inputs this design assumes actually present?
- Kill criterion: index gaps, or bodies absent in archive mode. Derivation would have
  nothing to build from.

**A2 — Invariant and timing pass.** One walk from genesis across the band on an archive
snapshot, appending commitments block by block and comparing the recomputed root against
the stored root at every height. Timed, bucketed by height.

- Answers: does `frontier.root() == stored root` hold at arbitrary heights, and what is the
  per-block replay cost curve?
- Exit criteria: 100% root match across the band, plus a cost curve sufficient to choose
  the §4.1 grid.
- Kill criterion: any mismatch. The verification property in §4.2 is the design. Without
  it, the artifact becomes trusted input and needs a different review.

**A3 — Reproduce the client failure.** Point a wallet at a fast-synced snapshot and
capture the exact error sequence.

- Answers: confirms the §2.2 failure model and fixes the regression-test targets.

**Provisioned legacy publisher candidate (2026-08-03).** A fresh Mainnet node is syncing
from genesis specifically to provide the archive, non-VCT database required by phase C
and the phase D differential test:

- DigitalOcean droplet `roman-zakura-archive-vct-off` in the `misc` project, `nyc3`,
  provisioned from the latest `zakura-pr-node-*` image
  (`zakura-pr-node-20260727-0814`).
- `c-8` compute (8 dedicated vCPUs, 16 GiB RAM) with a dedicated 1,000 GiB ext4 volume
  mounted at `/mnt/data`; state is stored under `/mnt/data/zakura-cache`.
- Public addresses: IPv4 `104.131.174.28`, IPv6
  `2604:a880:800:14:0:3:5157:1000`. Roman's and `zebra-ci`'s public SSH keys are
  installed.
- Built from `main` commit `dc4ec28dc6ad9782d1e43ad69025fdf7faccfea3`
  (`zakurad 1.0.4+59.gdc4ec28`). The service config at
  `/etc/zakura/zakura.toml` uses `p2p_stack = "dual"`,
  `storage_mode = "archive"`, `checkpoint_sync = true`, and
  `vct_fast_sync = false`.
- The systemd service is `zakurad`; logs are written to
  `/var/log/zakura/zakura.log`. RPC and metrics are loopback-only on ports 8232
  and 9999. Initial verification confirmed that the node was active and advancing from
  genesis.

The 1,000 GiB allocation matches the state volume on the existing archive snapshot host,
but capacity remains an operational checkpoint until this fresh sync reaches tip.

### 8.2 Phase B — minimal POC

Deliberately narrow: one RPC, one height, no artifact.

1. **Wire up the typed error** (§4.5) in `z_gettreestate`, `getblock`, and
   `getblockheader`. Independently shippable.
2. **Derive on demand behind a config flag.** Serve `z_gettreestate` at a single height in
   the band by replaying from an empty genesis frontier, verified against the stored root.
   No cache, no grid, no artifact, no subtrees.

Exit criteria: `z_gettreestate <h>` returns a treestate with a matching root on a
fast-synced snapshot, where it previously returned `null`.

This proves the serving seam. It will be slow, which is expected: A2 already determines
what grid is needed to fix that.

### 8.3 Phase C — re-indexing

1. Promote the A2 pass into an exporter subcommand emitting the frontier artifact at the
   chosen grid, plus the subtree-root artifact read from the publisher's subtree column
   families (§4.6, §5). Requires the new below-tip pool-frontier producer described in §5.
2. Determinism gates for both artifacts: two runs byte-identical, and an export at a
   later tip is a pure prefix-append of the earlier one, matching the checkpoint grid
   contract. For the subtree-root artifact this gate carries the trust argument, so it is
   not optional.
3. Node-side load and verification: every frontier entry checked against stored roots and
   rejected on mismatch; the subtree-root artifact validated for framing and digest, and
   wired into `z_getsubtreesbyindex`.
4. Anchor selection and memoization of derived frontiers, replacing the phase B genesis
   replay, including the opportunistic subtree-root check at completion positions.

Exit criteria: a cold `z_gettreestate` anywhere in the band completes within the RPC
budget, and a sequential sweep runs at cache speed.

### 8.4 Phase D — end-to-end proof

The decision noted above: either invest in letting regtest produce an absent band (a
test-only source and a synthetic frontier), or prove only against Mainnet snapshots.

The recommendation is the regtest investment. It is the difference between proving this
once by hand and holding it with a CI regression test, and the wallet side already has a
regtest harness to drive.

1. **Regtest loop.** Produce a small absent band, run wallet init and a full scan against
   it, assert success.
2. **Differential test.** The same wallet and birthday, deep in the band, synced against a
   fast-synced snapshot and against a legacy archive node. Assert identical resulting
   wallet state: notes, balances, and witnesses.

Exit criteria: identical wallet state from both backends.

The differential test is what actually proves this design, because it catches subtly wrong
frontiers that a "does it start" test would pass.

### 8.5 Sequencing

A1 gates A2. A2 gates the grid choice and therefore all of phase C. Phase B step 1 and the
client-side change in §6 depend on none of this and can proceed in parallel.

A1 and A3 are short. A2 is the long pole in derisking, and phase C should not be committed
to before A2 reports.

## 9. Relationship to the VCT roadmap

This supersedes increments 7 and 8 as specified. Increment 9 (cross-client spec) gains a
natural extension: because the artifact is self-verifying, its format and verification rule
are publishable alongside the root payload schema, and any client can serve or consume it
without a trust relationship.
