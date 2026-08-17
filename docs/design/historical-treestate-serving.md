# Historical treestate serving on fast-synced nodes

Status: proposed

## 1. Summary

A verified-commitment-trees (VCT) fast-synced node never builds the per-height
Sapling/Orchard/Ironwood note-commitment trees below the checkpoint handoff. That is
deliberate and consensus-safe (see
[verified-commitment-trees.md](verified-commitment-trees.md)), but it removes the
data behind `z_gettreestate`, `z_getsubtreesbyindex`, and the `trees` field of `getblock`
for that band. Wallets that sync against a fast-synced archive snapshot cannot start.

The design in one paragraph: publish a **coarse artifact of per-pool frontiers** at a
sparse height grid alongside the existing release-state bundle, and have the node derive
any other height on demand by replaying retained block bodies forward from the nearest
entry, caching what it derives. Every frontier the node accepts, whether read from the
artifact or derived locally, is checked by comparing its root against the already
authenticated root in `commitment_roots_by_height`. That check makes the frontier
artifact carry **no trust weight**, which is what lets it be coarse, small, and
distributed outside the binary.

Completed **subtree roots** ship separately in the committed bundle for efficient serving,
but carry no additional trust weight either. Subtree roots are likewise verified against the corresponding authenticated final frontier during export, independently before publication, and once per process when a node loads the embedded artifact.

## 2. Problem

### 2.1 The absent band

The fast commit path writes anchors, the history tree, and the
`commitment_roots_by_height` serving index, then returns before writing per-height trees
or subtrees (`zakura-state/.../zakura_db/shielded.rs`, the `fast_write.anchor_roots`
branch). The result is a half-open band `[U, H)` where those column families are empty:

- `U` is `vct_upgrade_height`, the first height this binary vct-committed. For a snapshot
  synced from scratch it is effectively genesis.
- `H` is `vct_synced_below`, the last checkpoint. The currently embedded Mainnet
  frontier puts it at 3,418,406, which is also the last entry in
  `main-checkpoints.txt`. Shipped snapshots can carry an older marker: the
  2026-08-02 Mainnet archive snapshot records `H = 3,358,006`, because it was built
  by a binary whose handoff was earlier. `H` is read from the database, never
  assumed, so this only matters when reasoning about a particular snapshot.

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

## 3. Goals and non-goals

Goals:

- Restore `z_gettreestate`, `z_getsubtreesbyindex`, and verbose `getblock`/`getblockheader`
  across the absent band on an archive-mode fast-synced node.
- Add no **new** trust surface. Every served frontier and subtree root is
  cryptographically pinned by an authenticated frontier or root. Nothing rests on an
  unverified runtime artifact.
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

1. The frontier artifact needs no review-based trust. It does not need to be committed
   source or embedded in the binary. The subtree-root artifact is embedded for its release
   and serving lifecycle, not because its roots are trusted without verification (§4.6).
2. It can be fetched at runtime from any source, including eventually a peer, without
   weakening anything. That is a natural fit for the cross-client spec work in roadmap
   increment 9.
3. Locally derived frontiers get the same check for free, so §4.3 is self-validating too.

### 4.3 On-demand derivation and caching

To serve an arbitrary height `h` in the band: take the nearest artifact entry at or below
`h`, replay the note commitments from the retained block bodies in `(entry, h]`, verify
the resulting root against the stored root at `h`, and serve.

Two properties make a coarse grid viable:

**Wallet access is sequential.** Scan batches are contiguous: a wallet's next request is
for the block immediately preceding the range it just finished, which is the block it just
scanned to. A node that caches its most recently derived frontier replays _forward by
one batch_, not from a distant grid point.

**Therefore spacing only costs the first request of a sweep.** Steady-state replay cost is
bounded by the client's batch size, independent of grid spacing. This is why the
recommendation is to go coarse on the artifact and spend the effort on the cache.

Derived frontiers should be kept in a bounded cache, and may optionally be persisted.
A node that persists them accumulates a demand-shaped index over time, with no upfront
pass and no follower lane.

### 4.4 Serving the three RPCs

- **`z_gettreestate`** is served directly by §4.3.
- **`z_getsubtreesbyindex`** is served from the embedded subtree-root artifact (§4.6),
  never by replay. That artifact and its serving path have already landed; only the
  frontier grid and the derivation seam remain. Replay is the wrong tool here: wallets doing spend-before-sync fetch
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
converts today's silent-corruption path into a diagnosable one (§2.2). A pruned node with
derivation enabled is the same failure: it cannot replay retained bodies, so it returns
that typed error immediately rather than walking the range until the first missing body.

### 4.6 The subtree-root artifact

A completed subtree root is an interior node rather than a complete frontier, so it cannot
be checked by comparing it directly with `commitment_roots_by_height`. It is nevertheless
pinned by the final frontier. Ommers at levels at or above the tracked subtree height are
pairwise hashes of the completed subtrees they span. Folding the candidate roots in index
order must reproduce every corresponding ommer; the frontier's own subtree-level root
checks the boundary case where it ends exactly at a subtree completion. A wrong, missing,
or extra root therefore fails verification without replaying any of the subtree's 65,536
leaves.

The release-state pipeline extends the embedded prefix with retained database rows and
verifies the complete result against the newly produced final frontier before returning
either artifact. The `verify-historical-treestates` command can repeat that proof offline
for a candidate bundle. A consuming node verifies the embedded artifact once per process
against its embedded frontier before exposing it to the read service; RPC requests do not
repeat the proof.

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

The artifact ships in the committed bundle rather than the runtime-fetched frontier
artifact because it is a small, append-only serving index coupled to the release
checkpoint. Its location is an operational choice, not a source of trust.

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
bodies. When a database upgraded to VCT above genesis, generation anchors on the stored
per-pool trees at `U - 1` and replays only the absent band `[U, H)`.

This remains the design's central trade: the ordered pass happens once, on one host, and is
verified independently by every consumer.

Artifact entries should follow the same append-only grid contract as the checkpoint list,
so successive bundles remain byte-for-byte prefix-compatible and the import workflow can
verify updates as pure appends.

~~The same exporter pass emits the subtree-root artifact (§4.6).~~ **Superseded
(2026-08-17):** subtree generation moved to the release-state pipeline, where
`zakura-checkpoints` extends the embedded artifact with the rows the database retained
after the previous checkpoint and proves the result against the new frontier. It needs
neither historical block bodies nor a shared pass with the grid, so the two generators are
now independent: `zakurad export-historical-treestates` emits only the frontier grid.

The command defaults to a cost-weighted grid at a 2 s per-entry budget
(`--target-cost-ms 2000`). `--spacing` still produces a uniform grid; that mode cannot
bound the worst-case cold request at a sane size and is not recommended.

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

## 7. Open questions

~~**Replay cost is unmeasured.**~~ **Answered (2026-08-03).** Measured on a Mainnet
fast-synced archive snapshot: ~1.9 ms/block mean across the band, varying ~50x by height
(0.28 ms median below 400k, 3.6 ms median through 1.6M-2.0M). Slow enough that the §4.1
recommendation inverts — see the measured note there.

**Subtree boundary alignment** is resolved by §4.6: subtree roots are published, not
derived on demand, so no replay ever needs to stop at a mid-block position. The complete
subtree artifact is proven directly against the final frontier.

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
   chosen grid. The independent release-state pipeline extends the subtree-root artifact
   from retained database rows and proves it against the new final frontier (§4.6, §5).
2. Determinism gates for both artifacts: two runs byte-identical, and an export at a
   later tip is a pure prefix-append of the earlier one, matching the checkpoint grid
   contract.
3. Node-side load and verification: every frontier entry checked against stored roots and
   rejected on mismatch; the subtree-root artifact proven against the final frontier and
   wired into `z_getsubtreesbyindex`.
4. Anchor selection and caching of derived frontiers, replacing the phase B genesis
   replay.

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
