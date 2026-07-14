# Zakura v1.0 Launch Audit — Top 5 Bugs to Fix

**Date:** 2026-07-13
**Scope:** Custom Zakura subsystems layered on the Zebra fork — network sync (header/block/legacy gossip, transport), consensus verification, state (VCT / commitment-aux), untrusted-input deserialization, RPC, and peer/resource management.
**Method:** Five parallel focused code auditors, each tracing untrusted-input paths to their sinks, followed by direct verification of every finding cited below against the source.
**Excluded by request:** the already-fixed "Mainnet/Testnet accepts Regtest-shaped Equihash proofs" issue (PR #111). Verified as solved: `Solution::check` now binds `(n, k)` to the network, so a 36-byte Regtest solution is rejected on Mainnet/Testnet (`zakura-chain/src/work/equihash.rs:105`).

## Headline

The consensus core (PoW/difficulty, checkpoints, subsidy, activation heights, branch IDs, Orchard flag rejection, NU7 branch-id gating) is faithful to upstream Zebra — no injected consensus-fork bug was found there. The real launch risk lives in the **new Zakura code**: the header-sync wire protocol, the VCT/commitment-aux serving path, the state on-disk format versioning, and the legacy gossip correlation logic. The five below are ranked by how painful they are if hit in production, weighing both attacker-exploitability and the likelihood of encountering them during a normal launch rollout.

**Team disposition (2026-07-13):** #1 is accepted as **known** and will not be re-raised (Zakura's requester always requests roots). #2 is a **non-issue** for this launch — it ships a fresh stack with no prior state or migration path. That leaves **#3 (header-sync difficulty filter), #4 (VCT zero-aux serving), and #5 (legacy gossip tx correlation)** as the actionable pre-launch fixes, plus the honorable mentions.

---

## 1. Header-sync wire codec rejects every valid roots-free `Headers` message

> **Status: KNOWN — ignore going forward.** Accepted by the team as known behavior. Zakura's own requester always sets `want_tree_aux_roots: true`, so peer-to-peer sync is unaffected in the shipped configuration. Documented here for reference; not a launch blocker and not to be re-raised.

- **Severity:** High (core protocol path broken; serve-side and a latent decode-side defect)
- **Location:** `zakura-network/src/zakura/header_sync/wire.rs:152` (encode) and `:246` (decode); helper `zakura-network/src/zakura/header_sync/validation.rs:345`
- **Class:** Correctness / protocol liveness

**What's wrong.** The header-sync protocol supports two response shapes: with commitment roots (`want_tree_aux_roots = true`) and without. The flag is written/read faithfully (`wire.rs:159`, `:222`), the decode loop only fills `tree_aux_roots` when `has_roots` is set (`:232`–`:245`), and the reactor treats a roots-free response as legal (`reactor.rs` uses `validate_tree_aux_roots_len(...).is_ok()` as a *soft* boolean). But both `encode` and `decode` call `validate_tree_aux_roots_len(count, roots.len())` **unconditionally**, and that helper errors whenever `count != roots.len()`:

```rust
// wire.rs:246 (decode), roots empty when has_roots == false:
validate_tree_aux_roots_len(count, tree_aux_roots.len())?;   // validate(count, 0) -> Err for any count > 0
```

So any non-empty `Headers` message that legitimately carries no roots is a hard `TreeAuxRootCountMismatch` error in both directions. On the encode side a serving node literally cannot serialize a valid roots-free response it just built (the flag at `:159` is written as `!tree_aux_roots.is_empty()`, so "empty" is an explicitly supported state that then fails one line earlier).

**Impact / how it's reached.** Zakura's own requester currently hardcodes `want_tree_aux_roots: true` at every call site (`header_sync/state.rs:102`, `:132`, `:188`), so **Zakura-to-Zakura initial sync is not broken today** — which is why this survived. The damage is: (a) the serve path errors for *any* peer that requests headers with `want_tree_aux_roots = false` (a designed, wire-supported request — see the `UnrequestedTreeAuxRoots` guard at `wire.rs:226`), so Zakura cannot serve that peer; and (b) the roots-free mode is dead-on-arrival, so the moment any requester (a future config, a light path, an alternate client) sets the flag false, header sync fails to decode valid responses. This is a functional protocol break sitting one config flag away from production.

**Fix.** Gate the check on presence:

```rust
// decode
if has_roots { validate_tree_aux_roots_len(count, tree_aux_roots.len())?; }
// encode: rely on the flag written at :159, or
if !tree_aux_roots.is_empty() { validate_tree_aux_roots_len(headers.len(), tree_aux_roots.len())?; }
```

Add an integration test for the `want_tree_aux_roots = false` round trip — the existing tests only exercise the roots-present case.

---

## 2. State database format version never bumped for the VCT format change

> **Status: NON-ISSUE.** Launch ships a fresh stack with no prior on-disk state and no migration from an earlier format, so there is no older/downgraded binary to open an incompatible DB. The version-guard concern does not apply to a clean-genesis launch. Retained for the record; revisit only if a future release must be readable by, or must read, a pre-v1.0 database.

- **Severity:** High (upgrade/downgrade safety across the launch boundary; panic on DB open)
- **Location:** `zakura-state/src/constants.rs:78-91` (`DATABASE_FORMAT_VERSION = 28`, `MINOR = 0`, `PATCH = 0`)
- **Class:** Data-format compatibility / operational

**What's wrong.** The VCT work added three new column families (`COMMITMENT_ROOTS_BY_HEIGHT`, `VCT_SYNC_METADATA`, `VCT_UPGRADE_METADATA`) and a new on-disk semantic — the `[U, H)` "absent band" where per-height Sapling/Orchard/Ironwood trees are deliberately not written on a fast-synced node. The format version string has stayed `28.0.0` across all of it. The constant's own doc (`constants.rs:80-86`) states the minor version must be incremented "each time ... adding new column families." That policy was not followed.

**Impact / how it's reached.** Every operator hits this on the rc → v1.0 upgrade, and again on any rollback. Because the version string cannot distinguish an old archive DB, a pre-marker fast-synced DB, and a current fast-synced DB, a binary that lacks the absent-band logic (an earlier rc, or a downgrade) that opens a fast-synced DB will walk into the `.expect("... note commitment trees must exist ...")` panics in `sapling_tree_by_height` / `orchard_tree_by_height` (`shielded.rs:366`, `:506`) — a hard crash on startup with no graceful "incompatible database" message. This is exactly the failure the version guard exists to prevent, and it will bite real node operators during the launch window.

**Fix.** Bump the format version to reflect the VCT change (at minimum `MINOR`; arguably `MAJOR`, since the absent-band is a breaking read-format change), and add an on-open guard that refuses (or migrates) a fast-synced DB whose marker set doesn't match the running code, instead of letting the tree-lookup `expect()`s fire.

---

## 3. Header-sync difficulty pre-filter omits the PoWLimit upper bound

- **Severity:** Medium–High (exploitable by any peer; header-sync poisoning / wasted work)
- **Location:** `zakura-network/src/zakura/header_sync/validation.rs:299-310` (`validate_difficulty_filter`)
- **Class:** Consensus-adjacent validation gap / untrusted input

**What's wrong.** During header sync, each header is checked with:

```rust
let threshold = difficulty_threshold.to_expanded().ok_or(InvalidDifficultyThreshold)?;
if hash > threshold { return Err(DifficultyFilter { hash, threshold }); }
```

This enforces only the difficulty *filter* (`hash ≤ the header's own claimed threshold`). It never enforces the network PoWLimit upper bound that the authoritative block-level check applies (`difficulty_threshold_is_valid` compares the threshold against `target_difficulty_limit`). A peer can therefore advertise headers with an arbitrarily easy (huge) `difficulty_threshold` plus a hash trivially mined to satisfy it, and pass this network-layer gate.

**Impact.** Full blocks are re-verified by the block/checkpoint verifier (which does enforce PoWLimit), so this is **not a consensus-fork bug** — an invalid chain won't be finalized. But the header-sync layer is the node's admission gate for candidate header chains; accepting near-zero-work headers lets a malicious peer feed low-work header ranges that the node commits effort to organizing, correlating, and attempting to fill, a cheap poisoning/CPU-and-bandwidth-waste vector during the most fragile phase (initial sync). The behavior is inherited unchanged from the sibling fork, so it may be intentional, but it is worth closing before launch given the fork's stronger header-sync ambitions.

**Fix.** Also validate the threshold against the network PoWLimit before the filter check, mirroring `difficulty_threshold_is_valid` — reject any `difficulty_threshold` easier than `network.target_difficulty_limit()`.

---

## 4. VCT serving fabricates zero auth-data-root and tx-counts when a block body is unavailable

- **Severity:** Medium (serves consensus-invalid data; poisons consumers, can stall a pruned fleet)
- **Location:** `zakura-state/src/service/finalized_state/commitment_aux.rs:470-492` (`produce_block_roots`, the `else` branch)
- **Class:** Invariant-violation-as-default / fleet correctness

**What's wrong.** For heights below the VCT upgrade height, `produce_block_roots` derives the real Sapling/Orchard/Ironwood commitment roots from the per-height trees, but reads the ZIP-244 auth-data root and the shielded tx-counts from the block body via `db.block(height)`. When the body is absent (a pruned node, or any node whose body below `U` was pruned while header/tree rows remain), the code does not omit the height — it emits the **correct commitment roots** paired with **fabricated `(0, 0, 0, auth_data_root=[0;32])`**:

```rust
} else {
    metrics::counter!("state.block_roots.zero_aux_fallback").increment(1);
    // ... error log ...
    (0, 0, 0, AuthDataRoot::from([0u8; 32]))
};
```

**Impact.** A consumer folds these values into its ZIP-221 history MMR, produces a wrong leaf, fails successor authentication, and rejects/evicts the range. The recently added sender diagnostics (PR #109, tree-aux root diagnostics) then mis-attribute the malformed range to this honest node. If every reachable server for a pre-`U` height is pruned-below-`U`, the consumer stalls until the `VCT_ROOT_STALL_WARN_AFTER` escalation (`vct_write.rs:204`) with no verifiable root available. The inline comment claims "the recipient simply re-fetches from a node that has it," but the recipient has no way to know the data is fabricated — the roots look authoritative. Serving a root with fabricated leaf inputs is strictly worse than serving nothing.

**Fix.** When the body is unavailable, omit the height (break, exactly as the tree lookups already do) so the served run is a truthful contiguous prefix, or persist tx-counts and the auth-data root in the serving index so they never need the body. Never emit real roots with zeroed leaf inputs.

---

## 5. Legacy gossip: fetched transactions are not bound to the requested tx IDs

- **Severity:** Medium (peer substitution + per-request accounting corruption)
- **Location:** `zakura-network/src/zakura/legacy_gossip.rs:649-661` and `:677-685` (root cause: requested-ID set is captured for blocks near `:1910`, but not for transactions)
- **Class:** Response-correlation gap / untrusted input

**What's wrong.** The block-fetch path deliberately binds each delivered/`missing` block to a hash that was actually requested (`:640-645`, `:669-673`, with a comment describing the substitution attack it prevents). The transaction path has no equivalent check. `MSG_RESPONSE_TRANSACTION` accepts whatever transaction the peer returns, and `MSG_RESPONSE_MISSING_TRANSACTIONS` accepts arbitrary IDs as "missing," correlated only by request-ID and kind — not by the set of tx IDs the node actually asked for.

**Impact.** The node issues `TransactionsById([X])` (request id R); a malicious peer answers R with a `MSG_RESPONSE_TRANSACTION` carrying a different valid transaction Y, or lists never-requested IDs as missing. The requestor believes its fetch for X succeeded but holds Y, and the peer controls per-request download/source accounting. Blast radius is bounded because delivered transactions are still consensus/mempool-verified before acceptance, so **no invalid transaction enters state** — this is attacker-controlled substitution and accounting corruption, not a validity break. It's included because it's an exploitable asymmetry with an existing, proven fix template one function away.

**Fix.** Mirror the block fix: capture `requested_transaction_ids: Option<HashSet<UnminedTxId>>` in the request path, thread it into `decode_response`, and reject any delivered or `missing` transaction whose id is not in the requested set (`UnsolicitedTransaction`).

---

## Honorable mentions (fix soon; not in the top 5)

- **Ironwood row absence silently becomes the empty-tree root** — `commitment_aux.rs:438` (`ironwood_root_or_empty`) and the dead-but-latent copy in `zakura_db/block.rs:232`. If the "Sapling/Orchard break first" invariant is ever violated (partial migration, corruption), this serves a wrong-but-plausible root instead of failing loudly. Make it a hard error; consider deleting the unused `finalized_commitment_roots_by_height_range`.
- **Legacy gossip list readers pre-allocate to the count cap** — `legacy_gossip.rs:812/837/858/913` use `Vec::with_capacity(count)` bounded only by a fixed max (25,000), not by bytes remaining, so a ~3-byte payload forces ~1.6 MiB of allocation before failing. Cap initial capacity by `count.min(remaining_bytes / min_item_size)` per the workspace `TrustedPreallocate` pattern.
- **Provisional and committed roots share one column family** — `COMMITMENT_ROOTS_BY_HEIGHT` holds both unverified header-ahead roots and the verified serving index, keyed only by height (`zakura_db/block.rs:685`). Safe today only because callers gate to `height <= finalized_tip`; separate the namespaces so a future caller can't leak unverified roots into authoritative serving.
- **No timeout-based reclaim of per-peer serving slots** — `block_sync/state.rs:781`; `served_blocks_inflight` is only decremented on a response event, so a dropped driver response permanently consumes a serving slot for that peer. Self-limited to serving-from-us. Add an age-based sweep.
- **Transport guard byte-budget leak (latent)** — `transport/guard.rs:302`; budget is reserved before the meter check and never released in `pipe.rs` `run_one`. Unreachable today (`byte_budget: None` everywhere) but becomes a per-peer liveness DoS the moment a byte budget is wired on. Fix before enabling budgets.

## Areas audited and cleared (high confidence)

Header-sync response correlation (request-ID reservation, bounded retired-ID tombstones, stale-session filtering); header/block wire decode bounds and `TrustedPreallocate` maxima; block-body hash-binding to the validated header chain; download memory admission (`ByteBudget` CAS, resident look-ahead gate); global + per-IP connection caps, inbound message-rate token buckets, serving amplification clamps; Tower batch/fallback `poll_ready` capacity and readiness; RPC cookie auth (constant-time, 0600, symlink refusal) and height-range clamping; `zakura-script` FFI (safe `libzcash_script` wrapper, validated `input_index`); PoW/difficulty math, checkpoint hash-binding, subsidy, activation heights, branch IDs, Halo2 circuit-era routing, Orchard reserved-flag rejection (#121), and NU7 placeholder branch-id test-gating (#116) — all correct.
