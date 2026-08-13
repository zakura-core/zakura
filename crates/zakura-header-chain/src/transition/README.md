# Atomic header-chain transitions

The header chain receives evidence from work that finishes at different times:
downloaded headers, full-state verification, body outcomes, checkpoint growth,
and operator actions. Applying each result directly would let the retained fork
graph, selected header path, verified path, finality, and published frontiers
disagree.

This package is the synchronous policy boundary that prevents that split. It
projects one admitted event over a frozen `HeaderChainEngine`, settles all
affected chain policy together, and returns one invariant-verified
`EngineTransition`. Higher layers still own asynchronous work, durable storage,
and publication.

The most important constraint is that planning is not a commit: observers may
rely on the result only after the complete write set is durable and the
corresponding engine state is ready to publish.

## Mental model

`HeaderChainEngine` owns one coherent in-memory view:

- the retained header DAG and auxiliary deliveries;
- the **selected projection**, the strongest eligible header path; and
- the **verified projection**, the contiguous full-state-accepted path in
  integrated mode, or just finality in headers-only mode.

Selected and verified are deliberately separate. Full state can verify a weaker
side path without replacing the greater-work header winner. Finality anchors
both projections, while retention may keep other eligible or verified branches.

The engine decides admissibility, freshness, replay, eligibility, fork choice,
finality, retention, and the exact next state. It performs no store reads,
durable writes, async scheduling, or publication.

## Boundary and client surface

| Abstraction | Role |
| --- | --- |
| `TransitionEvent` / `TransitionRequest` | Evidence or commands describing the trigger, never caller-selected consequences |
| `TransitionInput` | One event plus only the durable facts that variant may consume |
| `TransitionContext` | Frozen config, authoritative clock, capability provider, and active retention references |
| `HeaderChainEngine` | Coherent graph and projections; owns transition policy |
| `EngineTransition` | Verified before/after snapshots, exact `ChangeSet`, domain, and effects |
| `ChangeSet` | Complete atomic header-chain write set |
| `EngineSnapshot` | Externally meaningful metadata view to publish after commit |
| `ApplyResult` | Adapter receipt: committed, no-change, stale, or resource-stalled |
| `RecoveryPlan` | Audited startup repair and hydration plan, deterministic for a given store, config, and recovery time |

The state adapter is the trust boundary. It authenticates higher-level work,
loads event-specific durable rows, supplies capabilities and local time, and
serializes planning with storage. Admission checks the claimed event against
those capabilities; the event type alone does not make evidence authoritative.

## One transition

The ordinary path is:

```text
higher-level result + durable facts + capabilities
                         |
                         v
              pure transition planning
                         |
              +----------+----------+
              |                     |
       TransitionFailure      EngineTransition
        zero effects          verified write set
                                    |
                                    v
                         atomic durable commit
                                    |
                                    v
                         install into engine
                                    |
                                    v
                         publish EngineSnapshot
```

Planning has six phases:

1. **Authenticate and admit.** Check configuration, mode, capabilities, bounded
   input, and event-specific preconditions.
2. **Bind replay and freshness.** Validate the event's state version or async
   work owner, and recognize an exact replay in the current fingerprint slot.
3. **Project the evidence.** Stage graph, body, eligibility, and auxiliary
   changes in a copy-on-write view; the engine remains unchanged.
4. **Settle global policy.** Recompute verified and selected paths as needed,
   derive finality, enforce retention, and discard event effects on a protected
   resource stall.
5. **Assemble writes.** Derive graph, projection, index, finality, auxiliary,
   alarm, and metadata changes from the settled view.
6. **Verify independently.** Recheck the projected result and wrap it in the
   crate-private verified plan type only if every commit invariant holds.

On the normal runtime path, the adapter writes the entire `ChangeSet` atomically
with singleton metadata last, installs that same transition into the engine,
swaps any related full-state memory, and publishes the after-snapshot.

`install_committed_transition` also checks that the engine still equals the
plan's before-snapshot, and applying the private graph delta can fail. Before
commit, stale work can be replanned. After the durable batch commits, an install
failure is fail-closed because disk may already be ahead of memory; recovery,
not a forced install, restores one coherent state.

### Runtime exceptions to the ordinary path

- A header-chain no-change may still accompany an atomic full-state write and
  memory swap; it means only that the header chain has no durable effect.
- Repeating a resource stall whose alarm is already durable performs no write
  and publishes nothing.
- The combined auxiliary-authentication/checkpoint path privately stages the
  first transition in the locked engine so it can plan the second, then commits
  both with full-state changes in one batch. A pre-commit failure reloads the
  engine from unchanged durable state.
- Refuting a migrated headers-only pin commits and installs the fatal alarm but
  returns an error before publication. Startup and future applies then fail
  closed until the state is deleted and resynchronized.

## Authority, freshness, and replay

Admission is event-specific. Integrated full-state events require integrated
mode and exact full-state authority; operator retries require scheduler
authority; header completions require registered completion authority.
Deferred-time reevaluation is mode-independent. Prepared headers are rechecked
for identity, ancestry, configuration, contextual difficulty/time, and
ownership. Validation leases are checked for network/trust-anchor binding,
hash/ancestry coherence, overlap, and adapter authority.

Freshness has two forms:

- Serialized events use an exact `state_version` compare-and-swap.
- Asynchronous header work uses header generation plus its finalized anchor and
  branch. Body-forward work additionally uses verified generation. Unrelated
  changes may advance `state_version` without invalidating those owners.

Older header work has one narrow recovery path: durable finality history may
prove that finality advanced monotonically over its prepared range. The planner
then trims the consumed prefix, rebinds the suffix, or reports the work already
applied. Missing or contradictory history is stale.

Replay protection stores the most recent fingerprint-bearing state-changing
transition. An exact replay is a verified no-change even if its serialized
version is old, but authority and asynchronous-owner freshness checks still
apply. Reusing the current domain-local key for a different payload fails as
`ConflictingReplay`. A later fingerprint-bearing commit replaces the slot;
non-fingerprinted changes such as deferred reevaluation or a resource-stall
alarm do not. This is not a historical replay ledger.

`TransitionFailure::Stale` is the normal reload/reschedule result.
`StalePreparation` is different: prepared work no longer matches its durable
validation context and must be reconstructed or revalidated.

## Fork choice, finality, and retention

Event-local changes do not choose their own consequences. Settlement always
recomputes policy over the complete projected state:

- The selected tip is the deterministic greatest-work eligible tip.
- In integrated mode, the verified projection contains contiguous full-state
  accepted bodies. Explicit and checkpoint finality must lie on that path.
- In headers-only mode, a sufficiently deep selected tip advances a local depth
  pin, and the verified projection collapses to finality.
- Advancing finality appends provenance, removes old ancestors and competing
  sibling subtrees, and rebases retained work coordinates to the new anchor.
- Retention protects selected and verified paths, every full-state-verified body
  path, and active adapter references. It evicts only weaker unprotected
  branches.

If protected paths alone exceed limits, the planner discards the submitted
event's effects rather than deleting authority-owned state and returns a
verified resource-stall transition.

## Commit invariants and durable state

Before a plan becomes commit-capable, verification checks the graph delta and
write set against the source snapshot, including:

- canonical hashes, parent/child/height indexes, accumulated work, and
  permanent invalid-body tombstones;
- contiguous finalized-rooted selected and verified projections and
  deterministic fork choice;
- monotone finality, checkpoint and migrated trust pins, and protected paths;
- exact version/generation increments, retention limits, and auxiliary foreign
  keys.

`state_version` advances for any actual header-chain effect.
`header_generation` advances only when header topology, validation,
eligibility, selection, or finality changes invalidate header work.
`verified_generation` advances only when the verified projection or finality
changes invalidate body-forward work. Snapshot comparison derives retirement
signals; the coordinator must consume them and add any exact owners to retire.

The durable write set distinguishes source of truth from caches:

- **Authoritative:** node source fields and direct eligibility reasons,
  permanent invalid-body tombstones, auxiliary rows, finality history, and
  non-derived singleton metadata.
- **Reconstructible:** indexes; selected projection, frontier, and score;
  verified projection; inherited eligibility; oldest-retained metadata; and the
  selected-tip body-unavailability alarm.

All fields must land in one transaction. Partial application is undefined.

## Outcomes and failures

| Planner result | Adapter outcome |
| --- | --- |
| verified header-chain mutation | `ApplyResult::Committed` |
| verified header-chain no-change | `ApplyResult::NoChange` |
| verified `resource_stalled` | `ApplyResult::ResourceStalled` |
| `TransitionFailure::Stale` | `ApplyResult::Stale` with zero effects |
| any other `TransitionFailure` | adapter error/refusal with zero planned effects |
| migrated-pin refutation | commit fatal alarm, then adapter error without publication |

`TransitionEffect` records orthogonal facts that may coexist: header-work
rebase, finality advancement, auxiliary authentication, and resource stall.

`MissingDurableFacts` means required durable validation context could not be
supplied or authenticated, such as a missing or incoherent validation lease. It
is an adapter contract failure, not store I/O.

### Keep the three resource outcomes separate

1. **`TransitionFailure::AuxiliaryLimitExceeded`** refuses the event before any
   durable mutation and raises no resource-stall alarm.
2. **Verified `resource_stalled`** means retention cannot enforce limits without
   breaking protected paths. The durable alarm may be recorded or retained.
3. **`InvariantViolation::Limits`** means commit verification found a projected
   graph above frozen limits. Planning fails closed with zero effects and does
   not return a stall receipt.

Prepared header count is also checked twice: admission uses the active engine's
`limits.max_headers_per_transition`, while `PreparedHeaderBatch::new` enforces
the frozen `MAX_HEADERS_PER_TRANSITION_V1` format bound.

## Startup audit and recovery

Publication stays unavailable while recovery loads every durable row, fails
closed on authoritative contradictions, reconstructs derived views, and
classifies one atomic `RecoveryPlan`. Production startup uses
`audit_store_for_trust_anchor_update`, which permits only an audited
trust-anchor-manifest digest rebind in addition to ordinary repairs.

Recovery can rebuild indexes, projections, inherited eligibility, retention
metadata, and the selected-tip body-unavailability alarm, and can promote
elapsed time deferrals. It does not generically rebuild every alarm:
resource-stall state participates in authoritative limit auditing, and
migrated-pin refutation blocks startup.

The adapter commits any repairs before hydrating `HeaderChainEngine` with
`from_audited_state` and creating the publisher. `StoreAuditRead` must expose one
coherent `state_version` across a pass, visit finality history once in ascending
epoch order, and perform no writes or publication.

Atomic storage gives crash recovery a simple rule: reopening exposes either the
complete before-state or the complete durable after-state. Durable state may be
ahead of a publication interrupted after commit; startup audits and republishes
that durable state.

## Surprises for maintainers

- A successful plan is not proof of a mutation; replay, already-applied work,
  and an insertion immediately removed by retention can all be no-change.
- Selected and verified tips can legitimately name different forks.
- A global version change does not necessarily stale generation-owned async
  work.
- Replay protection remembers one current fingerprint slot, which
  non-fingerprinted commits do not replace.
- `RetiredWork::from_snapshots` reports generation changes; it does not itself
  cancel or retire coordinator work.

## Source map

- Engine boundary and installation: [`engine/mod.rs`](engine/mod.rs)
- Planning phases: [`planner.rs`](planner.rs)
- Admission and freshness: [`planner/admission.rs`](planner/admission.rs),
  [`planner/replay.rs`](planner/replay.rs)
- Finality and retention settlement:
  [`planner/settlement.rs`](planner/settlement.rs)
- Write derivation and invariant gate:
  [`planner/write_set.rs`](planner/write_set.rs),
  [`invariants/mod.rs`](invariants/mod.rs)
- Outcomes and durable DTOs: [`types/outcome.rs`](types/outcome.rs),
  [`types/write_set.rs`](types/write_set.rs)
- Recovery contracts and phases: [`recovery/contracts.rs`](recovery/contracts.rs),
  [`recovery/mod.rs`](recovery/mod.rs)
- Runtime commit/publication integration:
  [`zakura-state/header_chain.rs`](../../../zakura-state/src/service/finalized_state/header_chain.rs)
