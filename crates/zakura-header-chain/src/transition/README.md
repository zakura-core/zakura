# Transition engine maintainer guide

The transition engine converts one authenticated event and its durable facts into an invariant-verified
`EngineTransition`. If planning fails, it returns a `TransitionFailure` and plans no effects.

The engine is a synchronous policy boundary. It decides admissibility, freshness, replay, fork choice, finality,
retention, and the exact next state. It does not read from storage or perform asynchronous work.

This guide covers the transition model itself. The
[engine specification](../../../../docs/specs/fork-aware-header-chain-engine.md) defines the normative `LC-*`
requirements.

## Engine state

`HeaderChainEngine` owns one coherent in-memory state with these parts:

- The retained header graph contains the finalized anchor, retained descendants, validation
  state, eligibility state, body state, and auxiliary delivery references.
- The selected projection is the deterministic greatest-work eligible path from finality to `header_best`.
- The verified projection is the contiguous full-state-accepted path in integrated mode. In headers-only mode,
  it contains only the finalized frontier.
- The finalized frontier anchors both projections. Finality never moves
  backward.
- `EngineMetadata` holds the frontiers, score, retention floor, alarms, replay fingerprint, configuration identity,
  and counters.

The counters describe different kinds of change:

- `state_version` advances for every actual header-chain effect.
- `header_generation` advances when header topology, validation, eligibility, selection, or finality invalidates
  header work.
- `verified_generation` advances when the verified projection or finality invalidates body-forward work.

Selected and verified projections can name different forks. Full state can
verify a weaker side path without replacing the greater-work selected path.

## Planning contract

`HeaderChainEngine::plan_transition` receives a `TransitionInput` and a frozen `TransitionContext`.
The input binds a `TransitionEvent` to the durable facts that the event may consume. The context supplies immutable
configuration, time, authenticated capabilities, and active retention references.

The planner never mutates the source engine. It builds a `PlanCandidate` over a graph overlay that stages edits without
changing the source graph. The resulting `EngineTransition` exposes the before and after snapshots, exact `ChangeSet`,
domain, and effects through a crate-private verified `TransitionPlan`.

Planning has six phases:

1. Authenticate and admit. The planner checks the engine mode, capability,
   configuration, input bounds, and event-specific preconditions.
2. Bind replay and freshness. The planner checks the serialized version or the asynchronous work
   owner. It also recognizes an exact replay in the current fingerprint slot.
3. Project event evidence. The event handler stages graph, validation, body,
   eligibility, and auxiliary changes in `ProjectedTransitionState`.
4. Settle global policy. The planner recomputes the verified and selected projections, derives
   finality, applies retention, and converts protected resource pressure into a resource-stall
   plan.
5. Assemble writes. The planner derives the graph delta, projections, indexes,
   finality record, auxiliary rows, alarms, and metadata from the settled
   state.
6. Verify independently. The invariant verifier checks the candidate against the source engine.
   A failed check returns `TransitionFailure::Invariant` with no planned effects.

## Freshness and replay

Serialized events use an exact `state_version` comparison. A mismatch returns
`TransitionFailure::Stale`.

Asynchronous header work uses its `header_generation`, finalized anchor, and branch identity. Body-forward work also
uses `verified_generation`. An unrelated `state_version` change does not stale an owner that still matches those fields.

Durable finality history can rebase older header work across monotone finality. The planner can trim the consumed
prefix, bind the remaining suffix to the new anchor, or report that finality already applied all work. Missing or
contradictory history makes the work stale.

Replay protection retains the fingerprint of the most recent fingerprint-bearing state-changing transition. The
fingerprint includes a stable domain, a domain-local evidence key, and a canonical payload digest.

An exact replay in that slot produces a verified no-change plan even when its serialized version is old. Authority and
owner freshness checks still apply. Reusing the current domain-local key with a different payload returns
`TransitionFailure::ConflictingReplay`.

A later fingerprint-bearing state change replaces the slot. A transition with
no fingerprint does not replace it. The slot is not a historical replay
ledger.

`TransitionFailure::StalePreparation` has a narrower meaning than `Stale`. Prepared headers return
`StalePreparation` when they no longer match their durable validation context and require new preparation or validation.

## Fork choice, finality, and retention

Settlement evaluates the complete projected state after the event handler
finishes.

- Fork choice selects the deterministic greatest-work eligible tip.
- Integrated finality must lie on the verified projection and must come from
  authenticated full-state evidence.
- Headers-only finality advances a local depth pin when the selected path
  exceeds the configured depth.
- Finality removes old ancestors and competing sibling subtrees. It also
  rebases retained work coordinates to the new anchor.
- Retention protects selected and verified paths, every retained full-state-verified body path, and active retention
  references.
- Retention evicts only weaker unprotected branches.

When protected paths alone exceed the limits, settlement discards the event's projected effects. It returns a verified
resource-stall transition that keeps or raises the durable resource alarm.

## Invariant gate

The invariant verifier treats the candidate as untrusted. It derives its checks from the source
engine, graph delta, and proposed write set rather than from the event handler's conclusions.

The verifier checks:

- canonical node hashes, ancestry, heights, indexes, and accumulated work;
- direct and inherited eligibility, plus permanent invalid-body tombstones;
- contiguous finalized-rooted selected and verified projections;
- deterministic fork choice and mode-specific verified state;
- monotone finality, finality provenance, and authenticated trust pins;
- protected paths, frozen resource limits, and auxiliary foreign keys; and
- exact state, generation, and finality counter changes.

`EngineTransition` remains commit-capable only because callers cannot construct
its crate-private verified plan directly.

## Planner outcomes

The planner has four main outcomes:

| Planner outcome | Meaning |
| --- | --- |
| State-changing `EngineTransition` | Valid evidence produced a complete write set. |
| No-change `EngineTransition` | Valid evidence produced no durable effect. |
| Resource-stall `EngineTransition` | Retention refused the event and may change only the alarm. |
| `TransitionFailure` | Planning failed and produced no planned effects. |

Replay, already-applied rebased work, immediate retention eviction, and valid redundant evidence can all produce no
change. Callers must inspect `EngineTransition::is_no_change` and `EngineTransition::effect` instead of assuming that
success changed state.

Keep these three resource-limit outcomes separate:

1. `TransitionFailure::AuxiliaryLimitExceeded` refuses the event before any
   mutation and does not raise the resource-stall alarm.
2. A verified `resource_stalled` effect means retention cannot meet limits
   without evicting protected state. The plan keeps or raises the alarm.
3. `InvariantViolation::Limits` means verification found a projected graph
   above frozen limits. Planning fails with no effects and does not return a
   resource-stall plan.

## Recovery audit

Recovery reads a coherent durable snapshot and audits every authoritative row. It fails closed on contradictions in
node identity, ancestry, work, validation, body authority, trust pins, eligibility roots, auxiliary provenance,
finality, configuration, protected paths, or limits.

After that audit, recovery reconstructs derived views. These views include
indexes, projections, inherited eligibility, retention metadata, elapsed time
deferrals, and the selected-tip body-unavailability alarm.

The audit returns a `RecoveryPlan`. The plan contains corrected metadata, graph
rows, indexes, projections, deferred entries, and an exact set of
`RecoveryRepair` values. `RecoveryPlan::is_clean` reports whether the store
needs a repair.

For a fixed store snapshot, configuration, and recovery time, the audit returns
the same plan. `audit_store_for_trust_anchor_update` permits only the audited
manifest-digest rebind in addition to ordinary reconstructible repairs.

## Changing the transition model

When you add or change an event, update each part of the model that owns a
decision:

1. Add exhaustive event dispatch. Define the variant, its admission authority, its durable input
   facts, and its handler under [`planner/event_effects`](planner/event_effects/mod.rs).
2. Define replay identity. Keep existing `TransitionDomain` codes stable,
   append new codes, choose the domain-local evidence key, and hash every field
   that can change effects in
   [`types/event/replay.rs`](types/event/replay.rs).
3. Derive writes after settlement. Event handlers should mutate only `ProjectedTransitionState`;
   [`planner/write_set.rs`](planner/write_set.rs) must derive the complete write set from the
   settled state.
4. Preserve verifier independence. Add checks under [`invariants`](invariants/mod.rs) that
   recompute the new invariant from the source engine and candidate. Do not trust a flag or value
   produced only by the event handler.
5. Keep graph mutation inside the overlay. Use the graph edit methods exposed
   by `ProjectedTransitionState`. Do not mutate `HeaderChainEngine` while
   planning or construct graph and index writes separately.

Update unit, property, conformance, and recovery tests for every affected rule.
If the behavior changes an `LC-*` requirement, update the normative
specification and corresponding tests in the same change.

The main transition entry points are
[`engine/mod.rs`](engine/mod.rs), [`planner.rs`](planner.rs), and
[`recovery/mod.rs`](recovery/mod.rs). Public transition types are re-exported
from [`mod.rs`](mod.rs).
