# Header-chain Transition

Synchronous policy for one header-chain update.

Higher layers own transport, async orchestration, durable storage, and
publication. This module turns authenticated evidence into an atomic plan the
runtime may persist and then install—without writing or publishing itself.

It exists so competing headers, full-state verification, body evidence, and
operator actions cannot each mutate retained forks, selected paths, finality, or
published frontiers independently.

## Engine

`HeaderChainEngine` is the coherent in-memory chain: retained fork graph,
selected and verified projections, metadata, and auxiliary deliveries. It is
what judges evidence—admissibility, freshness, eligibility, fork choice,
finality, and retention—and what derives the exact write set for one transition.

It performs no durable write and no publication. The state runtime serializes
persist → install → publish against that plan; the engine only decides which
history the evidence updates and what the next coherent state must be.

Hydrate with `from_audited_state` after recovery. Plan with `plan_transition`
against a frozen view; after the runtime commits the `ChangeSet`,
`install_committed_transition` advances this same engine only when its snapshot
before commit is unchanged.

## Client surface

| Abstraction | Role |
| --- | --- |
| `TransitionEvent` / `TransitionRequest` | Authenticated facts; never desired consequences |
| `TransitionInput` | Event plus the durable facts that variant may consume |
| `TransitionContext` | Config, clock, authority capabilities, retention pins |
| `HeaderChainEngine` | Coherent chain state; judges evidence and plans transitions |
| `EngineTransition` | Verified plan: snapshot before/after commit, `ChangeSet`, effects |
| `ChangeSet` | Exact atomic durable mutation |
| `EngineSnapshot` | Externally meaningful view after commit and publication |
| `ApplyResult` | Adapter receipt: committed, no-change, stale, or stalled |
| `audit_store` / `RecoveryPlan` | Startup audit and reconstructible repair plan |

Peers and consensus never submit transition events. The state adapter
authenticates higher-level work, loads event-specific durable facts, and builds
the input.

## Lifecycle

```text
authenticate evidence → TransitionInput + TransitionContext
        │
        ▼
HeaderChainEngine::plan_transition   (pure; engine unchanged)
        │
        ├─ TransitionFailure  → zero durable effects
        │
        ▼
EngineTransition (ChangeSet + snapshot after commit + effects)
        │
        ▼
adapter atomically persists ChangeSet
        │
        ▼
install_committed_transition → publish EngineSnapshot
```

Planning is not authority. Installation requires the durable batch to have
committed and the engine's snapshot before commit to be unchanged since planning.
Otherwise the install fails as stale.

## Observable effects

A verified plan may:

- commit graph, projection, eligibility, finality, metadata, and alarm changes
  (`ApplyResult::Committed`);
- admit valid evidence with **no durable mutation** (`is_no_change` /
  `ApplyResult::NoChange`);
- refuse under resource pressure while committing only a resource-stall alarm
  (`ApplyResult::ResourceStalled`).

`TransitionEffect` records orthogonal side effects that may coexist (header-work
rebase, finality advancement, auxiliary authentication, resource stall).
Generation changes on the published snapshot retire obsolete async work via
`RetiredWork::from_snapshots`.

### Planner failure → adapter receipt

| Planner result | Adapter outcome |
| --- | --- |
| verified mutation | `ApplyResult::Committed` |
| verified no-change | `ApplyResult::NoChange` |
| verified `resource_stalled` | `ApplyResult::ResourceStalled` |
| `TransitionFailure::Stale` | `ApplyResult::Stale` (zero effects) |
| any other `TransitionFailure` | adapter error / refuse; zero durable effects |

`MissingDurableFacts` means the adapter omitted `TransitionInput` rows (leases,
rebase history, migrated pin); it is not store I/O (`Store`).

### Stall / limit three-way distinction

Do not collapse these:

1. **`TransitionFailure::AuxiliaryLimitExceeded`** — refuse before any durable
   mutation; no resource-stall alarm.
2. **Verified `resource_stalled` → `ApplyResult::ResourceStalled`** — retention
   cannot enforce limits without breaking protected paths; the durable
   resource-stall alarm may commit or remain.
3. **`InvariantViolation::Limits`** (via `TransitionFailure::Invariant`) —
   commit-time verification found a projected graph above frozen limits; fail
   closed with zero effects (not a stall receipt).

Prepared header batch size is gated at planning by
`limits.max_headers_per_transition` in admission (authoritative for the active
engine). `PreparedHeaderBatch::new` also rejects above the frozen
`MAX_HEADERS_PER_TRANSITION_V1` constant; unifying those checks is deferred.

## Startup recovery

Before publication, `audit_store` loads authoritative rows and fails closed on
contradictions. Derived views (indexes, projections, inherited eligibility,
alarms) may be reconstructed into a `RecoveryPlan` that the adapter applies
atomically before hydrating the engine.

`StoreAuditRead` implementations must expose one coherent `state_version` across
every row read on a pass, visit finality history in ascending epoch order, and
perform no writes or publication.
