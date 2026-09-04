# `GetBlocks` property-testing infrastructure

> **Status: first draft.** This document defines the `GetBlocks` prototype and planned stateful
> checks for the [property-testing architecture](property-testing.md). It covers block-sync version
> 2 regulation. It does not cover the planned header subscription.

## Goal

Start with one complete message declaration and its baseline DoS properties. The first prototype
covers `GetBlocks` framing, decoding, allocation, range validation, and Work calculation. It creates
the smallest infrastructure that a second message can evaluate before Zakura commits to a larger
state-machine framework.

The prototype requires:

- one production declaration for the payload, allocation, count, and Work bounds
- a transport payload cap enforced from the fixed frame header before payload allocation
- a legal-value strategy with deterministic minimum and maximum cases
- a closed inventory of single-rule violations
- common canonical encoding, exact decode, frame, and bounded-payload properties
- a deterministic coverage check for every boundary and violation

The prototype does not claim `GetBlocks` regulation conformance. Production does not yet implement
the serving Work bucket, `Delay`, or Work refunds. Tests must not model those behaviors as though
production enforced them.

## Stateful expansion

After the message prototype works, build one reusable action vocabulary, one independent reference
model, one bounded state explorer, and one production runner. Use them to check a complete
`GetBlocks` exchange against two peers.

The bounded model stays deliberately small:

- two remote peers
- one request type, `GetBlocks`, with its three response types
- ranges of one to three blocks
- Work states that distinguish empty, partial, and sufficient capacity
- at most two response frames in flight
- response queues with capacity two
- response reordering, duplication, work reassignment, timeout, and connection closure

Two peers are part of the stateful expansion. They expose reservation ownership, peer isolation, and
bounded progress failures that a single-peer model cannot expose. Later generated tests vary the
peer count independently from the fixed two-peer exhaustive model.

## Scope discipline

The stateful expansion adds no new testing framework. It reuses the workspace's `proptest`, `serde`,
`serde_json`, Tokio test time, `TestClock`, synthetic peers, and trace capture.

The stateful expansion adds only these protocol-specific components:

- a serializable block-sync action sequence
- a small independent reference model and its invariants
- an observation type shared by the model and production runner
- a production admission adapter and stepwise observation comparison
- one conformant generated property test

The stateful expansion does not add:

- a generic state-machine testing framework
- a custom Proptest `ValueTree` or shrinking algorithm
- a generic model-checking engine
- a task scheduler or replacement async runtime
- an artifact database or artifact-management service
- a new support crate
- a symbolic execution, bounded code verification, or concurrency-testing tool

After the stateful expansion works, measured gaps decide whether to add the adversarial suite,
bounded explorer, synthetic-peer replay, or another library. Do not build those extensions only to
satisfy a speculative future use.

Keep every stateful type block-sync-specific. Do not design a common service action trait,
generic resource ledger, or reusable protocol-model crate. If a second service later needs the same
code, compare the two implementations before extracting their common behavior.

## Existing infrastructure

Zakura already has useful block-sync test infrastructure. The stateful expansion must extend it
instead of replacing it.

| Capability | Current state | Use in this slice |
| --- | --- | --- |
| In-memory peers on the production `BlockSyncService::add_peer` path | [`SyntheticBlockSyncPeers`](../../crates/zakura-network/src/zakura/testkit/block_sync_peer.rs) encodes and decodes real stream-6 frames | Replay minimized scenarios through the real peer routine and reactor |
| Multi-peer block-sync scenarios | [`Scenario` and `PeerSpec`](../../crates/zakura-network/src/zakura/testkit/blocksync_fuzz/scenario.rs) configure peers, ranges, timing, queues, and the block corpus | Reuse configuration and corpus helpers where their behavior matches the new action model |
| Adversarial peer behavior | The [synthetic serve loop](../../crates/zakura-network/src/zakura/testkit/blocksync_fuzz/peer.rs) supports drops, withholding, reordering, stalls, degradation, and disconnects | Reuse for transport-level replay and scheduled integration cases |
| Production reactor harness | [`run_scenario`](../../crates/zakura-network/src/zakura/testkit/blocksync_fuzz/mod.rs) runs the real work queue, byte budget, peer routines, and sequencer | Reuse as the broad integration layer after stepwise admission checks pass |
| Trace-derived bounds | [`invariants.rs`](../../crates/zakura-network/src/zakura/testkit/blocksync_fuzz/invariants.rs) checks progress, request bounds, byte bounds, and trace consistency | Keep as end-to-end checks and add regulation observations |
| Manual monotonic clock | [`TestClock`](../../crates/zakura-network/src/zakura/testkit/clock.rs) drives existing rate-limit tests | Extend the regulation state to accept this clock |
| Property-test library | The workspace already uses `proptest` | Use it for generated scenarios and shrinking |

The current scenario harness does not provide the following components:

- an independent regulation reference model
- a serializable action sequence
- state-dependent action generation
- state-aware shrinking
- a stepwise production comparison
- exhaustive exploration of a finite model
- complete clock control in the block-sync reactor
- explicit regulation Work charges, refunds, and concurrency slots

The current harness describes itself as “deterministic-ish.” It uses real Tokio time and named
scenarios. A seed determines corpus and peer choices, but it does not serialize every action choice.
The stateful expansion must not treat a current harness seed as a stable replay artifact.

## Boundaries

The stateful expansion checks regulation at two boundaries.

The admission boundary runs one action at a time. It returns an `Observation` after each action. The
property test compares that record with the reference model after each action. The bounded explorer
applies the same reference-model transition without running production.

The synthetic-peer boundary sends real frames through `SyntheticBlockSyncPeers`. It checks framing,
decoding, peer-routine integration, reactor integration, and backpressure. It replays selected
scenarios after the admission boundary agrees with the model.

The stateful expansion controls admission action order and monotonic time. It does not control every
Tokio task choice. The production runner reaches a defined quiescence point after each action before
it records the observation. A later slice can add explicit task choices if the quiescence rule
cannot distinguish a required behavior.

## Finite model

### Domain

The model uses fixed logical identifiers:

```text
Peer       = P0 | P1
Height     = H1 | H2 | H3
RangeSize  = 1 | 2 | 3
QueueDepth = 0 | 1 | 2
Time       = T0 | T1 | T2
```

The model uses abstract Work units. A `GetBlocks` request costs one request unit plus one unit per
requested block. The maximum charge is therefore four units. Each peer stores `available` and
`outstanding` Work in `0..=4`. The model derives the state for a proposed request as follows:

```text
Empty      = available == 0
Partial    = 0 < available < request charge
Sufficient = available >= request charge
```

The production declaration computes charges in bytes. The production admission state uses those
declaration-derived charges. The property test compares verdicts, ownership, charge and refund
cardinality. It checks the production conservation equation separately. It does not compare the
model's abstract Work units with production byte counts.

The finite model permits at most two response frames across both peers to remain queued or
delivered but not handled. Each peer queue has capacity two. These bounds expose cross-peer
isolation while keeping exhaustive exploration tractable.

### State

The model state contains:

- connection state for each peer
- available and outstanding Work for each peer
- concurrency-slot ownership for each peer
- admitted inbound `GetBlocks` requests
- local outbound range reservations and their unconsumed heights
- pending and delivered response frames
- queued response count for each peer
- protocol deadlines and logical time
- reassigned local work ownership
- terminal verdict and cleanup state
- bounded-progress age for each runnable peer

The state uses indexed peer collections. It must not define separate `peer_0` and `peer_1` fields.
This representation lets generated tests increase the peer count without rewriting the model.

### Actions

One versioned action enum drives the model, generated tests, regression scenarios, and production
runner:

```rust
enum Action {
    Connect { peer: PeerId },
    SendLocalGetBlocks { peer: PeerId, range: BlockRange },
    ReceivePeerGetBlocks { peer: PeerId, range: BlockRange },
    ReceiveBlock { peer: PeerId, height: Height },
    ReceiveBlocksDone { peer: PeerId, range: BlockRange, returned: u8 },
    ReceiveRangeUnavailable { peer: PeerId, range: BlockRange },
    CompleteHandler { peer: PeerId, outcome: HandlerOutcome },
    DrainQueuedResponse { peer: PeerId },
    RefillWork { peer: PeerId, units: u8 },
    ReassignLocalWork { range: BlockRange, to: PeerId },
    ChangeFinality,
    AdvanceTime,
    ProtocolTimeout { peer: PeerId },
    CloseConnection { peer: PeerId },
}
```

Repeating `ReceiveBlock` generates a duplicate. Delivering heights in a different order generates
response reordering. The scenario does not need special duplicate or reorder flags.

Every action defines:

- its protocol preconditions
- its model transition
- its expected observation
- its conformant or adversarial classification
- its shrink rules

Conformant action generation selects only actions whose sender preconditions hold. Adversarial
generation selects one rule to violate and records that rule in the scenario. The model never uses
production admission code to decide whether an action applies.

### Observation

The model and production runner return the same bounded `Observation` shape after every action:

```rust
struct Observation {
    verdict: Verdict,
    reservation_delta: ReservationDelta,
    work_delta: WorkDelta,
    slot_delta: SlotDelta,
    queued_response_delta: i8,
    handler_event: Option<HandlerEvent>,
    connection_state: ConnectionState,
}
```

The observation excludes task identifiers, wall-clock timestamps, channel implementation details,
and unrelated reactor state. The trace retains those details for diagnosis. The stepwise comparison
uses only the fields in `Observation`.

## Properties

The model and production runner check every safety property after every action:

- a conformant action never returns `Disconnect`
- every admitted response consumes one live reservation part
- no response consumes the same reservation part twice
- reassignment and finality changes do not remove a reservation
- a protocol timeout or connection close removes only the affected peer's reservations
- available Work, outstanding charges, refunds, and consumed Work satisfy conservation
- each admitted request owns one concurrency slot
- every terminal path releases its slot once
- each queue and per-peer state collection stays within its capacity
- a delayed peer does not change another peer's reservation, Work, slot, or queue state
- model and production observations agree after every action
- a replay produces the same observations and final state twice

The first bounded-progress property uses these explicit assumptions:

- The runner selects an enabled peer within two scheduling choices.
- A peer with sufficient Work and queue capacity produces a handler or queue transition within four
  model actions.

The model stores a bounded-progress age and converts this property into a safety invariant. A later
scheduler model may replace these initial bounds when measurements identify a more accurate bound.

## Bounded state exploration

Bounded exploration is the first optional extension. Add it after the generated test and production
runner agree on the reference model. The generated test provides useful coverage without waiting
for an explorer.

The bounded explorer starts from the empty two-peer state. It obtains applicable actions from the
reference model. It applies each action, checks every invariant, canonicalizes the resulting state,
and visits each new state until no unvisited state remains.

The explorer records a predecessor and action for every state. When an invariant fails, it emits the
shortest known counterexample as a versioned scenario. The production runner replays that scenario
and reports whether production has the same failure or diverges from the model.

The explorer reports:

- states visited
- transitions checked
- maximum counterexample depth reached
- actions reached
- verdicts reached
- invariant checks executed

The explorer must not silently stop at a depth, state, or time limit. If a configured resource
limit stops exploration before the frontier empties, the test reports an incomplete exploration and
does not claim exhaustive coverage.

Implement the explorer as a small model-specific breadth-first search over `ModelState`. Do not add
a generic explorer API or external model-checking dependency. Reconsider that decision only if the
finite model needs reduction or liveness features that the local search cannot provide.

## Generated property tests and shrinking

The generated suite uses ordinary Proptest collection strategies. It generates a sequence of small
`ActionChoice` values. Each choice contains an action selector and bounded operands. A materializer
starts from the initial reference state, maps each choice to an applicable action, and advances the
reference state.

This choice sequence keeps shrinking simple. Proptest removes choices and shrinks their selectors
and operands. The materializer then rebuilds an applicable action sequence from the initial state.
The stateful expansion does not implement a custom `ValueTree` or action-deletion algorithm.

The materializer uses boundary-biased mappings. The mappings favor empty and full Work states,
queue depths zero and two, range sizes one and three, the action before a timeout, the action after a
timeout, and cross-peer alternation.

The generator records action and transition coverage. Required actions and verdicts must appear in
the pull-request profile.

The later adversarial suite stores one `ViolationKind` outside the shrinkable conformant choice
sequence. Its materializer builds a conformant prefix and then applies that violation when its
preconditions hold. Proptest can shorten the prefix without changing the violation class.

If this strategy produces large or unclear counterexamples, record examples and measure the
problem before adding another library. A later change may evaluate `proptest-state-machine` or a
small custom strategy. The stateful expansion does neither.

## Production runner

The production runner applies one action and waits for the admission state to reach quiescence. A
quiescence point means that the action has produced its immediate verdict and all synchronous
resource deltas, while no handler completion or time advance that belongs to a later action has run.

The runner uses the production message declarations for caps and Work calculations. It observes
semantic resource events from the regulation trace. It must not reconstruct expected state from
private production fields.

The runner first targets the admission boundary. The synthetic-peer adapter then translates the
same actions into real block-sync frames and reactor events. Actions that the broader reactor cannot
control stepwise remain admission-only until the runtime exposes a deterministic seam.

## Scenario serialization

Derive `Serialize` and `Deserialize` on the scenario and action types. Print the minimized scenario
as JSON when a property fails. A confirmed regression file contains only stable replay inputs:

```text
schema version
suite and selected adversarial rule
model bounds
ordered actions
```

The failure message reports the seed, profile, first divergent action, expected observation, and
production observation. The regression file does not store those diagnostics because the runner
can recompute them.

The repository keeps confirmed minimized failures as explicit regression scenarios. CI replays the
serialized actions directly. It does not rely on a property strategy to reconstruct them from a
seed. The stateful expansion does not add an artifact manager, corpus database, or automatic file
writer.

## Implementation layout

The message prototype uses these block-sync modules:

```text
crates/zakura-network/src/zakura/block_sync/declaration.rs
crates/zakura-network/src/zakura/block_sync/property_tests.rs
```

The production declaration owns the bounds and context-free validation. The test module owns the
legal strategy, deterministic boundaries, violation inventory, and common property checks.

Start the stateful expansion in one test-only module:

```text
crates/zakura-network/src/zakura/testkit/regulation.rs
```

Keep the action types, reference model, Proptest strategy, observation type, and production adapter
together while they remain small. Split a component into a submodule only when the file becomes hard
to navigate. Do not create a support crate for possible future services.

The reference model remains test-only. Production code must not depend on the reference model,
generator, or bounded explorer.

## Delivery order

The message prototype has five steps:

1. Add the production declaration and route the codec through its validator.
2. Enforce its payload cap before the transport allocates the payload.
3. Add the legal strategy and deterministic boundary values.
4. Add the closed single-rule violation inventory.
5. Run the common codec, frame, allocation-bound, and Work-equation properties.

The stateful expansion then has five steps:

1. Add versioned action, scenario, and observation types.
2. Add the two-peer reference model and its safety invariants.
3. Add the direct production admission runner and stepwise observation comparison.
4. Add one conformant Proptest using a shrinkable choice sequence.
5. Print and replay one serialized scenario.

Stop after this expansion and measure runtime, transition coverage, counterexample quality, and the
amount of production code changed for the test seam.

Add later extensions separately:

1. Add the one-rule adversarial suite when conformant generation is stable.
2. Add the model-specific bounded explorer when the state count is known.
3. Replay selected scenarios through `SyntheticBlockSyncPeers` when admission replay is stable.
4. Add dedicated CI profiles when ordinary crate tests no longer provide enough cases.

The message prototype does not satisfy every conformance requirement. Complete the bounded explorer
extension before the regulated block-sync feature claims conformance with the specification.

## Completion criteria

The message prototype is complete when:

- the transport rejects an oversized `GetBlocks` payload before allocation
- every deterministic legal boundary satisfies exact decode, canonical encode, and frame round-trip
- every bounded payload decodes without a panic and every accepted payload is canonical
- every declared single-rule violation runs and produces its specified error class
- generated Work charges match an independent transcription of the specification equation

The stateful expansion is complete when:

- generated conformant scenarios produce no `Disconnect`
- the generated test reaches both peers, every conformant action, and every Work state
- Proptest shrinks a seeded test failure to a valid replayable scenario
- model and production observations match after every action
- serialized scenarios replay independently of the generation strategy
- two repeated replays produce the same observations and final resource state
- a delayed or closed peer cannot stop the other peer within the stated progress bound
- an intentional test-only defect in reservation consumption causes the expected property to fail

The bounded-explorer extension is complete when it empties its frontier, reaches every action and
required verdict, and emits a replayable shortest counterexample for a seeded invariant defect.

The synthetic-peer extension is complete when selected scenarios pass through the real block-sync
frame and reactor path without changing their semantic observations.

## Later expansion

Keep exhaustive exploration at two peers until a named invariant requires a larger finite model.
Increase generated property tests to peer counts such as `1..=4` first. The indexed peer state and
logical identifiers support that change without a new action format.

Later slices can add simultaneous ranges, more response frames, explicit task choices, partitions,
crash recovery, discovery messages, and the header subscription. Each expansion must name the new
property and report its effect on state count and runtime.
