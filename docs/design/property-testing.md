# Property testing

> **Status: first draft.** The [peer message regulation design](peer-message-regulation.md) and
> [specification](../specs/peer-message-regulation.md) define correct protocol behavior. This
> document defines the property-testing architecture that checks that behavior. The
> [first block-sync infrastructure](property-testing-block-sync-infrastructure.md) defines the
> first bounded model and implementation slice.

TL;DR: Property testing runs production protocol code through generated executions and searches
for counterexamples to required properties. Bounded model exploration checks every reachable state
in a deliberately small protocol model. Neither technique replaces real transport, load, or fault
tests.

## Motivation

Zakura already tests codecs, validators, and handlers in isolation. A complete asynchronous
exchange adds another class of failures. The result can depend on when bytes arrive, when a deadline
fires, or which peer advances first. Example tests cover only the cases that developers select, and
the live runtime can make a failure difficult to reproduce.

Property testing represents one execution as data. A generated scenario describes the peers,
protocol inputs, failures, time, and action order. The production runner follows the scenario while
it runs the production protocol code. One test then reaches input and timing combinations that no
developer wrote by hand.

When a scenario fails, the property-testing framework reduces it to a minimal failing test case.
Property-testing libraries call this process _shrinking_. The action strategy interprets each
shrunk choice from the current reference-model state. This keeps generated actions applicable
without a custom shrinking framework. The strategy can reduce a long execution to a partial length
prefix followed by a timeout. The runner serializes the minimized scenario so it can replay the
exact actions after strategies or backend implementations change.

## Claim strength

Generated tests and coverage-guided fuzzing search a large but finite set of executions. A passing
run provides evidence for a property, but it does not prove that the property holds for every
execution. This documentation uses _check_ for those claims.

Bounded model exploration can establish a stronger claim. If the explorer visits every reachable
state in a finite model, a passing run establishes the property for that model and its stated
bounds. It does not establish that the model matches production. The production runner must replay
model counterexamples and compare production behavior with the model after every action.

## Implementation discipline

The first slice uses one existing testing stack:

- `proptest` generates and shrinks choices
- `serde` and `serde_json` serialize confirmed regression scenarios
- Tokio test time and `TestClock` control time at existing seams
- the Zakura testkit provides synthetic peers, trace capture, and reactor integration

Zakura implements only protocol-specific code: actions, reference state, transitions, invariants,
canonical observations, and the production adapter. A small bounded explorer may enumerate the
finite reference model after that model works under generated tests.

The first slice does not add a general model-checking framework, symbolic executor, concurrency
testing framework, task scheduler, artifact service, or testing support crate. It does not implement
a custom Proptest `ValueTree`. Add one of those tools only when a measured limitation blocks a named
property.

## Architecture

One serializable scenario drives two independent paths. A reference model predicts the required
behavior. The production protocol core runs with the scenario's protocol inputs and action order.
The oracle compares their observations after every action. This stepwise comparison identifies the
first divergence and prevents later transitions from hiding compensating errors.

```mermaid
flowchart TD
    generator[Property generator or bounded explorer] -->|creates one execution| scenario["Serializable scenario<br/>protocol inputs + action order"]
    scenario -->|predict each action| model[Independent reference model]
    scenario -->|run each action| production["Production protocol core<br/>controlled runner"]
    model -->|expected observation| oracle{Same observation and state summary?}
    production -->|observed behavior + trace| oracle
    oracle -->|yes: continue| generator
    oracle -->|no: reduce while preserving failure| failure[Minimal failing scenario]
```

### Components

| Component | Responsibility | Why it matters |
| --- | --- | --- |
| Proptest strategy | Generate a shrinkable choice sequence and interpret each choice from the current model state | Generated conformant scenarios satisfy sender preconditions and use Proptest's existing shrinking machinery |
| Scenario | Describe one execution as versioned serializable data | Replay the exact actions without reconstructing random calls |
| Reference model | Predict protocol observations and state changes | Provide an expected result independent of production transitions |
| Bounded explorer | Visit every reachable state in the configured finite model | Check small state spaces without relying on random selection |
| Production runner | Apply one scenario to the production protocol core | Exercise the code Zakura ships |
| Controlled runtime | Control byte delivery, monotonic time, randomness, and the action order required by the current slice | Reach timing-dependent behavior quickly and repeatably |
| Oracle and execution trace | Compare every action and record the first semantic divergence | Detect transient and compensating errors |
| Real transport check | Run selected scenarios through the production transport adapter | Detect integration errors below the controlled boundary |

### Runtime boundary

The controlled runtime replaces only capabilities that can change an execution. The boundary
covers byte delivery, monotonic time, and randomness in the first slice. Protocol framing,
decoding, validation, state transitions, and resource accounting remain above it so the test runs
the code Zakura ships.

The first slice controls observable admission events. It does not claim to control every Tokio task
choice. A later slice can add explicit task scheduling when a named property depends on task order.
Real transport tests retain the production runtime and accept its scheduling behavior.

The boundary must stay narrow. An adapter that delivers only decoded messages cannot check partial
reads and length limits. A general runtime abstraction would add production structure before a
protocol slice shows that the structure is useful.

### Reference model

The reference model describes only the protocol states and effects needed to predict a result. It
does not call the production state transitions that it checks. Otherwise, the same defect could
make both paths agree.

The model also avoids duplicating unrelated production logic. It can use a decoded message when a
property concerns scheduling or resource cleanup. Codec properties and fuzzing remain responsible
for raw byte behavior.

The generator uses model state only to select actions whose protocol preconditions hold. The
production runner, not the generator, must preserve reservation, Work, slot, and state-capacity
invariants.

### Stepwise oracle

Each action produces a canonical observation from the model and production runner. The observation
contains only protocol concepts that the property needs:

- admission verdict
- reservation and credit changes
- Work charge or refund
- concurrency-slot acquisition or release
- queued response-byte changes
- handler dispatch or completion
- connection state

The oracle compares the observation after every action. It also compares a bounded state summary
after every action. The state summary omits backend identifiers and internal ordering that cannot
affect protocol behavior.

### Deterministic replay

The scenario and execution trace use protocol concepts instead of backend types, task identifiers,
or wall-clock timestamps. A trace names peers, streams, ranges, and messages by scenario-assigned
identifiers, and records:

- protocol phase changes
- framing, decoding, and validation decisions
- injected failures and action order
- deadlines and time advancement
- resource acquisition and release
- terminal outcomes

Stable inputs and observations let Zakura replay a failure after a backend change. They also let
controlled and real-transport runs compare the same protocol behavior. The runner executes a
scenario twice before it trusts the result. Unequal canonical observations or final resource states
mean the boundary missed a source of nondeterminism.

The failure output contains the generation seed, test profile, and first divergent observation. A
confirmed regression scenario is one JSON value with a schema version, suite, model bounds, and
minimized action list. The suite keeps that scenario in addition to any seed that Proptest persists.
A strategy change can map an old seed to a different scenario.

## Test layers

The architecture combines four layers because no single layer provides input coverage, exhaustive
small-state coverage, controlled concurrency, and transport confidence.

| Layer | Role |
| --- | --- |
| Pure properties and fuzzing | Explore codecs, validation, raw framing, and structured action mutations |
| Bounded model exploration | Check every reachable state in a small abstract protocol model |
| Controlled system tests | Exercise production protocol logic under generated failures and action orders |
| Real transport checks | Validate the production adapter, transport boundary, backpressure, and runtime behavior |

## Regulation properties

The regulation suite separates conformant scenarios from adversarial scenarios. A conformant
generator creates only actions that satisfy the sender obligations in the specification. No action
in that suite may produce `Disconnect`. An adversarial generator violates one declared rule and
checks the verdict for that rule. A later mixed-adversary profile may combine violations after the
single-violation suite identifies each verdict unambiguously.

The action strategy keeps a scenario in its original suite. It interprets a shrunk conformant
choice sequence from the initial model state, so every resulting action remains applicable. An
adversarial scenario stores its selected violation separately from its conformant prefix. Shrinking
can shorten the prefix but cannot replace one adversarial violation with another.

### Message addition contract

The implementation must make the compiler enforce this inventory equation:

```text
wire variants = message kinds = declarations = handler arms = reference-model arms
```

The equation uses sets of message kinds. It does not compare a fixed message count. A new protocol
version can add or remove messages without weakening the check.

Each protocol defines one closed `MessageKind` inventory. One macro invocation or an equivalent
compiler-generated implementation defines both the enum and `MessageKind::ALL`. Developers must
not maintain `ALL` as a separate array. The wire message enum exposes `kind()` through an exhaustive
match. The declaration lookup, handler dispatch, and reference-model transition also use exhaustive
matches without wildcard arms. Adding a wire variant or message kind then leaves a compile error at
every required integration point.

Each declaration must provide these property inputs:

- a legal-value strategy
- deterministic minimum and maximum legal values
- the payload cap
- allocation bounds for every variable-length field
- a closed inventory of message-specific rules
- an explicit state effect or an explicit `NoStateEffect` marker

Each message-specific rule must provide a conformant precondition, one strategy that violates only
that rule, the expected admission result, and the matching outbound obligation. The reference model
must compute the expected transition independently. It must not call the production admission or
handler decision that the property checks.

The property harness iterates over `MessageKind::ALL`. It runs the common codec, framing,
allocation, and dispatch properties for each kind. It also runs every declared message-specific
rule. The harness records a hit for each message kind, boundary value, and rule. The test fails when
any required hit remains zero. Random case counts can then increase exploration, but they cannot
decide whether the suite covers a message or rule at least once.

The declared role selects additional mandatory properties. An announcement exercises its cadence
boundary and matching sender rate. A request exercises its Work charge, `Delay` boundary,
concurrency slot, response bound, and every terminal refund path. A response exercises matching,
consumption, duplicate, mismatch, reordering, and connection-close behavior for its reservation.
The harness applies these properties from the role. A message author cannot opt out of them.

The resulting change formula is:

```text
add a message
  = add its wire variant and discriminator
  + complete its declaration and legal boundary values
  + implement its codec and handler arm
  + define each adversarial rule and outbound obligation
  + implement its independent reference-model transition
```

The change is incomplete while any term is missing. `cargo test --no-run` catches missing
exhaustive arms. The deterministic closure test catches a missing strategy output, boundary value,
or rule execution. The generated and bounded suites then search combinations and state sequences.

This contract prevents mechanical omission. It cannot prove that a strategy explores every useful
value or that the reference model states the correct policy. Reviewers must still compare each new
declaration, rule, and model transition with the protocol specification.

Each message declaration supplies its wire bounds and valid-value strategy. The property harness
must not maintain a second list of message caps. It checks these codec properties separately:

- decoding an encoded legal message returns the same message
- every encoded legal message fits its declared frame cap
- decoding rejects noncanonical encodings and trailing bytes
- encoding an accepted canonical payload returns the same canonical payload
- every payload within the wire cap returns a decode result without panicking

Each message declaration also supplies explicit allocation bounds for variable-length fields. The
allocation property checks requested allocation and retained decoded state against those bounds.
The wire cap does not serve as an allocation bound.

The stateful suite checks these protocol properties after every action:

- every admitted response consumes exactly one live reservation or one unconsumed range part
- scheduler reassignment, finality changes, and local interest changes do not remove reservations
- Work, refunds, outstanding charges, and available capacity satisfy the declared conservation
  equation
- each concurrency slot has one owner and every terminal path releases that slot once
- per-peer filter, reservation, delayed-frame, and queued-response state stays within its declared
  capacity
- two runs of the same scenario produce the same canonical observations and final resource state

Metamorphic properties provide an oracle that does not depend on the reference model:

- splitting one frame at different byte boundaries preserves its semantic result
- renaming peers preserves observations after applying the same renaming to those observations
- swapping independent actions from different peers preserves their final resource states
- translating every monotonic timestamp by the same duration preserves verdicts and resource deltas

Each Work implementation defines its conservation equation from the same declaration values that
compute charges and refunds. A typical form is:

```text
initial capacity + refills + refunds
    = available Work + outstanding charges + consumed Work
```

The suite expresses progress as a bounded property. If another peer or service stream remains
runnable, and the runner schedules every runnable participant within `N` steps, that participant
must produce an observable transition within `M` steps. Each slice states `N`, `M`, and its fairness
assumption. An unbounded statement such as “one peer never stops another” is not a finite test
oracle.

The first model does not need a general task scheduler. It represents only observable admission
actions: receive a frame or frame fragment, advance monotonic time, refill or refund Work, complete
or fail a handler, reassign work, and close a connection. A later slice may add explicit task
choices when a property depends on their order.

## CI profiles

Fast feedback and broad exploration need different case counts, so property tests run under three
profiles:

- A developer profile runs a small generated set and replays every checked-in regression scenario.
- Pull requests replay regression scenarios, run a stable seed set, add one deterministic seed derived
  from the CI run, and run the bounded explorer.
- A scheduled job explores many seeds and reports minimized scenarios for new failures.

Each slice sets its case counts and exploration bounds from measured runtime. CI reports action and
transition coverage so a passing run cannot hide a generator that never reaches a required state.

## Rollout

Start with the closed message inventory and pure declaration and codec properties for every current
message. Next, implement the
[first block-sync infrastructure](property-testing-block-sync-infrastructure.md). It models the
existing block-sync version 2 range exchange from `GetBlocks` through its terminal response. The
first bounded model uses two peers, one request type, ranges of at most three blocks, at most two
in-flight responses, and small queues.

The first model checks Work, range reservations, duplicate and out-of-order bodies, reassignment,
terminal validation, timeout, connection closure, and cleanup without depending on the planned
header subscription. Two peers provide the first isolation and bounded-progress checks.

Run minimized block-sync scenarios through the existing synthetic-peer adapter and selected cases
through the real transport after the model and production admission state agree. Add the header
subscription only after its version 9 wire contract is final.

Keep the exhaustive model small. Expand generated property tests independently to variable peer
counts, more simultaneous ranges, task choices, partitions, and crash recovery when a named
property requires them. This split provides multi-peer property coverage without making exhaustive
state exploration intractable.
