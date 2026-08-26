# Handshake property-testing MVP

This directory tests the native Zakura control handshake as one protocol exchange. The test runner
executes the production codecs, validation, framing, deadlines, nonce binding, and limit
negotiation.

The [MVP design](../../../../../../../art/inbox/expectations/handshake-property-testing-design.md)
defines the intended coverage. The
[message regulation design](../../../../../../../zakura.peer-message-regulation/docs/design/peer-message-regulation.md)
and
[message regulation specification](../../../../../../../zakura.peer-message-regulation/docs/specs/peer-message-regulation.md)
define the receiver-owned bounds that motivate these properties.

## Test shape

[`control_handshake.rs`](../../control_handshake.rs) contains the production initiator and responder
core. The core depends on byte reads, byte writes, a deadline clock, a supplied nonce, and an event
observer. The existing handler supplies Iroh streams, Tokio time, and an `OsRng` nonce.

The test runner supplies the same core with a generated `HandshakeScenario`. The current topology
contains one initiator, one responder, and one connection. A scenario selects both frame limits, a
deadline, one message mutation, one transport fault, and a scheduler seed.

`proptest` generates and shrinks these scenarios. A separate named matrix runs every supported
mutation and fault. The pure reference model predicts the terminal outcome before either backend
runs.

The scenario vocabulary currently covers:

- conformant hello and ack messages;
- wrong magic, control version, protocol version, path, role, network, chain, identity, and nonce
  values;
- nonzero native upgrade nonces and transcripts;
- missing or unsupported capability and channel bits;
- zero limits and a receiver limit violation;
- trailing bytes;
- zero and oversized length prefixes;
- deterministic delay and short writes;
- a stalled hello or ack, which exercises the deadline;
- close before either message;
- injected cancellation before either message.

The canonical trace records stable handshake events. It excludes runtime task identifiers, socket
addresses, and wall-clock values. The Commonware property runs each scenario twice and requires
equal outcomes, traces, and trace hashes.

On a property failure, the runner writes the minimized scenario and trace under
`target/handshake-sim/failures/`. Replay an artifact with:

```console
ZAKURA_HANDSHAKE_REPLAY=target/handshake-sim/failures/<artifact>.json \
  cargo test -p zakura-network replay_handshake_artifact_from_env -- --nocapture
```

## Backend decision

Keep both backends for the next prototype slice. They cover different boundaries and share little
adapter code.

### Commonware

The Commonware backend owns scheduling, simulated time, the byte-stream network, seeded nonce
generation, and the runtime auditor. A seed reproduces the application trace exactly. The adapter
also injects delay, short writes, stalls, closes, and cancellation at the stream boundary.

The MVP pins `commonware-runtime` to `0.0.65`. Current CalVer releases conflict with the prerelease
`sha2` version in the Zcash dependency graph. Version `0.0.65` resolves with the current workspace
and exposes the deterministic capabilities needed by this spike.

The suite compares full auditor state for the conformant replay case. Version `0.0.65` does not
provide a small, stable leak-counter API for every canceled execution. The runner still joins both
roles and compares the canonical result for every generated scenario.

The replay artifact records the seed, scenario, and canonical application trace. Commonware does
not expose its scheduler choice stream through this adapter. A long-lived scheduler should record
explicit choices so a runtime change cannot redirect an old seed to a different schedule.

### Turmoil

The Turmoil backend runs the production core over its simulated TCP socket stack. This path checks
that framing survives real `AsyncRead` and `AsyncWrite` behavior. The named matrix covers all stream
faults and message mutations. A packet rule adds deterministic latency.

The `ClientServer` fixture owns a Tokio runtime, so callers must run Turmoil outside another Tokio
runtime. The fixture also couples larger fixed latency values to its TCP retransmission policy. The
MVP uses a one-millisecond packet delay in the named overlap suite. A later network-fault slice
should use Turmoil's direct `Net`, `Rule`, and `Verdict` APIs.

### Iroh differential

Turmoil cannot replace Iroh's QUIC UDP stack. The real-Iroh test creates two relay-free loopback
endpoints and runs the same production core. It compares the no-fault outcome and negotiated limits
with Commonware and Turmoil.

This result supports a narrow capability boundary. It does not justify a workspace-wide runtime
abstraction. The next reactor slice should extend the shared scenario, model, and trace before it
adds another runtime trait.

## Properties and limits

The current suite checks these properties:

- declared lengths pass the configured cap and the 16 KiB hard cap before body allocation;
- malformed or noncanonical messages never establish;
- wrong-network policy closes neutrally;
- identity and nonce mismatches produce peer violations;
- conformant peers establish under fair scheduling, recoverable delay, and short writes;
- local close, timeout, and cancellation never become peer violations;
- accepted limits remain nonzero and below both peer frame limits;
- both roles expose equal negotiated limits;
- one stalled real-Iroh handshake cannot starve a healthy peer while one permit remains;
- closing the stalled handshake releases its production pending-handshake permit;
- completed deterministic runs leave no owned role future or stream in the runner;
- identical Commonware inputs produce identical application traces;
- Commonware, Turmoil, and real Iroh agree on the conformant exchange.

The production handler still owns pending-handshake permits and service exposure. A handler-level
real-Iroh scenario covers one stalled peer beside one healthy peer and checks permit release after
close. The next slice must generate permit caps and cover permit exhaustion, timeout, cancellation,
panic, and service exposure.

## Commands

Run the handshake suite:

```console
cargo test -p zakura-network zakura::testkit::handshake -- --nocapture
```

Compile the byte-level fuzz target:

```console
cargo check --manifest-path fuzz/handshake/Cargo.toml
```

Run the fuzz target with `cargo-fuzz` installed:

```console
cargo +nightly fuzz run --fuzz-dir fuzz/handshake control_payload
```

The fuzz target checks length admission and canonical hello and ack round trips. It feeds arbitrary
payloads to both production decoders.
