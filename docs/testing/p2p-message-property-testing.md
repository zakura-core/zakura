# GetBlocks property-testing pilot

This document describes a scoped local pilot for the stream-6 `GetBlocks`
message. It is an example under evaluation, not a repository-wide testing
standard or a ratified protocol specification.

The executable evidence is in
`crates/zakura-network/src/zakura/block_sync/property_tests/get_blocks.rs`.
Run it with:

```sh
./scripts/test-p2p-message-contracts.sh
```

## Scope

The first layer checks the `GetBlocks` wire contract and one
production-boundary consequence:

- exact encoding and byte order;
- legal field bounds;
- malformed, truncated, trailing, and mismatched frames;
- agreement between an independent test oracle and the production codec;
- canonical re-encoding;
- panic freedom over generated shallow payloads and frames;
- malformed-message cancellation versus a nonfatal `RangeUnavailable` response.

The stateful layer in
`crates/zakura-network/src/zakura/block_sync/property_tests/get_blocks_serving.rs`
runs generated requests through the real service, peer routine, reactor, state
driver seam, and framed response path. It checks:

- reactor admission before a peer routine reads its first frame;
- replacement-session ownership under disconnect races;
- response isolation across reconnects and delayed state work;
- serving-slot conservation under duplicate and mismatched completions;
- exact contiguous-prefix selection under generated response byte caps;
- bounded state queries when a peer exceeds its serving slots; and
- honest-peer progress while another peer sends a generated request burst.

The first four properties intentionally fail against the implementation at the
base of this draft. That red result is the evidence this layer found the bugs;
it is not a claim that the branch is ready to merge. Three separate production
fixes should make the unchanged property bodies pass.

This layer still does not claim to measure long-running resource amplification
or transport saturation. Those need wall-clock load tests in `blocksync_fuzz`.

## What property testing establishes

Property testing does not enumerate every possible value and is not formal
verification. This pilot combines:

- hand-written vectors for exact, reviewable wire bytes;
- exhaustive tests for small finite domains, such as counts `0..=129` and all
  `u8` discriminators;
- generated boundary-biased inputs across larger domains; and
- shrinking that reduces a generated failure to a smaller reproducible input.

Only explicitly enumerated domains are exhaustive. Generated cases provide
broad evidence, not a proof over all inputs.

## Contract source

The oracle records the current stream-6 wire format as an implementation
contract:

| Field | Value |
| --- | --- |
| Rust variant | `BlockSyncMessage::GetBlocks` |
| Stream | kind 6, version 2 |
| Direction | request |
| Outer and inner type | 2 |
| Layout | `[2][start_height: u32 LE][count: u32 LE]` |
| Canonical payload length | 9 bytes |
| Start-height range | `0..=0x7fffffff` |
| Count range | `1..=128` |

The oracle uses literals and its own byte parsing. It does not call production
constants or validation helpers. A separate test checks those literals against
production so changing one side cannot silently change both expectations.

This repository does not currently contain a ratified external specification
for these exact stream-6 bytes. A stricter policy proposal must therefore be
identified as a proposal rather than reported as an implementation defect.

## Rule ledger

Each row maps to a deterministic evidence function:

| ID | Requirement |
| --- | --- |
| GB-01 | The payload discriminator is 2 |
| GB-02 | The canonical payload is exactly 9 little-endian bytes |
| GB-03 | Count is in `1..=128` |
| GB-04 | Start height does not exceed `Height::MAX` |
| GB-05 | The decoder consumes the payload exactly |
| GB-06 | Frame flags are zero |
| GB-07 | Outer frame type agrees with the payload discriminator |

The report fails if an ID is missing, duplicated, out of order, or lacks
deterministic evidence.

## Three decoder outcomes

The test adapter reports three different production outcomes:

1. The bytes decoded as `GetBlocks`.
2. The bytes decoded successfully as another stream-6 message.
3. The decoder rejected the bytes.

This distinction matters because `BlocksDone` and `RangeUnavailable` share the
same nine-byte field layout. A payload whose discriminator selects either one
is not a rejected frame. It is a valid different message and must not inflate
the pilot's rejected-input count.

## Input lanes

The pilot uses three complementary lanes:

**Legal by construction.** The generator chooses a supported height and count,
with extra weight on zero, one, maxima, and values immediately around useful
boundaries.

**Single-rule invalid.** Deterministic tests change one rule at a time and check
the relevant production error when that error is part of the stable behavior.

**Compound invalid.** A Cartesian matrix and generated raw frames combine
multiple invalid fields. These cases check safe rejection without specifying
which error must win unless precedence is itself part of the contract.

Round trips are not sufficient on their own. An encoder and decoder can share
the same bug, so legal encodings are also compared with independently written
bytes.

## Production-boundary anchor

One focused test connects byte-level classification to node behavior through
the real `add_peer` → peer routine → reactor seam:

- a `GetBlocks` payload with a trailing byte is malformed, reports
  `MalformedMessage`, and cancels that connection;
- a correctly encoded request above the local tip receives
  `RangeUnavailable`, remains connected, and can issue another request.

This anchor does not model sustained spam. It only proves that the wire
classification reaches the intended connection policy.

## Stateful serving model

The serving cases are deliberately small enough for proptest to shrink. Each
case uses real framed messages and a paused current-thread Tokio runtime. The
four failing properties force the relevant lifecycle ordering while generating
the peer parameters, request ranges, completion kind, and surrounding values.
This avoids depending on a lucky task schedule while still exploring many
concrete cases.

The separate contention property fills one peer's serving slots, sends up to 64
additional `GetBlocks` requests, and then proves that another peer can still be
queried and served. Requests beyond the first peer's cap receive
`RangeUnavailable`; they do not amplify into state queries or a global serving
stall.

The byte-cap property generates three declared block sizes and a response cap.
It gives extra weight to caps exactly one byte below, equal to, or one byte above
a generated prefix sum. It requires the reactor to send exactly the largest
contiguous prefix whose sum fits the cap, followed by the matching `BlocksDone`.
If the first block does not fit, the response is `RangeUnavailable`.

The local script uses seed `8650902` by default. Override it with
`ZAKURA_SERVING_PROPTEST_SEED` to explore a different deterministic sequence.
Failures print the minimized case. Persistence is disabled so an intentionally
failing run does not create untracked regression files.

## Bugs exposed by the model

The properties currently expose four failures:

1. A peer routine can read `Status` and `GetBlocks` before reactor admission.
2. An old disconnect can remove a replacement session's registry entry, causing
   a valid request to be reported as `GetBlocksSpam`.
3. A delayed range response can be sent through a replacement session.
4. An unknown or duplicate completion can release another live request's slot.

The planned production work is deliberately separate. One fix gates the peer
routine on reactor admission, one makes disconnect removal session-owned, and
one gives state work an exact request identity and derives capacity from the
live request ledger. After those changes merge, these property bodies should be
rerun unchanged.

## Deliberately deferred policy questions

Two earlier candidate rules are not wire-contract divergences:

- The codec accepts a supported start height and count independently even when
  their mathematical inclusive end would exceed `Height::MAX`. The serving
  path uses checked arithmetic and clamps the request to the locally servable
  range. Rejecting such a request during decoding would be a new policy.
- The transport intentionally admits stream-6 payloads up to 3 MiB so the codec
  can classify oversized or future-expanded messages. A ten-byte `GetBlocks`
  payload is still rejected as trailing data. Adding a nine-byte transport
  allocation gate would be a transport-design change, not a discovered codec
  defect.

## Local workflow

Run the focused report and normal generated exploration:

```sh
./scripts/test-p2p-message-contracts.sh
```

On the unfixed baseline this command is expected to exit nonzero with GS-01
through GS-04. Run only the already-satisfied serving properties with:

```sh
./scripts/test-p2p-message-contracts.sh --passing-serving
```

Run 10,000 wire attempts and 1,000 stateful attempts per property:

```sh
./scripts/test-p2p-message-contracts.sh --deep
```

Override that count when investigating a seed:

```sh
PROPTEST_CASES=50000 \
ZAKURA_SERVING_PROPTEST_CASES=5000 \
ZAKURA_SERVING_PROPTEST_SEED=8650902 \
./scripts/test-p2p-message-contracts.sh --deep
```

Run the slower mutation-sensitivity check when `cargo-mutants` is installed:

```sh
./scripts/test-p2p-message-contracts.sh --mutants
```

Mutation mode remains scoped to the passing wire-contract pilot while the
serving baseline intentionally fails. Production-fix mutation checks belong
with the three fix branches.

Wire properties use standard proptest replay behavior. Serving properties use
the explicit seed described above.

CI policy is intentionally deferred until local runtime and counterexample
quality have been measured.

## Deferred load testing

The next layer should extend `blocksync_fuzz` with sustained request rates,
slow readers, full outbound queues, disconnect churn, and resident-memory and
state-query counters. That harness is appropriate for wall-clock throughput and
resource ceilings. The small state model remains the faster source of replayable,
shrinkable counterexamples.
