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

The pilot checks the `GetBlocks` wire contract and one production-boundary
consequence:

- exact encoding and byte order;
- legal field bounds;
- malformed, truncated, trailing, and mismatched frames;
- agreement between an independent test oracle and the production codec;
- canonical re-encoding;
- panic freedom over generated shallow payloads and frames;
- malformed-message cancellation versus a nonfatal `RangeUnavailable` response.

It does not claim to test request floods, response bodies, serving-slot
concurrency, state-query behavior, resource amplification, scheduling, or
honest-peer liveness. Those require a stateful serving test rather than more
random codec inputs.

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

Run 10,000 generated attempts per property:

```sh
./scripts/test-p2p-message-contracts.sh --deep
```

Override that count when investigating a seed:

```sh
PROPTEST_CASES=50000 ./scripts/test-p2p-message-contracts.sh --deep
```

Run the slower mutation-sensitivity check when `cargo-mutants` is installed:

```sh
./scripts/test-p2p-message-contracts.sh --mutants
```

Proptest prints a replay seed and persists minimized regressions when source
persistence is available. Use `PROPTEST_RNG_SEED` with the reported seed to
replay a generated failure.

CI policy is intentionally deferred until local runtime and counterexample
quality have been measured.

## Next experiment

The next useful step is a separate stateful serving property test. It should
generate short, shrinkable action sequences over two or three peers using the
real reactor test seam. Its first properties should cover response-ledger and
serving-slot conservation, byte-cap prefix selection, connection-generation
isolation, bounded state-query amplification, and honest-peer progress after
contention drains.

Longer request-flood and resource measurements belong in the existing
`blocksync_fuzz` harness after the deterministic state model demonstrates these
properties with minimal replayable failures.
