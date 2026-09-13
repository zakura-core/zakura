# Bounded decoding

This slice extracts the bounded decoder from #971 and its F03 allocation
witnesses from #896. It builds on the shared test instruments in #973.
Frame policy, response authorization and transport settings have separate review
boundaries.

## Contract

When bytes are already present in a payload, `ZcashReader` carries their remaining
length through nested values. Collection counts must fit the available bytes
according to the element's minimum encoded size before allocation. Nested
decoders use `read_value`, `read_external_count` and `read_bytes` to preserve this
information.

**Example:** A complete Block header followed by a count of 1,024 transactions
and no transaction bytes previously reserved a collection before failing. The
bounded path rejects the count without that extra allocation. Complete block
fixtures and generated transactions must still match the streaming decoder.

The existing streaming entry points remain available. Unknown stream length
does not provide the same input bound. Wrapping a stream in a maximum read
allowance does not prove that those bytes exist. Legacy custom implementations
continue to decode through their streaming entry point, while the bounded entry
point rejects types that have not implemented it.

The element minima describe structurally decodable values. They must not impose
stronger consensus validity rules. Growth also stays within the declared count.
These are decoder allocation guarantees, not a bound on all process memory or
all decoded object overhead.

## Coverage

The chain tests cover nested limits, parent advancement, missing collection
input, byte strings, split proof arrays, growth boundaries, block fixtures and
custom streaming implementations.

The network caller retains all seven F03 witnesses in
`block_sync::wire::bounded_decoding`. Three fixed cases cover allocation-free
fixed fields, missing transactions and actual retained allocations. Four
generated properties cover complete transaction compatibility, independent
collection minima, incomplete byte strings and proof-array growth edges.

The assertions and generated input ranges are preserved from #896. The F03
identifiers remain stable so the full compliance ledger can map these tests
after the remaining slices are extracted.

## Execution

```sh
PROPTEST_CASES=64 PROPTEST_RNG_SEED=896 cargo nextest run --locked \
  -p zakura-chain -p zakura-network --lib --profile bounded-decoding

PROPTEST_CASES=2048 PROPTEST_RNG_SEED=896 cargo nextest run --locked \
  -p zakura-network --lib --profile bounded-decoding
```

The profile has finite deadlines and no retries. Ordinary unit profiles also run
the seven witnesses without retries. Allocation tracking is installed only in
the network unit-test binary and measures synchronous work on its calling thread.
