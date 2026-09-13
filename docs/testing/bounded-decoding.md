# Bounded decoding

This change prevents a complete message from making the decoder reserve a
collection for items whose bytes are missing.

**Example:** A complete Block header followed by a count of 1,024 transactions
and no transaction bytes previously reserved a collection before failing. The
new decoding path rejects the count without that extra allocation. Complete
block fixtures and generated transactions must still decode to the same values.

This is the second part of the review split. It extracts the decoder from #971
and its seven F03 allocation tests from #896, above the shared test helpers in
PR #973. F03 identifies the allocation property in the message regulation specification.

## Contract

When all payload bytes are already in memory, `ZcashReader` keeps track of how
many remain. Before reserving a collection, the decoder checks that even the
smallest encoding of those items can fit. Decoders for nested values use
`read_value`, `read_external_count` and `read_bytes` so they can make the same check.

The existing methods for decoding streams, where bytes may still be arriving,
remain available. An unknown stream length cannot provide the same check.
Limiting how many bytes a decoder may read does not prove that those bytes exist.
Custom types that implement only the older streaming method continue to work
through it. They must implement the new method to decode through the bounded path.

The minimum sizes describe encodings the decoder can read. They must still allow
values that later fail consensus validation. Buffers also stop growing at the
declared count. These checks limit collection allocations. They do not bound the
whole program's memory use or all memory needed for a decoded object.

## Network rules

Configure `ZcashDecoder::for_network(&network)` once when setting up message
decoding. Reuse that decoder for each payload. Its readers carry the rules through
nested values and read limits, so those calls do not need a network argument.

Regtest headers have a smaller Equihash solution. A counted header occupies
178 bytes on Regtest and 1,488 bytes on Mainnet or Testnet. For example, two Mainnet
headers need 2,976 bytes after the count. Using Regtest's minimum would let the
same count pass with only 356 bytes. The configured decoder rejects it before
reserving the collection. It also rejects an Equihash length from the wrong
network before reading or allocating solution bytes.

Native GetBlocks responses, buffered block replay, and legacy `block` and
`headers` messages use this configuration. Retained block bytes keep their
decoder so replay uses the same rules. Other message adapters can use the same
shared reader as their compliance checks are added. Generic decoding methods
remain available for offline data from any network.

## Coverage

The chain tests check that nested reads preserve the remaining byte count and
advance the containing reader. They also cover missing collection data, byte
strings, proof arrays stored separately from their counts, buffer growth, complete
block fixtures, and custom streaming implementations.

The network caller retains the original seven F03 tests in
`block_sync::wire::bounded_decoding`. Three fixed cases cover allocation-free
fixed fields, missing transactions and actual retained allocations. Four
generated properties cover complete transaction compatibility, collection sizes
checked against the wire format, incomplete byte strings and proof buffer growth.

A further generated property checks that header counts use each network's
minimum before allocating, including nested readers and shared headers. Fixed
tests cover both accepted and rejected network formats, legacy message decoding,
and buffered block replay.

The assertions and generated input ranges are preserved from #896. The F03
identifiers stay the same so the compliance ledger can find these tests after
the remaining PRs are split out.

## Execution

```sh
PROPTEST_CASES=64 PROPTEST_RNG_SEED=896 cargo nextest run --locked \
  -p zakura-chain -p zakura-network --lib --profile bounded-decoding

PROPTEST_CASES=2048 PROPTEST_RNG_SEED=896 cargo nextest run --locked \
  -p zakura-network --lib --profile bounded-decoding
```

The profile has time limits and no retries. Ordinary unit profiles also run the
allocation tests without retries. For the network caller, allocation tracking is
installed only in the unit test program and measures work on the calling thread.
