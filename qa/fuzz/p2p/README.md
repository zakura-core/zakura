# P2P machine fuzz targets

These targets call the independent models beside the production pure owners.
The `p2p-fuzz` feature exposes the drivers. The targets do not start QUIC or the
simulator step loop.

| Target | Property | Decoder |
| --- | --- | --- |
| `block_work_ownership` | `block/work-ownership` | 1 |
| `block_peer_registry` | `block/peer-registry` | 1 |
| `block_retry_authority` | `block/retry-authority` | 1 |

Each decoder reads at most 512 bytes. Each model checks the state after every
operation. The colocated tests retain nonzero witnesses and planted failures.
Those tests establish logical behavior. They do not establish runtime resource
retention or full semantic trace replay.

Run targets manually with a compatible nightly toolchain and cargo-fuzz:

```sh
cargo +nightly fuzz run --fuzz-dir qa/fuzz/p2p block_work_ownership -- -max_len=512
```

This package preserves the three drivers from PR #1679, change
`d17393bea6a5f32633fb29b82426fd7b82b1b086`, at reviewed head
`d4fc17f468487d03b3b554dade009bf2538be95b`. The head's later witness-message
correction remains intact. Decoder 1 keeps the original byte interpretation.
The request-lifecycle target follows the B4 pure-state port. Other machine
targets follow their owning delivery packets. Shared replay integration and
campaign scheduling remain open. No fuzz campaign blocks PR CI.

## Sequencer targets

| Target | Property | Decoder |
| --- | --- | --- |
| `block_sequencer_order` | `block/contiguous-submission` | 1 |
| `block_sequencer_reset` | `block/retired-apply-isolation` | 1 |
| `block_sequencer_evidence` | `block/required-evidence` | 1 |

These targets preserve PR #1680 at
`3bc8261c6d1a0c1ec1daf55d361f5a33e34b5a8d`. Each target uses the independent
model from its proptest driver. Ordering and evidence consume three-byte
operations. Reset consumes one byte for the retired-apply count, then three-byte
operations. Each decoder reads at most 512 bytes. Empty input reports zero
applicability. Setup transitions do not count as generated coverage.

The ordering witness retries a refused contiguous submission. The reset witness
completes a retired apply while current work remains. The evidence witness holds
required evidence through modeled backpressure, then delivers it. Planted state
corruption must fail the intended model check. These checks do not establish
production-mutation detection, decoded-memory retention, actual channel delivery,
or fresh-process semantic replay. The block fixture copies public mainnet block
1 from `zakura-test/src/vectors/block-main-0-000-001.txt`.
