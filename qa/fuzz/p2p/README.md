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
