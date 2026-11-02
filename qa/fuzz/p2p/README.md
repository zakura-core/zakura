# P2P machine fuzzing

This isolated `cargo-fuzz` package is intentionally not a workspace member. Each
target drives one pure P2P machine from fuzz bytes and checks it against an
independent model after every input. A target panics with the property ID that
the machine broke. The targets do not start QUIC or the simulator step loop.

Each owning PR adds its targets as `[[bin]]` entries, with a row in the table
below.

| Target | Property | Decoder |
| --- | --- | --- |
| `block_peer_registry` | `block/peer-registry` | 1 |
| `block_retry_authority` | `block/retry-authority` | 1 |

`qa/fuzz/sim/block_work_ownership` drives the `block/work-ownership` model through
trace and replay, so it needs no target here.

The input decoding, the model and the checks live in `zakura-network` behind the
`p2p-fuzz` feature, so the machine types stay private to the crate.

Run a target from this directory with a pinned nightly toolchain. Pass
`--fuzz-dir .`, because `cargo-fuzz` otherwise looks for `fuzz/` under the
workspace root:

```console
cargo +nightly-2026-07-15 fuzz run --fuzz-dir . <target>
```

`cargo-fuzz` creates and updates each target's local `corpus/` and `artifacts/`
directories. Git ignores both. Run targets locally or in the nightly or manual
campaign; no fuzz campaign blocks PR CI.

When libFuzzer reports a failure, reproduce and minimize the artifact:

```console
cargo +nightly-2026-07-15 fuzz run --fuzz-dir . <target> path/to/crash-artifact
cargo +nightly-2026-07-15 fuzz tmin --fuzz-dir . <target> path/to/crash-artifact
```

Turn every confirmed bug into a normal deterministic test in the owning crate.
Do not commit raw or minimized corpus files. Machine bytes are compatible with
their target and decoder version. They are not portable simulator traces.

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
