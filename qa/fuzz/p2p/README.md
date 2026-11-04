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
| `block_query` | `block/query-dispatch`, `block/query-completion` | 1 |
| `block_reactor` | `block/reactor-policy` | 3 |

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

`block_query` and `block_reactor` preserve the query portion of PR #1677 and the
reactor model from PR #1683. `block_query` generates accepted and refused
proposals, exact and stale replies, resets, authority changes, and retry expiry.
Its identity ledger calls no production policy helper. `block_reactor` shares the
reactor policy model with proptest and covers view mirrors, reaction epochs, reset
refills, query refusal, retry expiry, and authority changes. Decoder 2 of
`block_reactor` added a closed-channel refusal to bit 16 of the refill byte. Decoder
3 delivers an authority byte as a committed view, which retires the pending query,
and reads bit 4 of a refill's byte as the first view's arrival. No committed corpus
uses an older decoder. Both targets decide pure transitions. Runtime witnesses cover
the action channel and storage waits.

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
