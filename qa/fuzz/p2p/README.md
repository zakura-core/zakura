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
| `block_fill` | `block/fill-retry-run`, `block/fill-retained-funding`, `block/fill-commit-window` | 1 |
| `block_request_lifecycle` | `block/request-lifecycle` | 1 |

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

The registry and retry targets preserve the drivers from PR #1679, change
`d17393bea6a5f32633fb29b82426fd7b82b1b086`, at reviewed head
`d4fc17f468487d03b3b554dade009bf2538be95b`. The head's later witness-message
correction remains intact. Decoder 1 keeps the original byte interpretation.
`block_fill` comes from PR #1677, head
`7aef27b246dcf8f0d3625d598553d2a827411ea0`. Decoder 1 keeps that byte layout, and
its funding check now runs through queue admission. The driver takes up to 30
items, which the one-block production cap does not reach.
`block_request_lifecycle` comes from PR #1679 and tracks requests,
transport-start facts, and no-progress charges. Its witness expires one written
request and one unwritten request, matches a replacement request, and resets the
remaining window. Other machine
targets follow their owning delivery packets. Shared replay integration and
campaign scheduling remain open.
