# P2P machine fuzzing

This isolated `cargo-fuzz` package is intentionally not a workspace member. Each
target drives one pure P2P machine from fuzz bytes and checks it against an
independent model after every input. A target panics with the property ID that
the machine broke. The targets do not start QUIC or the simulator step loop.

Each owning PR adds its targets as `[[bin]]` entries, with a row in the table
below.

| Target | Property | Decoder |
| --- | --- | --- |
| `service_session` | `session/offer-assembly`, `session/one-active-generation`, `session/legal-reopen` | 1 |
| `cadence` | `cadence/hold`, `cadence/settlement` | 1 |
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

## Service session target

`service_session` replays one connection's inputs to the service-session state
in `crates/zakura-network/src/zakura/handler/service_session/state.rs`. Its first
input byte selects the opening policy and connection roles; the rest selects
inputs. A unit test in `zakura-network` runs the same entry over fixed seeds in
every test run.

## Cadence target

`cadence` calls the production sender's hold decision and synchronous send path
for two rows sharing one queue. Its independent ledger checks `cadence/hold`
and `cadence/settlement`. A receive settles the real frame guard. This boundary
does not exercise scheduled transport writes.

Decoder version 1 reads at most 512 bytes. Each byte chooses update, elapsed
seconds, send or settlement. The fixed witness exercises pacing, an unwritten
frame, a full queue and settlement. Planted failures break pacing and ownership.
Proptest and cargo-fuzz call `replay_cadence_bytes`.

Run the applicability witnesses with `cargo test -p zakura-network --lib
cadence_witness -- --nocapture` from the repository root. Their printed counters
are witness evidence; they do not claim applicability for every fuzz input.
An empty input has zero applicability.

`.github/workflows/p2p-fuzz.yml` runs each target in a bounded daily campaign.
