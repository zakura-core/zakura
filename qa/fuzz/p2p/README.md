# P2P machine fuzzing

This isolated `cargo-fuzz` package is intentionally not a workspace member.
Each target drives one pure P2P machine from fuzz bytes and checks it against
an independent model after every input. A target panics with the property ID
that the machine broke.

Run a target from this directory with a pinned nightly toolchain. Pass
`--fuzz-dir .`, because `cargo-fuzz` otherwise looks for `fuzz/` under the
workspace root:

```console
cargo +nightly-2026-07-15 fuzz run --fuzz-dir . service_session
```

The `service_session` target replays one connection's inputs
to the service-session state in
`crates/zakura-network/src/zakura/handler/service_session/state.rs` and checks
`session/offer-assembly`, `session/one-active-generation`, and
`session/legal-reopen`. Its first input byte selects the opening policy and
connection roles; the rest selects inputs.

The input decoding, the model, and the checks live in `zakura-network` behind
the `p2p-fuzz` feature, so the machine types stay private to the crate. A unit
test there runs the same entry over fixed seeds in every test run.

`cargo-fuzz` creates and updates each target's local `corpus/` and `artifacts/`
directories; both are intentionally ignored by Git.

When libFuzzer reports a failure, reproduce and minimize the artifact:

```console
cargo +nightly-2026-07-15 fuzz run --fuzz-dir . service_session path/to/crash-artifact
cargo +nightly-2026-07-15 fuzz tmin --fuzz-dir . service_session path/to/crash-artifact
```

Turn every confirmed bug into a normal deterministic test in the owning crate.
Do not commit raw or minimized corpus files.

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

No CI workflow schedules this target or the machine targets below. Run them
with `cargo fuzz run` as shown above.
Machine bytes are compatible with their target and decoder version. They are
not portable simulator traces. The planned P1 registry will index these local
property IDs and their witness paths.
