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

The available target is `service_session`. It replays one connection's inputs
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

## Block query and fill targets

| Target | Local property IDs | Applicability witness |
| --- | --- | --- |
| `block_query` | `block/query-dispatch`, `block/query-completion` | `query_witness_exercises_refusal_stale_identity_and_reset` |
| `block_fill` | `block/fill-retry-run`, `block/fill-retained-funding`, `block/fill-commit-window` | `fill_witness_uses_the_retained_run` |

Both decoder-v1 targets read at most 512 bytes. Proptest and cargo-fuzz call the
same driver in the production state module. Empty inputs report zero applicable
transitions. The witnesses print counters with `--nocapture`. Planted failures
must panic with the intended property ID.

`block_query` generates accepted and refused proposals, exact and stale replies,
resets, authority changes and retry expiry. Its identity ledger does not call
production policy helpers. It checks that refusals preserve the accepted query
and that a reply matches both the query ID and authority.

`block_fill` varies floor races, retry facts and retained size estimates. Its
reference keeps the first contiguous allowed run. It derives priority from that
run's actual start and checks the commit-window take ceiling. These cases prove
pure decisions. They do not prove asynchronous reservation cleanup.

Each target receives its own 900-second daily and 3,600-second weekly budget.
Keep the existing session budget intact. The planned P1 registry will index the
local IDs and witnesses. Machine decoder-v1 bytes are not portable sim traces.
