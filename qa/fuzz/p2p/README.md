# P2P machine fuzzing

This isolated `cargo-fuzz` package is intentionally not a workspace member. Each
target drives one pure P2P machine from fuzz bytes and checks it against an
independent model after every input. A target panics with the property ID that
the machine broke. The targets do not start QUIC or the simulator step loop.

Each owning PR adds its targets as `[[bin]]` entries, with a row in the table
below. Decoders read at most 512 bytes. Empty inputs report zero applicability.

| Target | Property | Decoder | Applicability witness |
| --- | --- | --- | --- |
| `discovery_session` | `discovery/session-policy` | 1 | `discovery_session_witness_reaches_close_after_recheck` |
| `discovery_redial` | `discovery/redial-policy` | 1 | `redial_witness_exercises_limits_resets_and_lagging_registration` |
| `discovery_query` | `discovery/query-ownership` | 1 | `query_witness_exercises_ownership_and_caps` |
| `discovery_query` | `discovery/answer-settlement` | 2 | `settlement_witness_exercises_holds_and_retirement` |
| `discovery_candidate` | `discovery/dial-attempts` | 1 | `candidate_witness_exercises_identity_backoff_and_local_limits` |

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

## Discovery targets

`discovery_session` drives `Hello`, admitted answers and operation outcomes, and
its model checks exchange completion, single settlement and the ownership
recheck before a close.

`discovery_redial` drives attempt and registration lifetimes, and its model
checks local limits, backoff growth, registration upgrades, resets and stopping.

`discovery_query` replays each input under both decoders, so its corpus keeps its
decoder-1 meaning: decoder 1 checks pure query ownership and wire caps, and
decoder 2 checks the real `Queries` ledger of successive sessions on one pool,
without a reader to take the `Hello` hold.

`discovery_candidate` drives dial, completion, round and logical-time operations,
and its model checks canonical IP reservations and per-node backoff, not an
asynchronous worker's admission permit.

These targets check pure policies. They do not check worker cancellation or
connection ownership across scheduled IO.
