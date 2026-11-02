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

`discovery_session` decodes hello, admitted answers and operation outcomes.
It checks exchange completion, single settlement, and ownership checks before
close. Its witness reaches a close after the answer wait and ownership recheck.
Two planted failures skip the recheck or discard the success charge.

`discovery_redial` uses three header bytes for policy and backoff bounds.
Each remaining triple selects an attempt and registration lifetime. Its witness
checks local limits, backoff growth, registration upgrades, resets and stopping.
Two planted failures reset a failed attempt or charge a local limit.

`discovery_query` decodes reservation, queue, release and claim operations.
Its model calculates wire caps independently and checks that an answer claims
its own reservation once. Its witness exercises refusal, unsent release, an
exact-cap answer, an oversized answer and a stale claim. Planted failures lose
a reservation or release a queued query.

`discovery_query` replays every input under decoder 1 and then decoder 2, so
its corpus keeps its decoder-1 meaning. Decoder 2 drives the real `Queries` of
successive sessions on one pool: queries, writes, the transport's admission,
claims, settlements, `Hello` holds and retirement. Its model checks that each
claimed answer keeps its pool entry and its kind until it settles, including
after its session retires. It also checks that the close wait resolves exactly
when nothing delivered is unsettled, and that retirement closes the connection
exactly when a written query is unanswered. Its witness exercises each of those
holds. Planted failures count a settling `Hello` that does not exist or leak a
pool entry. It does not run a reader, so it does not check that the reader takes
the `Hello` hold.

`discovery_candidate` decodes triples into dial, completion, duplicate, round
and logical-time operations. Its model tracks canonical IP reservations and
per-node backoff. Its witness exercises a duplicate completion, failure backoff
and a local-limit pause. Planted failures release reservations early or discard
backoff. Final cleanup settles every remaining logical reservation. This does
not check that an asynchronous worker releases its admission permit.
