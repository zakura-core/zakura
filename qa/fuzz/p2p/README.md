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

`p2p-fuzz.yml` schedules this target and the machine targets below. It runs each
one nightly and for longer on Sunday, or on a manual dispatch. No fuzz campaign
blocks PR CI.
Machine bytes are compatible with their target and decoder version. They are
not portable simulator traces. The `[package.metadata.zakura-machine.targets]`
table in `Cargo.toml` registers each target's property IDs and decoder version.
`cargo xtask sim-suite` generates its coverage rows from that table, and
`scripts/check-fuzz-campaign.py` fails a pull request whose target has no entry or
no campaign.

## Corpus and replay

The campaign caches `corpus/<target>/` under the target's name and decoder version,
so a new decoder starts an empty corpus. Before fuzzing, the campaign replays that
corpus in a fresh process with `-runs=0`. After a failure, it replays each file in
`artifacts/<target>/` the same way and uploads them. Reproduce both from this
directory:

```console
cargo +nightly-2026-07-15 fuzz run --fuzz-dir . <target> corpus/<target> -- -runs=0
cargo +nightly-2026-07-15 fuzz run --fuzz-dir . <target> path/to/artifact -- -runs=1
```

## Block machine targets

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

`block_fill` comes from PR #1677, head
`7aef27b246dcf8f0d3625d598553d2a827411ea0`. Decoder 1 keeps that byte layout, and
its funding check now runs through queue admission. The driver takes up to 30
items, which the one-block production cap does not reach.
`block_request_lifecycle` comes from PR #1679 and tracks requests,
transport-start facts, and no-progress charges. Its witness expires one written
request and one unwritten request, matches a replacement request, and resets the
remaining window. Other machine
targets follow their owning delivery packets.

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
