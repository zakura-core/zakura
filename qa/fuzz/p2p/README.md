# P2P machine fuzzing

This isolated `cargo-fuzz` package is intentionally not a workspace member.
Each target drives one pure P2P machine from fuzz bytes and checks its
properties after every input. A target panics with the property ID
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
connection roles; the rest selects inputs. Decoder version 1 reads at most 512
bytes. The replay checks both machines' invariants after every input, and
checks each result against what the connection loop recorded from earlier
results. Named tests pin which result each rule picks.

The input decoding and the checks live in `zakura-network` behind the
`p2p-fuzz` feature, so the machine types stay private to the crate. Unit tests
there run the same entry on two witnesses and random histories in every test
run.

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
for two rows sharing one queue. It records each row's latest value, last send
and queued frames, and checks `cadence/hold` and `cadence/settlement` against
those records: each result names a hold the records show, and a queued frame
carries the row's latest value. Named tests pin which hold a row reports first.
A receive settles the real frame guard. This boundary does not exercise
scheduled transport writes.

Decoder version 1 reads at most 512 bytes. Each byte chooses update, elapsed
seconds, send or settlement. The fixed witness exercises pacing, an unwritten
frame, a full queue and settlement. Planted failures break pacing and ownership.
Proptest and cargo-fuzz call `replay_cadence_bytes`.

Run the applicability witness with `cargo test -p zakura-network --lib
regulation::cadence::fuzz -- --nocapture` from the repository root. Its printed
counters are witness evidence; they do not claim applicability for every fuzz
input. An empty input has zero applicability.

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
target drives one pure P2P machine from fuzz bytes and checks its properties
after every input. A target panics with the property ID that
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

`qa/fuzz/sim/block_work_ownership` drives the `block/work-ownership` owner through
trace and replay, so it needs no target here.

The retry target checks the supplier and alarm owners' invariants and compares
each lookup only with the deferrals it wrote. The peer-table target keeps a
small bookkeeping model of generations, close flags and published claims.

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
reactor target from PR #1683. `block_query` generates accepted and refused
proposals, exact and stale replies, resets, authority changes, and retry expiry.
Its identity ledger calls no production policy helper. `block_reactor` shares its
byte decoder with proptest, checks the reactor's invariants after every input,
and compares each query and mirror with the inputs it sent. It covers view
mirrors, reaction epochs, reset refills, query refusal, retry expiry, and
authority changes. Decoder 2 of
`block_reactor` added a closed-channel refusal to bit 16 of the refill byte. Decoder
3 delivers an authority byte as a committed view, which retires the pending query,
and reads bit 4 of a refill's byte as the first view's arrival. No committed corpus
uses an older decoder. Both targets decide pure transitions. Runtime witnesses cover
the action channel and storage waits.

`block_fill` comes from PR #1677, head
`7aef27b246dcf8f0d3625d598553d2a827411ea0`. Decoder 1 keeps that byte layout, and
its funding check now runs through queue admission. The driver takes up to 30
items, which the one-block production cap does not reach.
`block_request_lifecycle` comes from PR #1679. Its ledger tracks the requests
it issued, the writes it started, and the requests sent since the last body,
and it checks the request state's invariants after every input. Its witness expires one written
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
`3bc8261c6d1a0c1ec1daf55d361f5a33e34b5a8d`. Each target checks the sequencer's
invariants and compares its outputs with bookkeeping of the heights it
delivered and the driver accepted. Ordering and evidence consume three-byte
operations. Reset consumes one byte for the retired-apply count, then three-byte
operations. Each decoder reads at most 512 bytes. Empty input reports zero
applicability. Setup transitions do not count as generated coverage.

The ordering witness retries a refused contiguous submission. The reset witness
completes a retired apply while current work remains. The evidence witness holds
required evidence through modeled backpressure, then delivers it. Planted state
corruption must fail the intended check. These checks do not establish
production-mutation detection, decoded-memory retention, actual channel delivery,
or fresh-process semantic replay. The block fixture copies public mainnet block
1 from `zakura-test/src/vectors/block-main-0-000-001.txt`.

## Discovery targets

Each discovery target drives the production machine, checks its invariants
after every operation, and compares outputs only with what the replay sent.
Named scenarios beside each machine pin its rules.

`discovery_session` drives `Hello`, admitted answers and operation outcomes
under decoder 1. It checks exchange completion, single settlement and the
ownership recheck before a close.

`discovery_redial` drives attempt and registration lifetimes under decoder 2.
Its header byte chooses a maintained or a bounded policy. It checks local
limits, the backoff bounds, registration upgrades, resets and stopping.

`discovery_query` replays each input under both decoders, so its corpus keeps its
decoder-1 meaning: decoder 1 checks pure query ownership and each query's own
cap, and decoder 2 checks the real `Queries` ledger of successive sessions on
one pool, without a reader to take the `Hello` hold.

`discovery_candidate` drives dial, completion, round, logical-time and
ranking operations under decoder 2. It checks canonical IP reservations, the
bounds of per-node backoff and the round interval, not an asynchronous
worker's admission permit. A ranking forms the preferred set from the
standings it chooses and runs the preferred pass, and checks that the set
holds only gateways that may fill a held place and that no preferred dial
targets a gateway in backoff, connected or already being dialed, or a
gateway with a lower node ID before the mutual-dial grace has passed.

These targets check pure policies. They do not check worker cancellation or
connection ownership across scheduled IO.

## Handshake parser targets

| Target | Property | Decoder |
| --- | --- | --- |
| `credential_parse` | `credential/canonical-encoding`, `credential/node-id`, `credential/bounded-parse` | 1 |
| `key_list_decode` | `key-list/canonical-encoding`, `key-list/stable-id` | 1 |
| `composite_verify` | `composite/tamper-refused` | 1 |

These targets feed peer-chosen bytes to what `zakura-quic` parses before a v2
TLS handshake authenticates the peer, not machine histories. The drivers and
their checks live in `crates/zakura-quic/src/fuzz.rs` behind the crate's
`p2p-fuzz` feature, so `NodeCredential::from_spki` and `composite::verify` stay
private to the crate.

`credential_parse` reads its whole input both as a TLS raw public key
(`NodeCredential::from_spki`) and as a bare credential encoding
(`NodeCredential::decode`). An accepted credential re-encodes to exactly the
input and decodes back to itself. The driver rebuilds an accepted SPKI's
canonical DER, and the node ID from the ID-12 key fingerprints, from the spec
rather than with the code under test, and compares them with the input and the
credential's ID. The target
installs `MeteredAllocator` from this package's library, and no single
allocation of the two parses may exceed the input's length by more than 256
bytes.

`key_list_decode` decodes its input as a key list, as discovery does with a
peer's record. An accepted list re-encodes to exactly the input, and its node
ID is the ID-9 hash of the input's bytes.

`composite_verify` runs the Ed25519 + ML-DSA-44 verifier once per input. An
input whose first byte is even carries an Ed25519 key, a `u16`-prefixed
ML-DSA-44 key, a `u16`-prefixed message and the signature. An odd first byte
makes the rest four-byte edits of an honest signature, signed once per
process, so an empty corpus still explores near-valid signatures. The honest
signature must verify, and every edit of it must fail. A raw input checks only
that the verifier doesn't panic. ML-DSA verification
makes this target far slower per input than the others.

The campaign corpus isn't committed. To start a local campaign from the unit
tests' valid and malformed values, write the seeds first:

```console
cargo +nightly-2026-07-15 run --example handshake_seeds
cargo +nightly-2026-07-15 fuzz run --fuzz-dir . credential_parse
```

`zakura-quic`'s unit tests replay every seed and check that a seed named
`valid-` is accepted and one named `invalid-` is refused.

## BPv1 targets

| Target | Property | Decoder |
| --- | --- | --- |
| `bpv1_peer` | `bpv1/peer-replay` | 2 |
| `bpv1_block` | `bpv1/block-replay` | 3 |
| `bpv1_mesh` | `bpv1/mesh` | 1 |
| `bpv1_wire` | `bpv1/wire` | 1 |
| `bpv1_codec` | `bpv1/codec` | 1 |

The drivers and their checks live in
`crates/zakura-network/src/zakura/bpv1/` behind the `p2p-fuzz` feature:
`peer/fuzz.rs`, `blocks/fuzz.rs` and the `fuzz/` module.

`bpv1_peer` drives one `PeerState` with every `PeerInput` but `SessionEnd`,
which ends each replay so the drain check runs. `bpv1_block` drives one
`BlockTable` with every `BlockInput`. Both answer the work IDs the machine
issues, at any later step. A completion may answer current work, repeat
finished work or name work the machine never issued. A part may fail its
proof or meet a refusal, and a stripe job may fail as a cancelled job does.
Each replay asserts the machine's invariants after every input.
`bpv1_peer`'s `Have` operation sends a `u16` list or a bitmap, and can add
an index past the layout, a bitmap padding bit or a bitmap sized for another
layout, which close with row 16.

`bpv1_mesh` runs four nodes' production `PeerState`, `BlockTable` and
`PeerPaths` machines and one `ProposerState` over in-memory links. The input
picks which frame or job completion each node takes next, ticks, link loss
and what node 3, the adversary, sends. Jobs complete with production
verification and coding. The replay asserts every machine's invariants after
every step, that no honest link closes with a protocol row, and that every
ended session drains. One operation lets block sync carry every honest
tip more than twice the propagation window past the mesh's blocks, so the
tables evict their tombstones while frames are still in flight.

`bpv1_wire` decodes one arbitrary frame with every BPv1 decoder. A decoded
frame re-encodes to the same bytes, the largest allocation stays within
`Bpv1Message::max_heap_bytes`, and each stream decoder closes with the row
the frame breaks. Only an honest part proves. A decoded `Have` re-encodes in
the smaller form for the smallest layout that holds its indices. The
target installs
`MeteredAllocator`. `bpv1_codec` encodes an arbitrary body, decodes each
stripe from a chosen `k`-subset, refuses `k - 1` parts, catches a lying
proposer and checks canonical assembly.

To start a local campaign from the in-code seeds, write them first. To see
which inputs, outputs and close rows the seeds and a short generated run
reach, print the quality report; it runs no campaign:

```console
cargo +nightly-2026-07-15 run --example bpv1_seeds
cargo +nightly-2026-07-15 run --example bpv1_quality -- 200
cargo +nightly-2026-07-15 fuzz run --fuzz-dir . bpv1_mesh
```

`zakura-network`'s unit tests replay every seed, check that the seeds reach
every expected input, and replay generated and edited inputs.
