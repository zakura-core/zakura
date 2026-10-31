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

These targets call the independent models beside the production pure owners.
The `p2p-fuzz` feature exposes the drivers. The targets do not start QUIC or the
simulator step loop.

| Target | Property | Decoder |
| --- | --- | --- |
| `block_work_ownership` | `block/work-ownership` | 1 |
| `block_peer_registry` | `block/peer-registry` | 1 |
| `block_retry_authority` | `block/retry-authority` | 1 |
| `block_query` | `block/query-dispatch`, `block/query-completion` | 1 |
| `block_reactor` | `block/reactor-policy` | 2 |
| `block_fill` | `block/fill-retry-run`, `block/fill-retained-funding`, `block/fill-commit-window` | 1 |
| `block_request_lifecycle` | `block/request-lifecycle` | 1 |

Each decoder reads at most 512 bytes. Each model checks the state after every
operation. The colocated tests retain nonzero witnesses and planted failures.
Those tests establish logical behavior. They do not establish runtime resource
retention or full semantic trace replay.

Run targets manually with a compatible nightly toolchain and cargo-fuzz:

```sh
cargo +nightly fuzz run --fuzz-dir qa/fuzz/p2p block_work_ownership -- -max_len=512
```

This package preserves the three drivers from PR #1679, change
`d17393bea6a5f32633fb29b82426fd7b82b1b086`, at reviewed head
`d4fc17f468487d03b3b554dade009bf2538be95b`. The head's later witness-message
correction remains intact. Decoder 1 keeps the original byte interpretation.
`block_query` and `block_reactor` preserve the query portion of PR #1677 and the
reactor model from PR #1683. `block_query` generates accepted and refused
proposals, exact and stale replies, resets, authority changes, and retry expiry.
Its identity ledger calls no production policy helper. `block_reactor` shares the
reactor policy model with proptest and covers view mirrors, reaction epochs, reset
refills, query refusal, retry expiry, and authority changes. Decoder 2 of
`block_reactor` adds a closed-channel refusal to bit 16 of the refill byte. No
committed corpus uses decoder 1. Both decide pure
transitions. Runtime witnesses cover the action channel and storage waits.


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

## Discovery state targets

| Target | Local property ID | Applicability witness |
| --- | --- | --- |
| `discovery_book` | `discovery/record-book` | `discovery_book_witness_exercises_provenance_quota_and_expiry` |
| `discovery_state` | `discovery/session-installation` | `discovery_state_witness_exercises_stale_verification_and_expiry` |

Both targets reuse the existing independent models. Decoder version 1 reads at
most 512 bytes in four-byte operations. Empty inputs report zero applicability.

The book witness imports relayed records, rejects a relay-quota overflow, updates
dial metadata, changes provenance, advances time and rejects an expired record.
Two planted failures add unearned confirmation or an extra dial failure.
The session witness replaces a session before verification completes, rejects
the stale result, installs the current result and expires a live service summary.
Two planted failures remove the current admission or install an unearned hint.
Each planted failure must report the intended property ID.

Decoder version 1 excludes peer sampling, and its corpus keeps that meaning.
The existing proptest checks sampling eligibility and disclosure limits. The
`zakura-sim` target `discovery/record-book` uses decoder version 2 to replay
signed samples under a fixed disclosure key, a seeded shuffle and a fixture clock.
Record metadata and session checks compare normalized state and do not depend on
hash iteration order. These targets check synchronous installation rules;
`discovery/verification-capacity` checks the real handle's task cancellation and
verification waits.

## Discovery session and redial targets

| Target | Local property ID | Applicability witness |
| --- | --- | --- |
| `discovery_session` | `discovery/session-policy` | `discovery_session_witness_reaches_close_after_recheck` |
| `discovery_redial` | `discovery/redial-policy` | `redial_witness_exercises_limits_resets_and_lagging_registration` |

Both decoder-v1 targets read at most 512 bytes and reuse their existing
independent models for proptest and cargo-fuzz. Empty inputs report zero
applicability.

`discovery_session` decodes hello, admitted answers and operation outcomes.
It checks exchange completion, single settlement, and ownership checks before
close. Its witness reaches a close after the answer wait and ownership recheck.
Two planted failures skip the recheck or discard the success charge.

`discovery_redial` uses three header bytes for policy and backoff bounds.
Each remaining triple selects an attempt and registration lifetime. Its witness
checks local limits, backoff growth, registration upgrades, resets and stopping.
Two planted failures reset a failed attempt or charge a local limit.

These targets check pure policies. They do not check worker cancellation or
connection ownership across scheduled IO.

## Discovery query and candidate targets

| Target | Local property ID | Applicability witness |
| --- | --- | --- |
| `discovery_query` | `discovery/query-ownership` | `query_witness_exercises_ownership_and_caps` |
| `discovery_candidate` | `discovery/dial-attempts` | `candidate_witness_exercises_identity_backoff_and_local_limits` |

Both decoder-v1 targets share their existing proptest models with cargo-fuzz.
They read at most 512 bytes and report zero applicability for empty histories.

`discovery_query` decodes reservation, queue, release and claim operations.
Its model calculates wire caps independently and checks that an answer claims
its own reservation once. Its witness exercises refusal, unsent release, an
exact-cap answer, an oversized answer and a stale claim. Planted failures lose
a reservation or release a queued query.

`discovery_candidate` decodes triples into dial, completion, duplicate, round
and logical-time operations. Its model tracks canonical IP reservations and
per-node backoff. Its witness exercises a duplicate completion, failure backoff
and a local-limit pause. Planted failures release reservations early or discard
backoff. Final cleanup settles every remaining logical reservation. This does
not check that an asynchronous worker releases its admission permit.
