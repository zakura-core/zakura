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

Each nightly target keeps its own 900-second daily and 3,600-second weekly
budget. CI restores corpora by target and decoder version. CI uploads the corpus,
crash artifacts, witness counters, compiler version, source revision and fuzz log.
Machine bytes are compatible with their target and decoder version. They are
not portable simulator traces. The planned P1 registry will index these local
property IDs and their witness paths.

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

## Discovery session and redial targets

| Target | Local property ID | Applicability witness |
| --- | --- | --- |
| `discovery_session` | `discovery/session-policy` | `discovery_session_witness_reaches_close_after_recheck` |
| `discovery_redial` | `discovery/redial-policy` | `redial_witness_exercises_limits_resets_and_lagging_registration` |

Both decoder-v1 targets read at most 512 bytes and reuse their existing
independent models for proptest and cargo-fuzz. Empty inputs report zero
applicability. Each target retains its own daily and weekly budget.

`discovery_session` decodes hello, admitted answers and operation outcomes.
It checks exchange completion, single settlement, and ownership checks before
close. Its witness reaches a close after the answer wait and ownership recheck.
Two planted failures skip the recheck or discard the success charge.

`discovery_redial` uses three header bytes for policy and backoff bounds.
Each remaining triple selects an attempt and registration lifetime. Its witness
checks local limits, backoff growth, registration upgrades, resets and stopping.
Two planted failures reset a failed attempt or charge a local limit.

These targets check pure policies. They do not prove worker cancellation or
connection ownership across scheduled IO. The planned P1 registry will index
the local IDs and witnesses.

## Discovery query and candidate targets

| Target | Local property ID | Applicability witness |
| --- | --- | --- |
| `discovery_query` | `discovery/query-ownership` | `query_witness_exercises_ownership_and_caps` |
| `discovery_candidate` | `discovery/dial-attempts` | `candidate_witness_exercises_identity_backoff_and_local_limits` |

Both decoder-v1 targets share their existing proptest models with cargo-fuzz.
They read at most 512 bytes and report zero applicability for empty histories.
Each target has its own 900-second daily and 3,600-second weekly budget.

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
not prove that an asynchronous worker releases its admission permit.

## Block ownership and lifecycle targets

| Target | Local property ID | Applicability witness |
| --- | --- | --- |
| `block_work_ownership` | `block/work-ownership` | `work_witness_exercises_reset_stale_return_and_settlement` |
| `block_peer_registry` | `block/peer-registry` | `registry_witness_exercises_replacement_and_exact_owner` |
| `block_retry_authority` | `block/retry-authority` | `retry_witness_exercises_expiry_and_authority_changes` |
| `block_request_lifecycle` | `block/request-lifecycle` | `lifecycle_witness_exercises_replacement_timeout_and_skipped_write` |

These decoder-v1 targets reuse the owning modules' independent models. Proptest
and cargo-fuzz call the same driver. Each decoder reads at most 512 bytes in
four-byte operations. Empty inputs report zero applicability. Each target has
two planted failures that must name its property ID, and its own daily and
weekly fuzz budget.

`block_work_ownership` tracks each height's location, exact claim owner and
charged bytes. The witness resets between two published claims, returns the
stale claim, and settles exactly the charged bytes.

`block_peer_registry` tracks admitted session generations and outstanding
request owners. The witness rejects stale-generation operations and a removal
that names the wrong request owner.

`block_retry_authority` tracks supplier-local deferrals and all-supplier alarms.
The witness changes authority and advances logical time through expiry.

`block_request_lifecycle` tracks requests, transport-start facts and no-progress
charges. The witness expires one written request and one unwritten request,
matches a replacement request, and resets the remaining window.

These ledgers prove logical state transitions. Scheduled IO still needs separate
proof that the actual guard or permit settles with its owner. P1 will register
these local property IDs when its shared schema exists.

## Sequencer targets

| Target | Local property ID | Applicability witness |
| --- | --- | --- |
| `block_sequencer_order` | `block/contiguous-submission` | `sequencer_order_witness_retries_a_refused_contiguous_prefix` |
| `block_sequencer_reset` | `block/retired-apply-isolation` | `sequencer_reset_witness_isolates_a_retired_completion` |
| `block_sequencer_evidence` | `block/required-evidence` | `sequencer_evidence_witness_retains_evidence_while_full` |

Each decoder-v1 target reads at most 512 bytes and uses the existing sequencer
model for proptest and cargo-fuzz. The ordering witness delivers bodies out of
order and retries a refused submission. The reset witness completes a retired
apply while current work remains. The evidence witness holds required evidence
while the action channel is full, then delivers it. Each witness has a planted
failure that must report the intended property ID.

Ordering and evidence use three-byte operations. Reset uses one header byte for
the retired-apply count, then three-byte operations. Empty inputs report zero
applicability. Setup creates valid fixture state; setup transitions do not count
as generated coverage. The package-local block fixture copies public mainnet
block 1 from `zakura-test/src/vectors/block-main-0-000-001.txt`.

Each target has its own 900-second daily and 3,600-second weekly budget. These
tests prove logical submission and evidence ownership. They do not prove
decoded-memory retention or asynchronous channel delivery.

## Header publisher target

`header_publisher` shares the existing publisher model between proptest and
cargo-fuzz. Its local property ID is `header/publisher-policy`. The model checks
credit, contiguous pages, legal terminal outcomes, stale jobs and shutdown.
It calculates schema-0 Regtest entry size as 181 bytes and the page prefix as
82 bytes from the wire fields, independently of production size helpers.

Decoder version 1 reads at most 512 bytes in four-byte operations. Empty inputs
report zero applicability. `publisher_witness_exercises_credit_terminal_and_stale_page`
spends credit on one page, writes the terminal outcome, then rejects a stale page.
Three planted failures lose credit, a job, or a terminal outcome. They must
report the intended property ID. Each operation compares the complete credit
ledger with the independent model.

The target has its own 900-second daily and 3,600-second weekly budget. It checks
the publisher state. Scheduled storage waits and transport writes remain
separate simulator adapter obligations.
