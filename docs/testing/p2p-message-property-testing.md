# P2P message property testing

This is the local development standard for property testing Zakura P2P
messages. The reference implementation deliberately stops at two stream-6
messages, `GetBlocks` and `Status`, in
`crates/zakura-network/src/zakura/block_sync/property_tests/`. They share the
same reporting harness but keep separate contract oracles and evidence.

Both targets are **candidate contracts** based on the proposed message
regulation design. A reported divergence means the target and current code
disagree. It does not, by itself, decide whether the code or the target should
change.

## What this kind of testing establishes

Think of a property suite as a contract examiner. Instead of checking only a
few examples, it manufactures many examples, checks each one against the same
rules, and reduces a failure to a small reproducible input.

It is not formal verification and it does not test every possible byte string.
The suite combines three different strengths:

- Golden vectors check exact, reviewable wire bytes.
- Exhaustive tests cover genuinely small finite domains, such as counts
  `0..=129`.
- Proptest samples large domains, emphasizes boundaries, and shrinks failures.

Only the first two are exhaustive over the values they enumerate. Generated
tests provide broad evidence, not a proof over all possible inputs.

## Required shape of a message suite

Every message property suite should contain the following parts in this order.

### 1. Message metadata

Record the facts a reviewer needs before reading code.

| Field | Required content |
| --- | --- |
| Message | Rust variant and protocol name |
| Stream | Stream kind and version |
| Direction | Request, response, notification, or both |
| Discriminator | Exact outer and inner type values |
| Canonical layout | Field order, widths, and byte order |
| Maximum size | Payload and frame limits, including when enforced |
| Contract source | Candidate design, ratified spec, or implementation contract |

### 2. A rule ledger

Give every rule a stable ID formed from a short message prefix and a two-digit
number, such as `GB-03`. Each ledger row must include:

| Field | Meaning |
| --- | --- |
| ID | Stable rule identifier |
| Requirement | One behavior that can be tested independently |
| Status | `conformant` or `candidate-contract divergence` |
| Evidence | A deterministic test function that demonstrates the status |

Keep one rule per row. Separate encoding, semantic bounds, frame agreement,
and resource limits even when one decoder branch currently enforces several of
them.

The report test must execute every ledger evidence function and print its case
census. This makes a missing evidence link fail locally instead of leaving an
unchecked row in a Markdown table. The census separates candidate-legal,
rule-invalid, compound-invalid, and known-divergence witnesses so a large test
count cannot disguise shallow coverage.

### 3. An independent contract oracle

The oracle answers whether bytes satisfy the selected contract. It must not
call the production encoder, decoder, constants, or validation helpers.

Use contract literals inside the test module, then add a separate test that
compares those literals with production constants. This deliberate duplication
prevents a single incorrect helper from making both implementation and test
agree.

The oracle should return structured rejection reasons. The reasons make
single-rule tests readable and make a discovered divergence precise.

### 4. Exact golden vectors

Include, at minimum:

- the smallest legal value;
- an ordinary value whose byte order is visually obvious;
- every meaningful maximum or transition boundary;
- a vector that combines dependent maxima, when fields constrain each other.

Golden vectors must spell out the expected bytes. Generating the expected bytes
with the production encoder defeats their purpose.

### 5. Three input lanes

**Legal by construction.** Generate only values that satisfy every contract
rule. For dependent fields, derive later ranges from earlier choices. For
example, a legal `GetBlocks` start height depends on its count.

**Single-rule invalid.** Start from a legal value and violate exactly one rule.
These tests should check the specific rejection boundary and, where stable and
useful, the structured production error.

**Compound invalid.** Generate raw or structured inputs that can violate
several rules at once. These tests check safe rejection, panic freedom, and
resource bounds. Do not over-specify which error wins unless error precedence
is itself part of the protocol contract.

Bias all generators toward zero, one, maximum, maximum plus one, and dependent
boundaries. Also retain a broad random branch so the suite explores values the
author did not list manually.

### 6. Cross-layer properties

A complete message suite should cover the applicable properties below:

- candidate-legal values encode to the oracle's exact bytes;
- candidate-legal bytes decode to the original value;
- accepted encodings re-encode canonically;
- invalid encodings are rejected without panicking;
- the decoder consumes the payload exactly;
- outer frame type, inner discriminator, flags, and version agree;
- counts, lengths, and allocations are bounded at the earliest relevant layer;
- encode and decode behavior agree with the oracle except for ledgered
  divergences.

Round trips alone are insufficient. An encoder and decoder can share the same
bug and still round-trip successfully.

### 7. Explicit divergence evidence

Never hide known disagreements by weakening the oracle or excluding an input
from a generator. Add a ledger row and a deterministic minimal witness that
states:

- the current behavior;
- the target behavior;
- the smallest clear input that distinguishes them;
- whether the target is candidate or ratified;
- the eventual resolution when the team decides it.

The evidence test should pass while accurately characterizing the current
behavior. If either side changes, the test fails and forces the ledger to be
reviewed. A divergence is not an automatic production-code change.

## `GetBlocks` reference contract

The pilot currently reports these nine rules:

| ID | Candidate requirement | Current result |
| --- | --- | --- |
| GB-01 | Payload discriminator is `2` | Conformant |
| GB-02 | Canonical payload is 9 little-endian bytes | Conformant |
| GB-03 | Count is in `1..=128` | Conformant |
| GB-04 | Start height does not exceed `Height::MAX` | Conformant |
| GB-05 | Inclusive range end does not exceed `Height::MAX` | Divergence |
| GB-06 | No trailing payload bytes | Conformant |
| GB-07 | Frame flags are zero | Conformant |
| GB-08 | Outer and inner message types agree | Conformant |
| GB-09 | The 9-byte message cap applies before payload allocation | Divergence |

GB-05 checks the last legal start and the first overflowing start for every
count in `1..=128`. This yields 127 deterministic divergence witnesses. The
candidate oracle rejects each overflowing inclusive range; the current codec
accepts it because it validates the two fields separately.

GB-09's witness is a canonical 9-byte request plus one trailing byte. The
stream-wide transport admission cap accepts the 10-byte payload, then the codec
rejects it as trailing data. This identifies where a message-specific
pre-allocation cap would differ from the current design.

## `Status` reference contract

The second and final pilot message reports these twelve rules:

| ID | Candidate requirement | Current result |
| --- | --- | --- |
| ST-01 | Payload discriminator is `1` | Conformant |
| ST-02 | Canonical payload is 53 little-endian bytes | Conformant |
| ST-03 | `servable_low` does not exceed `Height::MAX` | Conformant |
| ST-04 | `servable_high` does not exceed `Height::MAX` | Conformant |
| ST-05 | `servable_low <= servable_high` during decode | Divergence |
| ST-06 | Maximum blocks per response is in `1..=128` | Divergence |
| ST-07 | Maximum in-flight requests is in `1..=32768` | Divergence |
| ST-08 | Maximum response bytes is in `1..=33554432` | Divergence |
| ST-09 | No trailing payload bytes | Conformant |
| ST-10 | Frame flags are zero | Conformant |
| ST-11 | Outer and inner message types agree | Conformant |
| ST-12 | The 53-byte message cap applies before payload allocation | Divergence |

ST-05 records a layer mismatch. The codec accepts an inverted servable range,
then the peer routine rejects it later. The candidate contract requires bounded
decode to reject it.

ST-06 through ST-08 record normalization mismatches. The current decoder
clamps advertised capacities instead of rejecting out-of-range wire values.
The encoder is also asymmetric. It clamps maximum blocks, emits maximum
in-flight requests unchanged, and only raises a zero maximum response size.
The deterministic evidence checks the lower and upper boundary on each field.

ST-12 is the `Status` counterpart to GB-09. The stream-wide transport cap
admits a 54-byte payload before the codec rejects its trailing byte; the
candidate contract asks for the 53-byte cap before allocation.

## What the pilot actually executes

The normal local report executes deterministic evidence before any generated
exploration:

- every `u8` payload discriminator and every `u16` frame type and flag value;
- complete small boundary domains such as `GetBlocks` counts `0..=129`;
- legal, first-invalid, and extreme values for all `Status` capacities;
- truncation and trailing-byte cases for both canonical payload lengths;
- dependent-boundary matrices for ranges;
- compound-invalid Cartesian matrices;
- exact hand-written golden vectors; and
- representative messages through the real typed peer session and framed
  channel.

The report prints deterministic evidence executions by category and separately
prints the configured Proptest attempts. These numbers have different meanings.
Deterministic rows show exactly what was enumerated, but rule rows can reuse the
same input and the total is therefore not a count of unique wire messages.
Generated attempts explore the much larger remaining input space and shrink any
failure to a reproducible case. The local script runs report tests serially so
their tables remain readable.

## Local workflow

Run both pilot contracts and print their evidence during normal development:

```sh
./scripts/test-p2p-message-contracts.sh
```

Run only one pilot message while editing it:

```sh
./scripts/test-p2p-message-contracts.sh --message get-blocks
./scripts/test-p2p-message-contracts.sh --message status
```

Run a deeper generated pass before requesting review:

```sh
./scripts/test-p2p-message-contracts.sh --deep
```

The deep mode defaults to 10,000 generated attempts per property. Override it
with `PROPTEST_CASES` when useful. The script's `--mutants` mode performs a
slower, scoped mutation-sensitivity check when `cargo-mutants` is installed;
use it when changing the codec or the contract harness, not on every edit.

When Proptest finds a failure, preserve the minimal failing input it prints.
The test runner also records regressions when source-file persistence is
available. To replay a printed seed directly, use the reported value with
`PROPTEST_RNG_SEED` and rerun the same filtered test.

CI policy is intentionally deferred. These commands are the initial developer
contract; later CI tiers can select case counts based on measured runtime and
signal.

## New-message checklist

Before considering a new P2P message implementation complete:

- [ ] Write the metadata and candidate or ratified contract source.
- [ ] Assign stable rule IDs and create the executable ledger.
- [ ] Implement an oracle independent of production validation code.
- [ ] Add exact golden vectors.
- [ ] Exhaust every small boundary domain and finite frame matrix.
- [ ] Add legal, single-rule-invalid, and compound-invalid generators.
- [ ] Check canonical encoding, decoding, frame agreement, and resource caps.
- [ ] Record every known divergence with a minimal witness.
- [ ] Run the focused report and a deep local pass.
- [ ] Review every persisted regression and commit intentional cases.

This stable file layout, naming scheme, ledger, and command shape are also the
interface for future Codex skills and repository automation. Automation should
check that the artifacts exist and remain connected; it should not invent or
silently resolve protocol requirements.
