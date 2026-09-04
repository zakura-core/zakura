# Property testing for native P2P contracts

> **Status: proposal.** This document defines how Zakura turns native P2P
> requirements into executable evidence. The
> [message-regulation design](peer-message-regulation.md) separately defines
> the production traffic controls that some of these tests exercise.

Property testing generates many inputs or event histories and checks a property
that must hold for all of them. It searches for counterexamples and shrinks a
failure toward a smaller reproducer. It does not try every possible value and
does not prove production correct.

Bounded model exploration can make a stronger claim about a deliberately small
finite model. Native transport and load tests answer different questions again.
No one lane replaces the others.

## Contract layers

Each exchange uses the layers that apply:

| Prefix | Layer | Question |
| --- | --- | --- |
| `<MESSAGE>-WF-##` | Wire format | Which bytes are accepted, rejected, and canonical? |
| `<MESSAGE>-SM-##` | State model | What should happen across peer, state, and lifecycle events? |
| `<MESSAGE>-RL-##` | Regulated load | Do declared traffic controls bound resources and preserve useful progress? |

An exchange request owns regulation for the response work it causes. Response
messages still need their own wire and receiving-side contracts, but they do
not need a second regulator for the same response bytes.

Every requirement has a stable ID. Its primary test name begins with the
lowercase, underscore-separated ID, such as `gb_sm_01_...`. A contract is
specified when the requirements are written. It is implemented only when those
requirements point to executable evidence.

Shared framework requirements may map to one or more exchange-specific IDs
instead of duplicating the same test for every message. The exchange contract
must make that mapping explicit and may use shared evidence only when it runs
through the same production path.

## Test layers

Use the smallest production boundary that proves the claim:

| Layer | Purpose |
| --- | --- |
| Deterministic examples | Pin encodings, boundaries, invalid classes, and known schedules. |
| Generated properties | Search combinations and shrink failures. |
| Stateful model | Compare generated histories with the real service path after each step. |
| Lifecycle regressions | Force valid task orderings that a generated runner cannot reliably place. |
| Fast regulation properties | Check accounting, ownership, cleanup, and logical bounds without expensive transport load. |
| Native load properties | Apply real framed traffic, queue pressure, slow readers, and QUIC flow control. |
| Fuzzing | Search arbitrary bytes for panics, inexact decoding, and allocation failures. |
| Bounded exploration | Visit every state in a stated finite model when that stronger claim is useful. |

Keep each message validity check callable without I/O, locks, or mutable shared
state. Pass fixed network rules and the authenticated peer identity as explicit
inputs so generated tests and fuzz targets can exercise the check directly.

Native load tests must exercise a declared policy boundary. An unregulated
flood mostly measures the machine running it.

## Generated stateful tests

A stateful property test applies the same scenario to two paths:

```text
generated scenario ──→ independent reference model ──→ expected observation
                  └──→ production service path ───────→ observed result
```

The reference model describes only the behavior needed by the contract. It
must not call the production decision it checks. The production runner should
use real codecs, services, routines, reactors, identifiers, and frame paths
unless the property explicitly starts at a narrower boundary.

Compare expected and observed results after each step. A later transition can
otherwise hide an earlier mismatch.

### Generated inputs

Bias generation toward meaningful boundaries:

- minimum, maximum, just-inside, and just-outside wire values;
- empty, one-below-full, full, and one-above-full resource states;
- first, repeated, stale, replaced, cancelled, and completed sessions;
- successful, unavailable, short, exact-cap, and over-cap responses; and
- alternating peers and independent work.

Classify scenario inputs by who can create them:

- **Peer** inputs are frames and connection changes a remote peer can cause.
- **Driver** inputs are valid results returned through a production service
  boundary.
- **Internal** inputs are forged or unreachable events used to check fail-safe
  behavior.

A total generator may produce harmless operations against missing peers or
retired requests. Those cases improve shrinking and robustness. They do not
count as required coverage unless the contract says they should.

Keep conformant and deliberately invalid claims distinct. A conformant lane
generates only sender-permitted behavior. A focused invalid case should violate
one named rule so its expected outcome remains clear. Mixed hostile histories
may then search interactions after each individual rule has deterministic
coverage.

### Required coverage

Classify requirements as:

- **Occurrence:** mark the requirement only after its precondition occurred
  and the expected and production observations matched.
- **Invariant:** report how many successful step comparisons checked it.
- **Regression only:** name the deterministic test that forces its schedule.

Run a deterministic coverage floor before random histories. The suite must name
every missing requirement rather than relying on a random case count to reach
it. Coverage candidates created during a model transition become covered only
after the production comparison succeeds.

Use a requirement enum or machine-checked manifest tied to Rust's registered
test inventory. A missing ID or ID-named test must fail explicitly.

### Ordering and quiescence

A settled step needs an explicit test barrier for the production path being
observed. A fixed number of task yields or a short sleep is useful for
diagnosis, but it is not proof that the system settled.

An unsettled step may combine operations only when they share one FIFO path or
an explicit happens-before relationship. If multiple independent channels or
peers can race, the model must accept every legal interleaving or the runner
must settle between operations.

Keep forced-ordering regressions for production-valid schedules that normal
barriers intentionally make unreachable.

## Replaying failures

A seed reproduces a generated case only while its strategy and materializer are
unchanged. Always preserve an important failure as a focused regression.

When a suite claims replay across strategy or backend changes, serialize the
minimized scenario with:

- a schema version;
- the suite and selected invalid rule, if any;
- the model bounds; and
- the ordered protocol actions.

Replay serialized actions directly, not by reconstructing them from random
choices. Run a claimed deterministic replay twice and compare its observations
and final resource state.

Environment variables controlling cases or seeds must use defaults only when
absent. An invalid value must fail with the variable name and supplied value.
The run should print its effective case count and configured seed. The property
framework's persisted failure is still the authority for a randomly chosen
seed.

## Sensitivity

Coverage shows that a path ran. Sensitivity shows that the test can notice an
incorrect result.

For a new or substantially changed harness:

1. Restore each relevant historical defect family one at a time, or introduce
   an equivalent controlled mutation.
2. Confirm that the expected test becomes red.
3. Cover each observation channel the comparison relies on, such as actions,
   frames by session, peer snapshots, and cancellation state.
4. Record the experiment in the implementation PR.

This does not require one permanent mutant per requirement. Historical defects
plus one mutation per observation channel are enough to demonstrate that the
comparison is not blind.

## Reachability claims

A model failure proves a mismatch only at the boundary under test. Use these
terms for stronger claims:

| Result | Required evidence |
| --- | --- |
| Model detected | A generated or focused model case fails; remote reachability is unknown. |
| Peer-triggered, natural | Real connections and wire messages reproduce it without timing controls. |
| Peer-triggered, controlled schedule | A delay or barrier makes a production-valid ordering repeatable without inventing it. |
| Internal robustness only | The failure requires a forged or unreachable internal event. |
| Not reproduced | The attempted reproduction stayed green; record the conditions and attempts. |

For operational findings, save the failing history and configuration, run the
same peer sequence on affected and fixed revisions, and retain a deterministic
regression when practical.

## Inventory closure

The long-term compiler checks have two different scopes:

```text
wire kinds
  = codec and dispatch arms
  = wire-contract entries

regulated exchange roots
  = regulation declarations
  = regulated-load contract entries
```

The second set contains initiating requests or standalone announcements, not
every response variant. A request's response types remain in the wire set and
are named by its exchange declaration.

Derive inventories from production enums or one shared declaration rather than
maintaining fixed message counts. Until that mechanism exists, the catalog and
manifest tests are the review gate. Compiler closure is required before a
second exchange is called a reusable implementation of this standard.

## Implementation workflow

For a new or changed native P2P exchange:

1. Specify its wire, state-model, and applicable regulation requirements.
2. Add deterministic boundaries and invalid classes independently of
   production validation.
3. Exercise the real production path and compare after each step.
4. Make every requirement ID's coverage explicit before adding random cases.
5. Preserve failures as focused regressions and confirm harness sensitivity.
6. Reproduce peer-reachable findings before assigning operational severity.
7. Test load only after the regulation policy and overload outcomes are
   defined.

The [P2P contract catalog](../specs/native-p2p/README.md) tracks which
layers are specified and implemented. The
[block-range contract](../specs/native-p2p/block-range.md) is the
first concrete exchange.
