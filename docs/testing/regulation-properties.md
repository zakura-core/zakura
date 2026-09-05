# Regulation property tests

The suite checks that serving work stops at its resource limits and resumes
when capacity is released. It exercises the production accounting code with an
independent expected-state model. GetBlocks is the first policy; the shared
primitive properties also apply to future regulated messages.

## Start here

Run the focused suite from the repository root:

```sh
cargo nextest run --locked --profile regulation-properties \
  -p zakura-network -p zakura-state -p zakura --lib
```

For a reproducible extended run:

```sh
PROPTEST_CASES=2048 PROPTEST_RNG_SEED=892 cargo nextest run --locked \
  --profile regulation-properties -p zakura-network -p zakura-state \
  -p zakura --lib
```

The ordinary unit-test profiles include these tests. Their regulation overrides
and the focused profile use zero retries. The focused suite also runs the
serving policy's lifecycle tests, including concurrent claim and
cancellation checks. PR CI also runs a fixed seed;
scheduled and manual CI runs explore more cases with the CI run ID as their
seed. Stacked PRs on `adam/**` receive the same Rust CI entrypoints as other
development bases.

## What each layer establishes

| Layer | Production path | Independent expectation |
| --- | --- | --- |
| Primitives | Rate, byte, and slot budgets | Balance and owner totals |
| GetBlocks | Admission, sender, writer | Logical resource ledger |
| Reactor | Peer routine and response queue | Prefix, terminal, leases |
| Driver | Actual state worker | Read lifetime and waiting peer |
| Storage | Bounded range collector | Prefix sum and lookup sequence |

The ownership model does not call production cost, admission, transfer, or
refund helpers. It shares configuration inputs, not state transitions. The rate
oracle represents fractional units directly, rather than copying production's
separate whole-token and remainder fields. Credit discarded at burst capacity
is not available for later spending.

The transport adapter uses `worker_framed_channel` and
`QueuedFrame::write_with`. The ordinary synthetic receiver unwraps frames at
dequeue, so it cannot establish that bytes remain charged during an unfinished
application write. Read-only budget handles observe retired sessions without
keeping their permits or identity accounts alive.

## GetBlocks contract and witnesses

The ownership scenario uses two identities, up to eight session generations,
four request slots, four input slots, and output queues of depth two. Each
request allows one block, using the committed mainnet height-one fixture. Its
allowance is 2,000,010 payload bytes plus configured fixed work. It is a
reservation; the test does not allocate that many bytes to fill a budget.

Accepted local response caps must fit one maximum-sized block. Cost properties
check its block and terminal reservation; storage properties require a nonempty
prefix when valid-sized blocks are available. A fixed reactor witness uses the
minimum accepted cap to serve a near-limit serialization fixture and retain its
charge through the application write. That fixture checks size and ownership,
not consensus validity.

Separate cost properties vary legal counts and byte caps. Reactor histories use
one to three committed blocks and queue depths one to three. These deliberately
small capacities reach full queues and resource limits with bounded
allocations.

- **Admission before work and transactional rollback.** Each of the six
  peer/node rate, slot, and byte limits blocks admission; releasing ownership
  and refilling permits the retry.
- **Bounded pending input.** Session and node bounds are reached separately; an
  async node-slot waiter owns its partial session reservation and returns it on
  cancellation.
- **One active ownership group.** Read clones cannot start another execution;
  removing the ledger retains the shared charge until all read/result owners
  drop.
- **Bytes survive request settlement.** The saved reconnect scenario retains
  queued/writing bytes under the old session after its active request owner
  ends.
- **Exact spending.** A full output queue changes no rate or byte balance;
  successful enqueue spends the actual payload; provisional rollback refunds
  everything.
- **Terminal completion.** The reactor exercises full, partial, and empty
  responses through small queues and verifies their terminal and actual written
  prefix.
- **State lifetime and recovery.** Success, error, panic, timeout, cancellation
  before start, and cancellation while running; the waiting second peer
  proceeds after the read completes.
- **Identity retention.** Creating inactive accounts stays bounded without
  evicting a live request's identity deficit.
- **Replay and checker sensitivity.** Saved JSON replays twice; incompatible
  inputs fail; deliberately faulty observations are rejected and irrelevant
  history shrinks away.

Existing block-sync regression tests remain part of ordinary CI. They
additionally cover stale admission, terminal timeout, queue closure, shutdown,
replacement sessions, pending-input backpressure, and local-pressure
classification. They complement these generated suites; the property suite does
not claim to generate every reactor terminal path.

### What the defaults allow

`serving_regulation/properties/defaults.rs` uses unmodified configuration and
independent numeric expectations. The default response count is one block.
A maximum 2,000,000-byte block uses 2,000,010 payload bytes with its framing
and terminal, and costs 2,065,546 rate units including fixed work.

| Account | Maximum-block witness | What permits the next request |
| --- | --- | --- |
| Peer identity rate | 16 completed responses from a full burst | Refill at 16 MiB/s |
| Node rate | 64 completed responses shared across five identities | Refill at 64 MiB/s |
| Node active work | 64 retained requests after rate refill | Release one active owner |
| Session outstanding bytes | 33 retained full responses | Finish or drop a frame write |
| Node outstanding bytes | 134 retained full responses across five sessions | Finish or drop a frame write |
| Pending inputs | 64 per session and 1,024 across sessions | Release a retained input |

The burst witnesses finish writes and release active work, so another account
cannot explain their rate rejection. The rate tests also check the nanosecond
immediately before sufficient refill and the first instant that permits retry.
The active and byte tests refill between admissions to isolate those bounds.
Failed admissions must leave earlier reservations unchanged.

At these defaults the node active limit is lower than the advertised session
active limit; the latter is exercised separately with smaller configured bounds.
The pending witness starts at retained-input ownership. The peer routine's
separate backpressure tests cover its one decoded input waiting for those slots.

Settlement examples cover no queued output, an empty terminal, a small block,
and a maximum block. All initially reserve the worst case; only fixed work and
transferred payload remain spent after the final query owner drops. Frame bytes
remain outstanding until their transport owner ends. These sizing examples
transfer real charges without allocating bodies; they do not measure sync
throughput, total memory, or transport delivery.

## Reading a failure

Start with the first divergent action and its expected/observed snapshot. There
are three different kinds of account:

- Rate tokens refill with time. Fixed work and successfully queued payload stay
  spent; unused allowance is refunded subject to the burst cap.
- Outstanding bytes do not refill. They belong to the reservation remainder,
  queued frames, or pending application writes.
- Slots belong to provisional admission or a shared active ownership group.
  Multiple references to that group do not multiply its charge.

A cancelled read may still own capacity. A settled request may still have
writing bytes. Therefore final cleanup must end workers, results, and writes
too. Rate balances need not return to full, and the bounded idle identity cache
may remain.

The generated ownership suite prints a concrete JSON scenario on failure and
also uses Proptest's seed persistence. The JSON version fixes its fixture and
model bounds; its actions contain logical slots and session generations. Replay
checks action preconditions independently of the generator's distribution.
Changing that distribution does not reinterpret a saved scenario.

Keep a confirmed minimized scenario next to `writing_after_reconnect.json` and
add a named replay test with the specific intermediate assertion. The existing
JSON is a deterministic boundary witness, not a claim of a historic production
failure. Negative controls alter test observations only; production limits are
never disabled.

The component runner explicitly advances paused Tokio time and compares two
replays. The reactor adapter uses production timers, including its output-queue
poll, and waits for channel acknowledgements and frames under virtual
deadlines. It compares resource and response semantics, not identical task
schedules or wall-clock timestamps. The lifecycle unit tests also exercise
overlapping claims and cancellation on real threads. Either operation may win; no second claim may succeed, and the
remaining lease must retain its resources. These runs do not exhaust thread
schedules. No general scheduler or model-checking engine is involved.

## Adding another regulated message

Write a short resource contract before its property tests:

1. State its role and which work must be preceded by admission.
2. Name the session, identity, node, and transport accounts it uses.
3. Define worst-case reservation, actual spending, refund, and each release
   point.
4. Specify behavior at capacity and distinguish local overload from misconduct.
5. Supply deterministic boundary witnesses and an independent lifecycle model
   against the production boundary that owns those resources.
6. State progress assumptions, unsupported scenarios, and costs outside the
   bound.

Reuse the primitive properties and rational rate oracle. Keep message actions
and production adapters near their message policy. Extract further shared
helpers when a second implementation demonstrates identical behavior; do not
give future messages a GetBlocks-shaped action interface.

Requests need reservation and response ownership tests. Announcements need
cadence and bounded verification/storage effects. Responses need authorization,
credit consumption, and retained-data ownership. A message's tests are
incomplete if the generator can avoid its limit or if only a separate model is
exercised.

## Limits of the evidence

These are finite generated tests, not an exhaustive proof. The suite begins at
resource admission or decoded messages; it does not establish raw-frame
allocation safety, incomplete-frame deadlines, consensus validity, or total
RSS.

Decoded blocks, temporary serialization, a fetched boundary block, and QUIC
data after application handoff are outside this serialized-payload account.
GetBlocks node accounts do not yet cover other message families. Pricing and
production capacity still require representative measurements.

Progress assumes the needed capacity and dependencies become available. An
indefinitely stalled read may keep the active budget occupied. The combined
budgets do not promise strict fairness under continuous competing arrivals.
