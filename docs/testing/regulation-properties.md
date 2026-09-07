# Regulation property tests

These tests check who owns admitted work and when capacity returns. They exercise
production admission, response encoding, queueing, and write ownership against an
independent model. They do not establish complete conformance to the peer-message
regulation draft or whole-node overload protection.

## Running the tests

The ordinary unit-test lanes include these tests. The dedicated profile allows
longer local exploration without changing production limits:

```sh
cargo nextest run --locked --profile regulation-properties
```

For a focused network run:

```sh
cargo test --locked -p zakura-network --lib serving_regulation
cargo test --locked -p zakura-network --lib regulation_properties
cargo test --locked -p zakura-network --lib regulation::properties
```

Increase `PROPTEST_CASES` for additional generated histories. Failures print a
concrete JSON scenario and retain Proptest's normal seed-based reproduction.

## Test boundaries

| Layer | Production path exercised | Observation |
| --- | --- | --- |
| Primitive | Concurrency slots | Owned permit count |
| Serving | Admission, query lifecycle, encoder, transport queue | Node and session producer/pending counts |
| Reactor | Peer routine and response queue | Response prefix, terminal, producer lifetime |
| Driver | Query claim, timeout, cancellation, result handoff | Underlying query and result retain capacity |
| State | Range response limits | Actual returned body bytes stay within the cap |

The model tracks ledger, query, and frame owners independently. It does not use
production counters or release helpers to calculate expected state. Production
observations are compared after every action, including failed admission.

Transport witnesses use the same `QueuedFrame::write_with` boundary as the real
writer. Dequeue alone must not release a producer. Controlled writes exercise
completion, failure, and cancellation. The base PR also tests a real QUIC write
blocked by stream credit while another stream makes progress.

## Generated histories

The ownership model uses two peer identities, at most eight session generations,
four request slots, four input slots, and a one-frame output queue per session.
Requests contain one committed mainnet block fixture or an empty terminal. The
independently calculated maximum response payload is 2,000,010 bytes; this is a
wire bound, not an outstanding-byte budget.

Actions cover provisional admission, commit, query claim and cloning, ledger
closure, frame queueing, pending writes, write completion/failure/cancellation,
pending input, reconnects, and time advancement. Only reference-model-enabled
actions are generated. A concrete replay rejects an inapplicable action instead
of silently skipping it. Cleanup uses the same actions to finish every owner.

Deterministic witnesses ensure these boundaries are reached independently of
random coverage:

- **Admission and rollback:** both session and node producer limits block, then
  recover when ownership ends. Failed admission returns any earlier slot.
- **Pending input:** session and node limits block independently, including the
  partial session reservation held while waiting for a node slot.
- **One execution:** cloned query leases cannot claim a second state read.
  Ledger closure prevents a queued read from starting.
- **Write backpressure:** a queued or writing response keeps its session producer
  occupied after the ledger and query owners drop. Failed queue admission changes
  no ownership and can be retried after capacity becomes available.
- **Reconnects:** old query and frame owners remain counted under the old session.
  Replacing the session does not release their node capacity.
- **Response boundaries:** separate properties vary legal counts and response
  caps; real reactor histories cover empty, partial, and complete responses with
  queues of depth one through three.
- **Checker sensitivity:** deliberately missing write ownership, wrong-session
  attribution, and duplicate releases fail comparison. Shrinking removes
  irrelevant time advances while preserving a concrete failing observation.

The real reactor's existing timeout, shutdown, full-queue, stale-session, and
same-stream download tests complement the generated histories. The generator
is not claimed to cover every reactor terminal path.

## Production defaults

The default advertisement permits one block per response. One maximum-size body
plus its discriminator and terminal is 2,000,010 payload bytes. There is no fixed
request price, serving byte-rate allowance, or outstanding-byte balance.

| Boundary | Default witness | Recovery |
| --- | --- | --- |
| Session producer | One query/result/response shared by all its owners | Last owner finishes |
| Node producers | 64 sessions retaining responses | One producer finishes |
| Pending inputs | 64 per session, 1,024 across sessions | One retained input releases its slots |

Time advancement never frees owned capacity. A separate witness completes 4,096
responses without advancing time, proving admission does not wait for a refill.
These tests exercise resource counts without allocating maximum-size block bodies;
real response encoding and write tests cover the framing boundary separately.

## Replay and reuse

JSON version 2 records the producer-ownership contract. Version 1 described the
removed byte budgets and is rejected. The committed reconnect scenario is a
human-readable example. Replay observations include per-session attribution, so
matching aggregate totals cannot hide an ownership error.

For another message family:

1. Declare its actual rules and work unit; do not assume it needs byte accounting.
2. Identify admission and every owner that can outlive the handler, including
   state work, results, and transport writes where applicable.
3. Write an independent ownership model and concrete replay actions.
4. Add deterministic witnesses for each limit, release path, and legal retry.
5. Exercise the real handler, encoder, and transport boundaries against those
   expectations, including cancellation and session replacement.

Reuse the slot properties where the contract is identical. Extract additional
helpers only when a second message demonstrates common behavior. Keep message
semantics and fixtures explicit so a reviewer can understand what is tested.

These are finite generated histories, not exhaustive state exploration. They do
not measure RocksDB capacity, consensus scheduling, or full sync performance.
The local real-Iroh comparison and its limitations are documented in
[GetBlocks serving regulation](../design/getblocks-regulation.md).
