# Regulation property tests

These tests check who owns admitted work and when capacity returns. Generated
histories compare production admission, encoding, queues, and write ownership
against an independent model. Controlled tests cover the sequential serving task,
atomic outgoing-request publication, and the state API that owns blocking reads.

## Running the tests

The ordinary unit-test lanes include this coverage. To isolate it:

```sh
cargo nextest list --locked --profile regulation-properties
cargo nextest run --locked --profile regulation-properties
```

The profile disables retries. Increase `PROPTEST_CASES` for more generated
histories. Failures print a concrete JSON scenario and preserve Proptest's normal
seed-based reproduction. Focused network runs can select `serving_regulation`,
`regulation::properties`, or `regulation::request::properties` with `cargo test`.

The long transport measurements run separately, including when routine CI uses
`--run-ignored=all`:

```sh
cargo nextest run --locked --profile blocksync-transport-gate --run-ignored=all
```

This profile runs serially without retries. Twenty impaired reopen rounds can
take over an hour. A passing diagnostic that reproduces a stall is not a passed
activation gate. The required measurements, comparisons, and known failures are
recorded in [GetBlocks refactor execution](../design/getblocks-refactor-results.md).

## Coverage

| Boundary | Production behavior checked |
| --- | --- |
| Concurrency slots | Capacity follows owned permits, including failed admission |
| Shared finite requests | Provisional admission, rollback, execution claims, response and frame owners |
| GetBlocks permits | One active response per identity across reconnects; node limits; response byte caps |
| Sequential serving | Request before Status, queue backpressure, complete response endings, and caller abort |
| Outgoing requests | Publication, writer claim, expiry, enqueue failure, and reset settle exact ownership |
| Session pair | Bounded setup and retirement, role selection, cancellation, and independent response progress |
| State reads | Readiness, bounded returned bytes, cancellation, caller abort, and panic unwinding |

The ownership model tracks request, query, and frame references independently.
It never uses production counters or release helpers to calculate expected state.
Observations are compared after every action, including admission failure.
Dequeueing a frame must not release its permit: controlled writes use the same
`QueuedFrame::write_with` boundary as the transport writer.

Frame-header tests enforce message-specific payload limits before reading or
allocating payloads. Paired roles reject messages on the wrong stream. A decoder
property compares structured and arbitrary short payloads against independent
wire rules, checking both valid acceptance and invalid rejection.

## Generated histories

The GetBlocks model uses two identities, up to eight session generations, four
request slots, and a one-frame output queue per session. Actions cover admission,
query claim and cloning, producer closure, encoding, queueing, writes,
cancellation, reconnects, and time advancement. Only model-enabled actions are
generated. Replay rejects an inapplicable action instead of silently skipping it.

Deterministic witnesses check that:

- Peer and node limits block further work, then recover when the last owner ends.
- Cloned query leases cannot start a second read; cancelled work cannot start.
- Queued and partly written responses retain capacity after the producer drops.
- Old reads and frames remain charged across reconnects, while another identity
can use an available node slot.
- Response costs match independent wire arithmetic.
- Missing write ownership, wrong-session attribution, and duplicate releases are
detected. Shrinking retains the concrete ownership error.

The default permits one block per response and 64 active responses per node.
One maximum block plus its discriminator and ending uses 2,000,010 payload bytes.
That is a response bound, not an outstanding-byte budget. Time alone never frees
owned capacity. A witness completes 4,096 responses without advancing time to
check that admission does not depend on a refill timer.

Shared-request histories use a test-only GetPeers policy with the production
discovery codec. They exercise provisional, execution, response, and frame owners,
including FIFO admission and cancellation of partial peer claims. They poll the
production async admission future; there is no separate admission algorithm. This does not enable discovery
regulation or establish its full protocol conformance.

## Replay and reuse

JSON version 4 records one active response per identity, including old work that
survives reconnects. Earlier versions describe a different admission contract and
are rejected. The committed reconnect replay attributes each owner to its original
session, so matching node totals cannot conceal a session error. The old
`drop_ledger` action name remains accepted as an alias for `drop_producer`.

For another message family, define its work unit and every owner that can outlive
the handler. Reuse the shared request primitives where applicable, then exercise
the actual codec, handler, and transport against independent expectations.
Announcements and subscriptions need their own lifetime rules.

These are finite generated histories, not exhaustive state exploration. They do
not establish whole-node overload protection, RocksDB capacity, or full sync
performance. The separate [transport gate](../design/getblocks-refactor-results.md)
passed before activation.
