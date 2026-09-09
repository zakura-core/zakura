# Regulation property tests

The feature PR, #892, contains fixed regression and transport tests. This follow-on
suite explores generated ownership histories and checks them against independent
expectations. Both use the current two-stream design and sequential serving task.

## Running the tests

Ordinary unit-test lanes run these tests once. To select only this suite:

```sh
PROPTEST_CASES=256 PROPTEST_RNG_SEED=0 cargo nextest run --locked --profile regulation-properties
```

The profile selects the generated tests and their model witnesses, with no
retries. `cargo nextest list --locked --profile regulation-properties` shows the
selection. Scheduled and manually dispatched CI also run 2,048 generated cases
with a seed derived from the run ID.

The fixed regressions have a separate `blocksync-regression` profile. Long QUIC
measurements use `blocksync-transport-gate` and run separately, even when routine
CI uses `--run-ignored=all`. See the [transport results](../design/getblocks-refactor-results.md)
for the measurements and limitations.

## Coverage

| Boundary | Generated checks |
| --- | --- |
| Concurrency slots | Capacity follows owned permits through acquisition and release |
| Shared requests | Admission, rollback, execution claims, cancellation, response and frame owners |
| GetBlocks ownership | Peer and node limits, reconnects, encoding, queueing, and pending writes |
| Request decoding | Structured and arbitrary short payloads match independent wire rules |
| Sequential serving | Bursts of requests return complete prefixes and endings at different queue depths; cancellation during a write retains its slot until that write ends |
| Owned state reads | Byte caps, missing blocks, height overflow, and cancellation at generated lookup boundaries |

Serving histories use the same storage and session fixture as the fixed
regressions. They vary request counts, successful and failed reads, queue depth,
and the frame at which a pending write is cancelled. Responses are checked for
block hashes, order, and the ending count. A pending write must prevent the next
request from starting a storage read, including when that request is already
waiting in the input channel.

State properties run the actual bounded-read collector and blocking job. They
check the returned prefix and lookup count independently, then verify that the
result owns its resources until it is dropped. Fixed tests in #892 separately
cover caller abort, dropped waiters, read readiness, and panic unwinding.

The old driver query timeout and request-queue overflow policy were removed by
the refactor. Their properties are replaced by the serving and owned-read checks
above. No test expects a shallow output queue to truncate a valid response.

## Independent model and replay

The GetBlocks model tracks request, query, and frame references across two
identities and multiple session generations. Expected ownership never uses
production counters or release helpers. Every action is compared with production,
including failed admission. Controlled writes cross the same
`QueuedFrame::write_with` boundary as the transport writer.

Failures in the GetBlocks model print a concrete JSON scenario. Version 4 records
owners across reconnects; replay rejects inapplicable actions and older versions.
The committed reconnect scenario retains compatibility with the old `drop_ledger`
action name through its `drop_producer` alias. Negative controls deliberately
remove write ownership or misattribute sessions to check that the comparison
finds those errors and preserves them while shrinking.

Other properties retain Proptest's seed-based reproduction. The saved serving
seed moved with its property. Its original depth-one response is also covered by
the fixed response matrix; the generator now uses the sequential-serving inputs.

The shared-request model uses a test-only GetPeers policy with the production
discovery codec. This checks reuse of admission and ownership primitives without
enabling discovery regulation. These finite histories do not establish exhaustive
concurrency coverage, whole-node overload protection, or sync performance.
