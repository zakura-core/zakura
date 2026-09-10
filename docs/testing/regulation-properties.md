# Message regulation properties

This suite extends the fixed regressions in the GetBlocks stack through #945.
It follows the capacity and ownership approach in the [regulation draft][regulation]
and its [testing design][testing-design]. Generated cases search for counterexamples.
They do not prove every execution correct or qualify the transport for activation.

## Shared contracts

Finite requests use the same production admission code. Their tests should share
the same ownership checks too. `check_request_owners` and `check_admission_waiters`
accept a production request policy and an encoded request. Both run with GetBlocks
and a test-only GetPeers adapter using the production discovery codec. This does
not enable discovery regulation in the node.

**Example:** A owns the node's last worker slot. B and C are waiting. When A
finishes, B owns the released slot even before B's future runs again. If B cancels
at that point, C must receive it. Reconnecting B must not create another slot.
The generated FIFO model checks those transitions for both message adapters.

The shared ownership model checks provisional rollback, a single execution
claim, cloned work leases, producer cancellation, and retained frames. It checks
the total response cap independently of the production byte counter. Completing
a frame releases that frame's ownership but does not reset the response's total
byte allowance. Pending admission has its own independent FIFO model, including
partial peer claims and cancellation after capacity is granted but before polling.

Models select expectations without production counters. Tests compare the real
peer and node counters after every action, then drop all owners and check cleanup.
The GetBlocks adapter additionally exercises the real serving regulator, response
encoder, bounded output queue, and `QueuedFrame::write_with` boundary. Its session
model retains the original owner across reconnects. Observation handles are weak
and do not keep work alive.

## GetBlocks coverage

| Boundary | Generated checks | Production path |
| --- | --- | --- |
| Wire request | Legal round trips, canonical bytes, tags, flags, exact consumption, count and height bounds, arbitrary short payloads | `GetBlocksPolicy` and `BlockSyncMessage` codecs |
| Admission | Peer and node limits, waiting, FIFO grants, cancellation, reconnects, provisional rollback | Shared request admission from #943 |
| Response ownership | Single execution, cloned leases, response cap, queued frames, pending writes, reconnects | Serving regulator and transport writer |
| Sequential serving | Request bursts, response prefixes, hashes, order, terminal counts, output depths, cancellation during a write | Serving task activated by #945 |
| Storage | Contiguous prefixes, missing blocks, byte caps, integer overflow, cancellation between lookups, retained results | Bounded collector and owned blocking read from #942 |

Required boundaries also have fixed examples, so random sampling does not decide
whether they run. The writer mutation test deliberately drops the actual frame
guard before I/O finishes. The normal model comparison must reject that execution
and still reject it after irrelevant time advances are shrunk away.

The serving cancellation property checks that a pending write retains capacity.
After cancelling and dropping that write, it waits for replacement admission.
A separate encode can still be running or holding an undelivered result, so an
immediate zero-capacity assertion would contradict the ownership contract. The
concrete input from the old failing CI run remains a fixed regression.

## Coverage boundaries

The [serving design](../design/getblocks-regulation.md) is the implemented
contract. The broader regulation draft still proposes additional behavior.

| Area | Current evidence and next step |
| --- | --- |
| Outgoing request publication and expiry | #944 retains fixed production tests for queued versus started writes, partial expiry, resets, received bodies, and replacement owners. Generated scheduler and response histories are a separate extension. |
| Response authorization | This suite does not claim the draft's full authorization lifetime, overlapping-range rejection, duplicate-terminal rejection, or terminal correlation. The serving design explicitly leaves overlapping live ranges to existing reassignment and late-response rules. Add receiver properties when that contract is implemented. |
| Frame allocation | Fixed transport tests reject the oversized GetBlocks header before reading its payload. Decode properties start with a bounded buffer and make no allocation measurement claim. |
| QUIC and node load | Existing real-transport tests and the separate transport gate cover the exercised buffering and progress workloads. In-memory histories do not establish connection-credit headroom, process memory, Sybil fairness, or throughput. |
| Other messages | GetPeers demonstrates reuse of finite-request admission. Discovery cadence, announcements, header subscriptions, and their authorization remain separate implementations and properties. |

Keep the fixed database-abort, encoding, partial-write, and completed-download
tests. Generated models supplement those tests. They do not replace integration
coverage or permit enabling the paired transport before its qualification gate.

## Adding a message

1. Write its input, allocation, authorization, and progress rules against its
   protocol specification. Keep proposed behavior distinct from implemented rules.
2. Add deterministic legal and invalid boundaries plus bounded payload generation
   through its production codec. Check semantic round trips and canonical bytes.
3. For a finite request using shared admission, invoke both shared ownership
   checkers with its policy and a legal encoded frame. Do not copy the models.
4. Exercise its real handler and response path with message-specific inputs and
   expected results. Add a small independent state model only where useful.
5. Keep cadence and subscription properties beside their owning production code.
   Announcements and subscriptions do not inherit finite-request lifetimes.
6. Add the suite to the property profile and preserve a concrete regression for
   each counterexample. Identify transport claims that need a real QUIC test.

This needs no production declaration framework, custom scheduler, or exhaustive
explorer. Share a checker when messages have the same contract. Keep protocol
fields and expected response semantics in their own adapters.

## Running and replaying

Ordinary unit-test lanes run the properties once. Select the suite explicitly:

```sh
PROPTEST_CASES=256 PROPTEST_RNG_SEED=0 cargo nextest run --locked \
  -p zakura-network -p zakura-state --lib --profile regulation-properties
```

The profile also selects fixed model witnesses, with no retries. Scheduled and
manually dispatched unit-test workflows run 2,048 cases with the run ID as seed.
`blocksync-regression` selects fixed checks separately. Long real QUIC measurements
use `blocksync-transport-gate`, whose conditions are in the serving design.

GetBlocks ownership failures print a concrete JSON scenario. Schema version 4
records session owners and rejects inapplicable actions and unknown versions.
`drop_ledger` remains an alias for the old fixture's `drop_producer` action.
To replay a saved scenario against production and the independent model:

```sh
ZAKURA_REGULATION_REPLAY=/absolute/path/scenario.json cargo test --locked \
  -p zakura-network replay_preserves_writing_ownership_across_session_replacement
```

Replay checks JSON round trips, repeated observations, and final cleanup. Other
properties use Proptest's printed seed and persisted regression inputs. Keep the
tested revision, toolchain, case count, seed, and any failing scenario with the
validation results. Do not silently retry a failing generated case.

[regulation]: https://github.com/zakura-core/zakura/blob/d4f2fad598294d391b475f9c22e63352557ba5ff/docs/specs/peer-message-regulation.md
[testing-design]: https://github.com/zakura-core/zakura/blob/d4f2fad598294d391b475f9c22e63352557ba5ff/docs/design/property-testing.md
