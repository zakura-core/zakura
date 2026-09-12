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

Requester lifecycle histories use the production `ResponseScope` and
`ResponseAuthorization` with an independent exchange model. They vary publication,
first-write admission, valid endings, owner Drop, connection closure, and receiver
replacement. Old writer permissions survive those events so a stale writer must
remain fenced. These histories contain no GetBlocks range or scheduling fields.
The fixed service regressions separately exercise the real GetBlocks request queue,
including a write paused after its first claim and replacement on a new connection.
Discovery response and future subscription adapters remain to be added.

Requester metadata histories share the production node pool across four connection
contexts. An independent model sums live allocation owners after every reserve,
release, and context clone. Fixed tests measure the actual authorization allocation,
check admission before allocation, and retain writer handles after owner Drop.
Production transport tests cover service fanout and session replacement. Receiver
tests hold the pool full for a simulated minute, then release another connection's
owner and require progress without a peer fault. The current production charge
covers the authorization's shared allocation, including inline phase storage.
Cold allocation probes include pool creation, connection context and tokens,
receiver setup, first-use locks, authorization, and unfinished cleanup. They check
the fixed allowances separately and require zero allocations on denied admission.
Setup charges survive through their last owners. Failed receiver setup must leave
the current receiver usable. GetBlocks also reserves an
allocation plan for expected hashes, taken work, and writer state before taking
work. Generated measurements compare retained allocations with that plan for
request sizes 1–128. Fixed examples cover capacity boundaries, a smaller batch
under a constrained pool, and status handles that outlive their writer and
response owner. Retained window capacity is admitted with the request, then keeps
its own permit after the exchange ends. An independent collection model checks
growth, removal, and clearing against the node budget. Allocation probes require
both buffers to be funded during replacement and no allocation for denied growth
or changes within existing capacity. GetBlocks prepares its scratch snapshot,
published height index, and response ranges in the same admission. Probes require
publication to allocate nothing, including while the pool is full. The receiver
keeps capacity charged through body completion and the ending, and a replaced
generation cannot overwrite the new registry entry. Other metadata collections
and transport accounting remain to be audited.

## GetBlocks coverage

| Boundary | Generated checks | Production path |
| --- | --- | --- |
| Wire request | Legal round trips, canonical bytes, tags, flags, exact consumption, count and height bounds, arbitrary short payloads | `GetBlocksPolicy` and `BlockSyncMessage` codecs |
| Admission | Peer and node limits, waiting, FIFO grants, cancellation, reconnects, provisional rollback | Shared request admission from #943 |
| Response ownership | Single execution, cloned leases, response cap, queued frames, pending writes, reconnects | Serving regulator and transport writer |
| Sequential serving | Sequential legal requests, response prefixes, hashes, order, terminal counts, output depths, cancellation during a write | Serving task activated by #945 |
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

## Compliance witnesses

The [GetBlocks compliance suite](getblocks-compliance.md) now executes the missing
requirements from #747, including rules the #942–#945 stack does not yet satisfy.
A failing requirement is an ordinary failing test. It is not ignored, inverted,
or retried until green. The implemented [serving design](../design/getblocks-regulation.md)
remains distinct from the intended requirements being tested.

Receiver tests observe exact requested hashes, consumed parts, and terminal state.
Generated local histories combine response prefixes with deadlines, finality,
reorganizations, and loss of local interest. Wire tests measure actual allocation
requests. Real serving jobs and encoders can pause before returning their results.
QUIC tests require useful work in both directions and independent service progress.
The node composition test uses the real checkpoint verifier before state commit.

The reusable allocation, execution, process-usage, and lock probes live in
`zakura-test`. GetBlocks fields and response expectations stay in its test adapters.
The admission models still run with both GetBlocks and GetPeers. Other messages'
cadence, announcements, subscriptions, and authorization remain separate work.

Keep the fixed database-abort, request-write, encoding, and partial-write tests.
The completed-download fixture now observes consumed endings. Zero remaining body
work alone cannot establish that an exchange finished.

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

The receiver-history test has a five-minute outer limit in this profile because
2,048 cases decode real block fixtures serially. Its per-event waits remain two
seconds. This does not change response deadlines or the generated input bounds.

Ordinary unit-test lanes run the properties once. Select the suite explicitly:

```sh
PROPTEST_CASES=256 PROPTEST_RNG_SEED=0 cargo nextest run --locked \
  -p zakura-network -p zakura-state --lib --profile regulation-properties
```

The profile also selects fixed model witnesses and the new generated compliance
cases, with no retries. Scheduled and manually dispatched workflows run 2,048
cases with the run ID as seed. PR jobs run `regulation-compliance` even after a
unit-test failure, so every compliance family reports its result. Scheduled jobs
also run `regulation-load` with 64 rounds. See the compliance guide for bounds.
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
