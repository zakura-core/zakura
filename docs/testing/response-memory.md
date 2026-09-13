# Shared response metadata budgets

Response metadata is the bookkeeping retained while a peer may still respond.
It needs a shared limit so opening another service or replacing a receiver does
not give that connection a fresh allowance.

## Ownership

Each endpoint creates one pool with a 128 MiB node limit and a 16 MiB limit per
connection. Services and replacement receivers clone the same connection handle.
Every reservation must fit both limits. A failed reservation releases anything
it temporarily charged and leaves local work pending.

For example, two services using 40 bytes each share an 80-byte connection
allowance. A replacement service cannot reserve another byte until one of those
owners releases memory. This example leaves fixed setup costs out of the numbers.
Ending a response or closing a connection does not release memory that a writer
still holds.

Node, connection and receiver setup reserve named fixed allowances before
allocation. They cover accounting handles, counters and platform locks that may
allocate on first use. Cold allocation tests check real peaks against those
allowances. Each authorization record is charged from its allocation layout,
including the shared reference counts and alignment padding.

Setup must succeed before retiring an existing receiver or replacing a registered
connection. This keeps a failed local admission from disrupting working traffic.

## Properties

| Test group | Contract |
| --- | --- |
| `response_memory::tests` | Both limits, failed reservation rollback, integer boundaries, racing connections, final-owner release and capacity wakeups. |
| `response_properties::memory` | Generated histories independently sum live allocations across four connections and sixteen owners. Cold allocation observations include setup, authorization, transitions and cleanup. Denied admissions allocate nothing. |
| `transport::registry::tests::response_memory_survives_service_fanout_and_escalation` | Services and later stream activation share the original connection and node pools. |
| `service::tests::replacement_budget` | A replacement denied setup memory leaves its predecessor active and able to make requests. |
| `peer_routine::tests::memory` | Exhaustion leaves work pending without a peer fault. Another connection's release wakes the receiver. An idle receiver does not spin on its own releases. |

The same response lifecycle, discovery and subscription properties run against
the funded scopes. The `response-memory` nextest profile selects these checks
locally without retries or a workflow trigger.

## Request buffers

This layer funds shared setup and authorization records. The
[allocation-planning layer](request-allocation-planning.md) adds expected hashes,
request writers and retained collection capacity, together with the shared
funded-vector helper and its growth properties. Neither layer claims to bound
total process memory.
