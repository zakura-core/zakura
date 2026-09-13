# Request allocation planning

A request needs storage for its expected hashes, writer state and published
height index. Reserve that storage before taking work from the queue so a local
memory shortage cannot leave a half-published request.

For example, a request for 128 blocks may exceed the remaining metadata budget.
The receiver first tries tighter buffer growth, then smaller batches. If even one
block cannot fit, the work stays pending and resumes when capacity is released.
This does not blame or disconnect the peer.

## Retained storage

`ResponseVec` keeps a memory permit with a collection's backing allocation.
Removing its last item does not release that permit because the empty collection
still owns its capacity. Growth reserves the replacement buffer before allocating
it. Both buffers remain funded until the entries have moved and the old buffer
is freed. The helper is shared across messages.

GetBlocks plans its writer, expected hashes, outstanding requests and both sides
of the registry snapshot together. Costs come from element sizes and allocation
layouts. Publication swaps already funded buffers and does not allocate. An old
receiver generation cannot replace the current receiver's snapshot.

## Properties

| Test group | Contract |
| --- | --- |
| `response_properties::collection` | Generated insert, remove and clear histories independently track capacity, retained bytes and growth peaks. Real allocations must match the funded plan. |
| `peer_routine::response_contract::metadata` | Generated requests and capacity boundaries fund real retained allocations before work, during publication and after the ending. Denied snapshot growth allocates nothing and preserves published work. |
| `peer_routine::tests::memory` | Smaller batches fit tight budgets. Complete denial preserves pending work. Registry growth retains funding and replacement fences old publishers. |
| `response_vec::tests` and `response_memory::tests` | Permit splitting, old/new allocation overlap, exact growth, denial rollback and final-owner cleanup. |
| `work_queue::request_write::tests` | Writer status handles retain metadata funding after the response owner or writer exits. |

The local `request-allocation-planning` nextest profile selects these properties
and the affected receiver, registry and service regressions. It adds no workflow
trigger. These charges cover response bookkeeping, separately from body storage,
decoding and execution. They do not establish a bound on total process memory.
