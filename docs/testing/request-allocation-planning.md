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

GetBlocks plans its writer, expected hashes, outstanding requests, response lookup
indexes and both sides of the registry snapshot together. Vector costs come from element sizes and allocation
layouts. Publication swaps already funded buffers and does not allocate. An old
receiver generation cannot replace the current receiver's snapshot.

`ResponseIndex` keeps a conservative allowance for its tree nodes. Rust does not
expose the tree's capacity, so the allowance covers a full node per admitted key
and one spare node. It includes keys, child links and node bookkeeping, based on
the [standard library layout](https://doc.rust-lang.org/src/alloc/collections/btree/node.rs.html).
It stays charged through key replacement, endings and an empty tree, until the
index drops. This can stop request admission before the request-count ceiling.
Cold allocation properties must be rerun when changing the Rust toolchain.

A blocked receiver waits for the entire smallest request plan, including both
indexes. Having room for the authorization record alone must not repeatedly wake
a receiver whose buffers still cannot fit.

## Properties

| Test group | Contract |
| --- | --- |
| `response_properties::collection` | Generated insert, remove and clear histories independently track capacity, retained bytes and growth peaks. Real allocations must match the funded plan. |
| `peer_routine::response_contract::metadata` | Generated requests and capacity boundaries fund real retained allocations before work, during publication and after the ending. Denied snapshot growth allocates nothing and preserves published work. |
| `peer_routine::tests::memory` | Smaller batches fit tight budgets. Complete denial preserves pending work. Partial releases cannot wake a retry until the whole request plan fits. Registry growth retains funding and replacement fences old publishers. |
| `response_index::tests` | Full-window and generated tree changes measure actual growth, key replacement and removal peaks against funding. Denied growth allocates nothing, preserves keys and returns the allowance only when the index drops. |
| `response_vec::tests` and `response_memory::tests` | Permit splitting, old/new allocation overlap, exact growth, denial rollback and final-owner cleanup. |
| `work_queue::request_write::tests` | Writer status handles retain metadata funding after the response owner or writer exits. |

The local `request-allocation-planning` nextest profile selects these properties
and the affected receiver, registry and service regressions. It adds no workflow
trigger. These charges cover response bookkeeping, separately from body storage,
decoding and execution. They do not establish a bound on total process memory.
