# Shared response metadata

`ResponseMemory` bounds retained bookkeeping against node and connection byte
limits. Every service and replacement session on a connection must receive a
clone of the same `ConnectionResponseMemory`. Creating a fresh connection handle
for each service would give each one a separate connection allowance.

This layer provides the opt-in accounting tools. It does not yet distribute the
handles through production connection admission or activate a message adapter.
Existing unfunded `WriterFence::new` callers retain their current behavior.

## Ownership

A pool reserves its own setup before allocation. `try_connection` funds connection
setup, and `WriterFence::try_with_memory` funds the shared fence before creating
it. Construct a replacement successfully before retiring the current receiver.
The defaults are 128 MiB per node and 16 MiB per connection. Callers can supply
explicit limits with `ResponseMemory::new`.

`try_open` funds the exchange allocation. `try_open_with_retained_memory` admits
that allocation, caller metadata and container growth as one reservation. Its
separate permit follows the container. Metadata attached to the exchange stays
charged until the last `ExchangeWriter` drops, even after a validated ending or
receiver replacement. Memory refusal is local backpressure, not a peer violation.
`try_open` distinguishes that refusal from retirement. `open` returns `None` for
either condition and remains available to callers that do not need the distinction.

For example, two services using 40 bytes each exhaust an 80-byte connection
allowance. Replacing either service does not free the bytes still owned by its
old writer. This example omits fixed setup charges. Only releasing the old
allocation makes that capacity available again.

Fixed setup allowances cover accounting handles and first-use platform locks.
Cold allocation tests measure those peaks. Exchanges are charged by their shared
allocation layout, including reference counts and padding, plus a fixed allowance
for the phase mutex's platform storage. Heap allocations
inside caller metadata still need their own explicit charges.

## Waiting and validation

A connection at its own limit waits only for its owners to release bytes. Once
it has room, it can wait for node capacity released by another connection.
Waiters register before rechecking capacity to avoid missed wakeups. Waking does
not reserve space. The caller must retry admission and cancel the wait when its
session retires. A request larger than the configured limit cannot become ready.

The `response_memory` tests cover both limits, concurrent reservations, integer
boundaries, failed admission without allocation, independently retained writers
and containers, and eventual cleanup. Wakeup tests cover partial releases and
switches between node and connection pressure. Generated histories independently
sum retained allocations across four connections. Existing writer-fence tests
continue to exercise publication, retirement and first-byte ordering.

This accounting does not bound block bodies, decoded data, execution slots,
transport buffers or total process memory. Those resources retain separate limits.
