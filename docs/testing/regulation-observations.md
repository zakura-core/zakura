# Regulation test observations

These helpers let tests check how much memory the code requests, how much work
is still running, and what that work costs in CPU time and lock delays. They
measure the work separately from the node's own capacity counters, so an
incorrect counter cannot make a test pass by agreeing with itself.

The helpers live in `zakura-test` and can be used for any message. Each message's
tests supply its inputs, expected responses and workload limits.

## Allocations

`allocations.rs` checks how much memory a function requests while a test runs it.
It reports the largest request, the most memory held at once, and the memory
still held when the function returns.

**Example:** Allocating an 8 KiB temporary buffer and a retained 1 KiB result
records a 9 KiB peak and 1 KiB retained at return. Counting only the final buffer
would miss the temporary allocation.

To use it, install `allocations::TrackingAllocator` as the test program's global
allocator, the component that handles memory requests. Rust's system allocator
still allocates and frees the memory. Then call `allocations::measure` around the
function being tested. Only requests on that thread during that call count.

Set up test input and shared fixtures before measuring. Do not await or move
measured allocations across threads. The helper excludes memory used for its
own records. Calling `measure` inside another `measure` panics, and a panic stops
recording. These numbers describe memory requests by the measured function,
not the whole program's RAM usage.

## Blocking work

`execution.rs` helps check that unfinished work still counts against the node's
work limit. For example, a database read can keep running after its caller stops
waiting. A test can pause that read, cancel its caller, and check that its
capacity remains reserved until the read finishes.

Call `execution::ExecutionProbe::start` inside the blocking job. Keep the returned
guard inside the job until it ends. The helper counts the job as running until
that guard is dropped, including when the job fails or panics. Reserving capacity
or placing a job in a queue does not count as a start.

The probe can pause at entry or before returning a computed result. Hold a
`release_on_drop` guard in the test so failed assertions also release blocked
jobs. Waits have finite deadlines. The instrument's controls check both pause
points against real blocking tasks and check cleanup after panic.

## Process and lock observations

`resources.rs` measures CPU and memory use for the test program, and wait and hold
times for a lock being tested. `resources::ProcessUsage` reads CPU time and peak
RAM usage from the operating system on supported Unix targets. These totals
include other work in the same program. The memory peak does not fall when
memory is freed.

Capture the time before trying to acquire the lock, then call
`resources::LockProbe::acquired_since` immediately after acquiring it. Drop the
returned `resources::LockHold` immediately before releasing the lock. These
timings include delays from thread scheduling and the measurement itself. They
describe the tested workload, rather than a timing limit that holds on any machine.
Hold timing begins when `acquired_since` is called, so waiting for the probe's own
lock is included while the measured lock remains held.

`resources::load_rounds` defaults to four rounds only when
`ZAKURA_REGULATION_LOAD_ROUNDS` is absent. A valid integer override is clamped
to 1–256.
Malformed integers and non-Unicode values fail the test with the variable name,
value and reason.

## Running the controls

```sh
cargo test --locked -p zakura-test --lib
```

Run the controls locally while reviewing the split. Each later contract PR adds
its own tests and a focused local selection.
