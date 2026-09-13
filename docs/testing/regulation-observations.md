# Regulation test observations

`zakura-test` provides shared instruments for message regulation tests. They
observe operations independently of the production capacity counters. Message
adapters provide the payloads, expected responses and workload limits.

## Allocations

Install `allocations::TrackingAllocator` as the test binary's global allocator,
then call `allocations::measure` around a synchronous operation. The wrapper
delegates to the system allocator. Observation is opt-in and local to one thread.

**Example:** Allocating an 8 KiB temporary buffer and a retained 1 KiB result
records a 9 KiB peak and 1 KiB retained at return. Counting only the final buffer
would miss the temporary allocation.

Initialize lazy fixtures before measuring. Do not await or move measured
allocations across threads. Bookkeeping and other threads are excluded. Nested
measurement is rejected, and a panic clears the active observation. Report
requested sizes, transient peaks and retained output separately from process RSS.

## Blocking work

Call `execution::ExecutionProbe::start` inside the actual blocking closure.
Its guard counts running work until completion or Drop, including errors and
panic unwinding. Reserved permits and queued futures do not count as starts.

The probe can pause at entry or before returning a computed result. Hold a
`release_on_drop` guard in the test so failed assertions also release blocked
jobs. Waits have finite deadlines. The instrument's controls check both pause
points against real blocking tasks and check cleanup after panic.

## Process and lock observations

`resources::ProcessUsage` reports process CPU time and peak RSS on supported
Unix targets. These totals include concurrent activity in the same process and
do not measure one operation's retained allocations.

Create a `resources::LockHold` immediately after acquiring the actual mutex.
Drop it immediately before the mutex guard. Acquisition time includes scheduling
and observation overhead. Use it to describe the declared workload, not as a
universal timing limit.

`resources::load_rounds` defaults to four rounds. The
`ZAKURA_REGULATION_LOAD_ROUNDS` override is clamped to 1–256.

## Running the controls

```sh
cargo test --locked -p zakura-test --lib
```

Run the controls locally while reviewing the split. Each later contract PR adds
its own tests and a focused local selection.
