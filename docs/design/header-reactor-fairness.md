# Header reactor fairness

## Lifecycle admission

Header-service lifecycle events use a bounded queue. Each admitted connection reserves room for both its connection notification and its eventual teardown. Admission is keyed by peer and session generation. A stale teardown cannot remove the replacement session, and teardown still releases the old reservation after service replacement. The service waits for the reactor's admission acknowledgement before forwarding that session's messages.

The queue holds twice the configured header-service peer capacity. It is independent of the bounded wire-event queue. A rejected lifecycle admission cancels that session instead of adding another unbounded waiter or notification.

## Watch progress

Each reactor turn checks committed-state and VCT-repair watches before consuming at most one lifecycle event and one wire event. This lets state changes wake a resource-stalled reactor even when Status traffic and connection churn keep both queues ready. Existing completion and maintenance handling remains in the select loop. Turns that process queued events yield to the executor.

## Repair supplier history

Supplier history is pruned when membership changes rather than on every Status-triggered assignment attempt. Assignment still uses current connected peers, and prior attempts remain available for retry rotation. Status processing no longer allocates a set containing every connected peer just to prune unchanged membership.

## Validation

Tests exercise a real reactor with saturated Status and lifecycle queues, resource-stalled target resumption, repair-watch changes, and service replacement and teardown across hundreds of generations. A benchmark-style regression measures all active repair phases at 16, 92, and 512 peers.

An explicit QUIC load test drives the production handler and service pipe at the default 92-connection node cap, with 28 outbound peers and 64 inbound peers. The per-IP cap is raised only because every simulated peer uses loopback. It checks watch observation and a separate honest peer's locator request while 63 peers flood Status messages. This is a short local load check, not a sustained production soak or a network throughput forecast.
