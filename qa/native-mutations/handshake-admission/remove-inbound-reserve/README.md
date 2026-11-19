# Remove inbound reserve

The shared inbound-share calculation grants inbound work the entire handshake and connection budgets. The patch changes the production decisions and semaphore capacities together.

Run on the droplet with both generator harnesses from the work list:

```sh
cargo xtask planted loop --only handshake/pending-bounded --only handshake/dial-reserve-kept --generate-runs 100
```

Witness: `crates/zakura-sim/tests/handshake.rs::inbound_control_pressure_preserves_a_wire_dial`.

Required failure: the run ends at `handshake/pending-bounded`.

Witness: `crates/zakura-sim/tests/handshake.rs::inbound_connection_pressure_preserves_a_wire_dial`.

Required failure: the run ends at `handshake/dial-reserve-kept`.

The pending-handshake witness can also violate `handshake/dial-reserve-kept` at the same boundary. The connection witness keeps pending work below its cap.
