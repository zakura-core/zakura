# Submit while cooling

The patch drops the cooling check on an arriving body, so a body from a cooling peer reaches the pool.

Witness: `crates/zakura-sim/tests/tx_relay_wire.rs::cooling_bodies_dropped_holds_in_its_witness`.

Required failure: the run ends at `tx-relay/cooling-bodies-dropped`.
