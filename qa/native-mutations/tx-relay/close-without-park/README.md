# Close without park

The patch closes a stalled peer's connection on its first stall, instead of parking the peer first.

Witness: `crates/zakura-sim/tests/tx_relay_wire.rs::stalled_peers_parked_holds_in_its_witness`.

Required failure: the run ends at `tx-relay/stalled-peers-parked`.
