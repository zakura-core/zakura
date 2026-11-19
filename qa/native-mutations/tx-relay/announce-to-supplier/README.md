# Announce to supplier

The patch drops the skip set from the announcement filter, so the node announces a peer's own bodies back to it.

Witness: `crates/zakura-sim/tests/tx_relay_wire.rs::announce_after_admission_holds_in_its_witness`.

Required failure: the run ends at `tx-relay/announce-after-admission`.
