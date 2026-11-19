# Serve unannounced

The patch treats every requested id as announced, so the node serves a pool body to a peer it never told about it.

Witness: `crates/zakura-sim/tests/tx_relay_wire.rs::no_probe_holds_in_its_witness`.

Required failure: the run ends at `tx-relay/no-probe`.
