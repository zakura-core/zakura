# Ask before hello

The patch takes a peer's announcements into the wanted table before its `Hello`, and claims them with a default `Hello`, so the node asks a peer that never said what it serves.

Witness: `crates/zakura-sim/tests/tx_relay_wire.rs::hello_first_holds_in_its_witness`.

Required failure: the run ends at `tx-relay/hello-first`.
