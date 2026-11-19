# Wrong size row

The patch closes a wrong-size body under row 11 (`relay-close-11`), the wrong-id row, which the peer never broke.

Witness: `crates/zakura-sim/tests/tx_relay_wire.rs::fault_close_names_a_broken_row_holds_in_its_witness`.

Required failure: the run ends at `tx-relay/fault-close-names-a-broken-row`.
