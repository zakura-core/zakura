# Submit a wrong id

The patch drops the check that a body's id is the id requested, so the node submits a body it never asked for.

Witness: `crates/zakura-sim/tests/tx_relay_wire.rs::requested_bodies_only_holds_in_its_witness`.

Required failure: the run ends at `tx-relay/requested-bodies-only`.
