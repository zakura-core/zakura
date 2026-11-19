# Pending past bound

The patch gives each pending set one id of room past `PENDING_PER_PEER`, so a peer that announces more fills it past the bound.

Witness: `crates/zakura-sim/tests/tx_relay_wire.rs::pending_within_bound_holds_in_its_witness`.

Required failure: the run ends at `tx-relay/pending-within-bound`.
