# Skip set past bound

The patch forgets the oldest known id only past `KNOWN_CAP`, so a peer that announces more grows the skip set past the bound.

Witness: `crates/zakura-sim/tests/tx_relay_wire.rs::skip_set_within_bound_holds_in_its_witness`.

Required failure: the run ends at `tx-relay/skip-set-within-bound`.
