# Serving past limit

The patch refuses a peer's request only past `MAX_SERVES + 1` unstarted answers, so the node holds one request more than its `Hello` serves.

Witness: `crates/zakura-sim/tests/tx_relay_wire.rs::serving_within_limit_holds_in_its_witness`.

Required failure: the run ends at `tx-relay/serving-within-limit`.
