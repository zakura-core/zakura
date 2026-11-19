# Requests past limit

The patch lets a session claim one id past `MAX_REQUESTS_PER_PEER`, so a slow pool holds six of one peer's ids in flight.

Witness: `crates/zakura-sim/tests/tx_relay_wire.rs::requests_within_limit_holds_in_its_witness`.

Required failure: the run ends at `tx-relay/requests-within-limit`.
