# Ignore transport reservations

The outbound control claim ignores pending inbound TLS reservations.

The wire witness fills the combined budget with delayed TLS and outbound control work.
The next outbound claim exceeds the pending limit when this mutation removes the check.

Witness: `crates/zakura-sim/tests/handshake.rs::pending_transport_load_bounds_outbound_control_handshakes`.
Required failure: the run ends at `handshake/pending-bounded`.
