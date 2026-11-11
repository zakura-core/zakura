# Accept unsupported alpn

The patch adds the hostile peer's unsupported ALPN to the production acceptor policy. The property must reject TLS success before control validation can mask the policy error.

Witness: `crates/zakura-sim/tests/handshake.rs::quic_rejects_an_unsupported_alpn_before_control`.

Required failure: the run ends at `handshake/quic/alpn-required`.
