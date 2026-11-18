# Accept wrong dialed key

The expected-key policy has two enforcement sites: the raw-public-key verifier and the post-TLS identity check. This patch bypasses both. Removing only one leaves the other enforcing the same policy; neither single-site survivor establishes a missing end-to-end guard.

Witness: `crates/zakura-sim/tests/handshake.rs::quic_rejects_a_key_different_from_the_dialed_identity`.

Required failure: the run ends at `handshake/quic/identity-required`.
