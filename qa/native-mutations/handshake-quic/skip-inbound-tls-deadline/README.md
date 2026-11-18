# Skip inbound tls deadline

The patch removes the production QUIC guard. The fixture reaches that guard through the production endpoint and observes its admission or TLS result.

Run on the droplet:

```sh
cargo xtask planted loop --only handshake/quic/finishes-in-time --generate-runs 100 --generate-harness handshake/wire
```

Witness: `crates/zakura-sim/tests/handshake.rs::quic_deadline_ends_a_stalled_tls_attempt`.

Required failure: the run ends at `handshake/quic/finishes-in-time`.
