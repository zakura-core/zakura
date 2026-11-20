# Skip retry threshold

The patch removes the production QUIC guard. The fixture reaches that guard through the production endpoint and observes its admission or TLS result.

Run on the droplet:

```sh
cargo xtask planted loop --only handshake/quic/retry-before-unvalidated-accept --generate-runs 100 --generate-harness handshake/wire
```

Witness: `crates/zakura-sim/tests/handshake.rs::quic_retry_validates_sources_before_tls`.

Required failure: the run ends at `handshake/quic/retry-before-unvalidated-accept`.
