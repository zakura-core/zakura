# Skip authenticated identity

The responder accepts a control identity that differs from the authenticated QUIC identity.

The mutation changes production validation only. Honest control fields still
pass the remaining checks. The wire witness corrupts each field separately and
requires the corresponding refusal; this mutation fails when it reaches the
changed guard. The wire oracle also rejects registration of that malformed
exchange, so a later service failure cannot hide acceptance.

## Reproduce

Run on the droplet:

```sh
cargo xtask planted loop --only handshake/hello/identity-mismatch-refused --generate-runs 100 \
  --generate-harness handshake/wire --out results/skip-authenticated-identity
```

Witness: `crates/zakura-sim/tests/handshake.rs::hello_guards_refuse_each_corrupt_field`.

Required failure: the run ends at `handshake/hello/identity-mismatch-refused`.
