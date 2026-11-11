# Accept unoffered capability

The initiator accepts capability bits it never offered. Later service selection does not validate the acknowledgement against its offer.

The mutation changes production validation only. Honest control fields still
pass the remaining checks. The wire witness corrupts each field separately and
requires the corresponding refusal; this mutation fails when it reaches the
changed guard. The wire oracle also rejects registration of that malformed
exchange, so a later service failure cannot hide acceptance.

## Reproduce

Run on the droplet:

```sh
cargo xtask planted loop --only handshake/ack/unoffered-capability-refused --generate-runs 100 \
  --generate-harness handshake/wire --out results/accept-unoffered-capability
```

Witness: `crates/zakura-sim/tests/handshake.rs::ack_guards_refuse_each_corrupt_field`.

Required failure: the run ends at `handshake/ack/unoffered-capability-refused`.
