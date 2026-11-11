# Skip nonce echo

The initiator no longer checks the echoed local nonce. The remaining peer-nonce comparison is tautological because the caller supplies ack.peer_nonce as remote_peer_nonce.

The mutation changes production validation only. Honest control fields still
pass the remaining checks. The wire witness corrupts each field separately and
requires the corresponding refusal; this mutation fails when it reaches the
changed guard. The wire oracle also rejects registration of that malformed
exchange, so a later service failure cannot hide acceptance.

## Reproduce

Run on the droplet:

```sh
cargo xtask planted loop --only handshake/ack/bad-nonce-echo-refused --generate-runs 100 \
  --generate-harness handshake/wire --out results/skip-nonce-echo
```

Witness: `crates/zakura-sim/tests/handshake.rs::ack_guards_refuse_each_corrupt_field`.

Required failure: the run ends at `handshake/ack/bad-nonce-echo-refused`.
