# Skip network binding

The responder accepts a hello for another network. The authenticated transport identity does not replace the network check.

The mutation changes production validation only. Honest control fields still
pass the remaining checks. The wire witness corrupts each field separately and
requires the corresponding refusal; this mutation fails when it reaches the
changed guard. The wire oracle also rejects registration of that malformed
exchange, so a later service failure cannot hide acceptance.

## Reproduce

Run on the droplet:

```sh
cargo xtask planted loop --only handshake/hello/wrong-network-refused --generate-runs 100 \
  --generate-harness handshake/wire --out results/skip-network-binding
```

Witness: `crates/zakura-sim/tests/handshake.rs::hello_guards_refuse_each_corrupt_field`.

Required failure: the run ends at `handshake/hello/wrong-network-refused`.
