# Skip requested limit ceilings

The initiator accepts limits above its request while retaining the local hard limits.

The patch changes production validation only. Honest fields still pass the remaining checks. Each witness corrupts one field at a time and requires its named refusal.

## Reproduce

Run on the droplet:

```sh
cargo xtask planted loop --only handshake/ack/limits-above-request-refused --generate-runs 100 --generate-harness handshake/wire
```

Witness: `crates/zakura-sim/tests/handshake.rs::ack_guards_refuse_each_corrupt_field`.

Required failure: the run ends at `handshake/ack/limits-above-request-refused`.
