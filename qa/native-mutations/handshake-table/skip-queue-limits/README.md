# Skip queue limits

The shared limit validator omits bad-queue-limit checks for both control roles.

The patch changes production validation only. Honest fields still pass the remaining checks. Each witness corrupts one field at a time and requires its named refusal.

## Reproduce

Run on the droplet:

```sh
cargo xtask planted loop --only handshake/hello/bad-queue-limit-refused --only handshake/ack/bad-queue-limit-refused --generate-runs 100 --generate-harness handshake/wire
```

Witness: `crates/zakura-sim/tests/handshake.rs::hello_guards_refuse_each_corrupt_field`.

Required failure: the run ends at `handshake/hello/bad-queue-limit-refused`.

Witness: `crates/zakura-sim/tests/handshake.rs::ack_guards_refuse_each_corrupt_field`.

Required failure: the run ends at `handshake/ack/bad-queue-limit-refused`.
