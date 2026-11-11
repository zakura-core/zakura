# Skip native transcript

The native path accepts an upgrade transcript.

The patch changes production validation only. Honest fields still pass the remaining checks. Each witness corrupts one field at a time and requires its named refusal.

## Reproduce

Run on the droplet:

```sh
cargo xtask planted loop --only handshake/hello/nonzero-native-transcript-refused --generate-runs 100 --generate-harness handshake/wire
```

Witness: `crates/zakura-sim/tests/handshake.rs::hello_guards_refuse_each_corrupt_field`.

Required failure: the run ends at `handshake/hello/nonzero-native-transcript-refused`.
