# Skip control read hard cap

The control reader uses the larger configured frame cap instead of the 16 KiB hard cap. The retained peers declare an oversized length and send a short body. The decoder cannot hide the early admission failure behind its later payload check.

The patch changes production behavior only. Honest streams still pass the remaining guards.

## Reproduce

Run on the droplet:

```sh
cargo xtask planted loop --only handshake/hello/oversize-refused --only handshake/ack/oversize-refused --generate-runs 100 --generate-harness handshake/wire
```

Witness: `crates/zakura-sim/tests/handshake.rs::hello_guards_refuse_each_corrupt_field`.

Required failure: the run ends at `handshake/hello/oversize-refused`.

Witness: `crates/zakura-sim/tests/handshake.rs::ack_guards_refuse_each_corrupt_field`.

Required failure: the run ends at `handshake/ack/oversize-refused`.
