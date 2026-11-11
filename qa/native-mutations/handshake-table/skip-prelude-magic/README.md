# Skip prelude magic

The stream reader accepts an invalid prelude magic.

The patch changes production behavior only. Honest streams still pass the remaining guards.

## Reproduce

Run on the droplet:

```sh
cargo xtask planted loop --only handshake/prelude/bad-magic-resets --generate-runs 100 --generate-harness handshake/wire
```

Witness: `crates/zakura-sim/tests/handshake.rs::prelude_guards_apply_each_declared_consequence`.

Required failure: the run ends at `handshake/prelude/bad-magic-resets`.
