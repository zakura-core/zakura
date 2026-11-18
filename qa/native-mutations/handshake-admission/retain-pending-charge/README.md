# Retain pending charge

The machine retains a pending charge when its owner releases the actual permit.

Run on the droplet with both generator harnesses from the work list:

```sh
cargo xtask planted loop --only handshake/counts-match-permits --generate-runs 100
```

Witness: `crates/zakura-sim/tests/handshake.rs::counts_trace_replays_with_each_named_rule`.

Required failure: the run ends at `handshake/counts-match-permits`.
