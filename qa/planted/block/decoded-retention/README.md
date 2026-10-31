# Decoded retention mutations

The `.toml` cases exercise `block/decoded-retention` through the simulator's
existing owner target. Their causal traces support fresh-process replay.

`unfunded-state-read.patch` exercises a different boundary. It makes the real
state reader return a deep copy without carrying decoded ownership. The copy
preserves the block hash. The native test retires the original state and keeps
only the returned block. Its independent allocation probe must fail with
`the retained payload has a settlement owner`.

Run this native mutation from a clean checkout:

```sh
cargo +1.97.0 test --locked -p zakura-state --lib \
  retired_state_snapshots_and_read_results_keep_decoded_ownership
git apply qa/planted/block/decoded-retention/unfunded-state-read.patch
cargo +1.97.0 test --locked -p zakura-state --lib \
  retired_state_snapshots_and_read_results_keep_decoded_ownership
git apply -R qa/planted/block/decoded-retention/unfunded-state-read.patch
cargo +1.97.0 test --locked -p zakura-state --lib \
  retired_state_snapshots_and_read_results_keep_decoded_ownership
```

Restore the patch even if the mutant fails unexpectedly. The baseline and
restored runs must pass. Only the named ownership assertion counts as detection;
a build error or timeout does not count. This native test uses real state
workers and RocksDB. It does not provide deterministic backend replay or prove
a complete backend memory bound. Keep it outside blocking PR campaigns.
