# The look-ahead limit stops gating

Both gates that read the look-ahead limit `L` stop reading it. `admit`
(`admission.rs`) plans an above-window take from the byte budget alone, and
`retain_received_body`, the sequencer's final gate, keeps every body below
the block-count cap. Nothing then bounds above-window requests or retained
above-window bodies but the byte budget.

## Guards on the rule's path

1. Admission's look-ahead gate in `admit`: `lookahead_over_budget` and
   the remaining look-ahead room.
2. The sequencer's final gate, `retain_received_body`, which refuses an
   above-window body once retained bytes reach `L`.

The patch breaks both, because either one alone bounds retention.

## Witness

- `block-sync/retained-byte-bound`: `qa/planted/block-sync/retained-byte-bound/wide-commit-window.toml`, the committed input
  `block-sync-stuck-floor.bin`. One mirror serves 840 bodies but withholds
  height 3, and `L` is 4,000 bytes. With no gate, the node fetches and keeps
  every body it can, about 837, past `L + 401·b + Q·b`.
- `block-sync/lookahead-overshoot-bound`: `qa/planted/block-sync/lookahead-overshoot-bound/omit-outstanding-estimates.toml`, the committed
  input `block-sync-above-window.bin`. `M` is 20 MB, `L` is 5 MB, and every
  estimate is `B`. With no gate, the node requests above-window heights
  until the byte budget is full, past `L + H + (W + 1)·k·B`.

## Reproduce

The loop applies the patch in a worktree of its own and runs each case's
witness on the patched build:

```sh
cargo xtask planted loop --cases <dir> --only block-sync/retained-byte-bound --out <report dir>
cargo xtask planted loop --cases <dir> --only block-sync/lookahead-overshoot-bound --out <report dir>
```

Required failure for `wide-commit-window.toml`: the run ends at `block-sync/retained-byte-bound`.
Required failure for `omit-outstanding-estimates.toml`: the run ends at `block-sync/lookahead-overshoot-bound`.
