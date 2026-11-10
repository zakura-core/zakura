# Any nonzero ending closes its range

`Requester::finish` accepts any ending with a nonzero count. It no
longer checks that a `BlocksDone` reports exactly the bodies received, or
that a `RangeUnavailable` follows no body. The node consumes the bad
ending as a real one and never punishes the peer.

The first scenario of the witness's loop that the patch affects is
`blocks-done/bad-content-closes`: the probe ends the node's open range
with `BlocksDone { returned: 2 }` before any body. The relay labels it
`BadContent`. The node's later request for the same heights overlaps the
range the relay still holds open, so the relay labels it `BadContent`
too, which no sender check reads. The run reaches quiescence with the
fault unpunished.

## Guards on the rule's path

1. The codec's `validate_count` refuses only a zero count, which the
   probe doesn't send.
2. `Requester::finish`'s prefix check, which the patch removes. It is the
   only check that compares an ending with the bodies before it.

## Witness

`faults_inside_the_nodes_exchange_reach_their_exact_ids` in `crates/zakura-sim/tests/block_table.rs`. It holds on `main`, and the scenario named above is the first in its
loop that reaches the patched code.

## Reproduce

The loop applies the patch in its own worktree. Run it over this
property's cases:

```sh
cargo xtask planted loop --only block-sync/s6v3/blocks-done/bad-content-closes --out <report dir>
```

Required failure: the run ends at `block-sync/s6v3/blocks-done/bad-content-closes`.
