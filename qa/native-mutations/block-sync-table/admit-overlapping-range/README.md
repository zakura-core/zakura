# The serving session admits an overlapping range

`ServingSession::admit` no longer refuses a `GetBlocks` that overlaps a
range the session still serves. The node serves the overlapping request
beside the first.

The first scenario of the witness's loop that the patch affects is the
last one, the overlap (`get-blocks/bad-content-closes`): the probe asks
for height 1 twice while the node's first read is held. The adapter
settles the second request's label at the session's admission, against
the live range, as `BadContent`, so it opens no request. The node then
acquires a second serving lease for exchange 1, a second owner for one
open conformant request, and the safety check
`get-blocks/receiver-serving-bounded` fails at that step.

## Guards on the rule's path

1. The codec's `Range::new` checks one range's count and last height
   only.
2. `ServingSession::admit`'s live-range overlap check, which the patch
   removes. `Serve::admit_tracked` counts commitments and checks no
   ranges.

## Witness

`each_probe_fault_reaches_its_exact_id` in `crates/zakura-sim/tests/block_table.rs`. It holds on `main`, and the scenario named above is the first in its
loop that reaches the patched code.

## Reproduce

The loop applies the patch in its own worktree. Run it over this
property's cases:

```sh
cargo xtask planted loop --only block-sync/s6v3/get-blocks/receiver-serving-bounded --out <report dir>
```

Required failure: the run ends at `block-sync/s6v3/get-blocks/receiver-serving-bounded`.
