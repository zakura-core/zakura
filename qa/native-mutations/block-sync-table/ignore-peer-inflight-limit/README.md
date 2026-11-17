# Requests ignore the peer's in-flight limit

`clamped_limits` in the peer routine takes the protocol's in-flight
ceiling instead of the `max_inflight_requests` the peer advertised. The
routine then opens more requests than the peer allows.

Every scenario of the witness has a probe that advertises one request in
flight, one body per response and eight heights. Its first scenario
(`block/over-owed-closes`) is the first the patch affects: the node opens
a second `GetBlocks` while the first is open, the relay labels it
`OverMargin`, and the sender check `get-blocks/sender-within-limit`
fails at that step.

## Guards on the rule's path

1. `PeerState::requester_stop`: unanswered requests against
   `window.max_inflight_requests`.
2. The window's hard cap, `hard_outbound_capacity`, which reads the same
   field.

Both read the limit `clamped_limits` sets, so the patch breaks both. The
local reservation pool share (`Requester::at_capacity`) stays and doesn't
bind at eight heights.

## Witness

`faults_inside_the_nodes_exchange_reach_their_exact_ids` in `crates/zakura-sim/tests/block_table.rs`. It holds on `main`, and the scenario named above is the first in its
loop that reaches the patched code.

## Reproduce

The loop applies the patch in its own worktree. Run it over this
property's cases:

```sh
cargo xtask planted loop --only block-sync/s6v3/get-blocks/sender-within-limit --out <report dir>
```

Required failure: the run ends at `block-sync/s6v3/get-blocks/sender-within-limit`.
