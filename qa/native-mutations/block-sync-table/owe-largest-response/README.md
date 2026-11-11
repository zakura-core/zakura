# The requester owes every range the largest response

`Requester::reserve` books each range's reservation and its body-byte
limit at `MAX_BS_RESPONSE_BYTES`, not at the response-byte limit the peer
advertised. A body over the peer's own limit then passes the stream
reader's header check and the claim, and the routine queues its work.

The witness's first scenario (`block/over-owed-closes`) has a probe that
advertises 1,000 response bytes and then sends a larger body. The relay
labels that body `OverOwed`. With the patch the node admits its work,
which no conformant frame authorized, so the safety check
`block/receiver-work-authorized` fails at that step, before the
`block/over-owed-closes` obligation can fail at quiescence.

## Guards on the rule's path

1. The stream reader's header gate, `Requester::admit` →
   `Reservations::admit`: the frame's bytes against what the live ranges
   owe. The reservation's cap comes from `reserve`.
2. `Requester::claim_block`: the body against `Pending::max_body_bytes`,
   also set in `reserve`.
3. `Reservations::claim_frame`: the frame against the reservation's cap.

All three read the one byte limit `reserve` records, so the patch breaks
every guard on the path.

## Witness

`faults_inside_the_nodes_exchange_reach_their_exact_ids` in `crates/zakura-sim/tests/block_table.rs`. It holds on `main`, and the scenario named above is the first in its
loop that reaches the patched code.

## Reproduce

The loop applies the patch in its own worktree. Run it over this
property's cases:

```sh
cargo xtask planted loop --only block-sync/s6v3/block/receiver-work-authorized --out <report dir>
```

Required failure: the run ends at `block-sync/s6v3/block/receiver-work-authorized`.
