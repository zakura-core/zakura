# The serving session enforces twice the advertised limit

`Serving::session` hands the shared `Serve` twice the in-flight limit
the node advertises in its `Status`. `Serve`'s margin then admits up to
four times the advertised limit.

The witness's node advertises a limit of one. Its probe sends three
requests while the first two reads are held. The relay labels the third
`OverMargin`, past twice the advertised limit. With the patch the node
admits and serves it. Its serving lease answers no conformant open
request, so the safety check `get-blocks/receiver-serving-bounded` fails
at that step, before the `get-blocks/over-margin-closes` obligation can
fail at quiescence.

## Guards on the rule's path

1. `Serve::advertise` clamps the limit to the row's `max_in_flight`
   (32,768), which doesn't bind here.
2. `commitment_over_limit`, the shared margin check, which reads the
   limit the patch doubles.

## Witness

`requests_past_twice_the_serving_limit_reach_over_margin` in `crates/zakura-sim/tests/block_table.rs`. It holds on `main`, and the scenario named above is the first in its
loop that reaches the patched code.

## Reproduce

The loop applies the patch in its own worktree. Run it over this
property's cases:

```sh
cargo xtask planted loop --only block-sync/s6v3/get-blocks/receiver-serving-bounded --out <report dir>
```

Required failure: the run ends at `block-sync/s6v3/get-blocks/receiver-serving-bounded`.
