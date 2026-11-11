# Reserved flags ignored

`reserved-flags-ignored.patch` makes every reader ignore a frame's flag
bits, as if they were reserved for extensions. No native message defines a
flag bit, so the tables' rule is that a set bit closes the connection.

Guards on the rule's path, all broken:

- the stream reader's header check, `FrameFilter::check_header` in
  `transport/message_rule/frame_filter.rs`;
- discovery's codec, `wire_codec::decode_frame` in
  `wire_codec/message.rs`;
- block sync's envelope check, `LiveRequester::authorize` in
  `block_sync/regulated/live_requester.rs`.

Fault class: request authorization. The node admits a frame its table
refuses.

Witnesses:

- `block-sync/s6v3`: `each_probe_fault_reaches_its_exact_id` in
  `crates/zakura-sim/tests/block_table.rs`. Its `FlaggedStatus` scenario
  sends a valid `Status` with flag bit 0, so the flag is its only fault.
- `discovery/s4v2`: `hello_acts_apply_in_their_witnesses` in
  `crates/zakura-sim/tests/discovery_wire_witness.rs`. Its second act sends
  the peer's valid `Hello` with flag bit 0.

```sh
export RUSTFLAGS="--cfg zakura_sim" CXXFLAGS="-include cstdint"
cargo test --locked --profile planted -p zakura-sim --test block_table -- --exact each_probe_fault_reaches_its_exact_id
cargo test --locked --profile planted -p zakura-sim --test discovery_wire_witness -- --exact hello_acts_apply_in_their_witnesses
git apply reserved-flags-ignored.patch
# rerun both
git apply -R reserved-flags-ignored.patch
```

Required failure for `discovery/s4v2`: the run ends at
`discovery/s4v2/hello/reserved-flags-closes`. The node reads the flagged
`Hello` as a valid one and never closes.

Required failure for `block-sync/s6v3`: the run ends at
`block-sync/s6v3/status/reserved-flags-closes`.

Each raw flagged frame in the block-sync probe also breaks a content or
solicitation rule, and that rule's fault close discharges
`reserved-flags-closes`. The `FlaggedStatus` scenario breaks no other rule,
so the patched node admits it and never closes.
