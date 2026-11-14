# Undeclared type reaches the decoder

`undeclared-type-reaches-decoder.patch` changes
`FrameFilter::check_header` in `transport/message_rule/frame_filter.rs`. A
frame whose type the reader's table doesn't declare now passes with the
stream's frame cap, as on a stream without a table, so the service's
decoder judges it.

This breaks one guard only. Guards on the rule's path:

- the stream reader's header check, which the patch breaks;
- every tabled stream behind a QUIC reader: `admit_inbound_message` in
  `handler.rs` looks the frame's row up with an `expect` that the filter
  admitted only declared types;
- discovery: then `wire_codec::decode_frame` refuses an unknown type;
- block sync: then the routine's `SessionGuard::admit`
  (`transport/guard.rs`) refuses a disallowed type, and
  `LiveRequester::authorize` refuses a type with no row.

Fault class: request authorization. The node reads the payload of a frame
its table refuses.

Witness: `discovery/s4v2`, `hello_acts_apply_in_their_witnesses` in
`crates/zakura-sim/tests/discovery_wire_witness.rs`. Its first act sends the
peer's `Hello` as type 0.

`block-sync/s6v3` has no case, because the block-sync harness can't reach
the patch's first symptom. In production, a stream-6 reader reaches the
`expect` and panics, as discovery's does. The block-sync harness links the
node over in-memory channels, and its relay checks each header with
`StreamReader::admits` but never runs `admit_inbound_message`. Its frame
reaches the routine's `SessionGuard::admit` instead, which refuses it with
`service_protocol_reject`, so a stream-6 run would hold. The `expect` is the
same for every tabled stream, so the discovery case covers it.

```sh
export RUSTFLAGS="--cfg zakura_sim" CXXFLAGS="-include cstdint"
cargo test --locked --profile planted -p zakura-sim --test discovery_wire_witness -- --exact hello_acts_apply_in_their_witnesses
git apply undeclared-type-reaches-decoder.patch
# rerun it
git apply -R undeclared-type-reaches-decoder.patch
```

Required failure for `discovery/s4v2`: the run ends at
`discovery/s4v2/undeclared-type-closes`.

Expected result: killed by another rule, `sim/no-task-panic`. The handler's
`expect` panics in the stream reader task before the codec runs. The run
stops at the panic, so the `undeclared-type-closes` obligation stays open
and the ID reports `inconclusive`. The filter is the only check before the
`expect`, so this panic is the patch's first symptom on every tabled stream
behind a QUIC reader.
