# A protocol reject keeps the connection

`protocol-reject-keeps-connection.patch` changes `handle_pipe_exit` in
`transport/pipe.rs`, the one place every service's pipe decides whether its
exit is fatal to the connection. A `SinkReject::Protocol` now records no
close cause and cancels nothing, as a `SinkReject::Local` does. The stream's
own teardown stops the service, and the peer that sent the bad frame keeps
its connection.

Guards on the rule's path: a frame the codec or a later stage rejects
reaches the connection close through this function only. Block sync's
routine and discovery's pipe both return their protocol rejects to it, so
the patch breaks the only guard. A frame the stream reader refuses from its
header (`ordered_read_rejected`) doesn't pass through it, so the witnesses'
header faults still close.

Fault class: fault isolation. The node attributes a peer fault to a local
stream error.

Witnesses:

- `block-sync/s6v3`: `each_probe_fault_reaches_its_exact_id` in
  `crates/zakura-sim/tests/block_table.rs`. Its first scenario is an
  undeclared type, which the reader refuses. Its second sends a `Status`
  with bad content, which the routine rejects.
- `discovery/s4v2`: `hello_acts_apply_in_their_witnesses` in
  `crates/zakura-sim/tests/discovery_wire_witness.rs`. The undeclared type
  and the `Hello` row's flags, short and long acts fail at the reader. The
  bad-signature `Hello` is the first act the service rejects.

```sh
export RUSTFLAGS="--cfg zakura_sim" CXXFLAGS="-include cstdint"
cargo test --locked --profile planted -p zakura-sim --test block_table -- --exact each_probe_fault_reaches_its_exact_id
cargo test --locked --profile planted -p zakura-sim --test discovery_wire_witness -- --exact hello_acts_apply_in_their_witnesses
git apply protocol-reject-keeps-connection.patch
# rerun both
git apply -R protocol-reject-keeps-connection.patch
```

Required failure for `block-sync/s6v3`: the run ends at
`block-sync/s6v3/status/bad-content-closes`. The routine rejects the
`Status`, and the connection never gets a fault close.

Required failure for `discovery/s4v2`: the run ends at
`discovery/s4v2/hello/bad-content-closes`. The service rejects the
`Hello`'s signature, and the session's later close, if any, has a neutral
cause.
