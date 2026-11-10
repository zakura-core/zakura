# Cadence exhaustion throttles

`cadence-exhaustion-throttles.patch` changes `admit_inbound_message` in
`handler.rs`. A frame that finds its row's cadence bucket empty is now
`Throttled`, as an untabled stream's frame is when its message bucket is
empty. The reader closes the connection with `ordered_rate_limited`. The
row's declaration says the receiver records exhaustion without rejecting
the message, because transport buffering alone can empty a bucket.

Guards on the rule's path: this is the only place a tabled stream's cadence
charge acts. No later stage reads the charge.

Fault class: boundary accounting. The node turns an observation bucket into
a limit.

Witness: `discovery/s4v2`, `hello_acts_apply_in_their_witnesses` in
`crates/zakura-sim/tests/discovery_wire_witness.rs`. Its last act is the
`Hello` burst, which sends until the peer's model finds the bucket empty.

`block-sync/s6v3` has no witness. The block-sync harness links the node
over in-memory channels, and its relay checks each header with
`StreamReader::admits`. Nothing on that path runs `admit_inbound_message`,
so the patched code never runs there. The cadence branch is the same for
every tabled stream, so the discovery witness covers it.

```sh
export RUSTFLAGS="--cfg zakura_sim" CXXFLAGS="-include cstdint"
cargo test --locked --profile planted -p zakura-sim --test discovery_wire_witness -- --exact hello_acts_apply_in_their_witnesses
git apply cadence-exhaustion-throttles.patch
# rerun it
git apply -R cadence-exhaustion-throttles.patch
```

Required failure for `discovery/s4v2`: the run ends at
`discovery/s4v2/hello/cadence-never-closes`. The node closes a peer whose
burst only emptied the bucket, with cause `ordered_rate_limited`. That cause
is neutral, but `cadence-never-closes` counts a rate-limit close as a
punishment (`model::RATE_LIMIT_CAUSES`).
