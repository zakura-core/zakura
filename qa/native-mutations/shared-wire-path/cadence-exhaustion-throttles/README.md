# Cadence exhaustion throttles

`cadence-exhaustion-throttles.patch` changes `admit_tabled_message` in
`handler.rs`, the tabled-stream part of `admit_inbound_message`. A frame
that finds its row's cadence bucket empty is now `Throttled`, as an
untabled stream's frame is when its message bucket is empty. The reader
closes the connection with `ordered_rate_limited`. The row's declaration
says the receiver records exhaustion without rejecting the message, because
transport buffering alone can empty a bucket.

Guards on the rule's path: this is the only place a tabled stream's cadence
charge acts. No later stage reads the charge.

Fault class: boundary accounting. The node turns an observation bucket into
a limit.

Witnesses:

- `discovery/s4v2`: `hello_acts_apply_in_their_witnesses` in
  `crates/zakura-sim/tests/discovery_wire_witness.rs`. Its last act is the
  `Hello` burst, which sends until the peer's model finds the bucket empty.
- `block-sync/s6v3`: `each_probe_fault_reaches_its_exact_id` in
  `crates/zakura-sim/tests/block_table.rs`. Its `status-cadence-burst`
  scenario sends 30 `Status` frames. The harness's relay runs
  `admit_tabled_message` through `StreamAdmission`, on cadence buckets it
  owns.

```sh
export RUSTFLAGS="--cfg zakura_sim" CXXFLAGS="-include cstdint"
cargo test --locked --profile planted -p zakura-sim --test discovery_wire_witness -- --exact hello_acts_apply_in_their_witnesses
git apply cadence-exhaustion-throttles.patch
# rerun it
git apply -R cadence-exhaustion-throttles.patch
```

For `block-sync/s6v3`, run the same steps with
`--test block_table -- --exact each_probe_fault_reaches_its_exact_id`.

Required failure for `block-sync/s6v3`: the run ends at
`block-sync/s6v3/status/cadence-never-closes`.

Required failure for `discovery/s4v2`: the run ends at
`discovery/s4v2/hello/cadence-never-closes`.

On both tables, the node closes a peer whose burst only emptied the bucket,
with cause `ordered_rate_limited`. That cause is neutral, but
`cadence-never-closes` counts a rate-limit close as a punishment
(`model::RATE_LIMIT_CAUSES`).
