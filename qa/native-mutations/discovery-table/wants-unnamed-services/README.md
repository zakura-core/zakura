# A GetServices wants the services it doesn't name

`GetServices::wants` inverts its list check: a nonempty wanted list asks
for every service it doesn't name. An empty list still asks for every
service, so the node's own requests, which name none, are unchanged.

The witness asks the node for header sync alone. With the patch, the node
leaves out header sync, which it doesn't run anyway, and adds the
discovery summary, which the request didn't name. So
`discovery/s4v2/services/sender-within-owed` fails.

## Guards on the rule's path

1. `ServiceReporter::summaries` (`serving.rs`) asks `GetServices::wants`
   once per local service. The patch breaks the predicate it asks.
2. No later guard filters the answer. The requester's check in
   `requester.rs` asks the same predicate, but it judges the node's own
   requests, which name no service.

## Witness

`answers_within_their_requests_hold_sender_within_owed` in
`crates/zakura-sim/tests/discovery_wire_witness.rs`. It holds on `main`.

## Reproduce

The loop applies the patch in its own worktree. Run it over this
property's cases:

```sh
cargo xtask planted loop --only discovery/s4v2/services/sender-within-owed --out <report dir>
```

Required failure: the run ends at `discovery/s4v2/services/sender-within-owed`.
