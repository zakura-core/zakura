# A GetServices wants every service, whatever it names

`GetServices::wants` returns true for every service. An empty wanted list
asks for every service, and the patch treats every list that way. The
retained planted patch `ignore-wanted-services` skips the filter at one
call site, the discovery summary in `ServiceReporter::summaries`; this one
breaks the shared predicate that every call site asks.

The witness asks the node for header sync alone. The node runs no header
sync, so its honest answer carries no summary. With the patch, it carries
the discovery summary, which the request didn't name, so
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
