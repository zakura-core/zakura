# A wanted list may name one service twice

`read_wanted` no longer checks that a wanted list's service IDs are
unique. `GetPeers` and `GetServices` share it, so the patch is one break
for both rows. A `GetServices` that wants one service twice decodes, and
the node answers it. The retained planted patch
`drop-refused-get-services` drops every refused `GetServices` in the
reader; this one breaks the codec's shared list rule.

The witness's loop runs the `get-services` acts in order. The first the
patch affects is `BadContent { rule: GetServicesWanted }`. The node
answers it and never closes, so the obligation
`discovery/s4v2/get-services/bad-content-closes` fails at quiescence.

## Guards on the rule's path

1. The frame filter checks the payload length only.
2. `read_wanted`'s `check_unique`, which the patch removes.
   `ServiceReporter::summaries` asks `GetServices::wants` once per known
   service, so it never repeats a summary and refuses nothing.

## Witness

`get_services_acts_apply_in_their_witnesses` in `crates/zakura-sim/tests/discovery_wire_witness.rs`. It holds on `main`, and the scenario named above is the first in its
loop that reaches the patched code.

## Reproduce

The loop applies the patch in its own worktree. Run it over this
property's cases:

```sh
cargo xtask planted loop --only discovery/s4v2/get-services/bad-content-closes --out <report dir>
```

Required failure: the run ends at `discovery/s4v2/get-services/bad-content-closes`.
