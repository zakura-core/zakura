# The decoder admits a zero GetPeers limit

`check_limit` in discovery's codec refuses only a limit above
`MAX_DISCOVERY_RECORDS_PER_RESPONSE`. A `GetPeers` with limit 0 decodes,
and the node serves it an empty `Peers`. The retained planted patch
`drop-refused-get-peers` drops every refused `GetPeers` in the reader;
this one breaks the codec's own content rule.

The witness's loop runs the `get-peers` acts in order. The first the
patch affects is `BadContent { rule: GetPeersLimit }`, a `GetPeers` whose
limit is 0. The node answers it and never closes, so the obligation
`discovery/s4v2/get-peers/bad-content-closes` fails at quiescence.

## Guards on the rule's path

1. The frame filter checks the payload length only.
2. `check_limit` in `DiscoveryMessage::decode_payload`, which the patch
   loosens. Nothing after the decoder checks a `GetPeers` limit:
   `PeerSampler` takes `limit.min(window)`.

## Witness

`get_peers_acts_apply_in_their_witnesses` in `crates/zakura-sim/tests/discovery_wire_witness.rs`. It holds on `main`, and the scenario named above is the first in its
loop that reaches the patched code.

## Reproduce

The loop applies the patch in its own worktree. Run it over this
property's cases:

```sh
cargo xtask planted loop --only discovery/s4v2/get-peers/bad-content-closes --out <report dir>
```

Required failure: the run ends at `discovery/s4v2/get-peers/bad-content-closes`.
