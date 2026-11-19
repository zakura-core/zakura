# A peer sample ignores the GetPeers limit

`DiscoveryState::sample_peers` takes the configured sample limit alone, in
place of `limit.min(peer_sample_limit)`. Every `Peers` the node writes
then carries up to the configured limit's records, whatever its `GetPeers`
asked for. The retained planted patch `ignore-peers-limit` replaces the
limit at its one call site, `PeerSampler` in `serving.rs`; this one breaks
the state's own bound, which every sample takes.

The witness's `GetPeers` asks for one record. Its scenario's two other
peers listen on public addresses, so the node dials and confirms both
records (#2267). With the patch, the node's `Peers` carries both, and
`discovery/s4v2/peers/sender-within-owed` fails.

## Guards on the rule's path

1. `PeerSampler::produce` (`serving.rs`) passes the request's limit.
2. `DiscoveryState::sample_peers` (`state.rs`) bounds it by the configured
   sample limit. The patch drops the request's limit here.
3. `ZakuraDiscoveryBook::sample_peers_with_rng` (`protocol.rs`) bounds the
   limit by one response's window, 32 records, which a two-record book
   never reaches.

## Witness

`answers_within_their_requests_hold_sender_within_owed` in
`crates/zakura-sim/tests/discovery_wire_witness.rs`. It holds on `main`.

## Reproduce

The loop applies the patch in its own worktree. Run it over this
property's cases:

```sh
cargo xtask planted loop --only discovery/s4v2/peers/sender-within-owed --out <report dir>
```

Required failure: the run ends at `discovery/s4v2/peers/sender-within-owed`.
