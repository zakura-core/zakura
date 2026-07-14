# Private Zakura Dev Networks

When the team iterates on breaking changes to the Zakura (v2) P2P stack, separate
experiments must not interfere with each other or leak into the public network.
A node bootstraps from a few peers, but discovery and gossip then pull in the
rest of the network, so two engineers testing different changes at the same time
would otherwise collide.

The Zakura v2 stack is [experimental](../user/p2p.md). Private cohorts isolate
experiments from each other; they do not make the stack production-hardened.

A _private Zakura dev network_ (a "cohort") solves this without changing
consensus. Set a tag in the config and a node only forms Zakura (v2) connections
with peers that advertise the same tag. Public nodes and other cohorts ignore it,
and it ignores them, but it still runs on the real chain (same genesis, network
magic, and activation heights), so it validates exactly what production does.

This only scopes the **Zakura v2 overlay**. A dev node may still maintain legacy
TCP connections to public peers; the isolation applies to the v2 stack that is
under test. The tag has no effect unless Zakura P2P is enabled
(`p2p_stack = "zakura"` or `"dual"`).

## Configuration

Give every node in the group the same tag under `[network.zakura]`:

```toml
[network.zakura]
# Any node sharing this exact string joins the same private overlay.
# A different string, or no string, is a different (or the public) overlay.
dev_network = "evan-breaking-change"

# Seed the cohort by listing each other's native Zakura endpoints as
# `node_id@direct_addr`. The cohort then self-organizes via cohort-tagged
# discovery; it never reaches public or other-cohort peers.
bootstrap_peers = [
  "ed25519nodeid1@10.0.0.1:8234",
  "ed25519nodeid2@10.0.0.2:8234",
]

# Optional: tune the default of 16 if more cohort nodes share an egress IP.
max_connections_per_ip = 16
```

```toml
[network]
# Keep Zakura P2P enabled so the overlay runs.
p2p_stack = "dual"
```

## How it works

The tag scopes the node's Zakura identity at a single point. With a tag set, the
node advertises `ZakuraNetworkId::Configured` and a chain id derived from the
real genesis hash and the tag (a domain-separated hash, so a cohort id can never
collide with a real chain's genesis). Both fields are already exchanged and
validated in the Zakura handshake, the legacy→Zakura upgrade prelude, and signed
discovery records, so:

- a **public mainnet** Zakura node (`network_id = Mainnet`) and a dev node reject
  each other with `WrongNetwork`, falling back to a legacy connection;
- two **different cohorts** (both `Configured`, different chain id) reject each
  other with `WrongChain`, and their discovery records fail validation on import;
- **same-tag** peers match on both fields, complete the Zakura upgrade, and gossip
  cohort-tagged records so the group grows among itself.

Because the tag is mixed only into the Zakura peer-matching chain id and never
into block validation, consensus is unaffected.

## Verifying isolation

Launch two nodes with the same `dev_network` and `bootstrap_peers` pointing at
each other, plus a third node without the tag. Watch the `zakura.p2p.handshake.*`
metrics (or the Zakura trace tables): the two tagged nodes upgrade to Zakura and
discover each other, while the untagged node stays on legacy with no v2 upgrade.
All three keep syncing the same chain.
