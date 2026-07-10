# Private Zakura dev networks (cohorts)

Developer notes for the `[network.zakura] dev_network` cohort tag. For the
operator-facing setup guide, see
[`book/src/dev/private-zakura-network.md`](../../../book/src/dev/private-zakura-network.md).

## What it does

Zakura (v2) dev nodes bootstrap from a few peers, then discovery and gossip pull
in the rest of the network, so concurrent experiments interfere. Setting the same
`dev_network` tag on a group of nodes makes them form a private **Zakura v2
overlay** that is invisible to the v2 layer of any non-cohort node, while still
running on the **unchanged chain** (same genesis, network magic, and activation
heights). It only scopes the v2 overlay; legacy TCP connectivity to public peers
is intentionally unaffected, and the tag has no effect unless `v2_p2p` is enabled.

## Mechanism

A node decides whether to peer over Zakura by comparing two fields of its
[`ZakuraHandshakeConfig`](handshake.rs):

| Field | Normal value | Mismatch reject |
| --- | --- | --- |
| `network_id: ZakuraNetworkId` | `Mainnet` / `Testnet` / `Regtest` | `WrongNetwork` |
| `chain_id: [u8; 32]` | genesis block hash | `WrongChain` |

These two fields are already exchanged and validated in three places, so scoping
them is enough to isolate the overlay with **no wire-format change**:

- the Zakura control handshake over QUIC (`ZakuraControlHello::validate`),
- the legacy → Zakura upgrade prelude over TCP (`validate_prelude_static_fields`),
- signed discovery records, which carry `chain_id` and validate against
  `expected_chain_id`; records derive their `chain_id` from `handshake.chain_id`.

When `dev_network = Some(tag)`, `ZakuraHandshakeConfig::for_network_with_dev_cohort`
sets:

- `network_id = ZakuraNetworkId::Configured` — the coarse "not the public
  network" marker. A public node (`Mainnet`) rejects a dev node first on this
  field with `WrongNetwork`. `network_id` is checked before `chain_id`.
- `chain_id = derive_dev_chain_id(genesis_hash, tag)` — the fine-grained "which
  cohort" id. A domain-separated blake2b hash (personalization
  `zebra-zk-cohort1`) over the real genesis hash and the tag. The personalization
  guarantees a derived cohort id can never collide with a real chain's genesis
  hash, and mixing in the genesis hash keeps the same tag distinct across
  networks.

Resulting behavior:

- **public node ↔ dev node** → `WrongNetwork` (on `network_id`), both fall back to
  a legacy connection;
- **cohort A ↔ cohort B** → both advertise `Configured`, so they pass the
  `network_id` check and reject on `chain_id` with `WrongChain`; their discovery
  records also fail import;
- **same tag** → both fields match → the private overlay forms.

Because `chain_id` here is only a Zakura peer-matching id and block validation
uses the unchanged network parameters, consensus is untouched.

## Code map

- `handshake.rs` — `for_network_with_dev_cohort`, `derive_dev_chain_id`, and the
  `chain_id` doc; the `validate` paths that enforce the two fields.
- `handler.rs` — `ZakuraConfig.dev_network` and the authoritative build site that
  feeds the endpoint, supervisor, and discovery.
- `../peer/handshake.rs` — the legacy → Zakura upgrade path rebuilds the handshake
  config from scratch, so `upgrade_to_zakura_handshake` also threads the cohort
  tag. Without this a tagged node would advertise the cohort id on its native
  endpoint but the plain id during upgrades, and could not upgrade with its own
  cohort.

## Testing

Unit tests live in `handshake.rs` (`dev_cohort_*`, `derive_dev_chain_id_*`) and a
config round-trip in `../config/tests/vectors.rs`:

```bash
cargo test -p zakura-network --lib -- zakura::handshake::tests::dev_cohort
cargo test -p zakura-network --lib -- config::tests::vectors::zakura_dev_network
```

For a two-node smoke test, give two nodes `v2_p2p = true`, the same
`[network.zakura] dev_network`, and `zakura.bootstrap_peers` pointing at each
other, plus a third untagged node. The two tagged nodes upgrade to Zakura and
discover each other (`zakura.p2p.handshake.*` metrics / trace tables) while the
untagged node stays on legacy; all three keep syncing the same chain.
