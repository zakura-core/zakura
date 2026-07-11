# Zebra checkpoints

Zebra validates [settled network upgrades](https://zips.z.cash/protocol/protocol.pdf#blockchain) using a list of `Mainnet` and `Testnet` block hash checkpoints:

- [Mainnet checkpoints](https://github.com/zakura-core/zakura/blob/main/zakura-chain/src/parameters/checkpoint/main-checkpoints.txt)
- [Testnet checkpoints](https://github.com/zakura-core/zakura/blob/main/zakura-chain/src/parameters/checkpoint/test-checkpoints.txt)

Using these checkpoints increases Zebra's security against some attacks.

## Update checkpoints

Checkpoint lists are distributed with Zakura, maintainers should update them about every few months to get newer hashes. Here are [the exact commands for updating the lists](https://github.com/zakura-core/zakura/tree/main/zakura-utils/README.md#zakura-checkpoints).
