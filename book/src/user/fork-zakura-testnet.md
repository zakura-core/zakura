# Forking the Zcash Testnet with Zakura

> **Note:** This tutorial uses s-nomp for mining, which has known compatibility issues with NU5+.
> For current mining solutions, see [Mining Zcash with Zakura](mining.md). You may need to adapt
> this guide to use alternative mining pool software.

The Zcash blockchain community consistently explores upgrades to the Zcash protocol, introducing new features to the consensus layer. This tutorial guides teams or individuals through forking the Zcash Testnet locally using Zakura, enabling testing of custom functionalities in a private testnet environment.

As of writing, the current network upgrade on the Zcash Testnet is `Nu5`. While a future upgrade (`Nu6`) activation height will be known later, for this tutorial, we aim to activate after `Nu5`, allowing us to observe our code crossing the network upgrade and continuing isolated.

To achieve this, we'll use [Zakura](https://github.com/zakura-core/zakura) as the node, [s-nomp](https://github.com/ZcashFoundation/s-nomp) as the mining pool, and [nheqminer](https://github.com/ZcashFoundation/nheqminer) as the Equihash miner.

**Note:** This tutorial aims to remain generally valid after `Nu6`, with adjustments to the network upgrade name and block heights.

# Requirements

- A modified Zakura version capable of syncing up to our chosen activation height, including the changes from the [code changes step](#code-changes).
- Mining tools:
  - s-nomp pool
  - nheqminer

You may have two Zakura versions: one for syncing up to the activation height and another (preferably built on top of the first one) with the network upgrade and additional functionality.

**Note:** For mining setup please see [How to mine with Zakura on testnet](mining-testnet-s-nomp.md)

## Sync the Testnet to a Block after `Nu5` Activation

Select a height for the new network upgrade after `Nu5`. In the Zcash public testnet, `Nu5` activation height is `1_842_420`, and at the time of writing, the testnet was at around block `2_598_958`. To avoid dealing with checkpoints, choose a block that is not only after `Nu5` but also in the future. In this tutorial, we chose block `2_599_958`, which is 1000 blocks ahead of the current testnet tip.

Clone Zakura, create a config file, and use `state.debug_stop_at_height` to halt the Zakura sync after reaching our chosen network upgrade block height (`2_599_958`):

The relevant parts of the config file are:

```toml
[network]
listen_addr = "0.0.0.0:18233"
network = "Testnet"

[state]
debug_stop_at_height = 2599958
cache_dir = "/home/user/.cache/zakura"
```

Generate a Zakura config file:

```bash
zakurad generate -o myconf.toml
```

Start Zakura with the modified config:

```bash
zakurad -c myconf.toml start
```

Wait for the sync to complete (this may take up to 24 hours, depending on testnet conditions), resulting in a state up to the desired block in `~/.cache/zakura`.

## Code changes

We need to add the network upgrade variant to the `zcash_primitives` crate and Zakura.

### librustzcash / zcash_primitives

Add the new network upgrade variant and a branch id among some changes needed for the library to compile. Here are some examples:

- Sample internal commit: [Unclean test code](https://github.com/zcash/librustzcash/commit/76d81db22fb4c52302f81c9b3e1d98fb6b71188c)
- Public PR adding Nu6 behind a feature: [librustzcash PR #1048](https://github.com/zcash/librustzcash/pull/1048)

After the changes, check that the library can be built with `cargo build --release`.

## Zakura

Here we are making changes to create an isolated network version of Zakura. In addition to your own changes, this Zakura version needs to have the following:

- Add a `Nu6` variant to the `NetworkUpgrade` enum located in `zakura-chain/src/parameters/network_upgrade.rs`.
- Add consensus branch id, a random non-repeated string. We used `00000006` in our tests when writing this tutorial.
- Point to the modified `zcash_primitives` in `zakura-chain/Cargo.toml`. In my case, I had to replace the dependency line with something like:

  ```toml
  zcash_primitives = { git = "https://github.com/oxarbitrage/librustzcash", branch = "nu6-test", features = ["transparent-inputs"] }
  ```

- Make fixes needed to compile.
- Ignore how far we are from the tip in get block template: `zakura-rpc/src/methods/get_block_template_rpcs/get_block_template.rs`

Unclean test commit for Zakura: [Zakura commit](https://github.com/zakura-core/zakura/commit/d05af154c897d4820999fcb968b7b62d10b26aa8)

Make sure you can build the `zakurad` binary after the changes with `cargo build --release`

## Configuration for isolated network

Now that you have a synced state and a modified Zakura version, it's time to run your isolated network. Relevant parts of the configuration file:

Relevant parts of the configuration file:

```toml
[mempool]
debug_enable_at_height = 0
max_datacarrier_bytes = 83

[mining]
miner_address = 't27eWDgjFYJGVXmzrXeVjnb5J3uXDM9xH9v'

[network]
cache_dir = false
initial_testnet_peers = [
  "dnsseed.testnet.z.cash:18233",
  "testnet.seeder.zfnd.org:18233",
  "testnet.is.yolo.money:18233",
]
listen_addr = "0.0.0.0:18233"
network = "Testnet"

[rpc]
listen_addr = "0.0.0.0:18232"

[state]
cache_dir = "/home/oxarbitrage/.cache/zakura"
```

- `debug_enable_at_height= 0` enables the mempool independently of the tip height.
- The `[mining]` section is necessary for mining blocks, and the rpc endpoint `rpc.listen_addr` too.
- `initial_testnet_peers` is needed as Zakura starts behind the fork block, approximately 100 blocks behind, so it needs to receive those blocks again. This is necessary until the new fork passes more than 100 blocks after the fork height. At that point, this network can be isolated, and initial_testnet_peers can be set to `[]`.
- Ensure your `state.cache_dir` is the same as when you saved state in step 1.

Start the chain with:

```bash
zakurad -c myconf.toml start
```

Start s-nomp:

```bash
npm start
```

Start the miner:

```bash
nheqminer -l 127.0.0.1:1234 -u tmRGc4CD1UyUdbSJmTUzcB6oDqk4qUaHnnh.worker1 -t 1
```

## Confirm Forked Chain

After Zakura retrieves blocks up to your activation height from the network, the network upgrade will change, and no more valid blocks could be received from outside.

After a while, in s-nomp, you should see submitted blocks from time to time after the fork height.

```text
...
[Pool]        [zcash_testnet] (Thread 1) Block notification via RPC after block submission
[Pool]        [zcash_testnet] (Thread 1) Submitted Block using submitblock successfully to daemon instance(s)
[Pool]        [zcash_testnet] (Thread 1) Block found: ... by tmRGc4CD1UyUdbSJmTUzcB6oDqk4qUaHnnh.worker1
[Pool]        [zcash_testnet] (Thread 1) Block notification
...
```

You'll also see this in Zakura:

```text
...
INFO zakura_rpc::methods::get_block_template_rpcs: submit block accepted block_hash=block::Hash("...") block_height="..."
INFO zakura_rpc::methods::get_block_template_rpcs: submit block accepted block_hash=block::Hash("...") block_height="..."
...
```

Ignore messages in Zakura related to how far you are from the tip or network/system clock issues, etc.

Check that you are in the right branch with the curl command:

```bash
curl --silent --data-binary '{"jsonrpc": "1.0", "id":"curltest", "method": "getblockchaininfo", "params": [] }' -H 'Content-type: application/json' http://127.0.0.1:18232/ | jq
```

In the result, verify the tip of the chain is after your activation height for `Nu6` and that you are in branch `00000006` as expected.

## Final words

Next steps depend on your use case. You might want to submit transactions with new fields, accept those transactions as part of new blocks in the forked chain, or observe changes at activation without sending transactions. Further actions are not covered in this tutorial.
