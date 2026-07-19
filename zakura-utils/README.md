# Zebra Utilities

Tools for maintaining and testing Zebra:

- [zakura-release-state](#zakura-release-state)
- [zakura-checkpoints](#zakura-checkpoints)
- [zakurad-hash-lookup](#zakurad-hash-lookup)
- [zakurad-log-filter](#zakurad-log-filter)
- [zcash-rpc-diff](#zcash-rpc-diff)
- [scanning-results-reader](#scanning-results-reader)

Binaries are easier to use if they are located in your system execution path.

## zakura-release-state

`zakura-release-state` produces the coupled Mainnet checkpoint and verified commitment tree
frontier input for a release. It reads retained block hashes and `BlockInfo` sizes from a
finalized state database, so it works with pruned state and does not need historical block
bodies. The output directory is created atomically and contains exactly:

- `manifest.json`, which binds the fixed checkpoint base, finalized height and hash, timestamp,
  artifact names, sizes, and SHA-256 digests;
- `block-metadata.bin`, a compact contiguous sequence of block hashes and sizes; and
- `mainnet-frontier.bin`, the four final note commitment tree frontiers at that finalized height.

Build and export a bundle from a stopped, synced Mainnet node:

```sh
cargo run --locked -p zakura-utils --features zakura-release-state \
  --bin zakura-release-state -- export \
  --cache-dir /path/to/zakura-cache \
  --output-dir /path/to/new-bundle \
  --generated-at 2026-07-18T12:34:56Z
```

The fixed base is Mainnet checkpoint 3,358,006. A publisher can therefore keep producing bundles
after an earlier bundle has been imported without rebuilding its binary. Import authenticates
the source's fixed prefix and every existing later checkpoint against the bundle, preserves that
source prefix byte-for-byte, and appends a deterministic suffix whose terminal checkpoint is the
bundle's finalized height. Every checkpoint gap remains between 17 and 400 blocks.

Verify a downloaded bundle, import it into a source checkout, and verify the tracked result:

```sh
cargo run --locked -p zakura-utils --features zakura-release-state \
  --bin zakura-release-state -- verify --bundle-dir /path/to/bundle
cargo run --locked -p zakura-utils --features zakura-release-state \
  --bin zakura-release-state -- import --bundle-dir /path/to/bundle --source-dir .
cargo run --locked -p zakura-utils --features zakura-release-state \
  --bin zakura-release-state -- verify-source --source-dir . --require-bundle-provenance
```

Import updates `main-checkpoints.txt`, `mainnet-frontier.bin`, and the tracked
`mainnet-frontier.json` provenance record only after the entire bundle and source prefix validate.
An identical reimport is a no-op. Lower heights, same-height conflicts, digest mismatches, and
unknown manifest fields fail closed. Ordinary source validation accepts the initial
`legacy-bootstrap` provenance, but release preparation uses `--require-bundle-provenance` so a
future release cannot use that bootstrap state.

## zakura-checkpoints

This command generates a list of zebra checkpoints, and writes them to standard output. Each checkpoint consists of a block height and hash.

The former GCP checkpoint-generation and `checkpoint-update.yml` pipelines remain retired. For
Mainnet releases, use the coupled `zakura-release-state` flow above instead. A manually generated
Mainnet checkpoint list does not provide the matching frontier or bundle provenance required by
the release gate. This command remains useful for diagnostics and Testnet updates.

#### Manual Checkpoint Generation

To create checkpoints, you need a synchronized instance of `zakurad` or `zcashd`.
`zakurad` can be queried directly or via an installed `zcash-cli` RPC client.
`zcashd` must be queried via `zcash-cli`, which performs the correct RPC authentication.

#### Checkpoint Generation Setup

Make sure your `zakurad` or `zcashd` is [listening for RPC requests](https://docs.rs/zakura-rpc/latest/zakura_rpc/config/rpc/struct.Config.html#structfield.listen_addr),
and synced to the network tip.

If you are on a Debian system, `zcash-cli` [can be installed as a package](https://zcash.readthedocs.io/en/master/rtd_pages/install_debian_bin_packages.html).

`zakura-checkpoints` is a standalone rust binary, you can compile it using:

```sh
cargo install --locked --features zakura-checkpoints --git https://github.com/zakura-core/zakura zakura-utils
```

#### Checkpoint Generation Commands

You can update the checkpoints using these commands:

```sh
zakura-checkpoints --last-checkpoint $(tail -1 zakura-chain/src/parameters/checkpoint/main-checkpoints.txt | cut -d" " -f1) | tee --append zakura-chain/src/parameters/checkpoint/main-checkpoints.txt &
zakura-checkpoints --last-checkpoint $(tail -1 zakura-chain/src/parameters/checkpoint/test-checkpoints.txt | cut -d" " -f1) -- -testnet | tee --append zakura-chain/src/parameters/checkpoint/test-checkpoints.txt &
wait
```

When updating the lists there is no need to start from the genesis block. The program option
`--last-checkpoint` will let you specify at what block height you want to start. Usually, the
maintainers will copy the last height from each list, and start from there.

Other useful options are:

- `--transport direct`: connect directly to a `zakurad` instance
- `--addr`: supply a custom RPC address and port for the node
- `-- -testnet`: connect the `zcash-cli` binary to a testnet node instance

You can see all the `zakura-checkpoints` options using:

```sh
target/release/zakura-checkpoints --help
```

For more details about checkpoint lists, see the [`zakura-checkpoints` README.](https://github.com/zakura-core/zakura/tree/main/zakura-chain/src/parameters/checkpoint/README.md)

#### Checkpoint Generation for Testnet

To update the testnet checkpoints, `zakura-checkpoints` needs to connect to a testnet node.

To launch a testnet node, you can either:

- start `zakurad` [with a `zakurad.toml` with `network.network` set to `Testnet`](https://docs.rs/zakura-network/latest/zakura_network/config/struct.Config.html#structfield.network), or
- run `zcashd -testnet`.

Then use the commands above to regenerate the checkpoints.

#### Submit new checkpoints as pull request

- If you started from the last checkpoint in the current list, add the checkpoint list to the end
  of the existing checkpoint file. If you started from genesis, replace the entire file.
- Open a pull request with the updated Mainnet and Testnet lists at:
  <https://github.com/zakura-core/zakura/pulls>

## zakurad-hash-lookup

Given a block hash the script will get additional information using `zcash-cli`.

```sh
$ echo "00000001f53a5e284393dfecf2a2405f62c07e2503047a28e2d1b6e76b25f863" | zakurad-hash-lookup
high: 3299
time: 2016-11-02T13:24:26Z
hash: 00000001f53a5e284393dfecf2a2405f62c07e2503047a28e2d1b6e76b25f863
prev: 00000001dbbb8b26eb92003086c5bd854e16d9f16e2e5b4fcc007b6b0ae57be3
next: 00000001ff3ac2b4ccb57d9fd2d1187475156489ae22337ca866bbafe62991a2
$
```

This program is commonly used as part of `zakurad-log-filter` where hashes will be captured from `zakurad` output.

## zakurad-log-filter

The program is designed to filter the output from the zebra terminal or log file. Each time a hash is seen the script will capture it and get the additional information using `zakurad-hash-lookup`.

Assuming `zakurad`, `zcash-cli`, `zakurad-hash-lookup` and `zakurad-log-filter` are in your path the program can used as:

```sh
$ zakurad -v start | zakurad-log-filter
...
block::Hash("
high: 2800
time: 2016-11-01T16:17:16Z
hash: 00000001ecd754790237618cb79c4cd302e52571ecda7a80e6113c5e423c0e55
prev: 00000003ed8623d9499f4bf80f8bc410066194bf6813762b31560f9319205bf8
next: 00000001436277884eef900772f0fcec9566becccebaab4713fd665b60fab309
"))) max_checkpoint_height=Height(419581)
...
```

## zcash-rpc-diff

This program compares `zakurad` and `zcashd` RPC responses.

Make sure you have zcashd and zakurad installed and synced.

The script:

1. gets the `zakurad` and `zcashd` tip height and network
2. sends the RPC request to both of them using `zcash-cli`
3. compares the responses using `diff`
4. leaves the full responses in files in a temporary directory, so you can check them in detail
5. if possible, compares different RPC methods for consistency

Assuming `zakurad`'s RPC port is 28232, you should be able to run:

```sh
$ zakura-utils/zcash-rpc-diff 28232 getinfo
Checking zakurad network and tip height...
Checking zcashd network and tip height...

Request:
getinfo

Querying zakurad main chain at height 1649797...
Querying zcashd main chain at height 1649797...

Response diff (between zcashd port and port 28232):
--- /run/user/1000/tmp.g9CJecu2Wo/zakurad-main-1649797-getinfo.json      2022-04-29 14:08:46.766240355 +1000
+++ /run/user/1000/tmp.g9CJecu2Wo/zcashd-main-1649797-getinfo.json      2022-04-29 14:08:46.769240315 +1000
@@ -1,4 +1,16 @@
 {
-  "build": "1.0.0-beta.8+54.ge83e93a",
-  "subversion": "/Zebra:1.0.0-beta.8/"
+  "version": 4070050,
+  "build": "v4.7.0-gitian",
+  "subversion": "/MagicBean:4.7.0/",
... more extra zcashd fields ...
 }
```

Sometimes zcashd will have extra fields (`+`) or different data (`-` and `+`).
And sometimes it will have the same data, but in a different order.

The script will warn you if the heights or networks are different,
then display the results of querying the mismatched node states.

The script accepts any RPC, with any number of arguments.
If a node doesn't implement an RPC, the script will exit with an error.

#### Configuration

The script uses the configured `zcash-cli` RPC port,
and the `zakurad` port supplied on the command-line.

It doesn't actually check what kind of node it is talking to,
so you can compare two `zcashd` or `zakurad` nodes if you want.
(Just edit the `zcash.conf` file used by `zcash-cli`, or edit the script.)

You can override the binaries the script calls using these environmental variables:

- `$ZCASH_CLI`
- `$DIFF`
- `$JQ`
