# Zebra Utilities

Tools for maintaining and testing Zebra:

- [zakura-checkpoints](#zakura-checkpoints)
- [zakurad-hash-lookup](#zakurad-hash-lookup)
- [zakurad-log-filter](#zakurad-log-filter)
- [zcash-rpc-diff](#zcash-rpc-diff)
- [scanning-results-reader](#scanning-results-reader)

Binaries are easier to use if they are located in your system execution path.

## zakura-checkpoints

This command generates a list of zebra checkpoints, and writes them to standard output. Each checkpoint consists of a block height and hash.

#### Offline Mainnet export (release-state pipeline)

Mainnet checkpoint updates flow through the release-state pipeline: the publisher host runs
offline mode against a synced Mainnet **archive** state database. One run produces the whole
coupled release state for the last emitted checkpoint — the checkpoint list, the VCT frontier,
the completed-subtree roots, and the historical frontier grid. Build with the offline feature
and run:

```sh
cargo install --locked --features zakura-checkpoints-offline --git https://github.com/zakura-core/zakura zakura-utils

zakura-checkpoints \
  --state-cache-dir /path/to/archive-zakura-cache \
  --full-list \
  --mainnet-frontier-output /out/mainnet-frontier.bin \
  --mainnet-subtree-output /out/mainnet-treestate-subtrees.bin \
  --mainnet-frontier-grid-output /out/mainnet-frontier-grid.bin \
  > /out/main-checkpoints.txt
```

Offline mode reads canonical hashes and `BlockInfo` sizes straight from the finalized
database, which it opens as a read-only RocksDB secondary, so the node does not have to be
stopped. `--full-list` prints the embedded checkpoint list before the new entries, making
stdout a complete replacement `main-checkpoints.txt`. The subtree export keeps the roots
already embedded in the binary and adds later roots retained by the database.

The three artifact output flags are one set: supply all of them or none, so a replacement
checkpoint list can never ship without its coupled state. Checkpoints, the frontier, and the
subtree roots come out of pruned state too, but the frontier grid covers the heights below the
checkpoint, which a pruned database no longer holds — hence the archive requirement. A legacy
archive node generates the grid entirely from stored trees; a fast-synced one replays its
absent band instead, which takes hours on Mainnet. Either way the default cost-weighted
spacing reads every block body once to place its entries, so expect a long batch run: the
first full Mainnet run took 81 minutes on a legacy archive node and produced 5,472 entries
in 4.8 MB, without replaying a single block. `--frontier-grid-target-cost-ms` tunes the
grid's per-entry cost budget (default 2000); `--frontier-grid-spacing` produces a uniform grid
and is not recommended.

`--mainnet-frontier-grid-input <path>` resumes from a previously published grid instead of
rebuilding it. Its entries are re-checked against the database and then carried forward verbatim,
so the run scans only the blocks above the last carried entry and the output is a prefix-extension
of the input by construction. Use it for every run after the first: a full walk stays O(chain)
forever, while a resumed run costs only the new tail.

To produce a grid for a checkpoint the binary already ships — backfilling the one artifact a
committed release state is missing, without advancing the checkpoint list — run the grid
alone:

```sh
zakura-checkpoints \
  --state-cache-dir /path/to/archive-zakura-cache \
  --mainnet-frontier-grid-output /out/mainnet-frontier-grid.bin \
  --mainnet-frontier-grid-checkpoint <embedded checkpoint height>
```

That mode emits no checkpoints and refuses the other artifact outputs, which only exist for a
newly selected checkpoint. The height must be one of the embedded checkpoints, and the database
must agree with the embedded list at it.

Do not hand-append RPC-mode output to the Mainnet list: pipeline
updates must stay on the deterministic selection grid (see
`docs/design/verified-commitment-trees.md`, "Mainnet release-state pipeline").

The RPC modes below remain for Testnet updates and diagnostics.

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
zakura-checkpoints --last-checkpoint $(tail -1 crates/zakura-chain/src/parameters/checkpoint/main-checkpoints.txt | cut -d" " -f1) | tee --append crates/zakura-chain/src/parameters/checkpoint/main-checkpoints.txt &
zakura-checkpoints --last-checkpoint $(tail -1 crates/zakura-chain/src/parameters/checkpoint/test-checkpoints.txt | cut -d" " -f1) -- -testnet | tee --append crates/zakura-chain/src/parameters/checkpoint/test-checkpoints.txt &
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

For more details about checkpoint lists, see the [`zakura-checkpoints` README.](https://github.com/zakura-core/zakura/tree/main/crates/zakura-chain/src/parameters/checkpoint/README.md)

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
$ crates/zakura-utils/zcash-rpc-diff 28232 getinfo
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
