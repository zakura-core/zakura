# Profiling and Benchmarking Zakura

Let's have a look at how we can inspect and evaluate Zakura's performance.

## Profiling

To profile Zakura, you can use the [samply](https://github.com/mstange/samply)
profiler. Once you have it installed, you can run:

```bash
cargo build --profile profiling
sudo samply record ./target/profiling/zakurad
```

You can then press `Ctrl+c`, and the profiler will instruct you to navigate your
web browser to <http://127.0.0.1:3000> where you can snoop around the call stack
to see where Zakura loafs around the most.

## Benchmarking

To benchmark Zakura consistently, you'll need to suppress unpredictable latency
fluctuations coming from the network. You can do that by running two Zakura
instances on your localhost: one that is synced up to the block height of your
interest, and one that will connect only to the first instance.

To spin up the synced instance, you can run

```bash
cargo run --release -- --config /path/to/zakurad-synced.toml
```

with `/path/to/zakurad-synced.toml` pointing to the config below

```toml
# Config for a synced Zakura instance in a network-suppressed setup.

[network]
cache_dir = true
max_connections_per_ip = 1000
network = "Mainnet"

[state]
# cache_dir = "/path/to/.cache/zakura-synced"
delete_old_database = true
ephemeral = false

[sync]
checkpoint_verify_concurrency_limit = 1000
parallel_cpu_threads = 0
```

This config makes Zakura, among other things, accept quick reconnections from the
same IP, which will be localhost. Without this setup, Zakura would quickly start
treating localhost as a bad peer, and refuse subsequent reconnections, not
knowing that they come from separate instances.

To spin up the second instance, first compile the version you want to benchmark:

```bash
cargo build --release
```

and run

```bash
time ./target/release/zakurad --config /path/to/zakurad-isolated.toml
```

with `path/to/zakurad-isolated.toml` pointing to the config below

```toml
# Config for an isolated Zakura instance in a network-suppressed setup.

[network]
listen_addr = "127.0.0.1:8234"
cache_dir = false
crawl_new_peer_interval = "10d"

initial_mainnet_peers = [
    "127.0.0.1:8233",
]

initial_testnet_peers = [
    "127.0.0.1:8233",
]

max_connections_per_ip = 1
network = "Mainnet"
peerset_initial_target_size = 1

[state]
# cache_dir = "/path/to/.cache/zakura-isolated"
delete_old_database = true
ephemeral = true
debug_stop_at_height = 10_000
```

This config makes Zakura:

1. connect only to the synced instance via localhost;
2. use an ephemeral state, so you can run the benchmark again;
3. stop at height 10_000.

Note that:

- You can adjust both configs to your liking.
- You can repeat the `time` command as many times as you need.
- You can use the two-instance setup for profiling as well.
- You will likely need to rebuild Zakura for each change you want to benchmark.
  To speed the build process up, you can link RocksDB dynamically, as described
  in the section on [building Zakura][building-zakura].

[building-zakura]: ../user/install.md#building-zakura
