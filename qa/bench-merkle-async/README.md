# Merkle parallelism and async hashing benchmark harness

Benchmark-only harness for comparing three commits:

- A: `d1eb1e2c63577089e749c4f498d2fc9a8e90db55` (PR #2005 base)
- B: `5bd8267ebd822a995f22d8f902ae9c350b266c1a` (PR #2005 head, parallel Merkle levels >= 512)
- C: `4a90894a4ce63246ce46a647652c0895cf6b860c` (serial `FromIterator`, `Root::from_hashes_in_parallel`,
  `spawn_blocking` for blocks with >= 64 transactions)

`hashing_bench.rs` is identical in every arm. Each arm gets a small `arm.rs` shim that copies that
commit's production hashing lines from `SemanticBlockVerifier::call`: `arm-ab/arm.rs` for A and B,
`arm-c/arm.rs` for C, which calls the production `run_block_hashing` boundary.

## Modes

- `BENCH_MODE=merkle`: warm-data microbenchmark of Merkle root construction from precomputed
  transaction hashes (1 to 8,192 leaves). Leaf contents do not affect the cost, so transaction
  mix is irrelevant here. Each arm's root is checked against the serial `FromIterator` root.
- `BENCH_MODE=phase`: the block's transaction-ID, Merkle-check and authorizing-data hashing
  phase on fixture blocks built from mainnet vector transactions (made unique by expiry height,
  with a matching header Merkle root). It prints:
  - `VERIFY`: a digest of the transaction IDs, the auth-data root, and the error verdicts for a
    corrupted Merkle root and a duplicated transaction;
  - `BREAKDOWN`: per-step CPU time on a plain thread;
  - `PHASE`: phase latency on a 1-worker Tokio runtime, with a 1 ms ticker task on the same
    runtime. The ticker's maximum gap shows how long unrelated async work was held off. The
    `busy_rayon_synthetic` rows add deliberately injected contention: a thread keeps every
    global Rayon worker busy with 2 ms jobs.

## Running

```sh
./install.sh <arm checkout> ab   # or c
cargo test --release --locked -p zakura-consensus --lib --no-run
RAYON_NUM_THREADS=4 BENCH_ARM=A BENCH_MODE=merkle \
  <test binary> block::tests::hashing_bench::run --ignored --exact --nocapture --test-threads 1
```

`run.sh` alternates the arms for a number of rounds and writes raw CSV-like lines to stdout.

`run.sh` filters each run through `sed`, so a failing run shows up only as missing rows. Check the
counts before using a result: per round and arm, 18 `MERKLE` rows, or 7 `VERIFY`, 7 `BREAKDOWN`
and 14 `PHASE` rows. `summarize.py <merkle.csv> <phase.csv>` asserts identical Merkle roots across
arms and prints the median, range and sample count for each cell.

Limitations: phase fixtures are synthetic. Their expiry heights are rewritten, so signatures and
proofs would not validate. The mixed fixtures place shielded transactions first. Ticker gaps
include Tokio's roughly 2 ms timer floor. None of this measures full block verification or relay.
