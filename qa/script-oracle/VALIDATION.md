# Draft migration validation — October 1, 2026

The candidate starts from Zakura `8014ead3ff13913a6447d94db70e110254598b99`.
The owned script source starts from upstream
`cc4ec7ee8b96103586588e51e0f7cb3ebad4d944` (0.4.5).
The oracle lockfile pins `libzcash_script` 0.1.0.

The host runs Linux 7.2.6 on an AMD Ryzen AI 9 HX 370 with 24 logical CPUs.
Checks use rustc 1.97.1 (`8bab26f4f`, July 14, 2026) and GCC 16.2.1.
Fuzzing uses nightly-2026-07-15 and cargo-fuzz 0.13.2.
Other builds ran on the host, so these runs do not qualify deployment performance.

| Check | Result |
| --- | --- |
| `cargo test --locked -p zcash_script -p zakura-script` | 50 passed |
| Isolated oracle tests | 42 passed; includes deterministic 20,000-case corpus |
| Chain transaction unit tests | 93 passed |
| Consensus unit tests | 274 passed; 1 ignored |
| RPC `methods::types::` unit tests | 43 passed; 1 ignored |
| Clippy for owned script, adapter, consensus, all targets | Passed with `-D warnings` |
| Clippy for oracle workspace, all targets | Passed with `-D warnings` |
| Node release-feature check | Passed |
| Node release-feature + portable check | Passed |
| Both release dependency graphs | One owned script crate; no `libzcash_script` |
| Workspace and oracle formatting | Passed |
| Replay command | 1 fixture, 1 accepted input, 1 legacy sigop, 0 P2SH sigops |

The script tests sign all 256 V4 raw hash bytes.
They sign all six canonical hash types for V5 and V6.
They check callback failure followed by OP_NOT, stale callback state, previous
output alignment, oversized pushes, truncated pushes, accurate multisig counts,
coinbase sigops, and P2SH accounting.
The oracle compares execution acceptance and exact legacy/accurate/P2SH counts.
Interpreter errors retain the existing `TransactionError::Script` classification.

Each final smoke campaign used `-max_total_time=30`, `-rss_limit_mb=4096`, and
`-max_len=22000`. LibFuzzer reported 31 seconds for each target.

| Fuzz target | Executions | Result |
| --- | ---: | --- |
| Execution | 1,346,305 | No mismatch or crash |
| Legacy/accurate/P2SH counts | 2,553,642 | No mismatch or crash |
| Transaction callbacks | 89,302 | No mismatch or crash |

These smoke runs do not satisfy the proposed 24 CPU-hour budgets.
The replay fixture supplies the existing test's previous output.
Its block hash and height are unavailable.
No historical range or full semantic block replay has run.

The Criterion release benchmark compares both implementations in one binary.
The final benchmark results will be recorded after the run completes.
The benchmark uses 30 samples, a one-second warmup, and two-second measurement
windows. It passes inputs through `black_box` and checks fixture results before
measurement. It does not measure transaction hashing, full blocks, allocations,
binary size, or transaction latency percentiles.

The [remaining gates](README.md#gates-before-merge) block merge and rollout.
The private vendored package and workspace patch do not complete crates.io
package delivery. No package publication, merge, or deployment has occurred.
