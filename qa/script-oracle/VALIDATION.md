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
| Clippy for the complete workspace, all targets | Passed with `-D warnings` |
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
The [benchmark estimates](benchmark-results.json) retain the final run's means,
batch medians, and confidence intervals.
The benchmark uses 30 samples, a one-second warmup, and two-second measurement
windows. It passes inputs through `black_box` and checks fixture results before
measurement. It does not measure transaction hashing, full blocks, allocations,
binary size, or transaction latency percentiles.

The [remaining gates](README.md#gates-before-merge) block merge and rollout.
The private vendored package and workspace patch do not complete crates.io
package delivery. No package publication, merge, or deployment has occurred.

| Execution case | Rust mean | C++ mean | Rust change |
| --- | ---: | ---: | ---: |
| p2pkh | 27.946 µs | 26.789 µs | +4.3% |
| p2sh-p2pkh | 28.080 µs | 27.257 µs | +3.0% |
| p2sh-multisig | 54.542 µs | 53.552 µs | +1.8% |
| late-fail | 27.259 µs | 26.782 µs | +1.8% |
| malformed | 0.073 µs | 0.026 µs | +184.8% |

The signed-script means increased by roughly 2–4% on this host.
The malformed-script mean increased from 25.5 ns to 72.7 ns (about 2.85×).
The first exploratory run also showed this early-failure slowdown.
That result requires review under the proposed performance gate before merge.
The raw counting means for one complete push followed by CHECKSIG were about
3–4 ns in Rust, versus 422–431 ns in C++ for 520/521-byte pushes and 7.60 µs
for a 10,000-byte push. These counts exclude transaction script collection and
hashing. These results establish no whole-node speedup.

CI exposed integration gaps after the first push. The dependency check disables
Cargo tree color. Node tests retain the adapter as a dev-dependency. Docker
mounts the owned crate. Generated semver rustdoc workspaces apply the owned
patch. The C++ counting wrapper suppresses
an unused-parameter warning from its pinned header. Markdown and spelling checks
pass locally. The source spelling check excludes the pinned upstream source and
binary fuzz corpora. The adapter bumps to 5.0.0 for its error and sigops API changes.
Consensus bumps to 11.0.0 because it removes the public FFI error conversion.
These version bumps do not publish either crate or complete package delivery.

Semver checks built the current and published baseline versions of both crates.
The tool accepted the declared major bumps and skipped breaking-change lints
for those major releases. Workspace Clippy checks all node test targets.
