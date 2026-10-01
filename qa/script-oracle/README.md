# Rust script compatibility oracle

This workspace compares the owned Rust interpreter with `libzcash_script` 0.1.0.
Its lockfile pins the C++ package and registry checksum.
Production builds do not depend on this workspace.

`src/baseline.rs` freezes the Zakura adapter at
`8014ead3ff13913a6447d94db70e110254598b99`.
The snapshot removes feature selection and selects C++ directly.
The snapshot retains the random-hash shim for C++ callback failure.
The candidate returns `None` directly when its callback fails.
The transaction tests compare acceptance through both adapters.
They permit diagnostic differences because consensus classifies every script
failure through `TransactionError::Script`.

The wrapper in `cxx/count.cpp` exposes the original CScript accurate and P2SH
counting methods. [Header provenance](cxx/UPSTREAM.md) records their source.
The wrapper adds no counting logic.
Tests compare legacy, accurate, and P2SH counts over a deterministic corpus.
Execution tests use the production `P2SH | CHECKLOCKTIMEVERIFY` flags.
The imported upstream vectors also exercise their specified flags.

## Commands

Run these commands from the repository root with Rust 1.97.1 or later.
Set `CARGO_TARGET_DIR` under `$TMPDIR` to keep build output on disk.

```bash
export CARGO_TARGET_DIR="$TMPDIR/zakura-script-validation"
cargo test --locked -p zcash_script -p zakura-script
cargo test --locked --manifest-path qa/script-oracle/Cargo.toml
python3 scripts/check-script-dependencies.py
cargo bench --locked --manifest-path qa/script-oracle/Cargo.toml --bench script
```

The benchmark compares both interpreters in one release binary.
It measures script execution with a fixed hash callback and raw sigop counting.
It includes P2PKH, P2SH, multisig, malformed scripts, and late failure.
It does not measure transaction hashing, full blocks, allocations, or latency
percentiles for individual transactions.

## Fuzzing

Use cargo-fuzz 0.13.2 and nightly-2026-07-15.
Each target fails on an acceptance or count mismatch.
The execution target always returns a fixed hash from its callback to avoid the
original C++ callback failure defect.
The transaction target uses transaction hashes and previous outputs through the
frozen production adapter and candidate adapter.
The sigops target compares legacy, accurate, and P2SH counts.

```bash
export CARGO_TARGET_DIR="$TMPDIR/zakura-script-fuzz"
export CXXFLAGS='-include cstdint'
cargo +nightly-2026-07-15 fuzz run --fuzz-dir qa/script-oracle \
  --features fuzzing execution -- -max_total_time=86400 -rss_limit_mb=4096 -max_len=22000
cargo +nightly-2026-07-15 fuzz run --fuzz-dir qa/script-oracle \
  --features fuzzing sigops -- -max_total_time=86400 -rss_limit_mb=4096 -max_len=2000000
cargo +nightly-2026-07-15 fuzz run --fuzz-dir qa/script-oracle \
  --features fuzzing transaction_callbacks -- -max_total_time=86400 -rss_limit_mb=4096 -max_len=22000
```

These commands configure time budgets. They do not establish completed CPU-hour
coverage. Record execution time, CPU use, corpus hashes, and coverage separately.
Cargo-fuzz instruments Rust code. This setup does not instrument the C++ oracle
for coverage or claim a C++ memory audit.
Retain and minimize every failure before changing the baseline or candidate.

## Replay

The replay command accepts one JSON object per line.
Each object supplies `transaction` as serialized hex, `network_upgrade` as the
Zakura enum name, `previous_outputs` in input order, and `source` provenance.
Each previous output supplies its zatoshi `amount` and hex `script_pubkey`.
The command compares every transparent input and both sigop counts.
The command records rejection when both adapters reject an input.
A disagreement fails the command.

```bash
cargo run --locked --manifest-path qa/script-oracle/Cargo.toml --example replay \
  -- qa/script-oracle/fixtures/script-tx.jsonl
```

The supplied fixture comes from the existing V4 script regression.
Its height and block hash are unavailable.
It establishes the replay command's operation and does not establish a historical
mainnet or testnet range.
The command does not perform full semantic block verification or derive upgrade
context from height. Full block replay remains a separate qualification gate.

## Gates before merge

- Resolve package delivery. The owned crate keeps the upstream name and version
  for local type unification, and sets `publish = false`. The workspace patch
  does not propagate to crates.io consumers. Publishing needs an owned package
  and coordinated primitives/transparent dependencies.
- Complete the fuzz budgets and archive minimized failures and coverage.
- Replay historical mainnet and testnet ranges with exact previous outputs,
  heights, branch IDs, block hashes, and declared omissions.
- Compare full semantic block results and block sigop totals.
- Complete the supported upgrade boundary matrix, including experimental modes.
- Qualify full transaction/block performance, p50/p95/p99 latency, allocations,
  adversarial costs, build time, memory, and binary size on pinned hardware.
- Check the delivery path and supported platforms. Audit native dependencies and
  interpreter maintenance requirements.

Before release, revert the migration commit to restore the pinned C++ adapter.
The migration introduces no database format or consensus activation change.
