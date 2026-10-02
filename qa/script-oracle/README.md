# Script oracle

This package compares Zakura's transparent script verification with the C++
adapter it replaced. It is a separate workspace because it builds zcashd's C++
interpreter, which the node no longer depends on.

## What it compares

- **Candidate:** `zakura-script` from this repository, which evaluates scripts
  with the Rust `zcash_script` 0.4.5 interpreter and counts legacy sigops in
  Rust.
- **Baseline:** `src/baseline.rs`, the previous `zakura-script` adapter frozen
  at commit `3ef9f45f2`. It evaluates scripts with the C++ interpreter from
  `libzcash_script` 0.1.0 and counts legacy sigops with zcashd's
  `GetSigOpCount(false)`.

Each check in `src/lib.rs` panics when the two disagree:

| Check | Agreement required |
| --- | --- |
| `check_script` | Rust and C++ return the same result for one script pair, after normalizing errors to the cases C++ reports. Each interpreter gets the sighash callback its production adapter builds. |
| `check_script_sigops` | Legacy sigop counts are equal. Accurate counts of scripts of at most 520 bytes, which policy uses for P2SH redeem scripts, equal zcashd's. |
| `check_p2sh_sigops` | P2SH sigop counts equal the baseline's. Where zcashd's count differs, the scriptSig must fail evaluation in both interpreters. |
| `check_transaction` | Both adapters prepare the transaction or both refuse. Every input has the same verdict. Legacy and P2SH sigop counts are equal. |

`src/generate` turns fuzzer bytes into inputs for these checks. Signatures are
real ECDSA signatures over the digest the verifier computes, so generated
inputs reach successful signature checks as well as each failure path:

- Spends come from P2PK, P2PKH, and multisig templates, optionally behind
  P2SH and CLTV, and from an opcode grammar. The grammar reaches the push,
  op-count, stack-depth, and script-size limits, dead branches, and disabled
  and invalid opcodes. Signature and public-key encodings include high S,
  scalar overflow, strict-DER violations, and hybrid, off-curve, and
  out-of-range keys.
- Transactions are V4, V5, and V6, signed with Zakura's sighasher, and
  include coinbase transactions.

## Run the tests

The C++ build needs `CXXFLAGS="-include cstdint"` with GCC 15.

```sh
cargo test --release
```

- `tests/sigops.rs` covers push-encoding boundaries and seeded random scripts.
- `tests/vectors.rs` covers the upstream `zcash_script` vectors and the
  semantic audit's vectors.
- `tests/generated.rs` runs each generator on seeded inputs. It fails if
  generation stops producing rejected spends, accepted spends, or accepted
  spends that need a successful signature check.

## Fuzz

The targets are `script`, `sigops`, and `transaction`. Run them from this
directory, because `cargo-fuzz` otherwise looks for `fuzz/` under the
repository root:

```sh
cargo +nightly-2026-07-15 fuzz run --fuzz-dir . script -- -max_len=4096
```

The generators need inputs of a few kilobytes to build multi-input
transactions and long scripts. The campaign in `VALIDATION.md` used:

```sh
cargo +nightly-2026-07-15 fuzz build --fuzz-dir . -s none -a
target/x86_64-unknown-linux-gnu/release/script corpus/script \
  -fork=7 -ignore_crashes=1 -max_total_time=12600 -max_len=4096 \
  -len_control=20 -rss_limit_mb=2048 -timeout=20 -artifact_prefix=artifacts/script/
```

Reproduce a crash with
`cargo +nightly-2026-07-15 fuzz run --fuzz-dir . <target> artifacts/<target>/<crash>`.

## Replay a finalized state

`src/bin/replay.rs` runs every transaction in a Zakura finalized state through
both adapters. It opens the state read-only and needs the `replay` feature:

```sh
cargo run --release --features replay --bin replay -- \
  <state>/v30/mainnet --network mainnet --checkpoint mainnet.checkpoint
```

Any input that a finalized chain contains but either adapter rejects is a
finding. A restart with the same checkpoint file resumes the run.

## Measure coverage

```sh
cargo +nightly-2026-07-15 fuzz coverage --fuzz-dir . script corpus/script
```

Then report the interpreter and adapter sources with the toolchain's
`llvm-cov`:

```sh
llvm-cov report \
  target/x86_64-unknown-linux-gnu/coverage/x86_64-unknown-linux-gnu/release/script \
  -instr-profile=coverage/script/coverage.profdata \
  $(find ~/.cargo/registry/src/*/zcash_script-0.4.5/src -name '*.rs') \
  "$(git rev-parse --show-toplevel)/crates/zakura-script/src/lib.rs"
```
