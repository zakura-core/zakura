# Validation

This file records the evidence that the Rust `zakura-script` adapter matches
the C++ adapter it replaced. `README.md` describes the oracle and the
commands.

## Seeded tests

`cargo test --release` runs these tests in about 3 seconds on a 24-core
machine:

| Test | Inputs |
| --- | --- |
| `vectors::upstream_vectors_agree` | Every `zcash_script` 0.4.5 test vector |
| `vectors::audit_vectors_agree` | The semantic audit's vectors, plus negative-zero results |
| `sigops::push_encoding_boundaries` | Push encodings at each length boundary |
| `sigops::seeded_random_scripts` | Seeded random scripts |
| `generated::generated_scripts_agree` | 20,000 generated spends |
| `generated::generated_transactions_agree` | 5,000 generated V4, V5, and V6 transactions |

The generated tests fail unless at least 10% of cases are accepted, 40% are
rejected, and 5% are accepted after a successful signature check.

## Mutation testing

Each row injects a bug into `crates/zakura-script/src/lib.rs` and runs
`cargo test --release --no-fail-fast`. A seeded test failed for every
mutation:

| Mutation | Detected by |
| --- | --- |
| Accept hash type `0x00` as `SIGHASH_ALL` in ZIP 244 | `generated_transactions_agree` |
| Treat sequence `0xfffffffe` as final | `generated_transactions_agree` |
| Mask the V4 hash type byte with `0x83` | `generated_transactions_agree` |
| Read the lock time as `i32` | `generated_transactions_agree` |
| Treat a `PUSHDATA2` of more than 520 bytes as truncated when counting sigops | `generated_transactions_agree`, `push_encoding_boundaries` |
| Pass an empty script code to the V4 sighash | `generated_transactions_agree` |
| Accept `SIGHASH_SINGLE` when the input index equals the output count | `generated_transactions_agree` |
| Drop `CHECKLOCKTIMEVERIFY` from the verification flags | `generated_transactions_agree` |

The first mutation survived until the generator signed rejected hash types
over the digest a wrongly accepting verifier would compute, and chose
non-canonical hash types more often. `generated_scripts_agree` evaluates
scripts with fixed flags and its own sighash callback, so only the
transaction test exercises the adapter's flags, lock-time conversion, and
sighash dispatch.

## Fuzz campaign

The campaign ran each target with the command in `README.md`: libFuzzer fork
mode with 7 workers, `-max_len=4096`, and an empty starting corpus. The
binaries were built with `cargo fuzz build -s none -a` from commit
`9adae7ff4`; later commits do not change the generators or targets.

The campaign was planned for 3.5 hours, or 24.5 CPU-hours, per target. It was
stopped after 84 minutes because it used most of the shared machine:

| Target | Wall time | CPU-hours | Executions | Corpus | Crashes, timeouts, OOMs |
| --- | --- | --- | --- | --- | --- |
| `script` | 5,039 s | 9.8 | 35,425,718 | 2,194 inputs, 8.8 MB | 0 |
| `sigops` | 5,037 s | 9.8 | 1,042,387,120 | 1,212 inputs, 4.9 MB | 0 |
| `transaction` | 5,042 s | 9.8 | 14,104,569 | 2,567 inputs, 11 MB | 0 |
| Total | | 29.4 | 1,091,917,407 | | 0 |

New coverage had nearly stopped when the campaign ended. Over the last 77
minutes, `script` grew from 1,784 to 1,791 edges, `sigops` stayed at 466, and
`transaction` grew from 2,881 to 2,894.

The corpora stay out of git. Continue the campaign from them with the same
command.

## Coverage

`cargo fuzz coverage` replayed each final corpus. Line coverage:

| Source | `script` | `transaction` | `sigops` |
| --- | --- | --- | --- |
| `zcash_script` `interpreter.rs` | 90.6% | 84.8% | 22.2% |
| `zcash_script` `script/iter.rs` | 77.7% | 94.7% | 48.9% |
| `zcash_script` `signature.rs` | 78.4% | 78.4% | 0% |
| `zcash_script` `num.rs` | 81.0% | 81.0% | 0% |
| `zcash_script` `opcode/mod.rs` | 66.1% | 71.8% | 24.6% |
| `zcash_script` `external/pubkey.rs` | 70.0% | 70.0% | 0% |
| `zakura-script` `src/lib.rs` | not called | 82.7% | 24.5% |
| `src/baseline.rs` | not called | 83.0% | 11.3% |

The `sigops` target covers `legacy_sigop_count` and `skip_push_data`
completely. No target reaches these lines:

- Interpreter paths behind flags Zcash does not set: `STRICTENC`, `LOW_S`,
  `MINIMALDATA`, `CLEANSTACK`, `SIGPUSHONLY`, `NULLDUMMY`, and
  `DISCOURAGE_UPGRADABLE_NOPS`.
- `num.rs` overflow and `i64::MIN` cases for numbers longer than 5 bytes. The
  interpreter never parses numbers that long.
- Script display, disassembly, and error normalization helpers that
  verification does not call.
- The adapters' `Display` implementations, unused accessors, and coinbase
  arms. Both adapters refuse to prepare a coinbase transaction, so verification
  never reaches an input of one.

The generators rarely reach the negative-zero branch of `cast_to_bool`: the
final `script` corpus executes it 43 times. `audit_vectors_agree` pins it with
four vectors.

## Divergences

None. Neither the seeded tests nor the campaign found an input on which the
Rust and C++ adapters disagree.
