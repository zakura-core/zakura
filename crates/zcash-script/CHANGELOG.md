# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- next-header -->

## [Unreleased] - ReleaseDate

## [0.4.5] - 2026-05-29

### Changed

- Changed `sig_op_count()` to match zcashd

## [0.4.4] - 2026-04-17

## Added

- `zcash_script::signature::HashType::raw_bits()`

## [0.4.3], [0.5.2] - 2026-02-23

### Added

- `zcash_script::script::Code::is_unspendable()`
- `as_str()` and `req_sigs()` for `ScriptKind`

### Changed

- Bumped `bindgen` dependency from `>= 0.69.5` to `>= 0.71`.
- The `zcash_script::opcode::PossiblyBad::parse` fn is now `pub`.

### Fixed

- `SmallValue::to_asm()` fixed to match zcashd

## [0.4.2], [0.5.1] - 2025-10-24

### Fixed
- Fixed `no-std` support which was broken by [#256].

## [0.4.1] - 2025-10-23

### Added
- `zcash_script::descriptor` module, providing the subset of [BIP 380] Output
  Script Descriptors that are valid to use within the Zcash ecosystem ([#273]).
- `zcash_script::script::Asm` trait, enabling `asm`-style string representations
  similar to zcashd ([#256]).
- Implemented the `Display` trait for scripts and opcodes, using `Asm` ([#256]).

[#256]: https://github.com/ZcashFoundation/zcash_script/pull/256
[#273]: https://github.com/ZcashFoundation/zcash_script/pull/273
[BIP 380]: https://github.com/bitcoin/bips/blob/master/bip-0380.mediawiki

## [0.4.0] - 2025-09-25

This is a significant change, with a new Rust API that isn’t made to be swappable with the C++ API. The previous `ZcashScript`-based API is in the new `libzcash_script` crate.

### Removed

- The `cxx` module, `ZcashScript` trait, `ComparisonInterpreter`, `CxxInterpreter` structs, `Error` enum, `cxx_rust_comparison_interpreter` function, and `external-secp` feature have all moved to the new `libzcash_script` crate
- `check_verify_callback` has moved to the `testing` module in the new `libzcash_script` crate
- `normalize_error` has been removed, but there is a similar new function `testing::normalize_err` in the new `libzcash_script` crate
- `rust_interpreter` has been removed, but there is a similar `RustInterpreter` struct in the new `libzcash_script` crate
- `interpreter::State` no longer implements `Clone`
- All of the stepwise functionality (`ComparisonStepEvaluator`, `StepResults`, `StepwiseInterpreter`, `interpreter::DefaultStepEvaluator` (its re-export `DefaultStepEvaluator`), `stepwise_verify`, `interpreter::eval_step`, and `interpreter::StepFn`) has been removed
- The `interpreter::Stack` and `interpreter::State` operations have been removed
- `interpreter::CallbackTransactionSignatureChecker::verify_signature` has been removed
- `interpreter::CallbackTransactionSignatureChecker` is no longer re-exported as `CallbackTransactionSignatureChecker`
- `interpreter::SignatureChecker::check_sig` and `interpreter::SignatureChecker::check_lock_time` no longer have default impls


### Changed

- `interpreter::HashType` (its re-export `HashType`), `interpreter::InvalidHashType`, `interpreter::SignedOutputs` (and its re-export `SignedOutputs`) are now available only from the `signature` module
- `interpreter::BaseSignatureChecker` is now named `interpreter::NullSignatureChecker`
- `interpreter::eval_script` has been replaced by the `script::Code::eval` method
- `interpreter::verify_script` has been replaced by the `script::Raw::eval` method
- `interpterer::VerificationFlags` (and its re-export `VerificationFlags`) is now available only as `interpreter::Flags`
- `script_error::ScriptNumError` is now `num::Error`
- `script_error::ScriptError` is now `script::Error`, and its constructors have been split between that type, `opcode::Error`, `interpreter::Error`, and `signature::Error`. Many of the constructors of these types have changed – taking parameters, or combined or split into different specific cases.

### Added

- `no-std` support (`alloc` still required), by disabling the new default-enabled `std` feature flag.
- `interpreter::Error`, `opcode::Error`, and `signature::Error` have been added as subsets of `script::Error`
- script types `Script` and `script::Raw` have been added
- `script::Component` has been added
- The `pv` and `op` modules have been added
- The `pattern` module has been added with a number of functions for building scripts components
- The `script::Evaluable` and `opcode::Evaluable` traits have been added, with implementations for the relevant types
- script and component types have an `annotate` method for collecting potential issues with a script

## [0.3.2](https://github.com/ZcashFoundation/zcash_script/compare/v0.3.1...v0.3.2) - 2025-06-24

- bump secp256k1 back to 0.29 (#234)
- add: Implement `Display` and derive `Error` traits for error enums (#231)

## [0.3.1](https://github.com/ZcashFoundation/zcash_script/compare/v0.3.0...v0.3.1) - 2025-05-17

### Other

- Remove extra `&` from `SighashCalculator` ([#216](https://github.com/ZcashFoundation/zcash_script/pull/216))

## [0.3.0](https://github.com/ZcashFoundation/zcash_script/compare/v0.2.0...v0.3.0) - 2025-05-13

- Another major API update. The previous C++ functions were moved to the `cxx`
  module. A new Rust implementation has been added. In the root of the
  crate there are now "Intepreters" which allow choosing which implementation
  to use.

- Eliminate `ScriptNum` ([#208](https://github.com/ZcashFoundation/zcash_script/pull/208))
- Add various stepwise functionality ([#204](https://github.com/ZcashFoundation/zcash_script/pull/204))
- Addressing post-hoc PR feedback on #174 ([#197](https://github.com/ZcashFoundation/zcash_script/pull/197))
- Expose `ScriptError` on C++ side ([#195](https://github.com/ZcashFoundation/zcash_script/pull/195))
- Change type of `lock_time` parameter ([#190](https://github.com/ZcashFoundation/zcash_script/pull/190))
- Initial Rust implementation ([#174](https://github.com/ZcashFoundation/zcash_script/pull/174))
- Add a basic rust-toolchain.toml ([#176](https://github.com/ZcashFoundation/zcash_script/pull/176))
- Address Str4d’s comments on #171 ([#175](https://github.com/ZcashFoundation/zcash_script/pull/175))
- Provide a Rustier wrapper for zcash_script ([#171](https://github.com/ZcashFoundation/zcash_script/pull/171))

## [0.2.0] - 2024-06-10

- Major API update. Most functions have been removed and a new callback-based
  function was introduced. See `depend/zcash/src/script/zcash_script.h` for
  documentation. This allows removing all dependencies from this crate, greatly
  simplifying its maintenance.

## [0.1.16] - 2024-04-26

### Changed
- Update `depend/zcash` to version 5.9.0 which includes updated dependencies

## [0.1.15] - 2024-04-19

### Changed
- Update `depend/zcash` to version 5.8.0 which includes updated dependencies
- Update other dependencies to match Zebra
- Restore Windows support

## [0.1.14] - 2023-10-18

### Changed
- Update `depend/zcash` to version 5.7.0 which includes updated dependencies
- Update other dependencies to match Zebra
- Update the `build.rs` to handle subdirs

## [0.1.13] - 2023-06-29

### Changed
- Update `depend/zcash` to version 5.6.1 which includes updated dependencies
- Update other dependencies to match Zebra

## [0.1.12] - 2023-05-03

### Changed
- Update `depend/zcash` to version 5.5.0 which includes updated dependencies
    - This includes additional `zcashd` C++ and Rust code, and its dependencies
    - Fix code generation C++ header paths to avoid conflicts
- Update other dependencies to match Zebra

### Fixed
- Improve error reporting in `build.rs`

## [0.1.11] - 2023-02-24

### Changed
- Make dependencies automatically upgrade to match its parent project if a later minor version is already included
- Upgrade to Rust 2021 edition

## [0.1.10] - 2023-02-23

### Changed
- Updated `bindgen` version from 0.60.1 to 0.64.0

## [0.1.9] - 2023-02-23

### Changed
- Updated `depend/zcash` to version 5.4.0 which includes updated dependencies

## [0.1.8] - 2022-10-31

### Changed
- Updated `depend/zcash` to version 5.3.0 which includes updated dependencies
- Updated `bindgen` to version 0.60.1

## [0.1.7] - 2022-08-31

### Changed
- Updated `depend/zcash` to version 5.2.0 which includes updated dependencies
-
## [0.1.6] - 2022-05-16

### Changed
- Exposed precomputation API
- Updated `depend/zcash` to version 5.0.0 which includes API for V5 transactions

## [0.1.5] - 2020-12-09
### Changed
- Updated `depend/zcash` to new version including a precompute API

## [0.1.4] - 2020-11-17
### Changed
- switched from bindgen `0.55` to bindgen `0.54` to avoid a dependency
  conflict with `rocksdb`

## [0.1.3] - 2020-10-09
### Changed
- Enabled endomorphism optimization
- Changed zcash dependency from a submodule to a subtree

### Fixed
- Can now run `cargo publish` and `cargo release` without errors

## [0.1.2] - 2020-09-21
### Removed
- dependency on `color-eyre` in build.rs

## [0.1.1] - 2020-09-15
### Changed
- enabled the `parallel` feature of `cc` to enable parallel compilation

### Security
- Updated `bindgen` to a non yanked version

<!-- next-url -->
[Unreleased]: https://github.com/ZcashFoundation/zcash_script/compare/zcash_script-v0.4.5...HEAD
[0.4.5]: https://github.com/ZcashFoundation/zcash_script/compare/zcash_script-v0.4.4...zcash_script-v0.4.5
[0.4.4]: https://github.com/ZcashFoundation/zcash_script/compare/zcash_script-v0.4.2...zcash_script-v0.4.4
[0.4.3]: https://github.com/ZcashFoundation/zcash_script/compare/zcash_script-v0.4.2...zcash_script-v0.4.3
[0.4.2]: https://github.com/ZcashFoundation/zcash_script/compare/zcash_script-v0.4.1...zcash_script-v0.4.2
[0.4.1]: https://github.com/ZcashFoundation/zcash_script/compare/v0.4.0...zcash_script-v0.4.1
[0.4.0]: https://github.com/ZcashFoundation/zcash_script/compare/v0.3.2...v0.4.0
[0.3.2]: https://github.com/ZcashFoundation/zcash_script/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/ZcashFoundation/zcash_script/compare/v0.2.0...v0.3.1
[0.2.0]: https://github.com/ZcashFoundation/zcash_script/compare/v0.1.16...v0.2.0
[0.1.16]: https://github.com/ZcashFoundation/zcash_script/compare/v0.1.15...v0.1.16
[0.1.15]: https://github.com/ZcashFoundation/zcash_script/compare/v0.1.14...v0.1.15
[0.1.14]: https://github.com/ZcashFoundation/zcash_script/compare/v0.1.13...v0.1.14
[0.1.13]: https://github.com/ZcashFoundation/zcash_script/compare/v0.1.12...v0.1.13
[0.1.12]: https://github.com/ZcashFoundation/zcash_script/compare/v0.1.11...v0.1.12
[0.1.11]: https://github.com/ZcashFoundation/zcash_script/compare/v0.1.10...v0.1.11
[0.1.10]: https://github.com/ZcashFoundation/zcash_script/compare/v0.1.9...v0.1.10
[0.1.9]: https://github.com/ZcashFoundation/zcash_script/compare/v0.1.8...v0.1.9
[0.1.8]: https://github.com/ZcashFoundation/zcash_script/compare/v0.1.7...v0.1.8
[0.1.7]: https://github.com/ZcashFoundation/zcash_script/compare/v0.1.6...v0.1.7
[0.1.6]: https://github.com/ZcashFoundation/zcash_script/compare/v0.1.5...v0.1.6
[0.1.5]: https://github.com/ZcashFoundation/zcash_script/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/ZcashFoundation/zcash_script/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/ZcashFoundation/zcash_script/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/ZcashFoundation/zcash_script/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/ZcashFoundation/zcash_script/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/ZcashFoundation/zcash_script/releases/tag/v0.1.0
