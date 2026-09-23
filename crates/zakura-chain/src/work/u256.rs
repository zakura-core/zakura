//! Module for a 256-bit big int structure.
// This is a separate module to make it easier to disable clippy because
// it raises a lot of issues in the macro.
#![allow(clippy::all)]
#![allow(clippy::range_plus_one)]
#![allow(clippy::fallible_impl_from)]
// `uint`'s macro expansion trips this lint, and each toolchain knows only one of its two names: https://github.com/rust-lang/rust/issues/79813
#![allow(unknown_lints)]
#![allow(semicolon_in_expressions_from_macros)]
#![allow(semicolon_in_expressions_from_non_local_macros)]
// `construct_uint!` still expands to `*_value` / `std::isize::MAX` helpers that
// newer nightlies mark deprecated. Cap them here so path-dependent rustdoc
// builds under `CARGO_BUILD_WARNINGS=deny` stay clean.
#![allow(deprecated)]
#![allow(missing_docs)]

use uint::construct_uint;

construct_uint! {
    pub struct U256(4);
}
