//! Common [`zakura_test`](crate) types, traits, and functions.

pub use crate::command::{test_cmd, CommandExt, TestChild};
pub use std::process::Stdio;

pub use crate::report::{Section, SectionExt};
pub use eyre;
pub use eyre::Result;
pub use proptest::prelude::*;
