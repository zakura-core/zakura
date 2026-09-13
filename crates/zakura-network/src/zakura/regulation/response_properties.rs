//! Check the shared response rules with different message shapes.
//!
//! The lifecycle model races receiver replacement with publication and writing.
//! Discovery uses its real codec. The subscription is a test adapter for future
//! messages with renewable credit. Each adapter owns its identity and ending rules.

use super::*;
use proptest::prelude::*;

mod discovery;
mod lifecycle;
mod subscription;
