//! Check the real serving task while storage, encoding and writes are held open.
//!
//! Cancelling a request must stop future output, but it must not free a worker
//! slot that still has a running job or an unfinished write. These tests hold
//! those operations at known points, then release them and require recovery.

use super::*;
use crate::zakura::testkit::await_until;
use zakura_test::{allocations::measure, execution::ExecutionProbe};

const DEADLINE: Duration = Duration::from_secs(5);

mod fixtures;
use fixtures::*;
mod failures;
mod ownership;
mod ranges;

mod load;
