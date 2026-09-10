use std::sync::Arc;

use zakura_chain::block;

/// Ordered, nonempty canonical headers to validate against one exact parent frontier.
#[derive(Copy, Clone, Debug)]
pub struct HeaderBatchInput<'a> {
    /// Headers in exact parent-first wire order.
    pub headers: &'a [Arc<block::Header>],
}

impl<'a> HeaderBatchInput<'a> {
    /// Construct an input over one complete target response assembled by the requester.
    pub const fn new(headers: &'a [Arc<block::Header>]) -> Self {
        Self { headers }
    }
}
