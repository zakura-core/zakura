//! Created-output membership shared between non-finalized chain snapshots.

use std::sync::Arc;

use zakura_chain::transparent::{OrderedUtxo, OutPoint};

use super::SharedMap;

#[cfg(test)]
mod tests;

/// Created outputs retained by this chain.
/// Spent status is tracked separately.
pub(crate) type CreatedUtxos = SharedMap<OutPoint, Arc<OrderedUtxo>>;
