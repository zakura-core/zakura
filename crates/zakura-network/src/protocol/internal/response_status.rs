//! The status of a response to an inventory request.

use std::fmt;

#[cfg(any(test, feature = "proptest-impl"))]
use proptest_derive::Arbitrary;

use InventoryResponse::*;

/// A generic peer inventory response status.
///
/// `Available` is used for inventory that is present in the response,
/// and `Missing` is used for inventory that is missing from the response.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(any(test, feature = "proptest-impl"), derive(Arbitrary))]
pub enum InventoryResponse<A, M> {
    /// An available inventory item.
    Available(A),

    /// A missing inventory item.
    Missing(M),
}

impl<A, M> fmt::Display for InventoryResponse<A, M> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(self.command())
    }
}

impl<A, M> InventoryResponse<A, M> {
    /// Returns the response status type as a string.
    pub fn command(&self) -> &'static str {
        match self {
            InventoryResponse::Available(_) => "Available",
            InventoryResponse::Missing(_) => "Missing",
        }
    }

    /// Returns true if the inventory item was available.
    pub fn is_available(&self) -> bool {
        matches!(self, Available(_))
    }

    /// Returns true if the inventory item was missing.
    pub fn is_missing(&self) -> bool {
        matches!(self, Missing(_))
    }

    /// Converts from `&InventoryResponse<A, M>` to `InventoryResponse<&A, &M>`.
    pub fn as_ref(&self) -> InventoryResponse<&A, &M> {
        match self {
            Available(item) => Available(item),
            Missing(item) => Missing(item),
        }
    }
}

impl<A: Clone, M: Clone> InventoryResponse<A, M> {
    /// Get the available inventory item, if present.
    pub fn available(&self) -> Option<A> {
        if let Available(item) = self {
            Some(item.clone())
        } else {
            None
        }
    }

    /// Get the missing inventory item, if present.
    pub fn missing(&self) -> Option<M> {
        if let Missing(item) = self {
            Some(item.clone())
        } else {
            None
        }
    }
}
