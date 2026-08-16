//! Hash-scoped auxiliary authentication evidence.

use crate::BodyWorkOwner;

use super::super::auxiliary::{AuxAuthentication, PreparedAuxDelivery};

/// Auxiliary metadata authentication update.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuxEvidence {
    /// Current work owner.
    pub owner: BodyWorkOwner,
    /// One or two exact deliveries and their immutable provenance.
    pub deliveries: Vec<PreparedAuxDelivery>,
    /// New authentication state applied atomically to every named delivery.
    pub authentication: AuxAuthentication,
}
