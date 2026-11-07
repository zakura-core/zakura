//! Evidence that a discovery record passed its author checks.

use std::ops::Deref;

use super::protocol::{DiscoveryRecordError, DiscoveryRecordValidationContext, ZakuraNodeRecord};

/// A node record whose author checks passed: bounds, signature, network,
/// chain, and protocol overlap.
///
/// Only [`VerifiedRecord::verify`] creates one, so holding one proves the
/// checks ran. Discovery stores records in this form. A record identical to a
/// stored one therefore reuses the stored evidence instead of verifying again.
/// Expiry depends on the local clock, so the evidence does not cover it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct VerifiedRecord(ZakuraNodeRecord);

impl VerifiedRecord {
    /// Runs the author checks of `record` against `context`.
    pub(super) fn verify(
        record: ZakuraNodeRecord,
        context: &DiscoveryRecordValidationContext,
    ) -> Result<Self, DiscoveryRecordError> {
        record.verify_authentic(context)?;
        Ok(Self(record))
    }

    /// Reuses this evidence for `record` when `record` is identical to the
    /// verified one. Returns `record` back otherwise.
    ///
    /// The evidence takes `record` itself, so the check copies nothing.
    pub(super) fn covering(&self, record: ZakuraNodeRecord) -> Result<Self, ZakuraNodeRecord> {
        if self.0 == record {
            Ok(Self(record))
        } else {
            Err(record)
        }
    }
}

impl Deref for VerifiedRecord {
    type Target = ZakuraNodeRecord;

    fn deref(&self) -> &ZakuraNodeRecord {
        &self.0
    }
}
