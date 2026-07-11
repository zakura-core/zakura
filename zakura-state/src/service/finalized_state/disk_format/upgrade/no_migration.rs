//! An implementation of [`DiskFormatUpgrade`] for marking the database as upgraded to a new format version.

use crossbeam_channel::Receiver;

use semver::Version;
use zakura_chain::block::Height;

use crate::service::finalized_state::ZakuraDb;

use super::{CancelFormatChange, DiskFormatUpgrade};

/// Implements [`DiskFormatUpgrade`] for in-place upgrades that do not involve any migration
/// of existing data into the new format.
pub struct NoMigration {
    description: &'static str,
    version: Version,
}

impl NoMigration {
    /// Creates a new instance of the [`NoMigration`] upgrade.
    pub fn new(description: &'static str, version: Version) -> Self {
        Self {
            description,
            version,
        }
    }
}

impl DiskFormatUpgrade for NoMigration {
    fn version(&self) -> Version {
        self.version.clone()
    }

    fn description(&self) -> &'static str {
        self.description
    }

    #[allow(clippy::unwrap_in_result)]
    fn run(
        &self,
        _initial_tip_height: Height,
        _db: &ZakuraDb,
        _cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<(), CancelFormatChange> {
        Ok(())
    }

    fn needs_migration(&self) -> bool {
        false
    }
}
