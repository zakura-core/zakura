//! Backfill the initial Ironwood tree at NU6.3 activation for existing databases.

use crossbeam_channel::Receiver;
use semver::Version;
use zebra_chain::{block::Height, ironwood, parameters::NetworkUpgrade};

use crate::service::finalized_state::{DiskWriteBatch, ZebraDb};

use super::{CancelFormatChange, DiskFormatUpgrade};

/// Implements [`DiskFormatUpgrade`] for adding the Ironwood activation tree.
pub struct Upgrade;

impl DiskFormatUpgrade for Upgrade {
    fn version(&self) -> Version {
        Version::new(28, 0, 0)
    }

    fn description(&self) -> &'static str {
        "add Ironwood value pool, indexes, and activation tree"
    }

    #[allow(clippy::unwrap_in_result)]
    fn run(
        &self,
        initial_tip_height: Height,
        db: &ZebraDb,
        _cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<(), CancelFormatChange> {
        let Some(activation_height) = NetworkUpgrade::Nu6_3.activation_height(&db.network()) else {
            return Ok(());
        };

        if initial_tip_height < activation_height
            || db
                .ironwood_tree_by_height_range(..=activation_height)
                .next()
                .is_some()
        {
            return Ok(());
        }

        let mut batch = DiskWriteBatch::new();
        let ironwood_tree = ironwood::tree::NoteCommitmentTree::default();
        batch.create_ironwood_tree(db, &activation_height, &ironwood_tree);
        db.write_batch(batch)
            .expect("backfilling the Ironwood activation tree should always succeed");

        Ok(())
    }
}
