//! Reconstructs the durable highest completed checkpoint.

use crossbeam_channel::{Receiver, TryRecvError};
use semver::Version;
use zakura_chain::block::Height;

use crate::service::finalized_state::ZakuraDb;

use super::{CancelFormatChange, DiskFormatUpgrade};

/// First format with a durable highest completed checkpoint.
pub(crate) const UPGRADE_VERSION: Version = Version::new(28, 0, 2);

/// The highest-completed-checkpoint metadata upgrade.
pub struct Upgrade;

impl DiskFormatUpgrade for Upgrade {
    fn version(&self) -> Version {
        UPGRADE_VERSION
    }

    fn description(&self) -> &'static str {
        "reconstruct the durable highest completed checkpoint"
    }

    fn run(
        &self,
        _initial_tip_height: Height,
        db: &ZakuraDb,
        cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<(), CancelFormatChange> {
        check_cancelled(cancel_receiver)?;
        if let Err(error) = db.reconstruct_and_persist_highest_completed_checkpoint() {
            panic!("highest completed checkpoint migration failed closed: {error}");
        }
        check_cancelled(cancel_receiver)?;
        Ok(())
    }

    fn validate(
        &self,
        db: &ZakuraDb,
        _cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<Result<(), String>, CancelFormatChange> {
        match db.validate_highest_completed_checkpoint() {
            Ok(_) => Ok(Ok(())),
            Err(error) => Ok(Err(error.to_string())),
        }
    }
}

fn check_cancelled(
    cancel_receiver: &Receiver<CancelFormatChange>,
) -> Result<(), CancelFormatChange> {
    match cancel_receiver.try_recv() {
        Err(TryRecvError::Empty) => Ok(()),
        _ => Err(CancelFormatChange),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{service::finalized_state::FinalizedState, CheckpointVerifiedBlock, Config};
    use zakura_chain::{block::Block, parameters::Network, serialization::ZcashDeserializeInto};

    #[test]
    fn reconstructs_missing_row_from_canonical_state() {
        let mut state = FinalizedState::new(
            &Config::ephemeral(),
            &Network::Mainnet,
            #[cfg(feature = "elasticsearch")]
            false,
        )
        .expect("ephemeral state opens");
        let genesis = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
            .zcash_deserialize_into::<Arc<Block>>()
            .expect("genesis deserializes");
        state
            .commit_finalized_direct(
                CheckpointVerifiedBlock::from(genesis).into(),
                None,
                None,
                "highest-completed-checkpoint migration fixture",
            )
            .expect("genesis commits");
        state.db.delete_highest_completed_checkpoint_for_test();

        assert_eq!(
            state
                .db
                .try_highest_completed_checkpoint()
                .expect("missing row is valid"),
            None
        );
        state
            .db
            .reconstruct_and_persist_highest_completed_checkpoint()
            .expect("highest completed checkpoint reconstructs");
        assert_eq!(
            state
                .db
                .validate_highest_completed_checkpoint()
                .expect("reconstructed highest completed checkpoint validates")
                .expect("non-empty state has a highest completed checkpoint")
                .height,
            Height::MIN
        );
    }
}
