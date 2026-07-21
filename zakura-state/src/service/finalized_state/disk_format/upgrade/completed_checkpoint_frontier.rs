//! Reconstructs the completed-checkpoint row added after authenticated roots.

use crossbeam_channel::{Receiver, TryRecvError};
use semver::Version;
use zakura_chain::block::Height;

use crate::service::finalized_state::ZakuraDb;

use super::{CancelFormatChange, DiskFormatUpgrade};

/// First format with a durable completed-checkpoint frontier.
pub(crate) const UPGRADE_VERSION: Version = Version::new(28, 0, 3);

/// The completed-checkpoint metadata upgrade.
pub struct Upgrade;

impl Upgrade {
    fn run_cutover(
        &self,
        db: &ZakuraDb,
    ) -> Result<(), crate::service::finalized_state::HeaderRootAuthFrontierError> {
        db.reconstruct_and_persist_completed_checkpoint()
    }
}

impl DiskFormatUpgrade for Upgrade {
    fn version(&self) -> Version {
        UPGRADE_VERSION
    }

    fn description(&self) -> &'static str {
        "reconstruct the durable completed-checkpoint frontier"
    }

    fn run(
        &self,
        _initial_tip_height: Height,
        db: &ZakuraDb,
        cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<(), CancelFormatChange> {
        check_cancelled(cancel_receiver)?;
        if let Err(error) = self.run_cutover(db) {
            panic!("completed-checkpoint frontier migration failed closed: {error}");
        }
        check_cancelled(cancel_receiver)?;
        Ok(())
    }

    fn validate(
        &self,
        db: &ZakuraDb,
        _cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<Result<(), String>, CancelFormatChange> {
        match db.validate_header_root_auth_state() {
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
    use super::*;
    use crate::{
        service::finalized_state::{FinalizedState, HeaderRootAuthFrontierError},
        CheckpointVerifiedBlock, Config,
    };
    use std::sync::Arc;
    use zakura_chain::{
        block::{Block, Height},
        parameters::Network,
        serialization::ZcashDeserializeInto,
    };

    #[test]
    fn migrates_28_0_2_frontier_without_completed_checkpoint_row() {
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
                "completed-checkpoint migration fixture",
            )
            .expect("genesis commits");
        let db = &state.db;
        db.delete_completed_checkpoint_frontier_for_test();

        assert!(matches!(
            db.try_header_root_auth_frontier(),
            Err(HeaderRootAuthFrontierError::MissingCompletedCheckpoint)
        ));
        Upgrade.run_cutover(db).expect("28.0.2 metadata migrates");
        let restored = db
            .validate_header_root_auth_state()
            .expect("migrated state validates")
            .expect("frontier exists");
        assert_eq!(restored.state().completed_checkpoint_height, Height::MIN);
        assert_eq!(
            restored.state().completed_checkpoint_hash,
            Network::Mainnet.genesis_hash()
        );
    }
}
