//! Removes legacy unauthenticated root rows and initializes their durable frontier.

use crossbeam_channel::{Receiver, TryRecvError};
use semver::Version;
use zakura_chain::block::Height;

use crate::service::finalized_state::{DiskWriteBatch, HeaderRootAuthFrontierError, ZakuraDb};

use super::{CancelFormatChange, DiskFormatUpgrade};

/// First format where commitment-root rows are authenticated before persistence.
pub(crate) const UPGRADE_VERSION: Version = Version::new(28, 0, 2);

/// The verified header-root persistence boundary upgrade.
pub struct Upgrade;

impl Upgrade {
    fn run_cutover(&self, db: &ZakuraDb) -> Result<(), HeaderRootAuthFrontierError> {
        let mut batch = DiskWriteBatch::new();
        if let Some(body_tip) = db.finalized_tip_height() {
            batch.truncate_commitment_roots_after(db, body_tip);
            db.prepare_legacy_header_root_auth_frontier_from_body_tip(&mut batch)?;
        } else {
            batch.truncate_all_commitment_roots(db);
            batch.delete_header_root_auth_frontier(db);
        }
        db.write_batch(batch)?;
        Ok(())
    }
}

impl DiskFormatUpgrade for Upgrade {
    fn version(&self) -> Version {
        UPGRADE_VERSION
    }

    fn description(&self) -> &'static str {
        "remove unauthenticated header roots and initialize their verified frontier"
    }

    fn run(
        &self,
        _initial_tip_height: Height,
        db: &ZakuraDb,
        cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<(), CancelFormatChange> {
        check_cancelled(cancel_receiver)?;
        if let Err(error) = self.run_cutover(db) {
            panic!("header-root authentication cutover failed closed: {error}");
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
        constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
        service::finalized_state::{WriteDisk, STATE_COLUMN_FAMILIES_IN_CODE},
        Config,
    };
    use zakura_chain::{
        block, parallel::commitment_aux::BlockCommitmentRoots, parameters::Network,
    };

    #[test]
    fn no_tip_cutover_purges_legacy_roots() {
        let db = ZakuraDb::new(
            &Config::ephemeral(),
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &Network::Mainnet,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        )
        .expect("ephemeral database opens");
        db.insert_zakura_header_commitment_roots([BlockCommitmentRoots {
            height: Height(1),
            sapling_root: Default::default(),
            orchard_root: Default::default(),
            ironwood_root: Default::default(),
            sapling_tx: 0,
            orchard_tx: 0,
            ironwood_tx: 0,
            auth_data_root: block::merkle::AuthDataRoot::from([0; 32]),
        }])
        .expect("legacy root fixture writes");
        assert!(db.tip().is_none());

        Upgrade
            .run_cutover(&db)
            .expect("no-tip cutover purges unauthenticated roots");

        assert!(!db.has_commitment_root_rows());
        assert!(!db.has_header_root_auth_frontier_row());
        assert!(matches!(db.validate_header_root_auth_state(), Ok(None)));
    }

    #[test]
    fn tip_cutover_is_idempotent_on_rerun() {
        use crate::service::finalized_state::disk_format::upgrade::DiskFormatUpgrade;
        use std::sync::Arc;
        use zakura_chain::serialization::ZcashDeserializeInto;
        use zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES;

        let db = ZakuraDb::new(
            &Config::ephemeral(),
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &Network::Mainnet,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        )
        .expect("ephemeral database opens");

        let genesis: Arc<block::Block> = BLOCK_MAINNET_GENESIS_BYTES
            .zcash_deserialize_into()
            .expect("mainnet genesis deserializes");
        let hash_by_height = db
            .db()
            .cf_handle("hash_by_height")
            .expect("hash_by_height exists");
        let height_by_hash = db
            .db()
            .cf_handle("height_by_hash")
            .expect("height_by_hash exists");
        let block_header_by_height = db
            .db()
            .cf_handle("block_header_by_height")
            .expect("block_header_by_height exists");
        let mut batch = DiskWriteBatch::new();
        batch.zs_insert(&hash_by_height, Height::MIN, genesis.hash());
        batch.zs_insert(&height_by_hash, genesis.hash(), Height::MIN);
        batch.zs_insert(&block_header_by_height, Height::MIN, &genesis.header);
        // Body-derived root at the tip, plus unauthenticated header-ahead rows.
        batch.insert_body_derived_commitment_roots(
            &db,
            &BlockCommitmentRoots {
                height: Height::MIN,
                sapling_root: Default::default(),
                orchard_root: Default::default(),
                ironwood_root: Default::default(),
                sapling_tx: 0,
                orchard_tx: 0,
                ironwood_tx: 0,
                auth_data_root: block::merkle::AuthDataRoot::from([0; 32]),
            },
        );
        db.write_batch(batch).expect("genesis tip fixture writes");
        db.insert_zakura_header_commitment_roots([
            BlockCommitmentRoots {
                height: Height(1),
                sapling_root: Default::default(),
                orchard_root: Default::default(),
                ironwood_root: Default::default(),
                sapling_tx: 0,
                orchard_tx: 0,
                ironwood_tx: 0,
                auth_data_root: block::merkle::AuthDataRoot::from([0; 32]),
            },
            BlockCommitmentRoots {
                height: Height(2),
                sapling_root: Default::default(),
                orchard_root: Default::default(),
                ironwood_root: Default::default(),
                sapling_tx: 0,
                orchard_tx: 0,
                ironwood_tx: 0,
                auth_data_root: block::merkle::AuthDataRoot::from([0; 32]),
            },
        ])
        .expect("legacy header-ahead roots write");

        let (_cancel_tx, cancel_rx) = crossbeam_channel::bounded(1);
        DiskFormatUpgrade::run(&Upgrade, Height::MIN, &db, &cancel_rx)
            .expect("first cutover is not cancelled");
        DiskFormatUpgrade::run(&Upgrade, Height::MIN, &db, &cancel_rx)
            .expect("re-running cutover is idempotent");

        assert_eq!(
            DiskFormatUpgrade::validate(&Upgrade, &db, &cancel_rx),
            Ok(Ok(())),
            "state remains valid after an idempotent re-run"
        );
        assert!(db.commitment_roots(Height::MIN).is_some());
        assert_eq!(db.commitment_roots(Height(1)), None);
        assert_eq!(db.commitment_roots(Height(2)), None);
        let frontier = db
            .try_header_root_auth_frontier()
            .expect("frontier decodes")
            .expect("body tip has a frontier");
        assert_eq!(frontier.confirmed_height(), Height::MIN);
        assert_eq!(frontier.confirmed_hash(), genesis.hash());
    }
}
