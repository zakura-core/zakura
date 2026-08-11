//! Removes commitment-root rows that no committed body or trusted header proves.
//!
//! Before this format, the fast path persisted peer-supplied roots as they arrived, so a
//! database could hold rows above its finalized tip that nothing had verified. Those rows are
//! served to peers and read by the committer, so they are truncated back to the body tip.

use crossbeam_channel::{Receiver, TryRecvError};
use semver::Version;
use zakura_chain::block::Height;

use crate::service::finalized_state::{DiskWriteBatch, ZakuraDb};

use super::{CancelFormatChange, DiskFormatUpgrade};

/// First format where every stored commitment-root row is authenticated before persistence.
pub(crate) const UPGRADE_VERSION: Version = Version::new(28, 0, 2);

/// The unauthenticated commitment-root removal upgrade.
pub struct Upgrade;

/// Drops every commitment-root row above the finalized body tip.
pub(super) fn truncate_to_body_tip(db: &ZakuraDb) -> Result<(), rocksdb::Error> {
    let mut batch = DiskWriteBatch::new();
    match db.finalized_tip_height() {
        Some(body_tip) => batch.truncate_commitment_roots_after(db, body_tip),
        None => batch.truncate_all_commitment_roots(db),
    }
    db.write_batch(batch)
}

impl DiskFormatUpgrade for Upgrade {
    fn version(&self) -> Version {
        UPGRADE_VERSION
    }

    fn description(&self) -> &'static str {
        "remove unauthenticated commitment-root rows above the finalized body tip"
    }

    fn run(
        &self,
        _initial_tip_height: Height,
        db: &ZakuraDb,
        cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<(), CancelFormatChange> {
        check_cancelled(cancel_receiver)?;
        if let Err(error) = truncate_to_body_tip(db) {
            panic!("unauthenticated commitment-root removal failed closed: {error}");
        }
        check_cancelled(cancel_receiver)?;
        Ok(())
    }

    fn validate(
        &self,
        db: &ZakuraDb,
        _cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<Result<(), String>, CancelFormatChange> {
        let body_tip = db.finalized_tip_height();
        let first_unproven = match body_tip {
            Some(body_tip) => body_tip.next().ok().and_then(|above| {
                db.commitment_roots_by_height_range(above..=Height::MAX)
                    .first()
                    .map(|roots| roots.height)
            }),
            None => db.has_commitment_root_rows().then_some(Height::MIN),
        };

        Ok(match first_unproven {
            Some(height) => Err(format!(
                "commitment-root row at {height:?} is above the finalized body tip {body_tip:?}"
            )),
            None => Ok(()),
        })
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

    fn ephemeral_mainnet_db() -> ZakuraDb {
        ZakuraDb::new(
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
        .expect("ephemeral database opens")
    }

    fn roots_at(height: Height) -> BlockCommitmentRoots {
        BlockCommitmentRoots {
            height,
            sapling_root: Default::default(),
            orchard_root: Default::default(),
            ironwood_root: Default::default(),
            sapling_tx: 0,
            orchard_tx: 0,
            ironwood_tx: 0,
            auth_data_root: block::merkle::AuthDataRoot::from([0; 32]),
        }
    }

    #[test]
    fn no_tip_cutover_purges_legacy_roots() {
        let db = ephemeral_mainnet_db();
        db.insert_zakura_header_commitment_roots([roots_at(Height(1))])
            .expect("legacy root fixture writes");
        assert!(db.tip().is_none());

        truncate_to_body_tip(&db).expect("no-tip cutover purges unauthenticated roots");

        assert!(!db.has_commitment_root_rows());
        let (_cancel_tx, cancel_rx) = crossbeam_channel::bounded(1);
        assert_eq!(
            DiskFormatUpgrade::validate(&Upgrade, &db, &cancel_rx),
            Ok(Ok(()))
        );
    }

    #[test]
    fn tip_cutover_keeps_the_body_prefix_and_is_idempotent_on_rerun() {
        use std::sync::Arc;
        use zakura_chain::serialization::ZcashDeserializeInto;
        use zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES;

        let db = ephemeral_mainnet_db();
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
        batch.insert_body_derived_commitment_roots(&db, &roots_at(Height::MIN));
        db.write_batch(batch).expect("genesis tip fixture writes");
        db.insert_zakura_header_commitment_roots([roots_at(Height(1)), roots_at(Height(2))])
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
        assert!(
            db.commitment_roots(Height::MIN).is_some(),
            "the body-derived row at the tip is proven and must survive"
        );
        assert_eq!(db.commitment_roots(Height(1)), None);
        assert_eq!(db.commitment_roots(Height(2)), None);
    }

    #[test]
    fn validation_reports_a_row_left_above_the_body_tip() {
        use std::sync::Arc;
        use zakura_chain::serialization::ZcashDeserializeInto;
        use zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES;

        let db = ephemeral_mainnet_db();
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
        db.write_batch(batch).expect("genesis tip fixture writes");
        db.insert_zakura_header_commitment_roots([roots_at(Height(1))])
            .expect("unproven row writes");

        let (_cancel_tx, cancel_rx) = crossbeam_channel::bounded(1);
        assert!(
            DiskFormatUpgrade::validate(&Upgrade, &db, &cancel_rx)
                .expect("validation is not cancelled")
                .is_err(),
            "a row above the body tip must fail validation, not pass silently"
        );
    }
}
