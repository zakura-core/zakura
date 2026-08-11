//! Clears the retired header-root authentication frontier.
//!
//! Header-time root authentication now records its verdicts as header-chain auxiliary
//! evidence, so the single-row `header_root_auth_frontier` column family has no reader and no
//! writer. The column family is no longer in [`STATE_COLUMN_FAMILIES_IN_CODE`], so new
//! databases never create it; this upgrade clears the row a database written by an earlier
//! version still carries. RocksDB keeps the empty column family itself, because a database is
//! always opened with every column family already on disk.
//!
//! [`STATE_COLUMN_FAMILIES_IN_CODE`]: crate::service::finalized_state::STATE_COLUMN_FAMILIES_IN_CODE

use crossbeam_channel::{Receiver, TryRecvError};
use semver::Version;
use zakura_chain::block::Height;

use crate::service::finalized_state::ZakuraDb;

use super::{CancelFormatChange, DiskFormatUpgrade};

/// First format that no longer maintains a header-root authentication frontier.
pub(crate) const UPGRADE_VERSION: Version = Version::new(28, 1, 4);

/// The retired header-root authentication frontier removal upgrade.
pub struct Upgrade;

impl DiskFormatUpgrade for Upgrade {
    fn version(&self) -> Version {
        UPGRADE_VERSION
    }

    fn description(&self) -> &'static str {
        "clear the retired header-root authentication frontier"
    }

    fn run(
        &self,
        _initial_tip_height: Height,
        db: &ZakuraDb,
        cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<(), CancelFormatChange> {
        check_cancelled(cancel_receiver)?;
        if let Err(error) = db.clear_retired_header_root_auth_frontier() {
            panic!("retired header-root frontier removal failed: {error}");
        }
        check_cancelled(cancel_receiver)?;
        Ok(())
    }

    fn validate(
        &self,
        db: &ZakuraDb,
        _cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<Result<(), String>, CancelFormatChange> {
        Ok(if db.has_retired_header_root_auth_frontier_row() {
            Err("the retired header-root authentication frontier still has a row".to_string())
        } else {
            Ok(())
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
        service::finalized_state::{
            disk_format::RawBytes,
            zakura_db::commitment_roots_db::RETIRED_HEADER_ROOT_AUTH_FRONTIER, DiskWriteBatch,
            WriteDisk, STATE_COLUMN_FAMILIES_IN_CODE,
        },
        Config,
    };
    use zakura_chain::parameters::Network;

    /// Opens a database that also carries the retired column family, the way one written by an
    /// earlier version does.
    fn db_with_retired_frontier_cf() -> ZakuraDb {
        ZakuraDb::new(
            &Config::ephemeral(),
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &Network::Mainnet,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string)
                .chain([RETIRED_HEADER_ROOT_AUTH_FRONTIER.to_string()]),
            false,
        )
        .expect("ephemeral database opens")
    }

    #[test]
    fn a_retired_frontier_row_is_cleared_and_the_clear_is_idempotent() {
        let db = db_with_retired_frontier_cf();
        let cf = db
            .db()
            .cf_handle(RETIRED_HEADER_ROOT_AUTH_FRONTIER)
            .expect("the fixture created the retired column family");
        let mut batch = DiskWriteBatch::new();
        batch.zs_insert(
            &cf,
            RawBytes::new_raw_bytes(Vec::new()),
            RawBytes::new_raw_bytes(vec![2, 7, 7, 7]),
        );
        db.write_batch(batch).expect("retired frontier row writes");
        assert!(db.has_retired_header_root_auth_frontier_row());

        let (_cancel_tx, cancel_rx) = crossbeam_channel::bounded(1);
        DiskFormatUpgrade::run(&Upgrade, Height::MIN, &db, &cancel_rx)
            .expect("clearing the frontier is not cancelled");
        DiskFormatUpgrade::run(&Upgrade, Height::MIN, &db, &cancel_rx)
            .expect("re-running the clear is idempotent");

        assert!(!db.has_retired_header_root_auth_frontier_row());
        assert_eq!(
            DiskFormatUpgrade::validate(&Upgrade, &db, &cancel_rx),
            Ok(Ok(()))
        );
    }

    #[test]
    fn a_database_without_the_retired_column_family_needs_no_work() {
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
        assert!(
            db.db()
                .cf_handle(RETIRED_HEADER_ROOT_AUTH_FRONTIER)
                .is_none(),
            "a new database must not create the retired column family"
        );

        let (_cancel_tx, cancel_rx) = crossbeam_channel::bounded(1);
        DiskFormatUpgrade::run(&Upgrade, Height::MIN, &db, &cancel_rx)
            .expect("a missing column family is not an error");
        assert_eq!(
            DiskFormatUpgrade::validate(&Upgrade, &db, &cancel_rx),
            Ok(Ok(()))
        );
    }
}
