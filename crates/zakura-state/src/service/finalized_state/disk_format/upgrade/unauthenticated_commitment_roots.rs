//! Removes commitment-root rows that no committed body or trusted header proves.
//!
//! An earlier fast path persisted peer-supplied roots before authentication. The database could
//! therefore contain unverified rows above its finalized body tip. This upgrade removes those
//! rows before the committer or peer-serving code can read them.

use crossbeam_channel::{Receiver, TryRecvError};
use semver::Version;
use zakura_chain::block::Height;

use crate::service::finalized_state::{DiskWriteBatch, ZakuraDb};

use super::{CancelFormatChange, DiskFormatUpgrade, FormatChangeError};

/// First format where every stored commitment-root row is authenticated before persistence.
pub(crate) const UPGRADE_VERSION: Version = Version::new(28, 1, 5);

/// The unauthenticated commitment-root removal upgrade.
pub struct Upgrade;

#[cfg(test)]
type WriteHook = std::sync::Arc<dyn Fn() -> Result<(), String> + Send + Sync>;

#[cfg(test)]
static WRITE_HOOKS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, WriteHook>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

#[cfg(test)]
struct WriteHookGuard {
    database_path: std::path::PathBuf,
}

#[cfg(test)]
impl Drop for WriteHookGuard {
    fn drop(&mut self) {
        WRITE_HOOKS
            .lock()
            .expect("migration write-hook mutex is not poisoned")
            .remove(&self.database_path);
    }
}

#[cfg(test)]
fn register_write_hook(
    database_path: impl Into<std::path::PathBuf>,
    hook: WriteHook,
) -> WriteHookGuard {
    let database_path = database_path.into();
    let replaced = WRITE_HOOKS
        .lock()
        .expect("migration write-hook mutex is not poisoned")
        .insert(database_path.clone(), hook);
    assert!(
        replaced.is_none(),
        "database already has a migration write hook"
    );
    WriteHookGuard { database_path }
}

#[cfg(test)]
fn run_write_hook(database_path: &std::path::Path) -> Result<(), String> {
    let hook = WRITE_HOOKS
        .lock()
        .map_err(|_| "migration write-hook mutex is poisoned".to_string())?
        .get(database_path)
        .cloned();
    hook.map_or(Ok(()), |hook| hook())
}

/// Drops every commitment-root row above the finalized body tip.
pub(super) fn truncate_to_body_tip(state_database: &ZakuraDb) -> Result<(), rocksdb::Error> {
    let mut batch = DiskWriteBatch::new();
    match state_database.finalized_tip_height() {
        Some(body_tip) => batch.truncate_commitment_roots_after(state_database, body_tip),
        None => batch.truncate_all_commitment_roots(state_database),
    }
    state_database.write_batch(batch)
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
        _initial_finalized_tip_height: Option<Height>,
        state_database: &ZakuraDb,
        cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<(), FormatChangeError> {
        check_cancelled(cancel_receiver)?;
        #[cfg(test)]
        run_write_hook(state_database.path()).map_err(FormatChangeError::MigrationStorage)?;
        truncate_to_body_tip(state_database)
            .map_err(|error| FormatChangeError::MigrationStorage(error.to_string()))?;
        check_cancelled(cancel_receiver)?;
        Ok(())
    }

    fn validate(
        &self,
        state_database: &ZakuraDb,
        _cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<Result<(), String>, FormatChangeError> {
        let body_tip = state_database.finalized_tip_height();
        let first_unproven = match body_tip {
            Some(body_tip) => body_tip.next().ok().and_then(|above| {
                state_database
                    .commitment_roots_by_height_range(above..=Height::MAX)
                    .first()
                    .map(|roots| roots.height)
            }),
            None => state_database
                .has_commitment_root_rows()
                .then_some(Height::MIN),
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
        config::database_format_version_on_disk,
        constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
        service::finalized_state::{FinalizedState, WriteDisk, STATE_COLUMN_FAMILIES_IN_CODE},
        CheckpointVerifiedBlock, Config, StateInitError,
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

    fn persistent_config() -> (tempfile::TempDir, Config) {
        let cache = tempfile::tempdir().expect("temporary cache directory is created");
        let config = Config {
            cache_dir: cache.path().to_owned(),
            ephemeral: false,
            debug_skip_non_finalized_state_backup_task: true,
            ..Config::default()
        };
        (cache, config)
    }

    fn open_persistent(config: &Config) -> Result<ZakuraDb, StateInitError> {
        ZakuraDb::new(
            config,
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &Network::Mainnet,
            false,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        )
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
        assert!(matches!(
            DiskFormatUpgrade::validate(&Upgrade, &db, &cancel_rx),
            Ok(Ok(()))
        ));
    }

    #[test]
    fn startup_runs_cutover_without_a_finalized_tip() {
        let (_cache, config) = persistent_config();
        let db = open_persistent(&config).expect("fixture database opens");
        db.insert_zakura_header_commitment_roots([roots_at(Height(1))])
            .expect("legacy root fixture writes");
        db.update_format_version_on_disk(&Version::new(28, 1, 4))
            .expect("fixture version writes");
        drop(db);

        let reopened = open_persistent(&config).expect("startup migration succeeds");
        assert!(!reopened.has_commitment_root_rows());
        assert_eq!(
            reopened
                .format_version_on_disk()
                .expect("the migrated version is readable"),
            Some(state_database_format_version_in_code())
        );
    }

    #[test]
    fn historical_versions_run_the_new_cutover() {
        use std::sync::Arc;
        use zakura_chain::serialization::ZcashDeserializeInto;
        use zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES;

        for old_version in [Version::new(28, 0, 2), Version::new(28, 1, 3)] {
            let (_cache, config) = persistent_config();
            let mut state = FinalizedState::new(&config, &Network::Mainnet)
                .expect("fixture finalized state opens");
            let genesis: Arc<block::Block> = BLOCK_MAINNET_GENESIS_BYTES
                .zcash_deserialize_into()
                .expect("mainnet genesis deserializes");
            state
                .commit_finalized_direct(
                    CheckpointVerifiedBlock::from(genesis).into(),
                    None,
                    None,
                    "commitment-root migration test",
                )
                .expect("genesis commits");
            state
                .db
                .insert_zakura_header_commitment_roots([roots_at(Height(1)), roots_at(Height(2))])
                .expect("legacy header-ahead roots write");
            state
                .db
                .update_format_version_on_disk(&old_version)
                .expect("fixture version writes");
            drop(state);

            let reopened = open_persistent(&config).expect("historical database upgrades");
            assert!(reopened.commitment_roots(Height::MIN).is_some());
            assert_eq!(reopened.commitment_roots(Height(1)), None);
            assert_eq!(reopened.commitment_roots(Height(2)), None);
            assert_eq!(
                reopened
                    .format_version_on_disk()
                    .expect("the migrated version is readable"),
                Some(state_database_format_version_in_code())
            );
        }
    }

    #[test]
    fn storage_failure_preserves_the_old_version_and_startup_retries() {
        let (_cache, config) = persistent_config();
        let db = open_persistent(&config).expect("fixture database opens");
        db.insert_zakura_header_commitment_roots([roots_at(Height(1))])
            .expect("legacy root fixture writes");
        db.update_format_version_on_disk(&Version::new(28, 1, 4))
            .expect("fixture version writes");
        let db_path = db.path().to_owned();
        drop(db);

        let hook = register_write_hook(
            db_path,
            std::sync::Arc::new(|| Err("injected migration write failure".to_string())),
        );
        let Err(StateInitError::DatabaseFormatUpgrade { source, .. }) = open_persistent(&config)
        else {
            panic!("the injected storage failure must stop startup with a migration error");
        };
        assert!(matches!(
            source.downcast_ref::<FormatChangeError>(),
            Some(FormatChangeError::MigrationStorage(message))
                if message == "injected migration write failure"
        ));
        assert_eq!(
            database_format_version_on_disk(
                &config,
                STATE_DATABASE_KIND,
                state_database_format_version_in_code().major,
                &Network::Mainnet,
            )
            .expect("the preserved version is readable"),
            Some(Version::new(28, 1, 4))
        );

        drop(hook);
        let reopened = open_persistent(&config).expect("the retry succeeds");
        assert!(!reopened.has_commitment_root_rows());
        assert_eq!(
            reopened
                .format_version_on_disk()
                .expect("the migrated version is readable"),
            Some(state_database_format_version_in_code())
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
        DiskFormatUpgrade::run(&Upgrade, Some(Height::MIN), &db, &cancel_rx)
            .expect("first cutover is not cancelled");
        DiskFormatUpgrade::run(&Upgrade, Some(Height::MIN), &db, &cancel_rx)
            .expect("re-running cutover is idempotent");

        assert!(matches!(
            DiskFormatUpgrade::validate(&Upgrade, &db, &cancel_rx),
            Ok(Ok(()))
        ));
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
