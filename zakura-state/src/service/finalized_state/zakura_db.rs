//! Provides high-level access to the database using [`zakura_chain`] types.
//!
//! This module makes sure that:
//! - all disk writes happen inside a RocksDB transaction, and
//! - format-specific invariants are maintained.
//!
//! # Correctness
//!
//! [`crate::constants::state_database_format_version_in_code()`] must be incremented
//! each time the database format (column, serialization, etc) changes.

use std::{path::Path, sync::Arc};

use crossbeam_channel::bounded;
use semver::Version;

use zakura_chain::{block::Height, diagnostic::task::WaitForPanics, parameters::Network};

use crate::{
    config::database_format_version_on_disk,
    service::finalized_state::{
        disk_db::DiskDb,
        disk_format::{
            block::MAX_ON_DISK_HEIGHT,
            transparent::AddressLocation,
            upgrade::{DbFormatChange, DbFormatChangeThreadHandle},
        },
    },
    write_database_format_version_to_disk, BoxError, Config, StateInitError,
};

use super::disk_format::upgrade::repair_vct_sprout_history;
use super::disk_format::upgrade::restorable_db_versions;

pub mod block;
pub mod chain;
pub(crate) mod commitment_roots_db;
pub mod highest_completed_checkpoint;
pub mod metrics;

/// Minimum number of transactions in a block before the per-transaction batch
/// preparation work (raw-transaction serialization and block-size summation) is
/// run on the rayon pool instead of sequentially.
///
/// Below this, the rayon fork-join cost (waking workers, distributing the items,
/// and joining) outweighs the work itself. The parallel path is a clear win for
/// the large blocks in the heavy shielded region; for the small blocks of the
/// early chain it is pure overhead, so those run sequentially.
pub(crate) const PARALLEL_BLOCK_TX_THRESHOLD: usize = 16;

/// Minimum number of per-input/per-address database reads a block triggers before
/// the committer's UTXO and address-balance lookups are spread across the rayon
/// pool instead of run sequentially on the writer thread.
///
/// In the transparent-heavy ranges these point lookups are cache-served but
/// serial, and dominate the per-block write time while most cores sit idle. Above
/// this count the fan-out pays for the multithreading overhead.
pub(crate) const PARALLEL_BLOCK_READ_THRESHOLD: usize = 16;

pub mod prune;
pub mod rollback;
pub mod shielded;
mod snapshot;
pub mod transparent;

pub(in crate::service) use snapshot::ZakuraDbSnapshot;

#[cfg(any(test, feature = "proptest-impl"))]
// TODO: when the database is split out of zakura-state, always expose these methods.
pub mod arbitrary;

/// Wrapper struct to ensure high-level `zakura-state` database access goes through the correct API.
///
/// `rocksdb` allows concurrent writes through a shared reference,
/// so database instances are cloneable. When the final clone is dropped,
/// the database is closed.
#[derive(Clone, Debug)]
pub struct ZakuraDb {
    // Configuration
    //
    // This configuration cannot be modified after the database is initialized,
    // because some clones would have different values.
    //
    /// The configuration for the database.
    //
    // TODO: move the config to DiskDb
    config: Arc<Config>,

    /// Should format upgrades and format checks be skipped for this instance?
    /// Only used in test code.
    //
    // TODO: move this to DiskDb
    debug_skip_format_upgrades: bool,

    // Owned State
    //
    // Everything contained in this state must be shared by all clones, or read-only.
    //
    /// A handle to a running format change task, which cancels the task when dropped.
    ///
    /// # Concurrency
    ///
    /// This field should be dropped before the database field, so the format upgrade task is
    /// cancelled before the database is dropped. This helps avoid some kinds of deadlocks.
    //
    // TODO: move the generic upgrade code and fields to DiskDb
    format_change_handle: Option<DbFormatChangeThreadHandle>,

    /// The inner low-level database wrapper for the RocksDB database.
    db: DiskDb,
}

#[derive(Clone, Copy)]
enum DbOpenMode {
    Writable,
    ReadOnly,
    VctSproutValidation,
}

impl DbOpenMode {
    fn is_read_only(self) -> bool {
        !matches!(self, Self::Writable)
    }

    fn enforces_vct_repair_guard(self) -> bool {
        !matches!(self, Self::VctSproutValidation)
    }
}

impl ZakuraDb {
    /// Opens or creates the database at a path based on the kind, major version and network,
    /// with the supplied column families, preserving any existing column families,
    /// and returns a shared high-level typed database wrapper.
    ///
    /// If `debug_skip_format_upgrades` is true, don't do any format upgrades or format checks.
    ///
    /// This is used by tests and offline tools that have already checked the exact database
    /// format version before opening the database.
    //
    // TODO: rename to StateDb and remove the db_kind and column_families_in_code arguments
    #[allow(clippy::unwrap_in_result)]
    pub fn new(
        config: &Config,
        db_kind: impl AsRef<str>,
        format_version_in_code: &Version,
        network: &Network,
        debug_skip_format_upgrades: bool,
        column_families_in_code: impl IntoIterator<Item = String>,
        read_only: bool,
    ) -> Result<ZakuraDb, StateInitError> {
        let open_mode = if read_only {
            DbOpenMode::ReadOnly
        } else {
            DbOpenMode::Writable
        };

        Self::new_with_vct_repair_guard(
            config,
            db_kind,
            format_version_in_code,
            network,
            debug_skip_format_upgrades,
            column_families_in_code,
            open_mode,
        )
    }

    /// Opens a read-only database for the explicit VCT Sprout-history audit.
    ///
    /// Unlike normal read-only callers, the audit must inspect databases that predate the repair
    /// format. The returned database remains read-only; only the startup rejection of an
    /// unrepaired VCT database is skipped.
    /// This method is temporary and can be removed after the VCT Sprout-history repair is complete.
    pub(crate) fn new_for_vct_sprout_history_validation(
        config: &Config,
        db_kind: impl AsRef<str>,
        format_version_in_code: &Version,
        network: &Network,
        column_families_in_code: impl IntoIterator<Item = String>,
    ) -> Result<ZakuraDb, StateInitError> {
        Self::new_with_vct_repair_guard(
            config,
            db_kind,
            format_version_in_code,
            network,
            false,
            column_families_in_code,
            DbOpenMode::VctSproutValidation,
        )
    }

    #[allow(clippy::unwrap_in_result)]
    fn new_with_vct_repair_guard(
        config: &Config,
        db_kind: impl AsRef<str>,
        format_version_in_code: &Version,
        network: &Network,
        debug_skip_format_upgrades: bool,
        column_families_in_code: impl IntoIterator<Item = String>,
        open_mode: DbOpenMode,
    ) -> Result<ZakuraDb, StateInitError> {
        let read_only = open_mode.is_read_only();

        // A read-only secondary follows another process's primary database and must never delete
        // it, whereas an ephemeral database deletes its files on drop, so the two modes are
        // mutually exclusive. Reject the combination up front, before the read-only branch below
        // probes the (irrelevant) cache directory, so this configuration error surfaces as
        // `ReadOnlyEphemeralConflict` regardless of whether that directory happens to exist.
        // `DiskDb::new` re-checks the same invariant for callers that don't open through here.
        if read_only && config.ephemeral {
            return Err(StateInitError::ReadOnlyEphemeralConflict);
        }

        // A read-only secondary instance must never modify the primary's cache directory, so it
        // skips the post-major-upgrade DB reuse (which can create directories and rename the
        // on-disk database) and reads the on-disk format version directly. The cache directory is
        // checked for readability first, so a missing or unreadable directory returns a typed
        // `ReadOnlyCacheDirUnreadable` error here instead of panicking on the version-file read.
        let disk_version = if read_only {
            DiskDb::check_cache_dir_readable(&config.cache_dir)?;

            database_format_version_on_disk(config, &db_kind, format_version_in_code.major, network)
                .expect("unable to read database format version file")
        } else {
            DiskDb::try_reusing_previous_db_after_major_upgrade(
                &restorable_db_versions(),
                format_version_in_code,
                config,
                &db_kind,
                network,
            )
            .or_else(|| {
                database_format_version_on_disk(
                    config,
                    &db_kind,
                    format_version_in_code.major,
                    network,
                )
                .expect("unable to read database format version file")
            })
        };
        let disk_version_before_open = disk_version.clone();

        // Log any format changes before opening the database, in case opening fails.
        let format_change = DbFormatChange::open_database(format_version_in_code, disk_version);

        // A read-only secondary instance cannot create a database. If there's no database on
        // disk, fail with a clear, actionable error instead of silently "creating" one.
        //
        // The read-write path is unaffected: creating a new database is the correct behavior there.
        if read_only && format_change.is_newly_created() {
            let db_path = config.db_path(&db_kind, format_version_in_code.major, network);
            return Err(StateInitError::ReadOnlyDatabaseNotFound { path: db_path });
        }

        let upgrades_explicitly_disabled = debug_skip_format_upgrades;

        // Format upgrades try to write to the database, so we always skip them
        // if `read_only` is `true`.
        //
        // Offline tools can also skip them after checking the exact database format version.
        let debug_skip_format_upgrades = read_only || debug_skip_format_upgrades;

        // Open the low-level database and do initial checks.
        //
        // After the database directory is created, a newly created database temporarily
        // changes to the default database version. Then we set the correct version in the
        // upgrade thread. We need to do the version change in this order, because the version
        // file can only be changed while we hold the RocksDB database lock.
        let disk_db = DiskDb::new(
            config,
            db_kind,
            format_version_in_code,
            network,
            column_families_in_code,
            read_only,
        )?;

        let mut db = ZakuraDb {
            config: Arc::new(config.clone()),
            debug_skip_format_upgrades,
            format_change_handle: None,
            db: disk_db,
        };

        // The original Mainnet VCT fast path did not persist historical Sprout frontiers.
        // Never expose an affected database unless this writable startup can synchronously
        // complete the authenticated repair.
        let prepared_vct_repair = if open_mode.enforces_vct_repair_guard()
            && repair_vct_sprout_history::is_repair_eligible(&db, disk_version_before_open.as_ref())
        {
            if read_only || upgrades_explicitly_disabled {
                let reason = if read_only {
                    "read-only databases cannot be repaired"
                } else {
                    "database format upgrades are disabled"
                };
                return Err(StateInitError::VctSproutHistoryRepairRequired {
                    mode: if read_only { "read-only" } else { "writable" },
                    reason,
                });
            }

            Some(
                repair_vct_sprout_history::prepare_startup_repair(&db).map_err(|error| {
                    StateInitError::VctSproutHistoryRepairInvalid {
                        reason: error.to_string(),
                    }
                })?,
            )
        } else {
            None
        };

        let zero_location_utxos =
            db.address_utxo_locations(AddressLocation::from_usize(Height(0), 0, 0));
        if !zero_location_utxos.is_empty() {
            warn!(
                "You have been impacted by the Zebra 2.4.0 address indexer corruption bug. \
                If you rely on the data from the RPC interface, you will need to recover your database. \
                Follow the instructions in the 2.4.1 release notes: https://github.com/ZcashFoundation/zebra/releases/tag/v2.4.1 \
                If you just run the node for consensus and don't use data from the RPC interface, you can ignore this warning."
            )
        }

        // Optionally audit the zakura header store's on-disk invariants and
        // truncate any incoherent suffix. This can scan a large header frontier
        // while syncing from genesis, so operators opt in when they need a
        // startup repair. Read-only instances cannot repair; the audit is left
        // to an explicit writable reopen.
        if !read_only && config.repair_zakura_header_store_on_startup {
            db.audit_and_repair_zakura_header_store()
                .unwrap_or_else(|error| panic!("startup header-store repair failed: {error}"));
        }

        db.run_startup_format_change(format_change, prepared_vct_repair);

        Ok(db)
    }

    /// Complete the startup format change before exposing the database, then launch only
    /// configured periodic current-format checks in the background.
    pub(crate) fn run_startup_format_change(
        &mut self,
        format_change: DbFormatChange,
        prepared_vct_repair: Option<Arc<repair_vct_sprout_history::RepairInput>>,
    ) {
        if self.debug_skip_format_upgrades {
            return;
        }

        // No state service can commit while this synchronous startup operation is running.
        let initial_tip_height = self.finalized_tip_height();
        let (_never_cancel_handle, never_cancel_receiver) = bounded(1);
        format_change
            .run_format_change_or_check(
                self,
                initial_tip_height,
                &never_cancel_receiver,
                prepared_vct_repair,
            )
            .expect("startup format change cannot be cancelled");

        let format_change_handle =
            DbFormatChange::spawn_periodic_format_checks(self.clone(), initial_tip_height);

        self.format_change_handle = Some(format_change_handle);
    }

    /// Sets `finished_format_upgrades` to true on the inner [`DiskDb`] to indicate that Zebra has
    /// finished applying any required db format upgrades.
    pub fn mark_finished_format_upgrades(&self) {
        self.db.mark_finished_format_upgrades();
    }

    /// Returns true if the `finished_format_upgrades` flag has been set to true on the inner [`DiskDb`] to
    /// indicate that Zebra has finished applying any required db format upgrades.
    pub fn finished_format_upgrades(&self) -> bool {
        self.db.finished_format_upgrades()
    }

    /// Returns config for this database.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Returns the configured database kind for this database.
    pub fn db_kind(&self) -> String {
        self.db.db_kind()
    }

    /// Returns the format version of the running code that created this `ZakuraDb` instance in memory.
    pub fn format_version_in_code(&self) -> Version {
        self.db.format_version_in_code()
    }

    /// Returns the fixed major version for this database.
    pub fn major_version(&self) -> u64 {
        self.db.major_version()
    }

    /// Returns the format version of this database on disk.
    ///
    /// See `database_format_version_on_disk()` for details.
    pub fn format_version_on_disk(&self) -> Result<Option<Version>, BoxError> {
        database_format_version_on_disk(
            self.config(),
            self.db_kind(),
            self.major_version(),
            &self.network(),
        )
    }

    /// Updates the format of this database on disk to the suppled version.
    ///
    /// See `write_database_format_version_to_disk()` for details.
    pub(crate) fn update_format_version_on_disk(
        &self,
        new_version: &Version,
    ) -> Result<(), BoxError> {
        write_database_format_version_to_disk(
            self.config(),
            self.db_kind(),
            self.major_version(),
            new_version,
            &self.network(),
        )
    }

    /// Returns the configured network for this database.
    pub fn network(&self) -> Network {
        self.db.network()
    }

    /// Returns the `Path` where the files used by this database are located.
    pub fn path(&self) -> &Path {
        self.db.path()
    }

    /// Check for panics in code running in spawned threads.
    /// If a thread exited with a panic, resume that panic.
    ///
    /// This method should be called regularly, so that panics are detected as soon as possible.
    pub fn check_for_panics(&mut self) {
        if let Some(format_change_handle) = self.format_change_handle.as_mut() {
            format_change_handle.check_for_panics();
        }
    }

    /// When called with a secondary DB instance, tries to catch up with the primary DB instance
    pub fn try_catch_up_with_primary(&self) -> Result<(), rocksdb::Error> {
        self.db.try_catch_up_with_primary()
    }

    /// Spawns a blocking task to try catching up with the primary DB instance.
    pub async fn spawn_try_catch_up_with_primary(&self) -> Result<(), rocksdb::Error> {
        let db = self.clone();
        tokio::task::spawn_blocking(move || {
            let result = db.try_catch_up_with_primary();
            if let Err(catch_up_error) = &result {
                tracing::warn!(?catch_up_error, "failed to catch up to primary");
            }
            result
        })
        .wait_for_panics()
        .await
    }

    /// Shut down the database, cleaning up background tasks and ephemeral data.
    ///
    /// If `force` is true, clean up regardless of any shared references.
    /// `force` can cause errors accessing the database from other shared references.
    /// It should only be used in debugging or test code, immediately before a manual shutdown.
    ///
    /// See [`DiskDb::shutdown`] for details.
    pub fn shutdown(&mut self, force: bool) {
        // Are we shutting down the underlying database instance?
        let is_shutdown = force || self.db.shared_database_owners() <= 1;

        // # Concurrency
        //
        // The format upgrade task should be cancelled before the database is flushed or shut down.
        // This helps avoid some kinds of deadlocks.
        //
        // See also the correctness note in `DiskDb::shutdown()`.
        if !self.debug_skip_format_upgrades && is_shutdown {
            if let Some(format_change_handle) = self.format_change_handle.as_mut() {
                format_change_handle.force_cancel();
            }

            // # Correctness
            //
            // Check that the database format is correct before shutting down.
            // This lets users know to delete and re-sync their database immediately,
            // rather than surprising them next time Zebra starts up.
            //
            // # Testinng
            //
            // In Zebra's CI, panicking here stops us writing invalid cached states,
            // which would then make unrelated PRs fail when Zebra starts up.

            // If the upgrade has completed, or we've done a downgrade, check the state is valid.
            let disk_version = database_format_version_on_disk(
                &self.config,
                self.db_kind(),
                self.major_version(),
                &self.network(),
            )
            .expect("unexpected invalid or unreadable database version file");

            if let Some(disk_version) = disk_version {
                // We need to keep the cancel handle until the format check has finished,
                // because dropping it cancels the format check.
                let (_never_cancel_handle, never_cancel_receiver) = bounded(1);

                // We block here because the checks are quick and database validity is
                // consensus-critical.
                if disk_version >= self.db.format_version_in_code() {
                    DbFormatChange::check_new_blocks(self)
                        .run_format_change_or_check(
                            self,
                            // The initial tip height is not used by the new blocks format check.
                            None,
                            &never_cancel_receiver,
                            None,
                        )
                        .expect("cancel handle is never used");
                }
            }
        }

        self.check_for_panics();

        self.db.shutdown(force);
    }

    /// Check that the on-disk height is well below the maximum supported database height.
    ///
    /// Zebra only supports on-disk heights up to 3 bytes.
    ///
    /// # Logs an Error
    ///
    /// If Zebra is storing block heights that are close to [`MAX_ON_DISK_HEIGHT`].
    pub(crate) fn check_max_on_disk_tip_height(&self) -> Result<(), String> {
        if let Some((tip_height, tip_hash)) = self.tip() {
            if tip_height.0 > MAX_ON_DISK_HEIGHT.0 / 2 {
                let err = Err(format!(
                    "unexpectedly large tip height, database format upgrade required: \
                     tip height: {tip_height:?}, tip hash: {tip_hash:?}, \
                     max height: {MAX_ON_DISK_HEIGHT:?}"
                ));
                error!(?err);
                return err;
            }
        }

        Ok(())
    }

    /// Logs metrics related to the underlying RocksDB instance.
    ///
    /// This function prints various metrics and statistics about the RocksDB database,
    /// such as disk usage, memory usage, and other performance-related metrics.
    pub fn print_db_metrics(&self) {
        self.db.print_db_metrics();
    }

    /// Exports RocksDB metrics to Prometheus.
    ///
    /// This function collects database statistics and exposes them as Prometheus metrics.
    /// Call this periodically (e.g., every 30 seconds) from a background task.
    pub(crate) fn export_metrics(&self) {
        self.db.export_metrics();
    }

    /// Returns the estimated total disk space usage of the database.
    pub fn size(&self) -> u64 {
        self.db.size()
    }
}

impl Drop for ZakuraDb {
    fn drop(&mut self) {
        self.shutdown(false);
    }
}
