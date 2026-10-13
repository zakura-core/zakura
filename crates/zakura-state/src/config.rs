//! Cached state configuration for Zebra.

use std::{
    fmt, fs,
    io::ErrorKind,
    path::{Path, PathBuf},
    time::Duration,
};

use semver::Version;
use serde::{
    de::{self, MapAccess, Visitor},
    Deserialize, Deserializer, Serialize,
};

use zakura_chain::{common::default_cache_dir, parameters::Network};

use crate::{
    constants::{
        min_pruning_retention, DATABASE_FORMAT_VERSION_FILE_NAME, MIN_PRUNING_RETENTION,
        STATE_DATABASE_KIND,
    },
    state_database_format_version_in_code, BoxError,
};

mod location;

pub(crate) use location::{adopt_previous_major_database, find_database};
pub use location::{check_and_delete_old_databases, check_and_delete_old_state_databases};

/// Configuration for the state service.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// The root directory for storing cached block data.
    ///
    /// If you change this directory, you might also want to change `network.cache_dir`.
    ///
    /// This cache stores permanent blockchain state that can be replicated from
    /// the network, including the best chain, blocks, the UTXO set, and other indexes.
    /// Any state that can be rolled back is only stored in memory.
    ///
    /// The `zakura-state` cache does *not* include any private data, such as wallet data.
    ///
    /// You can delete the entire cached state directory, but it will impact your node's
    /// readiness and network usage. If you do, Zebra will re-sync from genesis the next
    /// time it is launched.
    ///
    /// Storage mode is controlled separately by [`storage_mode`](Config::storage_mode).
    ///
    /// The default directory is platform dependent, based on
    /// [`default_cache_dir`]:
    ///
    /// |Platform | Value                                           | Example                              |
    /// | ------- | ----------------------------------------------- | ------------------------------------ |
    /// | Linux   | `$XDG_CACHE_HOME/zakura` or `$HOME/.cache/zakura` | `/home/alice/.cache/zakura`           |
    /// | macOS   | `$HOME/Library/Caches/zakura`                    | `/Users/Alice/Library/Caches/zakura`  |
    /// | Windows | `{FOLDERID_LocalAppData}\zakura`                 | `C:\Users\Alice\AppData\Local\zakura` |
    /// | Other   | `std::env::current_dir()/cache/zakura`           | `/cache/zakura`                       |
    ///
    /// # Security
    ///
    /// If you are running Zebra with elevated permissions ("root"), create the
    /// directory for this file before running Zebra, and make sure the Zebra user
    /// account has exclusive access to that directory, and other users can't modify
    /// its parent directories.
    ///
    /// # Implementation Details
    ///
    /// Each state format version and network has a separate state.
    /// These states are stored in `state/vN/mainnet` and `state/vN/testnet` subdirectories,
    /// underneath the `cache_dir` path, where `N` is the state format version.
    ///
    /// When Zebra's state format changes, it creates a new state subdirectory for that version,
    /// and re-syncs from genesis.
    ///
    /// Old state versions are automatically deleted at startup. You can also manually delete old
    /// state versions.
    pub cache_dir: PathBuf,

    /// Whether to use an ephemeral database.
    ///
    /// Ephemeral databases are stored in a temporary directory created using [`tempfile::tempdir()`].
    /// They are deleted when Zebra exits successfully.
    /// (If Zebra panics or crashes, the ephemeral database won't be deleted.)
    ///
    /// Set to `false` by default. If this is set to `true`, [`cache_dir`] is ignored.
    ///
    /// Ephemeral directories are created in the [`std::env::temp_dir()`].
    /// Zebra names each directory after the state version and network, for example: `zakura-state-v21-mainnet-XnyGnE`.
    ///
    /// [`cache_dir`]: struct.Config.html#structfield.cache_dir
    pub ephemeral: bool,

    /// Whether to cache non-finalized blocks on disk to be restored when Zebra restarts.
    ///
    /// Set to `true` by default. If this is set to `false`, Zebra will irrecoverably drop
    /// non-finalized blocks when the process exits and will have to re-download them from
    /// the network when it restarts, if those blocks are still available in the network.
    ///
    /// Note: The non-finalized state will be written to a backup cache once per 5 seconds at most.
    ///       If blocks are added to the non-finalized state more frequently, the backup may not reflect
    ///       Zebra's last non-finalized state before it shut down.
    pub should_backup_non_finalized_state: bool,

    /// Legacy compatibility switch for the retired opt-in header-store repair.
    ///
    /// Native header-runtime attachment always performs its required startup audit.
    /// Attachment also always performs reconstruction.
    /// Earlier Zakura releases wrote this field to configuration files.
    /// State keeps deserializing the field for compatibility.
    /// State no longer serializes the field.
    #[serde(skip_serializing)]
    pub repair_zakura_header_store_on_startup: bool,

    /// Whether this process runs the native Zakura header runtime.
    ///
    /// `zakurad` enables this only when the selected P2P stack includes Zakura v2.
    /// `network.p2p_stack` owns this choice.
    /// State does not expose a separate user-facing option.
    #[serde(skip)]
    pub enable_zakura_header_seed_from_committed_blocks: bool,

    /// Mirror of `consensus.checkpoint_sync`, set by zakurad at startup.
    ///
    /// When `true` (the default), a node syncing under checkpoint trust uses the fast
    /// verified-commitment-trees path below the last checkpoint: per-block Sapling/Orchard
    /// roots are verified against the committed headers and folded into the anchor set and
    /// history tree, skipping the per-block frontier recompute. The
    /// `consensus.vct_fast_sync` setting is mirrored into state to keep checkpoint sync
    /// enabled while forcing the legacy per-block recompute.
    ///
    /// Skipped in serde because it is not an independent state setting — it tracks the
    /// consensus option, so the generic Zebra state config does not expose a duplicate.
    #[serde(skip)]
    pub checkpoint_sync: bool,

    /// Mirror of `consensus.vct_fast_sync`, set by zakurad at startup.
    ///
    /// When `true` (the default), checkpoint sync uses the verified-commitment-trees fast path on
    /// networks with embedded handoff frontiers. Set to `false` to keep `consensus.checkpoint_sync`
    /// enabled while forcing the legacy per-block Sapling/Orchard tree recompute in both Archive
    /// and Pruned storage modes.
    ///
    /// Skipped in serde because users configure this alongside `consensus.checkpoint_sync`, not
    /// as an independent state setting.
    #[serde(skip)]
    pub vct_fast_sync: bool,

    /// Optional path to a frontier artifact used to anchor historical tree derivation.
    ///
    /// Mainnet's reviewed frontier grid is embedded in the binary, so the default `None` uses that
    /// grid without deployment-time configuration. Setting a path overrides the embedded grid,
    /// primarily for tests and custom networks. The artifact holds note commitment frontiers at a
    /// sparse height grid, so a cold request replays from the nearest grid entry rather than from
    /// genesis. Every entry is checked against the authenticated root this node already stores
    /// before it is used, so a corrupt or hostile artifact is rejected rather than absorbed.
    ///
    /// Completed subtree roots need no equivalent setting: they ship embedded in the binary and
    /// are loaded without operator configuration.
    pub historical_frontier_artifact: Option<PathBuf>,

    /// Whether to delete the old database directories when present.
    ///
    /// Set to `true` by default. If this is set to `false`,
    /// no check for old database versions will be made and nothing will be
    /// deleted.
    pub delete_old_database: bool,

    /// Selects whether Zebra keeps all historical block data, or stores only the
    /// data required to validate future blocks.
    ///
    /// Set to [`StorageMode::Archive`] by default, which preserves all data and
    /// is required to answer historical RPC queries.
    ///
    /// [`StorageMode::Pruned`] deletes historical raw transaction bytes outside a
    /// retention window to reduce disk usage, while keeping all consensus-critical
    /// state (the UTXO set, nullifiers, anchors, note commitment trees, history
    /// tree, and value pools) as well as the transaction location indexes needed
    /// to validate future spends. This is a one-way mode: once data has been
    /// pruned, the database cannot be reopened in [`StorageMode::Archive`] without
    /// re-syncing from genesis.
    ///
    /// With checkpoint sync enabled, pruned storage stops maintaining transparent address
    /// balances, address UTXO and transaction lists, and transparent spender indexes in the
    /// existing database. Existing rows are retained but never served as complete results.
    /// The first skipped block records this permanently, including across restart and rollback.
    /// Restoring complete address queries requires syncing a fresh indexed database.
    pub storage_mode: StorageMode,

    // Debug configs
    //
    /// Commit blocks to the finalized state up to this height, then exit Zebra.
    ///
    /// Set to `None` by default: Zebra continues syncing indefinitely.
    pub debug_stop_at_height: Option<u32>,

    /// While Zebra is running, check state validity this often.
    ///
    /// Set to `None` by default: Zebra only checks state format validity on startup and shutdown.
    #[serde(with = "zakura_chain::serialization::serde_adapters::optional_duration")]
    pub debug_validity_check_interval: Option<Duration>,

    /// If true, skip spawning the non-finalized state backup task and instead write
    /// the non-finalized state to the backup directory synchronously before each update
    /// to the latest chain tip or non-finalized state channels.
    ///
    /// Set to `false` by default. When `true`, the non-finalized state is still restored
    /// from the backup directory on startup, but updates are written synchronously on every
    /// block commit rather than asynchronously every 5 seconds.
    ///
    /// This is intended for testing scenarios where blocks are committed rapidly and the
    /// async backup task may not flush all blocks before shutdown.
    pub debug_skip_non_finalized_state_backup_task: bool,
}

fn gen_temp_path(prefix: &str) -> PathBuf {
    tempfile::Builder::new()
        .prefix(prefix)
        .tempdir()
        .expect("temporary directory is created successfully")
        .keep()
}

impl Config {
    /// Whether this configuration stops optional transparent index updates.
    pub(crate) fn skips_transparent_indexes(&self) -> bool {
        self.pruning_config().is_some() && self.checkpoint_sync
    }

    /// Returns the path for the database, based on the kind, major version and network.
    /// Each incompatible database format or network gets its own unique path.
    pub fn db_path(
        &self,
        db_kind: impl AsRef<str>,
        major_version: u64,
        network: &Network,
    ) -> PathBuf {
        let db_kind = db_kind.as_ref();
        let major_version = format!("v{major_version}");
        let net_dir = network.lowercase_name();

        if self.ephemeral {
            gen_temp_path(&format!("zebra-{db_kind}-{major_version}-{net_dir}-"))
        } else {
            self.cache_dir
                .join(db_kind)
                .join(major_version)
                .join(net_dir)
        }
    }

    /// Returns the path for the non-finalized state backup directory, based on the network.
    /// Non-finalized state backup files are encoded in the network protocol format and remain
    /// valid across db format upgrades.
    pub fn non_finalized_state_backup_dir(&self, network: &Network) -> Option<PathBuf> {
        if self.ephemeral || !self.should_backup_non_finalized_state {
            // Ephemeral databases are intended to be irrecoverable across restarts and don't
            // require a backup for the non-finalized state.
            return None;
        }

        let net_dir = network.lowercase_name();
        Some(self.cache_dir.join("non_finalized_state").join(net_dir))
    }

    /// Returns the path for the database format minor/patch version file,
    /// based on the kind, major version and network.
    pub fn version_file_path(
        &self,
        db_kind: impl AsRef<str>,
        major_version: u64,
        network: &Network,
    ) -> PathBuf {
        let mut version_path = self.db_path(db_kind, major_version, network);

        version_path.push(DATABASE_FORMAT_VERSION_FILE_NAME);

        version_path
    }

    /// Returns a config for a temporary database that is deleted when it is dropped.
    pub fn ephemeral() -> Config {
        Config {
            ephemeral: true,
            ..Config::default()
        }
    }

    /// Returns the [`PruningConfig`] if this config selects pruned storage mode.
    pub fn pruning_config(&self) -> Option<&PruningConfig> {
        match &self.storage_mode {
            StorageMode::Archive => None,
            StorageMode::Pruned(pruning) => Some(pruning),
        }
    }

    /// Validates the configured [`StorageMode`].
    ///
    /// This must be called before opening the database, so that a misconfigured
    /// retention window fails fast at startup rather than mid-run.
    ///
    /// # Errors
    ///
    /// Returns an error if pruned mode is selected with a `tx_retention` below
    /// the network-specific floor ([`min_pruning_retention`]). A retention window
    /// at or below the reorg depth could let pruning delete data that a rollback
    /// needs to read.
    pub fn validate_storage_mode(&self, network: &Network) -> Result<(), BoxError> {
        if let Some(pruning) = self.pruning_config() {
            let floor = min_pruning_retention(network);
            if pruning.tx_retention < floor {
                return Err(format!(
                    "invalid pruning configuration: tx_retention ({}) must be at least {floor} \
                     on {network} so pruning cannot delete data within the reorg/rollback window",
                    pruning.tx_retention,
                )
                .into());
            }
        }

        Ok(())
    }

    /// Whether this node rebuilds historical note commitment trees on demand for RPC queries.
    ///
    /// True when an archive node either currently uses the verified-commitment-trees fast path or
    /// has a durable marker showing that its database previously completed a VCT fast sync. The
    /// marker keeps an existing absent tree band serviceable after sync settings change. A legacy
    /// database has no marker, so disabling either setting keeps derivation off.
    ///
    /// This is derived rather than configured because it grants no capability an operator needs to
    /// weigh: a derived frontier is served only when it reproduces the authenticated root this node
    /// already stores, so the answer is verified rather than trusted, and a node that can answer a
    /// treestate query correctly has no reason to refuse it. What an operator does choose is the
    /// cost bound: derivation anchors on the embedded Mainnet grid or the
    /// [`Self::historical_frontier_artifact`] override, and
    /// [`crate::MAX_HISTORICAL_TREE_REPLAY_BLOCKS`] bounds it from both ends, refusing a grid whose
    /// gaps are too wide at startup and a request that would replay too far at serving time.
    pub fn derive_historical_trees(&self, database_was_vct_fast_synced: bool) -> bool {
        self.pruning_config().is_none()
            && ((self.checkpoint_sync && self.vct_fast_sync) || database_was_vct_fast_synced)
    }
}

/// Selects whether Zebra keeps all historical block data, or stores only the data
/// required to validate future blocks.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageMode {
    /// Keep all historical data, including raw transactions and lookup indexes.
    ///
    /// This is the default and is required to answer historical RPC queries such
    /// as `getrawtransaction` for old transactions.
    #[default]
    Archive,

    /// Prune historical raw transaction bytes outside the retention window.
    ///
    /// Consensus-critical state and transaction location indexes are always
    /// retained. This is a one-way mode: a pruned database cannot be reopened in
    /// [`StorageMode::Archive`].
    Pruned(PruningConfig),
}

impl<'de> Deserialize<'de> for StorageMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            deserializer.deserialize_any(StorageModeVisitor)
        } else {
            StorageModeSerde::deserialize(deserializer).map(Into::into)
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum StorageModeSerde {
    Archive,
    Pruned(PruningConfig),
}

impl From<StorageModeSerde> for StorageMode {
    fn from(storage_mode: StorageModeSerde) -> Self {
        match storage_mode {
            StorageModeSerde::Archive => StorageMode::Archive,
            StorageModeSerde::Pruned(pruning) => StorageMode::Pruned(pruning),
        }
    }
}

struct StorageModeVisitor;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EmptyArchiveConfig {}

impl<'de> Visitor<'de> for StorageModeVisitor {
    type Value = StorageMode;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(r#"`"archive"`, `"pruned"`, or a storage mode table"#)
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match value {
            "archive" => Ok(StorageMode::Archive),
            "pruned" => Ok(StorageMode::Pruned(PruningConfig::default())),
            _ => Err(de::Error::unknown_variant(value, &["archive", "pruned"])),
        }
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let Some(key) = map.next_key::<String>()? else {
            return Err(de::Error::invalid_length(0, &self));
        };

        let storage_mode = match key.as_str() {
            "archive" => {
                let _ = map.next_value::<EmptyArchiveConfig>()?;
                StorageMode::Archive
            }
            "pruned" => StorageMode::Pruned(map.next_value()?),
            _ => return Err(de::Error::unknown_variant(&key, &["archive", "pruned"])),
        };

        if let Some(key) = map.next_key::<String>()? {
            return Err(de::Error::custom(format!(
                "multiple storage mode variants configured, including `{key}`"
            )));
        }

        Ok(storage_mode)
    }
}

/// Configuration for [`StorageMode::Pruned`].
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct PruningConfig {
    /// Number of recent finalized blocks below the tip whose raw transaction data
    /// is retained.
    ///
    /// Blocks older than this window have their raw transaction bytes (`tx_by_loc`)
    /// deleted. Transaction lookup indexes (`tx_loc_by_hash`, `hash_by_tx_loc`) are
    /// retained, because they are needed to resolve spends of UTXOs created in old
    /// blocks. Must be at least [`MIN_PRUNING_RETENTION`]; this is enforced by
    /// [`Config::validate_storage_mode`].
    pub tx_retention: u32,
}

impl Default for PruningConfig {
    fn default() -> Self {
        Self {
            tx_retention: MIN_PRUNING_RETENTION,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            cache_dir: default_cache_dir(),
            ephemeral: false,
            should_backup_non_finalized_state: true,
            repair_zakura_header_store_on_startup: false,
            enable_zakura_header_seed_from_committed_blocks: false,
            checkpoint_sync: true,
            vct_fast_sync: true,
            historical_frontier_artifact: None,
            delete_old_database: true,
            storage_mode: StorageMode::default(),
            debug_stop_at_height: None,
            debug_validity_check_interval: None,
            debug_skip_non_finalized_state_backup_task: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn optional_duration_preserves_config_toml() {
        let config: Config =
            toml::from_str(r#"debug_validity_check_interval = "15 seconds""#).unwrap();
        assert_eq!(
            config.debug_validity_check_interval,
            Some(Duration::from_secs(15))
        );
        assert!(toml::to_string(&config)
            .unwrap()
            .contains(r#"debug_validity_check_interval = "15s""#));
        assert_eq!(
            toml::from_str::<Config>("")
                .unwrap()
                .debug_validity_check_interval,
            None
        );
        assert!(!toml::to_string(&Config::default())
            .unwrap()
            .contains("debug_validity_check_interval"));
        assert!(toml::from_str::<Config>("debug_validity_check_interval = 15").is_err());
    }

    #[test]
    fn storage_mode_deserializes_from_documented_toml() {
        assert!(
            Config::default().vct_fast_sync,
            "VCT fast sync is enabled by default when checkpoint sync and embedded frontiers are available"
        );
        assert!(!Config::default().repair_zakura_header_store_on_startup);

        let legacy_repair: Config =
            toml::from_str(r#"repair_zakura_header_store_on_startup = true"#)
                .expect("legacy startup repair config remains parseable");
        assert!(legacy_repair.repair_zakura_header_store_on_startup);
        assert!(
            !toml::to_string(&legacy_repair)
                .expect("state config serializes")
                .contains("repair_zakura_header_store_on_startup"),
            "retired startup repair config is not emitted in new configs"
        );

        let archive: Config = toml::from_str(r#"storage_mode = "archive""#)
            .expect("archive storage mode deserializes from a string");
        assert_eq!(archive.storage_mode, StorageMode::Archive);
        assert!(
            archive.checkpoint_sync && archive.vct_fast_sync,
            "serde defaults preserve the internal checkpoint/VCT mirrors until zakurad overwrites them"
        );

        let archive_table: Config = toml::from_str("[storage_mode.archive]")
            .expect("an empty archive storage mode table deserializes");
        assert_eq!(archive_table.storage_mode, StorageMode::Archive);

        assert!(
            toml::from_str::<Config>(
                r#"
                [storage_mode.archive]
                tx_retention = 6000
                "#,
            )
            .is_err(),
            "archive mode must not silently ignore pruned-only or misspelled settings"
        );

        let pruned: Config = toml::from_str(r#"storage_mode = "pruned""#)
            .expect("pruned storage mode deserializes from a string");
        assert_eq!(
            pruned.storage_mode,
            StorageMode::Pruned(PruningConfig::default())
        );

        let pruned_with_retention: Config = toml::from_str(
            r#"
            [storage_mode.pruned]
            tx_retention = 6000
            "#,
        )
        .expect("pruned storage mode deserializes from a table");
        assert_eq!(
            pruned_with_retention.storage_mode,
            StorageMode::Pruned(PruningConfig { tx_retention: 6000 })
        );

        let serialized = toml::to_string(&Config::default()).expect("state config serializes");
        assert!(
            !serialized.contains("vct_fast_sync"),
            "vct_fast_sync is configured under [consensus], not [state]"
        );
    }

    #[test]
    fn historical_tree_derivation_follows_storage_mode_and_vct() {
        assert!(
            Config::default().derive_historical_trees(false),
            "an archive node on the VCT fast path can rebuild the trees it skipped storing"
        );

        let legacy_recompute = Config {
            vct_fast_sync: false,
            ..Config::default()
        };
        assert!(
            !legacy_recompute.derive_historical_trees(false),
            "a node that recomputes every per-height tree has no absent band to derive"
        );
        assert!(
            legacy_recompute.derive_historical_trees(true),
            "a durable VCT marker preserves derivation after the fast path is disabled"
        );

        let full_verification = Config {
            checkpoint_sync: false,
            ..Config::default()
        };
        assert!(
            !full_verification.derive_historical_trees(false),
            "without checkpoint sync the VCT mirror is inert, so no band is skipped"
        );
        assert!(
            full_verification.derive_historical_trees(true),
            "a durable VCT marker preserves derivation after checkpoint sync is disabled"
        );

        let pruned = Config {
            storage_mode: StorageMode::Pruned(PruningConfig::default()),
            ..Config::default()
        };
        assert!(
            !pruned.derive_historical_trees(false),
            "replay reads block bodies that pruned mode deletes"
        );
        assert!(
            !pruned.derive_historical_trees(true),
            "a durable VCT marker cannot make pruned block bodies available"
        );
    }

    #[test]
    fn retired_historical_tree_settings_are_rejected() {
        // Both keys were only ever available in pre-release builds, so a config carrying one is a
        // preview config that must be updated. Silently ignoring it would leave an operator
        // believing they had turned derivation off, or capped its replay.
        for retired in [
            "derive_historical_trees = true",
            "max_historical_tree_replay_blocks = 100",
        ] {
            assert!(
                toml::from_str::<Config>(retired).is_err(),
                "{retired} is no longer a state setting and must not parse"
            );
        }
    }
}

/// Returns the full semantic version of the on-disk state database, based on its config and network.
pub fn state_database_format_version_on_disk(
    config: &Config,
    network: &Network,
) -> Result<Option<Version>, BoxError> {
    database_format_version_on_disk(
        config,
        STATE_DATABASE_KIND,
        state_database_format_version_in_code().major,
        network,
    )
}

/// Returns the full semantic version of the existing database for `major_version`, read from its
/// version file.
///
/// This function finds the database in the same place as startup, so it also reads a
/// previous-major database that startup has not moved yet. It does not open RocksDB.
///
/// Returns `Ok(None)` if there is no database, or the database has no version file.
///
/// This is the format of the data on disk, the version
/// implemented by the running Zebra code can be different.
pub fn database_format_version_on_disk(
    config: &Config,
    db_kind: impl AsRef<str>,
    major_version: u64,
    network: &Network,
) -> Result<Option<Version>, BoxError> {
    match location::find_database(config, db_kind.as_ref(), major_version, network) {
        Some(db_path) => database_format_version_at_path(&db_path),
        None => Ok(None),
    }
}

/// Returns the full semantic version in the version file of the database at `db_path`.
///
/// Returns `Ok(None)` if the version file does not exist.
pub(crate) fn database_format_version_at_path(db_path: &Path) -> Result<Option<Version>, BoxError> {
    match fs::read_to_string(db_path.join(DATABASE_FORMAT_VERSION_FILE_NAME)) {
        Ok(version) => Ok(Some(version.parse()?)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

// Hide this destructive method from the public API, except in tests.
#[allow(unused_imports)]
pub(crate) use hidden::{
    write_database_format_version_to_disk, write_state_database_format_version_to_disk,
};

pub(crate) mod hidden {
    #![allow(dead_code)]

    use zakura_chain::common::atomic_write;

    use super::*;

    /// Writes `changed_version` to the on-disk state database after the format is changed.
    /// (Or a new database is created.)
    ///
    /// See `write_database_format_version_to_disk()` for details.
    pub fn write_state_database_format_version_to_disk(
        config: &Config,
        changed_version: &Version,
        network: &Network,
    ) -> Result<(), BoxError> {
        write_database_format_version_to_disk(
            config,
            STATE_DATABASE_KIND,
            state_database_format_version_in_code().major,
            changed_version,
            network,
        )
    }

    /// Writes `changed_version` to the on-disk database after the format is changed.
    /// (Or a new database is created.)
    ///
    /// The database path is based on its kind, `major_version_in_code`, and network.
    ///
    /// # Correctness
    ///
    /// This should only be called:
    /// - after each format upgrade is complete,
    /// - when creating a new database, or
    /// - when an older Zebra version opens a newer database.
    ///
    /// # Concurrency
    ///
    /// This must only be called while RocksDB has an open database for `config`.
    /// Otherwise, multiple Zebra processes could write the version at the same time,
    /// corrupting the file.
    pub fn write_database_format_version_to_disk(
        config: &Config,
        db_kind: impl AsRef<str>,
        major_version_in_code: u64,
        changed_version: &Version,
        network: &Network,
    ) -> Result<(), BoxError> {
        // Write the version file atomically so the cache is not corrupted if Zebra shuts down or
        // crashes.
        atomic_write(
            config.version_file_path(db_kind, major_version_in_code, network),
            changed_version.to_string().as_bytes(),
        )??;

        Ok(())
    }
}
