//! Tests for stale database cleanup.

use std::path::{Path, PathBuf};

use tempfile::TempDir;
use zakura_chain::parameters::Network;

use crate::{constants::STATE_DATABASE_KIND, Config};

use super::delete_old_databases;

const CURRENT_MAJOR_VERSION: u64 = 30;

fn persistent_config() -> (TempDir, Config) {
    let cache = tempfile::tempdir().expect("temporary cache directory is created");
    let config = Config {
        cache_dir: cache.path().to_path_buf(),
        ephemeral: false,
        delete_old_database: true,
        ..Config::default()
    };
    (cache, config)
}

/// Creates an empty RocksDB database for `network` in the `major_version` directory.
fn create_database(config: &Config, major_version: u64, network: &Network) -> PathBuf {
    let path = config.db_path(STATE_DATABASE_KIND, major_version, network);
    rocksdb::DB::open_default(&path).expect("fixture database is created");
    path
}

fn delete(config: &Config, network: &Network) {
    delete_old_databases(config, STATE_DATABASE_KIND, CURRENT_MAJOR_VERSION, network);
}

/// Cleanup keeps older databases until the current one exists, then deletes the running network's
/// older databases and leaves other networks' databases for their own startup to move.
#[test]
fn old_databases_are_deleted_only_after_the_current_database_exists() {
    let (_cache, config) = persistent_config();
    let mainnet = Network::Mainnet;
    let testnet = Network::new_default_testnet();

    let previous_mainnet = create_database(&config, 29, &mainnet);
    let previous_testnet = create_database(&config, 29, &testnet);
    let below_floor_mainnet = create_database(&config, 27, &mainnet);

    delete(&config, &mainnet);
    assert!(
        previous_mainnet.exists(),
        "a failed move leaves the previous database as the only copy"
    );
    assert!(below_floor_mainnet.exists());

    create_database(&config, CURRENT_MAJOR_VERSION, &mainnet);
    delete(&config, &mainnet);
    assert!(!previous_mainnet.exists());
    assert!(!below_floor_mainnet.exists());
    assert!(
        !below_floor_mainnet
            .parent()
            .expect("database paths have a major version parent")
            .exists(),
        "cleanup removes the emptied major directory"
    );
    assert!(
        previous_testnet.exists(),
        "a Testnet startup still moves its own previous-major database"
    );
}

/// Cleanup keeps a database that a running older node has open.
///
/// Only Linux has file locks that conflict with RocksDB's lock inside the same process.
#[cfg(target_os = "linux")]
#[test]
fn old_databases_that_another_instance_has_open_are_kept() {
    let (_cache, config) = persistent_config();
    let network = Network::Mainnet;

    let previous = create_database(&config, 29, &network);
    let older_node = rocksdb::DB::open_default(&previous).expect("older node opens its database");
    create_database(&config, CURRENT_MAJOR_VERSION, &network);

    delete(&config, &network);
    assert!(previous.exists());

    drop(older_node);
    delete(&config, &network);
    assert!(!previous.exists());
}

/// Cleanup never follows a symlink out of the cache directory.
#[cfg(unix)]
#[test]
fn old_databases_outside_the_cache_directory_are_kept() {
    let (_cache, config) = persistent_config();
    let network = Network::Mainnet;
    let outside = tempfile::tempdir().expect("outside directory is created");
    let outside_db: &Path = outside.path();

    rocksdb::DB::open_default(outside_db).expect("outside database is created");
    let previous = config.db_path(STATE_DATABASE_KIND, 29, &network);
    std::fs::create_dir_all(previous.parent().expect("database paths have a parent"))
        .expect("previous major directory is created");
    std::os::unix::fs::symlink(outside_db, &previous).expect("symlink is created");
    create_database(&config, CURRENT_MAJOR_VERSION, &network);

    delete(&config, &network);
    assert!(previous.exists());
    assert!(outside_db.join("CURRENT").exists());
}
