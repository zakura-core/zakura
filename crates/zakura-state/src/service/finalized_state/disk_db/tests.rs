//! Tests and test methods for low-level RocksDB access.

#![allow(clippy::unwrap_in_result)]
#![allow(dead_code)]

use std::{ops::Deref, sync::atomic::Ordering};

use semver::Version;
use zakura_chain::parameters::Network;

use crate::{
    service::finalized_state::disk_db::{format_bytes, DiskDb, DB},
    Config,
};

// Enable older test code to automatically access the inner database via Deref coercion.
impl Deref for DiskDb {
    type Target = DB;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

impl DiskDb {
    /// Returns a list of column family names in this database.
    pub fn list_cf(&self) -> Result<Vec<String>, rocksdb::Error> {
        let opts = DiskDb::options();
        let path = self.path();

        rocksdb::DB::list_cf(&opts, path)
    }
}

#[test]
fn format_bytes_preserves_decimal_unit_boundaries() {
    assert_eq!(format_bytes(0), "0 B");
    assert_eq!(format_bytes(999), "999 B");
    assert_eq!(format_bytes(1_000), "1 KB");
    assert_eq!(format_bytes(1_049), "1 KB");
    assert_eq!(format_bytes(1_050), "1.1 KB");
    assert_eq!(format_bytes(999_949), "999.9 KB");
    assert_eq!(format_bytes(999_950), "1000 KB");
    assert_eq!(format_bytes(1_000_000), "1 MB");
    assert_eq!(format_bytes(u64::MAX), "18.4 EB");
}

#[test]
fn exporting_metrics_refreshes_cached_disk_size() {
    let _init_guard = zakura_test::init();
    let db = DiskDb::new(
        &Config::ephemeral(),
        "cached-size-test",
        &Version::new(1, 0, 0),
        &Network::Mainnet,
        ["cached_size".to_owned()],
        false,
    )
    .expect("the ephemeral database configuration is valid");

    let cf = db
        .cf_handle("cached_size")
        .expect("the test column family was configured");
    db.put_cf(cf, b"key", [0xa5; 4096])
        .expect("writing the test value should succeed");
    db.flush_cf(cf)
        .expect("flushing the test column family should succeed");
    db.refresh_cached_size();

    let expected_size = db.size();
    assert!(
        expected_size > 0,
        "the flushed SST file should use disk space"
    );
    db.cached_size.store(u64::MAX, Ordering::Relaxed);
    assert_eq!(
        db.size(),
        expected_size,
        "the on-demand size must not return the cached estimate"
    );
    assert_eq!(
        db.cached_size(),
        u64::MAX,
        "the test must replace the cached estimate"
    );
    db.export_metrics();

    assert_eq!(
        db.cached_size(),
        expected_size,
        "the metrics export should refresh the cached disk size"
    );
}

/// Check that zs_iter_opts returns an upper bound one greater than provided inclusive end bounds.
#[test]
fn zs_iter_opts_increments_key_by_one() {
    let _init_guard = zakura_test::init();

    // TODO: add an empty key (`()` type or `[]` when serialized) test case
    let keys: [u32; 14] = [
        0,
        1,
        200,
        255,
        256,
        257,
        65535,
        65536,
        65537,
        16777215,
        16777216,
        16777217,
        16777218,
        u32::MAX,
    ];

    for key in keys {
        let (_, bytes) = DiskDb::zs_iter_bounds(&..=key.to_be_bytes().to_vec());
        let mut extra_bytes = bytes.expect("there should be an upper bound");
        let bytes = extra_bytes.split_off(extra_bytes.len() - 4);
        let upper_bound = u32::from_be_bytes(bytes.clone().try_into().expect("should be 4 bytes"));
        let expected_upper_bound = key.wrapping_add(1);

        assert_eq!(
            expected_upper_bound, upper_bound,
            "the upper bound should be 1 greater than the original key"
        );

        if expected_upper_bound == 0 {
            assert_eq!(
                extra_bytes,
                vec![1],
                "there should be an extra byte with a value of 1"
            );
        } else {
            assert_eq!(extra_bytes.len(), 0, "there should be no extra bytes");
        }
    }
}

mod major_upgrade_reuse {
    use std::fs;

    use semver::Version;
    use zakura_chain::parameters::Network;

    use crate::{
        database_format_version_on_disk,
        service::finalized_state::disk_db::{DiskDb, DB},
        Config,
    };

    const KIND: &str = "state";
    const RESTORABLE: &[u64] = &[27, 28, 29];

    fn config(tempdir: &tempfile::TempDir) -> Config {
        Config {
            cache_dir: tempdir.path().to_owned(),
            ephemeral: false,
            ..Config::default()
        }
    }

    /// Creates a database at `major`, with `version_file` as the raw version file contents.
    fn create_db(config: &Config, major: u64, version_file: Option<&str>) {
        let path = config.db_path(KIND, major, &Network::Mainnet);
        fs::create_dir_all(&path).expect("the test database directory is created");
        drop(DB::open(&DiskDb::options(), &path).expect("the test database opens"));
        if let Some(contents) = version_file {
            fs::write(
                config.version_file_path(KIND, major, &Network::Mainnet),
                contents,
            )
            .expect("the version file is written");
        }
    }

    fn reuse(config: &Config, restorable: &[u64]) -> Option<Version> {
        DiskDb::try_reusing_previous_db_after_major_upgrade(
            restorable,
            &Version::new(29, 0, 0),
            config,
            KIND,
            &Network::Mainnet,
        )
    }

    fn version_on_disk(config: &Config, major: u64) -> Option<Version> {
        database_format_version_on_disk(config, KIND, major, &Network::Mainnet)
            .expect("the version file is readable")
    }

    #[test]
    fn keeps_the_older_major_of_an_interrupted_upgrade() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = config(&tempdir);
        create_db(&config, 28, Some("27.3.0"));

        assert_eq!(reuse(&config, RESTORABLE), Some(Version::new(27, 3, 0)));
        assert_eq!(version_on_disk(&config, 29), Some(Version::new(27, 3, 0)));
    }

    #[test]
    fn resolves_legacy_and_missing_version_files_to_the_old_major() {
        for (version_file, expected) in [
            (Some("1.5"), Version::new(28, 1, 5)),
            (None, Version::new(28, 0, 0)),
        ] {
            let tempdir = tempfile::tempdir().unwrap();
            let config = config(&tempdir);
            create_db(&config, 28, version_file);

            assert_eq!(reuse(&config, RESTORABLE), Some(expected.clone()));
            // The version file moved with the directory, so a crash after the rename
            // cannot make the next startup infer 29.0.0.
            assert_eq!(version_on_disk(&config, 29), Some(expected));
        }
    }

    #[test]
    fn reuses_a_database_two_majors_back() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = config(&tempdir);
        create_db(&config, 27, Some("27.3.0"));

        assert_eq!(reuse(&config, RESTORABLE), Some(Version::new(27, 3, 0)));
        assert!(!config.db_path(KIND, 27, &Network::Mainnet).exists());
        assert_eq!(version_on_disk(&config, 29), Some(Version::new(27, 3, 0)));
    }

    #[test]
    fn prefers_the_newest_older_database() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = config(&tempdir);
        create_db(&config, 27, Some("27.3.0"));
        create_db(&config, 28, Some("28.1.5"));

        assert_eq!(reuse(&config, RESTORABLE), Some(Version::new(28, 1, 5)));
        assert!(config.db_path(KIND, 27, &Network::Mainnet).exists());
    }

    #[test]
    fn does_not_skip_a_major_without_a_reusable_upgrade() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = config(&tempdir);
        create_db(&config, 27, Some("27.3.0"));

        assert_eq!(reuse(&config, &[27, 29]), None);
        assert!(config.db_path(KIND, 27, &Network::Mainnet).exists());
    }

    #[test]
    fn does_not_replace_an_existing_current_database() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = config(&tempdir);
        create_db(&config, 28, Some("28.1.5"));
        create_db(&config, 29, Some("29.0.0"));

        assert_eq!(reuse(&config, RESTORABLE), None);
        assert!(config.db_path(KIND, 28, &Network::Mainnet).exists());
    }
}
