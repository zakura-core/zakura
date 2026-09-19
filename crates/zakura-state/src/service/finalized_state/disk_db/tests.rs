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

fn wait_for_child(child: &mut std::process::Child) -> std::process::ExitStatus {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("database test subprocess exceeded 30 seconds");
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

#[derive(Clone, Copy, PartialEq)]
pub(super) enum ReuseStep {
    Checkpoint,
    Version,
    Publish,
}

type ReuseHook = Box<dyn FnMut(ReuseStep, &std::path::Path, &std::path::Path)>;
thread_local! {
    static REUSE_HOOK: std::cell::RefCell<Option<ReuseHook>> = const { std::cell::RefCell::new(None) };
}

pub(super) fn reuse_step(step: ReuseStep, staged: &std::path::Path, destination: &std::path::Path) {
    REUSE_HOOK.with_borrow_mut(|hook| {
        if let Some(hook) = hook {
            hook(step, staged, destination);
        }
    });
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
        try_reuse(config, restorable).unwrap()
    }

    fn try_reuse(
        config: &Config,
        restorable: &[u64],
    ) -> Result<Option<Version>, crate::StateInitError> {
        let guard =
            super::super::DatabaseStartupGuard::acquire(config, KIND, &Network::Mainnet, false)?;
        DiskDb::try_reusing_previous_db_after_major_upgrade(
            restorable,
            &Version::new(29, 0, 0),
            config,
            KIND,
            &Network::Mainnet,
            &guard,
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
            // Publication includes the full source version, so an interruption
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
        assert!(config.db_path(KIND, 27, &Network::Mainnet).exists());
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
    #[test]
    fn checkpoint_runs_pending_migrations_and_reopens_current_state() {
        use crate::{
            constants::state_database_format_version_in_code,
            service::finalized_state::{zakura_db::ZakuraDb, STATE_COLUMN_FAMILIES_IN_CODE},
        };
        let tempdir = tempfile::tempdir().unwrap();
        let config = config(&tempdir);
        create_db(&config, 27, Some("27.3.0"));
        let version = state_database_format_version_in_code();
        let open = || {
            ZakuraDb::new(
                &config,
                KIND,
                &version,
                &Network::Mainnet,
                false,
                STATE_COLUMN_FAMILIES_IN_CODE
                    .iter()
                    .map(ToString::to_string),
                false,
            )
            .unwrap()
        };
        let db = open();
        assert_eq!(db.format_version_on_disk().unwrap(), Some(version.clone()));
        assert_eq!(version_on_disk(&config, 27), Some(Version::new(27, 3, 0)));
        drop(db);
        let db = open();
        assert_eq!(db.format_version_on_disk().unwrap(), Some(version));
    }

    #[test]
    fn skips_malformed_candidates_but_does_not_hide_all_invalid_caches() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = config(&tempdir);
        create_db(&config, 28, Some("not-a-version"));
        assert!(try_reuse(&config, RESTORABLE).is_err());
        assert!(!config.db_path(KIND, 29, &Network::Mainnet).exists());
        create_db(&config, 27, Some("27.3.0"));
        assert_eq!(reuse(&config, RESTORABLE), Some(Version::new(27, 3, 0)));
        assert_eq!(
            fs::read_to_string(config.version_file_path(KIND, 28, &Network::Mainnet)).unwrap(),
            "not-a-version"
        );
    }

    #[test]
    fn skips_ineligible_versions_and_structurally_invalid_candidates() {
        for version in ["26.0.0", "30.0.0"] {
            let tempdir = tempfile::tempdir().unwrap();
            let config = config(&tempdir);
            create_db(&config, 28, Some(version));
            assert!(try_reuse(&config, &[28, 29]).is_err());
            create_db(&config, 27, Some("27.0.0"));
            assert_eq!(reuse(&config, &[28, 29]), Some(Version::new(27, 0, 0)));
        }
        let tempdir = tempfile::tempdir().unwrap();
        let config = config(&tempdir);
        fs::create_dir_all(config.db_path(KIND, 28, &Network::Mainnet)).unwrap();
        create_db(&config, 27, Some("27.0.0"));
        assert_eq!(reuse(&config, RESTORABLE), Some(Version::new(27, 0, 0)));
    }

    #[test]
    fn does_not_replace_an_invalid_current_path() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = config(&tempdir);
        create_db(&config, 28, Some("28.1.5"));
        let current = config.db_path(KIND, 29, &Network::Mainnet);
        fs::create_dir_all(&current).unwrap();
        fs::write(current.join("sentinel"), "preserve").unwrap();
        assert!(try_reuse(&config, RESTORABLE).is_err());
        assert_eq!(
            fs::read_to_string(current.join("sentinel")).unwrap(),
            "preserve"
        );
    }

    #[test]
    fn checkpoint_preserves_source_siblings_and_all_column_families() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = config(&tempdir);
        create_db(&config, 28, Some("1.5"));
        let source_path = config.db_path(KIND, 28, &Network::Mainnet);
        let sibling = source_path.parent().unwrap().join("testnet");
        fs::create_dir_all(&sibling).unwrap();
        fs::write(sibling.join("sentinel"), "preserve").unwrap();
        {
            let source = DB::open_cf(&DiskDb::options(), &source_path, ["future_cf"]).unwrap();
            source
                .put_cf(source.cf_handle("future_cf").unwrap(), b"key", b"original")
                .unwrap();
            source.flush().unwrap();
        }
        assert_eq!(reuse(&config, RESTORABLE), Some(Version::new(28, 1, 5)));
        assert_eq!(
            fs::read_to_string(
                source_path.join(crate::constants::DATABASE_FORMAT_VERSION_FILE_NAME)
            )
            .unwrap(),
            "1.5"
        );
        assert_eq!(
            fs::read_to_string(sibling.join("sentinel")).unwrap(),
            "preserve"
        );
        let destination_path = config.db_path(KIND, 29, &Network::Mainnet);
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let sst = fs::read_dir(&source_path)
                .unwrap()
                .map(Result::unwrap)
                .find(|e| e.path().extension().is_some_and(|ext| ext == "sst"))
                .unwrap();
            assert_eq!(
                sst.metadata().unwrap().ino(),
                fs::metadata(destination_path.join(sst.file_name()))
                    .unwrap()
                    .ino()
            );
        }
        let source = DB::open_cf(&DiskDb::options(), &source_path, ["future_cf"]).unwrap();
        let destination =
            DB::open_cf(&DiskDb::options(), &destination_path, ["future_cf"]).unwrap();
        let source_cf = source.cf_handle("future_cf").unwrap();
        let destination_cf = destination.cf_handle("future_cf").unwrap();
        assert_eq!(
            destination.get_cf(destination_cf, b"key").unwrap().unwrap(),
            b"original"
        );
        destination
            .put_cf(destination_cf, b"key", b"destination")
            .unwrap();
        source.put_cf(source_cf, b"key", b"source").unwrap();
        assert_eq!(
            source.get_cf(source_cf, b"key").unwrap().unwrap(),
            b"source"
        );
        assert_eq!(
            destination.get_cf(destination_cf, b"key").unwrap().unwrap(),
            b"destination"
        );
    }

    #[test]
    fn startup_guard_serializes_writers_and_low_level_opens() {
        use super::super::DatabaseStartupGuard;
        let tempdir = tempfile::tempdir().unwrap();
        let config = config(&tempdir);
        create_db(&config, 28, Some("28.0.0"));
        let guard = DatabaseStartupGuard::acquire(&config, KIND, &Network::Mainnet, false).unwrap();
        let other = config.clone();
        std::thread::spawn(move || {
            assert!(try_reuse(&other, RESTORABLE).is_err());
            assert!(DiskDb::new(
                &other,
                KIND,
                &Version::new(29, 0, 0),
                &Network::Mainnet,
                [],
                false
            )
            .is_err());
        })
        .join()
        .unwrap();
        assert!(!config.db_path(KIND, 29, &Network::Mainnet).exists());
        drop(guard);
        assert_eq!(reuse(&config, RESTORABLE), Some(Version::new(28, 0, 0)));
    }

    #[test]
    fn unpublished_failures_preserve_source_and_retry() {
        use super::{ReuseStep, REUSE_HOOK};
        struct ClearHook;
        impl Drop for ClearHook {
            fn drop(&mut self) {
                REUSE_HOOK.with_borrow_mut(|hook| *hook = None);
            }
        }
        for step in [
            ReuseStep::Checkpoint,
            ReuseStep::Version,
            ReuseStep::Publish,
        ] {
            let tempdir = tempfile::tempdir().unwrap();
            let config = config(&tempdir);
            create_db(&config, 28, Some("28.0.0"));
            REUSE_HOOK.with_borrow_mut(|hook| {
                *hook = Some(Box::new(move |actual, staged, destination| {
                    if actual != step {
                        return;
                    }
                    match step {
                        ReuseStep::Checkpoint => fs::create_dir(staged).unwrap(),
                        ReuseStep::Version => fs::create_dir(
                            staged.join(crate::constants::DATABASE_FORMAT_VERSION_FILE_NAME),
                        )
                        .unwrap(),
                        ReuseStep::Publish => {
                            fs::write(destination, "external destination").unwrap();
                        }
                    }
                }))
            });
            let reset = ClearHook;
            assert!(try_reuse(&config, RESTORABLE).is_err());
            drop(reset);
            assert_eq!(version_on_disk(&config, 28), Some(Version::new(28, 0, 0)));
            let current = config.db_path(KIND, 29, &Network::Mainnet);
            if step == ReuseStep::Publish {
                assert_eq!(
                    fs::read_to_string(&current).unwrap(),
                    "external destination"
                );
                fs::remove_file(&current).unwrap();
            } else {
                assert!(!current.exists());
            }
            assert!(fs::read_dir(current.parent().unwrap())
                .unwrap()
                .next()
                .is_none());
            assert_eq!(reuse(&config, RESTORABLE), Some(Version::new(28, 0, 0)));
        }
    }

    /// Runs only when explicitly spawned by the parent test, in a separate process.
    #[test]
    fn locked_source_child() {
        let Some(path) = std::env::var_os("ZAKURA_REUSE_LOCK_TEST") else {
            return;
        };
        let path = std::path::PathBuf::from(path);
        let _source = DB::open(&DiskDb::options(), path.join("state/v28/mainnet")).unwrap();
        fs::write(path.join("ready"), "").unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !path.join("release").exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "parent must release the child within 30 seconds"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    #[test]
    fn live_source_process_is_never_moved_or_bypassed() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = config(&tempdir);
        create_db(&config, 28, Some("28.0.0"));
        create_db(&config, 27, Some("27.0.0"));
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "service::finalized_state::disk_db::tests::major_upgrade_reuse::locked_source_child"])
            .env("ZAKURA_REUSE_LOCK_TEST", tempdir.path()).spawn().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !tempdir.path().join("ready").exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "child must acquire the source lock"
            );
            assert!(
                child.try_wait().unwrap().is_none(),
                "child exited before acquiring the lock"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let result = try_reuse(&config, RESTORABLE);
        fs::write(tempdir.path().join("release"), "").unwrap();
        assert!(super::wait_for_child(&mut child).success());
        assert!(matches!(
            result,
            Err(crate::StateInitError::DatabaseOpen { .. })
        ));
        assert_eq!(version_on_disk(&config, 28), Some(Version::new(28, 0, 0)));
        assert!(!config.db_path(KIND, 29, &Network::Mainnet).exists());
        assert_eq!(reuse(&config, RESTORABLE), Some(Version::new(28, 0, 0)));
    }
}

#[test]
fn ephemeral_version_io_owns_exactly_one_directory_per_database() {
    use crate::{
        config::{database_format_version_on_disk, write_database_format_version_to_disk},
        constants::{
            state_database_format_version_in_code, DATABASE_FORMAT_VERSION_FILE_NAME,
            STATE_DATABASE_KIND,
        },
        service::finalized_state::{zakura_db::ZakuraDb, STATE_COLUMN_FAMILIES_IN_CODE},
    };
    use std::fs;
    // Isolate the OS temporary directory without changing this test process's environment.
    let Some(root) = std::env::var_os("ZAKURA_EPHEMERAL_PATH_TEST") else {
        let root = tempfile::tempdir().unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "service::finalized_state::disk_db::tests::ephemeral_version_io_owns_exactly_one_directory_per_database"])
            .env("ZAKURA_EPHEMERAL_PATH_TEST", root.path())
            .env("TMPDIR", root.path()).env("TMP", root.path()).env("TEMP", root.path())
            .spawn().unwrap();
        assert!(wait_for_child(&mut child).success());
        return;
    };
    let root = std::path::PathBuf::from(root);
    let config = Config::ephemeral();
    let version = state_database_format_version_in_code();
    let entries = || fs::read_dir(&root).unwrap().count();
    let before = entries();
    let guard = super::DatabaseStartupGuard::acquire(
        &config,
        STATE_DATABASE_KIND,
        &Network::Mainnet,
        false,
    )
    .unwrap();
    assert_eq!(
        DiskDb::try_reusing_previous_db_after_major_upgrade(
            &[27, 28, 29],
            &version,
            &config,
            STATE_DATABASE_KIND,
            &Network::Mainnet,
            &guard,
        )
        .unwrap(),
        None
    );
    assert_eq!(
        database_format_version_on_disk(
            &config,
            STATE_DATABASE_KIND,
            version.major,
            &Network::Mainnet
        )
        .unwrap(),
        None
    );
    write_database_format_version_to_disk(
        &config,
        STATE_DATABASE_KIND,
        version.major,
        &version,
        &Network::Mainnet,
    )
    .unwrap();
    assert_eq!(
        entries(),
        before,
        "config-only probes must not allocate temporary paths"
    );
    let open = || {
        ZakuraDb::new(
            &config,
            STATE_DATABASE_KIND,
            &version,
            &Network::Mainnet,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        )
        .unwrap()
    };
    let first = open();
    let second = open();
    assert_ne!(first.path(), second.path());
    assert_eq!(entries(), before + 2);
    let written = Version::new(28, 1, 5);
    first.update_format_version_on_disk(&written).unwrap();
    assert_eq!(
        first.format_version_on_disk().unwrap(),
        Some(written.clone())
    );
    assert_eq!(
        fs::read_to_string(first.path().join(DATABASE_FORMAT_VERSION_FILE_NAME)).unwrap(),
        written.to_string()
    );
    assert_eq!(
        second.format_version_on_disk().unwrap(),
        Some(Version::new(version.major, 0, 0))
    );
    assert_eq!(
        entries(),
        before + 2,
        "live version I/O must reuse the open database path"
    );
    let first_path = first.path().to_owned();
    drop(first);
    assert!(!first_path.exists());
    assert!(second.path().exists());
    drop(second);
    assert_eq!(entries(), before);
}
