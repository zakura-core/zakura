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
