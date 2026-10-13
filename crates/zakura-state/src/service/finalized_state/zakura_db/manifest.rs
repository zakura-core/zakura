//! Reads and writes the facts that the state database records about itself.
//!
//! See [`STATE_MANIFEST`] for the column family layout.

use semver::{BuildMetadata, Version};

use crate::{
    service::finalized_state::{
        disk_db::{DiskWriteBatch, ReadDisk, WriteDisk},
        RawBytes, ZakuraDb, STATE_MANIFEST,
    },
    BoxError,
};

/// The manifest key for the minimum reader version.
const MIN_READER_VERSION: &[u8] = b"min_reader_version";

impl ZakuraDb {
    /// Returns the oldest format version whose code may open this database.
    ///
    /// Startup refuses to open the database with older code. A database that this code has not
    /// opened read-write yet has no recorded version, and this method returns `Ok(None)`.
    pub fn min_reader_version(&self) -> Result<Option<Version>, BoxError> {
        let Some(manifest) = self.db.cf_handle(STATE_MANIFEST) else {
            return Ok(None);
        };

        let Some(value) = self.db.zs_get::<_, _, RawBytes>(
            &manifest,
            &RawBytes::new_raw_bytes(MIN_READER_VERSION.to_vec()),
        ) else {
            return Ok(None);
        };

        Ok(Some(std::str::from_utf8(value.raw_bytes())?.parse()?))
    }
}

impl DiskWriteBatch {
    /// Records `version` as the oldest format version whose code may open `db`.
    ///
    /// Build metadata does not affect compatibility, so this method drops it.
    pub(crate) fn update_min_reader_version(&mut self, db: &ZakuraDb, version: &Version) {
        let manifest = db
            .db
            .cf_handle(STATE_MANIFEST)
            .expect("read-write opens create the manifest column family");

        let version = Version {
            build: BuildMetadata::EMPTY,
            ..version.clone()
        };

        self.zs_insert(
            &manifest,
            RawBytes::new_raw_bytes(MIN_READER_VERSION.to_vec()),
            RawBytes::new_raw_bytes(version.to_string().into_bytes()),
        );
    }
}
