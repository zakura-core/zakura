//! Operational metadata about node software that writes the database.

use crate::service::finalized_state::{
    disk_db::{DiskWriteBatch, WriteDisk},
    FromDisk, IntoDisk, NODE_SOFTWARE_METADATA,
};

use super::ZakuraDb;

const LAST_WRITER_SOFTWARE_KEY: MetadataKey = MetadataKey("last_writer.software");
const LAST_WRITER_VERSION_KEY: MetadataKey = MetadataKey("last_writer.version");
const LAST_WRITER_LAST_KNOWN_TAG_KEY: MetadataKey = MetadataKey("last_writer.last_known_tag");

#[derive(Clone, Copy, Debug)]
struct MetadataKey(&'static str);

impl IntoDisk for MetadataKey {
    type Bytes = Vec<u8>;

    fn as_bytes(&self) -> Self::Bytes {
        self.0.as_bytes().to_vec()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MetadataValue(String);

impl IntoDisk for MetadataValue {
    type Bytes = Vec<u8>;

    fn as_bytes(&self) -> Self::Bytes {
        self.0.as_bytes().to_vec()
    }
}

impl FromDisk for MetadataValue {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        Self(String::from_utf8_lossy(bytes.as_ref()).into_owned())
    }
}

/// Metadata identifying the node software that most recently opened the
/// database writable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatabaseWriterMetadata {
    software: String,
    version: String,
    last_known_tag: String,
}

impl DatabaseWriterMetadata {
    /// Creates metadata for a database writer.
    pub fn new(
        software: impl Into<String>,
        version: impl Into<String>,
        last_known_tag: impl Into<String>,
    ) -> Self {
        Self {
            software: software.into(),
            version: version.into(),
            last_known_tag: last_known_tag.into(),
        }
    }

    /// Creates default Zakura metadata for callers that do not know their node
    /// software version.
    pub fn default_zakura() -> Self {
        Self::new("Zakura", "unknown", "")
    }

    /// Returns the node software name.
    pub fn software(&self) -> &str {
        &self.software
    }

    /// Returns the node software version.
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Returns the last known release tag for this node software.
    pub fn last_known_tag(&self) -> &str {
        &self.last_known_tag
    }
}

impl ZakuraDb {
    /// Records the node software that most recently opened this database writable.
    pub fn record_database_writer_metadata(
        &self,
        metadata: &DatabaseWriterMetadata,
    ) -> Result<(), crate::BoxError> {
        let metadata_cf = self
            .db
            .cf_handle(NODE_SOFTWARE_METADATA)
            .ok_or("node software metadata column family is missing")?;

        let mut batch = DiskWriteBatch::new();
        batch.zs_insert(
            &metadata_cf,
            LAST_WRITER_SOFTWARE_KEY,
            MetadataValue(metadata.software.clone()),
        );
        batch.zs_insert(
            &metadata_cf,
            LAST_WRITER_VERSION_KEY,
            MetadataValue(metadata.version.clone()),
        );
        batch.zs_insert(
            &metadata_cf,
            LAST_WRITER_LAST_KNOWN_TAG_KEY,
            MetadataValue(metadata.last_known_tag.clone()),
        );

        self.db.write(batch).map_err(Into::into)
    }

    /// Returns the recorded node software metadata, if all fields are present.
    pub fn database_writer_metadata(
        &self,
    ) -> Result<Option<DatabaseWriterMetadata>, rocksdb::Error> {
        let Some(metadata_cf) = self.db.cf_handle(NODE_SOFTWARE_METADATA) else {
            return Ok(None);
        };
        let read = |key: MetadataKey| {
            self.db
                .raw_get_cf(&metadata_cf, key.0.as_bytes())
                .map(|value| value.map(MetadataValue::from_bytes))
        };
        let Some(software) = read(LAST_WRITER_SOFTWARE_KEY)? else {
            return Ok(None);
        };
        let Some(version) = read(LAST_WRITER_VERSION_KEY)? else {
            return Ok(None);
        };
        let Some(last_known_tag) = read(LAST_WRITER_LAST_KNOWN_TAG_KEY)? else {
            return Ok(None);
        };
        Ok(Some(DatabaseWriterMetadata::new(
            software.0,
            version.0,
            last_known_tag.0,
        )))
    }
}

#[cfg(test)]
mod tests {
    use semver::Version;
    use zakura_chain::parameters::Network;

    use crate::{
        constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
        service::finalized_state::{
            ZakuraDb, NODE_SOFTWARE_METADATA, STATE_COLUMN_FAMILIES_IN_CODE,
        },
        Config,
    };

    use super::DatabaseWriterMetadata;

    fn open_with_metadata(
        config: &Config,
        read_only: bool,
        metadata: &DatabaseWriterMetadata,
    ) -> ZakuraDb {
        ZakuraDb::new_with_database_writer_metadata(
            config,
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &Network::Mainnet,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            read_only,
            Some(metadata),
        )
        .expect("opening the finalized state database should succeed")
    }

    #[test]
    fn default_zakura_does_not_invent_a_node_version() {
        let metadata = DatabaseWriterMetadata::default_zakura();

        assert_eq!(metadata.software(), "Zakura");
        assert_eq!(metadata.version(), "unknown");
        assert_eq!(metadata.last_known_tag(), "");
    }

    #[test]
    fn writable_open_records_database_writer_metadata() {
        let _init_guard = zakura_test::init();
        let metadata = DatabaseWriterMetadata::new("Zakura", "1.2.3+4.gabcdef123456", "v1.2.3");

        let db = open_with_metadata(&Config::ephemeral(), false, &metadata);

        assert_eq!(
            db.database_writer_metadata()
                .expect("metadata read succeeds"),
            Some(metadata)
        );
    }

    #[test]
    fn read_only_open_does_not_overwrite_database_writer_metadata() {
        let _init_guard = zakura_test::init();
        let tempdir = tempfile::tempdir().expect("temporary cache directory is created");
        let config = Config {
            cache_dir: tempdir.path().to_path_buf(),
            ephemeral: false,
            ..Config::default()
        };
        let original = DatabaseWriterMetadata::new("Zakura", "1.2.3+4.gabcdef123456", "v1.2.3");
        let attempted = DatabaseWriterMetadata::new("Zakura", "9.9.9+1.gffffffffffff", "v9.9.9");

        {
            let mut db = open_with_metadata(&config, false, &original);
            assert_eq!(
                db.database_writer_metadata()
                    .expect("metadata read succeeds"),
                Some(original.clone())
            );
            db.shutdown(true);
        }

        let db = open_with_metadata(&config, true, &attempted);

        assert_eq!(
            db.database_writer_metadata()
                .expect("metadata read succeeds"),
            Some(original)
        );
    }

    #[test]
    fn writable_reopen_updates_database_writer_metadata() {
        let _init_guard = zakura_test::init();
        let tempdir = tempfile::tempdir().expect("temporary cache directory is created");
        let config = Config {
            cache_dir: tempdir.path().to_path_buf(),
            ephemeral: false,
            ..Config::default()
        };
        let original = DatabaseWriterMetadata::new("Zakura", "1.2.3+4.gabcdef123456", "v1.2.3");
        let updated = DatabaseWriterMetadata::new("Other node", "unknown", "");

        {
            let mut db = open_with_metadata(&config, false, &original);
            assert_eq!(
                db.database_writer_metadata()
                    .expect("metadata read succeeds"),
                Some(original)
            );
            db.shutdown(true);
        }

        let db = open_with_metadata(&config, false, &updated);

        assert_eq!(
            db.database_writer_metadata()
                .expect("metadata read succeeds"),
            Some(updated)
        );
    }

    #[test]
    fn missing_metadata_cf_does_not_prevent_writable_open() {
        let _init_guard = zakura_test::init();
        let db = ZakuraDb::new(
            &Config::ephemeral(),
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &Network::Mainnet,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .filter(|name| **name != NODE_SOFTWARE_METADATA)
                .map(ToString::to_string),
            false,
        )
        .expect("metadata failure does not prevent database startup");
        assert_eq!(
            db.database_writer_metadata()
                .expect("metadata read succeeds"),
            None
        );
        assert!(db
            .record_database_writer_metadata(&DatabaseWriterMetadata::default_zakura())
            .is_err());
    }

    #[test]
    fn opening_old_database_creates_metadata_cf_and_records_writer() {
        check_metadata_upgrade(Version::new(28, 0, 1));
    }

    #[test]
    fn opening_previous_database_creates_metadata_cf_and_records_writer() {
        check_metadata_upgrade(Version::new(28, 1, 5));
    }

    fn check_metadata_upgrade(old_version: Version) {
        let _init_guard = zakura_test::init();
        let tempdir = tempfile::tempdir().expect("temporary cache directory is created");
        let config = Config {
            cache_dir: tempdir.path().to_path_buf(),
            ephemeral: false,
            ..Config::default()
        };
        let metadata = DatabaseWriterMetadata::new("Zakura", "1.2.4+1.g111111111111", "v1.2.4");

        {
            let column_families_without_metadata = STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .filter(|cf_name| **cf_name != NODE_SOFTWARE_METADATA)
                .map(ToString::to_string);
            let mut db = ZakuraDb::new_with_database_writer_metadata(
                &config,
                STATE_DATABASE_KIND,
                &old_version,
                &Network::Mainnet,
                true,
                column_families_without_metadata,
                false,
                None,
            )
            .expect("old database opens without node software metadata");

            db.update_format_version_on_disk(&old_version)
                .expect("test database version is set to the old format");
            assert_eq!(
                db.database_writer_metadata()
                    .expect("metadata read succeeds"),
                None
            );
            db.shutdown(true);
        }

        {
            let db = open_with_metadata(&config, true, &metadata);
            assert_eq!(
                db.database_writer_metadata()
                    .expect("metadata read succeeds"),
                None
            );
            assert!(db.db.cf_handle(NODE_SOFTWARE_METADATA).is_none());
            assert_eq!(
                db.format_version_on_disk()
                    .expect("version remains readable"),
                Some(old_version)
            );
        }

        let db = ZakuraDb::new_with_database_writer_metadata(
            &config,
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &Network::Mainnet,
            false,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
            Some(&metadata),
        )
        .expect("current database opens and upgrades from the old format");

        assert_eq!(
            db.database_writer_metadata()
                .expect("metadata read succeeds"),
            Some(metadata)
        );
        assert_eq!(
            db.format_version_on_disk()
                .expect("version remains readable"),
            Some(state_database_format_version_in_code())
        );
    }
}
