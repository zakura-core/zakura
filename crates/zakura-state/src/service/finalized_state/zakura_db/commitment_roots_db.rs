//! State-owned access to the commitment-root index.
//!
//! This module is the lifecycle boundary for commitment-root rows. It keeps
//! disk-row conversion, contiguous reads, and the distinct body, reorganization,
//! rollback, and repair write policies in one place.

use std::ops::{Bound, RangeBounds};

use zakura_chain::{block::Height, parallel::commitment_aux::BlockCommitmentRoots};

use crate::service::finalized_state::{
    disk_db::{DiskWriteBatch, ReadDisk, WriteDisk},
    disk_format::{block::HEIGHT_DISK_BYTES, shielded::CommitmentRootsByHeight, RawBytes},
    FromDisk, IntoDisk, TypedColumnFamily,
};

use super::ZakuraDb;

/// The name of the per-height commitment-root column family.
pub const COMMITMENT_ROOTS_BY_HEIGHT: &str = "commitment_roots_by_height";

/// The name the retired header-root authentication frontier column family was created under.
///
/// Header-time root authentication now records its verdicts as header-chain auxiliary
/// evidence, so nothing reads or writes this column family. It is absent from
/// [`STATE_COLUMN_FAMILIES_IN_CODE`], so only a database written by an earlier version has it.
///
/// [`STATE_COLUMN_FAMILIES_IN_CODE`]: crate::service::finalized_state::STATE_COLUMN_FAMILIES_IN_CODE
pub(crate) const RETIRED_HEADER_ROOT_AUTH_FRONTIER: &str = "header_root_auth_frontier";

type CommitmentRootsCf<'cf> = TypedColumnFamily<'cf, Height, CommitmentRootsByHeight>;

/// A root-index row that prevents historical treestate derivation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitmentRootIndexIssue {
    /// The expected height has no canonical row.
    Missing(Height),
    /// The expected height has a noncanonical key or a value that is not a commitment-root row.
    Malformed(Height),
}

fn disk_row(roots: &BlockCommitmentRoots) -> CommitmentRootsByHeight {
    CommitmentRootsByHeight {
        sapling: roots.sapling_root,
        orchard: roots.orchard_root,
        auth_data_root: roots.auth_data_root,
        ironwood: roots.ironwood_root,
        sapling_tx: roots.sapling_tx,
        orchard_tx: roots.orchard_tx,
        ironwood_tx: roots.ironwood_tx,
    }
}

fn domain_roots(height: Height, row: CommitmentRootsByHeight) -> BlockCommitmentRoots {
    BlockCommitmentRoots {
        height,
        sapling_root: row.sapling,
        orchard_root: row.orchard,
        auth_data_root: row.auth_data_root,
        ironwood_root: row.ironwood,
        sapling_tx: row.sapling_tx,
        orchard_tx: row.orchard_tx,
        ironwood_tx: row.ironwood_tx,
    }
}

fn inclusive_bounds(range: impl RangeBounds<Height>) -> Option<(Height, Height)> {
    let start = match range.start_bound() {
        Bound::Included(height) => *height,
        Bound::Excluded(height) => height.next().ok()?,
        Bound::Unbounded => Height::MIN,
    };
    let end = match range.end_bound() {
        Bound::Included(height) => *height,
        Bound::Excluded(height) => height.previous().ok()?,
        Bound::Unbounded => Height::MAX,
    };

    (start <= end).then_some((start, end))
}

impl ZakuraDb {
    fn commitment_roots_cf(&self) -> CommitmentRootsCf<'_> {
        CommitmentRootsCf::new(&self.db, COMMITMENT_ROOTS_BY_HEIGHT)
            .expect("column family was created when database was created")
    }

    pub(super) fn has_commitment_roots_index(&self) -> bool {
        CommitmentRootsCf::new(&self.db, COMMITMENT_ROOTS_BY_HEIGHT).is_some()
    }

    /// Returns the commitment roots stored at `height`.
    pub fn commitment_roots(&self, height: Height) -> Option<BlockCommitmentRoots> {
        self.commitment_roots_cf()
            .zs_get(&height)
            .map(|row| domain_roots(height, row))
    }

    pub(crate) fn has_commitment_root_rows(&self) -> bool {
        !self.commitment_roots_cf().zs_is_empty()
    }

    /// Returns whether the retired header-root authentication frontier still holds its row.
    pub(crate) fn has_retired_header_root_auth_frontier_row(&self) -> bool {
        self.db
            .cf_handle(RETIRED_HEADER_ROOT_AUTH_FRONTIER)
            .is_some_and(|cf| !self.db.zs_is_empty(&cf))
    }

    /// Deletes the retired header-root authentication frontier row, if this database has one.
    pub(crate) fn clear_retired_header_root_auth_frontier(&self) -> Result<(), rocksdb::Error> {
        // Absent on any database created after the column family left the in-code list.
        let Some(cf) = self.db.cf_handle(RETIRED_HEADER_ROOT_AUTH_FRONTIER) else {
            return Ok(());
        };

        let mut batch = DiskWriteBatch::new();
        // The frontier was a singleton keyed by the empty byte string.
        batch.zs_delete(&cf, RawBytes::new_raw_bytes(Vec::new()));
        self.write_batch(batch)
    }

    /// Returns the contiguous stored prefix of `range`.
    ///
    /// The read stops at the first missing height. Every returned row is
    /// authoritative: authenticated by the header-root lane or derived from a
    /// committed body (the index stores no per-row provenance).
    pub fn commitment_roots_by_height_range(
        &self,
        range: impl RangeBounds<Height>,
    ) -> Vec<BlockCommitmentRoots> {
        self.contiguous_commitment_roots(range)
    }

    fn contiguous_commitment_roots(
        &self,
        range: impl RangeBounds<Height>,
    ) -> Vec<BlockCommitmentRoots> {
        let Some((start, end)) = inclusive_bounds(range) else {
            return Vec::new();
        };
        let cf = self.commitment_roots_cf();
        let mut roots = Vec::new();

        for height in (start.0..=end.0).map(Height) {
            let Some(row) = cf.zs_get(&height) else {
                break;
            };
            roots.push(domain_roots(height, row));
        }

        roots
    }

    /// Persists raw roots for test fixtures outside a larger transaction.
    ///
    /// Production rows are written by the body-commit path.
    #[cfg(any(test, feature = "proptest-impl"))]
    pub fn insert_zakura_header_commitment_roots(
        &self,
        roots: impl IntoIterator<Item = BlockCommitmentRoots>,
    ) -> Result<(), rocksdb::Error> {
        let mut batch = DiskWriteBatch::new();
        for roots in roots {
            batch.insert_unauthenticated_commitment_roots_for_test(self, &roots);
        }
        self.write_batch(batch)
    }

    /// Returns the first missing or malformed roots row in `range`, if any.
    ///
    /// Streams raw index entries instead of materialising rows, so this stays usable across a whole
    /// fast-synced absent band, where [`Self::commitment_roots_by_height_range`] would build a
    /// multi-hundred-megabyte vector.
    pub fn first_commitment_root_issue(
        &self,
        range: impl RangeBounds<Height>,
    ) -> Option<CommitmentRootIndexIssue> {
        let (start, end) = inclusive_bounds(range)?;
        let Some(commitment_roots) = self.db.cf_handle(COMMITMENT_ROOTS_BY_HEIGHT) else {
            return Some(CommitmentRootIndexIssue::Missing(start));
        };
        let start_key = RawBytes::new_raw_bytes(start.as_bytes().to_vec());
        let end_key = RawBytes::new_raw_bytes(end.as_bytes().to_vec());

        let mut expected = start;
        for (key, raw_value) in self.db.zs_forward_range_iter::<_, RawBytes, RawBytes, _>(
            &commitment_roots,
            start_key..=end_key,
        ) {
            if key.raw_bytes().len() != HEIGHT_DISK_BYTES {
                return Some(CommitmentRootIndexIssue::Malformed(expected));
            }

            let height = Height::from_bytes(key.raw_bytes());
            if height != expected {
                return Some(CommitmentRootIndexIssue::Missing(expected));
            }
            if raw_value.raw_bytes().len()
                != std::mem::size_of::<<CommitmentRootsByHeight as IntoDisk>::Bytes>()
            {
                return Some(CommitmentRootIndexIssue::Malformed(height));
            }

            // The last height in the range has no successor to expect, and `next()` would
            // overflow at `Height::MAX`.
            if height == end {
                return None;
            }

            // Unreachable while the `height == end` return above precedes it, but an overflow
            // must never read as a clean scan.
            expected = match height.next() {
                Ok(next) => next,
                Err(_) => return Some(CommitmentRootIndexIssue::Malformed(height)),
            };
        }

        // The iterator ran out before reaching `end`, so the gap starts wherever it stopped.
        Some(CommitmentRootIndexIssue::Missing(expected))
    }

    /// Returns at most `limit` root heights for startup repair.
    pub(crate) fn commitment_root_heights_for_repair(
        &self,
        start: Height,
        limit: usize,
    ) -> Vec<Height> {
        self.commitment_roots_cf()
            .zs_forward_range_iter(start..)
            .map(|(height, _row)| height)
            .take(limit)
            .collect()
    }

    /// Visits root rows in `range` for rollback and migration bookkeeping.
    pub(super) fn visit_commitment_roots_for_migration(
        &self,
        range: impl RangeBounds<Height>,
        mut visit: impl FnMut(Height, BlockCommitmentRoots),
    ) {
        for (height, row) in self.commitment_roots_cf().zs_forward_range_iter(range) {
            visit(height, domain_roots(height, row));
        }
    }
}

impl DiskWriteBatch {
    /// Inserts or replaces an authoritative body-derived commitment-root row.
    pub fn insert_body_derived_commitment_roots(
        &mut self,
        db: &ZakuraDb,
        roots: &BlockCommitmentRoots,
    ) {
        let _ = db
            .commitment_roots_cf()
            .with_batch_for_writing(self)
            .zs_insert(&roots.height, &disk_row(roots));
    }

    pub(crate) fn truncate_all_commitment_roots(&mut self, db: &ZakuraDb) {
        let writer = db
            .commitment_roots_cf()
            .with_batch_for_writing(self)
            .zs_delete_range(&Height::MIN, &Height::MAX)
            .zs_delete(&Height::MAX);
        let _ = writer;
    }

    /// Inserts a raw fixture row unless a committed body owns the height.
    ///
    /// Test-only: production writes go through the sealed verified-root boundary
    /// ([`ZakuraDb::write_verified_header_commitment_roots`]) or the body-commit path.
    #[cfg(any(test, feature = "proptest-impl"))]
    pub(super) fn insert_unauthenticated_commitment_roots_for_test(
        &mut self,
        db: &ZakuraDb,
        roots: &BlockCommitmentRoots,
    ) {
        if db.contains_height(roots.height) {
            return;
        }

        let _ = db
            .commitment_roots_cf()
            .with_batch_for_writing(self)
            .zs_insert(&roots.height, &disk_row(roots));
    }

    /// Deletes the header-supplied row superseded by a committed body.
    ///
    /// Only body commit calls this, and the same batch writes the authoritative
    /// body-derived replacement row, so the index never loses contiguity.
    pub(super) fn delete_superseded_header_commitment_root(
        &mut self,
        db: &ZakuraDb,
        height: Height,
    ) {
        let _ = db
            .commitment_roots_cf()
            .with_batch_for_writing(self)
            .zs_delete(&height);
    }

    /// Deletes the inclusive header-supplied suffix displaced by a header reorganization.
    pub(super) fn delete_header_reorg_commitment_roots(
        &mut self,
        db: &ZakuraDb,
        start: Height,
        end: Height,
    ) {
        if start > end {
            return;
        }

        let mut writer = db.commitment_roots_cf().with_batch_for_writing(self);
        for height in (start.0..=end.0).map(Height) {
            writer = writer.zs_delete(&height);
        }
        let _ = writer;
    }

    /// Truncates authoritative rows strictly above a finalized rollback target.
    pub(crate) fn truncate_commitment_roots_after(&mut self, db: &ZakuraDb, target: Height) {
        let Ok(start) = target.next() else {
            return;
        };
        let writer = db
            .commitment_roots_cf()
            .with_batch_for_writing(self)
            .zs_delete_range(&start, &Height::MAX)
            .zs_delete(&Height::MAX);
        let _ = writer;
    }

    /// Deletes one row selected by startup repair or a database migration.
    pub(super) fn delete_commitment_root_for_repair(&mut self, db: &ZakuraDb, height: Height) {
        let _ = db
            .commitment_roots_cf()
            .with_batch_for_writing(self)
            .zs_delete(&height);
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    /// Inserts a body-derived row assembled from separate test fixture fields.
    pub fn insert_commitment_roots_by_height(
        &mut self,
        db: &ZakuraDb,
        height: Height,
        sapling_root: &zakura_chain::sapling::tree::Root,
        orchard_root: &zakura_chain::orchard::tree::Root,
        ironwood_root: &zakura_chain::ironwood::tree::Root,
        sapling_tx: u64,
        orchard_tx: u64,
        ironwood_tx: u64,
        auth_data_root: &zakura_chain::block::merkle::AuthDataRoot,
    ) {
        self.insert_body_derived_commitment_roots(
            db,
            &BlockCommitmentRoots {
                height,
                sapling_root: *sapling_root,
                orchard_root: *orchard_root,
                auth_data_root: *auth_data_root,
                ironwood_root: *ironwood_root,
                sapling_tx,
                orchard_tx,
                ironwood_tx,
            },
        );
    }

    #[cfg(test)]
    /// Deletes the half-open row range used by legacy serving tests.
    pub fn delete_range_commitment_roots_by_height(
        &mut self,
        db: &ZakuraDb,
        from: &Height,
        until_strictly_before: &Height,
    ) {
        let _ = db
            .commitment_roots_cf()
            .with_batch_for_writing(self)
            .zs_delete_range(from, until_strictly_before);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{
        constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
        service::finalized_state::{
            HighestCompletedCheckpoint, HighestCompletedCheckpointTracker, WriteDisk,
            STATE_COLUMN_FAMILIES_IN_CODE,
        },
        Config,
    };
    use zakura_chain::{
        block::Block,
        parameters::{testnet, Network},
        serialization::ZcashDeserializeInto,
        work::difficulty::ParameterDifficulty,
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

    fn mainnet_block_at(height: u32) -> Arc<Block> {
        let (blocks, _) = Network::Mainnet.block_sapling_roots_map();
        Arc::new(
            blocks
                .get(&height)
                .expect("test vector block exists")
                .zcash_deserialize_into()
                .expect("test vector block deserializes"),
        )
    }

    /// Builds a database whose only committed body is genesis, bracketed by a configured
    /// checkpoint at height 2 that the stored headers 1 and 2 complete.
    fn two_block_checkpoint_fixture_with_config(
        config: &Config,
    ) -> (ZakuraDb, HighestCompletedCheckpoint) {
        let genesis = mainnet_block_at(0);
        let block1 = mainnet_block_at(1);
        let block2 = mainnet_block_at(2);
        let network = testnet::Parameters::build()
            .with_network_name("RootAuthTest")
            .expect("test network name is valid")
            .with_genesis_hash(genesis.hash())
            .expect("genesis hash is valid")
            .with_target_difficulty_limit(Network::Mainnet.target_difficulty_limit())
            .expect("difficulty limit is valid")
            .with_activation_heights(testnet::ConfiguredActivationHeights {
                heartwood: Some(2),
                canopy: Some(2),
                ..Default::default()
            })
            .expect("activation heights are valid")
            .clear_funding_streams()
            .with_checkpoints(testnet::ConfiguredCheckpoints::HeightsAndHashes(vec![
                (Height::MIN, genesis.hash()),
                (Height(2), block2.hash()),
            ]))
            .expect("linked checkpoints are valid")
            .to_network()
            .expect("test network is valid");
        let db = ZakuraDb::new(
            config,
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &network,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        )
        .expect("ephemeral database opens");
        let hash_by_height = db.db.cf_handle("hash_by_height").unwrap();
        let height_by_hash = db.db.cf_handle("height_by_hash").unwrap();
        let block_header_by_height = db.db.cf_handle("block_header_by_height").unwrap();
        let mut batch = DiskWriteBatch::new();
        batch.zs_insert(&hash_by_height, Height::MIN, genesis.hash());
        batch.zs_insert(&height_by_hash, genesis.hash(), Height::MIN);
        batch.zs_insert(&block_header_by_height, Height::MIN, &genesis.header);
        db.write_batch(batch).expect("genesis rows write");

        let header_hash_by_height = db.db.cf_handle("zakura_header_hash_by_height").unwrap();
        let header_height_by_hash = db.db.cf_handle("zakura_header_height_by_hash").unwrap();
        let header_by_height = db.db.cf_handle("zakura_header_by_height").unwrap();
        let mut batch = DiskWriteBatch::new();
        let linked = [(Height(1), block1.clone()), (Height(2), block2.clone())];
        for (height, block) in &linked {
            let hash = block.hash();
            batch.zs_insert(&header_hash_by_height, height, hash);
            batch.zs_insert(&header_height_by_hash, hash, height);
            batch.zs_insert(&header_by_height, height, &block.header);
        }
        db.write_batch(batch).expect("linked headers write");
        let (tracker, _receiver) = HighestCompletedCheckpointTracker::open(&db);
        let completed = tracker
            .current()
            .expect("linked fixture completes the height 2 checkpoint");
        (db, completed)
    }

    #[test]
    fn production_root_column_access_is_centralized() {
        let production_sources = [
            ("block.rs", include_str!("block.rs")),
            ("shielded.rs", include_str!("shielded.rs")),
            ("rollback.rs", include_str!("rollback.rs")),
        ];

        for (path, source) in production_sources {
            let compact = source.split_whitespace().collect::<String>();
            assert!(
                !compact.contains("cf_handle(COMMITMENT_ROOTS_BY_HEIGHT)"),
                "{path} accesses the commitment-root column family directly",
            );
        }
    }

    #[test]
    fn startup_repair_reconstructs_checkpoint_after_deleting_covered_headers() {
        let cache = tempfile::tempdir().expect("temporary cache directory is created");
        let config = Config {
            cache_dir: cache.path().to_owned(),
            ephemeral: false,
            ..Config::default()
        };
        let (mut db, completed) = two_block_checkpoint_fixture_with_config(&config);
        assert_eq!(completed.height, Height(2));

        let header_hash_by_height = db.db.cf_handle("zakura_header_hash_by_height").unwrap();
        let header_by_height = db.db.cf_handle("zakura_header_by_height").unwrap();
        let mut batch = DiskWriteBatch::new();
        batch.zs_delete(&header_hash_by_height, Height(1));
        batch.zs_delete(&header_by_height, Height(1));
        db.write_batch(batch)
            .expect("interior checkpoint corruption writes");
        db.update_format_version_on_disk(&state_database_format_version_in_code())
            .expect("fixture format version writes");

        let network = db.network();
        db.shutdown(true);
        drop(db);
        let db = ZakuraDb::new(
            &config,
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &network,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        )
        .expect("database reopens after repairing the checkpoint bracket");

        let (tracker, _receiver) = HighestCompletedCheckpointTracker::open(&db);
        let repaired = tracker
            .current()
            .expect("repaired database retains the genesis checkpoint");
        assert_eq!(
            repaired.height,
            Height::MIN,
            "repair must not preserve a checkpoint whose header bracket was deleted"
        );
        assert_eq!(repaired.hash, db.network().genesis_hash());
    }

    #[test]
    fn first_commitment_root_issue_finds_the_first_missing_height() {
        let _init_guard = zakura_test::init();
        let db = ephemeral_mainnet_db();

        let roots = |height: u32| BlockCommitmentRoots {
            height: Height(height),
            sapling_root: Default::default(),
            orchard_root: Default::default(),
            ironwood_root: Default::default(),
            auth_data_root: zakura_chain::block::merkle::AuthDataRoot::from([0; 32]),
            sapling_tx: 0,
            orchard_tx: 0,
            ironwood_tx: 0,
        };

        db.insert_zakura_header_commitment_roots((1..=5).map(roots))
            .expect("seeding roots succeeds");

        assert_eq!(
            db.first_commitment_root_issue(Height(1)..=Height(5)),
            None,
            "a fully stored range has no gap"
        );
        assert_eq!(
            db.first_commitment_root_issue(Height(1)..=Height(7)),
            Some(CommitmentRootIndexIssue::Missing(Height(6))),
            "a range running past the stored rows reports where they stop"
        );
        assert_eq!(
            db.first_commitment_root_issue(Height(0)..=Height(5)),
            Some(CommitmentRootIndexIssue::Missing(Height(0))),
            "a missing first height is reported, not skipped"
        );

        // Punch a hole in the middle: this is the case that would silently serve a wrong
        // treestate if the scan only checked the range's endpoints.
        let mut batch = DiskWriteBatch::new();
        batch.delete_range_commitment_roots_by_height(&db, &Height(3), &Height(4));
        db.write_batch(batch).expect("deleting a row succeeds");

        assert_eq!(
            db.first_commitment_root_issue(Height(1)..=Height(5)),
            Some(CommitmentRootIndexIssue::Missing(Height(3))),
            "an interior hole is found"
        );
        assert_eq!(
            db.first_commitment_root_issue(Height(4)..=Height(5)),
            None,
            "a range above the hole is still gap-free"
        );
        assert_eq!(
            db.first_commitment_root_issue(Height(5)..=Height(4)),
            None,
            "an empty range has no gap"
        );
    }

    #[test]
    fn first_commitment_root_issue_validates_raw_entries() {
        let _init_guard = zakura_test::init();
        let db = ephemeral_mainnet_db();
        let roots = |height: u32| BlockCommitmentRoots {
            height: Height(height),
            sapling_root: Default::default(),
            orchard_root: Default::default(),
            ironwood_root: Default::default(),
            auth_data_root: zakura_chain::block::merkle::AuthDataRoot::from([0; 32]),
            sapling_tx: 0,
            orchard_tx: 0,
            ironwood_tx: 0,
        };
        db.insert_zakura_header_commitment_roots((1..=3).map(roots))
            .expect("seeding roots succeeds");

        let roots_cf = db
            .db
            .cf_handle(COMMITMENT_ROOTS_BY_HEIGHT)
            .expect("test database has the roots column family");
        let mut batch = DiskWriteBatch::new();
        batch.zs_insert(&roots_cf, Height(2), RawBytes::new_raw_bytes(vec![0xff]));
        db.write_batch(batch)
            .expect("writing a malformed value succeeds");

        assert_eq!(
            db.first_commitment_root_issue(Height(1)..=Height(3)),
            Some(CommitmentRootIndexIssue::Malformed(Height(2))),
            "a malformed value is corrupt input, not a present row"
        );

        db.insert_zakura_header_commitment_roots([roots(2)])
            .expect("restoring valid roots succeeds");
        let mut batch = DiskWriteBatch::new();
        batch.zs_insert(
            &roots_cf,
            RawBytes::new_raw_bytes(vec![0, 0, 1, 0]),
            RawBytes::new_raw_bytes(vec![0]),
        );
        db.write_batch(batch)
            .expect("writing a noncanonical key succeeds");

        assert_eq!(
            db.first_commitment_root_issue(Height(1)..=Height(3)),
            Some(CommitmentRootIndexIssue::Malformed(Height(2))),
            "a noncanonical key is corrupt input, not a canonical height"
        );
    }
}
