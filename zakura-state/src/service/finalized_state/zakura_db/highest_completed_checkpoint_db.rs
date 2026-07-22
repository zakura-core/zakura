//! Durable tracking for completely stored canonical checkpoint brackets.

use std::{collections::BTreeMap, sync::Arc};

use thiserror::Error;
use zakura_chain::block::{self, Height};

use crate::service::finalized_state::{disk_db::DiskWriteBatch, RawBytes, TypedColumnFamily};

use super::ZakuraDb;

/// The column family containing the single highest completed checkpoint row.
pub const HIGHEST_COMPLETED_CHECKPOINT: &str = "highest_completed_checkpoint";

type HighestCompletedCheckpointCf<'cf> = TypedColumnFamily<'cf, RawBytes, RawBytes>;

const ROW_KEY: &[u8] = &[];
const FORMAT_VERSION: u8 = 1;
const ENCODED_LEN: usize = 1 + 4 + 32;

/// The highest configured checkpoint whose complete canonical bracket is durable.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct HighestCompletedCheckpoint {
    /// The completed configured checkpoint height.
    pub height: Height,
    /// The configured checkpoint hash stored canonically at `height`.
    pub hash: block::Hash,
}

/// Errors restoring or advancing the highest completed checkpoint.
#[derive(Debug, Error)]
pub enum HighestCompletedCheckpointError {
    /// The persisted row has an unsupported encoding.
    #[error("invalid highest completed checkpoint encoding")]
    InvalidEncoding,
    /// The persisted value is not a configured checkpoint.
    #[error("highest completed checkpoint {height:?} with hash {hash} is not configured")]
    InvalidCheckpoint {
        /// Recorded checkpoint height.
        height: Height,
        /// Recorded checkpoint hash.
        hash: block::Hash,
    },
    /// The persisted value does not match reconstruction from canonical headers.
    #[error(
        "highest completed checkpoint {stored:?} does not match reconstructed value {reconstructed:?}"
    )]
    Mismatch {
        /// Persisted highest completed checkpoint.
        stored: HighestCompletedCheckpoint,
        /// Reconstructed highest completed checkpoint.
        reconstructed: HighestCompletedCheckpoint,
    },
    /// A non-empty canonical store has no durable highest completed checkpoint.
    #[error("missing highest completed checkpoint for non-empty state")]
    Missing,
    /// A canonical header required to establish the trusted body base is missing.
    #[error("missing canonical header at trusted body height {height:?}")]
    MissingCanonicalHeader {
        /// Missing header height.
        height: Height,
    },
    /// A height operation overflowed.
    #[error("highest completed checkpoint height overflow")]
    HeightOverflow,
    /// The highest completed checkpoint could not be written.
    #[error("could not write highest completed checkpoint: {0}")]
    Storage(#[from] rocksdb::Error),
}

fn encode_highest_completed_checkpoint(checkpoint: HighestCompletedCheckpoint) -> RawBytes {
    let mut bytes = Vec::with_capacity(ENCODED_LEN);
    bytes.push(FORMAT_VERSION);
    bytes.extend_from_slice(&checkpoint.height.0.to_le_bytes());
    bytes.extend_from_slice(&checkpoint.hash.0);
    RawBytes::new_raw_bytes(bytes)
}

fn decode_highest_completed_checkpoint(
    bytes: &RawBytes,
) -> Result<HighestCompletedCheckpoint, HighestCompletedCheckpointError> {
    let bytes = bytes.raw_bytes();
    if bytes.len() != ENCODED_LEN || bytes[0] != FORMAT_VERSION {
        return Err(HighestCompletedCheckpointError::InvalidEncoding);
    }

    let height =
        Height(u32::from_le_bytes(bytes[1..5].try_into().map_err(
            |_| HighestCompletedCheckpointError::InvalidEncoding,
        )?));
    let hash = block::Hash(
        bytes[5..]
            .try_into()
            .map_err(|_| HighestCompletedCheckpointError::InvalidEncoding)?,
    );

    Ok(HighestCompletedCheckpoint { height, hash })
}

impl ZakuraDb {
    fn highest_completed_checkpoint_cf(&self) -> HighestCompletedCheckpointCf<'_> {
        HighestCompletedCheckpointCf::new(&self.db, HIGHEST_COMPLETED_CHECKPOINT)
            .expect("column family was created when database was created")
    }

    fn genesis_checkpoint(
        &self,
    ) -> Result<HighestCompletedCheckpoint, HighestCompletedCheckpointError> {
        let height = Height::MIN;
        let hash = self
            .network()
            .checkpoint_list()
            .hash(height)
            .ok_or(HighestCompletedCheckpointError::Missing)?;
        Ok(HighestCompletedCheckpoint { height, hash })
    }

    /// Loads the durable highest completed checkpoint and verifies that it names a configured checkpoint.
    pub(crate) fn try_highest_completed_checkpoint(
        &self,
    ) -> Result<Option<HighestCompletedCheckpoint>, HighestCompletedCheckpointError> {
        let checkpoint = self
            .highest_completed_checkpoint_cf()
            .zs_get(&RawBytes::new_raw_bytes(ROW_KEY.to_vec()))
            .as_ref()
            .map(decode_highest_completed_checkpoint)
            .transpose()?;

        if let Some(checkpoint) = checkpoint {
            if self.network().checkpoint_list().hash(checkpoint.height) != Some(checkpoint.hash) {
                return Err(HighestCompletedCheckpointError::InvalidCheckpoint {
                    height: checkpoint.height,
                    hash: checkpoint.hash,
                });
            }
        }

        Ok(checkpoint)
    }

    fn trusted_body_base(
        &self,
        canonical_tip: Height,
    ) -> Result<(HighestCompletedCheckpoint, Height, block::Hash), HighestCompletedCheckpointError>
    {
        let checkpoints = self.network().checkpoint_list();
        let Some(body_tip) = self.finalized_tip_height() else {
            let genesis = self.genesis_checkpoint()?;
            return Ok((genesis, genesis.height, genesis.hash));
        };
        let base_height = body_tip.min(canonical_tip);
        let base_hash = self.header_hash(base_height).ok_or(
            HighestCompletedCheckpointError::MissingCanonicalHeader {
                height: base_height,
            },
        )?;
        let completed = checkpoints
            .iter_cloned()
            .take_while(|(height, _)| *height <= base_height)
            .last()
            .map(|(height, hash)| HighestCompletedCheckpoint { height, hash })
            .ok_or(HighestCompletedCheckpointError::Missing)?;

        Ok((completed, base_height, base_hash))
    }

    fn advance_highest_completed_checkpoint_through(
        &self,
        mut completed: HighestCompletedCheckpoint,
        mut cursor_height: Height,
        mut cursor_hash: block::Hash,
        canonical_tip: Height,
        pending: &[(Height, block::Hash, Arc<block::Header>)],
    ) -> Result<HighestCompletedCheckpoint, HighestCompletedCheckpointError> {
        let checkpoints = self.network().checkpoint_list();
        if checkpoints.hash(completed.height) != Some(completed.hash) {
            return Err(HighestCompletedCheckpointError::InvalidCheckpoint {
                height: completed.height,
                hash: completed.hash,
            });
        }

        let pending: BTreeMap<_, _> = pending
            .iter()
            .map(|(height, hash, header)| (*height, (*hash, header)))
            .collect();
        let completed_height = completed.height;

        for (checkpoint_height, checkpoint_hash) in checkpoints
            .iter_cloned()
            .skip_while(|(height, _)| *height <= completed_height)
            .take_while(|(height, _)| *height <= canonical_tip)
        {
            let mut height = cursor_height
                .next()
                .map_err(|_| HighestCompletedCheckpointError::HeightOverflow)?;
            let mut bracket_complete = true;

            while height <= checkpoint_height {
                let item = pending
                    .get(&height)
                    .map(|(hash, header)| (*hash, (*header).clone()))
                    .or_else(|| self.header_by_height(height));
                let Some((hash, header)) = item else {
                    bracket_complete = false;
                    break;
                };
                if block::Hash::from(header.as_ref()) != hash
                    || header.previous_block_hash != cursor_hash
                {
                    bracket_complete = false;
                    break;
                }
                cursor_height = height;
                cursor_hash = hash;
                height = match height.next() {
                    Ok(next) => next,
                    Err(_) => break,
                };
            }

            if !bracket_complete || cursor_hash != checkpoint_hash {
                break;
            }

            completed = HighestCompletedCheckpoint {
                height: checkpoint_height,
                hash: checkpoint_hash,
            };
        }

        Ok(completed)
    }

    /// Reconstructs the highest completed checkpoint from canonical body and header rows.
    pub(crate) fn reconstruct_highest_completed_checkpoint(
        &self,
    ) -> Result<Option<HighestCompletedCheckpoint>, HighestCompletedCheckpointError> {
        let canonical_tip = match (self.finalized_tip_height(), self.best_header_tip()) {
            (Some(body), Some((headers, _))) => Some(body.max(headers)),
            (Some(body), None) => Some(body),
            (None, Some((headers, _))) => Some(headers),
            (None, None) => None,
        };
        let Some(canonical_tip) = canonical_tip else {
            return Ok(None);
        };

        let (completed, cursor_height, cursor_hash) = self.trusted_body_base(canonical_tip)?;
        self.advance_highest_completed_checkpoint_through(
            completed,
            cursor_height,
            cursor_hash,
            canonical_tip,
            &[],
        )
        .map(Some)
    }

    /// Fully validates the durable highest completed checkpoint against canonical headers.
    ///
    /// This performs a historical scan and is intended for startup validation,
    /// migration, and explicit repair, not steady-state writes.
    pub(crate) fn validate_highest_completed_checkpoint(
        &self,
    ) -> Result<Option<HighestCompletedCheckpoint>, HighestCompletedCheckpointError> {
        let stored = self.try_highest_completed_checkpoint()?;
        let reconstructed = self.reconstruct_highest_completed_checkpoint()?;
        match (stored, reconstructed) {
            (None, None) => Ok(None),
            (Some(stored), Some(reconstructed)) if stored == reconstructed => Ok(Some(stored)),
            (Some(stored), Some(reconstructed)) => Err(HighestCompletedCheckpointError::Mismatch {
                stored,
                reconstructed,
            }),
            (None, Some(_)) | (Some(_), None) => Err(HighestCompletedCheckpointError::Missing),
        }
    }

    /// Reconstructs and atomically persists the highest completed checkpoint during format migration.
    pub(crate) fn reconstruct_and_persist_highest_completed_checkpoint(
        &self,
    ) -> Result<(), HighestCompletedCheckpointError> {
        let Some(checkpoint) = self.reconstruct_highest_completed_checkpoint()? else {
            return Ok(());
        };
        let mut batch = DiskWriteBatch::new();
        batch.set_highest_completed_checkpoint(self, checkpoint);
        self.write_batch(batch)?;
        Ok(())
    }

    pub(crate) fn highest_completed_checkpoint_for_tip(
        &self,
        tip_height: Height,
        pending: &[(Height, block::Hash, Arc<block::Header>)],
    ) -> Result<HighestCompletedCheckpoint, HighestCompletedCheckpointError> {
        let (completed, cursor_height, cursor_hash) = self.trusted_body_base(tip_height)?;
        self.advance_highest_completed_checkpoint_through(
            completed,
            cursor_height,
            cursor_hash,
            tip_height,
            pending,
        )
    }

    #[cfg(test)]
    pub(crate) fn delete_highest_completed_checkpoint_for_test(&self) {
        let mut batch = DiskWriteBatch::new();
        let _ = self
            .highest_completed_checkpoint_cf()
            .with_batch_for_writing(&mut batch)
            .zs_delete(&RawBytes::new_raw_bytes(ROW_KEY.to_vec()));
        self.write_batch(batch)
            .expect("test highest completed checkpoint deletion must write successfully");
    }
}

impl DiskWriteBatch {
    pub(crate) fn set_highest_completed_checkpoint(
        &mut self,
        db: &ZakuraDb,
        checkpoint: HighestCompletedCheckpoint,
    ) {
        let _ = db
            .highest_completed_checkpoint_cf()
            .with_batch_for_writing(self)
            .zs_insert(
                &RawBytes::new_raw_bytes(ROW_KEY.to_vec()),
                &encode_highest_completed_checkpoint(checkpoint),
            );
    }

    pub(crate) fn clear_highest_completed_checkpoint(&mut self, db: &ZakuraDb) {
        let _ = db
            .highest_completed_checkpoint_cf()
            .with_batch_for_writing(self)
            .zs_delete(&RawBytes::new_raw_bytes(ROW_KEY.to_vec()));
    }

    /// Advances checkpoint progress in the same transaction as canonical headers.
    pub(crate) fn advance_highest_completed_checkpoint_for_header_range(
        &mut self,
        db: &ZakuraDb,
        headers: &[(Height, block::Hash, Arc<block::Header>)],
    ) -> Result<(), HighestCompletedCheckpointError> {
        let Some(last_height) = headers.last().map(|(height, _, _)| *height) else {
            return Ok(());
        };
        let stored = db.try_highest_completed_checkpoint()?;
        let (body_completed, body_height, body_hash) = db.trusted_body_base(last_height)?;
        let (completed, cursor_height, cursor_hash) = if stored
            .is_some_and(|stored| stored.height > body_completed.height)
        {
            let stored = stored
                .expect("stored highest completed checkpoint exists because it was just checked");
            (stored, stored.height, stored.hash)
        } else {
            (body_completed, body_height, body_hash)
        };
        let advanced = db.advance_highest_completed_checkpoint_through(
            completed,
            cursor_height,
            cursor_hash,
            last_height,
            headers,
        )?;

        if advanced != completed || stored.is_none() {
            self.set_highest_completed_checkpoint(db, advanced);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
        service::finalized_state::{disk_db::WriteDisk, STATE_COLUMN_FAMILIES_IN_CODE},
        Config,
    };
    use zakura_chain::{
        block::Block,
        parameters::{testnet, Network},
        serialization::ZcashDeserializeInto,
        work::difficulty::ParameterDifficulty,
    };

    fn mainnet_block(height: usize) -> Arc<Block> {
        zakura_test::vectors::CONTINUOUS_MAINNET_BLOCKS
            .values()
            .nth(height)
            .expect("requested test block exists")
            .zcash_deserialize_into()
            .expect("test block deserializes")
    }

    fn checkpoint_fixture(config: &Config) -> (ZakuraDb, Arc<Block>, Arc<Block>) {
        let genesis = mainnet_block(0);
        let block1 = mainnet_block(1);
        let block2 = mainnet_block(2);
        let network = testnet::Parameters::build()
            .with_network_name("HighestCompletedCheckpointTest")
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
        .expect("test database opens");

        let hash_by_height = db.db.cf_handle("hash_by_height").unwrap();
        let height_by_hash = db.db.cf_handle("height_by_hash").unwrap();
        let block_header_by_height = db.db.cf_handle("block_header_by_height").unwrap();
        let mut batch = DiskWriteBatch::new();
        batch.zs_insert(&hash_by_height, Height::MIN, genesis.hash());
        batch.zs_insert(&height_by_hash, genesis.hash(), Height::MIN);
        batch.zs_insert(&block_header_by_height, Height::MIN, &genesis.header);
        batch.set_highest_completed_checkpoint(
            &db,
            HighestCompletedCheckpoint {
                height: Height::MIN,
                hash: genesis.hash(),
            },
        );
        db.write_batch(batch).expect("genesis fixture writes");

        (db, block1, block2)
    }

    #[test]
    fn linked_bracket_advances_and_survives_restart() {
        let cache = tempfile::tempdir().expect("temporary cache directory is created");
        let config = Config {
            cache_dir: cache.path().to_owned(),
            ephemeral: false,
            ..Config::default()
        };
        let (db, block1, block2) = checkpoint_fixture(&config);
        let network = db.network();
        let hash_by_height = db.db.cf_handle("zakura_header_hash_by_height").unwrap();
        let height_by_hash = db.db.cf_handle("zakura_header_height_by_hash").unwrap();
        let header_by_height = db.db.cf_handle("zakura_header_by_height").unwrap();
        let headers = [
            (Height(1), block1.hash(), block1.header.clone()),
            (Height(2), block2.hash(), block2.header.clone()),
        ];
        let mut batch = DiskWriteBatch::new();
        for (height, hash, header) in &headers {
            batch.zs_insert(&hash_by_height, height, hash);
            batch.zs_insert(&height_by_hash, hash, height);
            batch.zs_insert(&header_by_height, height, header);
        }
        batch
            .advance_highest_completed_checkpoint_for_header_range(&db, &headers)
            .expect("linked bracket advances");
        db.write_batch(batch).expect("linked bracket writes");

        let expected = HighestCompletedCheckpoint {
            height: Height(2),
            hash: block2.hash(),
        };
        assert_eq!(
            db.validate_highest_completed_checkpoint()
                .expect("highest completed checkpoint validates"),
            Some(expected)
        );
        drop(db);

        let reopened = ZakuraDb::new(
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
        .expect("persistent database reopens");
        assert_eq!(
            reopened
                .validate_highest_completed_checkpoint()
                .expect("restored highest completed checkpoint validates"),
            Some(expected)
        );
    }

    #[test]
    fn interior_gap_does_not_complete_checkpoint() {
        let (db, _block1, block2) = checkpoint_fixture(&Config::ephemeral());
        let pending = [(Height(2), block2.hash(), block2.header.clone())];
        let mut batch = DiskWriteBatch::new();
        batch
            .advance_highest_completed_checkpoint_for_header_range(&db, &pending)
            .expect("gap leaves the highest completed checkpoint unchanged");
        db.write_batch(batch)
            .expect("unchanged highest completed checkpoint batch writes");

        assert_eq!(
            db.try_highest_completed_checkpoint()
                .expect("highest completed checkpoint decodes")
                .expect("highest completed checkpoint exists")
                .height,
            Height::MIN
        );
    }

    #[test]
    fn startup_repair_rebases_after_deleting_checkpoint_bracket() {
        let (db, block1, block2) = checkpoint_fixture(&Config::ephemeral());
        let hash_by_height = db.db.cf_handle("zakura_header_hash_by_height").unwrap();
        let height_by_hash = db.db.cf_handle("zakura_header_height_by_hash").unwrap();
        let header_by_height = db.db.cf_handle("zakura_header_by_height").unwrap();
        let headers = [
            (Height(1), block1.hash(), block1.header.clone()),
            (Height(2), block2.hash(), block2.header.clone()),
        ];
        let mut batch = DiskWriteBatch::new();
        for (height, hash, header) in &headers {
            batch.zs_insert(&hash_by_height, height, hash);
            batch.zs_insert(&height_by_hash, hash, height);
            batch.zs_insert(&header_by_height, height, header);
        }
        batch
            .advance_highest_completed_checkpoint_for_header_range(&db, &headers)
            .expect("linked bracket advances");
        db.write_batch(batch).expect("linked bracket writes");

        let mut corrupt = DiskWriteBatch::new();
        corrupt.zs_delete(&hash_by_height, Height(1));
        corrupt.zs_delete(&header_by_height, Height(1));
        db.write_batch(corrupt).expect("corrupt fixture writes");
        db.audit_and_repair_zakura_header_store()
            .expect("startup-style repair succeeds")
            .expect("corrupt header store is repaired");

        assert_eq!(
            db.validate_highest_completed_checkpoint()
                .expect("repaired highest completed checkpoint validates")
                .expect("genesis highest completed checkpoint remains")
                .height,
            Height::MIN
        );
    }

    /// Fully synced nodes often have an empty provisional header frontier while
    /// still holding a durable completed-checkpoint row. The empty-frontier
    /// audit fast path must still repair that independent row before format
    /// validation fails closed.
    #[test]
    fn empty_frontier_startup_audit_repairs_stale_checkpoint() {
        let (db, _block1, block2) = checkpoint_fixture(&Config::ephemeral());

        // Fixture tip is genesis with empty zakura header CFs. Plant a stale
        // Height(2) completed checkpoint that is not durable on disk.
        let mut batch = DiskWriteBatch::new();
        batch.set_highest_completed_checkpoint(
            &db,
            HighestCompletedCheckpoint {
                height: Height(2),
                hash: block2.hash(),
            },
        );
        db.write_batch(batch).expect("stale checkpoint writes");

        assert!(matches!(
            db.validate_highest_completed_checkpoint(),
            Err(HighestCompletedCheckpointError::Mismatch { .. })
        ));

        assert!(
            db.audit_and_repair_zakura_header_store()
                .expect("empty-frontier audit succeeds")
                .is_some(),
            "stale checkpoint on an empty frontier must be repaired"
        );
        assert_eq!(
            db.try_highest_completed_checkpoint()
                .expect("repaired checkpoint decodes")
                .expect("genesis checkpoint remains"),
            HighestCompletedCheckpoint {
                height: Height::MIN,
                hash: db
                    .network()
                    .checkpoint_list()
                    .hash(Height::MIN)
                    .expect("genesis checkpoint is configured"),
            }
        );
        assert_eq!(
            db.validate_highest_completed_checkpoint()
                .expect("repaired highest completed checkpoint validates")
                .expect("genesis highest completed checkpoint remains")
                .height,
            Height::MIN
        );
    }

    #[test]
    fn malformed_row_fails_closed() {
        let (db, _block1, _block2) = checkpoint_fixture(&Config::ephemeral());
        let mut batch = DiskWriteBatch::new();
        let _ = db
            .highest_completed_checkpoint_cf()
            .with_batch_for_writing(&mut batch)
            .zs_insert(
                &RawBytes::new_raw_bytes(ROW_KEY.to_vec()),
                &RawBytes::new_raw_bytes(vec![0xff]),
            );
        db.write_batch(batch).expect("malformed row writes");

        assert!(matches!(
            db.try_highest_completed_checkpoint(),
            Err(HighestCompletedCheckpointError::InvalidEncoding)
        ));
    }
}
