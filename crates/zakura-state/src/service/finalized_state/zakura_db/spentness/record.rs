//! The durable progress record, stored with each atomic block batch.

use serde::{Deserialize, Serialize};
use zakura_chain::{block::Height, parameters::spentness_hints::Commitment};

use super::{SpentnessError, SpentnessStatus};
use crate::service::finalized_state::{
    disk_db::{DiskDb, DiskWriteBatch, ReadDisk, WriteDisk},
    disk_format::{IntoDisk, RawBytes},
    zakura_db::ZakuraDb,
};

/// Column family that holds the single progress record.
pub(crate) const METADATA: &str = "spentness_metadata";
/// Column family that holds the outputs the artifact omits, keyed by output location.
///
/// Applying writes every omitted output after genesis. The rebuild resolves each
/// spend from this column family and deletes the row, and completion clears it.
pub(crate) const OMITTED_OUTPUTS: &str = "spentness_omitted_outputs";
const RECORD_VERSION: u32 = 2;
/// Upper bound for the encoded record, checked on read and write.
const MAX_RECORD_BYTES: usize = 4096;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    version: u32,
    progress: Progress,
}

/// Construction progress. Each block batch stores the next value atomically.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "phase", deny_unknown_fields)]
pub(crate) enum Progress {
    /// Checkpoint blocks through `height` are committed with artifact survivors.
    ///
    /// `next_ordinal` is the artifact bit for the next block's first output.
    /// `omitted_outputs` counts the rows written to [`OMITTED_OUTPUTS`].
    Applying {
        commitment: Commitment,
        height: u32,
        block_hash: [u8; 32],
        next_ordinal: u64,
        omitted_outputs: u64,
    },
    /// The terminal block is committed, and derived indexes are rebuilt through
    /// `indexed_height`.
    ///
    /// `transparent_value` is the replayed transparent pool at that height.
    /// `unspent_omitted` counts the omitted outputs that no replayed input has spent.
    Rebuilding {
        commitment: Commitment,
        indexed_height: Option<u32>,
        transparent_value: u64,
        unspent_omitted: u64,
    },
    /// Rebuilt indexes passed the final audit. Rollback cannot cross `rollback_floor`.
    Complete {
        commitment: Commitment,
        rollback_floor: u32,
    },
}

impl Progress {
    pub(crate) fn commitment(&self) -> &Commitment {
        match self {
            Self::Applying { commitment, .. }
            | Self::Rebuilding { commitment, .. }
            | Self::Complete { commitment, .. } => commitment,
        }
    }

    /// The consumer-facing status for this durable phase.
    pub(super) fn status(&self) -> SpentnessStatus {
        match self {
            Self::Applying { commitment, .. } => SpentnessStatus::Applying {
                terminal_height: Height(commitment.terminal_height),
            },
            Self::Rebuilding { .. } => SpentnessStatus::Rebuilding,
            Self::Complete { .. } => SpentnessStatus::Usable,
        }
    }
}

/// Read the progress record, or `None` for an ordinary database.
pub(super) fn read_progress(db: &DiskDb) -> Result<Option<Progress>, SpentnessError> {
    let Some(cf) = db.cf_handle(METADATA) else {
        return Ok(None);
    };
    let Some(bytes) = db.zs_get::<_, _, RawBytes>(&cf, &()) else {
        return Ok(None);
    };
    let bytes = bytes.as_bytes();
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(SpentnessError::Inconsistent("oversized progress record"));
    }
    let record: Record = serde_json::from_slice(&bytes)?;
    if record.version != RECORD_VERSION {
        return Err(SpentnessError::Inconsistent("unsupported progress version"));
    }
    Ok(Some(record.progress))
}

impl DiskWriteBatch {
    /// Store `progress` with the other writes in this batch.
    pub(crate) fn prepare_spentness_progress(
        &mut self,
        db: &ZakuraDb,
        progress: Progress,
    ) -> Result<(), SpentnessError> {
        let bytes = serde_json::to_vec(&Record {
            version: RECORD_VERSION,
            progress,
        })?;
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(SpentnessError::Inconsistent("oversized progress record"));
        }
        let cf = db
            .db
            .cf_handle(METADATA)
            .expect("spentness metadata is a declared column family");
        self.zs_insert(&cf, (), RawBytes::new_raw_bytes(bytes));
        Ok(())
    }
}
