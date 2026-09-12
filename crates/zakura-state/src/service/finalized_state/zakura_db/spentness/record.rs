//! The durable progress record, stored with each atomic block batch.

use serde::{Deserialize, Serialize};
use zakura_chain::{
    amount::{Amount, NonNegative},
    block::Height,
    parameters::spentness_hints::Commitment,
    value_balance::ValueBalance,
};

use super::{SpentnessError, SpentnessStatus};
use crate::service::finalized_state::{
    disk_db::{DiskDb, DiskWriteBatch, ReadDisk, WriteDisk},
    disk_format::{IntoDisk, RawBytes},
    zakura_db::ZakuraDb,
};

/// Column family that holds the single progress record.
pub(crate) const METADATA: &str = "spentness_metadata";
const RECORD_VERSION: u32 = 1;
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
    /// `survivor_value` is the transparent value of the survivors inserted so far.
    Applying {
        commitment: Commitment,
        height: u32,
        block_hash: [u8; 32],
        next_ordinal: u64,
        survivor_value: u64,
    },
    /// The terminal block is committed, and derived indexes are rebuilt through
    /// `indexed_height`. `replay_accounting` holds the replayed pools at that height.
    Rebuilding {
        commitment: Commitment,
        indexed_height: Option<u32>,
        replay_accounting: PoolAccounting,
        survivor_value: u64,
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

/// Value pools in zatoshis, as stored in the progress record.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PoolAccounting {
    transparent: u64,
    sprout: u64,
    sapling: u64,
    orchard: u64,
    deferred: u64,
    ironwood: u64,
}

impl From<ValueBalance<NonNegative>> for PoolAccounting {
    fn from(pool: ValueBalance<NonNegative>) -> Self {
        Self {
            transparent: pool.transparent_amount().into(),
            sprout: pool.sprout_amount().into(),
            sapling: pool.sapling_amount().into(),
            orchard: pool.orchard_amount().into(),
            deferred: pool.deferred_amount().into(),
            ironwood: pool.ironwood_amount().into(),
        }
    }
}

impl TryFrom<PoolAccounting> for ValueBalance<NonNegative> {
    type Error = SpentnessError;

    fn try_from(accounting: PoolAccounting) -> Result<Self, Self::Error> {
        let mut pool =
            ValueBalance::from_transparent_amount(Amount::try_from(accounting.transparent)?);
        pool.set_sprout_value_balance(ValueBalance::from_sprout_amount(Amount::try_from(
            accounting.sprout,
        )?));
        pool.set_sapling_value_balance(ValueBalance::from_sapling_amount(Amount::try_from(
            accounting.sapling,
        )?));
        pool.set_orchard_value_balance(ValueBalance::from_orchard_amount(Amount::try_from(
            accounting.orchard,
        )?));
        pool.set_deferred_amount(Amount::try_from(accounting.deferred)?);
        pool.set_ironwood_value_balance(ValueBalance::from_ironwood_amount(Amount::try_from(
            accounting.ironwood,
        )?));
        Ok(pool)
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
