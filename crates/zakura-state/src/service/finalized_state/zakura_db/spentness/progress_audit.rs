//! Offline cursor diagnostics that never open incomplete state for consumers.
//!
//! The audit re-enumerates retained transactions, checks them against their header
//! Merkle roots, and compares the output count with the recorded cursor.

use std::sync::Arc;

use serde::Serialize;
use zakura_chain::{
    block::{self, Hash, Height},
    parameters::{spentness_hints::Commitment, Network},
    transaction::Transaction,
};

use super::{
    record::{read_progress, Progress},
    startup::open_existing_state,
    SpentnessError, SpentnessSetup,
};
use crate::{
    constants::state_database_format_version_in_code,
    service::finalized_state::{
        disk_db::{DiskDb, ReadDisk},
        disk_format::TransactionLocation,
    },
    Config, StateInitError,
};

/// Independent output enumeration for a durable construction cursor.
#[derive(Clone, Debug, Serialize)]
pub struct SpentnessProgressAudit {
    /// Release authority recorded when construction started.
    pub commitment: Commitment,
    /// Last block included in this audit.
    pub body_height: Height,
    /// Output count stored by the construction transition.
    pub recorded_outputs: u64,
    /// Output count independently enumerated from retained bodies.
    pub enumerated_outputs: u64,
    /// Whether retained bodies agree with the recorded cursor.
    pub cursor_matches: bool,
}

/// Re-enumerate retained bodies without repairing progress or exposing monetary state.
pub fn audit_spentness_progress(
    config: &Config,
    network: &Network,
) -> Result<SpentnessProgressAudit, StateInitError> {
    audit_progress_with_setup(config, &SpentnessSetup::ordinary(network), network)
}

pub(crate) fn audit_progress_with_setup(
    config: &Config,
    setup: &SpentnessSetup,
    network: &Network,
) -> Result<SpentnessProgressAudit, StateInitError> {
    let version = state_database_format_version_in_code();
    let Some((db, _)) =
        open_existing_state(config, network)?.filter(|(_, disk_version)| *disk_version == version)
    else {
        return Err(SpentnessError::FormatChanged.into());
    };
    let progress = read_progress(&db)?.ok_or(SpentnessError::Inconsistent(
        "database has no spentness construction record",
    ))?;
    setup.authority.check(progress.commitment())?;
    let (height, hash, recorded_outputs) = cursor_boundary(&progress)?;
    check_tip(&db, height, hash)?;
    let enumerated_outputs = count_retained_outputs(&db, height)?;
    Ok(SpentnessProgressAudit {
        commitment: progress.commitment().clone(),
        body_height: height,
        recorded_outputs,
        enumerated_outputs,
        cursor_matches: recorded_outputs == enumerated_outputs,
    })
}

/// The block and output count that the recorded cursor claims.
fn cursor_boundary(progress: &Progress) -> Result<(Height, Hash, u64), SpentnessError> {
    match progress {
        Progress::Applying {
            height,
            block_hash,
            next_ordinal,
            ..
        } => Ok((Height(*height), Hash(*block_hash), *next_ordinal)),
        Progress::Rebuilding { commitment, .. } => Ok((
            Height(commitment.terminal_height),
            Hash(commitment.terminal_block_hash),
            commitment.output_count,
        )),
        Progress::Complete { .. } => Err(SpentnessError::Inconsistent(
            "completed state has no construction cursor to audit",
        )),
    }
}

fn check_tip(db: &DiskDb, height: Height, hash: Hash) -> Result<(), SpentnessError> {
    let hashes = db
        .cf_handle("hash_by_height")
        .expect("block hash column family is declared");
    if db.zs_last_key_value(&hashes) != Some((height, hash)) {
        return Err(SpentnessError::Inconsistent(
            "construction record differs from the finalized tip",
        ));
    }
    Ok(())
}

/// Count outputs in retained bodies from genesis through `tip`, checking each block's links.
fn count_retained_outputs(db: &DiskDb, tip: Height) -> Result<u64, SpentnessError> {
    let mut outputs = 0;
    let mut previous_hash = None;
    for height in (0..=tip.0).map(Height) {
        let header = retained_header(db, height, previous_hash)?;
        let transactions = retained_transactions(db, height)?;
        let merkle_root: block::merkle::Root = transactions.iter().map(Transaction::hash).collect();
        if transactions.is_empty() || merkle_root != header.merkle_root {
            return Err(SpentnessError::Mismatch(
                "cursor audit retained transactions differ from their header",
            ));
        }
        let block_outputs: usize = transactions
            .iter()
            .map(|transaction| transaction.outputs().len())
            .sum();
        outputs += u64::try_from(block_outputs)
            .map_err(|_| SpentnessError::Mismatch("cursor audit output count overflow"))?;
        previous_hash = Some(header.hash());
    }
    Ok(outputs)
}

/// Read the header at `height` and check its hash index entry and parent link.
fn retained_header(
    db: &DiskDb,
    height: Height,
    previous_hash: Option<Hash>,
) -> Result<Arc<block::Header>, SpentnessError> {
    let headers = db
        .cf_handle("block_header_by_height")
        .expect("block header column family is declared");
    let hashes = db
        .cf_handle("hash_by_height")
        .expect("block hash column family is declared");
    let header: Arc<block::Header> =
        db.zs_get(&headers, &height)
            .ok_or(SpentnessError::Mismatch(
                "cursor audit is missing a retained header",
            ))?;
    let indexed_hash: Option<Hash> = db.zs_get(&hashes, &height);
    let linked = previous_hash.is_none_or(|previous| previous == header.previous_block_hash);
    if indexed_hash != Some(header.hash()) || !linked {
        return Err(SpentnessError::Mismatch(
            "cursor audit found inconsistent retained headers",
        ));
    }
    Ok(header)
}

/// Read the retained transactions at `height`, requiring contiguous locations.
fn retained_transactions(db: &DiskDb, height: Height) -> Result<Vec<Transaction>, SpentnessError> {
    let transactions = db
        .cf_handle("tx_by_loc")
        .expect("transaction column family is declared");
    let range =
        TransactionLocation::min_for_height(height)..=TransactionLocation::max_for_height(height);
    db.zs_forward_range_iter::<_, TransactionLocation, Transaction, _>(&transactions, range)
        .enumerate()
        .map(|(index, (location, transaction))| {
            if location != TransactionLocation::from_usize(height, index) {
                return Err(SpentnessError::Mismatch(
                    "cursor audit found a missing transaction location",
                ));
            }
            Ok(transaction)
        })
        .collect()
}
