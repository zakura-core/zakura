//! One-way migration from the legacy single-chain header overlay.

use std::{collections::BTreeMap, sync::Arc};

use sha2::{Digest, Sha256};
use thiserror::Error;
use zakura_chain::{
    block,
    serialization::{ZcashDeserializeInto, ZcashSerialize},
};
use zakura_header_chain::{
    validate_commitment_structure, validate_compact_target,
    validate_contextual_difficulty_and_time, validate_encoding_version_hash, validate_future_time,
    validate_hash_filter, AdjustedDifficulty, AlarmSet, BodyValidationState, ChainScore, ChangeSet,
    Clock, EligibilityReason, EngineConfig, EngineMetadata, EngineMode, EvidenceId, FinalityEpoch,
    FinalityRecord, FinalitySource, Frontier, FrontierSet, HeaderChainDiskVersion,
    HeaderGeneration, HeaderNode, HeaderValidationState, IndexChanges, PowPolicy, ProjectionDelta,
    StateVersion, SystemClock, VerifiedGeneration, VerifiedHeaderRef, WorkCoordinate,
};

use super::{HeaderChainRuntime, HeaderChainStore, HeaderChainStoreError, StartupReport};
use crate::service::finalized_state::{
    disk_format::{
        block::HEIGHT_DISK_BYTES, header_chain_values::HeaderValidationContextDisk, FromDisk,
    },
    zakura_db::{
        block::{
            ZAKURA_HEADER_BY_HEIGHT, ZAKURA_HEADER_HASH_BY_HEIGHT, ZAKURA_HEADER_HEIGHT_BY_HASH,
        },
        ZakuraDb,
    },
    HEADER_VALIDATION_CONTEXT,
};

/// Successful one-way legacy import.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderChainMigrationReport {
    /// Finalized anchor imported from full state.
    pub anchor: Frontier,
    /// Legacy selected suffix rows imported above the anchor.
    pub imported_headers: usize,
    /// Immutable predecessor context rows copied below the anchor.
    pub validation_context_rows: usize,
    /// Audited and published startup result after the atomic format marker.
    pub startup: StartupReport,
}

/// Legacy input was not coherent enough for a one-way authenticated import.
#[derive(Debug, Error)]
pub enum HeaderChainMigrationError {
    /// The new schema already has its format-complete metadata marker.
    #[error("fork-aware header-chain schema is already initialized")]
    AlreadyInitialized,
    /// Full state has no finalized tip to authenticate the import.
    #[error("legacy header migration requires a finalized full-state anchor")]
    MissingFinalizedAnchor,
    /// The supplied engine bootstrap is not an exact full-state ancestor of the finalized tip.
    #[error("engine bootstrap anchor is not an exact finalized full-state ancestor")]
    AnchorMismatch,
    /// A legacy row/key/value or single-chain invariant was malformed.
    #[error("legacy header store is incoherent: {0}")]
    Legacy(&'static str),
    /// Exact work or graph construction failed.
    #[error("legacy header path could not form a coherent work DAG")]
    Graph,
    /// The durable import or its mandatory startup audit failed.
    #[error(transparent)]
    Store(#[from] HeaderChainStoreError),
    /// RocksDB failed while exhaustively reading the legacy columns.
    #[error("legacy header migration read failed: {0}")]
    RocksDb(#[from] rocksdb::Error),
}

/// Validate the old single-chain overlay, import it atomically, audit it, then publish.
pub fn migrate_v7_header_store(
    source: &ZakuraDb,
    config: &EngineConfig,
) -> Result<(HeaderChainRuntime, HeaderChainMigrationReport), HeaderChainMigrationError> {
    migrate_v7_header_store_reconciled(source, config, Vec::new())
}

/// Import the legacy overlay and reconcile restored full state before publication.
pub(in crate::service) fn migrate_v7_header_store_reconciled(
    source: &ZakuraDb,
    config: &EngineConfig,
    restored_path: Vec<VerifiedHeaderRef>,
) -> Result<(HeaderChainRuntime, HeaderChainMigrationReport), HeaderChainMigrationError> {
    let store = HeaderChainStore::new(source.header_chain_disk_db());
    if store.metadata_row()?.is_some() {
        return Err(HeaderChainMigrationError::AlreadyInitialized);
    }
    let (anchor_height, anchor_hash) = source
        .tip()
        .ok_or(HeaderChainMigrationError::MissingFinalizedAnchor)?;
    let anchor = Frontier::new(anchor_height, anchor_hash);
    let (anchor_header, anchor_coordinate) = finalized_anchor(source, config, anchor)?;
    if !trust_reasons(config, anchor).is_empty() {
        return Err(HeaderChainMigrationError::Legacy(
            "finalized anchor conflicts with authenticated trust pins",
        ));
    }

    let legacy = legacy_rows(source, anchor)?;
    let evidence = migration_evidence(anchor, legacy.last().map_or(anchor, |row| row.frontier));
    let anchor_work = anchor_header
        .difficulty_threshold
        .to_work()
        .ok_or(HeaderChainMigrationError::Graph)?;
    let anchor_node = HeaderNode::from_durable_parts(
        anchor_header.clone(),
        anchor.hash,
        anchor_header.previous_block_hash,
        anchor.height,
        anchor_work,
        anchor_coordinate,
        HeaderValidationState::Valid,
        Default::default(),
        BodyValidationState::Verified { evidence },
        Vec::new(),
    )
    .map_err(|_| HeaderChainMigrationError::Graph)?;
    let mut nodes = vec![anchor_node];
    let mut coordinate = anchor_coordinate;
    let pow_policy = PowPolicy::for_network(&config.network).map_err(|_| {
        HeaderChainMigrationError::Legacy("proof-of-work policy is not authenticated")
    })?;
    let now = SystemClock.now();
    for row in &legacy {
        let hash = validate_encoding_version_hash(&row.header)
            .map_err(|_| HeaderChainMigrationError::Legacy("legacy header version is invalid"))?;
        if hash != row.frontier.hash {
            return Err(HeaderChainMigrationError::Legacy(
                "legacy header hash changed during observable validation",
            ));
        }
        validate_commitment_structure(&row.header, &config.network, row.frontier.height).map_err(
            |_| HeaderChainMigrationError::Legacy("legacy header commitment is malformed"),
        )?;
        let target = validate_compact_target(&row.header, &config.network)
            .map_err(|_| HeaderChainMigrationError::Legacy("legacy header target is invalid"))?;
        validate_hash_filter(hash, target).map_err(|_| {
            HeaderChainMigrationError::Legacy("legacy header hash exceeds its target")
        })?;
        pow_policy.validate_solution(&row.header).map_err(|_| {
            HeaderChainMigrationError::Legacy("legacy header proof of work is invalid")
        })?;
        validate_future_time(&row.header, now, row.frontier.height, hash).map_err(|_| {
            HeaderChainMigrationError::Legacy("legacy header exceeds the local future-time bound")
        })?;
        let previous = row
            .frontier
            .height
            .previous()
            .map_err(|_| HeaderChainMigrationError::Graph)?;
        let context = source.recent_header_context(previous).map_err(|_| {
            HeaderChainMigrationError::Legacy("legacy contextual window is incoherent")
        })?;
        let adjustment = AdjustedDifficulty::new_from_header_time(
            row.header.time,
            previous,
            &config.network,
            context,
        );
        validate_contextual_difficulty_and_time(row.header.difficulty_threshold, adjustment)
            .map_err(|_| {
                HeaderChainMigrationError::Legacy("legacy contextual header rule failed")
            })?;
        let work = row
            .header
            .difficulty_threshold
            .to_work()
            .ok_or(HeaderChainMigrationError::Graph)?;
        if !trust_reasons(config, row.frontier).is_empty() {
            return Err(HeaderChainMigrationError::Legacy(
                "legacy selected path conflicts with authenticated trust pins",
            ));
        }
        coordinate = coordinate
            .checked_add(work)
            .map_err(|_| HeaderChainMigrationError::Graph)?;
        nodes.push(
            HeaderNode::from_durable_parts(
                row.header.clone(),
                row.frontier.hash,
                row.header.previous_block_hash,
                row.frontier.height,
                work,
                coordinate,
                HeaderValidationState::Valid,
                Default::default(),
                BodyValidationState::Unknown,
                Vec::new(),
            )
            .map_err(|_| HeaderChainMigrationError::Graph)?,
        );
    }
    let legacy_tip = legacy.last().map_or(anchor, |row| row.frontier);
    let header_best = legacy_tip;
    let header_best_score = ChainScore::new(
        coordinate
            .suffix_after(anchor_coordinate)
            .map_err(|_| HeaderChainMigrationError::Graph)?,
        header_best.hash,
    );
    let selected_projection: Vec<_> = nodes
        .iter()
        .map(|node| Frontier::new(node.height, node.hash))
        .collect();
    let candidate_tips = vec![(header_best_score, header_best.hash)];
    let finality = FinalityRecord {
        previous: config.bootstrap_anchor.frontier,
        current: anchor,
        source: match config.mode {
            EngineMode::Integrated => FinalitySource::FullState { evidence },
            EngineMode::HeadersOnly => FinalitySource::MigratedHeadersOnly,
        },
        epoch: FinalityEpoch::new(0),
    };
    let metadata = EngineMetadata {
        disk_format: HeaderChainDiskVersion(1),
        mode: config.mode,
        network_id: config.network.kind(),
        anchor_manifest_digest: config.trust_anchor_digest(),
        work_origin: config.bootstrap_anchor.frontier,
        state_version: StateVersion::new(1),
        header_generation: HeaderGeneration::new(1),
        verified_generation: VerifiedGeneration::new(1),
        finality_epoch: FinalityEpoch::new(0),
        frontiers: FrontierSet {
            finalized: anchor,
            header_best,
            verified_best: anchor,
        },
        header_best_score,
        oldest_retained_height: anchor.height,
        alarms: AlarmSet::default(),
        last_transition_id: evidence,
    };
    let changes = ChangeSet {
        put_nodes: nodes,
        delete_nodes: Vec::new(),
        index_changes: IndexChanges {
            inserted: selected_projection.clone(),
            deleted: Vec::new(),
        },
        candidate_tips,
        selected_projection: ProjectionDelta {
            remove_from: None,
            put: selected_projection,
        },
        verified_projection: ProjectionDelta {
            remove_from: None,
            put: vec![anchor],
        },
        eligibility_changes: Vec::new(),
        aux_changes: Vec::new(),
        finality_append: Some(finality),
        metadata,
    };
    let contexts = validation_context(source, anchor, anchor_header.previous_block_hash)?;
    let mut base_batch = super::super::DiskWriteBatch::new();
    for context in &contexts {
        store.put_value(
            &mut base_batch,
            HEADER_VALIDATION_CONTEXT,
            context.header.hash().0,
            context,
        )?;
    }
    let mut no_fault = |_| Ok(());
    let batch = store.batch_for_with_fault(&changes, base_batch, &mut no_fault)?;
    store.db.write(batch)?;
    let imported_headers = legacy.len();
    let validation_context_rows = contexts.len();
    let (runtime, startup) = store.startup_reconciled(config, anchor, Vec::new(), restored_path)?;
    Ok((
        runtime,
        HeaderChainMigrationReport {
            anchor,
            imported_headers,
            validation_context_rows,
            startup,
        },
    ))
}

fn finalized_anchor(
    source: &ZakuraDb,
    config: &EngineConfig,
    finalized: Frontier,
) -> Result<(Arc<block::Header>, WorkCoordinate), HeaderChainMigrationError> {
    let bootstrap = config.bootstrap_anchor.frontier;
    if bootstrap.height > finalized.height {
        return Err(HeaderChainMigrationError::AnchorMismatch);
    }
    let (stored_bootstrap_hash, stored_bootstrap) = source
        .header_by_height(bootstrap.height)
        .ok_or(HeaderChainMigrationError::AnchorMismatch)?;
    if stored_bootstrap_hash != bootstrap.hash
        || stored_bootstrap.as_ref() != config.bootstrap_anchor.header.as_ref()
    {
        return Err(HeaderChainMigrationError::AnchorMismatch);
    }
    let bootstrap_work = stored_bootstrap
        .difficulty_threshold
        .to_work()
        .ok_or(HeaderChainMigrationError::Graph)?;
    let mut coordinate = WorkCoordinate::new(bootstrap.hash, bootstrap_work.as_u256());
    let mut header = stored_bootstrap;
    let mut height = bootstrap.height;
    while height < finalized.height {
        height = height
            .next()
            .map_err(|_| HeaderChainMigrationError::Graph)?;
        let (hash, next) = source
            .header_by_height(height)
            .ok_or(HeaderChainMigrationError::AnchorMismatch)?;
        if next.hash() != hash || next.previous_block_hash != header.hash() {
            return Err(HeaderChainMigrationError::AnchorMismatch);
        }
        let work = next
            .difficulty_threshold
            .to_work()
            .ok_or(HeaderChainMigrationError::Graph)?;
        coordinate = coordinate
            .checked_add(work)
            .map_err(|_| HeaderChainMigrationError::Graph)?;
        header = next;
    }
    if header.hash() != finalized.hash {
        return Err(HeaderChainMigrationError::AnchorMismatch);
    }
    Ok((header, coordinate))
}

struct LegacyRow {
    frontier: Frontier,
    header: Arc<block::Header>,
}

fn legacy_rows(
    source: &ZakuraDb,
    anchor: Frontier,
) -> Result<Vec<LegacyRow>, HeaderChainMigrationError> {
    let db = source.header_chain_disk_db();
    let header_rows = raw_rows(&db, ZAKURA_HEADER_BY_HEIGHT)?;
    let hash_rows = raw_rows(&db, ZAKURA_HEADER_HASH_BY_HEIGHT)?;
    let reverse_rows = raw_rows(&db, ZAKURA_HEADER_HEIGHT_BY_HASH)?;
    if header_rows.len() != hash_rows.len() || hash_rows.len() != reverse_rows.len() {
        return Err(HeaderChainMigrationError::Legacy(
            "legacy core column-family row counts differ",
        ));
    }

    let mut headers = BTreeMap::new();
    for (key, value) in header_rows {
        let height = decode_height(&key)?;
        let header: block::Header = value
            .zcash_deserialize_into()
            .map_err(|_| HeaderChainMigrationError::Legacy("invalid canonical header row"))?;
        if header
            .zcash_serialize_to_vec()
            .map_err(|_| HeaderChainMigrationError::Legacy("header row could not reserialize"))?
            != value
        {
            return Err(HeaderChainMigrationError::Legacy(
                "header row is not an exact canonical encoding",
            ));
        }
        headers.insert(height, Arc::new(header));
    }
    let mut hashes = BTreeMap::new();
    for (key, value) in hash_rows {
        let height = decode_height(&key)?;
        let hash = decode_hash(&value)?;
        hashes.insert(height, hash);
    }
    for (key, value) in reverse_rows {
        let hash = decode_hash(&key)?;
        let height = decode_height(&value)?;
        if hashes.get(&height) != Some(&hash) {
            return Err(HeaderChainMigrationError::Legacy(
                "legacy reverse index does not round-trip",
            ));
        }
    }
    if headers.keys().ne(hashes.keys()) {
        return Err(HeaderChainMigrationError::Legacy(
            "legacy header and hash heights differ",
        ));
    }

    let mut rows = Vec::new();
    let mut expected_height = anchor.height.next().ok();
    let mut parent = anchor.hash;
    for (height, header) in headers {
        if Some(height) != expected_height {
            return Err(HeaderChainMigrationError::Legacy(
                "legacy selected suffix is not contiguous above finalized",
            ));
        }
        let hash = hashes[&height];
        if header.hash() != hash || header.previous_block_hash != parent {
            return Err(HeaderChainMigrationError::Legacy(
                "legacy selected suffix hash or parent linkage differs",
            ));
        }
        rows.push(LegacyRow {
            frontier: Frontier::new(height, hash),
            header,
        });
        parent = hash;
        expected_height = height.next().ok();
    }
    Ok(rows)
}

fn validation_context(
    source: &ZakuraDb,
    anchor: Frontier,
    mut expected_hash: block::Hash,
) -> Result<Vec<HeaderValidationContextDisk>, HeaderChainMigrationError> {
    let mut contexts = Vec::new();
    let mut height = anchor.height;
    for _ in 0..27 {
        let Ok(previous) = height.previous() else {
            break;
        };
        let (hash, header) =
            source
                .header_by_height(previous)
                .ok_or(HeaderChainMigrationError::Legacy(
                    "full-state validation context has a gap",
                ))?;
        if header.hash() != hash || hash != expected_hash {
            return Err(HeaderChainMigrationError::Legacy(
                "full-state validation context hash or linkage differs",
            ));
        }
        expected_hash = header.previous_block_hash;
        contexts.push(HeaderValidationContextDisk {
            header,
            height: previous,
        });
        height = previous;
    }
    contexts.reverse();
    Ok(contexts)
}

fn raw_rows(
    db: &super::super::DiskDb,
    family: &'static str,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, HeaderChainMigrationError> {
    let cf = db
        .cf_handle(family)
        .ok_or(HeaderChainMigrationError::Legacy(
            "legacy column family is missing",
        ))?;
    Ok(db.raw_range_cf(&cf, &[], None)?)
}

fn decode_height(bytes: &[u8]) -> Result<block::Height, HeaderChainMigrationError> {
    if bytes.len() != HEIGHT_DISK_BYTES {
        return Err(HeaderChainMigrationError::Legacy(
            "invalid legacy height width",
        ));
    }
    Ok(block::Height::from_bytes(bytes))
}

fn decode_hash(bytes: &[u8]) -> Result<block::Hash, HeaderChainMigrationError> {
    bytes
        .try_into()
        .map(block::Hash)
        .map_err(|_| HeaderChainMigrationError::Legacy("invalid legacy hash width"))
}

fn trust_reasons(config: &EngineConfig, frontier: Frontier) -> Vec<EligibilityReason> {
    let mut reasons = Vec::new();
    if let Some(pin) = config.settled_manifest.pin_for_network(&config.network) {
        if pin.activation.height == frontier.height && pin.activation.hash != frontier.hash {
            reasons.push(EligibilityReason::SettledUpgradeConflict {
                height: frontier.height,
                expected: pin.activation.hash,
            });
        }
    }
    if let Some(expected) = config.local_checkpoints.hash(frontier.height) {
        if expected != frontier.hash {
            reasons.push(EligibilityReason::CheckpointConflict {
                height: frontier.height,
                expected,
            });
        }
    }
    reasons
}

fn migration_evidence(anchor: Frontier, tip: Frontier) -> EvidenceId {
    let mut hasher = Sha256::new();
    hasher.update(b"zakura-header-chain-v7-migration-v1");
    hasher.update(anchor.height.0.to_be_bytes());
    hasher.update(anchor.hash.0);
    hasher.update(tip.height.0.to_be_bytes());
    hasher.update(tip.hash.0);
    EvidenceId::from_digest(hasher.finalize().into())
}
