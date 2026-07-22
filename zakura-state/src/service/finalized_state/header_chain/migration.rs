//! Initialization of the fork-aware header DAG from authenticated full state.

use std::sync::Arc;

use sha2::{Digest, Sha256};
use thiserror::Error;
use zakura_chain::block;
use zakura_header_chain::{
    AlarmSet, BodyValidationState, ChainScore, ChangeSet, EngineConfig, EngineMetadata, EngineMode,
    EvidenceId, FinalityEpoch, FinalityRecord, FinalitySource, Frontier, FrontierSet,
    HeaderChainDiskVersion, HeaderGeneration, HeaderNode, HeaderValidationState, IndexChanges,
    ProjectionDelta, StateVersion, VerifiedGeneration, VerifiedHeaderRef, WorkCoordinate,
};

use super::{HeaderChainRuntime, HeaderChainStore, HeaderChainStoreError, StartupReport};
use crate::service::finalized_state::{
    disk_format::header_chain_values::HeaderValidationContextDisk,
    zakura_db::{
        block::{
            ZAKURA_HEADER_BY_HEIGHT, ZAKURA_HEADER_HASH_BY_HEIGHT, ZAKURA_HEADER_HEIGHT_BY_HASH,
        },
        ZakuraDb,
    },
    HEADER_VALIDATION_CONTEXT,
};

/// Successful initialization from authenticated full-state facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderChainInitializationReport {
    /// Finalized anchor imported from full state.
    pub anchor: Frontier,
    /// Immutable predecessor context rows copied below the anchor.
    pub validation_context_rows: usize,
    /// Audited and published startup result.
    pub startup: StartupReport,
}

/// Header-chain initialization failed before publication.
#[derive(Debug, Error)]
pub enum HeaderChainInitializationError {
    /// The new schema already has its format-complete metadata marker.
    #[error("fork-aware header-chain schema is already initialized")]
    AlreadyInitialized,
    /// Predecessor overlay rows require an explicit database resync.
    #[error(
        "incompatible predecessor header overlay found; resync this database before starting Zakura"
    )]
    IncompatibleLegacyOverlay,
    /// Full state has no finalized tip to authenticate initialization.
    #[error("header-chain initialization requires a finalized full-state anchor")]
    MissingFinalizedAnchor,
    /// The engine bootstrap is not an exact full-state ancestor of the finalized tip.
    #[error("engine bootstrap anchor is not an exact finalized full-state ancestor")]
    AnchorMismatch,
    /// Exact work construction failed.
    #[error("authenticated full-state path could not form an exact work coordinate")]
    Work,
    /// Authenticated full-state context is missing or incoherent.
    #[error("authenticated full-state header context is incoherent: {0}")]
    FullState(&'static str),
    /// The durable initialization or mandatory startup audit failed.
    #[error(transparent)]
    Store(#[from] HeaderChainStoreError),
    /// RocksDB failed while checking predecessor columns.
    #[error("predecessor header overlay check failed: {0}")]
    RocksDb(#[from] rocksdb::Error),
}

/// Initialize an absent DAG only from authenticated full-state facts.
///
/// The predecessor overlay is checked before any DAG row is written. Its rows are
/// never decoded, deleted, or reinterpreted.
pub(in crate::service) fn initialize_header_chain_reconciled(
    source: &ZakuraDb,
    config: &EngineConfig,
    restored_path: Vec<VerifiedHeaderRef>,
) -> Result<(HeaderChainRuntime, HeaderChainInitializationReport), HeaderChainInitializationError> {
    let store = HeaderChainStore::new(source.header_chain_disk_db());
    if store.metadata_row()?.is_some() {
        return Err(HeaderChainInitializationError::AlreadyInitialized);
    }
    if legacy_overlay_has_rows(source)? {
        return Err(HeaderChainInitializationError::IncompatibleLegacyOverlay);
    }

    let (anchor_height, anchor_hash) = source
        .tip()
        .ok_or(HeaderChainInitializationError::MissingFinalizedAnchor)?;
    let anchor = Frontier::new(anchor_height, anchor_hash);
    let (anchor_header, anchor_coordinate) = finalized_anchor(source, config, anchor)?;
    let evidence = initialization_evidence(anchor);
    let anchor_work = anchor_header
        .difficulty_threshold
        .to_work()
        .ok_or(HeaderChainInitializationError::Work)?;
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
    .map_err(|_| HeaderChainInitializationError::Work)?;
    let score = ChainScore::new(
        anchor_coordinate
            .suffix_after(anchor_coordinate)
            .map_err(|_| HeaderChainInitializationError::Work)?,
        anchor.hash,
    );
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
            header_best: anchor,
            verified_best: anchor,
        },
        header_best_score: score,
        oldest_retained_height: anchor.height,
        alarms: AlarmSet::default(),
        last_transition_id: evidence,
    };
    let changes = ChangeSet {
        put_nodes: vec![anchor_node],
        delete_nodes: Vec::new(),
        index_changes: IndexChanges {
            inserted: vec![anchor],
            deleted: Vec::new(),
        },
        candidate_tips: vec![(score, anchor.hash)],
        selected_projection: ProjectionDelta {
            remove_from: None,
            put: vec![anchor],
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
    let validation_context_rows = contexts.len();
    let (runtime, startup) = store.startup_reconciled(config, anchor, Vec::new(), restored_path)?;
    Ok((
        runtime,
        HeaderChainInitializationReport {
            anchor,
            validation_context_rows,
            startup,
        },
    ))
}

fn legacy_overlay_has_rows(source: &ZakuraDb) -> Result<bool, HeaderChainInitializationError> {
    let db = source.header_chain_disk_db();
    for family in [
        ZAKURA_HEADER_BY_HEIGHT,
        ZAKURA_HEADER_HASH_BY_HEIGHT,
        ZAKURA_HEADER_HEIGHT_BY_HASH,
    ] {
        let Some(cf) = db.cf_handle(family) else {
            continue;
        };
        if !db.raw_range_cf(&cf, &[], None)?.is_empty() {
            return Ok(true);
        }
    }
    Ok(false)
}

fn finalized_anchor(
    source: &ZakuraDb,
    config: &EngineConfig,
    finalized: Frontier,
) -> Result<(Arc<block::Header>, WorkCoordinate), HeaderChainInitializationError> {
    let bootstrap = config.bootstrap_anchor.frontier;
    if bootstrap.height > finalized.height {
        return Err(HeaderChainInitializationError::AnchorMismatch);
    }
    let (stored_bootstrap_hash, stored_bootstrap) = source
        .header_by_height(bootstrap.height)
        .ok_or(HeaderChainInitializationError::AnchorMismatch)?;
    if stored_bootstrap_hash != bootstrap.hash
        || stored_bootstrap.as_ref() != config.bootstrap_anchor.header.as_ref()
    {
        return Err(HeaderChainInitializationError::AnchorMismatch);
    }
    let bootstrap_work = stored_bootstrap
        .difficulty_threshold
        .to_work()
        .ok_or(HeaderChainInitializationError::Work)?;
    let mut coordinate = WorkCoordinate::new(bootstrap.hash, bootstrap_work.as_u256());
    let mut header = stored_bootstrap;
    let mut height = bootstrap.height;
    while height < finalized.height {
        height = height
            .next()
            .map_err(|_| HeaderChainInitializationError::Work)?;
        let (hash, next) = source
            .header_by_height(height)
            .ok_or(HeaderChainInitializationError::AnchorMismatch)?;
        if next.hash() != hash || next.previous_block_hash != header.hash() {
            return Err(HeaderChainInitializationError::AnchorMismatch);
        }
        let work = next
            .difficulty_threshold
            .to_work()
            .ok_or(HeaderChainInitializationError::Work)?;
        coordinate = coordinate
            .checked_add(work)
            .map_err(|_| HeaderChainInitializationError::Work)?;
        header = next;
    }
    if header.hash() != finalized.hash {
        return Err(HeaderChainInitializationError::AnchorMismatch);
    }
    Ok((header, coordinate))
}

fn validation_context(
    source: &ZakuraDb,
    anchor: Frontier,
    mut expected_hash: block::Hash,
) -> Result<Vec<HeaderValidationContextDisk>, HeaderChainInitializationError> {
    let mut contexts = Vec::new();
    let mut height = anchor.height;
    for _ in 0..27 {
        let Ok(previous) = height.previous() else {
            break;
        };
        let (hash, header) =
            source
                .header_by_height(previous)
                .ok_or(HeaderChainInitializationError::FullState(
                    "validation context has a gap",
                ))?;
        if header.hash() != hash || hash != expected_hash {
            return Err(HeaderChainInitializationError::FullState(
                "validation context linkage differs",
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

fn initialization_evidence(anchor: Frontier) -> EvidenceId {
    let mut hasher = Sha256::new();
    hasher.update(b"zakura-header-chain-full-state-initialization-v1");
    hasher.update(anchor.height.0.to_be_bytes());
    hasher.update(anchor.hash.0);
    EvidenceId::from_digest(hasher.finalize().into())
}
