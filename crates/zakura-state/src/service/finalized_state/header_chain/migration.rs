//! Initialization of the fork-aware header DAG from authenticated full state.

use std::sync::Arc;

use sha2::{Digest, Sha256};
use thiserror::Error;
use zakura_chain::{block, parameters::NetworkKind, work::difficulty::U256};
use zakura_header_chain::{
    AlarmSet, BodyValidationState, ChainScore, ChangeSet, EngineConfig, EngineMetadata, EngineMode,
    EvidenceId, FinalityEpoch, FinalityRecord, FinalitySource, Frontier, FrontierSet,
    HeaderChainDiskVersion, HeaderGeneration, HeaderNode, HeaderValidationState, IndexChanges,
    ProjectionDelta, StateVersion, VerifiedGeneration, VerifiedHeaderRef, WorkCoordinate,
};

use super::{HeaderChainRuntime, HeaderChainStore, HeaderChainStoreError, StartupReport};
use crate::service::finalized_state::{
    disk_db::{RawVisitError, ReadDisk, WriteDisk},
    disk_format::{
        header_chain::HeaderAuxDeliveryKey,
        header_chain_values::{
            decode_v1_aux_delivery, decode_v1_consensus_invalid_body_tombstone,
            decode_v1_engine_metadata, decode_v1_full_state_body_validation_evidence_authority,
            FullStateBodyValidationEvidenceAuthorityDisk, HeaderChainValueError,
            HeaderRowCountDisk, HeaderValidationContextDisk,
        },
        FallibleDiskValue, FromDisk, IntoDisk, RawBytes,
    },
    zakura_db::{
        block::{
            ZAKURA_HEADER_BY_HEIGHT, ZAKURA_HEADER_HASH_BY_HEIGHT, ZAKURA_HEADER_HEIGHT_BY_HASH,
        },
        ZakuraDb,
    },
    DiskWriteBatch, HEADER_AUX_DELIVERY, HEADER_BODY_EVIDENCE_AUTHORITY,
    HEADER_CONSENSUS_INVALID_BODY_TOMBSTONE, HEADER_ENGINE_META, HEADER_FINALITY_HISTORY,
    HEADER_VALIDATION_CONTEXT,
};

impl HeaderChainStore {
    /// Atomically migrate released version-one rows to the current format.
    pub(in crate::service) fn migrate_v1_to_current(
        &self,
        config: &EngineConfig,
    ) -> Result<bool, HeaderChainStoreError> {
        let _writer = self
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        let metadata_cf = self.cf(HEADER_ENGINE_META)?;
        let Some(metadata_bytes) = self.db.raw_get_cf(&metadata_cf, super::METADATA_KEY)? else {
            return Ok(false);
        };
        let mut version_bytes = [0; 4];
        version_bytes.copy_from_slice(
            metadata_bytes
                .get(..4)
                .ok_or(HeaderChainValueError::Truncated)?,
        );
        let version = u32::from_be_bytes(version_bytes);
        if version == HeaderChainDiskVersion::CURRENT.0 {
            EngineMetadata::decode(&metadata_bytes)?;
            let tombstone_count_exists = self
                .get_value::<HeaderRowCountDisk>(HEADER_ENGINE_META, super::TOMBSTONE_COUNT_KEY)?
                .is_some();
            let finality_count_exists = self
                .get_value::<HeaderRowCountDisk>(
                    HEADER_ENGINE_META,
                    super::FINALITY_HISTORY_COUNT_KEY,
                )?
                .is_some();
            let v1_authorities = self.first_row_has_version(HEADER_BODY_EVIDENCE_AUTHORITY, 1)?;
            let v1_tombstones =
                self.first_row_has_version(HEADER_CONSENSUS_INVALID_BODY_TOMBSTONE, 1)?;
            if tombstone_count_exists && finality_count_exists && !v1_authorities && !v1_tombstones
            {
                return Ok(false);
            }
            let mut batch = DiskWriteBatch::new();
            let authority_rows = if v1_authorities {
                self.stage_v1_body_evidence_authorities(config, &mut batch)?
            } else {
                0
            };
            let tombstone_rows = self.stage_tombstone_count(&mut batch)?;
            let finality_rows = self.stage_finality_history_count(&mut batch)?;
            self.db.write(batch)?;
            tracing::info!(
                authority_rows,
                tombstone_rows,
                finality_rows,
                disk_format = HeaderChainDiskVersion::CURRENT.0,
                "completed an interrupted durable header-chain migration"
            );
            return Ok(true);
        }

        let mut metadata =
            decode_v1_engine_metadata(&metadata_bytes, config.network_policy_digest())?;
        if metadata.network_id != config.network().kind() {
            return Err(HeaderChainStoreError::Incoherent(
                "version-one network kind does not match the configured network",
            ));
        }
        if metadata.network_id != NetworkKind::Mainnet {
            return Err(HeaderChainStoreError::Incoherent(
                "version-one network policy is ambiguous; rebuild the header-chain database",
            ));
        }
        let limit =
            zakura_header_chain::RowLimit::new(config.limits.max_aux_deliveries_total.get());
        let aux_cf = self.cf(HEADER_AUX_DELIVERY)?;
        let mut batch = DiskWriteBatch::new();
        let mut rows = 0;
        self.db
            .raw_visit_cf(&aux_cf, &mut |key, value| {
                if rows == limit.get() {
                    return Err(HeaderChainStoreError::Store(
                        zakura_header_chain::StoreError::LimitExceeded {
                            collection: zakura_header_chain::StoreCollection::AuxiliaryDeliveries,
                            limit,
                        },
                    ));
                }
                rows += 1;
                if key.len() != 64 {
                    return Err(HeaderChainStoreError::Incoherent(
                        "invalid version-one auxiliary key width",
                    ));
                }
                let key = HeaderAuxDeliveryKey::from_bytes(key);
                let delivery = decode_v1_aux_delivery(value)?;
                if delivery.header_hash != key.header || delivery.delivery_id != key.delivery {
                    return Err(HeaderChainStoreError::Incoherent(
                        "version-one auxiliary key/value mismatch",
                    ));
                }
                self.put_value(&mut batch, HEADER_AUX_DELIVERY, key.as_bytes(), &delivery)?;
                Ok(())
            })
            .map_err(|error| match error {
                RawVisitError::RocksDb(error) => HeaderChainStoreError::RocksDb(error),
                RawVisitError::Visitor(error) => error,
            })?;

        metadata.disk_format = HeaderChainDiskVersion::CURRENT;
        metadata.state_version = metadata.state_version.checked_next()?;
        metadata.last_transition = None;
        let authority_rows = self.stage_v1_body_evidence_authorities(config, &mut batch)?;
        let tombstone_rows = self.stage_tombstone_count(&mut batch)?;
        let finality_rows = self.stage_finality_history_count(&mut batch)?;
        self.put_value(
            &mut batch,
            HEADER_ENGINE_META,
            super::METADATA_KEY,
            &metadata,
        )?;
        self.db.write(batch)?;
        tracing::info!(
            auxiliary_rows = rows,
            authority_rows,
            tombstone_rows,
            finality_rows,
            from_version = 1,
            to_version = HeaderChainDiskVersion::CURRENT.0,
            "migrated the durable header-chain format"
        );
        Ok(true)
    }

    fn first_row_has_version(
        &self,
        family: &'static str,
        version: u8,
    ) -> Result<bool, HeaderChainStoreError> {
        let cf = self.cf(family)?;
        Ok(self
            .db
            .raw_first_cf(&cf)?
            .is_some_and(|(_, value)| value.first() == Some(&version)))
    }

    fn stage_v1_body_evidence_authorities(
        &self,
        config: &EngineConfig,
        batch: &mut DiskWriteBatch,
    ) -> Result<usize, HeaderChainStoreError> {
        let authority_cf = self.cf(HEADER_BODY_EVIDENCE_AUTHORITY)?;
        // Authorities cover the finalized anchor, bounded non-finalized nodes, and bounded
        // tombstones whose consensus-invalid nodes have already been pruned.
        let maximum_authorities = config
            .limits
            .max_non_finalized_nodes
            .get()
            .checked_add(1)
            .and_then(|maximum| maximum.checked_add(super::TOMBSTONE_LIMIT))
            .ok_or(HeaderChainStoreError::Incoherent(
                "body-evidence authority limit overflow",
            ))?;
        let limit = zakura_header_chain::RowLimit::new(maximum_authorities);
        let mut rows = 0;
        self.db
            .raw_visit_cf(&authority_cf, &mut |key, value| {
                if rows == limit.get() {
                    return Err(HeaderChainStoreError::Store(
                        zakura_header_chain::StoreError::LimitExceeded {
                            collection: zakura_header_chain::StoreCollection::HeaderNodes,
                            limit,
                        },
                    ));
                }
                rows += 1;
                let hash = block::Hash(key.try_into().map_err(|_| {
                    HeaderChainStoreError::Incoherent(
                        "invalid version-one body-evidence authority key width",
                    )
                })?);
                let authority = match value.first() {
                    Some(1) => {
                        // v1 omitted height. Pruned consensus-invalid headers keep authority
                        // rows after the node is deleted, so there is no height to recover.
                        let Some(node) = self.header_node(hash)? else {
                            self.delete_raw(batch, HEADER_BODY_EVIDENCE_AUTHORITY, hash.0)?;
                            return Ok(());
                        };
                        decode_v1_full_state_body_validation_evidence_authority(value, node.height)?
                    }
                    _ => FullStateBodyValidationEvidenceAuthorityDisk::decode(value)?,
                };
                if !authority.attests_to_body_validation_state(
                    hash,
                    &match &authority {
                        FullStateBodyValidationEvidenceAuthorityDisk::Verified {
                            evidence, ..
                        } => BodyValidationState::Verified {
                            evidence: *evidence,
                        },
                        FullStateBodyValidationEvidenceAuthorityDisk::ConsensusInvalid(
                            tombstone,
                        ) => BodyValidationState::ConsensusInvalid {
                            evidence: tombstone.evidence,
                            rule: tombstone.rule.clone(),
                        },
                    },
                ) {
                    return Err(HeaderChainStoreError::Incoherent(
                        "body-evidence authority key/value mismatch",
                    ));
                }
                if value.first() == Some(&1) {
                    self.put_value(batch, HEADER_BODY_EVIDENCE_AUTHORITY, hash.0, &authority)?;
                }
                Ok(())
            })
            .map_err(|error| match error {
                RawVisitError::RocksDb(error) => HeaderChainStoreError::RocksDb(error),
                RawVisitError::Visitor(error) => error,
            })?;
        Ok(rows)
    }

    fn stage_finality_history_count(
        &self,
        batch: &mut DiskWriteBatch,
    ) -> Result<usize, HeaderChainStoreError> {
        let finality_cf = self.cf(HEADER_FINALITY_HISTORY)?;
        let limit = zakura_header_chain::RowLimit::new(super::FINALITY_HISTORY_LIMIT);
        let mut rows = 0;
        self.db
            .raw_visit_cf(&finality_cf, &mut |_, _| {
                if rows == limit.get() {
                    return Err(HeaderChainStoreError::Store(
                        zakura_header_chain::StoreError::LimitExceeded {
                            collection: zakura_header_chain::StoreCollection::FinalityHistory,
                            limit,
                        },
                    ));
                }
                rows += 1;
                Ok(())
            })
            .map_err(|error| match error {
                RawVisitError::RocksDb(error) => HeaderChainStoreError::RocksDb(error),
                RawVisitError::Visitor(error) => error,
            })?;
        self.put_value(
            batch,
            HEADER_ENGINE_META,
            super::FINALITY_HISTORY_COUNT_KEY,
            &HeaderRowCountDisk(u64::try_from(rows).map_err(|_| {
                HeaderChainStoreError::Incoherent("finality history count does not fit u64")
            })?),
        )?;
        Ok(rows)
    }

    fn stage_tombstone_count(
        &self,
        batch: &mut DiskWriteBatch,
    ) -> Result<usize, HeaderChainStoreError> {
        let tombstone_cf = self.cf(HEADER_CONSENSUS_INVALID_BODY_TOMBSTONE)?;
        let mut tombstone_rows = 0;
        self.db
            .raw_visit_cf(&tombstone_cf, &mut |key, value| {
                if tombstone_rows == super::TOMBSTONE_LIMIT {
                    return Err(HeaderChainStoreError::Store(
                        zakura_header_chain::StoreError::LimitExceeded {
                            collection:
                                zakura_header_chain::StoreCollection::ConsensusInvalidBodyTombstones,
                            limit: zakura_header_chain::RowLimit::new(super::TOMBSTONE_LIMIT),
                        },
                    ));
                }
                tombstone_rows += 1;
                let hash = block::Hash(key.try_into().map_err(|_| {
                    HeaderChainStoreError::Incoherent(
                        "invalid version-one consensus-invalid tombstone key width",
                    )
                })?);
                let tombstone = match value.first() {
                    Some(1) => {
                        // v1 omitted height. Tombstones are append-only evidence for pruned
                        // headers, so a missing node is a legal v1 layout, not corruption.
                        let Some(node) = self.header_node(hash)? else {
                            self.delete_raw(
                                batch,
                                HEADER_CONSENSUS_INVALID_BODY_TOMBSTONE,
                                hash.0,
                            )?;
                            tombstone_rows -= 1;
                            return Ok(());
                        };
                        decode_v1_consensus_invalid_body_tombstone(value, node.height)?
                    }
                    _ => zakura_header_chain::ConsensusInvalidBodyTombstone::decode(value)?,
                };
                if tombstone.hash != hash {
                    return Err(HeaderChainStoreError::Incoherent(
                        "consensus-invalid tombstone key/value mismatch",
                    ));
                }
                if value.first() == Some(&1) {
                    self.put_value(
                        batch,
                        HEADER_CONSENSUS_INVALID_BODY_TOMBSTONE,
                        hash.0,
                        &tombstone,
                    )?;
                }
                Ok(())
            })
            .map_err(|error| match error {
                RawVisitError::RocksDb(error) => HeaderChainStoreError::RocksDb(error),
                RawVisitError::Visitor(error) => error,
            })?;
        self.put_value(
            batch,
            HEADER_ENGINE_META,
            super::TOMBSTONE_COUNT_KEY,
            &HeaderRowCountDisk(u64::try_from(tombstone_rows).map_err(|_| {
                HeaderChainStoreError::Incoherent("tombstone count does not fit u64")
            })?),
        )?;
        Ok(tombstone_rows)
    }
}

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
    /// Full state has no finalized tip to authenticate initialization.
    #[error("header-chain initialization requires a finalized full-state anchor")]
    MissingFinalizedAnchor,
    /// The engine bootstrap is above the finalized tip, or the finalized anchor is incoherent.
    #[error("engine bootstrap or finalized full-state anchor is incoherent")]
    AnchorMismatch,
    /// Exact finalized-anchor work construction failed.
    #[error("authenticated finalized anchor could not form an exact work coordinate")]
    Work,
    /// Authenticated full-state context is missing or incoherent.
    #[error("authenticated full-state header context is incoherent: {0}")]
    FullState(&'static str),
    /// The durable initialization or mandatory startup audit failed.
    #[error(transparent)]
    Store(#[from] HeaderChainStoreError),
    /// RocksDB rejected the atomic legacy-overlay replacement.
    #[error("header-chain initialization database write failed: {0}")]
    RocksDb(#[from] rocksdb::Error),
}

/// Initialize an absent DAG only from authenticated full-state facts.
///
/// Initialization discards obsolete predecessor overlay rows in the same atomic
/// batch that publishes the replacement DAG.
pub(in crate::service) fn initialize_header_chain_reconciled(
    source: &ZakuraDb,
    config: &EngineConfig,
    restored_path: Vec<VerifiedHeaderRef>,
) -> Result<(HeaderChainRuntime, HeaderChainInitializationReport), HeaderChainInitializationError> {
    let store = HeaderChainStore::new(source.header_chain_disk_db());
    if store.metadata_row()?.is_some() {
        return Err(HeaderChainInitializationError::AlreadyInitialized);
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
        previous: config.bootstrap_anchor().frontier,
        current: anchor,
        source: match config.mode {
            EngineMode::Integrated => FinalitySource::FullState { evidence },
            EngineMode::HeadersOnly => FinalitySource::MigratedHeadersOnly,
        },
        epoch: FinalityEpoch::new(0),
    };
    let metadata = EngineMetadata {
        disk_format: HeaderChainDiskVersion::CURRENT,
        mode: config.mode,
        network_id: config.network().kind(),
        network_policy_digest: config.network_policy_digest(),
        anchor_manifest_digest: config.trust_anchor_digest(),
        work_origin: anchor,
        state_version: StateVersion::new(1),
        header_generation: HeaderGeneration::new(1),
        verified_generation: VerifiedGeneration::new(1),
        finality_epoch: FinalityEpoch::new(0),
        headers_only_migration_epoch: None,
        frontiers: FrontierSet {
            finalized: anchor,
            header_best: anchor,
            verified_best: anchor,
        },
        header_best_score: score,
        oldest_retained_height: anchor.height,
        alarms: AlarmSet::default(),
        last_transition: None,
    };
    let changes = ChangeSet {
        put_nodes: vec![anchor_node],
        delete_nodes: Vec::new(),
        put_consensus_invalid_body_tombstones: Vec::new(),
        index_changes: IndexChanges {
            inserted: vec![anchor],
            deleted: Vec::new(),
        },
        selected_projection: ProjectionDelta {
            remove_before: None,
            remove_from: None,
            put: vec![anchor],
        },
        verified_projection: ProjectionDelta {
            remove_before: None,
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
    clear_legacy_overlay(source, &mut base_batch);
    for context in &contexts {
        store.put_value(
            &mut base_batch,
            HEADER_VALIDATION_CONTEXT,
            context.header.hash().0,
            context,
        )?;
    }
    let batch = store.batch_for_combined(&changes, base_batch)?;
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

fn clear_legacy_overlay(source: &ZakuraDb, batch: &mut super::super::DiskWriteBatch) {
    let db = source.header_chain_disk_db();
    for family in [
        ZAKURA_HEADER_BY_HEIGHT,
        ZAKURA_HEADER_HASH_BY_HEIGHT,
        ZAKURA_HEADER_HEIGHT_BY_HASH,
    ] {
        let Some(cf) = db.cf_handle(family) else {
            continue;
        };
        let Some((first, _)) = db.zs_first_key_value::<_, RawBytes, RawBytes>(&cf) else {
            continue;
        };
        let (last, _) = db
            .zs_last_key_value::<_, RawBytes, RawBytes>(&cf)
            .expect("last legacy overlay row exists because the first row exists");
        batch.zs_delete_range(&cf, &first, &last);
        batch.zs_delete(&cf, &last);
    }
}

fn finalized_anchor(
    source: &ZakuraDb,
    config: &EngineConfig,
    finalized: Frontier,
) -> Result<(Arc<block::Header>, WorkCoordinate), HeaderChainInitializationError> {
    let bootstrap = config.bootstrap_anchor().frontier;
    if bootstrap.height > finalized.height {
        return Err(HeaderChainInitializationError::AnchorMismatch);
    }
    let (stored_bootstrap_hash, stored_bootstrap) =
        finalized_header_by_height(source, bootstrap.height)
            .ok_or(HeaderChainInitializationError::AnchorMismatch)?;
    if stored_bootstrap_hash != bootstrap.hash
        || stored_bootstrap.as_ref() != config.bootstrap_anchor().header.as_ref()
    {
        return Err(HeaderChainInitializationError::AnchorMismatch);
    }
    let header = source
        .block_header(finalized.height.into())
        .ok_or(HeaderChainInitializationError::AnchorMismatch)?;
    if header.hash() != finalized.hash {
        return Err(HeaderChainInitializationError::AnchorMismatch);
    }
    // Every selectable branch descends from finality, so pre-finality work is a
    // shared constant. Rebasing here avoids rescanning the complete finalized chain.
    let coordinate = WorkCoordinate::new(finalized.hash, U256::zero());
    Ok((header, coordinate))
}

fn validation_context(
    source: &ZakuraDb,
    anchor: Frontier,
    expected_hash: block::Hash,
) -> Result<Vec<HeaderValidationContextDisk>, HeaderChainInitializationError> {
    linked_validation_context(anchor, expected_hash, |height| {
        finalized_header_by_height(source, height)
    })
}

fn finalized_header_by_height(
    source: &ZakuraDb,
    height: block::Height,
) -> Option<(block::Hash, Arc<block::Header>)> {
    let hash = source.hash(height)?;
    let header = source.block_header(height.into())?;
    Some((hash, header))
}

fn linked_validation_context(
    anchor: Frontier,
    mut expected_hash: block::Hash,
    mut header_by_height: impl FnMut(block::Height) -> Option<(block::Hash, Arc<block::Header>)>,
) -> Result<Vec<HeaderValidationContextDisk>, HeaderChainInitializationError> {
    let mut contexts = Vec::new();
    let mut height = anchor.height;
    for _ in 0..27 {
        let Ok(previous) = height.previous() else {
            break;
        };
        let (hash, header) = header_by_height(previous).ok_or(
            HeaderChainInitializationError::FullState("validation context has a gap"),
        )?;
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

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use zakura_chain::block::genesis::regtest_genesis_block;

    use super::*;

    fn linked_headers(count: u32) -> Vec<Arc<block::Header>> {
        let mut headers = vec![regtest_genesis_block().header.clone()];
        for height in 1..count {
            let previous = headers
                .last()
                .expect("the generated chain always starts at genesis");
            let mut header = **previous;
            header.previous_block_hash = previous.hash();
            header.time += Duration::seconds(1);
            header.nonce.0[0] = u8::try_from(height).expect("the test chain is shorter than 256");
            headers.push(Arc::new(header));
        }
        headers
    }

    #[test]
    fn later_anchor_predecessor_context_has_exact_one_to_twenty_eight_boundary() {
        let headers = linked_headers(30);

        for anchor_height in 0..=29 {
            let anchor_index = usize::try_from(anchor_height).expect("the test height fits");
            let anchor_header = &headers[anchor_index];
            let anchor = Frontier::new(block::Height(anchor_height), anchor_header.hash());
            let contexts =
                linked_validation_context(anchor, anchor_header.previous_block_hash, |height| {
                    let header =
                        headers[usize::try_from(height.0).expect("the test height fits")].clone();
                    Some((header.hash(), header))
                })
                .expect("the exact backward-linked context is authenticated");

            let expected_predecessors =
                usize::try_from(anchor_height.min(27)).expect("the bound fits in usize");
            assert_eq!(contexts.len(), expected_predecessors);
            assert_eq!(
                contexts.len() + 1,
                usize::try_from((anchor_height + 1).min(28)).expect("the bound fits in usize"),
                "the anchor plus predecessor facts has the exact one-to-28-header boundary"
            );
            if contexts.is_empty() {
                continue;
            }
            assert_eq!(
                contexts.last().map(|context| context.height),
                Some(block::Height(anchor_height - 1))
            );
            assert_eq!(
                contexts.first().map(|context| context.height),
                Some(block::Height(
                    anchor_height
                        - u32::try_from(expected_predecessors)
                            .expect("the fixed predecessor bound fits in u32")
                ))
            );
            for pair in contexts.windows(2) {
                assert_eq!(pair[1].header.previous_block_hash, pair[0].header.hash());
            }
            assert_eq!(
                anchor_header.previous_block_hash,
                contexts
                    .last()
                    .expect("a non-genesis anchor has context")
                    .header
                    .hash()
            );
        }
    }

    #[test]
    fn later_anchor_predecessor_context_rejects_gap_hash_and_link_corruption() {
        let headers = linked_headers(30);
        let anchor_header = headers.last().expect("the generated chain is nonempty");
        let anchor = Frontier::new(block::Height(29), anchor_header.hash());

        assert!(matches!(
            linked_validation_context(anchor, anchor_header.previous_block_hash, |_| None),
            Err(HeaderChainInitializationError::FullState(
                "validation context has a gap"
            ))
        ));
        assert!(matches!(
            linked_validation_context(anchor, block::Hash([0xff; 32]), |height| {
                let header = headers
                    [usize::try_from(height.0).expect("the generated test height fits in usize")]
                .clone();
                Some((header.hash(), header))
            },),
            Err(HeaderChainInitializationError::FullState(
                "validation context linkage differs"
            ))
        ));
        assert!(matches!(
            linked_validation_context(anchor, anchor_header.previous_block_hash, |height| {
                let header = headers
                    [usize::try_from(height.0).expect("the generated test height fits in usize")]
                .clone();
                let hash = if height == block::Height(27) {
                    block::Hash([0xee; 32])
                } else {
                    header.hash()
                };
                Some((hash, header))
            },),
            Err(HeaderChainInitializationError::FullState(
                "validation context linkage differs"
            ))
        ));
    }
}
