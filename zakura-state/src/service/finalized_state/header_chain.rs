//! Durable adapter for the fork-aware header-chain transition engine.

#![allow(dead_code)] // Constructed by the full-state migration and service wiring in PR-9.

use std::sync::{Arc, Mutex};

use sha2::{Digest, Sha256};
use thiserror::Error;
use zakura_chain::block;
use zakura_header_chain::{
    apply_transition, ApplyResult, AuxDelta, ChainScore, ChangeSet, CommittedTransition,
    EligibilityReason, EngineMetadata, EngineSnapshot, EvidenceId, FinalityRecord, Frontier,
    HeaderNode, NoChangeReceipt, StaleReceipt, StoreError, StoreRead, TransitionContext,
    TransitionFailure, TransitionRequest, ValidationLease,
};

use super::{
    disk_format::{
        header_chain::{
            EligibilityReasonKind, HeaderAuxDeliveryKey, HeaderCandidateKey, HeaderChildKey,
            HeaderDeferredKey, HeaderEligibilityRootKey, HeaderFinalityKey, HeaderHeightHashKey,
            HeaderHeightKey,
        },
        header_chain_values::{
            HeaderAuxDeliveryDisk, HeaderChainValue, HeaderChainValueError,
            HeaderEligibilityReasonDisk, HeaderEngineMetadataDisk, HeaderFinalityRecordDisk,
            HeaderNodeDisk, HeaderValidationContextDisk,
        },
        FromDisk, IntoDisk, RawBytes,
    },
    DiskDb, DiskWriteBatch, WriteDisk, HEADER_AUX_DELIVERY, HEADER_CANDIDATE, HEADER_CHILD,
    HEADER_DEFERRED, HEADER_ELIGIBILITY_ROOT, HEADER_ENGINE_META, HEADER_FINALITY_HISTORY,
    HEADER_HEIGHT_HASH, HEADER_NODE_BY_HASH, HEADER_SELECTED, HEADER_VALIDATION_CONTEXT,
    HEADER_VERIFIED,
};

const METADATA_KEY: &[u8] = b"";

/// Failure at the durable header-chain boundary.
#[derive(Debug, Error)]
pub enum HeaderChainStoreError {
    /// The database has not yet been initialized by migration/bootstrap.
    #[error("header-chain metadata is not initialized")]
    Uninitialized,
    /// A durable key or value was malformed or internally contradictory.
    #[error("incoherent durable header-chain rows: {0}")]
    Incoherent(&'static str),
    /// Stable value encoding failed before the batch was committed.
    #[error(transparent)]
    Codec(#[from] HeaderChainValueError),
    /// Pure transition planning rejected the request before commit.
    #[error(transparent)]
    Transition(#[from] TransitionFailure),
    /// RocksDB rejected the one atomic write batch.
    #[error("header-chain atomic write failed: {0}")]
    RocksDb(#[from] rocksdb::Error),
    /// The serialized writer lock was poisoned by a prior panic.
    #[error("header-chain serialized writer lock is poisoned")]
    WriterPoisoned,
}

/// One RocksDB-backed header DAG with a process-local serialized writer.
#[derive(Clone, Debug)]
pub struct HeaderChainStore {
    db: DiskDb,
    writer: Arc<Mutex<()>>,
}

impl HeaderChainStore {
    /// Attach the header-chain adapter to the existing finalized-state database.
    pub fn new(db: DiskDb) -> Self {
        Self {
            db,
            writer: Arc::new(Mutex::new(())),
        }
    }

    /// Apply the sole pure planner under the serialized CAS and commit its complete write set.
    pub fn apply(
        &self,
        request: TransitionRequest,
        context: &TransitionContext<'_>,
    ) -> Result<ApplyResult, HeaderChainStoreError> {
        let _writer = self
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        let event = request.event.idempotency_key();
        let branch = request.event.work_owner().map(|owner| owner.branch);
        let plan = match apply_transition(self, request, context) {
            Ok(plan) => plan,
            Err(TransitionFailure::Stale { current }) => {
                return Ok(ApplyResult::Stale(StaleReceipt {
                    current_version: current,
                    branch,
                }));
            }
            Err(error) => return Err(error.into()),
        };
        if plan.is_no_change() {
            return Ok(ApplyResult::NoChange(NoChangeReceipt {
                state_version: plan.before().state_version,
                event,
            }));
        }

        let durable_tx_id = plan.change_set().metadata.state_version.get();
        let batch = self.batch_for(plan.change_set())?;
        self.db.write(batch)?;
        Ok(ApplyResult::Committed(Box::new(
            plan.into_committed_receipt(durable_tx_id),
        )))
    }

    /// Bootstrap an empty header schema with one already-authenticated anchor.
    ///
    /// Migration calls this only while publication and normal writers are disabled.
    pub fn initialize(
        &self,
        metadata: EngineMetadata,
        anchor: HeaderNode,
    ) -> Result<CommittedTransition, HeaderChainStoreError> {
        let _writer = self
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        if self.metadata_row()?.is_some() {
            return Err(HeaderChainStoreError::Incoherent(
                "header-chain metadata already exists",
            ));
        }
        if metadata.frontiers.finalized != Frontier::new(anchor.height, anchor.hash)
            || metadata.frontiers.header_best != metadata.frontiers.finalized
            || metadata.frontiers.verified_best != metadata.frontiers.finalized
            || metadata.state_version.get() == 0
        {
            return Err(HeaderChainStoreError::Incoherent(
                "initial metadata does not describe the anchor",
            ));
        }
        let change_set = ChangeSet {
            put_nodes: vec![anchor.clone()],
            delete_nodes: Vec::new(),
            index_changes: zakura_header_chain::IndexChanges {
                inserted: vec![metadata.frontiers.finalized],
                deleted: Vec::new(),
            },
            candidate_tips: vec![(metadata.header_best_score, anchor.hash)],
            selected_projection: zakura_header_chain::ProjectionDelta {
                remove_from: None,
                put: vec![metadata.frontiers.finalized],
            },
            verified_projection: zakura_header_chain::ProjectionDelta {
                remove_from: None,
                put: vec![metadata.frontiers.finalized],
            },
            eligibility_changes: Vec::new(),
            aux_changes: Vec::new(),
            finality_append: None,
            metadata: metadata.clone(),
        };
        self.db.write(self.batch_for(&change_set)?)?;
        let current = metadata.snapshot();
        Ok(CommittedTransition {
            previous: current.clone(),
            current,
            cause: zakura_header_chain::TransitionCause::Recovery,
            inserted: vec![anchor.hash],
            eligibility_changed: Vec::new(),
            evicted: Vec::new(),
            retired_work: zakura_header_chain::RetiredWork::default(),
            durable_tx_id: metadata.state_version.get(),
        })
    }

    fn batch_for(&self, changes: &ChangeSet) -> Result<DiskWriteBatch, HeaderChainStoreError> {
        let mut batch = DiskWriteBatch::new();

        for hash in &changes.delete_nodes {
            if let Some(node) = self.node(*hash).map_err(|_| {
                HeaderChainStoreError::Incoherent("deleted node could not be decoded")
            })? {
                self.delete_raw(&mut batch, HEADER_NODE_BY_HASH, hash.0)?;
                self.delete_raw(
                    &mut batch,
                    HEADER_CHILD,
                    HeaderChildKey {
                        parent: node.parent_hash,
                        child: *hash,
                    }
                    .as_bytes(),
                )?;
                self.delete_raw(
                    &mut batch,
                    HEADER_HEIGHT_HASH,
                    HeaderHeightHashKey {
                        height: node.height,
                        hash: *hash,
                    }
                    .as_bytes(),
                )?;
                self.delete_deferred_for(&mut batch, &node)?;
                self.delete_reason_rows(&mut batch, *hash)?;
            }
            for (key, _) in self.scan_prefix(HEADER_CHILD, &hash.0)? {
                self.delete_raw(&mut batch, HEADER_CHILD, key)?;
            }
        }

        for node in &changes.put_nodes {
            if let Some(old) = self.node(node.hash).map_err(|_| {
                HeaderChainStoreError::Incoherent("replaced node could not be decoded")
            })? {
                self.delete_deferred_for(&mut batch, &old)?;
            }
            self.put_value(
                &mut batch,
                HEADER_NODE_BY_HASH,
                node.hash.0,
                &HeaderNodeDisk::from_domain(node),
            )?;
            if node.hash != changes.metadata.frontiers.finalized.hash {
                self.put_empty(
                    &mut batch,
                    HEADER_CHILD,
                    HeaderChildKey {
                        parent: node.parent_hash,
                        child: node.hash,
                    }
                    .as_bytes(),
                )?;
            }
            self.put_empty(
                &mut batch,
                HEADER_HEIGHT_HASH,
                HeaderHeightHashKey {
                    height: node.height,
                    hash: node.hash,
                }
                .as_bytes(),
            )?;
            if let zakura_header_chain::HeaderValidationState::DeferredUntil(until) =
                node.validation
            {
                let key = HeaderDeferredKey::new(
                    until.timestamp(),
                    until.timestamp_subsec_nanos(),
                    node.hash,
                )
                .map_err(|_| HeaderChainStoreError::Incoherent("invalid deferred timestamp"))?;
                self.put_empty(&mut batch, HEADER_DEFERRED, key.as_bytes())?;
            }
            self.delete_reason_rows(&mut batch, node.hash)?;
            for reason in &node.eligibility.direct_reasons {
                self.put_reason(&mut batch, node.hash, reason)?;
            }
        }

        self.replace_candidates(&mut batch, &changes.candidate_tips)?;
        self.apply_projection(&mut batch, HEADER_SELECTED, &changes.selected_projection)?;
        self.apply_projection(&mut batch, HEADER_VERIFIED, &changes.verified_projection)?;

        for delta in &changes.aux_changes {
            match delta {
                AuxDelta::Put(delivery) => self.put_value(
                    &mut batch,
                    HEADER_AUX_DELIVERY,
                    HeaderAuxDeliveryKey {
                        header: delivery.header_hash,
                        delivery: delivery.delivery_id,
                    }
                    .as_bytes(),
                    &HeaderAuxDeliveryDisk(**delivery),
                )?,
                AuxDelta::Delete(delivery) => {
                    for (key, _) in self.scan_raw(HEADER_AUX_DELIVERY)? {
                        if key.len() == 64 && key[32..] == delivery.digest() {
                            self.delete_raw(&mut batch, HEADER_AUX_DELIVERY, key)?;
                        }
                    }
                }
            }
        }

        if let Some(record) = changes.finality_append {
            self.put_value(
                &mut batch,
                HEADER_FINALITY_HISTORY,
                HeaderFinalityKey(record.epoch).as_bytes(),
                &HeaderFinalityRecordDisk(record),
            )?;
        }

        // The singleton logical root is deliberately enqueued last in the same atomic batch.
        self.put_value(
            &mut batch,
            HEADER_ENGINE_META,
            METADATA_KEY,
            &HeaderEngineMetadataDisk(changes.metadata.clone()),
        )?;
        Ok(batch)
    }

    fn metadata_row(&self) -> Result<Option<EngineMetadata>, HeaderChainStoreError> {
        self.get_value::<HeaderEngineMetadataDisk>(HEADER_ENGINE_META, METADATA_KEY)
            .map(|value| value.map(|value| value.0))
    }

    fn direct_reasons(
        &self,
        hash: block::Hash,
    ) -> Result<Vec<EligibilityReason>, HeaderChainStoreError> {
        let mut reasons = Vec::new();
        for tag in 0..=4 {
            let mut prefix = Vec::with_capacity(33);
            prefix.push(tag);
            prefix.extend(hash.0);
            for (key, value) in self.scan_prefix(HEADER_ELIGIBILITY_ROOT, &prefix)? {
                if key.len() != 65 {
                    return Err(HeaderChainStoreError::Incoherent(
                        "invalid eligibility-root key width",
                    ));
                }
                let key = HeaderEligibilityRootKey::try_from_bytes(&key)
                    .map_err(|_| HeaderChainStoreError::Incoherent("invalid eligibility key"))?;
                let reason = HeaderEligibilityReasonDisk::decode(&value)?.into_domain();
                if reason_kind(&reason) != key.kind || reason_evidence(&reason) != key.evidence {
                    return Err(HeaderChainStoreError::Incoherent(
                        "eligibility key/value mismatch",
                    ));
                }
                reasons.push(reason);
            }
        }
        Ok(reasons)
    }

    fn delete_reason_rows(
        &self,
        batch: &mut DiskWriteBatch,
        hash: block::Hash,
    ) -> Result<(), HeaderChainStoreError> {
        for tag in 0..=4 {
            let mut prefix = Vec::with_capacity(33);
            prefix.push(tag);
            prefix.extend(hash.0);
            for (key, _) in self.scan_prefix(HEADER_ELIGIBILITY_ROOT, &prefix)? {
                self.delete_raw(batch, HEADER_ELIGIBILITY_ROOT, key)?;
            }
        }
        Ok(())
    }

    fn put_reason(
        &self,
        batch: &mut DiskWriteBatch,
        root: block::Hash,
        reason: &EligibilityReason,
    ) -> Result<(), HeaderChainStoreError> {
        let key = HeaderEligibilityRootKey {
            kind: reason_kind(reason),
            root,
            evidence: reason_evidence(reason),
        };
        self.put_value(
            batch,
            HEADER_ELIGIBILITY_ROOT,
            key.as_bytes(),
            &HeaderEligibilityReasonDisk::from_domain(reason),
        )
    }

    fn delete_deferred_for(
        &self,
        batch: &mut DiskWriteBatch,
        node: &HeaderNode,
    ) -> Result<(), HeaderChainStoreError> {
        if let zakura_header_chain::HeaderValidationState::DeferredUntil(until) = node.validation {
            let key = HeaderDeferredKey::new(
                until.timestamp(),
                until.timestamp_subsec_nanos(),
                node.hash,
            )
            .map_err(|_| HeaderChainStoreError::Incoherent("invalid deferred timestamp"))?;
            self.delete_raw(batch, HEADER_DEFERRED, key.as_bytes())?;
        }
        Ok(())
    }

    fn replace_candidates(
        &self,
        batch: &mut DiskWriteBatch,
        candidates: &[(ChainScore, block::Hash)],
    ) -> Result<(), HeaderChainStoreError> {
        for (key, _) in self.scan_raw(HEADER_CANDIDATE)? {
            self.delete_raw(batch, HEADER_CANDIDATE, key)?;
        }
        for (score, hash) in candidates {
            if score.tip_hash != *hash {
                return Err(HeaderChainStoreError::Incoherent(
                    "candidate score/hash mismatch",
                ));
            }
            self.put_empty(
                batch,
                HEADER_CANDIDATE,
                HeaderCandidateKey(*score).as_bytes(),
            )?;
        }
        Ok(())
    }

    fn apply_projection(
        &self,
        batch: &mut DiskWriteBatch,
        family: &'static str,
        delta: &zakura_header_chain::ProjectionDelta,
    ) -> Result<(), HeaderChainStoreError> {
        if let Some(remove_from) = delta.remove_from {
            for (key, _) in self.scan_raw(family)? {
                if key.len() != 4 {
                    return Err(HeaderChainStoreError::Incoherent(
                        "invalid projection key width",
                    ));
                }
                let height = u32::from_be_bytes(
                    key.as_slice()
                        .try_into()
                        .map_err(|_| HeaderChainStoreError::Incoherent("projection key width"))?,
                );
                if height >= remove_from.0 {
                    self.delete_raw(batch, family, key)?;
                }
            }
        }
        for frontier in &delta.put {
            self.put_raw(
                batch,
                family,
                HeaderHeightKey(frontier.height).as_bytes(),
                frontier.hash.0,
            )?;
        }
        Ok(())
    }

    fn get_value<V: HeaderChainValue>(
        &self,
        family: &'static str,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<V>, HeaderChainStoreError> {
        let cf = self.cf(family)?;
        let value = self.db.raw_get_cf(&cf, key.as_ref())?;
        value
            .map(|value| V::decode(&value).map_err(Into::into))
            .transpose()
    }

    fn scan_raw(
        &self,
        family: &'static str,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, HeaderChainStoreError> {
        let cf = self.cf(family)?;
        Ok(self.db.raw_range_cf(&cf, &[], None)?)
    }

    fn scan_prefix(
        &self,
        family: &'static str,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, HeaderChainStoreError> {
        let cf = self.cf(family)?;
        let upper = prefix_end(prefix);
        Ok(self.db.raw_range_cf(&cf, prefix, upper.as_deref())?)
    }

    fn cf(
        &self,
        family: &'static str,
    ) -> Result<rocksdb::ColumnFamilyRef<'_>, HeaderChainStoreError> {
        self.db
            .cf_handle(family)
            .ok_or(HeaderChainStoreError::Incoherent(
                "missing header-chain column family",
            ))
    }

    fn put_value<V: HeaderChainValue>(
        &self,
        batch: &mut DiskWriteBatch,
        family: &'static str,
        key: impl AsRef<[u8]>,
        value: &V,
    ) -> Result<(), HeaderChainStoreError> {
        self.put_raw(batch, family, key, value.encode()?)
    }

    fn put_empty(
        &self,
        batch: &mut DiskWriteBatch,
        family: &'static str,
        key: impl AsRef<[u8]>,
    ) -> Result<(), HeaderChainStoreError> {
        self.put_raw(batch, family, key, [])
    }

    fn put_raw(
        &self,
        batch: &mut DiskWriteBatch,
        family: &'static str,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<(), HeaderChainStoreError> {
        let cf = self.cf(family)?;
        batch.zs_insert(
            &cf,
            RawBytes::new_raw_bytes(key.as_ref().to_vec()),
            RawBytes::new_raw_bytes(value.as_ref().to_vec()),
        );
        Ok(())
    }

    fn delete_raw(
        &self,
        batch: &mut DiskWriteBatch,
        family: &'static str,
        key: impl AsRef<[u8]>,
    ) -> Result<(), HeaderChainStoreError> {
        let cf = self.cf(family)?;
        batch.zs_delete(&cf, RawBytes::new_raw_bytes(key.as_ref().to_vec()));
        Ok(())
    }
}

impl StoreRead for HeaderChainStore {
    fn snapshot(&self) -> Result<EngineSnapshot, StoreError> {
        Ok(self.metadata()?.snapshot())
    }

    fn metadata(&self) -> Result<EngineMetadata, StoreError> {
        self.metadata_row()
            .map_err(store_error)?
            .ok_or(StoreError::Unavailable("header-chain metadata is absent"))
    }

    fn node(&self, hash: block::Hash) -> Result<Option<HeaderNode>, StoreError> {
        let value = self
            .get_value::<HeaderNodeDisk>(HEADER_NODE_BY_HASH, hash.0)
            .map_err(store_error)?;
        value
            .map(|value| {
                if value.hash != hash {
                    return Err(StoreError::Incoherent("node key/hash mismatch"));
                }
                let reasons = self.direct_reasons(hash).map_err(store_error)?;
                value
                    .into_domain(reasons)
                    .map_err(|_| StoreError::Incoherent("invalid durable node"))
            })
            .transpose()
    }

    fn children(&self, parent: block::Hash) -> Result<Vec<block::Hash>, StoreError> {
        let mut children = Vec::new();
        for (key, value) in self
            .scan_prefix(HEADER_CHILD, &parent.0)
            .map_err(store_error)?
        {
            if key.len() != 64 || !value.is_empty() {
                return Err(StoreError::Incoherent("invalid child-index row"));
            }
            children.push(block::Hash(
                key[32..]
                    .try_into()
                    .map_err(|_| StoreError::Incoherent("invalid child hash"))?,
            ));
        }
        children.sort_unstable_by_key(|hash| hash.0);
        Ok(children)
    }

    fn hashes_at_height(&self, height: block::Height) -> Result<Vec<block::Hash>, StoreError> {
        let mut hashes = Vec::new();
        for (key, value) in self
            .scan_prefix(HEADER_HEIGHT_HASH, &height.0.to_be_bytes())
            .map_err(store_error)?
        {
            if key.len() != 36 || !value.is_empty() {
                return Err(StoreError::Incoherent("invalid height-index row"));
            }
            hashes.push(block::Hash(
                key[4..]
                    .try_into()
                    .map_err(|_| StoreError::Incoherent("invalid height-index hash"))?,
            ));
        }
        hashes.sort_unstable_by_key(|hash| hash.0);
        Ok(hashes)
    }

    fn selected_hash(&self, height: block::Height) -> Result<Option<block::Hash>, StoreError> {
        self.projection_hash(HEADER_SELECTED, height)
    }

    fn verified_hash(&self, height: block::Height) -> Result<Option<block::Hash>, StoreError> {
        self.projection_hash(HEADER_VERIFIED, height)
    }

    fn candidate_tips(&self) -> Result<Vec<(ChainScore, block::Hash)>, StoreError> {
        let mut candidates = Vec::new();
        for (key, value) in self.scan_raw(HEADER_CANDIDATE).map_err(store_error)? {
            if key.len() != 64 || !value.is_empty() {
                return Err(StoreError::Incoherent("invalid candidate-index row"));
            }
            let score = HeaderCandidateKey::from_bytes(&key).0;
            candidates.push((score, score.tip_hash));
        }
        Ok(candidates)
    }

    fn validation_context(&self, parent: block::Hash) -> Result<ValidationLease, StoreError> {
        let metadata = self.metadata()?;
        let parent_node = self
            .node(parent)?
            .ok_or(StoreError::Incoherent("validation parent is not retained"))?;
        let parent_frontier = Frontier::new(parent_node.height, parent);
        let mut predecessors = Vec::new();
        let mut current_hash = parent;
        let mut expected_height = parent_node.height;
        for _ in 0..28 {
            if let Some(node) = self.node(current_hash)? {
                if node.height != expected_height {
                    return Err(StoreError::Incoherent("validation context height mismatch"));
                }
                predecessors.push(zakura_header_chain::HeaderContextFact {
                    frontier: Frontier::new(node.height, node.hash),
                    difficulty_threshold: node.header.difficulty_threshold,
                    time: node.header.time,
                });
                current_hash = node.parent_hash;
            } else {
                let context = self
                    .get_value::<HeaderValidationContextDisk>(
                        HEADER_VALIDATION_CONTEXT,
                        current_hash.0,
                    )
                    .map_err(store_error)?;
                let Some(context) = context else { break };
                if context.header.hash() != current_hash || context.height != expected_height {
                    return Err(StoreError::Incoherent(
                        "invalid immutable validation context",
                    ));
                }
                predecessors.push(context.fact());
                current_hash = context.header.previous_block_hash;
            }
            let Ok(previous_height) = expected_height.previous() else {
                break;
            };
            expected_height = previous_height;
        }
        Ok(ValidationLease::new(
            parent_frontier,
            predecessors,
            metadata.anchor_manifest_digest,
        ))
    }

    fn aux_deliveries(
        &self,
        hash: block::Hash,
    ) -> Result<Vec<zakura_header_chain::AuxDelivery>, StoreError> {
        let mut deliveries = Vec::new();
        for (key, value) in self
            .scan_prefix(HEADER_AUX_DELIVERY, &hash.0)
            .map_err(store_error)?
        {
            if key.len() != 64 {
                return Err(StoreError::Incoherent("invalid auxiliary key width"));
            }
            let delivery = HeaderAuxDeliveryDisk::decode(&value)
                .map_err(|_| StoreError::Incoherent("invalid auxiliary value"))?
                .0;
            if delivery.header_hash != hash || key[32..] != delivery.delivery_id.digest() {
                return Err(StoreError::Incoherent("auxiliary key/value mismatch"));
            }
            deliveries.push(delivery);
        }
        deliveries.sort_unstable_by_key(|delivery| delivery.delivery_id);
        Ok(deliveries)
    }

    fn finality_history(&self) -> Result<Vec<FinalityRecord>, StoreError> {
        let mut records = Vec::new();
        for (key, value) in self
            .scan_raw(HEADER_FINALITY_HISTORY)
            .map_err(store_error)?
        {
            if key.len() != 8 {
                return Err(StoreError::Incoherent("invalid finality key width"));
            }
            let record = HeaderFinalityRecordDisk::decode(&value)
                .map_err(|_| StoreError::Incoherent("invalid finality value"))?
                .0;
            if key != record.epoch.get().to_be_bytes() {
                return Err(StoreError::Incoherent("finality key/value mismatch"));
            }
            records.push(record);
        }
        Ok(records)
    }
}

impl HeaderChainStore {
    fn projection_hash(
        &self,
        family: &'static str,
        height: block::Height,
    ) -> Result<Option<block::Hash>, StoreError> {
        let cf = self.cf(family).map_err(store_error)?;
        let value = self
            .db
            .raw_get_cf(&cf, &HeaderHeightKey(height).as_bytes())
            .map_err(|_| StoreError::Unavailable("projection read failed"))?;
        value
            .map(|value| {
                value
                    .as_slice()
                    .try_into()
                    .map(block::Hash)
                    .map_err(|_| StoreError::Incoherent("invalid projection hash width"))
            })
            .transpose()
    }
}

fn reason_kind(reason: &EligibilityReason) -> EligibilityReasonKind {
    match reason {
        EligibilityReason::SettledUpgradeConflict { .. } => EligibilityReasonKind::SettledUpgrade,
        EligibilityReason::CheckpointConflict { .. } => EligibilityReasonKind::LocalCheckpoint,
        EligibilityReason::FinalityConflict { .. } => EligibilityReasonKind::Finality,
        EligibilityReason::ConsensusBodyInvalid { .. } => EligibilityReasonKind::ConsensusBody,
        EligibilityReason::OperatorInvalid { .. } => EligibilityReasonKind::Operator,
    }
}

fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    for index in (0..end.len()).rev() {
        if end[index] != u8::MAX {
            end[index] = end[index].saturating_add(1);
            end.truncate(index + 1);
            return Some(end);
        }
    }
    None
}

fn reason_evidence(reason: &EligibilityReason) -> EvidenceId {
    if let EligibilityReason::ConsensusBodyInvalid { evidence, .. } = reason {
        return *evidence;
    }
    let mut hasher = Sha256::new();
    hasher.update(b"zakura-header-chain-eligibility-reason-v1");
    hasher.update([reason_tag(reason)]);
    match reason {
        EligibilityReason::SettledUpgradeConflict { height, expected }
        | EligibilityReason::CheckpointConflict { height, expected } => {
            hasher.update(height.0.to_be_bytes());
            hasher.update(expected.0);
        }
        EligibilityReason::FinalityConflict { finalized } => {
            hasher.update(finalized.height.0.to_be_bytes());
            hasher.update(finalized.hash.0);
        }
        EligibilityReason::OperatorInvalid { id } => hasher.update(id.bytes()),
        EligibilityReason::ConsensusBodyInvalid { .. } => unreachable!("returned above"),
    }
    EvidenceId::from_digest(hasher.finalize().into())
}

fn reason_tag(reason: &EligibilityReason) -> u8 {
    match reason {
        EligibilityReason::SettledUpgradeConflict { .. } => 0,
        EligibilityReason::CheckpointConflict { .. } => 1,
        EligibilityReason::FinalityConflict { .. } => 2,
        EligibilityReason::ConsensusBodyInvalid { .. } => 3,
        EligibilityReason::OperatorInvalid { .. } => 4,
    }
}

fn store_error(error: HeaderChainStoreError) -> StoreError {
    match error {
        HeaderChainStoreError::Uninitialized => StoreError::Unavailable("store is uninitialized"),
        _ => StoreError::Incoherent("durable header-chain read failed"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
        service::finalized_state::STATE_COLUMN_FAMILIES_IN_CODE,
        Config,
    };
    use zakura_chain::{
        block::genesis::regtest_genesis_block,
        parameters::{testnet::RegtestParameters, Network},
    };
    use zakura_header_chain::{
        AlarmSet, BodyEvidence, BodyRuleId, BodyValidationState, CheckpointSet, EligibilityReason,
        EngineConfig, EngineMode, FinalityEpoch, FrontierSet, FullStateEvidenceAuthority,
        HeaderChainDiskVersion, HeaderGeneration, HeaderValidationState, StateVersion, SuffixWork,
        SystemClock, TransientBodyFailure, TransientBodyFailureKind, TransitionEvent,
        TrustedAnchor, VerifiedGeneration, WorkCoordinate,
    };

    struct Authority(EvidenceId);

    impl FullStateEvidenceAuthority for Authority {
        fn authorizes(&self, evidence: EvidenceId) -> bool {
            evidence == self.0
        }
    }

    fn open(config: &Config, network: &Network) -> DiskDb {
        DiskDb::new(
            config,
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            network,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        )
        .expect("the header-chain fixture database opens")
    }

    fn fixture() -> (EngineConfig, HeaderNode, EngineMetadata) {
        let network = Network::new_regtest(RegtestParameters::default());
        let block = regtest_genesis_block();
        let frontier = Frontier::new(block::Height(0), block.hash());
        let config = EngineConfig::new(
            EngineMode::Integrated,
            network,
            TrustedAnchor {
                frontier,
                header: block.header.clone(),
            },
            CheckpointSet::default(),
        )
        .expect("the regtest engine configuration is coherent");
        let work = block
            .header
            .difficulty_threshold
            .to_work()
            .expect("the regtest genesis target has exact work");
        let node = HeaderNode::from_durable_parts(
            block.header.clone(),
            frontier.hash,
            block.header.previous_block_hash,
            frontier.height,
            work,
            WorkCoordinate::new(frontier.hash, work.as_u256()),
            HeaderValidationState::Valid,
            zakura_header_chain::EligibilityState::default(),
            BodyValidationState::Unknown,
            Vec::new(),
        )
        .expect("the canonical genesis fields agree");
        let metadata = EngineMetadata {
            disk_format: HeaderChainDiskVersion(1),
            mode: EngineMode::Integrated,
            network_id: config.network.kind(),
            anchor_manifest_digest: config.trust_anchor_digest(),
            work_origin: frontier,
            state_version: StateVersion::new(1),
            header_generation: HeaderGeneration::new(1),
            verified_generation: VerifiedGeneration::new(1),
            finality_epoch: FinalityEpoch::new(0),
            frontiers: FrontierSet {
                finalized: frontier,
                header_best: frontier,
                verified_best: frontier,
            },
            header_best_score: ChainScore::new(SuffixWork::zero(), frontier.hash),
            oldest_retained_height: frontier.height,
            alarms: AlarmSet::default(),
            last_transition_id: EvidenceId::from_digest([0; 32]),
        };
        (config, node, metadata)
    }

    #[test]
    fn serialized_apply_commits_before_receipt_and_reopens_exactly() {
        let cache = tempfile::tempdir().expect("the test cache directory is created");
        let db_config = Config {
            cache_dir: cache.path().to_owned(),
            ephemeral: false,
            debug_skip_non_finalized_state_backup_task: true,
            ..Config::default()
        };
        let (engine_config, anchor, metadata) = fixture();
        let network = engine_config.network.clone();
        let db = open(&db_config, &network);
        let store = HeaderChainStore::new(db.clone());
        let initialized = store
            .initialize(metadata.clone(), anchor.clone())
            .expect("an empty header schema initializes atomically");
        assert_eq!(initialized.durable_tx_id, 1);
        assert_eq!(store.node(anchor.hash), Ok(Some(anchor.clone())));
        assert_eq!(store.selected_hash(anchor.height), Ok(Some(anchor.hash)));
        assert_eq!(store.verified_hash(anchor.height), Ok(Some(anchor.hash)));
        assert_eq!(
            store.candidate_tips(),
            Ok(vec![(metadata.header_best_score, anchor.hash)])
        );

        let evidence = EvidenceId::from_digest([7; 32]);
        let authority = Authority(evidence);
        let context = TransitionContext {
            config: &engine_config,
            clock: &SystemClock,
            full_state_authority: Some(&authority),
            startup_capability: None,
        };
        let request = TransitionRequest {
            expected_version: StateVersion::new(1),
            event: TransitionEvent::BodyEvidence(BodyEvidence::Transient(TransientBodyFailure {
                hash: anchor.hash,
                evidence,
                kind: TransientBodyFailureKind::Storage,
            })),
        };
        let receipt = store
            .apply(request.clone(), &context)
            .expect("the transition commits");
        let ApplyResult::Committed(receipt) = receipt else {
            panic!("a new body evidence ID must commit");
        };
        assert_eq!(receipt.previous.state_version, StateVersion::new(1));
        assert_eq!(receipt.current.state_version, StateVersion::new(2));
        assert_eq!(receipt.durable_tx_id, 2);
        assert!(matches!(
            store.node(anchor.hash).expect("the node row decodes").expect("the anchor remains").body,
            BodyValidationState::Unavailable(summary) if summary.attempts == 1
        ));
        assert!(matches!(
            store.apply(request, &context).expect("idempotent replay succeeds"),
            ApplyResult::NoChange(receipt) if receipt.state_version == StateVersion::new(2)
        ));
        assert!(matches!(
            store
                .apply(
                    TransitionRequest {
                        expected_version: StateVersion::new(1),
                        event: TransitionEvent::ReevaluateDeferred,
                    },
                    &context,
                )
                .expect("a stale CAS is a typed zero-effect result"),
            ApplyResult::Stale(receipt) if receipt.current_version == StateVersion::new(2)
        ));

        drop(store);
        drop(db);
        let reopened = HeaderChainStore::new(open(&db_config, &network));
        assert_eq!(
            reopened.snapshot().expect("committed metadata reopens"),
            receipt.current
        );
        assert!(matches!(
            reopened
                .node(anchor.hash)
                .expect("the reopened node row decodes")
                .expect("the reopened anchor exists")
                .body,
            BodyValidationState::Unavailable(summary) if summary.attempts == 1
        ));
    }

    #[test]
    fn failed_batch_encoding_has_zero_durable_effects() {
        let cache = tempfile::tempdir().expect("the test cache directory is created");
        let db_config = Config {
            cache_dir: cache.path().to_owned(),
            ephemeral: true,
            debug_skip_non_finalized_state_backup_task: true,
            ..Config::default()
        };
        let (engine_config, mut anchor, metadata) = fixture();
        let store = HeaderChainStore::new(open(&db_config, &engine_config.network));
        store
            .initialize(metadata.clone(), anchor.clone())
            .expect("the empty schema initializes");

        let evidence = EvidenceId::from_digest([9; 32]);
        let rule = BodyRuleId::new("x".repeat(129));
        anchor.body = BodyValidationState::ConsensusInvalid {
            evidence,
            rule: rule.clone(),
        };
        anchor
            .eligibility
            .direct_reasons
            .insert(EligibilityReason::ConsensusBodyInvalid { evidence, rule });
        let mut next_metadata = metadata.clone();
        next_metadata.state_version = StateVersion::new(2);
        let changes = ChangeSet {
            put_nodes: vec![anchor],
            delete_nodes: Vec::new(),
            index_changes: zakura_header_chain::IndexChanges::default(),
            candidate_tips: vec![(
                metadata.header_best_score,
                metadata.frontiers.header_best.hash,
            )],
            selected_projection: zakura_header_chain::ProjectionDelta::default(),
            verified_projection: zakura_header_chain::ProjectionDelta::default(),
            eligibility_changes: Vec::new(),
            aux_changes: Vec::new(),
            finality_append: None,
            metadata: next_metadata,
        };

        assert!(matches!(
            store.batch_for(&changes),
            Err(HeaderChainStoreError::Codec(
                HeaderChainValueError::Oversized {
                    field: "body_rule",
                    length: 129
                }
            ))
        ));
        assert_eq!(
            store
                .metadata()
                .expect("the original metadata remains readable")
                .state_version,
            StateVersion::new(1)
        );
    }
}
