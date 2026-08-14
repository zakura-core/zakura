//! Coherent, bounded RocksDB views used by startup recovery audits.

use super::*;

/// One RocksDB snapshot retained for a complete header-chain startup audit.
pub struct HeaderChainAuditSnapshot<'a> {
    store: &'a HeaderChainStore,
    snapshot: rocksdb::SnapshotWithThreadMode<'a, rocksdb::DB>,
}

impl HeaderChainAuditSnapshot<'_> {
    fn get_value<V: FallibleDiskValue<Error = HeaderChainValueError>>(
        &self,
        family: &'static str,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<V>, StoreError> {
        let cf = self.store.cf(family).map_err(store_error)?;
        self.snapshot
            .get_cf(&cf, key.as_ref())
            .map_err(|_| StoreError::Unavailable("header-chain snapshot read failed"))?
            .map(|value| {
                V::decode(&value).map_err(|_| StoreError::Incoherent("invalid durable value"))
            })
            .transpose()
    }

    fn visit_raw(
        &self,
        collection: StoreCollection,
        family: &'static str,
        limit: RowLimit,
        visitor: &mut dyn FnMut(&[u8], &[u8]) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        let cf = self.store.cf(family).map_err(store_error)?;
        for (index, row) in self
            .snapshot
            .iterator_cf(&cf, rocksdb::IteratorMode::Start)
            .enumerate()
        {
            if index == limit.get() {
                return Err(StoreError::LimitExceeded { collection, limit });
            }
            let (key, value) =
                row.map_err(|_| StoreError::Unavailable("header-chain snapshot iterator failed"))?;
            visitor(&key, &value)?;
        }
        Ok(())
    }

    fn visit_projection(
        &self,
        collection: StoreCollection,
        family: &'static str,
        limit: RowLimit,
        visitor: &mut dyn FnMut(Frontier) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        self.visit_raw(collection, family, limit, &mut |key, value| {
            if key.len() != 4 || value.len() != 32 {
                return Err(StoreError::Incoherent("invalid projection row width"));
            }
            let height = HeaderHeightKey::from_bytes(key).0;
            let hash = block::Hash(
                value
                    .try_into()
                    .map_err(|_| StoreError::Incoherent("invalid projection hash"))?,
            );
            visitor(Frontier::new(height, hash))
        })
    }
}

impl StoreAuditRead for HeaderChainStore {
    type Snapshot<'a> = HeaderChainAuditSnapshot<'a>;

    fn audit_snapshot(&self) -> Result<Self::Snapshot<'_>, StoreError> {
        Ok(HeaderChainAuditSnapshot {
            store: self,
            snapshot: self.db.rocksdb_snapshot(),
        })
    }
}

impl StoreAuditSnapshot for HeaderChainAuditSnapshot<'_> {
    fn snapshot(&self) -> Result<EngineSnapshot, StoreError> {
        Ok(self.metadata()?.snapshot())
    }

    fn metadata(&self) -> Result<EngineMetadata, StoreError> {
        self.get_value(HEADER_ENGINE_META, METADATA_KEY)?
            .ok_or(StoreError::Unavailable("header-chain metadata is absent"))
    }

    fn visit_header_nodes(
        &self,
        limit: RowLimit,
        visitor: &mut dyn FnMut(HeaderNode) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        let mut reasons_by_hash: HashMap<block::Hash, Vec<EligibilityReason>> = HashMap::new();
        let reason_limit = limit
            .get()
            .checked_mul(zakura_header_chain::MAX_DIRECT_ELIGIBILITY_REASONS_V1)
            .ok_or(StoreError::Incoherent(
                "eligibility-reason recovery limit overflow",
            ))?;
        self.visit_eligibility_roots(RowLimit::new(reason_limit), &mut |(hash, reason)| {
            reasons_by_hash.entry(hash).or_default().push(reason);
            Ok(())
        })?;
        self.visit_raw(
            StoreCollection::HeaderNodes,
            HEADER_NODE_BY_HASH,
            limit,
            &mut |key, value| {
                if key.len() != 32 {
                    return Err(StoreError::Incoherent("invalid node key width"));
                }
                let hash = block::Hash(
                    key.try_into()
                        .map_err(|_| StoreError::Incoherent("invalid node hash key"))?,
                );
                let disk = HeaderNodeDisk::decode(value)
                    .map_err(|_| StoreError::Incoherent("invalid durable node value"))?;
                if disk.hash != hash {
                    return Err(StoreError::Incoherent("node key/hash mismatch"));
                }
                let node = disk
                    .into_domain(reasons_by_hash.remove(&hash).unwrap_or_default())
                    .map_err(|_| StoreError::Incoherent("invalid durable node"))?;
                visitor(node)
            },
        )?;
        if !reasons_by_hash.is_empty() {
            return Err(StoreError::Incoherent("eligibility root has no node"));
        }
        Ok(())
    }

    fn visit_consensus_invalid_body_tombstones(
        &self,
        limit: RowLimit,
        visitor: &mut dyn FnMut(
            zakura_header_chain::ConsensusInvalidBodyTombstone,
        ) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        self.visit_raw(
            StoreCollection::ConsensusInvalidBodyTombstones,
            HEADER_CONSENSUS_INVALID_BODY_TOMBSTONE,
            limit,
            &mut |key, value| {
                if key.len() != 32 {
                    return Err(StoreError::Incoherent("invalid tombstone key width"));
                }
                let tombstone = zakura_header_chain::ConsensusInvalidBodyTombstone::decode(value)
                    .map_err(|_| StoreError::Incoherent("invalid tombstone value"))?;
                if key != tombstone.hash.0 {
                    return Err(StoreError::Incoherent("tombstone key/hash mismatch"));
                }
                visitor(tombstone)
            },
        )
    }

    fn consensus_invalid_body_tombstone_count(&self) -> Result<usize, StoreError> {
        let count = self
            .get_value::<HeaderRowCountDisk>(HEADER_ENGINE_META, TOMBSTONE_COUNT_KEY)?
            .ok_or(StoreError::Incoherent(
                "consensus-invalid tombstone count is absent",
            ))?;
        usize::try_from(count.0)
            .map_err(|_| StoreError::Incoherent("tombstone count does not fit usize"))
    }

    fn full_state_attests_to_body_validation_state(
        &self,
        header_hash: block::Hash,
        body_validation_state: &zakura_header_chain::BodyValidationState,
    ) -> Result<bool, StoreError> {
        let authority = self.get_value::<FullStateBodyValidationEvidenceAuthorityDisk>(
            HEADER_BODY_EVIDENCE_AUTHORITY,
            header_hash.0,
        )?;
        Ok(authority.is_some_and(|authority| {
            authority.attests_to_body_validation_state(header_hash, body_validation_state)
        }))
    }

    fn visit_header_child_edges(
        &self,
        limit: RowLimit,
        visitor: &mut dyn FnMut((block::Hash, block::Hash)) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        self.visit_raw(
            StoreCollection::HeaderChildEdges,
            HEADER_CHILD,
            limit,
            &mut |key, value| {
                if key.len() != 64 || !value.is_empty() {
                    return Err(StoreError::Incoherent("invalid child-index row"));
                }
                let key = HeaderChildKey::from_bytes(key);
                visitor((key.parent, key.child))
            },
        )
    }

    fn visit_selected_projection(
        &self,
        limit: RowLimit,
        visitor: &mut dyn FnMut(Frontier) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        self.visit_projection(
            StoreCollection::SelectedProjection,
            HEADER_SELECTED,
            limit,
            visitor,
        )
    }

    fn visit_verified_projection(
        &self,
        limit: RowLimit,
        visitor: &mut dyn FnMut(Frontier) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        self.visit_projection(
            StoreCollection::VerifiedProjection,
            HEADER_VERIFIED,
            limit,
            visitor,
        )
    }

    fn visit_deferred_entries(
        &self,
        limit: RowLimit,
        visitor: &mut dyn FnMut((chrono::DateTime<Utc>, block::Hash)) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        self.visit_raw(
            StoreCollection::DeferredHeaderEntries,
            HEADER_DEFERRED,
            limit,
            &mut |key, value| {
                if key.len() != 44 || !value.is_empty() {
                    return Err(StoreError::Incoherent("invalid deferred-index row"));
                }
                let key = HeaderDeferredKey::try_from_bytes(key)
                    .map_err(|_| StoreError::Incoherent("invalid deferred-index key"))?;
                let until = Utc
                    .timestamp_opt(key.seconds, key.nanoseconds)
                    .single()
                    .ok_or(StoreError::Incoherent("invalid deferred-index timestamp"))?;
                visitor((until, key.hash))
            },
        )
    }

    fn visit_eligibility_roots(
        &self,
        limit: RowLimit,
        visitor: &mut dyn FnMut((block::Hash, EligibilityReason)) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        self.visit_raw(
            StoreCollection::EligibilityReasonRoots,
            HEADER_ELIGIBILITY_ROOT,
            limit,
            &mut |key, value| {
                let key = HeaderEligibilityRootKey::try_from_bytes(key)
                    .map_err(|_| StoreError::Incoherent("invalid eligibility-root key"))?;
                let reason = HeaderEligibilityReasonDisk::decode(value)
                    .map_err(|_| StoreError::Incoherent("invalid eligibility-root value"))?
                    .into_domain();
                if reason_kind(&reason) != key.kind || reason_evidence(&reason) != key.evidence {
                    return Err(StoreError::Incoherent(
                        "eligibility-root key/value mismatch",
                    ));
                }
                visitor((key.root, reason))
            },
        )
    }

    fn visit_aux_deliveries(
        &self,
        limit: RowLimit,
        visitor: &mut dyn FnMut(UntrustedAuxDeliveryRow) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        self.visit_raw(
            StoreCollection::AuxiliaryDeliveries,
            HEADER_AUX_DELIVERY,
            limit,
            &mut |key, value| {
                if key.len() != 64 {
                    return Err(StoreError::Incoherent("invalid auxiliary key width"));
                }
                let key = HeaderAuxDeliveryKey::from_bytes(key);
                let delivery = decode_untrusted_aux_delivery(value)
                    .map_err(|_| StoreError::Incoherent("invalid auxiliary value"))?;
                if delivery.delivery().header_hash != key.header
                    || delivery.delivery().delivery_id != key.delivery
                {
                    return Err(StoreError::Incoherent("auxiliary key/value mismatch"));
                }
                visitor(delivery)
            },
        )
    }

    fn visit_validation_context_records(
        &self,
        limit: RowLimit,
        visitor: &mut dyn FnMut(ValidationContextRecord) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        self.visit_raw(
            StoreCollection::ValidationContexts,
            HEADER_VALIDATION_CONTEXT,
            limit,
            &mut |key, value| {
                if key.len() != 32 {
                    return Err(StoreError::Incoherent(
                        "invalid validation-context key width",
                    ));
                }
                let hash = block::Hash(
                    key.try_into()
                        .map_err(|_| StoreError::Incoherent("invalid validation-context key"))?,
                );
                let record = HeaderValidationContextDisk::decode(value)
                    .map_err(|_| StoreError::Incoherent("invalid validation-context value"))?;
                if record.header.hash() != hash {
                    return Err(StoreError::Incoherent(
                        "validation-context key/hash mismatch",
                    ));
                }
                visitor(ValidationContextRecord {
                    header: record.header,
                    height: record.height,
                })
            },
        )
    }

    fn authenticated_canonical_hash(
        &self,
        height: block::Height,
    ) -> Result<Option<block::Hash>, StoreError> {
        let read_hash = |family| -> Result<Option<block::Hash>, StoreError> {
            let cf = self.store.cf(family).map_err(store_error)?;
            self.snapshot
                .get_cf(&cf, height.as_bytes())
                .map_err(|_| StoreError::Unavailable("canonical snapshot read failed"))
                .map(|value| value.map(block::Hash::from_bytes))
        };
        let hash = read_hash("hash_by_height")?;
        if hash.is_some() {
            return Ok(hash);
        }
        let hash = read_hash("zakura_header_hash_by_height")?;
        #[cfg(test)]
        if hash.is_none() {
            let mut found = None;
            self.visit_header_nodes(
                RowLimit::new(zakura_header_chain::MAX_NON_FINALIZED_NODES_V1 + 1),
                &mut |node| {
                    if node.height == height {
                        found = Some(node.hash);
                    }
                    Ok(())
                },
            )?;
            return Ok(found);
        }
        Ok(hash)
    }

    fn visit_finality_history(
        &self,
        limit: RowLimit,
        visitor: &mut dyn FnMut(FinalityRecord) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        self.visit_raw(
            StoreCollection::FinalityHistory,
            HEADER_FINALITY_HISTORY,
            limit,
            &mut |key, value| {
                if key.len() != 8 {
                    return Err(StoreError::Incoherent("invalid finality key width"));
                }
                let record = FinalityRecord::decode(value)
                    .map_err(|_| StoreError::Incoherent("invalid finality value"))?;
                if key != record.epoch.get().to_be_bytes() {
                    return Err(StoreError::Incoherent("finality key/value mismatch"));
                }
                visitor(record)
            },
        )
    }

    fn finality_history_checkpoint(&self) -> Result<Option<FinalityHistoryCheckpoint>, StoreError> {
        self.get_value(HEADER_ENGINE_META, FINALITY_HISTORY_CHECKPOINT_KEY)
    }

    fn finality_history_count(&self) -> Result<usize, StoreError> {
        let count = self
            .get_value::<HeaderRowCountDisk>(HEADER_ENGINE_META, FINALITY_HISTORY_COUNT_KEY)?
            .ok_or(StoreError::Incoherent("finality history count is absent"))?;
        usize::try_from(count.0)
            .map_err(|_| StoreError::Incoherent("finality history count does not fit usize"))
    }
}
