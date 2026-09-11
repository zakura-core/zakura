//! Short-lived, coherent page reads that can finish while the chain advances.

use super::*;

pub(super) struct HeaderPathReadSnapshot<'a> {
    disk: audit_snapshot::HeaderChainAuditSnapshot<'a>,
    auxiliary: HashMap<block::Hash, Vec<AuxDelivery>>,
}

impl HeaderChainReader {
    pub(super) fn capture_path_read(
        &self,
        cursor: &CanonicalHeaderPathCursor,
        max_count: u32,
    ) -> Result<HeaderPathReadSnapshot<'_>, HeaderChainStoreError> {
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        let engine = self
            .transition_engine
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        let start = match cursor.position {
            CanonicalHeaderPathPosition::Retained { next } => next,
            CanonicalHeaderPathPosition::Finalized { .. } => 0,
            CanonicalHeaderPathPosition::Complete => cursor.retained_path.len(),
        };
        let count = usize::try_from(max_count).unwrap_or(usize::MAX);
        let auxiliary = cursor.retained_path[start..]
            .iter()
            .take(count)
            .map(|hash| (*hash, engine.aux_deliveries(*hash).to_vec()))
            .collect();
        // Capture authoritative outcomes and durable rows under the same commit barrier.
        // The snapshot lives only for this bounded page, never for an idle peer's lease.
        Ok(HeaderPathReadSnapshot {
            disk: self.store.audit_snapshot()?,
            auxiliary,
        })
    }
}

impl HeaderPathReadSnapshot<'_> {
    pub(super) fn read_page(
        &self,
        cursor: &CanonicalHeaderPathCursor,
        max_count: u32,
    ) -> Result<
        Option<(RetainedPathPage, CanonicalHeaderPathPosition, Frontier)>,
        HeaderChainStoreError,
    > {
        let count = usize::try_from(max_count).unwrap_or(usize::MAX);
        let mut headers = Vec::with_capacity(count);
        let mut aux_deliveries = Vec::with_capacity(count);
        let mut previous = cursor.last_frontier;
        let mut position = cursor.position;
        while headers.len() < count {
            let (frontier, header, deliveries) = match position {
                CanonicalHeaderPathPosition::Complete => break,
                CanonicalHeaderPathPosition::Finalized { next, end } => {
                    if next > end || previous.height.next().ok() != Some(next) {
                        return Err(StoreError::Incoherent(
                            "finalized header cursor has a non-contiguous height",
                        )
                        .into());
                    }
                    let hash = self
                        .disk
                        .finalized_hash(next)?
                        .ok_or(StoreError::Incoherent(
                            "finalized header cursor has a missing hash",
                        ))?;
                    let frontier = Frontier::new(next, hash);
                    let header = self.disk.finalized_header(frontier)?;
                    position = if next == end {
                        if cursor.retained_path.is_empty() {
                            CanonicalHeaderPathPosition::Complete
                        } else {
                            CanonicalHeaderPathPosition::Retained { next: 0 }
                        }
                    } else {
                        CanonicalHeaderPathPosition::Finalized {
                            next: next.next().map_err(|_| {
                                StoreError::Incoherent("finalized header cursor height overflowed")
                            })?,
                            end,
                        }
                    };
                    (frontier, header, Vec::new())
                }
                CanonicalHeaderPathPosition::Retained { next } => {
                    let hash = *cursor
                        .retained_path
                        .get(next)
                        .ok_or(StoreError::Incoherent(
                            "retained header cursor exceeded its immutable suffix",
                        ))?;
                    let item = if let Some(node) = self.disk.retained_path_node(hash)? {
                        let deliveries =
                            self.auxiliary.get(&hash).ok_or(StoreError::Incoherent(
                                "retained header cursor has no captured auxiliary state",
                            ))?;
                        let durable = self.disk.untrusted_aux_deliveries(hash)?;
                        if !auxiliary_rows_are_coherent(
                            &node.aux_delivery_ids,
                            deliveries,
                            &durable,
                        ) {
                            return Err(StoreError::Incoherent(
                                "retained node and auxiliary delivery index disagree",
                            )
                            .into());
                        }
                        (
                            Frontier::new(node.height, hash),
                            node.header,
                            deliveries.clone(),
                        )
                    } else if let Some(frontier) = self.disk.finalized_frontier(hash)? {
                        // Finalization can move a leased header out of the retained graph.
                        (frontier, self.disk.finalized_header(frontier)?, Vec::new())
                    } else {
                        // Full-state fork replacement may retire a leased losing branch.
                        return Ok(None);
                    };
                    position = if next.saturating_add(1) == cursor.retained_path.len() {
                        CanonicalHeaderPathPosition::Complete
                    } else {
                        CanonicalHeaderPathPosition::Retained {
                            next: next.saturating_add(1),
                        }
                    };
                    item
                }
            };
            if previous.height.next().ok() != Some(frontier.height)
                || header.previous_block_hash != previous.hash
            {
                return Err(
                    StoreError::Incoherent("header cursor has a non-contiguous item").into(),
                );
            }
            previous = frontier;
            headers.push(header);
            aux_deliveries.push(deliveries);
        }
        let complete = matches!(position, CanonicalHeaderPathPosition::Complete);
        if complete && previous != cursor.target {
            return Err(
                StoreError::Incoherent("header cursor completed before its exact target").into(),
            );
        }
        Ok(Some((
            RetainedPathPage {
                lease_id: cursor.lease_id,
                common_ancestor: cursor.last_frontier,
                target: cursor.target,
                scope: cursor.scope,
                headers,
                aux_deliveries,
                complete,
            },
            position,
            previous,
        )))
    }
}
