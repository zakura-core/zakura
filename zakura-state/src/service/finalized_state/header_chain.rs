//! Durable adapter for the fork-aware header-chain transition engine.

#![allow(dead_code)] // Constructed by the full-state migration and service wiring in PR-9.

use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::{TimeZone, Utc};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{sync::watch, time::Instant};
use zakura_chain::{block, parallel::commitment_aux::BlockCommitmentRoots};
use zakura_header_chain::{
    apply_transition, audit_store, ApplyResult, AuxDelivery, AuxDelta, ChainScore, ChangeSet,
    CommittedTransition, CounterExhausted, EligibilityReason, EngineConfig, EngineMetadata,
    EngineMode, EngineSnapshot, EvidenceId, FinalityRecord, FinalitySource, Frontier,
    FullStateEvidenceAuthority, FullStateFinalized, HeaderLocator, HeaderNode, NoChangeReceipt,
    RecoveryFailure, RecoveryPlan, RecoveryRepair, SourceId, StaleReceipt, StoreAuditRead,
    StoreError, StoreRead, SystemClock, TransitionContext, TransitionEvent, TransitionFailure,
    TransitionRequest, ValidationContextRecord, ValidationLease, VerifiedChainChanged,
    VerifiedChangeCause, VerifiedHeaderRef, WorkOwner, WorkScope,
};

use crate::{
    RetainedPathLease, RetainedPathLeaseOutcome, RetainedPathPage, RetainedPathReadOutcome,
    MAX_RETAINED_PATH_LEASES,
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
const RETAINED_PATH_LEASE_IDLE: Duration = Duration::from_secs(30);

#[cfg(any(test, feature = "header-fuzz"))]
mod fuzz;
pub(in crate::service) mod migration;
#[cfg(any(test, feature = "header-fuzz"))]
pub use fuzz::{replay_recovery_rows_bytes, RecoveryRowsReplaySummary};

pub(crate) fn select_vct_aux_delivery(deliveries: Vec<AuxDelivery>) -> Option<AuxDelivery> {
    deliveries
        .into_iter()
        .filter(|delivery| {
            delivery.tree_aux.is_some()
                && !matches!(
                    delivery.authentication,
                    zakura_header_chain::AuxAuthentication::Rejected { .. }
                )
        })
        .min_by_key(|delivery| {
            (
                !matches!(
                    delivery.authentication,
                    zakura_header_chain::AuxAuthentication::Authenticated { .. }
                ),
                delivery.delivery_id,
            )
        })
}

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
    /// A runtime durable read failed before transition planning.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// RocksDB rejected the one atomic write batch.
    #[error("header-chain atomic write failed: {0}")]
    RocksDb(#[from] rocksdb::Error),
    /// The serialized writer lock was poisoned by a prior panic.
    #[error("header-chain serialized writer lock is poisoned")]
    WriterPoisoned,
    /// A staged full-state value disagreed with the header plan derived from the same evidence.
    #[error("staged full-state verified frontier {expected:?} differs from projected header frontier {actual:?}")]
    VerifiedFrontierMismatch {
        /// Exact staged full-state winner.
        expected: Frontier,
        /// Header transition result derived before any write.
        actual: Frontier,
    },
    /// Exhaustive startup audit or deterministic reconstruction failed.
    #[error(transparent)]
    Recovery(#[from] RecoveryFailure),
    /// A monotonic durable counter was exhausted during an explicit store migration.
    #[error(transparent)]
    Counter(#[from] CounterExhausted),
    /// An imported headers-only trust pin was refuted; this store must be destroyed and resynced.
    #[error(
        "header_chain_migrated_pin_refuted at {pin:?}; delete the migrated header store and resync"
    )]
    MigratedPinRefuted {
        /// Exact preserved pin contradicted by deterministic body validation.
        pin: Frontier,
    },
    /// A test crash was injected at a named durable/publication boundary.
    #[cfg(test)]
    #[error("injected header-chain crash at {0:?}")]
    InjectedCrash(FaultPoint),
}

/// One successful startup audit and optional atomic repair.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartupReport {
    /// Snapshot read before any reconstructible repair.
    pub previous: EngineSnapshot,
    /// Audited snapshot that is safe to publish.
    pub current: EngineSnapshot,
    /// Exact reconstructible categories repaired in one batch.
    pub repairs: BTreeSet<RecoveryRepair>,
    /// Publication is true only for a successful, fully audited startup.
    pub publication_allowed: bool,
}

/// The sole latest-value publisher for durable header-chain snapshots.
#[derive(Clone, Debug)]
pub struct Publisher {
    sender: watch::Sender<EngineSnapshot>,
    mirrors: Arc<Mutex<Vec<watch::Sender<Option<EngineSnapshot>>>>>,
}

impl Publisher {
    fn new(snapshot: EngineSnapshot) -> Self {
        record_published_snapshot(&snapshot);
        let (sender, _) = watch::channel(snapshot);
        Self {
            sender,
            mirrors: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Return the latest durable snapshot.
    pub fn snapshot(&self) -> EngineSnapshot {
        self.sender.borrow().clone()
    }

    /// Subscribe to the latest durable snapshot without replay dependence.
    pub fn subscribe(&self) -> watch::Receiver<EngineSnapshot> {
        self.sender.subscribe()
    }

    /// Mirror committed snapshots into a channel that can predate runtime attachment.
    pub(crate) fn mirror_to(&self, sender: watch::Sender<Option<EngineSnapshot>>) {
        sender.send_replace(Some(self.snapshot()));
        self.mirrors
            .lock()
            .expect("header-chain publisher mirror mutex is never poisoned")
            .push(sender);
    }

    fn publish(&self, snapshot: EngineSnapshot) {
        record_published_snapshot(&snapshot);
        self.sender.send_replace(snapshot.clone());
        self.mirrors
            .lock()
            .expect("header-chain publisher mirror mutex is never poisoned")
            .retain(|mirror| {
                if mirror.receiver_count() == 0 {
                    false
                } else {
                    mirror.send_replace(Some(snapshot.clone()));
                    true
                }
            });
    }
}

fn record_published_snapshot(snapshot: &EngineSnapshot) {
    metrics::gauge!("sync.header_chain.frontier.finalized_height")
        .set(f64::from(snapshot.frontiers.finalized.height.0));
    metrics::gauge!("sync.header_chain.frontier.header_best_height")
        .set(f64::from(snapshot.frontiers.header_best.height.0));
    metrics::gauge!("sync.header_chain.frontier.verified_best_height")
        .set(f64::from(snapshot.frontiers.verified_best.height.0));
    metrics::gauge!("sync.header_chain.frontier.divergence").set(f64::from(
        snapshot
            .frontiers
            .header_best
            .height
            .0
            .saturating_sub(snapshot.frontiers.verified_best.height.0),
    ));
    // Metric gauges are approximate floating-point telemetry; the durable counters remain exact.
    metrics::gauge!("sync.header_chain.generation.header")
        .set(snapshot.header_generation.get() as f64);
    // Metric gauges are approximate floating-point telemetry; the durable counters remain exact.
    metrics::gauge!("sync.header_chain.generation.verified")
        .set(snapshot.verified_generation.get() as f64);
    metrics::gauge!("sync.header_chain.alarm.resource_stalled").set(
        if snapshot.alarms.resource_stalled {
            1.0
        } else {
            0.0
        },
    );
    metrics::gauge!("sync.header_chain.alarm.migrated_pin_refuted").set(
        if snapshot.alarms.migrated_pin_refuted.is_some() {
            1.0
        } else {
            0.0
        },
    );

    tracing::info!(
        mode = ?snapshot.mode,
        state_version = snapshot.state_version.get(),
        header_generation = snapshot.header_generation.get(),
        verified_generation = snapshot.verified_generation.get(),
        finalized_height = snapshot.frontiers.finalized.height.0,
        finalized_hash = ?snapshot.frontiers.finalized.hash,
        header_best_height = snapshot.frontiers.header_best.height.0,
        header_best_hash = ?snapshot.frontiers.header_best.hash,
        verified_best_height = snapshot.frontiers.verified_best.height.0,
        verified_best_hash = ?snapshot.frontiers.verified_best.hash,
        resource_stalled = snapshot.alarms.resource_stalled,
        body_unavailable = snapshot
            .alarms
            .header_best_body_unavailable
            .is_some_and(|alarm| alarm.alarmed),
        migrated_pin_refuted = ?snapshot.alarms.migrated_pin_refuted,
        "published committed Zakura header-chain snapshot"
    );
}

/// An audited durable store paired with its only production publisher.
#[derive(Clone, Debug)]
pub struct HeaderChainRuntime {
    store: HeaderChainStore,
    publisher: Publisher,
    leases: Arc<Mutex<RetainedPathLeaseRegistry>>,
}

/// Read-only coherent queries serialized against durable header transitions.
#[derive(Clone, Debug)]
pub(crate) struct HeaderChainReader {
    store: HeaderChainStore,
    leases: Arc<Mutex<RetainedPathLeaseRegistry>>,
}

/// One atomically read selected-path window with exact auxiliary provenance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SelectedAuxWindow {
    pub(crate) snapshot: EngineSnapshot,
    pub(crate) current: HeaderNode,
    pub(crate) current_deliveries: Vec<AuxDelivery>,
    pub(crate) successor: Option<(HeaderNode, Vec<AuxDelivery>)>,
}

#[derive(Debug, Default)]
struct RetainedPathLeaseRegistry {
    next_lease_id: u64,
    by_peer: HashMap<SourceId, RetainedPathLease>,
}

impl RetainedPathLeaseRegistry {
    fn expire(&mut self, now: Instant) {
        self.by_peer.retain(|_, lease| lease.idle_deadline > now);
    }

    fn insert(
        &mut self,
        peer: SourceId,
        session_id: u64,
        frontiers: (Frontier, Frontier),
        path: Arc<[block::Hash]>,
        generation: zakura_header_chain::HeaderGeneration,
        now: Instant,
    ) -> RetainedPathLeaseOutcome {
        self.expire(now);
        if self
            .by_peer
            .get(&peer)
            .is_some_and(|lease| lease.session_id == session_id)
        {
            return RetainedPathLeaseOutcome::Busy;
        }
        self.by_peer.remove(&peer);
        if self.by_peer.len() >= MAX_RETAINED_PATH_LEASES {
            return RetainedPathLeaseOutcome::Busy;
        }
        let Some(lease_id) = self.next_lease_id.checked_add(1) else {
            return RetainedPathLeaseOutcome::Busy;
        };
        self.next_lease_id = lease_id;
        let lease = RetainedPathLease {
            lease_id,
            peer,
            session_id,
            target: frontiers.0,
            common_ancestor: frontiers.1,
            path,
            generation,
            idle_deadline: now + RETAINED_PATH_LEASE_IDLE,
        };
        self.by_peer.insert(peer, lease.clone());
        RetainedPathLeaseOutcome::Acquired(lease)
    }

    fn get(
        &mut self,
        peer: SourceId,
        session_id: u64,
        lease_id: u64,
        now: Instant,
    ) -> Option<RetainedPathLease> {
        self.expire(now);
        let lease = self.by_peer.get(&peer)?;
        if lease.session_id != session_id || lease.lease_id != lease_id {
            return None;
        }
        Some(lease.clone())
    }

    fn renew(&mut self, peer: SourceId, session_id: u64, lease_id: u64, now: Instant) -> bool {
        let Some(lease) = self.by_peer.get_mut(&peer) else {
            return false;
        };
        if lease.session_id != session_id || lease.lease_id != lease_id {
            return false;
        }
        lease.idle_deadline = now + RETAINED_PATH_LEASE_IDLE;
        true
    }

    fn release(&mut self, peer: SourceId, session_id: u64, lease_id: u64) -> bool {
        let matches = self
            .by_peer
            .get(&peer)
            .is_some_and(|lease| lease.session_id == session_id && lease.lease_id == lease_id);
        if matches {
            self.by_peer.remove(&peer);
        }
        matches
    }

    fn active_references(&mut self, now: Instant) -> Vec<block::Hash> {
        self.expire(now);
        self.by_peer
            .values()
            .flat_map(|lease| {
                std::iter::once(lease.common_ancestor.hash).chain(lease.path.iter().copied())
            })
            .collect()
    }
}

impl HeaderChainReader {
    fn coherent_aux_deliveries(
        &self,
        node: &HeaderNode,
    ) -> Result<Vec<AuxDelivery>, HeaderChainStoreError> {
        let deliveries = self.store.aux_deliveries(node.hash)?;
        let indexed: BTreeSet<_> = node.aux_delivery_ids.iter().copied().collect();
        let stored: BTreeSet<_> = deliveries
            .iter()
            .map(|delivery| delivery.delivery_id)
            .collect();
        if indexed.len() != node.aux_delivery_ids.len()
            || stored.len() != deliveries.len()
            || indexed != stored
        {
            return Err(HeaderChainStoreError::Store(StoreError::Incoherent(
                "retained node and auxiliary delivery index disagree",
            )));
        }
        Ok(deliveries)
    }

    fn selected_aux_delivery(
        &self,
        node: &HeaderNode,
    ) -> Result<Option<AuxDelivery>, HeaderChainStoreError> {
        Ok(select_vct_aux_delivery(self.coherent_aux_deliveries(node)?))
    }

    /// Return the contiguous selected-path auxiliary roots starting at `start`.
    pub(crate) fn selected_block_roots(
        &self,
        start: block::Height,
        count: u32,
    ) -> Result<Vec<BlockCommitmentRoots>, HeaderChainStoreError> {
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        let mut roots = Vec::new();
        for offset in 0..count {
            let Some(height) = start + i64::from(offset) else {
                break;
            };
            let Some(hash) = self.store.selected_hash(height)? else {
                break;
            };
            let Some(node) = self.store.node(hash)? else {
                return Err(StoreError::Incoherent(
                    "selected auxiliary root header is not retained",
                )
                .into());
            };
            if node.height != height {
                return Err(StoreError::Incoherent(
                    "selected auxiliary root header height disagrees with its index",
                )
                .into());
            }
            let Some(delivery) = self.selected_aux_delivery(&node)? else {
                break;
            };
            let Some(aux) = delivery.tree_aux else {
                break;
            };
            if delivery.header_hash != hash || aux.height != height {
                return Err(StoreError::Incoherent(
                    "selected auxiliary root delivery disagrees with its header",
                )
                .into());
            }
            roots.push(BlockCommitmentRoots {
                height,
                sapling_root: aux.sapling_root,
                orchard_root: aux.orchard_root,
                ironwood_root: aux.ironwood_root,
                sapling_tx: aux.sapling_tx_count,
                orchard_tx: aux.orchard_tx_count,
                ironwood_tx: aux.ironwood_tx_count,
                auth_data_root: aux.auth_data_root,
            });
        }
        Ok(roots)
    }

    pub(crate) fn validation_context(
        &self,
        parent_hash: block::Hash,
    ) -> Result<Option<ValidationLease>, HeaderChainStoreError> {
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        if self.store.node(parent_hash)?.is_none() {
            return Ok(None);
        }
        self.store
            .validation_context(parent_hash)
            .map(Some)
            .map_err(HeaderChainStoreError::Store)
    }

    pub(crate) fn selected_tip(&self) -> Result<Frontier, HeaderChainStoreError> {
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        Ok(self.store.snapshot()?.frontiers.header_best)
    }

    pub(crate) fn selected_hash(
        &self,
        height: block::Height,
    ) -> Result<Option<block::Hash>, HeaderChainStoreError> {
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        self.store
            .selected_hash(height)
            .map_err(HeaderChainStoreError::Store)
    }

    pub(crate) fn selected_successor(
        &self,
        height: block::Height,
        hash: block::Hash,
    ) -> Result<Option<HeaderNode>, HeaderChainStoreError> {
        let Ok(successor_height) = height.next() else {
            return Ok(None);
        };
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        let Some(successor_hash) = self.store.selected_hash(successor_height)? else {
            return Ok(None);
        };
        let Some(successor) = self.store.node(successor_hash)? else {
            return Ok(None);
        };
        Ok((successor.parent_hash == hash).then_some(successor))
    }

    /// Read one exact selected header and its optional direct successor without
    /// allowing a concurrent transition to mix branches or auxiliary records.
    pub(crate) fn selected_aux_window(
        &self,
        height: block::Height,
        hash: block::Hash,
    ) -> Result<Option<SelectedAuxWindow>, HeaderChainStoreError> {
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        if self.store.selected_hash(height)? != Some(hash) {
            return Ok(None);
        }
        let current = self.store.node(hash)?.ok_or(StoreError::Incoherent(
            "selected auxiliary header is not retained",
        ))?;
        if current.height != height {
            return Err(StoreError::Incoherent(
                "selected auxiliary header height disagrees with its index",
            )
            .into());
        }
        let current_deliveries = self.coherent_aux_deliveries(&current)?;
        let successor = match height.next() {
            Ok(successor_height) => match self.store.selected_hash(successor_height)? {
                Some(successor_hash) => {
                    let successor =
                        self.store
                            .node(successor_hash)?
                            .ok_or(StoreError::Incoherent(
                                "selected auxiliary successor is not retained",
                            ))?;
                    if successor.height != successor_height || successor.parent_hash != hash {
                        return Err(StoreError::Incoherent(
                            "selected auxiliary successor does not extend the requested header",
                        )
                        .into());
                    }
                    let deliveries = self.coherent_aux_deliveries(&successor)?;
                    Some((successor, deliveries))
                }
                None => None,
            },
            Err(_) => None,
        };
        Ok(Some(SelectedAuxWindow {
            snapshot: self
                .store
                .snapshot()
                .map_err(HeaderChainStoreError::Store)?,
            current,
            current_deliveries,
            successor,
        }))
    }

    pub(crate) fn selected_locator(&self) -> Result<HeaderLocator, HeaderChainStoreError> {
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        let snapshot = self
            .store
            .snapshot()
            .map_err(HeaderChainStoreError::Store)?;
        HeaderLocator::for_selected_path(&snapshot, |height| self.store.selected_hash(height))
            .map_err(HeaderChainStoreError::Store)
    }

    /// Resolve an exact, still-current VCT repair owner to one selected header request.
    pub(crate) fn vct_repair_context(
        &self,
        owner: WorkOwner,
        height: block::Height,
    ) -> Result<Option<zakura_header_chain::VctRepairContext>, HeaderChainStoreError> {
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        let snapshot = self
            .store
            .snapshot()
            .map_err(HeaderChainStoreError::Store)?;
        if owner.scope() != WorkScope::for_body_work(&snapshot)
            || height <= snapshot.frontiers.finalized.height
            || height > snapshot.frontiers.header_best.height
        {
            return Ok(None);
        }
        let Some(target_hash) = self.store.selected_hash(height)? else {
            return Err(StoreError::Incoherent(
                "VCT repair height is absent from the selected projection",
            )
            .into());
        };
        let target = self.store.node(target_hash)?.ok_or(StoreError::Incoherent(
            "selected VCT repair header is not retained",
        ))?;
        if target.height != height {
            return Err(StoreError::Incoherent(
                "selected VCT repair header height disagrees with its index",
            )
            .into());
        }
        let parent_height = block::Height(height.0.checked_sub(1).ok_or(
            StoreError::Incoherent("non-finalized VCT repair header has no predecessor height"),
        )?);
        if self.store.selected_hash(parent_height)? != Some(target.parent_hash) {
            return Err(StoreError::Incoherent(
                "selected VCT repair header does not extend its selected predecessor",
            )
            .into());
        }
        let parent = Frontier::new(parent_height, target.parent_hash);
        Ok(Some(zakura_header_chain::VctRepairContext {
            target: Frontier::new(height, target_hash),
            locator: HeaderLocator::for_continuation(parent),
        }))
    }

    pub(crate) fn acquire_retained_path(
        &self,
        peer: SourceId,
        session_id: u64,
        target_tip_hash: block::Hash,
        locator_hashes: &[block::Hash],
    ) -> Result<RetainedPathLeaseOutcome, HeaderChainStoreError> {
        if locator_hashes.is_empty()
            || locator_hashes.len() > zakura_header_chain::MAX_HEADER_LOCATOR_HASHES
        {
            return Err(HeaderChainStoreError::Store(StoreError::Incoherent(
                "retained path locator count is outside protocol bounds",
            )));
        }
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        let snapshot = self.store.snapshot()?;
        let Some(target_node) = self.store.node(target_tip_hash)? else {
            return Ok(RetainedPathLeaseOutcome::TargetNotRetained);
        };
        let target = Frontier::new(target_node.height, target_tip_hash);
        let mut reverse_path = vec![target];
        let mut current = target_node;
        while current.height > snapshot.frontiers.finalized.height {
            let Some(parent) = self.store.node(current.parent_hash)? else {
                return Ok(RetainedPathLeaseOutcome::HistoryPruned);
            };
            if parent.height.next().ok() != Some(current.height) {
                return Err(HeaderChainStoreError::Store(StoreError::Incoherent(
                    "retained target path has non-contiguous heights",
                )));
            }
            reverse_path.push(Frontier::new(parent.height, parent.hash));
            current = parent;
        }
        if reverse_path.last().copied() != Some(snapshot.frontiers.finalized) {
            return Ok(RetainedPathLeaseOutcome::HistoryPruned);
        }
        reverse_path.reverse();
        let common_index = locator_hashes.iter().find_map(|locator_hash| {
            reverse_path
                .iter()
                .position(|frontier| frontier.hash == *locator_hash)
        });
        let Some(common_index) = common_index else {
            return Ok(RetainedPathLeaseOutcome::NoLocatorIntersection);
        };
        let common_ancestor = reverse_path[common_index];
        let path: Arc<[block::Hash]> = reverse_path[common_index.saturating_add(1)..]
            .iter()
            .map(|frontier| frontier.hash)
            .collect();
        let mut leases = self
            .leases
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        Ok(leases.insert(
            peer,
            session_id,
            (target, common_ancestor),
            path,
            snapshot.header_generation,
            Instant::now(),
        ))
    }

    pub(crate) fn read_retained_path(
        &self,
        peer: SourceId,
        session_id: u64,
        lease_id: u64,
        after_hash: block::Hash,
        max_count: u32,
    ) -> Result<RetainedPathReadOutcome, HeaderChainStoreError> {
        if max_count == 0 {
            return Err(HeaderChainStoreError::Store(StoreError::Incoherent(
                "retained path page count is zero",
            )));
        }
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        let mut leases = self
            .leases
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        let lease = leases.get(peer, session_id, lease_id, Instant::now());
        let Some(lease) = lease else {
            return Ok(RetainedPathReadOutcome::Unavailable);
        };
        let (start, page_ancestor) = if after_hash == lease.common_ancestor.hash {
            (0, lease.common_ancestor)
        } else {
            let Some(index) = lease.path.iter().position(|hash| *hash == after_hash) else {
                return Ok(RetainedPathReadOutcome::Unavailable);
            };
            let node = self.store.node(after_hash)?.ok_or(StoreError::Incoherent(
                "active retained path page ancestor is absent",
            ))?;
            (
                index.saturating_add(1),
                Frontier::new(node.height, node.hash),
            )
        };
        let count = usize::try_from(max_count).unwrap_or(usize::MAX);
        let end = start.saturating_add(count).min(lease.path.len());
        let mut nodes = Vec::with_capacity(end.saturating_sub(start));
        let mut aux_deliveries = Vec::with_capacity(end.saturating_sub(start));
        for hash in &lease.path[start..end] {
            let node = self.store.node(*hash)?.ok_or(StoreError::Incoherent(
                "active retained path node is absent",
            ))?;
            aux_deliveries.push(self.coherent_aux_deliveries(&node)?);
            nodes.push(node);
        }
        let renewed = leases.renew(peer, session_id, lease_id, Instant::now());
        debug_assert!(renewed, "the lease registry is locked across the page read");
        Ok(RetainedPathReadOutcome::Page(RetainedPathPage {
            lease_id,
            common_ancestor: page_ancestor,
            target: lease.target,
            nodes,
            aux_deliveries,
            complete: end == lease.path.len(),
        }))
    }

    pub(crate) fn release_retained_path(
        &self,
        peer: SourceId,
        session_id: u64,
        lease_id: u64,
    ) -> Result<bool, HeaderChainStoreError> {
        Ok(self
            .leases
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?
            .release(peer, session_id, lease_id))
    }
}

impl HeaderChainRuntime {
    /// Return the sole committed-snapshot publisher.
    pub fn publisher(&self) -> &Publisher {
        &self.publisher
    }

    /// Return a read-only handle whose compound reads share the transition lock.
    pub(crate) fn reader(&self) -> HeaderChainReader {
        HeaderChainReader {
            store: self.store.clone(),
            leases: self.leases.clone(),
        }
    }

    /// Read the exact durable verified projection used to prove full-state finality.
    pub(in crate::service) fn verified_projection(
        &self,
    ) -> Result<Vec<Frontier>, HeaderChainStoreError> {
        self.store
            .verified_projection()
            .map_err(HeaderChainStoreError::Store)
    }

    /// Apply, commit, and publish one serialized transition.
    pub fn apply(
        &self,
        request: TransitionRequest,
        context: &TransitionContext<'_>,
    ) -> Result<ApplyResult, HeaderChainStoreError> {
        self.apply_combined(request, context, DiskWriteBatch::new(), || {})
    }

    fn apply_with_fault<F>(
        &self,
        request: TransitionRequest,
        context: &TransitionContext<'_>,
        fault: F,
    ) -> Result<ApplyResult, HeaderChainStoreError>
    where
        F: FnMut(FaultPoint) -> Result<(), HeaderChainStoreError>,
    {
        self.apply_combined_with_fault(request, context, DiskWriteBatch::new(), || {}, fault)
    }

    pub(in crate::service) fn apply_combined<M>(
        &self,
        request: TransitionRequest,
        context: &TransitionContext<'_>,
        full_state_batch: DiskWriteBatch,
        memory_swap: M,
    ) -> Result<ApplyResult, HeaderChainStoreError>
    where
        M: FnOnce(),
    {
        self.apply_combined_with_fault(request, context, full_state_batch, memory_swap, |_| Ok(()))
    }

    pub(in crate::service) fn apply_combined_expected<M>(
        &self,
        request: TransitionRequest,
        context: &TransitionContext<'_>,
        full_state_batch: DiskWriteBatch,
        expected_verified: Frontier,
        memory_swap: M,
    ) -> Result<ApplyResult, HeaderChainStoreError>
    where
        M: FnOnce(),
    {
        self.apply_combined_with_fault_and_expected(
            request,
            context,
            full_state_batch,
            memory_swap,
            Some(expected_verified),
            |_| Ok(()),
        )
    }

    fn apply_combined_with_fault<M, F>(
        &self,
        request: TransitionRequest,
        context: &TransitionContext<'_>,
        full_state_batch: DiskWriteBatch,
        memory_swap: M,
        fault: F,
    ) -> Result<ApplyResult, HeaderChainStoreError>
    where
        M: FnOnce(),
        F: FnMut(FaultPoint) -> Result<(), HeaderChainStoreError>,
    {
        self.apply_combined_with_fault_and_expected(
            request,
            context,
            full_state_batch,
            memory_swap,
            None,
            fault,
        )
    }

    fn apply_combined_with_fault_and_expected<M, F>(
        &self,
        request: TransitionRequest,
        context: &TransitionContext<'_>,
        full_state_batch: DiskWriteBatch,
        memory_swap: M,
        expected_verified: Option<Frontier>,
        mut fault: F,
    ) -> Result<ApplyResult, HeaderChainStoreError>
    where
        M: FnOnce(),
        F: FnMut(FaultPoint) -> Result<(), HeaderChainStoreError>,
    {
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        let mut retention_references = context.retention_references.to_vec();
        retention_references.extend(
            self.leases
                .lock()
                .map_err(|_| HeaderChainStoreError::WriterPoisoned)?
                .active_references(Instant::now()),
        );
        retention_references.sort_unstable_by_key(|hash| hash.0);
        retention_references.dedup();
        let context = TransitionContext {
            config: context.config,
            clock: context.clock,
            full_state_authority: context.full_state_authority,
            startup_capability: context.startup_capability,
            retention_references: &retention_references,
        };
        let before = self.store.snapshot()?;
        if let Some(pin) = before.alarms.migrated_pin_refuted {
            return Err(HeaderChainStoreError::MigratedPinRefuted { pin });
        }
        fault(FaultPoint::AfterSnapshot)?;
        let event = request.event.idempotency_key();
        let branch = request.event.work_owner().map(|owner| owner.branch);
        let metadata = self.store.metadata()?;
        let is_idempotent_replay = event.is_some_and(|event| metadata.last_transition_id == event);
        if !is_idempotent_replay && request.expected_version != before.state_version {
            return Ok(ApplyResult::Stale(StaleReceipt {
                current_version: before.state_version,
                branch,
            }));
        }
        fault(FaultPoint::AfterVersionCheck)?;
        let plan = match apply_transition(&self.store, request, &context) {
            Ok(plan) => plan,
            Err(TransitionFailure::Stale { current }) => {
                return Ok(ApplyResult::Stale(StaleReceipt {
                    current_version: current,
                    branch,
                }));
            }
            Err(error) => return Err(error.into()),
        };
        if let Some(expected) = expected_verified {
            let actual = plan.change_set().metadata.frontiers.verified_best;
            if expected != actual {
                return Err(HeaderChainStoreError::VerifiedFrontierMismatch { expected, actual });
            }
        }
        if plan.is_no_change() {
            fault(FaultPoint::BeforeDbCommit)?;
            self.store.db.write(full_state_batch)?;
            fault(FaultPoint::AfterDbCommit)?;
            fault(FaultPoint::BeforeMemorySwap)?;
            memory_swap();
            fault(FaultPoint::BeforeReactorObserve)?;
            return Ok(ApplyResult::NoChange(NoChangeReceipt {
                state_version: plan.before().state_version,
                event,
            }));
        }

        let durable_tx_id = plan.change_set().metadata.state_version.get();
        let migrated_pin_refuted = plan.change_set().metadata.alarms.migrated_pin_refuted;
        let batch =
            self.store
                .batch_for_with_fault(plan.change_set(), full_state_batch, &mut fault)?;
        fault(FaultPoint::BeforeDbCommit)?;
        self.store.db.write(batch)?;
        fault(FaultPoint::AfterDbCommit)?;
        if let Some(pin) = migrated_pin_refuted {
            return Err(HeaderChainStoreError::MigratedPinRefuted { pin });
        }
        fault(FaultPoint::BeforeMemorySwap)?;
        memory_swap();
        let receipt = plan.into_committed_receipt(durable_tx_id);
        fault(FaultPoint::BeforePublish)?;
        self.publisher.publish(receipt.current.clone());
        fault(FaultPoint::AfterPublish)?;
        fault(FaultPoint::BeforeReactorObserve)?;
        Ok(ApplyResult::Committed(Box::new(receipt)))
    }
}

/// Deterministic state-writer and observer boundaries used by the crash harness.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum FaultPoint {
    AfterSnapshot,
    AfterVersionCheck,
    AfterEachNodeWrite,
    AfterEachIndexWrite,
    AfterProjectionWrite,
    AfterMetadataWrite,
    BeforeDbCommit,
    AfterDbCommit,
    BeforeMemorySwap,
    BeforePublish,
    AfterPublish,
    BeforeReactorObserve,
}

impl FaultPoint {
    /// Complete ordered state-writer crash surface used by deterministic recovery tests.
    pub const ALL: [Self; 12] = [
        Self::AfterSnapshot,
        Self::AfterVersionCheck,
        Self::AfterEachNodeWrite,
        Self::AfterEachIndexWrite,
        Self::AfterProjectionWrite,
        Self::AfterMetadataWrite,
        Self::BeforeDbCommit,
        Self::AfterDbCommit,
        Self::BeforeMemorySwap,
        Self::BeforePublish,
        Self::AfterPublish,
        Self::BeforeReactorObserve,
    ];
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

    pub(in crate::service) fn is_initialized(&self) -> Result<bool, HeaderChainStoreError> {
        Ok(self.metadata_row()?.is_some())
    }

    /// Exhaustively audit, atomically repair reconstructible caches, then enable publication.
    pub fn startup(
        self,
        config: &EngineConfig,
    ) -> Result<(HeaderChainRuntime, StartupReport), HeaderChainStoreError> {
        let writer = self
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        let plan = audit_store(&self, config)?;
        if let Some(pin) = plan.metadata.alarms.migrated_pin_refuted {
            return Err(HeaderChainStoreError::MigratedPinRefuted { pin });
        }
        let previous = plan.before.clone();
        let repairs = plan.repairs.clone();
        if !plan.is_clean() {
            self.db.write(self.recovery_batch(&plan)?)?;
        }
        let current = plan.metadata.snapshot();
        let report = StartupReport {
            previous,
            current: current.clone(),
            repairs,
            publication_allowed: true,
        };
        let publisher = Publisher::new(current);
        drop(writer);
        Ok((
            HeaderChainRuntime {
                store: self,
                publisher,
                leases: Arc::new(Mutex::new(RetainedPathLeaseRegistry::default())),
            },
            report,
        ))
    }

    /// Explicitly preserve a headers-only store's pins while changing its durable mode.
    pub fn migrate_headers_only_to_integrated(
        self,
        integrated_config: &EngineConfig,
        full_state_verified: Frontier,
    ) -> Result<(HeaderChainRuntime, StartupReport), HeaderChainStoreError> {
        if integrated_config.mode != EngineMode::Integrated {
            return Err(HeaderChainStoreError::Incoherent(
                "mode migration target is not integrated",
            ));
        }
        let mut headers_only_config = integrated_config.clone();
        headers_only_config.mode = EngineMode::HeadersOnly;
        let writer = self
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        let source = audit_store(&self, &headers_only_config)?;
        if let Some(pin) = source.metadata.alarms.migrated_pin_refuted {
            return Err(HeaderChainStoreError::MigratedPinRefuted { pin });
        }
        if source.metadata.frontiers.finalized != full_state_verified {
            return Err(HeaderChainStoreError::Incoherent(
                "integrated migration requires full-state verification through the preserved pin",
            ));
        }
        let previous = source.before.clone();
        let mut repairs = source.repairs.clone();
        if !source.is_clean() {
            self.db.write(self.recovery_batch(&source)?)?;
        }

        let history = self.finality_history()?;
        let mut metadata = self.metadata()?;
        metadata.mode = EngineMode::Integrated;
        metadata.state_version = metadata.state_version.checked_next()?;
        metadata.header_generation = metadata.header_generation.checked_next()?;
        metadata.verified_generation = metadata.verified_generation.checked_next()?;
        let mut hasher = Sha256::new();
        hasher.update(b"zakura-header-chain-mode-migration-v1");
        hasher.update(metadata.state_version.get().to_be_bytes());
        hasher.update(metadata.frontiers.finalized.height.0.to_be_bytes());
        hasher.update(metadata.frontiers.finalized.hash.0);
        metadata.last_transition_id = EvidenceId::from_digest(hasher.finalize().into());

        let mut batch = DiskWriteBatch::new();
        for record in history.into_iter().map(preserve_headers_only_pin) {
            self.put_value(
                &mut batch,
                HEADER_FINALITY_HISTORY,
                HeaderFinalityKey(record.epoch).as_bytes(),
                &HeaderFinalityRecordDisk(record),
            )?;
        }
        self.put_value(
            &mut batch,
            HEADER_ENGINE_META,
            METADATA_KEY,
            &HeaderEngineMetadataDisk(metadata),
        )?;
        self.db.write(batch)?;

        let target = audit_store(&self, integrated_config)?;
        repairs.extend(target.repairs.iter().copied());
        if !target.is_clean() {
            self.db.write(self.recovery_batch(&target)?)?;
        }
        let current = target.metadata.snapshot();
        let report = StartupReport {
            previous,
            current: current.clone(),
            repairs,
            publication_allowed: true,
        };
        let publisher = Publisher::new(current);
        drop(writer);
        Ok((
            HeaderChainRuntime {
                store: self,
                publisher,
                leases: Arc::new(Mutex::new(RetainedPathLeaseRegistry::default())),
            },
            report,
        ))
    }

    /// Audit and reconcile the exact restored full-state path before enabling publication.
    pub(in crate::service) fn startup_reconciled(
        self,
        config: &EngineConfig,
        full_state_finalized: Frontier,
        finalized_path: Vec<VerifiedHeaderRef>,
        restored_path: Vec<VerifiedHeaderRef>,
    ) -> Result<(HeaderChainRuntime, StartupReport), HeaderChainStoreError> {
        struct Authority(EvidenceId);

        impl FullStateEvidenceAuthority for Authority {
            fn authorizes(&self, evidence: EvidenceId) -> bool {
                evidence == self.0
            }
        }

        let writer = self
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        let initial = audit_store(&self, config)?;
        if let Some(pin) = initial.metadata.alarms.migrated_pin_refuted {
            return Err(HeaderChainStoreError::MigratedPinRefuted { pin });
        }
        let previous = initial.before.clone();
        let mut repairs = initial.repairs.clone();
        if !initial.is_clean() {
            self.db.write(self.recovery_batch(&initial)?)?;
        }

        let snapshot = self.snapshot()?;
        let mut authoritative_path = finalized_path;
        authoritative_path.extend(restored_path);
        let mut expected_projection = vec![snapshot.frontiers.finalized];
        expected_projection.extend(
            authoritative_path
                .iter()
                .map(|header| Frontier::new(header.height, header.hash)),
        );
        if self.verified_projection()? != expected_projection {
            let mut hasher = Sha256::new();
            hasher.update(b"zakura-header-chain-startup-reconciliation-v1");
            hasher.update(snapshot.state_version.get().to_be_bytes());
            hasher.update(snapshot.frontiers.verified_best.hash.0);
            for header in &authoritative_path {
                hasher.update(header.height.0.to_be_bytes());
                hasher.update(header.hash.0);
            }
            let evidence = EvidenceId::from_digest(hasher.finalize().into());
            let authority = Authority(evidence);
            let context = TransitionContext {
                config,
                clock: &SystemClock,
                full_state_authority: Some(&authority),
                startup_capability: None,
                retention_references: &[],
            };
            let plan = apply_transition(
                &self,
                TransitionRequest {
                    expected_version: snapshot.state_version,
                    event: TransitionEvent::VerifiedChainChanged(VerifiedChainChanged {
                        full_state_transition_id: evidence,
                        old_tip: snapshot.frontiers.verified_best,
                        new_path: authoritative_path,
                        cause: VerifiedChangeCause::Reset,
                    }),
                },
                &context,
            )?;
            if !plan.is_no_change() {
                self.db.write(self.batch_for(plan.change_set())?)?;
            }
        }

        let snapshot = self.snapshot()?;
        if snapshot.frontiers.finalized != full_state_finalized {
            let proof = self
                .verified_projection()?
                .into_iter()
                .take_while(|frontier| frontier.height <= full_state_finalized.height)
                .map(|frontier| frontier.hash)
                .collect::<Vec<_>>();
            let mut hasher = Sha256::new();
            hasher.update(b"zakura-header-chain-startup-finalization-v1");
            hasher.update(snapshot.state_version.get().to_be_bytes());
            hasher.update(full_state_finalized.height.0.to_be_bytes());
            hasher.update(full_state_finalized.hash.0);
            for hash in &proof {
                hasher.update(hash.0);
            }
            let evidence = EvidenceId::from_digest(hasher.finalize().into());
            let authority = Authority(evidence);
            let context = TransitionContext {
                config,
                clock: &SystemClock,
                full_state_authority: Some(&authority),
                startup_capability: None,
                retention_references: &[],
            };
            let plan = apply_transition(
                &self,
                TransitionRequest {
                    expected_version: snapshot.state_version,
                    event: TransitionEvent::FullStateFinalized(FullStateFinalized {
                        full_state_transition_id: evidence,
                        new_finalized: full_state_finalized,
                        verified_path_proof: proof,
                    }),
                },
                &context,
            )?;
            if !plan.is_no_change() {
                self.db.write(self.batch_for(plan.change_set())?)?;
            }
        }

        let final_audit = audit_store(&self, config)?;
        repairs.extend(final_audit.repairs.iter().copied());
        if !final_audit.is_clean() {
            self.db.write(self.recovery_batch(&final_audit)?)?;
        }
        let current = final_audit.metadata.snapshot();
        let report = StartupReport {
            previous,
            current: current.clone(),
            repairs,
            publication_allowed: true,
        };
        let publisher = Publisher::new(current);
        drop(writer);
        Ok((
            HeaderChainRuntime {
                store: self,
                publisher,
                leases: Arc::new(Mutex::new(RetainedPathLeaseRegistry::default())),
            },
            report,
        ))
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
        self.batch_for_with_fault(changes, DiskWriteBatch::new(), &mut |_| Ok(()))
    }

    fn batch_for_with_fault<F>(
        &self,
        changes: &ChangeSet,
        mut batch: DiskWriteBatch,
        fault: &mut F,
    ) -> Result<DiskWriteBatch, HeaderChainStoreError>
    where
        F: FnMut(FaultPoint) -> Result<(), HeaderChainStoreError>,
    {
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
            fault(FaultPoint::AfterEachNodeWrite)?;
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
            fault(FaultPoint::AfterEachNodeWrite)?;
        }

        self.replace_candidates(&mut batch, &changes.candidate_tips)?;
        fault(FaultPoint::AfterEachIndexWrite)?;
        self.apply_projection(&mut batch, HEADER_SELECTED, &changes.selected_projection)?;
        fault(FaultPoint::AfterProjectionWrite)?;
        self.apply_projection(&mut batch, HEADER_VERIFIED, &changes.verified_projection)?;
        fault(FaultPoint::AfterProjectionWrite)?;

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
            fault(FaultPoint::AfterEachIndexWrite)?;
        }

        if let Some(record) = changes.finality_append {
            self.put_value(
                &mut batch,
                HEADER_FINALITY_HISTORY,
                HeaderFinalityKey(record.epoch).as_bytes(),
                &HeaderFinalityRecordDisk(record),
            )?;
            fault(FaultPoint::AfterEachIndexWrite)?;
        }

        // The singleton logical root is deliberately enqueued last in the same atomic batch.
        self.put_value(
            &mut batch,
            HEADER_ENGINE_META,
            METADATA_KEY,
            &HeaderEngineMetadataDisk(changes.metadata.clone()),
        )?;
        fault(FaultPoint::AfterMetadataWrite)?;
        Ok(batch)
    }

    fn recovery_batch(&self, plan: &RecoveryPlan) -> Result<DiskWriteBatch, HeaderChainStoreError> {
        let mut batch = DiskWriteBatch::new();
        if plan.repairs.contains(&RecoveryRepair::InheritedEligibility) {
            for node in &plan.nodes {
                self.put_value(
                    &mut batch,
                    HEADER_NODE_BY_HASH,
                    node.hash.0,
                    &HeaderNodeDisk::from_domain(node),
                )?;
            }
        }
        if plan.repairs.contains(&RecoveryRepair::ChildIndex) {
            self.clear_family(&mut batch, HEADER_CHILD)?;
            for (parent, child) in &plan.child_edges {
                self.put_empty(
                    &mut batch,
                    HEADER_CHILD,
                    HeaderChildKey {
                        parent: *parent,
                        child: *child,
                    }
                    .as_bytes(),
                )?;
            }
        }
        if plan.repairs.contains(&RecoveryRepair::HeightIndex) {
            self.clear_family(&mut batch, HEADER_HEIGHT_HASH)?;
            for frontier in &plan.height_entries {
                self.put_empty(
                    &mut batch,
                    HEADER_HEIGHT_HASH,
                    HeaderHeightHashKey {
                        height: frontier.height,
                        hash: frontier.hash,
                    }
                    .as_bytes(),
                )?;
            }
        }
        if plan.repairs.contains(&RecoveryRepair::DeferredIndex) {
            self.clear_family(&mut batch, HEADER_DEFERRED)?;
            for (until, hash) in &plan.deferred_entries {
                let key = HeaderDeferredKey::new(
                    until.timestamp(),
                    until.timestamp_subsec_nanos(),
                    *hash,
                )
                .map_err(|_| HeaderChainStoreError::Incoherent("invalid recovery timestamp"))?;
                self.put_empty(&mut batch, HEADER_DEFERRED, key.as_bytes())?;
            }
        }
        if plan.repairs.contains(&RecoveryRepair::CandidateIndex) {
            self.clear_family(&mut batch, HEADER_CANDIDATE)?;
            for (score, hash) in &plan.candidate_entries {
                if score.tip_hash != *hash {
                    return Err(HeaderChainStoreError::Incoherent(
                        "recovery candidate score/hash mismatch",
                    ));
                }
                self.put_empty(
                    &mut batch,
                    HEADER_CANDIDATE,
                    HeaderCandidateKey(*score).as_bytes(),
                )?;
            }
        }
        if plan.repairs.contains(&RecoveryRepair::SelectedProjection) {
            self.replace_projection(&mut batch, HEADER_SELECTED, &plan.selected_projection)?;
        }
        if plan.repairs.contains(&RecoveryRepair::VerifiedProjection) {
            self.replace_projection(&mut batch, HEADER_VERIFIED, &plan.verified_projection)?;
        }
        self.put_value(
            &mut batch,
            HEADER_ENGINE_META,
            METADATA_KEY,
            &HeaderEngineMetadataDisk(plan.metadata.clone()),
        )?;
        Ok(batch)
    }

    fn clear_family(
        &self,
        batch: &mut DiskWriteBatch,
        family: &'static str,
    ) -> Result<(), HeaderChainStoreError> {
        for (key, _) in self.scan_raw(family)? {
            self.delete_raw(batch, family, key)?;
        }
        Ok(())
    }

    fn replace_projection(
        &self,
        batch: &mut DiskWriteBatch,
        family: &'static str,
        projection: &[Frontier],
    ) -> Result<(), HeaderChainStoreError> {
        self.clear_family(batch, family)?;
        for frontier in projection {
            self.put_raw(
                batch,
                family,
                HeaderHeightKey(frontier.height).as_bytes(),
                frontier.hash.0,
            )?;
        }
        Ok(())
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

fn preserve_headers_only_pin(mut record: FinalityRecord) -> FinalityRecord {
    if matches!(record.source, FinalitySource::HeadersOnlyDepth { .. }) {
        record.source = FinalitySource::MigratedHeadersOnly;
    }
    record
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

impl StoreAuditRead for HeaderChainStore {
    fn all_nodes(&self) -> Result<Vec<HeaderNode>, StoreError> {
        let mut reasons_by_hash: HashMap<block::Hash, Vec<EligibilityReason>> = HashMap::new();
        for (hash, reason) in self.all_reason_rows()? {
            reasons_by_hash.entry(hash).or_default().push(reason);
        }
        let mut nodes = Vec::new();
        for (key, value) in self.scan_raw(HEADER_NODE_BY_HASH).map_err(store_error)? {
            if key.len() != 32 {
                return Err(StoreError::Incoherent("invalid node key width"));
            }
            let hash = block::Hash(
                key.as_slice()
                    .try_into()
                    .map_err(|_| StoreError::Incoherent("invalid node hash key"))?,
            );
            let disk = HeaderNodeDisk::decode(&value)
                .map_err(|_| StoreError::Incoherent("invalid durable node value"))?;
            if disk.hash != hash {
                return Err(StoreError::Incoherent("node key/hash mismatch"));
            }
            let node = disk
                .into_domain(reasons_by_hash.remove(&hash).unwrap_or_default())
                .map_err(|_| StoreError::Incoherent("invalid durable node"))?;
            nodes.push(node);
        }
        if !reasons_by_hash.is_empty() {
            return Err(StoreError::Incoherent("eligibility root has no node"));
        }
        Ok(nodes)
    }

    fn child_edges(&self) -> Result<Vec<(block::Hash, block::Hash)>, StoreError> {
        let mut edges = Vec::new();
        for (key, value) in self.scan_raw(HEADER_CHILD).map_err(store_error)? {
            if key.len() != 64 || !value.is_empty() {
                return Err(StoreError::Incoherent("invalid child-index row"));
            }
            let key = HeaderChildKey::from_bytes(&key);
            edges.push((key.parent, key.child));
        }
        Ok(edges)
    }

    fn height_entries(&self) -> Result<Vec<Frontier>, StoreError> {
        let mut entries = Vec::new();
        for (key, value) in self.scan_raw(HEADER_HEIGHT_HASH).map_err(store_error)? {
            if key.len() != 36 || !value.is_empty() {
                return Err(StoreError::Incoherent("invalid height-index row"));
            }
            let key = HeaderHeightHashKey::from_bytes(&key);
            entries.push(Frontier::new(key.height, key.hash));
        }
        Ok(entries)
    }

    fn selected_projection(&self) -> Result<Vec<Frontier>, StoreError> {
        self.projection_entries(HEADER_SELECTED)
    }

    fn verified_projection(&self) -> Result<Vec<Frontier>, StoreError> {
        self.projection_entries(HEADER_VERIFIED)
    }

    fn candidate_entries(&self) -> Result<Vec<(ChainScore, block::Hash)>, StoreError> {
        self.candidate_tips()
    }

    fn deferred_entries(&self) -> Result<Vec<(chrono::DateTime<Utc>, block::Hash)>, StoreError> {
        let mut entries = Vec::new();
        for (key, value) in self.scan_raw(HEADER_DEFERRED).map_err(store_error)? {
            if key.len() != 44 || !value.is_empty() {
                return Err(StoreError::Incoherent("invalid deferred-index row"));
            }
            let key = HeaderDeferredKey::try_from_bytes(&key)
                .map_err(|_| StoreError::Incoherent("invalid deferred-index key"))?;
            let until = Utc
                .timestamp_opt(key.seconds, key.nanoseconds)
                .single()
                .ok_or(StoreError::Incoherent("invalid deferred-index timestamp"))?;
            entries.push((until, key.hash));
        }
        Ok(entries)
    }

    fn eligibility_roots(&self) -> Result<Vec<(block::Hash, EligibilityReason)>, StoreError> {
        self.all_reason_rows()
    }

    fn all_aux_deliveries(&self) -> Result<Vec<AuxDelivery>, StoreError> {
        let mut deliveries = Vec::new();
        for (key, value) in self.scan_raw(HEADER_AUX_DELIVERY).map_err(store_error)? {
            if key.len() != 64 {
                return Err(StoreError::Incoherent("invalid auxiliary key width"));
            }
            let key = HeaderAuxDeliveryKey::from_bytes(&key);
            let delivery = HeaderAuxDeliveryDisk::decode(&value)
                .map_err(|_| StoreError::Incoherent("invalid auxiliary value"))?
                .0;
            if delivery.header_hash != key.header || delivery.delivery_id != key.delivery {
                return Err(StoreError::Incoherent("auxiliary key/value mismatch"));
            }
            deliveries.push(delivery);
        }
        Ok(deliveries)
    }

    fn validation_context_records(&self) -> Result<Vec<ValidationContextRecord>, StoreError> {
        let mut records = Vec::new();
        for (key, value) in self
            .scan_raw(HEADER_VALIDATION_CONTEXT)
            .map_err(store_error)?
        {
            if key.len() != 32 {
                return Err(StoreError::Incoherent(
                    "invalid validation-context key width",
                ));
            }
            let hash = block::Hash(
                key.as_slice()
                    .try_into()
                    .map_err(|_| StoreError::Incoherent("invalid validation-context key"))?,
            );
            let record = HeaderValidationContextDisk::decode(&value)
                .map_err(|_| StoreError::Incoherent("invalid validation-context value"))?;
            if record.header.hash() != hash {
                return Err(StoreError::Incoherent(
                    "validation-context key/hash mismatch",
                ));
            }
            records.push(ValidationContextRecord {
                header: record.header,
                height: record.height,
            });
        }
        Ok(records)
    }
}

impl HeaderChainStore {
    fn all_reason_rows(&self) -> Result<Vec<(block::Hash, EligibilityReason)>, StoreError> {
        let mut reasons = Vec::new();
        for (key, value) in self
            .scan_raw(HEADER_ELIGIBILITY_ROOT)
            .map_err(store_error)?
        {
            let key = HeaderEligibilityRootKey::try_from_bytes(&key)
                .map_err(|_| StoreError::Incoherent("invalid eligibility-root key"))?;
            let reason = HeaderEligibilityReasonDisk::decode(&value)
                .map_err(|_| StoreError::Incoherent("invalid eligibility-root value"))?
                .into_domain();
            if reason_kind(&reason) != key.kind || reason_evidence(&reason) != key.evidence {
                return Err(StoreError::Incoherent(
                    "eligibility-root key/value mismatch",
                ));
            }
            reasons.push((key.root, reason));
        }
        Ok(reasons)
    }

    fn projection_entries(&self, family: &'static str) -> Result<Vec<Frontier>, StoreError> {
        let mut projection = Vec::new();
        for (key, value) in self.scan_raw(family).map_err(store_error)? {
            if key.len() != 4 || value.len() != 32 {
                return Err(StoreError::Incoherent("invalid projection row width"));
            }
            let height = HeaderHeightKey::from_bytes(&key).0;
            let hash = block::Hash(
                value
                    .as_slice()
                    .try_into()
                    .map_err(|_| StoreError::Incoherent("invalid projection hash"))?,
            );
            projection.push(Frontier::new(height, hash));
        }
        projection.sort_unstable_by_key(|frontier| (frontier.height, frontier.hash.0));
        Ok(projection)
    }

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
    use std::{
        num::NonZeroU64,
        sync::atomic::{AtomicBool, Ordering},
    };

    use super::*;
    use crate::{
        constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
        service::finalized_state::{
            zakura_db::block::ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT, STATE_COLUMN_FAMILIES_IN_CODE,
        },
        service::{
            non_finalized_state::NonFinalizedState,
            write::{PreparedFullStateTransition, PreparedFullStateTransitionError},
        },
        Config,
    };
    use zakura_chain::{
        block::genesis::regtest_genesis_block,
        parameters::{testnet::RegtestParameters, Network},
    };
    use zakura_header_chain::{
        AlarmSet, BodyEvidence, BodyRuleId, BodyUnavailableSummary, BodyValidationState,
        CheckpointSet, EligibilityReason, EngineConfig, EngineMode, FinalityEpoch, FrontierSet,
        FullStateEvidenceAuthority, HeaderChainDiskVersion, HeaderGeneration,
        HeaderValidationState, StateVersion, SuffixWork, SystemClock, TransientBodyFailure,
        TransientBodyFailureKind, TransitionEvent, TrustedAnchor, VerifiedBodyEvidence,
        VerifiedChainChanged, VerifiedChangeCause, VerifiedGeneration, WorkCoordinate,
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
    fn publisher_mirror_stays_absent_until_attachment_then_tracks_commits() {
        let (_, _, metadata) = fixture();
        let initial = metadata.snapshot();
        let publisher = Publisher::new(initial.clone());
        let (mirror_sender, mirror_receiver) = watch::channel(None);

        assert_eq!(*mirror_receiver.borrow(), None);

        publisher.mirror_to(mirror_sender);
        assert_eq!(*mirror_receiver.borrow(), Some(initial.clone()));

        let mut committed = initial;
        committed.state_version = StateVersion::new(2);
        publisher.publish(committed.clone());
        assert_eq!(*mirror_receiver.borrow(), Some(committed));
    }

    #[test]
    fn coherent_reader_builds_locator_from_the_durable_selected_projection() {
        let db_config = Config::ephemeral();
        let (engine_config, anchor, metadata) = fixture();
        let store = HeaderChainStore::new(open(&db_config, &engine_config.network));
        store
            .initialize(metadata, anchor.clone())
            .expect("the empty schema initializes");
        let (runtime, _) = store
            .startup(&engine_config)
            .expect("the initialized store audits");

        assert_eq!(
            runtime
                .reader()
                .selected_locator()
                .expect("the selected projection is coherent")
                .entries(),
            &[Frontier::new(anchor.height, anchor.hash)]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn retained_path_leases_are_exact_bounded_session_scoped_and_expiring() {
        let db_config = Config::ephemeral();
        let (engine_config, anchor, metadata) = fixture();
        let anchor_frontier = Frontier::new(anchor.height, anchor.hash);
        let store = HeaderChainStore::new(open(&db_config, &engine_config.network));
        store
            .initialize(metadata, anchor.clone())
            .expect("the empty schema initializes");
        let mut child_header = *anchor.header;
        child_header.previous_block_hash = anchor.hash;
        let child_header = Arc::new(child_header);
        let child = VerifiedHeaderRef {
            height: anchor.height.next().expect("genesis has a successor"),
            hash: child_header.hash(),
            header: child_header,
        };
        let mut grandchild_header = *anchor.header;
        grandchild_header.previous_block_hash = child.hash;
        let grandchild_header = Arc::new(grandchild_header);
        let grandchild = VerifiedHeaderRef {
            height: child.height.next().expect("the child has a successor"),
            hash: grandchild_header.hash(),
            header: grandchild_header,
        };
        let (runtime, _) = store
            .startup_reconciled(
                &engine_config,
                anchor_frontier,
                Vec::new(),
                vec![child.clone(), grandchild.clone()],
            )
            .expect("the selected two-header path reconciles");
        let reader = runtime.reader();
        let validation_lease = reader
            .validation_context(anchor.hash)
            .expect("the retained parent context is coherent")
            .expect("the retained anchor has validation context");
        assert_eq!(validation_lease.parent, anchor_frontier);
        assert_eq!(
            validation_lease.trust_anchor_digest,
            engine_config.trust_anchor_digest()
        );
        assert_eq!(
            reader
                .validation_context(block::Hash([0xff; 32]))
                .expect("an absent parent is a normal stale read"),
            None
        );
        let window = reader
            .selected_aux_window(child.height, child.hash)
            .expect("the exact selected auxiliary window is coherent")
            .expect("the selected child is retained");
        assert_eq!(
            window.snapshot,
            runtime.publisher().snapshot(),
            "the auxiliary window carries the snapshot read under the same transition lock"
        );
        assert_eq!(window.current.hash, child.hash);
        assert!(window.current_deliveries.is_empty());
        let (window_successor, successor_deliveries) =
            window.successor.expect("the selected grandchild follows");
        assert_eq!(window_successor.hash, grandchild.hash);
        assert!(successor_deliveries.is_empty());
        assert_eq!(
            reader
                .selected_aux_window(child.height, block::Hash([0xfe; 32]))
                .expect("a stale branch hash is a normal read outcome"),
            None
        );
        let snapshot = runtime.publisher().snapshot();
        let owner = WorkScope::for_body_work(&snapshot)
            .bind(7, NonZeroU64::new(8).expect("eight is nonzero"));
        let repair = reader
            .vct_repair_context(owner, child.height)
            .expect("the selected repair context is coherent")
            .expect("the current owner resolves its selected header");
        assert_eq!(repair.target, Frontier::new(child.height, child.hash));
        assert_eq!(repair.locator.entries(), &[anchor_frontier]);

        let mut stale_owner = owner;
        stale_owner.state_version = StateVersion::new(
            owner
                .state_version
                .get()
                .checked_add(1)
                .expect("the fixture state version can advance"),
        );
        assert_eq!(
            reader
                .vct_repair_context(stale_owner, child.height)
                .expect("a stale repair owner is a normal read outcome"),
            None
        );
        assert_eq!(
            reader
                .vct_repair_context(owner, anchor.height)
                .expect("a finalized repair height is a normal stale outcome"),
            None
        );

        let aux = zakura_header_chain::TreeAuxRecordV1 {
            height: child.height,
            sapling_root: Default::default(),
            orchard_root: Default::default(),
            ironwood_root: Default::default(),
            sapling_tx_count: 13,
            orchard_tx_count: 14,
            ironwood_tx_count: 15,
            auth_data_root: zakura_chain::block::merkle::AuthDataRoot::from([16; 32]),
        };
        let delivery = AuxDelivery {
            delivery_id: EvidenceId::from_digest([0x91; 32]),
            header_hash: child.hash,
            source: SourceId::from_digest([0x92; 32]),
            owner,
            body_size: zakura_header_chain::BodySizeHint::Unknown,
            tree_aux: Some(aux),
            authentication: zakura_header_chain::AuxAuthentication::Unauthenticated,
        };
        let mut child_node = runtime
            .store
            .node(child.hash)
            .expect("the selected child row decodes")
            .expect("the selected child is retained");
        child_node.aux_delivery_ids.push(delivery.delivery_id);
        let mut aux_batch = DiskWriteBatch::new();
        runtime
            .store
            .put_value(
                &mut aux_batch,
                HEADER_NODE_BY_HASH,
                child.hash.0,
                &HeaderNodeDisk::from_domain(&child_node),
            )
            .expect("the selected child with auxiliary evidence encodes");
        runtime
            .store
            .put_value(
                &mut aux_batch,
                HEADER_AUX_DELIVERY,
                HeaderAuxDeliveryKey {
                    header: child.hash,
                    delivery: delivery.delivery_id,
                }
                .as_bytes(),
                &HeaderAuxDeliveryDisk(delivery),
            )
            .expect("the selected auxiliary delivery encodes");
        runtime
            .store
            .db
            .write(aux_batch)
            .expect("the coherent selected auxiliary fixture commits");
        let roots = reader
            .selected_block_roots(child.height, 2)
            .expect("selected auxiliary roots are coherent");
        assert_eq!(roots.len(), 1, "the read stops at the first missing height");
        assert_eq!(roots[0].height, child.height);
        assert_eq!(roots[0].sapling_tx, aux.sapling_tx_count);
        assert_eq!(roots[0].orchard_tx, aux.orchard_tx_count);
        assert_eq!(roots[0].ironwood_tx, aux.ironwood_tx_count);
        assert_eq!(roots[0].auth_data_root, aux.auth_data_root);
        assert!(matches!(
            crate::service::write::HeaderChainWriter::new(
                runtime.clone(),
                engine_config.clone()
            )
            .vct_aux_window(child.height, child.hash)
            .expect("the selected auxiliary window is coherent"),
            crate::service::write::VctAuxWindowRead::Missing { height }
                if height == grandchild.height
        ));

        let owner = SourceId::from_digest([1; 32]);
        let acquired = reader
            .acquire_retained_path(owner, 7, grandchild.hash, &[anchor.hash])
            .expect("the coherent target path is readable");
        let RetainedPathLeaseOutcome::Acquired(lease) = acquired else {
            panic!("the exact retained target should acquire a lease");
        };
        assert_eq!(
            lease.target,
            Frontier::new(grandchild.height, grandchild.hash)
        );
        assert_eq!(lease.common_ancestor, anchor_frontier);
        assert_eq!(lease.path.as_ref(), &[child.hash, grandchild.hash]);
        assert_eq!(
            lease.generation,
            runtime.publisher().snapshot().header_generation
        );
        assert_eq!(
            reader
                .acquire_retained_path(owner, 7, grandchild.hash, &[anchor.hash])
                .expect("the lease bound is a normal outcome"),
            RetainedPathLeaseOutcome::Busy
        );
        assert_eq!(
            reader
                .read_retained_path(owner, 8, lease.lease_id, anchor.hash, 1)
                .expect("a mismatched session is non-fatal"),
            RetainedPathReadOutcome::Unavailable
        );
        let RetainedPathReadOutcome::Page(page) = reader
            .read_retained_path(owner, 7, lease.lease_id, anchor.hash, 1)
            .expect("the lease page is readable")
        else {
            panic!("the current owner should read its lease");
        };
        assert_eq!(page.nodes.len(), 1);
        assert_eq!(page.nodes[0].hash, child.hash);
        assert_eq!(page.common_ancestor, anchor_frontier);
        assert_eq!(page.aux_deliveries, vec![vec![delivery]]);
        assert!(!page.complete);
        let RetainedPathReadOutcome::Page(continuation) = reader
            .read_retained_path(owner, 7, lease.lease_id, child.hash, 1)
            .expect("the continuation page is readable")
        else {
            panic!("the current owner should read its continuation");
        };
        assert_eq!(
            continuation.common_ancestor,
            Frontier::new(child.height, child.hash)
        );
        assert_eq!(continuation.nodes[0].hash, grandchild.hash);
        assert!(continuation.complete);

        let before = runtime.publisher().snapshot();
        runtime
            .apply(
                TransitionRequest {
                    expected_version: before.state_version,
                    event: TransitionEvent::OperatorInvalidate(
                        zakura_header_chain::OperatorInvalidate {
                            target: child.hash,
                            id: zakura_header_chain::OperatorInvalidationId::new([3; 16]),
                            operator_reason_digest: [4; 32],
                            evidence: EvidenceId::from_digest([3; 32]),
                        },
                    ),
                },
                &TransitionContext {
                    config: &engine_config,
                    clock: &SystemClock,
                    full_state_authority: None,
                    startup_capability: None,
                    retention_references: &[],
                },
            )
            .expect("the selected path can change while the lease is active");
        assert_eq!(
            runtime.publisher().snapshot().frontiers.header_best,
            anchor_frontier
        );
        let RetainedPathReadOutcome::Page(page_after_reselection) = reader
            .read_retained_path(owner, 7, lease.lease_id, anchor.hash, 1)
            .expect("the immutable lease survives reselection")
        else {
            panic!("the lease remains available after reselection");
        };
        assert_eq!(page_after_reselection.nodes[0].hash, child.hash);

        assert_eq!(
            reader
                .acquire_retained_path(
                    SourceId::from_digest([2; 32]),
                    7,
                    block::Hash([0xfe; 32]),
                    &[anchor.hash],
                )
                .expect("an absent target is a normal outcome"),
            RetainedPathLeaseOutcome::TargetNotRetained
        );
        assert_eq!(
            reader
                .acquire_retained_path(
                    SourceId::from_digest([2; 32]),
                    7,
                    child.hash,
                    &[block::Hash([0xfd; 32])],
                )
                .expect("a disjoint locator is a normal outcome"),
            RetainedPathLeaseOutcome::NoLocatorIntersection
        );
        let RetainedPathLeaseOutcome::Acquired(target_intersection) = reader
            .acquire_retained_path(
                SourceId::from_digest([2; 32]),
                7,
                child.hash,
                &[child.hash, anchor.hash],
            )
            .expect("the first requester-order intersection is selected")
        else {
            panic!("the target itself intersects the locator");
        };
        assert_eq!(target_intersection.common_ancestor.hash, child.hash);
        assert!(target_intersection.path.is_empty());
        assert!(reader
            .release_retained_path(
                SourceId::from_digest([2; 32]),
                7,
                target_intersection.lease_id,
            )
            .expect("the requester-order test lease releases"));

        assert!(reader
            .release_retained_path(owner, 7, lease.lease_id)
            .expect("the exact owner can release its lease"));
        for marker in 1..=MAX_RETAINED_PATH_LEASES {
            let marker = u8::try_from(marker).expect("the lease cap fits in one byte");
            assert!(matches!(
                reader
                    .acquire_retained_path(
                        SourceId::from_digest([marker; 32]),
                        9,
                        child.hash,
                        &[anchor.hash],
                    )
                    .expect("bounded acquisition returns an outcome"),
                RetainedPathLeaseOutcome::Acquired(_)
            ));
        }
        assert_eq!(
            reader
                .acquire_retained_path(
                    SourceId::from_digest([0xff; 32]),
                    9,
                    child.hash,
                    &[anchor.hash],
                )
                .expect("capacity refusal is a normal outcome"),
            RetainedPathLeaseOutcome::Busy
        );
        let active_references = runtime
            .leases
            .lock()
            .expect("the lease registry mutex is not poisoned")
            .active_references(Instant::now());
        assert!(active_references.contains(&anchor.hash));
        assert!(active_references.contains(&child.hash));

        tokio::time::advance(RETAINED_PATH_LEASE_IDLE + Duration::from_secs(1)).await;
        assert!(runtime
            .leases
            .lock()
            .expect("the lease registry mutex is not poisoned")
            .active_references(Instant::now())
            .is_empty());
        assert!(matches!(
            reader
                .acquire_retained_path(
                    SourceId::from_digest([0xff; 32]),
                    10,
                    child.hash,
                    &[anchor.hash],
                )
                .expect("expired slots are reclaimed"),
            RetainedPathLeaseOutcome::Acquired(_)
        ));

        let snapshot = runtime.publisher().snapshot();
        let delivery = AuxDelivery {
            delivery_id: EvidenceId::from_digest([0xa1; 32]),
            header_hash: anchor.hash,
            source: SourceId::from_digest([0xa2; 32]),
            owner: zakura_header_chain::WorkOwner {
                state_version: snapshot.state_version,
                header_generation: snapshot.header_generation,
                verified_generation: Some(snapshot.verified_generation),
                branch: zakura_header_chain::BranchId::new(anchor.hash, anchor.hash),
                session_id: 11,
                request_id: std::num::NonZeroU64::new(12).expect("twelve is nonzero"),
            },
            body_size: zakura_header_chain::BodySizeHint::Unknown,
            tree_aux: None,
            authentication: zakura_header_chain::AuxAuthentication::Unauthenticated,
        };
        let mut corrupt = DiskWriteBatch::new();
        runtime
            .store
            .put_value(
                &mut corrupt,
                HEADER_AUX_DELIVERY,
                HeaderAuxDeliveryKey {
                    header: anchor.hash,
                    delivery: delivery.delivery_id,
                }
                .as_bytes(),
                &HeaderAuxDeliveryDisk(delivery),
            )
            .expect("the contradictory auxiliary row encodes");
        runtime
            .store
            .db
            .write(corrupt)
            .expect("the contradictory auxiliary row commits");
        assert!(matches!(
            reader.selected_aux_window(anchor.height, anchor.hash),
            Err(HeaderChainStoreError::Store(StoreError::Incoherent(
                "retained node and auxiliary delivery index disagree"
            )))
        ));
    }

    #[test]
    fn startup_reconciles_restored_full_state_before_first_publication() {
        let cache = tempfile::tempdir().expect("the test cache directory is created");
        let db_config = Config {
            cache_dir: cache.path().to_owned(),
            ephemeral: false,
            debug_skip_non_finalized_state_backup_task: true,
            ..Config::default()
        };
        let (engine_config, anchor, metadata) = fixture();
        let anchor_frontier = Frontier::new(anchor.height, anchor.hash);
        let db = open(&db_config, &engine_config.network);
        let store = HeaderChainStore::new(db.clone());
        store
            .initialize(metadata, anchor.clone())
            .expect("the header schema initializes");

        let mut child_header = *anchor.header;
        child_header.previous_block_hash = anchor.hash;
        let child_header = Arc::new(child_header);
        let child = VerifiedHeaderRef {
            height: anchor
                .height
                .next()
                .expect("genesis has a successor height"),
            hash: child_header.hash(),
            header: child_header,
        };
        let (runtime, report) = store
            .startup_reconciled(
                &engine_config,
                anchor_frontier,
                Vec::new(),
                vec![child.clone()],
            )
            .expect("restored full state reconciles before publication");

        assert!(report.publication_allowed);
        assert_eq!(
            runtime.publisher().snapshot().frontiers.verified_best,
            Frontier::new(child.height, child.hash)
        );
        assert_eq!(
            runtime
                .verified_projection()
                .expect("projection is readable"),
            vec![anchor_frontier, Frontier::new(child.height, child.hash)]
        );
        assert!(matches!(
            runtime.store.node(child.hash),
            Ok(Some(HeaderNode {
                body: BodyValidationState::Verified { .. },
                ..
            }))
        ));

        drop(runtime);
        let reopened = HeaderChainStore::new(db.clone())
            .startup_reconciled(&engine_config, anchor_frontier, Vec::new(), Vec::new())
            .expect("a restart resets a committed-but-unrestored verified suffix")
            .0;
        assert_eq!(
            reopened.publisher().snapshot().frontiers.verified_best,
            anchor_frontier
        );
        assert_eq!(
            reopened
                .verified_projection()
                .expect("reset projection is readable"),
            vec![anchor_frontier]
        );

        drop(reopened);
        let finalized_child = Frontier::new(child.height, child.hash);
        let advanced = HeaderChainStore::new(db)
            .startup_reconciled(&engine_config, finalized_child, vec![child], Vec::new())
            .expect("a dark checkpoint gap is reconciled and finalized before publication")
            .0;
        let snapshot = advanced.publisher().snapshot();
        assert_eq!(snapshot.frontiers.finalized, finalized_child);
        assert_eq!(snapshot.frontiers.verified_best, finalized_child);
        assert_eq!(
            advanced
                .verified_projection()
                .expect("advanced projection is readable"),
            vec![finalized_child]
        );
    }

    #[test]
    fn migrated_headers_only_pin_refutation_is_durable_and_fail_closed() {
        let cache = tempfile::tempdir().expect("the test cache directory is created");
        let db_config = Config {
            cache_dir: cache.path().to_owned(),
            ephemeral: false,
            debug_skip_non_finalized_state_backup_task: true,
            ..Config::default()
        };
        let (integrated_config, anchor, mut metadata) = fixture();
        let mut headers_only_config = integrated_config.clone();
        headers_only_config.mode = EngineMode::HeadersOnly;
        metadata.mode = EngineMode::HeadersOnly;
        let anchor_frontier = Frontier::new(anchor.height, anchor.hash);
        let db = open(&db_config, &integrated_config.network);
        let store = HeaderChainStore::new(db.clone());
        store
            .initialize(metadata, anchor.clone())
            .expect("the headers-only schema initializes");
        let record = FinalityRecord {
            previous: anchor_frontier,
            current: anchor_frontier,
            source: FinalitySource::MigratedHeadersOnly,
            epoch: FinalityEpoch::new(0),
        };
        let mut batch = DiskWriteBatch::new();
        store
            .put_value(
                &mut batch,
                HEADER_FINALITY_HISTORY,
                HeaderFinalityKey(record.epoch).as_bytes(),
                &HeaderFinalityRecordDisk(record),
            )
            .expect("the finality record encodes");
        db.write(batch).expect("the headers-only record commits");
        audit_store(&store, &headers_only_config).expect("the source store is coherent");
        assert!(matches!(
            preserve_headers_only_pin(FinalityRecord {
                previous: anchor_frontier,
                current: Frontier::new(block::Height(1), block::Hash([89; 32])),
                source: FinalitySource::HeadersOnlyDepth {
                    selected_tip: Frontier::new(block::Height(1_001), block::Hash([90; 32])),
                },
                epoch: FinalityEpoch::new(1),
            })
            .source,
            FinalitySource::MigratedHeadersOnly
        ));
        assert!(matches!(
            store.clone().migrate_headers_only_to_integrated(
                &integrated_config,
                Frontier::new(anchor.height, block::Hash([99; 32])),
            ),
            Err(HeaderChainStoreError::Incoherent(
                "integrated migration requires full-state verification through the preserved pin"
            ))
        ));

        let (runtime, report) = store
            .migrate_headers_only_to_integrated(&integrated_config, anchor_frontier)
            .expect("the explicit mode migration succeeds before publication");
        assert_eq!(report.current.mode, EngineMode::Integrated);
        assert!(matches!(
            runtime.store.finality_history().as_deref(),
            Ok([FinalityRecord {
                source: FinalitySource::MigratedHeadersOnly,
                ..
            }])
        ));

        let evidence = EvidenceId::from_digest([77; 32]);
        let authority = Authority(evidence);
        let snapshot = runtime.publisher().snapshot();
        let context = TransitionContext {
            config: &integrated_config,
            clock: &SystemClock,
            full_state_authority: Some(&authority),
            startup_capability: None,
            retention_references: &[],
        };
        let result = runtime.apply(
            TransitionRequest {
                expected_version: snapshot.state_version,
                event: TransitionEvent::MigratedPinRefutation(
                    zakura_header_chain::MigratedPinRefutation {
                        full_state_transition_id: evidence,
                        pin: anchor_frontier,
                        invalid_header: anchor_frontier,
                        rule: BodyRuleId::new("migrated-pin-refutation"),
                    },
                ),
            },
            &context,
        );
        assert!(matches!(
            result,
            Err(HeaderChainStoreError::MigratedPinRefuted { pin }) if pin == anchor_frontier
        ));
        assert_eq!(runtime.publisher().snapshot(), snapshot);
        assert_eq!(
            runtime
                .store
                .metadata()
                .expect("incident metadata is readable")
                .alarms
                .migrated_pin_refuted,
            Some(anchor_frontier)
        );

        drop(runtime);
        assert!(matches!(
            HeaderChainStore::new(db).startup(&integrated_config),
            Err(HeaderChainStoreError::MigratedPinRefuted { pin }) if pin == anchor_frontier
        ));
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
        let (runtime, startup) = store
            .startup(&engine_config)
            .expect("the coherent store audits before publication");
        assert!(startup.repairs.is_empty());
        assert_eq!(runtime.publisher().snapshot(), metadata.snapshot());
        let mut subscriber = runtime.publisher().subscribe();

        let evidence = EvidenceId::from_digest([7; 32]);
        let authority = Authority(evidence);
        let availability = BodyUnavailableSummary {
            started_at: Utc
                .timestamp_opt(1_000, 0)
                .single()
                .expect("valid fixture time"),
            attempts: 10,
            suppliers: 2,
            supplier_set_digest: [0x22; 32],
            alarmed: true,
            next_probe_at: Utc
                .timestamp_opt(1_600, 0)
                .single()
                .expect("valid fixture time"),
        };
        let context = TransitionContext {
            config: &engine_config,
            clock: &SystemClock,
            full_state_authority: Some(&authority),
            startup_capability: None,
            retention_references: &[],
        };
        let request = TransitionRequest {
            expected_version: StateVersion::new(1),
            event: TransitionEvent::BodyEvidence(BodyEvidence::Transient(TransientBodyFailure {
                hash: anchor.hash,
                evidence,
                kind: TransientBodyFailureKind::Storage,
                availability,
            })),
        };
        let receipt = runtime
            .apply(request.clone(), &context)
            .expect("the transition commits");
        let ApplyResult::Committed(receipt) = receipt else {
            panic!("a new body evidence ID must commit");
        };
        assert_eq!(receipt.previous.state_version, StateVersion::new(1));
        assert_eq!(receipt.current.state_version, StateVersion::new(2));
        assert_eq!(receipt.durable_tx_id, 2);
        assert!(subscriber
            .has_changed()
            .expect("the publisher remains open"));
        assert_eq!(*subscriber.borrow_and_update(), receipt.current);
        assert_eq!(
            receipt.current.alarms.header_best_body_unavailable,
            Some(availability)
        );
        assert!(matches!(
            runtime.store.node(anchor.hash).expect("the node row decodes").expect("the anchor remains").body,
            BodyValidationState::Unavailable(summary)
                if summary == availability
        ));
        assert!(matches!(
            runtime.apply(request, &context).expect("idempotent replay succeeds"),
            ApplyResult::NoChange(receipt) if receipt.state_version == StateVersion::new(2)
        ));
        assert!(matches!(
            runtime
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

        drop(runtime);
        drop(db);
        let reopened = HeaderChainStore::new(open(&db_config, &network));
        let (reopened, report) = reopened
            .startup(&engine_config)
            .expect("the committed store reopens through exhaustive audit");
        assert_eq!(report.current, receipt.current);
        assert_eq!(reopened.publisher().snapshot(), receipt.current);
        assert!(matches!(
            reopened
                .store
                .node(anchor.hash)
                .expect("the reopened node row decodes")
                .expect("the reopened anchor exists")
                .body,
            BodyValidationState::Unavailable(summary)
                if summary == availability
        ));
        let verified_evidence = EvidenceId::from_digest([8; 32]);
        let verified_authority = Authority(verified_evidence);
        let verified_context = TransitionContext {
            config: &engine_config,
            clock: &SystemClock,
            full_state_authority: Some(&verified_authority),
            startup_capability: None,
            retention_references: &[],
        };
        let verified = reopened
            .apply(
                TransitionRequest {
                    expected_version: StateVersion::new(2),
                    event: TransitionEvent::BodyEvidence(BodyEvidence::Verified(
                        VerifiedBodyEvidence {
                            hash: anchor.hash,
                            evidence: verified_evidence,
                        },
                    )),
                },
                &verified_context,
            )
            .expect("verified body evidence clears persistent unavailability");
        let ApplyResult::Committed(verified) = verified else {
            panic!("new verified body evidence commits");
        };
        assert_eq!(
            verified.current.frontiers.header_best,
            receipt.current.frontiers.header_best
        );
        assert_eq!(verified.current.alarms.header_best_body_unavailable, None);
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

    #[test]
    fn prepared_full_state_swaps_only_after_combined_commit() {
        let db_config = Config::ephemeral();
        let (engine_config, anchor, metadata) = fixture();
        let store = HeaderChainStore::new(open(&db_config, &engine_config.network));
        store
            .initialize(metadata.clone(), anchor.clone())
            .expect("the empty schema initializes");
        let (runtime, _) = store
            .startup(&engine_config)
            .expect("the initial store audits");
        let evidence = EvidenceId::from_digest([0x44; 32]);
        let request = TransitionRequest {
            expected_version: metadata.state_version,
            event: TransitionEvent::BodyEvidence(BodyEvidence::Transient(TransientBodyFailure {
                hash: anchor.hash,
                evidence,
                kind: TransientBodyFailureKind::Storage,
                availability: BodyUnavailableSummary {
                    attempts: 1,
                    suppliers: 1,
                    alarmed: false,
                    ..Default::default()
                },
            })),
        };
        assert!(matches!(
            PreparedFullStateTransition::new(
                EvidenceId::from_digest([0x45; 32]),
                metadata.frontiers.verified_best,
                Vec::new(),
                NonFinalizedState::new(&engine_config.network),
                None,
                request.clone(),
            ),
            Err(PreparedFullStateTransitionError::IdentityMismatch)
        ));
        let verified_request = TransitionRequest {
            expected_version: metadata.state_version,
            event: TransitionEvent::VerifiedChainChanged(VerifiedChainChanged {
                full_state_transition_id: evidence,
                old_tip: metadata.frontiers.verified_best,
                new_path: Vec::new(),
                cause: VerifiedChangeCause::Reset,
            }),
        };
        assert!(matches!(
            PreparedFullStateTransition::new(
                evidence,
                Frontier::new(block::Height(1), block::Hash([0x55; 32])),
                Vec::new(),
                NonFinalizedState::new(&engine_config.network),
                None,
                verified_request,
            ),
            Err(PreparedFullStateTransitionError::VerifiedPathMismatch)
        ));

        let staged = NonFinalizedState::new(&engine_config.network);
        let mut live = NonFinalizedState::new(&Network::Mainnet);
        let prepared = PreparedFullStateTransition::new(
            evidence,
            metadata.frontiers.verified_best,
            Vec::new(),
            staged,
            None,
            request,
        )
        .expect("the duplicated staged facts agree");
        let context = TransitionContext {
            config: &engine_config,
            clock: &SystemClock,
            full_state_authority: None,
            startup_capability: None,
            retention_references: &[],
        };
        let result = prepared
            .commit(&runtime, &mut live, &context)
            .expect("the staged mutation commits");
        let ApplyResult::Committed(receipt) = result else {
            panic!("new staged evidence must commit");
        };
        assert_eq!(live.network, engine_config.network);
        assert_eq!(runtime.publisher().snapshot(), receipt.current);
        assert_eq!(
            runtime
                .store
                .snapshot()
                .expect("the combined commit is durable"),
            receipt.current
        );
    }

    #[test]
    fn no_change_header_plan_still_commits_full_state_then_swaps_without_publication() {
        let db_config = Config::ephemeral();
        let (engine_config, anchor, metadata) = fixture();
        let store = HeaderChainStore::new(open(&db_config, &engine_config.network));
        store
            .initialize(metadata.clone(), anchor.clone())
            .expect("the empty schema initializes");
        let (runtime, _) = store
            .startup(&engine_config)
            .expect("the initial store audits");
        let evidence = EvidenceId::from_digest([0x61; 32]);
        let request = TransitionRequest {
            expected_version: metadata.state_version,
            event: TransitionEvent::OperatorReconsider(zakura_header_chain::OperatorReconsider {
                target: anchor.hash,
                id: zakura_header_chain::OperatorInvalidationId::new([0x62; 16]),
                evidence,
            }),
        };
        let marker_key = [0x63; 4];
        let mut full_state_batch = DiskWriteBatch::new();
        runtime
            .store
            .put_raw(
                &mut full_state_batch,
                ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT,
                marker_key,
                [0x64],
            )
            .expect("the full-state marker stages");
        let mut live = NonFinalizedState::new(&Network::Mainnet);
        let prepared = PreparedFullStateTransition::new(
            evidence,
            metadata.frontiers.verified_best,
            Vec::new(),
            NonFinalizedState::new(&engine_config.network),
            Some(full_state_batch),
            request,
        )
        .expect("the no-change header evidence is coherent");
        let result = prepared
            .commit(
                &runtime,
                &mut live,
                &TransitionContext {
                    config: &engine_config,
                    clock: &SystemClock,
                    full_state_authority: None,
                    startup_capability: None,
                    retention_references: &[],
                },
            )
            .expect("the full-state-only mutation commits");

        assert!(matches!(result, ApplyResult::NoChange(_)));
        assert_eq!(live.network, engine_config.network);
        assert_eq!(runtime.publisher().snapshot(), metadata.snapshot());
        let marker_cf = runtime
            .store
            .cf(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT)
            .expect("the marker column is open");
        assert_eq!(
            runtime
                .store
                .db
                .raw_get_cf(&marker_cf, &marker_key)
                .expect("the committed marker reads"),
            Some(vec![0x64])
        );
    }

    #[test]
    fn mismatched_staged_frontier_writes_and_swaps_nothing() {
        let db_config = Config::ephemeral();
        let (engine_config, anchor, metadata) = fixture();
        let store = HeaderChainStore::new(open(&db_config, &engine_config.network));
        store
            .initialize(metadata.clone(), anchor.clone())
            .expect("the empty schema initializes");
        let (runtime, _) = store
            .startup(&engine_config)
            .expect("the initial store audits");
        let marker_key = [0x71; 4];
        let mut full_state_batch = DiskWriteBatch::new();
        runtime
            .store
            .put_raw(
                &mut full_state_batch,
                ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT,
                marker_key,
                [0x72],
            )
            .expect("the full-state marker stages");
        let swapped = AtomicBool::new(false);
        let expected = Frontier::new(block::Height(1), anchor.hash);
        let error = runtime
            .apply_combined_expected(
                TransitionRequest {
                    expected_version: metadata.state_version,
                    event: TransitionEvent::OperatorReconsider(
                        zakura_header_chain::OperatorReconsider {
                            target: anchor.hash,
                            id: zakura_header_chain::OperatorInvalidationId::new([0x73; 16]),
                            evidence: EvidenceId::from_digest([0x74; 32]),
                        },
                    ),
                },
                &TransitionContext {
                    config: &engine_config,
                    clock: &SystemClock,
                    full_state_authority: None,
                    startup_capability: None,
                    retention_references: &[],
                },
                full_state_batch,
                expected,
                || swapped.store(true, Ordering::SeqCst),
            )
            .expect_err("a mismatched full-state frontier fails before mutation");

        assert!(matches!(
            error,
            HeaderChainStoreError::VerifiedFrontierMismatch {
                expected: error_expected,
                actual,
            } if error_expected == expected && actual == metadata.frontiers.verified_best
        ));
        assert!(!swapped.load(Ordering::SeqCst));
        assert_eq!(runtime.publisher().snapshot(), metadata.snapshot());
        let marker_cf = runtime
            .store
            .cf(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT)
            .expect("the marker column is open");
        assert_eq!(
            runtime
                .store
                .db
                .raw_get_cf(&marker_cf, &marker_key)
                .expect("the absent marker reads"),
            None
        );
    }

    #[test]
    fn startup_repairs_every_reconstructible_index_atomically_before_publication() {
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
        store
            .initialize(metadata.clone(), anchor.clone())
            .expect("the empty schema initializes");
        let mut corrupt = DiskWriteBatch::new();
        let bogus_parent = block::Hash([0x11; 32]);
        let bogus_child = block::Hash([0x22; 32]);
        let mut child_header = *anchor.header;
        child_header.previous_block_hash = anchor.hash;
        let child_hash = child_header.hash();
        let child_eligibility = zakura_header_chain::EligibilityState {
            inherited_from: Some(bogus_parent),
            ..Default::default()
        };
        let child = HeaderNode::from_durable_parts(
            Arc::new(child_header),
            child_hash,
            anchor.hash,
            block::Height(1),
            anchor.block_work,
            anchor
                .work_coordinate()
                .checked_add(anchor.block_work)
                .expect("the fixture work coordinate does not overflow"),
            HeaderValidationState::Valid,
            child_eligibility,
            BodyValidationState::Unknown,
            Vec::new(),
        )
        .expect("the child fixture is internally coherent");
        store
            .put_value(
                &mut corrupt,
                HEADER_NODE_BY_HASH,
                child.hash.0,
                &HeaderNodeDisk::from_domain(&child),
            )
            .expect("the child source row encodes");
        store
            .put_empty(
                &mut corrupt,
                HEADER_CHILD,
                HeaderChildKey {
                    parent: bogus_parent,
                    child: bogus_child,
                }
                .as_bytes(),
            )
            .expect("the child cache accepts the fixture row");
        store
            .delete_raw(
                &mut corrupt,
                HEADER_HEIGHT_HASH,
                HeaderHeightHashKey {
                    height: anchor.height,
                    hash: anchor.hash,
                }
                .as_bytes(),
            )
            .expect("the height cache row is addressable");
        store
            .delete_raw(
                &mut corrupt,
                HEADER_SELECTED,
                HeaderHeightKey(anchor.height).as_bytes(),
            )
            .expect("the selected cache row is addressable");
        store
            .delete_raw(
                &mut corrupt,
                HEADER_VERIFIED,
                HeaderHeightKey(anchor.height).as_bytes(),
            )
            .expect("the verified cache row is addressable");
        store
            .delete_raw(
                &mut corrupt,
                HEADER_CANDIDATE,
                HeaderCandidateKey(metadata.header_best_score).as_bytes(),
            )
            .expect("the candidate cache row is addressable");
        store
            .put_empty(
                &mut corrupt,
                HEADER_DEFERRED,
                HeaderDeferredKey::new(1, 0, bogus_child)
                    .expect("the fixture timestamp is valid")
                    .as_bytes(),
            )
            .expect("the deferred cache accepts the fixture row");
        let mut corrupt_metadata = metadata.clone();
        corrupt_metadata.oldest_retained_height = block::Height(1);
        store
            .put_value(
                &mut corrupt,
                HEADER_ENGINE_META,
                METADATA_KEY,
                &HeaderEngineMetadataDisk(corrupt_metadata),
            )
            .expect("the fixture metadata encodes");
        db.write(corrupt)
            .expect("the fixture cache corruption is durable");

        let (runtime, report) = store
            .startup(&engine_config)
            .expect("a reconstructible cache is repaired");
        assert_eq!(
            report.repairs,
            BTreeSet::from([
                RecoveryRepair::ChildIndex,
                RecoveryRepair::HeightIndex,
                RecoveryRepair::DeferredIndex,
                RecoveryRepair::CandidateIndex,
                RecoveryRepair::SelectedProjection,
                RecoveryRepair::VerifiedProjection,
                RecoveryRepair::InheritedEligibility,
                RecoveryRepair::RetentionMetadata,
            ])
        );
        assert_eq!(report.previous.state_version, StateVersion::new(1));
        assert_eq!(report.current.state_version, StateVersion::new(2));
        assert_eq!(report.current.header_generation, HeaderGeneration::new(2));
        assert_eq!(
            report.current.verified_generation,
            VerifiedGeneration::new(2)
        );
        assert_eq!(report.current.oldest_retained_height, anchor.height);
        assert!(report.publication_allowed);
        assert_eq!(runtime.publisher().snapshot(), report.current);
        assert_eq!(
            runtime.store.selected_hash(anchor.height),
            Ok(Some(anchor.hash))
        );
        assert_eq!(
            runtime.store.selected_hash(child.height),
            Ok(Some(child.hash))
        );
        assert_eq!(
            runtime.store.verified_hash(anchor.height),
            Ok(Some(anchor.hash))
        );
        assert_eq!(
            runtime.store.child_edges(),
            Ok(vec![(anchor.hash, child.hash)])
        );
        assert_eq!(
            runtime.store.height_entries(),
            Ok(vec![
                Frontier::new(anchor.height, anchor.hash),
                Frontier::new(child.height, child.hash),
            ])
        );
        assert_eq!(runtime.store.deferred_entries(), Ok(Vec::new()));
        assert_eq!(
            runtime.store.candidate_tips(),
            Ok(vec![(report.current.header_best_score, child.hash)])
        );
        assert_eq!(
            runtime
                .store
                .node(child.hash)
                .expect("the repaired child decodes")
                .expect("the repaired child remains")
                .eligibility
                .inherited_from,
            None
        );

        drop(runtime);
        drop(db);
        let (reopened, reopened_report) = HeaderChainStore::new(open(&db_config, &network))
            .startup(&engine_config)
            .expect("the atomic repair reopens coherently");
        assert!(reopened_report.repairs.is_empty());
        assert_eq!(reopened.publisher().snapshot(), report.current);
    }

    #[test]
    fn authoritative_corruption_fails_before_publisher_construction() {
        let db_config = Config::ephemeral();
        let (engine_config, anchor, metadata) = fixture();
        let store = HeaderChainStore::new(open(&db_config, &engine_config.network));
        store
            .initialize(metadata, anchor.clone())
            .expect("the empty schema initializes");
        let mut corrupt = DiskWriteBatch::new();
        store
            .delete_raw(&mut corrupt, HEADER_NODE_BY_HASH, anchor.hash.0)
            .expect("the anchor row is addressable");
        store
            .db
            .write(corrupt)
            .expect("the fixture source corruption is durable");

        assert!(matches!(
            store.startup(&engine_config),
            Err(HeaderChainStoreError::Recovery(
                RecoveryFailure::Source { .. }
            ))
        ));
    }

    #[test]
    fn aud_14_every_state_writer_crash_point_reopens_complete_before_or_after() {
        for (index, target) in FaultPoint::ALL.into_iter().enumerate() {
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
            store
                .initialize(metadata.clone(), anchor.clone())
                .expect("the empty schema initializes");
            let (runtime, _) = store
                .startup(&engine_config)
                .expect("the initial store audits");
            let marker = u8::try_from(index + 1).expect("the fault-point list fits in u8");
            let evidence = EvidenceId::from_digest([marker; 32]);
            let authority = Authority(evidence);
            let context = TransitionContext {
                config: &engine_config,
                clock: &SystemClock,
                full_state_authority: Some(&authority),
                startup_capability: None,
                retention_references: &[],
            };
            let request = TransitionRequest {
                expected_version: StateVersion::new(1),
                event: TransitionEvent::BodyEvidence(BodyEvidence::Transient(
                    TransientBodyFailure {
                        hash: anchor.hash,
                        evidence,
                        kind: TransientBodyFailureKind::Storage,
                        availability: BodyUnavailableSummary {
                            attempts: 1,
                            suppliers: 1,
                            alarmed: false,
                            ..Default::default()
                        },
                    },
                )),
            };
            let marker_key = [marker; 4];
            let mut full_state_batch = DiskWriteBatch::new();
            runtime
                .store
                .put_raw(
                    &mut full_state_batch,
                    ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT,
                    marker_key,
                    [marker],
                )
                .expect("the combined full-state marker can be staged");
            let memory_swapped = Arc::new(AtomicBool::new(false));
            let swap_probe = memory_swapped.clone();
            let result = runtime.apply_combined_with_fault(
                request,
                &context,
                full_state_batch,
                move || swap_probe.store(true, Ordering::SeqCst),
                |point| {
                    if point == target {
                        Err(HeaderChainStoreError::InjectedCrash(point))
                    } else {
                        Ok(())
                    }
                },
            );
            assert!(matches!(
                result,
                Err(HeaderChainStoreError::InjectedCrash(point)) if point == target
            ));
            let committed = matches!(
                target,
                FaultPoint::AfterDbCommit
                    | FaultPoint::BeforeMemorySwap
                    | FaultPoint::BeforePublish
                    | FaultPoint::AfterPublish
                    | FaultPoint::BeforeReactorObserve
            );
            let marker_cf = runtime
                .store
                .cf(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT)
                .expect("the marker column family is open");
            assert_eq!(
                runtime
                    .store
                    .db
                    .raw_get_cf(&marker_cf, &marker_key)
                    .expect("the combined marker read succeeds")
                    .is_some(),
                committed,
                "{target:?}"
            );
            let swap_completed = matches!(
                target,
                FaultPoint::BeforePublish
                    | FaultPoint::AfterPublish
                    | FaultPoint::BeforeReactorObserve
            );
            assert_eq!(
                memory_swapped.load(Ordering::SeqCst),
                swap_completed,
                "{target:?}"
            );
            let published = runtime.publisher().snapshot().state_version;
            let publish_completed = matches!(
                target,
                FaultPoint::AfterPublish | FaultPoint::BeforeReactorObserve
            );
            assert_eq!(
                published,
                if publish_completed {
                    StateVersion::new(2)
                } else {
                    StateVersion::new(1)
                },
                "{target:?}"
            );
            if publish_completed {
                assert_eq!(
                    runtime
                        .store
                        .snapshot()
                        .expect("published state is durable"),
                    runtime.publisher().snapshot(),
                    "{target:?}"
                );
            }
            drop(runtime);
            drop(db);

            let (reopened, report) = HeaderChainStore::new(open(&db_config, &network))
                .startup(&engine_config)
                .expect("the crash boundary reopens to a coherent transaction");
            let expected = if committed {
                StateVersion::new(2)
            } else {
                StateVersion::new(1)
            };
            assert_eq!(report.current.state_version, expected, "{target:?}");
            assert_eq!(
                reopened.publisher().snapshot(),
                report.current,
                "{target:?}"
            );
            assert_eq!(
                reopened.store.snapshot().expect("the snapshot reopens"),
                report.current,
                "{target:?}"
            );
        }
    }
}
