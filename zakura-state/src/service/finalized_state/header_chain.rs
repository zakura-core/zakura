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

#[cfg(test)]
#[path = "header_chain/coherence.rs"]
mod coherence;
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
        scope: zakura_header_chain::WorkScope,
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
            scope,
            idle_deadline: now + RETAINED_PATH_LEASE_IDLE,
        };
        self.by_peer.insert(peer, lease.clone());
        RetainedPathLeaseOutcome::Acquired(Box::new(lease))
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

    fn release(
        &mut self,
        peer: SourceId,
        session_id: u64,
        lease_id: u64,
        scope: zakura_header_chain::WorkScope,
    ) -> bool {
        let matches = self.by_peer.get(&peer).is_some_and(|lease| {
            lease.session_id == session_id && lease.lease_id == lease_id && lease.scope == scope
        });
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
    fn coherent_selected_node(
        &self,
        height: block::Height,
    ) -> Result<Option<HeaderNode>, StoreError> {
        let Some(hash) = self.store.selected_hash(height)? else {
            let snapshot = self.store.snapshot()?;
            if height >= snapshot.frontiers.finalized.height
                && height <= snapshot.frontiers.header_best.height
            {
                return Err(StoreError::Incoherent(
                    "selected projection has a gap within its published bounds",
                ));
            }
            return Ok(None);
        };
        let node = self.store.node(hash)?.ok_or(StoreError::Incoherent(
            "selected projection references a missing node",
        ))?;
        if node.height != height {
            return Err(StoreError::Incoherent(
                "selected projection node height disagrees with its index",
            ));
        }
        Ok(Some(node))
    }

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
            let Some(node) = self.coherent_selected_node(height)? else {
                break;
            };
            let hash = node.hash;
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
        self.coherent_selected_node(height)
            .map(|node| node.map(|node| node.hash))
            .map_err(HeaderChainStoreError::Store)
    }

    pub(crate) fn selected_successor(
        &self,
        height: block::Height,
        hash: block::Hash,
    ) -> Result<Option<HeaderNode>, HeaderChainStoreError> {
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        if self
            .coherent_selected_node(height)?
            .is_none_or(|node| node.hash != hash)
        {
            return Ok(None);
        }
        let Ok(successor_height) = height.next() else {
            return Ok(None);
        };
        let Some(successor) = self.coherent_selected_node(successor_height)? else {
            return Ok(None);
        };
        if successor.parent_hash != hash {
            return Err(StoreError::Incoherent(
                "selected successor does not extend its selected predecessor",
            )
            .into());
        }
        Ok(Some(successor))
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
        let Some(current) = self.coherent_selected_node(height)? else {
            return Ok(None);
        };
        if current.hash != hash {
            return Ok(None);
        }
        let current_deliveries = self.coherent_aux_deliveries(&current)?;
        let successor = match height.next() {
            Ok(successor_height) => match self.coherent_selected_node(successor_height)? {
                Some(successor) => {
                    if successor.parent_hash != hash {
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
        HeaderLocator::for_selected_path(&snapshot, |height| {
            self.coherent_selected_node(height)
                .map(|node| node.map(|node| node.hash))
        })
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
        let Some(target) = self.coherent_selected_node(height)? else {
            return Err(StoreError::Incoherent(
                "VCT repair height is absent from the selected projection",
            )
            .into());
        };
        let target_hash = target.hash;
        let parent_height = block::Height(height.0.checked_sub(1).ok_or(
            StoreError::Incoherent("non-finalized VCT repair header has no predecessor height"),
        )?);
        if self
            .coherent_selected_node(parent_height)?
            .map(|node| node.hash)
            != Some(target.parent_hash)
        {
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
        scope: zakura_header_chain::WorkScope,
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
        if scope != zakura_header_chain::WorkScope::for_header_target(&snapshot, target_tip_hash) {
            return Ok(RetainedPathLeaseOutcome::Busy);
        }
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
            scope,
            Instant::now(),
        ))
    }

    pub(crate) fn read_retained_path(
        &self,
        peer: SourceId,
        session_id: u64,
        lease_id: u64,
        scope: zakura_header_chain::WorkScope,
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
        if lease.scope != scope {
            return Ok(RetainedPathReadOutcome::Unavailable);
        }
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
        Ok(RetainedPathReadOutcome::Page(Box::new(RetainedPathPage {
            lease_id,
            common_ancestor: page_ancestor,
            target: lease.target,
            scope: lease.scope,
            nodes,
            aux_deliveries,
            complete: end == lease.path.len(),
        })))
    }

    pub(crate) fn release_retained_path(
        &self,
        peer: SourceId,
        session_id: u64,
        lease_id: u64,
        scope: zakura_header_chain::WorkScope,
    ) -> Result<bool, HeaderChainStoreError> {
        Ok(self
            .leases
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?
            .release(peer, session_id, lease_id, scope))
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

    /// Ordered crash surface reached by a transition with no header-chain changes.
    pub const NO_CHANGE: [Self; 6] = [
        Self::AfterSnapshot,
        Self::AfterVersionCheck,
        Self::BeforeDbCommit,
        Self::AfterDbCommit,
        Self::BeforeMemorySwap,
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
        self.startup_with_fault(config, |_| Ok(()))
    }

    fn startup_with_fault<F>(
        self,
        config: &EngineConfig,
        mut fault: F,
    ) -> Result<(HeaderChainRuntime, StartupReport), HeaderChainStoreError>
    where
        F: FnMut(FaultPoint) -> Result<(), HeaderChainStoreError>,
    {
        let writer = self
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        let plan = audit_store(&self, config)?;
        if let Some(pin) = plan.metadata.alarms.migrated_pin_refuted {
            return Err(HeaderChainStoreError::MigratedPinRefuted { pin });
        }
        fault(FaultPoint::AfterSnapshot)?;
        let previous = plan.before.clone();
        let repairs = plan.repairs.clone();
        if !plan.is_clean() {
            fault(FaultPoint::BeforeDbCommit)?;
            self.db.write(self.recovery_batch(&plan)?)?;
            fault(FaultPoint::AfterDbCommit)?;
        }
        let current = plan.metadata.snapshot();
        let report = StartupReport {
            previous,
            current: current.clone(),
            repairs,
            publication_allowed: true,
        };
        fault(FaultPoint::BeforePublish)?;
        let publisher = Publisher::new(current);
        fault(FaultPoint::AfterPublish)?;
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
        if self.metadata_row()?.is_some_and(|metadata| {
            metadata.frontiers.finalized != changes.metadata.frontiers.finalized
        }) {
            let staged_nodes: HashMap<_, _> = changes
                .put_nodes
                .iter()
                .map(|node| (node.hash, node))
                .collect();
            let contexts = authenticated_context_headers(
                self,
                changes.metadata.frontiers.finalized.hash,
                Some(&staged_nodes),
            )?;
            for (key, _) in self.scan_raw(HEADER_VALIDATION_CONTEXT)? {
                self.delete_raw(&mut batch, HEADER_VALIDATION_CONTEXT, key)?;
            }
            for context in contexts {
                self.put_value(
                    &mut batch,
                    HEADER_VALIDATION_CONTEXT,
                    context.header.hash().0,
                    &context,
                )?;
            }
        }

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
        let mut predecessors = vec![zakura_header_chain::HeaderContextFact {
            frontier: parent_frontier,
            difficulty_threshold: parent_node.header.difficulty_threshold,
            time: parent_node.header.time,
        }];
        predecessors.extend(
            authenticated_context_headers(self, parent, None)?
                .into_iter()
                .rev()
                .map(|context| context.fact()),
        );
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

fn authenticated_context_headers(
    store: &HeaderChainStore,
    parent: block::Hash,
    staged_nodes: Option<&HashMap<block::Hash, &HeaderNode>>,
) -> Result<Vec<HeaderValidationContextDisk>, StoreError> {
    let staged_parent = staged_nodes.and_then(|nodes| nodes.get(&parent).copied());
    let stored_parent = if staged_parent.is_none() {
        store.node(parent)?
    } else {
        None
    };
    let parent_node = staged_parent
        .or(stored_parent.as_ref())
        .ok_or(StoreError::Incoherent("validation parent is not retained"))?;
    let required = usize::try_from(parent_node.height.0.min(27))
        .map_err(|_| StoreError::Incoherent("validation context bound does not fit in usize"))?;
    let mut contexts = Vec::with_capacity(required);
    let mut current_hash = parent_node.parent_hash;
    let mut expected_height = parent_node.height;
    for _ in 0..required {
        expected_height = expected_height
            .previous()
            .map_err(|_| StoreError::Incoherent("validation context height underflow"))?;
        let staged_node = staged_nodes.and_then(|nodes| nodes.get(&current_hash).copied());
        let stored_node = if staged_node.is_none() {
            store.node(current_hash)?
        } else {
            None
        };
        let context = if let Some(node) = staged_node.or(stored_node.as_ref()) {
            HeaderValidationContextDisk {
                header: node.header.clone(),
                height: node.height,
            }
        } else {
            store
                .get_value::<HeaderValidationContextDisk>(HEADER_VALIDATION_CONTEXT, current_hash.0)
                .map_err(store_error)?
                .ok_or(StoreError::Incoherent("validation context has a gap"))?
        };
        if context.header.hash() != current_hash || context.height != expected_height {
            return Err(StoreError::Incoherent(
                "invalid immutable validation context",
            ));
        }
        current_hash = context.header.previous_block_hash;
        contexts.push(context);
    }
    contexts.reverse();
    Ok(contexts)
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
        AlarmSet, BodyCommitmentKind, BodyEvidence, BodyPayloadMismatch, BodyRuleId,
        BodyUnavailableSummary, BodyValidationState, BranchId, CheckpointSet, EligibilityReason,
        EngineConfig, EngineMode, FinalityEpoch, FrontierSet, FullStateEvidenceAuthority,
        HeaderBatchInput, HeaderChainDiskVersion, HeaderGeneration, HeaderRules,
        HeaderValidationState, InsertHeaders, OperatorInvalidate, OperatorInvalidationId,
        OperatorReconsider, SourceId, StateVersion, SuffixWork, SystemClock, TargetCompletion,
        TransientBodyFailure, TransientBodyFailureKind, TransitionEvent, TrustedAnchor,
        VerifiedBodyEvidence, VerifiedChainChanged, VerifiedChangeCause, VerifiedGeneration,
        WorkCoordinate,
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
    fn atomic_finality_context_can_use_a_newly_staged_anchor_path() {
        let db_config = Config::ephemeral();
        let (engine_config, anchor, metadata) = fixture();
        let store = HeaderChainStore::new(open(&db_config, &engine_config.network));
        store
            .initialize(metadata, anchor.clone())
            .expect("the empty schema initializes");

        let mut nodes = Vec::new();
        let mut parent = anchor;
        for height in 1..=28 {
            let mut header = *parent.header;
            header.previous_block_hash = parent.hash;
            header.time += chrono::Duration::seconds(1);
            header.nonce.0[0] =
                u8::try_from(height).expect("the staged test path is shorter than 256");
            let header = Arc::new(header);
            let hash = header.hash();
            let node = HeaderNode::from_durable_parts(
                header,
                hash,
                parent.hash,
                block::Height(height),
                parent.block_work,
                parent
                    .work_coordinate()
                    .checked_add(parent.block_work)
                    .expect("the short staged path cannot exhaust cumulative work"),
                HeaderValidationState::Valid,
                Default::default(),
                BodyValidationState::Unknown,
                Vec::new(),
            )
            .expect("the staged node fields are coherent");
            parent = node.clone();
            nodes.push(node);
        }
        let staged: HashMap<_, _> = nodes.iter().map(|node| (node.hash, node)).collect();
        let contexts = authenticated_context_headers(&store, parent.hash, Some(&staged))
            .expect("the atomic batch can authenticate context from its staged node overlay");
        assert_eq!(contexts.len(), 27);
        assert_eq!(
            contexts.first().map(|context| context.height),
            Some(block::Height(1))
        );
        assert_eq!(
            contexts.last().map(|context| context.height),
            Some(block::Height(27))
        );
        assert_eq!(
            parent.header.previous_block_hash,
            contexts
                .last()
                .expect("the context is nonempty")
                .header
                .hash()
        );
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
        let lease_scope = zakura_header_chain::WorkScope::for_header_target(
            &runtime.publisher().snapshot(),
            grandchild.hash,
        );
        let acquired = reader
            .acquire_retained_path(owner, 7, grandchild.hash, &[anchor.hash], lease_scope)
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
        assert_eq!(lease.scope, lease_scope);
        let mut wrong_scope = lease_scope;
        wrong_scope.header_generation = wrong_scope
            .header_generation
            .checked_next()
            .expect("the fixture generation has a successor");
        assert_eq!(
            reader
                .acquire_retained_path(
                    SourceId::from_digest([0xee; 32]),
                    7,
                    grandchild.hash,
                    &[anchor.hash],
                    wrong_scope,
                )
                .expect("a stale acquisition scope is a normal refusal"),
            RetainedPathLeaseOutcome::Busy
        );
        assert_eq!(
            reader
                .acquire_retained_path(owner, 7, grandchild.hash, &[anchor.hash], lease_scope,)
                .expect("the lease bound is a normal outcome"),
            RetainedPathLeaseOutcome::Busy
        );
        assert_eq!(
            reader
                .read_retained_path(owner, 8, lease.lease_id, lease_scope, anchor.hash, 1)
                .expect("a mismatched session is non-fatal"),
            RetainedPathReadOutcome::Unavailable
        );
        assert_eq!(
            reader
                .read_retained_path(owner, 7, lease.lease_id, wrong_scope, anchor.hash, 1)
                .expect("a mismatched branch scope is non-fatal"),
            RetainedPathReadOutcome::Unavailable
        );
        assert!(!reader
            .release_retained_path(owner, 7, lease.lease_id, wrong_scope)
            .expect("a mismatched release scope is non-fatal"));
        let RetainedPathReadOutcome::Page(page) = reader
            .read_retained_path(owner, 7, lease.lease_id, lease_scope, anchor.hash, 1)
            .expect("the lease page is readable")
        else {
            panic!("the current owner should read its lease");
        };
        assert_eq!(page.nodes.len(), 1);
        assert_eq!(page.nodes[0].hash, child.hash);
        assert_eq!(page.common_ancestor, anchor_frontier);
        assert_eq!(page.scope, lease_scope);
        assert_eq!(page.aux_deliveries, vec![vec![delivery]]);
        assert!(!page.complete);
        let RetainedPathReadOutcome::Page(continuation) = reader
            .read_retained_path(owner, 7, lease.lease_id, lease_scope, child.hash, 1)
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
                    retention_references: &[],
                },
            )
            .expect("the selected path can change while the lease is active");
        assert_eq!(
            runtime.publisher().snapshot().frontiers.header_best,
            anchor_frontier
        );
        let RetainedPathReadOutcome::Page(page_after_reselection) = reader
            .read_retained_path(owner, 7, lease.lease_id, lease_scope, anchor.hash, 1)
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
                    zakura_header_chain::WorkScope::for_header_target(
                        &runtime.publisher().snapshot(),
                        block::Hash([0xfe; 32]),
                    ),
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
                    zakura_header_chain::WorkScope::for_header_target(
                        &runtime.publisher().snapshot(),
                        child.hash,
                    ),
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
                zakura_header_chain::WorkScope::for_header_target(
                    &runtime.publisher().snapshot(),
                    child.hash,
                ),
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
                target_intersection.scope,
            )
            .expect("the requester-order test lease releases"));

        assert!(reader
            .release_retained_path(owner, 7, lease.lease_id, lease_scope)
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
                        zakura_header_chain::WorkScope::for_header_target(
                            &runtime.publisher().snapshot(),
                            child.hash,
                        ),
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
                    zakura_header_chain::WorkScope::for_header_target(
                        &runtime.publisher().snapshot(),
                        child.hash,
                    ),
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
                    zakura_header_chain::WorkScope::for_header_target(
                        &runtime.publisher().snapshot(),
                        child.hash,
                    ),
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
    fn aud_14_startup_recovery_reopens_complete_before_or_after_without_publication() {
        const STARTUP_FAULT_POINTS: [FaultPoint; 5] = [
            FaultPoint::AfterSnapshot,
            FaultPoint::BeforeDbCommit,
            FaultPoint::AfterDbCommit,
            FaultPoint::BeforePublish,
            FaultPoint::AfterPublish,
        ];

        for target in STARTUP_FAULT_POINTS {
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
            store
                .delete_raw(
                    &mut corrupt,
                    HEADER_SELECTED,
                    HeaderHeightKey(anchor.height).as_bytes(),
                )
                .expect("the selected projection row is addressable");
            db.write(corrupt)
                .expect("the reconstructible selected-index corruption is durable");
            assert_eq!(store.selected_hash(anchor.height), Ok(None));

            let observer = store.clone();
            let result = store.startup_with_fault(&engine_config, |point| {
                if point == target {
                    Err(HeaderChainStoreError::InjectedCrash(point))
                } else {
                    Ok(())
                }
            });
            assert!(matches!(
                result,
                Err(HeaderChainStoreError::InjectedCrash(point)) if point == target
            ));

            let committed = matches!(
                target,
                FaultPoint::AfterDbCommit | FaultPoint::BeforePublish | FaultPoint::AfterPublish
            );
            assert_eq!(
                observer.selected_hash(anchor.height),
                if committed {
                    Ok(Some(anchor.hash))
                } else {
                    Ok(None)
                },
                "{target:?}"
            );
            let durable = observer
                .metadata()
                .expect("the startup-recovery metadata is readable");
            assert_eq!(
                durable.state_version,
                if committed {
                    StateVersion::new(2)
                } else {
                    metadata.state_version
                },
                "{target:?}"
            );
            assert_eq!(
                durable.header_generation,
                if committed {
                    HeaderGeneration::new(2)
                } else {
                    metadata.header_generation
                },
                "{target:?}"
            );
            assert_eq!(
                durable.verified_generation, metadata.verified_generation,
                "{target:?}"
            );

            drop(db);
            let (reopened, report) = observer
                .startup(&engine_config)
                .expect("the interrupted startup recovery completes before publication");
            assert_eq!(
                report.repairs,
                if committed {
                    BTreeSet::new()
                } else {
                    BTreeSet::from([RecoveryRepair::SelectedProjection])
                },
                "{target:?}"
            );
            assert_eq!(
                report.current.state_version,
                StateVersion::new(2),
                "{target:?}"
            );
            assert_eq!(
                report.current.header_generation,
                HeaderGeneration::new(2),
                "{target:?}"
            );
            assert_eq!(
                report.current.verified_generation, metadata.verified_generation,
                "{target:?}"
            );
            assert_eq!(
                reopened.store.selected_hash(anchor.height),
                Ok(Some(anchor.hash)),
                "{target:?}"
            );
            assert_eq!(
                reopened.publisher().snapshot(),
                report.current,
                "{target:?}"
            );
        }
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

    #[test]
    fn aud_14_requester_insertion_reopens_complete_before_or_after() {
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
            let anchor_frontier = metadata.frontiers.finalized;
            let lease = runtime
                .reader()
                .validation_context(anchor.hash)
                .expect("the anchor validation context is coherent")
                .expect("the initialized anchor is retained");
            let rules = HeaderRules::for_validation_lease(network.clone(), &lease)
                .expect("the authenticated regtest policy is valid");
            let marker = u8::try_from(index + 0x20).expect("the fault-point list fits in u8");
            let mut child_header = *anchor.header;
            child_header.previous_block_hash = anchor.hash;
            child_header.time += chrono::Duration::seconds(1);
            child_header.nonce.0[0] = marker;
            let child_header = Arc::new(child_header);
            let headers = [child_header.clone()];
            let batch = zakura_header_chain::prepare_headers(
                HeaderBatchInput::new(&headers),
                &lease,
                &rules,
                &SystemClock,
            )
            .expect("the exact next child prepares through production validation");
            let child = Frontier::new(
                anchor_frontier
                    .height
                    .next()
                    .expect("the genesis anchor has a next height"),
                child_header.hash(),
            );
            let owner = WorkOwner {
                state_version: metadata.state_version,
                header_generation: metadata.header_generation,
                verified_generation: None,
                branch: BranchId::new(anchor.hash, child.hash),
                session_id: 1,
                request_id: NonZeroU64::new(1).expect("one is nonzero"),
            };
            let request = TransitionRequest {
                expected_version: metadata.state_version,
                event: TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
                    owner,
                    source: SourceId::from_digest([marker.wrapping_add(1); 32]),
                    parent_hash: anchor.hash,
                    target_tip_hash: child.hash,
                    completion: TargetCompletion::TargetComplete {
                        common_ancestor: anchor_frontier,
                    },
                    batch,
                    aux: Vec::new(),
                })),
            };
            let context = TransitionContext {
                config: &engine_config,
                clock: &SystemClock,
                full_state_authority: None,
                retention_references: &[],
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
                .expect("the paired full-state marker can be staged");
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
            assert_eq!(
                runtime
                    .store
                    .node(child.hash)
                    .expect("the child row read succeeds")
                    .is_some(),
                committed,
                "{target:?}"
            );
            assert_eq!(
                runtime
                    .store
                    .selected_hash(child.height)
                    .expect("the selected projection read succeeds"),
                committed.then_some(child.hash),
                "{target:?}"
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
                    .expect("the paired marker read succeeds")
                    .is_some(),
                committed,
                "{target:?}"
            );
            assert_eq!(
                memory_swapped.load(Ordering::SeqCst),
                matches!(
                    target,
                    FaultPoint::BeforePublish
                        | FaultPoint::AfterPublish
                        | FaultPoint::BeforeReactorObserve
                ),
                "{target:?}"
            );
            assert_eq!(
                runtime.publisher().snapshot().frontiers.header_best,
                if matches!(
                    target,
                    FaultPoint::AfterPublish | FaultPoint::BeforeReactorObserve
                ) {
                    child
                } else {
                    anchor_frontier
                },
                "{target:?}"
            );
            drop(runtime);
            drop(db);

            let (reopened, report) = HeaderChainStore::new(open(&db_config, &network))
                .startup(&engine_config)
                .expect("the requester crash boundary reopens coherently");
            assert_eq!(
                report.current.state_version,
                if committed {
                    StateVersion::new(2)
                } else {
                    StateVersion::new(1)
                },
                "{target:?}"
            );
            assert_eq!(
                report.current.frontiers.header_best,
                if committed { child } else { anchor_frontier },
                "{target:?}"
            );
            assert_eq!(
                reopened
                    .store
                    .node(child.hash)
                    .expect("the reopened child row read succeeds")
                    .is_some(),
                committed,
                "{target:?}"
            );
            assert_eq!(
                reopened.publisher().snapshot(),
                report.current,
                "{target:?}"
            );
            let reopened_marker_cf = reopened
                .store
                .cf(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT)
                .expect("the reopened marker column family is open");
            assert_eq!(
                reopened
                    .store
                    .db
                    .raw_get_cf(&reopened_marker_cf, &marker_key)
                    .expect("the reopened marker read succeeds")
                    .is_some(),
                committed,
                "{target:?}"
            );
        }
    }

    #[test]
    fn aud_14_finality_advance_reopens_complete_before_or_after() {
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
                .initialize(metadata, anchor.clone())
                .expect("the empty schema initializes");
            let anchor_frontier = Frontier::new(anchor.height, anchor.hash);
            let mut child_header = *anchor.header;
            child_header.previous_block_hash = anchor.hash;
            child_header.time += chrono::Duration::seconds(1);
            let child_header = Arc::new(child_header);
            let child = VerifiedHeaderRef {
                height: anchor
                    .height
                    .next()
                    .expect("the genesis anchor has a next height"),
                hash: child_header.hash(),
                header: child_header,
            };
            let mut grandchild_header = *child.header;
            grandchild_header.previous_block_hash = child.hash;
            grandchild_header.time += chrono::Duration::seconds(1);
            let grandchild_header = Arc::new(grandchild_header);
            let grandchild = VerifiedHeaderRef {
                height: child.height.next().expect("the child has a next height"),
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
                .expect("the verified suffix reconciles before the faulted finality transition");
            let before = runtime.publisher().snapshot();
            let new_finalized = Frontier::new(child.height, child.hash);
            let proof = runtime
                .verified_projection()
                .expect("the verified projection is readable")
                .into_iter()
                .take_while(|frontier| frontier.height <= new_finalized.height)
                .map(|frontier| frontier.hash)
                .collect::<Vec<_>>();
            let marker = u8::try_from(index + 0x60).expect("the fault-point list fits in u8");
            let evidence = EvidenceId::from_digest([marker; 32]);
            let authority = Authority(evidence);
            let context = TransitionContext {
                config: &engine_config,
                clock: &SystemClock,
                full_state_authority: Some(&authority),
                retention_references: &[],
            };
            let request = TransitionRequest {
                expected_version: before.state_version,
                event: TransitionEvent::FullStateFinalized(FullStateFinalized {
                    full_state_transition_id: evidence,
                    new_finalized,
                    verified_path_proof: proof,
                }),
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
                .expect("the paired finality marker can be staged");
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
            let durable = runtime
                .store
                .snapshot()
                .expect("the finality snapshot read succeeds");
            assert_eq!(
                durable.frontiers.finalized,
                if committed {
                    new_finalized
                } else {
                    anchor_frontier
                },
                "{target:?}"
            );
            assert_eq!(
                durable.frontiers.header_best,
                Frontier::new(grandchild.height, grandchild.hash),
                "{target:?}"
            );
            assert_eq!(
                runtime
                    .store
                    .node(anchor.hash)
                    .expect("the old anchor row read succeeds")
                    .is_none(),
                committed,
                "{target:?}"
            );
            assert!(runtime
                .store
                .node(child.hash)
                .expect("the new anchor row read succeeds")
                .is_some());
            assert!(runtime
                .store
                .node(grandchild.hash)
                .expect("the retained suffix row read succeeds")
                .is_some());
            let durable_metadata = runtime
                .store
                .metadata()
                .expect("the finality metadata read succeeds");
            assert_eq!(durable_metadata.work_origin, anchor_frontier, "{target:?}");
            assert_eq!(
                runtime
                    .store
                    .finality_history()
                    .expect("the finality history read succeeds")
                    .len(),
                usize::from(committed),
                "{target:?}"
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
                    .expect("the paired marker read succeeds")
                    .is_some(),
                committed,
                "{target:?}"
            );
            assert_eq!(
                memory_swapped.load(Ordering::SeqCst),
                matches!(
                    target,
                    FaultPoint::BeforePublish
                        | FaultPoint::AfterPublish
                        | FaultPoint::BeforeReactorObserve
                ),
                "{target:?}"
            );
            assert_eq!(
                runtime.publisher().snapshot().frontiers.finalized,
                if matches!(
                    target,
                    FaultPoint::AfterPublish | FaultPoint::BeforeReactorObserve
                ) {
                    new_finalized
                } else {
                    anchor_frontier
                },
                "{target:?}"
            );
            drop(runtime);
            drop(db);

            let (reopened, report) = HeaderChainStore::new(open(&db_config, &network))
                .startup(&engine_config)
                .expect("the finality crash boundary reopens coherently");
            assert_eq!(
                reopened.publisher().snapshot(),
                report.current,
                "{target:?}"
            );
            assert_eq!(
                report.current.frontiers.finalized,
                if committed {
                    new_finalized
                } else {
                    anchor_frontier
                },
                "{target:?}"
            );
            assert_eq!(
                reopened
                    .store
                    .metadata()
                    .expect("the reopened metadata is readable")
                    .work_origin,
                anchor_frontier,
                "{target:?}"
            );
            assert_eq!(
                reopened
                    .store
                    .finality_history()
                    .expect("the reopened finality history is readable")
                    .len(),
                usize::from(committed),
                "{target:?}"
            );
            let reopened_marker_cf = reopened
                .store
                .cf(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT)
                .expect("the reopened marker column family is open");
            assert_eq!(
                reopened
                    .store
                    .db
                    .raw_get_cf(&reopened_marker_cf, &marker_key)
                    .expect("the reopened marker read succeeds")
                    .is_some(),
                committed,
                "{target:?}"
            );
            let reopened_anchor = if committed {
                new_finalized
            } else {
                anchor_frontier
            };
            let lease = reopened
                .reader()
                .validation_context(reopened_anchor.hash)
                .expect("the reopened anchor context read succeeds")
                .expect("the reopened anchor is retained");
            assert_eq!(
                lease.predecessors.len(),
                if committed { 2 } else { 1 },
                "{target:?}"
            );
            let (next_header, expected_height) = if committed {
                (grandchild.header.clone(), grandchild.height)
            } else {
                (child.header.clone(), child.height)
            };
            let rules = HeaderRules::for_validation_lease(engine_config.network.clone(), &lease)
                .expect("the authenticated custom-network policy is valid");
            let prepared = zakura_header_chain::prepare_headers(
                HeaderBatchInput::new(std::slice::from_ref(&next_header)),
                &lease,
                &rules,
                &SystemClock,
            )
            .expect("the first post-anchor child validates after reopen");
            assert_eq!(prepared.headers()[0].height, expected_height, "{target:?}");
        }
    }

    #[test]
    fn aud_14_operator_reason_changes_reopen_complete_before_or_after() {
        for reconsider in [false, true] {
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
                    .initialize(metadata, anchor.clone())
                    .expect("the empty schema initializes");
                let anchor_frontier = Frontier::new(anchor.height, anchor.hash);
                let child_height = anchor
                    .height
                    .next()
                    .expect("the genesis anchor has a next height");
                let mut first_header = *anchor.header;
                first_header.previous_block_hash = anchor.hash;
                first_header.time += chrono::Duration::seconds(1);
                let first_header = Arc::new(first_header);
                let mut second_header = *first_header;
                second_header.nonce.0[0] ^= 1;
                let second_header = Arc::new(second_header);
                let (lower_header, higher_header) =
                    if first_header.hash().0 < second_header.hash().0 {
                        (first_header, second_header)
                    } else {
                        (second_header, first_header)
                    };
                let lower = Frontier::new(child_height, lower_header.hash());
                let higher = Frontier::new(child_height, higher_header.hash());
                let verified_lower = VerifiedHeaderRef {
                    height: child_height,
                    hash: lower.hash,
                    header: lower_header,
                };
                let (runtime, _) = store
                    .startup_reconciled(
                        &engine_config,
                        anchor_frontier,
                        Vec::new(),
                        vec![verified_lower],
                    )
                    .expect("the lower raw-hash branch reconciles from full state");
                let lease = runtime
                    .reader()
                    .validation_context(anchor.hash)
                    .expect("the anchor validation context is coherent")
                    .expect("the initialized anchor is retained");
                let rules = HeaderRules::for_validation_lease(network.clone(), &lease)
                    .expect("the authenticated regtest policy is valid");
                let headers = [higher_header];
                let batch = zakura_header_chain::prepare_headers(
                    HeaderBatchInput::new(&headers),
                    &lease,
                    &rules,
                    &SystemClock,
                )
                .expect("the equal-work competitor prepares through production validation");
                let before_insert = runtime.publisher().snapshot();
                let owner = WorkOwner {
                    state_version: before_insert.state_version,
                    header_generation: before_insert.header_generation,
                    verified_generation: None,
                    branch: BranchId::new(anchor.hash, higher.hash),
                    session_id: 1,
                    request_id: NonZeroU64::new(1).expect("one is nonzero"),
                };
                let context = TransitionContext {
                    config: &engine_config,
                    clock: &SystemClock,
                    full_state_authority: None,
                    retention_references: &[],
                };
                runtime
                    .apply(
                        TransitionRequest {
                            expected_version: before_insert.state_version,
                            event: TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
                                owner,
                                source: SourceId::from_digest([0xc1; 32]),
                                parent_hash: anchor.hash,
                                target_tip_hash: higher.hash,
                                completion: TargetCompletion::TargetComplete {
                                    common_ancestor: anchor_frontier,
                                },
                                batch,
                                aux: Vec::new(),
                            })),
                        },
                        &context,
                    )
                    .expect("the higher raw-hash competitor commits");
                assert_eq!(runtime.publisher().snapshot().frontiers.header_best, higher);
                assert_eq!(
                    runtime.publisher().snapshot().frontiers.verified_best,
                    lower
                );

                let invalidation_id = OperatorInvalidationId::new([0xd1; 16]);
                if reconsider {
                    let before_invalidation = runtime.publisher().snapshot();
                    runtime
                        .apply(
                            TransitionRequest {
                                expected_version: before_invalidation.state_version,
                                event: TransitionEvent::OperatorInvalidate(OperatorInvalidate {
                                    target: higher.hash,
                                    id: invalidation_id,
                                    operator_reason_digest: [0xd2; 32],
                                    evidence: EvidenceId::from_digest([0xd3; 32]),
                                }),
                            },
                            &context,
                        )
                        .expect("the exact operator reason is installed before reconsideration");
                    assert_eq!(runtime.publisher().snapshot().frontiers.header_best, lower);
                }

                let before = runtime.publisher().snapshot();
                let marker = u8::try_from(index + if reconsider { 0xa0 } else { 0x80 })
                    .expect("the fault-point list fits in u8");
                let evidence = EvidenceId::from_digest([marker; 32]);
                let event = if reconsider {
                    TransitionEvent::OperatorReconsider(OperatorReconsider {
                        target: higher.hash,
                        id: invalidation_id,
                        evidence,
                    })
                } else {
                    TransitionEvent::OperatorInvalidate(OperatorInvalidate {
                        target: higher.hash,
                        id: invalidation_id,
                        operator_reason_digest: [0xd2; 32],
                        evidence,
                    })
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
                    .expect("the paired operator marker can be staged");
                let memory_swapped = Arc::new(AtomicBool::new(false));
                let swap_probe = memory_swapped.clone();
                let result = runtime.apply_combined_with_fault(
                    TransitionRequest {
                        expected_version: before.state_version,
                        event,
                    },
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
                let selected_after = if reconsider { higher } else { lower };
                let selected_before = if reconsider { lower } else { higher };
                let reason_after = !reconsider;
                let durable = runtime
                    .store
                    .snapshot()
                    .expect("the operator snapshot read succeeds");
                let committed_version = before
                    .state_version
                    .checked_next()
                    .expect("the short fixture state version can advance");
                assert_eq!(
                    durable.state_version,
                    if committed {
                        committed_version
                    } else {
                        before.state_version
                    },
                    "{target:?}, reconsider={reconsider}"
                );
                assert_eq!(
                    durable.frontiers.header_best,
                    if committed {
                        selected_after
                    } else {
                        selected_before
                    },
                    "{target:?}, reconsider={reconsider}"
                );
                assert_eq!(
                    durable.frontiers.verified_best, lower,
                    "{target:?}, reconsider={reconsider}"
                );
                let reason = EligibilityReason::OperatorInvalid {
                    id: invalidation_id,
                };
                assert_eq!(
                    runtime
                        .store
                        .node(higher.hash)
                        .expect("the target node read succeeds")
                        .expect("the operator target remains retained")
                        .eligibility
                        .direct_reasons
                        .contains(&reason),
                    if committed { reason_after } else { reconsider },
                    "{target:?}, reconsider={reconsider}"
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
                        .expect("the paired marker read succeeds")
                        .is_some(),
                    committed,
                    "{target:?}, reconsider={reconsider}"
                );
                assert_eq!(
                    memory_swapped.load(Ordering::SeqCst),
                    matches!(
                        target,
                        FaultPoint::BeforePublish
                            | FaultPoint::AfterPublish
                            | FaultPoint::BeforeReactorObserve
                    ),
                    "{target:?}, reconsider={reconsider}"
                );
                assert_eq!(
                    runtime.publisher().snapshot().frontiers.header_best,
                    if matches!(
                        target,
                        FaultPoint::AfterPublish | FaultPoint::BeforeReactorObserve
                    ) {
                        selected_after
                    } else {
                        selected_before
                    },
                    "{target:?}, reconsider={reconsider}"
                );
                drop(runtime);
                drop(db);

                let (reopened, report) = HeaderChainStore::new(open(&db_config, &network))
                    .startup(&engine_config)
                    .expect("the operator crash boundary reopens coherently");
                assert_eq!(
                    reopened.publisher().snapshot(),
                    report.current,
                    "{target:?}, reconsider={reconsider}"
                );
                assert_eq!(
                    report.current.frontiers.header_best,
                    if committed {
                        selected_after
                    } else {
                        selected_before
                    },
                    "{target:?}, reconsider={reconsider}"
                );
                assert_eq!(
                    report.current.state_version,
                    if committed {
                        committed_version
                    } else {
                        before.state_version
                    },
                    "{target:?}, reconsider={reconsider}"
                );
                assert_eq!(
                    report.current.frontiers.verified_best, lower,
                    "{target:?}, reconsider={reconsider}"
                );
                assert_eq!(
                    reopened
                        .store
                        .node(higher.hash)
                        .expect("the reopened target node read succeeds")
                        .expect("the reopened operator target remains retained")
                        .eligibility
                        .direct_reasons
                        .contains(&reason),
                    if committed { reason_after } else { reconsider },
                    "{target:?}, reconsider={reconsider}"
                );
                let reopened_marker_cf = reopened
                    .store
                    .cf(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT)
                    .expect("the reopened marker column family is open");
                assert_eq!(
                    reopened
                        .store
                        .db
                        .raw_get_cf(&reopened_marker_cf, &marker_key)
                        .expect("the reopened marker read succeeds")
                        .is_some(),
                    committed,
                    "{target:?}, reconsider={reconsider}"
                );
            }
        }
    }

    #[test]
    fn aud_14_verified_grow_and_reset_reopen_complete_before_or_after() {
        for reset in [false, true] {
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
                    .initialize(metadata, anchor.clone())
                    .expect("the empty schema initializes");
                let anchor_frontier = Frontier::new(anchor.height, anchor.hash);
                let child_height = anchor
                    .height
                    .next()
                    .expect("the genesis anchor has a next height");
                let mut incumbent_header = *anchor.header;
                incumbent_header.previous_block_hash = anchor.hash;
                incumbent_header.time += chrono::Duration::seconds(1);
                incumbent_header.nonce.0[0] ^= 1;
                let incumbent_header = Arc::new(incumbent_header);
                let incumbent = VerifiedHeaderRef {
                    height: child_height,
                    hash: incumbent_header.hash(),
                    header: incumbent_header,
                };
                let mut replacement_header = *anchor.header;
                replacement_header.previous_block_hash = anchor.hash;
                replacement_header.time += chrono::Duration::seconds(1);
                replacement_header.nonce.0[0] ^= 2;
                let replacement_header = Arc::new(replacement_header);
                let replacement = VerifiedHeaderRef {
                    height: child_height,
                    hash: replacement_header.hash(),
                    header: replacement_header,
                };
                assert_ne!(incumbent.hash, replacement.hash);

                let (runtime, _) = if reset {
                    store
                        .startup_reconciled(
                            &engine_config,
                            anchor_frontier,
                            Vec::new(),
                            vec![incumbent.clone()],
                        )
                        .expect("the incumbent verified path reconciles")
                } else {
                    store
                        .startup(&engine_config)
                        .expect("the initialized store audits")
                };
                let before = runtime.publisher().snapshot();
                let old_verified = before.frontiers.verified_best;
                let event_header = if reset {
                    replacement.clone()
                } else {
                    incumbent.clone()
                };
                let event_frontier = Frontier::new(event_header.height, event_header.hash);
                let marker = u8::try_from(index + if reset { 0xd0 } else { 0xb0 })
                    .expect("the fault-point list fits in u8");
                let evidence = EvidenceId::from_digest([marker; 32]);
                let authority = Authority(evidence);
                let context = TransitionContext {
                    config: &engine_config,
                    clock: &SystemClock,
                    full_state_authority: Some(&authority),
                    retention_references: &[],
                };
                let request = TransitionRequest {
                    expected_version: before.state_version,
                    event: TransitionEvent::VerifiedChainChanged(VerifiedChainChanged {
                        full_state_transition_id: evidence,
                        old_tip: old_verified,
                        new_path: vec![event_header],
                        cause: if reset {
                            VerifiedChangeCause::Reset
                        } else {
                            VerifiedChangeCause::Grow
                        },
                    }),
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
                    .expect("the paired verified-path marker can be staged");
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
                let published = matches!(
                    target,
                    FaultPoint::AfterPublish | FaultPoint::BeforeReactorObserve
                );
                let committed_version = before
                    .state_version
                    .checked_next()
                    .expect("the short fixture state version can advance");
                let header_best_after = if reset {
                    [incumbent.hash, replacement.hash]
                        .into_iter()
                        .max_by_key(|hash| hash.0)
                        .map(|hash| Frontier::new(child_height, hash))
                        .expect("the two-child fixture is nonempty")
                } else {
                    event_frontier
                };
                let durable = runtime
                    .store
                    .snapshot()
                    .expect("the verified-path snapshot read succeeds");
                assert_eq!(
                    durable.state_version,
                    if committed {
                        committed_version
                    } else {
                        before.state_version
                    },
                    "{target:?}, reset={reset}"
                );
                assert_eq!(
                    durable.frontiers.verified_best,
                    if committed {
                        event_frontier
                    } else {
                        old_verified
                    },
                    "{target:?}, reset={reset}"
                );
                assert_eq!(
                    durable.frontiers.header_best,
                    if committed {
                        header_best_after
                    } else {
                        before.frontiers.header_best
                    },
                    "{target:?}, reset={reset}"
                );
                let event_node = runtime
                    .store
                    .node(event_frontier.hash)
                    .expect("the event node read succeeds");
                assert_eq!(event_node.is_some(), committed, "{target:?}, reset={reset}");
                if let Some(event_node) = event_node {
                    assert!(matches!(
                        event_node.body,
                        BodyValidationState::Verified {
                            evidence: node_evidence
                        } if node_evidence == evidence
                    ));
                }
                assert_eq!(
                    runtime
                        .store
                        .verified_projection()
                        .expect("the verified projection is readable"),
                    if committed {
                        vec![anchor_frontier, event_frontier]
                    } else if reset {
                        vec![
                            anchor_frontier,
                            Frontier::new(incumbent.height, incumbent.hash),
                        ]
                    } else {
                        vec![anchor_frontier]
                    },
                    "{target:?}, reset={reset}"
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
                        .expect("the paired marker read succeeds")
                        .is_some(),
                    committed,
                    "{target:?}, reset={reset}"
                );
                assert_eq!(
                    memory_swapped.load(Ordering::SeqCst),
                    matches!(
                        target,
                        FaultPoint::BeforePublish
                            | FaultPoint::AfterPublish
                            | FaultPoint::BeforeReactorObserve
                    ),
                    "{target:?}, reset={reset}"
                );
                assert_eq!(
                    runtime.publisher().snapshot().frontiers.verified_best,
                    if published {
                        event_frontier
                    } else {
                        old_verified
                    },
                    "{target:?}, reset={reset}"
                );
                drop(runtime);
                drop(db);

                let (reopened, report) = HeaderChainStore::new(open(&db_config, &network))
                    .startup(&engine_config)
                    .expect("the verified-path crash boundary reopens coherently");
                assert_eq!(
                    reopened.publisher().snapshot(),
                    report.current,
                    "{target:?}, reset={reset}"
                );
                assert_eq!(
                    report.current.frontiers.verified_best,
                    if committed {
                        event_frontier
                    } else {
                        old_verified
                    },
                    "{target:?}, reset={reset}"
                );
                assert_eq!(
                    reopened
                        .store
                        .node(event_frontier.hash)
                        .expect("the reopened event node read succeeds")
                        .is_some(),
                    committed,
                    "{target:?}, reset={reset}"
                );
                let reopened_marker_cf = reopened
                    .store
                    .cf(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT)
                    .expect("the reopened marker column family is open");
                assert_eq!(
                    reopened
                        .store
                        .db
                        .raw_get_cf(&reopened_marker_cf, &marker_key)
                        .expect("the reopened marker read succeeds")
                        .is_some(),
                    committed,
                    "{target:?}, reset={reset}"
                );
            }
        }
    }

    #[test]
    fn aud_14_body_retry_restarts_reopen_complete_before_or_after() {
        for operator_retry in [false, true] {
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
                    .initialize(metadata, anchor.clone())
                    .expect("the empty schema initializes");
                let (runtime, _) = store
                    .startup(&engine_config)
                    .expect("the initial store audits");
                let initial = runtime.publisher().snapshot();
                let started_at = Utc
                    .timestamp_opt(1_000, 0)
                    .single()
                    .expect("the fixture timestamp is valid");
                let old = BodyUnavailableSummary {
                    started_at,
                    attempts: 10,
                    suppliers: 2,
                    supplier_set_digest: [0x31; 32],
                    alarmed: true,
                    next_probe_at: Utc
                        .timestamp_opt(1_600, 0)
                        .single()
                        .expect("the fixture probe timestamp is valid"),
                };
                let seed_evidence = EvidenceId::from_digest(
                    [u8::try_from(index + 0x60).expect("the fault-point list fits in u8"); 32],
                );
                let seed_authority = Authority(seed_evidence);
                let seed_context = TransitionContext {
                    config: &engine_config,
                    clock: &SystemClock,
                    full_state_authority: Some(&seed_authority),
                    retention_references: &[],
                };
                runtime
                    .apply(
                        TransitionRequest {
                            expected_version: initial.state_version,
                            event: TransitionEvent::BodyEvidence(BodyEvidence::Transient(
                                TransientBodyFailure {
                                    hash: anchor.hash,
                                    evidence: seed_evidence,
                                    kind: TransientBodyFailureKind::Timeout,
                                    availability: old,
                                },
                            )),
                        },
                        &seed_context,
                    )
                    .expect("the persistent body alarm fixture commits");
                let before = runtime.publisher().snapshot();
                assert_eq!(before.alarms.header_best_body_unavailable, Some(old));

                let marker = u8::try_from(index + if operator_retry { 0x90 } else { 0x70 })
                    .expect("the fault-point list fits in u8");
                let fresh_at = started_at + chrono::Duration::minutes(20);
                let fresh = BodyUnavailableSummary {
                    started_at: fresh_at,
                    attempts: 0,
                    suppliers: if operator_retry {
                        old.suppliers
                    } else {
                        old.suppliers.saturating_add(1)
                    },
                    supplier_set_digest: if operator_retry {
                        old.supplier_set_digest
                    } else {
                        [0x32; 32]
                    },
                    alarmed: false,
                    next_probe_at: fresh_at,
                };
                let evidence = EvidenceId::from_digest([marker; 32]);
                let authority = Authority(evidence);
                let context = TransitionContext {
                    config: &engine_config,
                    clock: &SystemClock,
                    full_state_authority: if operator_retry {
                        None
                    } else {
                        Some(&authority)
                    },
                    retention_references: &[],
                };
                let event = if operator_retry {
                    TransitionEvent::OperatorBodyRetry(zakura_header_chain::OperatorBodyRetry {
                        hash: anchor.hash,
                        evidence,
                        availability: fresh,
                    })
                } else {
                    TransitionEvent::BodySupplierDiscovered(
                        zakura_header_chain::BodySupplierDiscovered {
                            hash: anchor.hash,
                            evidence,
                            availability: fresh,
                        },
                    )
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
                    .expect("the paired retry marker can be staged");
                let memory_swapped = Arc::new(AtomicBool::new(false));
                let swap_probe = memory_swapped.clone();
                let result = runtime.apply_combined_with_fault(
                    TransitionRequest {
                        expected_version: before.state_version,
                        event,
                    },
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
                let published = matches!(
                    target,
                    FaultPoint::AfterPublish | FaultPoint::BeforeReactorObserve
                );
                let committed_version = before
                    .state_version
                    .checked_next()
                    .expect("the short fixture state version can advance");
                let durable = runtime
                    .store
                    .snapshot()
                    .expect("the retry snapshot read succeeds");
                assert_eq!(
                    durable.state_version,
                    if committed {
                        committed_version
                    } else {
                        before.state_version
                    },
                    "{target:?}, operator_retry={operator_retry}"
                );
                assert_eq!(
                    durable.frontiers, before.frontiers,
                    "{target:?}, operator_retry={operator_retry}"
                );
                assert_eq!(
                    durable.header_generation, before.header_generation,
                    "{target:?}, operator_retry={operator_retry}"
                );
                assert_eq!(
                    durable.verified_generation, before.verified_generation,
                    "{target:?}, operator_retry={operator_retry}"
                );
                assert_eq!(
                    durable.alarms.header_best_body_unavailable,
                    if committed { None } else { Some(old) },
                    "{target:?}, operator_retry={operator_retry}"
                );
                assert_eq!(
                    runtime
                        .store
                        .node(anchor.hash)
                        .expect("the retry node read succeeds")
                        .expect("the selected retry node remains retained")
                        .body,
                    BodyValidationState::Unavailable(if committed { fresh } else { old }),
                    "{target:?}, operator_retry={operator_retry}"
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
                        .expect("the paired marker read succeeds")
                        .is_some(),
                    committed,
                    "{target:?}, operator_retry={operator_retry}"
                );
                assert_eq!(
                    memory_swapped.load(Ordering::SeqCst),
                    matches!(
                        target,
                        FaultPoint::BeforePublish
                            | FaultPoint::AfterPublish
                            | FaultPoint::BeforeReactorObserve
                    ),
                    "{target:?}, operator_retry={operator_retry}"
                );
                assert_eq!(
                    runtime.publisher().snapshot().state_version,
                    if published {
                        committed_version
                    } else {
                        before.state_version
                    },
                    "{target:?}, operator_retry={operator_retry}"
                );
                drop(runtime);
                drop(db);

                let (reopened, report) = HeaderChainStore::new(open(&db_config, &network))
                    .startup(&engine_config)
                    .expect("the retry crash boundary reopens coherently");
                assert_eq!(
                    reopened.publisher().snapshot(),
                    report.current,
                    "{target:?}, operator_retry={operator_retry}"
                );
                assert_eq!(
                    report.current.state_version,
                    if committed {
                        committed_version
                    } else {
                        before.state_version
                    },
                    "{target:?}, operator_retry={operator_retry}"
                );
                assert_eq!(
                    report.current.alarms.header_best_body_unavailable,
                    if committed { None } else { Some(old) },
                    "{target:?}, operator_retry={operator_retry}"
                );
                assert_eq!(
                    reopened
                        .store
                        .node(anchor.hash)
                        .expect("the reopened retry node read succeeds")
                        .expect("the reopened selected retry node remains retained")
                        .body,
                    BodyValidationState::Unavailable(if committed { fresh } else { old }),
                    "{target:?}, operator_retry={operator_retry}"
                );
                let reopened_marker_cf = reopened
                    .store
                    .cf(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT)
                    .expect("the reopened marker column family is open");
                assert_eq!(
                    reopened
                        .store
                        .db
                        .raw_get_cf(&reopened_marker_cf, &marker_key)
                        .expect("the reopened paired marker read succeeds")
                        .is_some(),
                    committed,
                    "{target:?}, operator_retry={operator_retry}"
                );
            }
        }
    }

    #[test]
    fn aud_14_body_conclusions_reopen_complete_before_or_after() {
        for consensus_invalid in [false, true] {
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
                    .initialize(metadata, anchor.clone())
                    .expect("the empty schema initializes");
                let (runtime, _) = store
                    .startup(&engine_config)
                    .expect("the initial store audits");
                let initial = runtime.publisher().snapshot();
                let anchor_frontier = Frontier::new(anchor.height, anchor.hash);
                let lease = runtime
                    .reader()
                    .validation_context(anchor.hash)
                    .expect("the anchor validation context is coherent")
                    .expect("the initialized anchor is retained");
                let rules = HeaderRules::for_validation_lease(network.clone(), &lease)
                    .expect("the authenticated regtest policy is valid");
                let marker = u8::try_from(index + if consensus_invalid { 0xc0 } else { 0x40 })
                    .expect("the fault-point list fits in u8");
                let mut child_header = *anchor.header;
                child_header.previous_block_hash = anchor.hash;
                child_header.time += chrono::Duration::seconds(1);
                child_header.nonce.0[0] = marker;
                let child_header = Arc::new(child_header);
                let headers = [child_header.clone()];
                let batch = zakura_header_chain::prepare_headers(
                    HeaderBatchInput::new(&headers),
                    &lease,
                    &rules,
                    &SystemClock,
                )
                .expect("the body-conclusion fixture header passes production validation");
                let child = Frontier::new(
                    anchor
                        .height
                        .next()
                        .expect("the genesis anchor has a next height"),
                    child_header.hash(),
                );
                let owner = WorkOwner {
                    state_version: initial.state_version,
                    header_generation: initial.header_generation,
                    verified_generation: None,
                    branch: BranchId::new(anchor.hash, child.hash),
                    session_id: 41,
                    request_id: NonZeroU64::new(42).expect("forty-two is nonzero"),
                };
                let insertion_context = TransitionContext {
                    config: &engine_config,
                    clock: &SystemClock,
                    full_state_authority: None,
                    retention_references: &[],
                };
                runtime
                    .apply(
                        TransitionRequest {
                            expected_version: initial.state_version,
                            event: TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
                                owner,
                                source: SourceId::from_digest([marker.wrapping_add(1); 32]),
                                parent_hash: anchor.hash,
                                target_tip_hash: child.hash,
                                completion: TargetCompletion::TargetComplete {
                                    common_ancestor: anchor_frontier,
                                },
                                batch,
                                aux: Vec::new(),
                            })),
                        },
                        &insertion_context,
                    )
                    .expect("the selected body-conclusion fixture commits");
                let before = runtime.publisher().snapshot();
                assert_eq!(before.frontiers.header_best, child);
                assert_eq!(
                    runtime
                        .store
                        .node(child.hash)
                        .expect("the child node read succeeds")
                        .expect("the selected child is retained")
                        .body,
                    BodyValidationState::Unknown
                );

                let evidence = EvidenceId::from_digest([marker.wrapping_add(2); 32]);
                let rule = BodyRuleId::new("aud14.commitment_matching_invalid");
                let source = SourceId::from_digest([marker.wrapping_add(3); 32]);
                let event = if consensus_invalid {
                    TransitionEvent::BodyEvidence(BodyEvidence::ConsensusInvalid(
                        zakura_header_chain::ConsensusBodyInvalid {
                            hash: child.hash,
                            evidence,
                            rule: rule.clone(),
                            source,
                        },
                    ))
                } else {
                    TransitionEvent::BodyEvidence(BodyEvidence::Verified(VerifiedBodyEvidence {
                        hash: child.hash,
                        evidence,
                    }))
                };
                let authority = Authority(evidence);
                let context = TransitionContext {
                    config: &engine_config,
                    clock: &SystemClock,
                    full_state_authority: Some(&authority),
                    retention_references: &[],
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
                    .expect("the paired body-conclusion marker can be staged");
                let memory_swapped = Arc::new(AtomicBool::new(false));
                let swap_probe = memory_swapped.clone();
                let result = runtime.apply_combined_with_fault(
                    TransitionRequest {
                        expected_version: before.state_version,
                        event,
                    },
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
                let published = matches!(
                    target,
                    FaultPoint::AfterPublish | FaultPoint::BeforeReactorObserve
                );
                let committed_version = before
                    .state_version
                    .checked_next()
                    .expect("the short fixture state version can advance");
                let committed_header_generation = before
                    .header_generation
                    .checked_next()
                    .expect("the short fixture header generation can advance");
                let selected_after = if consensus_invalid {
                    anchor_frontier
                } else {
                    child
                };
                let durable = runtime
                    .store
                    .snapshot()
                    .expect("the body-conclusion snapshot read succeeds");
                assert_eq!(
                    durable.state_version,
                    if committed {
                        committed_version
                    } else {
                        before.state_version
                    },
                    "{target:?}, consensus_invalid={consensus_invalid}"
                );
                assert_eq!(
                    durable.frontiers.header_best,
                    if committed { selected_after } else { child },
                    "{target:?}, consensus_invalid={consensus_invalid}"
                );
                assert_eq!(
                    durable.header_generation,
                    if committed && consensus_invalid {
                        committed_header_generation
                    } else {
                        before.header_generation
                    },
                    "{target:?}, consensus_invalid={consensus_invalid}"
                );
                assert_eq!(
                    durable.frontiers.verified_best, before.frontiers.verified_best,
                    "{target:?}, consensus_invalid={consensus_invalid}"
                );
                assert_eq!(
                    durable.verified_generation, before.verified_generation,
                    "{target:?}, consensus_invalid={consensus_invalid}"
                );
                let child_node = runtime
                    .store
                    .node(child.hash)
                    .expect("the body-conclusion node read succeeds")
                    .expect("the body-conclusion child remains retained");
                let expected_body = if committed {
                    if consensus_invalid {
                        BodyValidationState::ConsensusInvalid {
                            evidence,
                            rule: rule.clone(),
                        }
                    } else {
                        BodyValidationState::Verified { evidence }
                    }
                } else {
                    BodyValidationState::Unknown
                };
                assert_eq!(
                    child_node.body, expected_body,
                    "{target:?}, consensus_invalid={consensus_invalid}"
                );
                let invalid_reason = EligibilityReason::ConsensusBodyInvalid {
                    evidence,
                    rule: rule.clone(),
                };
                assert_eq!(
                    child_node
                        .eligibility
                        .direct_reasons
                        .contains(&invalid_reason),
                    committed && consensus_invalid,
                    "{target:?}, consensus_invalid={consensus_invalid}"
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
                        .expect("the paired marker read succeeds")
                        .is_some(),
                    committed,
                    "{target:?}, consensus_invalid={consensus_invalid}"
                );
                assert_eq!(
                    memory_swapped.load(Ordering::SeqCst),
                    matches!(
                        target,
                        FaultPoint::BeforePublish
                            | FaultPoint::AfterPublish
                            | FaultPoint::BeforeReactorObserve
                    ),
                    "{target:?}, consensus_invalid={consensus_invalid}"
                );
                assert_eq!(
                    runtime.publisher().snapshot().frontiers.header_best,
                    if published { selected_after } else { child },
                    "{target:?}, consensus_invalid={consensus_invalid}"
                );
                drop(runtime);
                drop(db);

                let (reopened, report) = HeaderChainStore::new(open(&db_config, &network))
                    .startup(&engine_config)
                    .expect("the body-conclusion crash boundary reopens coherently");
                assert_eq!(
                    reopened.publisher().snapshot(),
                    report.current,
                    "{target:?}, consensus_invalid={consensus_invalid}"
                );
                assert_eq!(
                    report.current.frontiers.header_best,
                    if committed { selected_after } else { child },
                    "{target:?}, consensus_invalid={consensus_invalid}"
                );
                assert_eq!(
                    report.current.state_version,
                    if committed {
                        committed_version
                    } else {
                        before.state_version
                    },
                    "{target:?}, consensus_invalid={consensus_invalid}"
                );
                let reopened_child = reopened
                    .store
                    .node(child.hash)
                    .expect("the reopened body-conclusion node read succeeds")
                    .expect("the reopened body-conclusion child remains retained");
                assert_eq!(
                    reopened_child.body, expected_body,
                    "{target:?}, consensus_invalid={consensus_invalid}"
                );
                assert_eq!(
                    reopened_child
                        .eligibility
                        .direct_reasons
                        .contains(&invalid_reason),
                    committed && consensus_invalid,
                    "{target:?}, consensus_invalid={consensus_invalid}"
                );
                let reopened_marker_cf = reopened
                    .store
                    .cf(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT)
                    .expect("the reopened marker column family is open");
                assert_eq!(
                    reopened
                        .store
                        .db
                        .raw_get_cf(&reopened_marker_cf, &marker_key)
                        .expect("the reopened paired marker read succeeds")
                        .is_some(),
                    committed,
                    "{target:?}, consensus_invalid={consensus_invalid}"
                );
            }
        }
    }

    #[test]
    fn aud_14_deferred_header_reevaluation_reopens_complete_before_or_after() {
        #[derive(Copy, Clone)]
        struct FixedClock(chrono::DateTime<Utc>);

        impl zakura_header_chain::Clock for FixedClock {
            fn now(&self) -> chrono::DateTime<Utc> {
                self.0
            }
        }

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
                .initialize(metadata, anchor.clone())
                .expect("the empty schema initializes");
            let (runtime, _) = store
                .startup(&engine_config)
                .expect("the initial store audits");
            let initial = runtime.publisher().snapshot();
            let anchor_frontier = Frontier::new(anchor.height, anchor.hash);
            let lease = runtime
                .reader()
                .validation_context(anchor.hash)
                .expect("the anchor validation context is coherent")
                .expect("the initialized anchor is retained");
            let rules = HeaderRules::for_validation_lease(network.clone(), &lease)
                .expect("the authenticated regtest policy is valid");
            let marker = u8::try_from(index + 0xa0).expect("the fault-point list fits in u8");
            let preparation_clock = FixedClock(anchor.header.time);
            let mut future_header = *anchor.header;
            future_header.previous_block_hash = anchor.hash;
            future_header.time += chrono::Duration::hours(3);
            future_header.nonce.0[0] = marker;
            let future_header = Arc::new(future_header);
            let headers = [future_header.clone()];
            let batch = zakura_header_chain::prepare_headers(
                HeaderBatchInput::new(&headers),
                &lease,
                &rules,
                &preparation_clock,
            )
            .expect("the locally future header is admitted as deferred");
            let deferred_until = future_header.time - chrono::Duration::hours(2);
            assert_eq!(
                batch.headers()[0].validation,
                HeaderValidationState::DeferredUntil(deferred_until)
            );
            let future = Frontier::new(
                anchor
                    .height
                    .next()
                    .expect("the genesis anchor has a next height"),
                future_header.hash(),
            );
            let owner = WorkOwner {
                state_version: initial.state_version,
                header_generation: initial.header_generation,
                verified_generation: None,
                branch: BranchId::new(anchor.hash, future.hash),
                session_id: 31,
                request_id: NonZeroU64::new(32).expect("thirty-two is nonzero"),
            };
            let insertion_context = TransitionContext {
                config: &engine_config,
                clock: &preparation_clock,
                full_state_authority: None,
                retention_references: &[],
            };
            runtime
                .apply(
                    TransitionRequest {
                        expected_version: initial.state_version,
                        event: TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
                            owner,
                            source: SourceId::from_digest([marker.wrapping_add(1); 32]),
                            parent_hash: anchor.hash,
                            target_tip_hash: future.hash,
                            completion: TargetCompletion::TargetComplete {
                                common_ancestor: anchor_frontier,
                            },
                            batch,
                            aux: Vec::new(),
                        })),
                    },
                    &insertion_context,
                )
                .expect("the deferred header insertion commits");
            let before = runtime.publisher().snapshot();
            assert_eq!(before.frontiers.header_best, anchor_frontier);
            assert_eq!(
                runtime
                    .store
                    .node(future.hash)
                    .expect("the deferred node read succeeds")
                    .expect("the deferred node is retained")
                    .validation,
                HeaderValidationState::DeferredUntil(deferred_until)
            );
            assert_eq!(
                runtime
                    .store
                    .deferred_entries()
                    .expect("the deferred index is readable"),
                vec![(deferred_until, future.hash)]
            );

            let reevaluation_clock = FixedClock(deferred_until);
            let context = TransitionContext {
                config: &engine_config,
                clock: &reevaluation_clock,
                full_state_authority: None,
                retention_references: &[],
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
                .expect("the paired reevaluation marker can be staged");
            let memory_swapped = Arc::new(AtomicBool::new(false));
            let swap_probe = memory_swapped.clone();
            let result = runtime.apply_combined_with_fault(
                TransitionRequest {
                    expected_version: before.state_version,
                    event: TransitionEvent::ReevaluateDeferred,
                },
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
            let published = matches!(
                target,
                FaultPoint::AfterPublish | FaultPoint::BeforeReactorObserve
            );
            let committed_version = before
                .state_version
                .checked_next()
                .expect("the short fixture state version can advance");
            let committed_header_generation = before
                .header_generation
                .checked_next()
                .expect("the short fixture header generation can advance");
            let durable = runtime
                .store
                .snapshot()
                .expect("the reevaluation snapshot read succeeds");
            assert_eq!(
                durable.state_version,
                if committed {
                    committed_version
                } else {
                    before.state_version
                },
                "{target:?}"
            );
            assert_eq!(
                durable.frontiers.header_best,
                if committed { future } else { anchor_frontier },
                "{target:?}"
            );
            assert_eq!(
                durable.header_generation,
                if committed {
                    committed_header_generation
                } else {
                    before.header_generation
                },
                "{target:?}"
            );
            assert_eq!(
                durable.frontiers.verified_best, before.frontiers.verified_best,
                "{target:?}"
            );
            assert_eq!(
                durable.verified_generation, before.verified_generation,
                "{target:?}"
            );
            assert_eq!(
                runtime
                    .store
                    .node(future.hash)
                    .expect("the future node read succeeds")
                    .expect("the future node remains retained")
                    .validation,
                if committed {
                    HeaderValidationState::Valid
                } else {
                    HeaderValidationState::DeferredUntil(deferred_until)
                },
                "{target:?}"
            );
            assert_eq!(
                runtime
                    .store
                    .deferred_entries()
                    .expect("the deferred index is readable"),
                if committed {
                    Vec::new()
                } else {
                    vec![(deferred_until, future.hash)]
                },
                "{target:?}"
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
                    .expect("the paired marker read succeeds")
                    .is_some(),
                committed,
                "{target:?}"
            );
            assert_eq!(
                memory_swapped.load(Ordering::SeqCst),
                matches!(
                    target,
                    FaultPoint::BeforePublish
                        | FaultPoint::AfterPublish
                        | FaultPoint::BeforeReactorObserve
                ),
                "{target:?}"
            );
            assert_eq!(
                runtime.publisher().snapshot().frontiers.header_best,
                if published { future } else { anchor_frontier },
                "{target:?}"
            );
            drop(runtime);
            drop(db);

            let (reopened, report) = HeaderChainStore::new(open(&db_config, &network))
                .startup(&engine_config)
                .expect("the deferred reevaluation crash boundary reopens coherently");
            assert_eq!(
                reopened.publisher().snapshot(),
                report.current,
                "{target:?}"
            );
            assert_eq!(
                report.current.frontiers.header_best,
                if committed { future } else { anchor_frontier },
                "{target:?}"
            );
            assert_eq!(
                report.current.state_version,
                if committed {
                    committed_version
                } else {
                    before.state_version
                },
                "{target:?}"
            );
            assert_eq!(
                reopened
                    .store
                    .node(future.hash)
                    .expect("the reopened future node read succeeds")
                    .expect("the reopened future node remains retained")
                    .validation,
                if committed {
                    HeaderValidationState::Valid
                } else {
                    HeaderValidationState::DeferredUntil(deferred_until)
                },
                "{target:?}"
            );
            assert_eq!(
                reopened
                    .store
                    .deferred_entries()
                    .expect("the reopened deferred index is readable"),
                if committed {
                    Vec::new()
                } else {
                    vec![(deferred_until, future.hash)]
                },
                "{target:?}"
            );
            let reopened_marker_cf = reopened
                .store
                .cf(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT)
                .expect("the reopened marker column family is open");
            assert_eq!(
                reopened
                    .store
                    .db
                    .raw_get_cf(&reopened_marker_cf, &marker_key)
                    .expect("the reopened paired marker read succeeds")
                    .is_some(),
                committed,
                "{target:?}"
            );
        }
    }

    #[test]
    fn aud_14_selected_auxiliary_repair_reopens_complete_before_or_after() {
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
                .initialize(metadata, anchor.clone())
                .expect("the empty schema initializes");
            let (runtime, _) = store
                .startup(&engine_config)
                .expect("the initial store audits");
            let initial = runtime.publisher().snapshot();
            let anchor_frontier = Frontier::new(anchor.height, anchor.hash);
            let lease = runtime
                .reader()
                .validation_context(anchor.hash)
                .expect("the anchor validation context is coherent")
                .expect("the initialized anchor is retained");
            let rules = HeaderRules::for_validation_lease(network.clone(), &lease)
                .expect("the authenticated regtest policy is valid");
            let marker = u8::try_from(index + 0x10).expect("the fault-point list fits in u8");
            let mut child_header = *anchor.header;
            child_header.previous_block_hash = anchor.hash;
            child_header.time += chrono::Duration::seconds(1);
            child_header.nonce.0[0] = marker;
            let child_header = Arc::new(child_header);
            let headers = [child_header.clone()];
            let insertion_batch = zakura_header_chain::prepare_headers(
                HeaderBatchInput::new(&headers),
                &lease,
                &rules,
                &SystemClock,
            )
            .expect("the selected repair fixture passes production validation");
            let child = Frontier::new(
                anchor
                    .height
                    .next()
                    .expect("the genesis anchor has a next height"),
                child_header.hash(),
            );
            let insertion_owner = WorkOwner {
                state_version: initial.state_version,
                header_generation: initial.header_generation,
                verified_generation: None,
                branch: BranchId::new(anchor.hash, child.hash),
                session_id: 51,
                request_id: NonZeroU64::new(52).expect("fifty-two is nonzero"),
            };
            let insertion_context = TransitionContext {
                config: &engine_config,
                clock: &SystemClock,
                full_state_authority: None,
                retention_references: &[],
            };
            runtime
                .apply(
                    TransitionRequest {
                        expected_version: initial.state_version,
                        event: TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
                            owner: insertion_owner,
                            source: SourceId::from_digest([marker.wrapping_add(1); 32]),
                            parent_hash: anchor.hash,
                            target_tip_hash: child.hash,
                            completion: TargetCompletion::TargetComplete {
                                common_ancestor: anchor_frontier,
                            },
                            batch: insertion_batch,
                            aux: Vec::new(),
                        })),
                    },
                    &insertion_context,
                )
                .expect("the selected repair target inserts without auxiliary metadata");
            let before = runtime.publisher().snapshot();
            assert_eq!(before.frontiers.header_best, child);
            assert!(runtime
                .store
                .aux_deliveries(child.hash)
                .expect("the initial auxiliary index is readable")
                .is_empty());

            let repair_lease = runtime
                .reader()
                .validation_context(anchor.hash)
                .expect("the repair validation context is coherent")
                .expect("the repair parent remains retained");
            let repair_rules = HeaderRules::for_validation_lease(network.clone(), &repair_lease)
                .expect("the authenticated repair policy is valid");
            let repair_batch = zakura_header_chain::prepare_headers(
                HeaderBatchInput::new(&headers),
                &repair_lease,
                &repair_rules,
                &SystemClock,
            )
            .expect("the selected header redelivery passes production validation");
            let repair_owner = WorkScope::for_body_work(&before)
                .bind(53, NonZeroU64::new(54).expect("fifty-four is nonzero"));
            let source = SourceId::from_digest([marker.wrapping_add(2); 32]);
            let delivery = AuxDelivery {
                delivery_id: EvidenceId::from_digest([marker.wrapping_add(3); 32]),
                header_hash: child.hash,
                source,
                owner: repair_owner,
                body_size: zakura_header_chain::BodySizeHint::Unknown,
                tree_aux: Some(zakura_header_chain::TreeAuxRecordV1 {
                    height: child.height,
                    sapling_root: Default::default(),
                    orchard_root: Default::default(),
                    ironwood_root: Default::default(),
                    sapling_tx_count: 4,
                    orchard_tx_count: 5,
                    ironwood_tx_count: 6,
                    auth_data_root: zakura_chain::block::merkle::AuthDataRoot::from(
                        [marker.wrapping_add(4); 32],
                    ),
                }),
                authentication: zakura_header_chain::AuxAuthentication::Unauthenticated,
            };
            let context = TransitionContext {
                config: &engine_config,
                clock: &SystemClock,
                full_state_authority: None,
                retention_references: &[],
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
                .expect("the paired selected-repair marker can be staged");
            let memory_swapped = Arc::new(AtomicBool::new(false));
            let swap_probe = memory_swapped.clone();
            let result = runtime.apply_combined_with_fault(
                TransitionRequest {
                    expected_version: before.state_version,
                    event: TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
                        owner: repair_owner,
                        source,
                        parent_hash: anchor.hash,
                        target_tip_hash: child.hash,
                        completion: TargetCompletion::SelectedAuxiliaryRepair {
                            common_ancestor: anchor_frontier,
                            selected_target: child,
                        },
                        batch: repair_batch,
                        aux: vec![delivery],
                    })),
                },
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
            let published = matches!(
                target,
                FaultPoint::AfterPublish | FaultPoint::BeforeReactorObserve
            );
            let committed_version = before
                .state_version
                .checked_next()
                .expect("the short fixture state version can advance");
            let durable = runtime
                .store
                .snapshot()
                .expect("the selected-repair snapshot read succeeds");
            assert_eq!(
                durable.state_version,
                if committed {
                    committed_version
                } else {
                    before.state_version
                },
                "{target:?}"
            );
            assert_eq!(durable.frontiers, before.frontiers, "{target:?}");
            assert_eq!(
                durable.header_generation, before.header_generation,
                "{target:?}"
            );
            assert_eq!(
                durable.verified_generation, before.verified_generation,
                "{target:?}"
            );
            let child_node = runtime
                .store
                .node(child.hash)
                .expect("the selected repair node read succeeds")
                .expect("the selected repair target remains retained");
            assert_eq!(
                child_node.aux_delivery_ids,
                if committed {
                    vec![delivery.delivery_id]
                } else {
                    Vec::new()
                },
                "{target:?}"
            );
            let stored_deliveries = runtime
                .store
                .aux_deliveries(child.hash)
                .expect("the selected repair auxiliary index is readable");
            assert_eq!(
                stored_deliveries,
                if committed {
                    vec![delivery]
                } else {
                    Vec::new()
                },
                "{target:?}"
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
                    .expect("the paired marker read succeeds")
                    .is_some(),
                committed,
                "{target:?}"
            );
            assert_eq!(
                memory_swapped.load(Ordering::SeqCst),
                matches!(
                    target,
                    FaultPoint::BeforePublish
                        | FaultPoint::AfterPublish
                        | FaultPoint::BeforeReactorObserve
                ),
                "{target:?}"
            );
            assert_eq!(
                runtime.publisher().snapshot().state_version,
                if published {
                    committed_version
                } else {
                    before.state_version
                },
                "{target:?}"
            );
            drop(runtime);
            drop(db);

            let (reopened, report) = HeaderChainStore::new(open(&db_config, &network))
                .startup(&engine_config)
                .expect("the selected-repair crash boundary reopens coherently");
            assert_eq!(
                reopened.publisher().snapshot(),
                report.current,
                "{target:?}"
            );
            assert_eq!(
                report.current.state_version,
                if committed {
                    committed_version
                } else {
                    before.state_version
                },
                "{target:?}"
            );
            assert_eq!(report.current.frontiers, before.frontiers, "{target:?}");
            let reopened_child = reopened
                .store
                .node(child.hash)
                .expect("the reopened selected repair node read succeeds")
                .expect("the reopened selected repair target remains retained");
            assert_eq!(
                reopened_child.aux_delivery_ids,
                if committed {
                    vec![delivery.delivery_id]
                } else {
                    Vec::new()
                },
                "{target:?}"
            );
            assert_eq!(
                reopened
                    .store
                    .aux_deliveries(child.hash)
                    .expect("the reopened selected repair auxiliary index is readable"),
                if committed {
                    vec![delivery]
                } else {
                    Vec::new()
                },
                "{target:?}"
            );
            let reopened_marker_cf = reopened
                .store
                .cf(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT)
                .expect("the reopened marker column family is open");
            assert_eq!(
                reopened
                    .store
                    .db
                    .raw_get_cf(&reopened_marker_cf, &marker_key)
                    .expect("the reopened paired marker read succeeds")
                    .is_some(),
                committed,
                "{target:?}"
            );
        }
    }

    #[test]
    fn aud_14_aux_authentication_reopens_complete_before_or_after() {
        const AUX_FAULT_POINTS: [FaultPoint; 11] = [
            FaultPoint::AfterSnapshot,
            FaultPoint::AfterVersionCheck,
            // Auxiliary evidence changes an existing delivery row, not its header node.
            FaultPoint::AfterEachIndexWrite,
            FaultPoint::AfterProjectionWrite,
            FaultPoint::AfterMetadataWrite,
            FaultPoint::BeforeDbCommit,
            FaultPoint::AfterDbCommit,
            FaultPoint::BeforeMemorySwap,
            FaultPoint::BeforePublish,
            FaultPoint::AfterPublish,
            FaultPoint::BeforeReactorObserve,
        ];

        for (index, target) in AUX_FAULT_POINTS.into_iter().enumerate() {
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
                .initialize(metadata, anchor.clone())
                .expect("the empty schema initializes");
            let (runtime, _) = store
                .startup(&engine_config)
                .expect("the initial store audits");
            let initial = runtime.publisher().snapshot();
            let anchor_frontier = Frontier::new(anchor.height, anchor.hash);
            let lease = runtime
                .reader()
                .validation_context(anchor.hash)
                .expect("the anchor validation context is coherent")
                .expect("the initialized anchor is retained");
            let rules = HeaderRules::for_validation_lease(network.clone(), &lease)
                .expect("the authenticated regtest policy is valid");
            let marker = u8::try_from(index + 0xe0).expect("the fault-point list fits in u8");

            let mut current_header = *anchor.header;
            current_header.previous_block_hash = anchor.hash;
            current_header.time += chrono::Duration::seconds(1);
            current_header.nonce.0[0] = marker;
            let current_header = Arc::new(current_header);
            let mut boundary_header = *current_header;
            boundary_header.previous_block_hash = current_header.hash();
            boundary_header.time += chrono::Duration::seconds(1);
            boundary_header.nonce.0[0] = marker.wrapping_add(1);
            let boundary_header = Arc::new(boundary_header);
            let headers = [current_header.clone(), boundary_header.clone()];
            let batch = zakura_header_chain::prepare_headers(
                HeaderBatchInput::new(&headers),
                &lease,
                &rules,
                &SystemClock,
            )
            .expect("the auxiliary fixture headers pass production validation");
            let current_height = anchor
                .height
                .next()
                .expect("the genesis anchor has a next height");
            let boundary_height = current_height
                .next()
                .expect("the first child has a next height");
            let current = Frontier::new(current_height, current_header.hash());
            let boundary = Frontier::new(boundary_height, boundary_header.hash());
            let insertion_owner = WorkOwner {
                state_version: initial.state_version,
                header_generation: initial.header_generation,
                verified_generation: None,
                branch: BranchId::new(anchor.hash, boundary.hash),
                session_id: 21,
                request_id: NonZeroU64::new(22).expect("twenty-two is nonzero"),
            };
            let source = SourceId::from_digest([marker.wrapping_add(2); 32]);
            let delivery = AuxDelivery {
                delivery_id: EvidenceId::from_digest([marker.wrapping_add(3); 32]),
                header_hash: current.hash,
                source,
                owner: insertion_owner,
                body_size: zakura_header_chain::BodySizeHint::Unknown,
                tree_aux: Some(zakura_header_chain::TreeAuxRecordV1 {
                    height: current.height,
                    sapling_root: Default::default(),
                    orchard_root: Default::default(),
                    ironwood_root: Default::default(),
                    sapling_tx_count: 1,
                    orchard_tx_count: 2,
                    ironwood_tx_count: 3,
                    auth_data_root: zakura_chain::block::merkle::AuthDataRoot::from(
                        [marker.wrapping_add(4); 32],
                    ),
                }),
                authentication: zakura_header_chain::AuxAuthentication::Unauthenticated,
            };
            let insertion_context = TransitionContext {
                config: &engine_config,
                clock: &SystemClock,
                full_state_authority: None,
                retention_references: &[],
            };
            runtime
                .apply(
                    TransitionRequest {
                        expected_version: initial.state_version,
                        event: TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
                            owner: insertion_owner,
                            source,
                            parent_hash: anchor.hash,
                            target_tip_hash: boundary.hash,
                            completion: TargetCompletion::TargetComplete {
                                common_ancestor: anchor_frontier,
                            },
                            batch,
                            aux: vec![delivery],
                        })),
                    },
                    &insertion_context,
                )
                .expect("the unauthenticated delivery inserts with its exact headers");

            let before = runtime.publisher().snapshot();
            let evidence = EvidenceId::from_digest([marker.wrapping_add(5); 32]);
            let authentication = zakura_header_chain::AuxAuthentication::Authenticated {
                evidence,
                boundary_hash: boundary.hash,
            };
            let authority = Authority(evidence);
            let context = TransitionContext {
                config: &engine_config,
                clock: &SystemClock,
                full_state_authority: Some(&authority),
                retention_references: &[],
            };
            let request = TransitionRequest {
                expected_version: before.state_version,
                event: TransitionEvent::AuxEvidence(Box::new(zakura_header_chain::AuxEvidence {
                    owner: WorkOwner {
                        state_version: before.state_version,
                        header_generation: before.header_generation,
                        verified_generation: Some(before.verified_generation),
                        branch: BranchId::new(anchor.hash, boundary.hash),
                        session_id: insertion_owner.session_id,
                        request_id: insertion_owner.request_id,
                    },
                    deliveries: vec![delivery],
                    authentication,
                })),
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
                .expect("the paired auxiliary marker can be staged");
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
            let published = matches!(
                target,
                FaultPoint::AfterPublish | FaultPoint::BeforeReactorObserve
            );
            let committed_version = before
                .state_version
                .checked_next()
                .expect("the short fixture state version can advance");
            let durable = runtime
                .store
                .snapshot()
                .expect("the auxiliary snapshot read succeeds");
            assert_eq!(
                durable.state_version,
                if committed {
                    committed_version
                } else {
                    before.state_version
                },
                "{target:?}"
            );
            assert_eq!(durable.frontiers, before.frontiers, "{target:?}");
            assert_eq!(
                durable.header_generation, before.header_generation,
                "{target:?}"
            );
            assert_eq!(
                durable.verified_generation, before.verified_generation,
                "{target:?}"
            );
            let stored_delivery = runtime
                .store
                .aux_deliveries(current.hash)
                .expect("the auxiliary row read succeeds");
            assert_eq!(stored_delivery.len(), 1, "{target:?}");
            assert_eq!(
                stored_delivery[0].authentication,
                if committed {
                    authentication
                } else {
                    zakura_header_chain::AuxAuthentication::Unauthenticated
                },
                "{target:?}"
            );
            let current_node = runtime
                .store
                .node(current.hash)
                .expect("the auxiliary header node read succeeds")
                .expect("the auxiliary header remains retained");
            assert_eq!(
                current_node.aux_delivery_ids,
                vec![delivery.delivery_id],
                "{target:?}"
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
                    .expect("the paired marker read succeeds")
                    .is_some(),
                committed,
                "{target:?}"
            );
            assert_eq!(
                memory_swapped.load(Ordering::SeqCst),
                matches!(
                    target,
                    FaultPoint::BeforePublish
                        | FaultPoint::AfterPublish
                        | FaultPoint::BeforeReactorObserve
                ),
                "{target:?}"
            );
            assert_eq!(
                runtime.publisher().snapshot().state_version,
                if published {
                    committed_version
                } else {
                    before.state_version
                },
                "{target:?}"
            );
            drop(runtime);
            drop(db);

            let (reopened, report) = HeaderChainStore::new(open(&db_config, &network))
                .startup(&engine_config)
                .expect("the auxiliary crash boundary reopens coherently");
            assert_eq!(
                reopened.publisher().snapshot(),
                report.current,
                "{target:?}"
            );
            assert_eq!(
                report.current.state_version,
                if committed {
                    committed_version
                } else {
                    before.state_version
                },
                "{target:?}"
            );
            assert_eq!(report.current.frontiers, before.frontiers, "{target:?}");
            let reopened_delivery = reopened
                .store
                .aux_deliveries(current.hash)
                .expect("the reopened auxiliary row is readable");
            assert_eq!(reopened_delivery.len(), 1, "{target:?}");
            assert_eq!(
                reopened_delivery[0].authentication,
                if committed {
                    authentication
                } else {
                    zakura_header_chain::AuxAuthentication::Unauthenticated
                },
                "{target:?}"
            );
            let reopened_marker_cf = reopened
                .store
                .cf(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT)
                .expect("the reopened marker column family is open");
            assert_eq!(
                reopened
                    .store
                    .db
                    .raw_get_cf(&reopened_marker_cf, &marker_key)
                    .expect("the reopened paired marker read succeeds")
                    .is_some(),
                committed,
                "{target:?}"
            );
        }
    }

    #[test]
    fn aud_14_two_delivery_aux_rejection_never_partially_commits() {
        const REJECTION_FAULT_CASES: [(FaultPoint, usize); 5] = [
            // Candidate replacement is the first index boundary; these are the two aux rows.
            (FaultPoint::AfterEachIndexWrite, 2),
            (FaultPoint::AfterEachIndexWrite, 3),
            (FaultPoint::AfterDbCommit, 1),
            (FaultPoint::BeforePublish, 1),
            (FaultPoint::AfterPublish, 1),
        ];

        for (index, (target, target_occurrence)) in REJECTION_FAULT_CASES.into_iter().enumerate() {
            let cache = tempfile::tempdir().expect("the test cache directory is created");
            let db_config = Config {
                cache_dir: cache.path().to_owned(),
                ephemeral: false,
                debug_skip_non_finalized_state_backup_task: true,
                ..Config::default()
            };
            let (engine_config, mut anchor, metadata) = fixture();
            let network = engine_config.network.clone();
            let db = open(&db_config, &network);
            let store = HeaderChainStore::new(db.clone());
            store
                .initialize(metadata.clone(), anchor.clone())
                .expect("the empty schema initializes");
            let marker = u8::try_from(index + 0x80).expect("the rejection cases fit in u8");
            let delivery_owner = WorkScope::for_body_work(&metadata.snapshot())
                .bind(61, NonZeroU64::new(62).expect("sixty-two is nonzero"));
            let first = AuxDelivery {
                delivery_id: EvidenceId::from_digest([marker.wrapping_add(1); 32]),
                header_hash: anchor.hash,
                source: SourceId::from_digest([marker.wrapping_add(2); 32]),
                owner: delivery_owner,
                body_size: zakura_header_chain::BodySizeHint::Unknown,
                tree_aux: None,
                authentication: zakura_header_chain::AuxAuthentication::Unauthenticated,
            };
            let second = AuxDelivery {
                delivery_id: EvidenceId::from_digest([marker.wrapping_add(3); 32]),
                source: SourceId::from_digest([marker.wrapping_add(4); 32]),
                ..first
            };
            anchor
                .aux_delivery_ids
                .extend([first.delivery_id, second.delivery_id]);
            let mut seed = DiskWriteBatch::new();
            store
                .put_value(
                    &mut seed,
                    HEADER_NODE_BY_HASH,
                    anchor.hash.0,
                    &HeaderNodeDisk::from_domain(&anchor),
                )
                .expect("the two-delivery anchor node encodes");
            for delivery in [first, second] {
                store
                    .put_value(
                        &mut seed,
                        HEADER_AUX_DELIVERY,
                        HeaderAuxDeliveryKey {
                            header: delivery.header_hash,
                            delivery: delivery.delivery_id,
                        }
                        .as_bytes(),
                        &HeaderAuxDeliveryDisk(delivery),
                    )
                    .expect("the unauthenticated auxiliary delivery encodes");
            }
            db.write(seed)
                .expect("the coherent two-delivery fixture commits");
            let (runtime, _) = store
                .startup(&engine_config)
                .expect("the two-delivery fixture audits");
            let before = runtime.publisher().snapshot();
            let evidence = EvidenceId::from_digest([marker.wrapping_add(5); 32]);
            let authentication = zakura_header_chain::AuxAuthentication::Rejected { evidence };
            let authority = Authority(evidence);
            let context = TransitionContext {
                config: &engine_config,
                clock: &SystemClock,
                full_state_authority: Some(&authority),
                retention_references: &[],
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
                .expect("the paired rejection marker can be staged");
            let memory_swapped = Arc::new(AtomicBool::new(false));
            let swap_probe = memory_swapped.clone();
            let mut index_occurrence = 0;
            let result = runtime.apply_combined_with_fault(
                TransitionRequest {
                    expected_version: before.state_version,
                    event: TransitionEvent::AuxEvidence(Box::new(
                        zakura_header_chain::AuxEvidence {
                            owner: WorkScope::for_body_work(&before)
                                .bind(delivery_owner.session_id, delivery_owner.request_id),
                            deliveries: vec![first, second],
                            authentication,
                        },
                    )),
                },
                &context,
                full_state_batch,
                move || swap_probe.store(true, Ordering::SeqCst),
                |point| {
                    if point == FaultPoint::AfterEachIndexWrite {
                        index_occurrence += 1;
                    }
                    if point == target
                        && (point != FaultPoint::AfterEachIndexWrite
                            || index_occurrence == target_occurrence)
                    {
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
                FaultPoint::AfterDbCommit | FaultPoint::BeforePublish | FaultPoint::AfterPublish
            );
            let published = target == FaultPoint::AfterPublish;
            let committed_version = before
                .state_version
                .checked_next()
                .expect("the short fixture state version can advance");
            let durable = runtime
                .store
                .snapshot()
                .expect("the rejection snapshot is readable");
            assert_eq!(
                durable.state_version,
                if committed {
                    committed_version
                } else {
                    before.state_version
                },
                "{target:?} occurrence {target_occurrence}"
            );
            assert_eq!(
                durable.frontiers, before.frontiers,
                "{target:?} occurrence {target_occurrence}"
            );
            let stored = runtime
                .store
                .aux_deliveries(anchor.hash)
                .expect("the rejected delivery rows are readable");
            assert_eq!(stored.len(), 2);
            assert!(stored.iter().all(|delivery| {
                delivery.authentication
                    == if committed {
                        authentication
                    } else {
                        zakura_header_chain::AuxAuthentication::Unauthenticated
                    }
            }));
            assert_eq!(
                runtime
                    .store
                    .node(anchor.hash)
                    .expect("the rejection anchor node read succeeds")
                    .expect("the rejection anchor remains retained")
                    .aux_delivery_ids,
                vec![first.delivery_id, second.delivery_id]
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
                    .expect("the paired marker read succeeds")
                    .is_some(),
                committed,
                "{target:?} occurrence {target_occurrence}"
            );
            assert_eq!(
                memory_swapped.load(Ordering::SeqCst),
                matches!(target, FaultPoint::BeforePublish | FaultPoint::AfterPublish),
                "{target:?} occurrence {target_occurrence}"
            );
            assert_eq!(
                runtime.publisher().snapshot().state_version,
                if published {
                    committed_version
                } else {
                    before.state_version
                },
                "{target:?} occurrence {target_occurrence}"
            );
            drop(runtime);
            drop(db);

            let (reopened, report) = HeaderChainStore::new(open(&db_config, &network))
                .startup(&engine_config)
                .expect("the two-delivery rejection reopens coherently");
            assert_eq!(reopened.publisher().snapshot(), report.current);
            assert_eq!(
                report.current.state_version,
                if committed {
                    committed_version
                } else {
                    before.state_version
                },
                "{target:?} occurrence {target_occurrence}"
            );
            let reopened_deliveries = reopened
                .store
                .aux_deliveries(anchor.hash)
                .expect("the reopened rejected delivery rows are readable");
            assert_eq!(reopened_deliveries.len(), 2);
            assert!(reopened_deliveries.iter().all(|delivery| {
                delivery.authentication
                    == if committed {
                        authentication
                    } else {
                        zakura_header_chain::AuxAuthentication::Unauthenticated
                    }
            }));
            let reopened_marker_cf = reopened
                .store
                .cf(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT)
                .expect("the reopened marker column family is open");
            assert_eq!(
                reopened
                    .store
                    .db
                    .raw_get_cf(&reopened_marker_cf, &marker_key)
                    .expect("the reopened paired marker read succeeds")
                    .is_some(),
                committed,
                "{target:?} occurrence {target_occurrence}"
            );
        }
    }

    #[test]
    fn aud_14_migrated_pin_refutation_fails_closed_at_every_reachable_boundary() {
        const REFUTATION_FAULT_POINTS: [FaultPoint; 7] = [
            FaultPoint::AfterSnapshot,
            FaultPoint::AfterVersionCheck,
            // Refutation changes only the incident alarm, not a retained node.
            FaultPoint::AfterEachIndexWrite,
            FaultPoint::AfterProjectionWrite,
            FaultPoint::AfterMetadataWrite,
            FaultPoint::BeforeDbCommit,
            FaultPoint::AfterDbCommit,
        ];

        for (index, target) in REFUTATION_FAULT_POINTS.into_iter().enumerate() {
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
            let network = integrated_config.network.clone();
            let db = open(&db_config, &network);
            let store = HeaderChainStore::new(db.clone());
            store
                .initialize(metadata, anchor)
                .expect("the headers-only schema initializes");
            let migrated_record = FinalityRecord {
                previous: anchor_frontier,
                current: anchor_frontier,
                source: FinalitySource::MigratedHeadersOnly,
                epoch: FinalityEpoch::new(0),
            };
            let mut migration_batch = DiskWriteBatch::new();
            store
                .put_value(
                    &mut migration_batch,
                    HEADER_FINALITY_HISTORY,
                    HeaderFinalityKey(migrated_record.epoch).as_bytes(),
                    &HeaderFinalityRecordDisk(migrated_record),
                )
                .expect("the migrated finality record encodes");
            db.write(migration_batch)
                .expect("the migrated finality record commits");
            audit_store(&store, &headers_only_config)
                .expect("the headers-only source store is coherent");
            let (runtime, _) = store
                .migrate_headers_only_to_integrated(&integrated_config, anchor_frontier)
                .expect("the explicit mode migration succeeds before publication");
            let before = runtime.publisher().snapshot();
            assert_eq!(before.mode, EngineMode::Integrated);
            assert_eq!(before.alarms.migrated_pin_refuted, None);

            let marker = u8::try_from(index + 0xf0).expect("the fault-point list fits in u8");
            let evidence = EvidenceId::from_digest([marker; 32]);
            let authority = Authority(evidence);
            let context = TransitionContext {
                config: &integrated_config,
                clock: &SystemClock,
                full_state_authority: Some(&authority),
                retention_references: &[],
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
                .expect("the paired refutation marker can be staged");
            let memory_swapped = Arc::new(AtomicBool::new(false));
            let swap_probe = memory_swapped.clone();
            let result = runtime.apply_combined_with_fault(
                TransitionRequest {
                    expected_version: before.state_version,
                    event: TransitionEvent::MigratedPinRefutation(
                        zakura_header_chain::MigratedPinRefutation {
                            full_state_transition_id: evidence,
                            pin: anchor_frontier,
                            invalid_header: anchor_frontier,
                            rule: BodyRuleId::new("aud14.migrated_pin_refutation"),
                        },
                    ),
                },
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

            let committed = target == FaultPoint::AfterDbCommit;
            let committed_version = before
                .state_version
                .checked_next()
                .expect("the short fixture state version can advance");
            let durable = runtime
                .store
                .snapshot()
                .expect("the refutation snapshot read succeeds");
            assert_eq!(
                durable.state_version,
                if committed {
                    committed_version
                } else {
                    before.state_version
                },
                "{target:?}"
            );
            assert_eq!(
                durable.alarms.migrated_pin_refuted,
                committed.then_some(anchor_frontier),
                "{target:?}"
            );
            assert_eq!(durable.frontiers, before.frontiers, "{target:?}");
            let marker_cf = runtime
                .store
                .cf(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT)
                .expect("the marker column family is open");
            assert_eq!(
                runtime
                    .store
                    .db
                    .raw_get_cf(&marker_cf, &marker_key)
                    .expect("the paired marker read succeeds")
                    .is_some(),
                committed,
                "{target:?}"
            );
            assert!(!memory_swapped.load(Ordering::SeqCst), "{target:?}");
            assert_eq!(runtime.publisher().snapshot(), before, "{target:?}");
            drop(runtime);
            drop(db);

            let reopened_store = HeaderChainStore::new(open(&db_config, &network));
            let reopened_metadata = reopened_store
                .metadata()
                .expect("the refutation metadata reopens");
            assert_eq!(
                reopened_metadata.alarms.migrated_pin_refuted,
                committed.then_some(anchor_frontier),
                "{target:?}"
            );
            let reopened_marker_cf = reopened_store
                .cf(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT)
                .expect("the reopened marker column family is open");
            assert_eq!(
                reopened_store
                    .db
                    .raw_get_cf(&reopened_marker_cf, &marker_key)
                    .expect("the reopened paired marker read succeeds")
                    .is_some(),
                committed,
                "{target:?}"
            );
            if committed {
                assert!(matches!(
                    reopened_store.startup(&integrated_config),
                    Err(HeaderChainStoreError::MigratedPinRefuted { pin })
                        if pin == anchor_frontier
                ));
            } else {
                let (reopened, report) = reopened_store
                    .startup(&integrated_config)
                    .expect("the uncommitted refutation reopens normally");
                assert_eq!(report.current, before, "{target:?}");
                assert_eq!(reopened.publisher().snapshot(), before, "{target:?}");
            }
        }
    }

    #[test]
    fn aud_14_no_change_crash_points_preserve_the_paired_full_state_transaction() {
        for (index, target) in FaultPoint::NO_CHANGE.into_iter().enumerate() {
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
            let marker = u8::try_from(index + 0x40).expect("the fault-point list fits in u8");
            let evidence = EvidenceId::from_digest([marker; 32]);
            let authority = Authority(evidence);
            let context = TransitionContext {
                config: &engine_config,
                clock: &SystemClock,
                full_state_authority: Some(&authority),
                retention_references: &[],
            };
            let request = TransitionRequest {
                expected_version: metadata.state_version,
                event: TransitionEvent::BodyEvidence(BodyEvidence::PayloadMismatch(
                    BodyPayloadMismatch {
                        evidence,
                        requested: anchor.hash,
                        delivered: block::Hash([marker; 32]),
                        kind: BodyCommitmentKind::HeaderHash,
                        source: SourceId::from_digest([marker.wrapping_add(1); 32]),
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
                .expect("the paired full-state marker can be staged");
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
                    .expect("the paired marker read succeeds")
                    .is_some(),
                committed,
                "{target:?}"
            );
            assert_eq!(
                memory_swapped.load(Ordering::SeqCst),
                target == FaultPoint::BeforeReactorObserve,
                "{target:?}"
            );
            assert_eq!(
                runtime.publisher().snapshot(),
                metadata.snapshot(),
                "a no-change transition never publishes at {target:?}"
            );
            drop(runtime);
            drop(db);

            let (reopened, report) = HeaderChainStore::new(open(&db_config, &network))
                .startup(&engine_config)
                .expect("the no-change crash boundary reopens coherently");
            assert_eq!(report.current, metadata.snapshot(), "{target:?}");
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
            let reopened_marker_cf = reopened
                .store
                .cf(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT)
                .expect("the reopened marker column family is open");
            assert_eq!(
                reopened
                    .store
                    .db
                    .raw_get_cf(&reopened_marker_cf, &marker_key)
                    .expect("the reopened paired marker read succeeds")
                    .is_some(),
                committed,
                "{target:?}"
            );
        }
    }
}
