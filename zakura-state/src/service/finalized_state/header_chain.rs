//! Durable adapter for the fork-aware header-chain transition engine.

#![allow(dead_code)] // Constructed by the full-state migration and service wiring in PR-9.

use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::{DateTime, TimeZone, Utc};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{sync::watch, time::Instant};
use zakura_chain::{block, parallel::commitment_aux::BlockCommitmentRoots};
use zakura_header_chain::{
    audit_store, ApplyResult, AuxDelivery, AuxDelta, ChangeSet, CounterExhausted,
    DurableTransitionFacts, EligibilityReason, EngineConfig, EngineMetadata, EngineMode,
    EngineSnapshot, EvidenceId, FinalityRecord, FinalitySource, Frontier,
    FullStateEvidenceAuthority, FullStateFinalized, HeaderChainEngine, HeaderLocator, HeaderNode,
    MemHeaderStore, NoChangeReceipt, RecoveryFailure, RecoveryPlan, RecoveryRepair, SourceId,
    StaleReceipt, StateVersion, StoreAuditRead, StoreError, SystemClock, TransitionContext,
    TransitionEvent, TransitionFailure, TransitionRequest, ValidationContextRecord,
    ValidationLease, VerifiedChainChanged, VerifiedChangeCause, VerifiedHeaderRef, WorkOwner,
    WorkScope,
};

use crate::{
    RetainedPathLease, RetainedPathLeaseOutcome, RetainedPathPage, RetainedPathReadOutcome,
    MAX_RETAINED_PATH_LEASES,
};

use super::{
    disk_format::{
        header_chain::{
            EligibilityReasonKind, HeaderAuxDeliveryKey, HeaderChildKey, HeaderDeferredKey,
            HeaderEligibilityRootKey, HeaderFinalityKey, HeaderHeightKey,
        },
        header_chain_values::{
            HeaderChainValueError, HeaderEligibilityReasonDisk, HeaderNodeDisk,
            HeaderValidationContextDisk,
        },
        FallibleDiskValue, FromDisk, IntoDisk, RawBytes,
    },
    DiskDb, DiskWriteBatch, WriteDisk, HEADER_AUX_DELIVERY, HEADER_CHILD, HEADER_DEFERRED,
    HEADER_ELIGIBILITY_ROOT, HEADER_ENGINE_META, HEADER_FINALITY_HISTORY, HEADER_NODE_BY_HASH,
    HEADER_SELECTED, HEADER_VALIDATION_CONTEXT, HEADER_VERIFIED,
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
    /// A prepared full-state mutation lost its exact serialized header-chain authority.
    #[error(
        "prepared full-state/header transition became stale at durable version {current_version:?}"
    )]
    StaleFullStateTransition {
        /// Current durable version observed instead of committing.
        current_version: StateVersion,
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
    transition_engine: Arc<Mutex<HeaderChainEngine>>,
}

fn load_transition_engine(
    store: &HeaderChainStore,
) -> Result<HeaderChainEngine, HeaderChainStoreError> {
    let metadata = store.metadata()?;
    let graph =
        MemHeaderStore::from_audited_nodes(metadata.frontiers.finalized, store.all_nodes()?)
            .map_err(|_| HeaderChainStoreError::Incoherent("audited node graph is invalid"))?;
    HeaderChainEngine::from_audited_state(
        graph,
        metadata,
        store.selected_projection()?,
        store.verified_projection()?,
        store.all_aux_deliveries()?,
    )
    .map_err(|_| HeaderChainStoreError::Incoherent("audited engine state is invalid"))
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

    /// Return the earliest durable deferred-header deadline.
    pub(in crate::service) fn earliest_deferred(
        &self,
    ) -> Result<Option<DateTime<Utc>>, HeaderChainStoreError> {
        self.store
            .earliest_deferred()
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

    #[cfg(test)]
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
        #[cfg(test)]
        {
            self.apply_combined_inner(
                request,
                context,
                full_state_batch,
                memory_swap,
                None,
                |_| Ok(()),
            )
        }
        #[cfg(not(test))]
        {
            self.apply_combined_inner(request, context, full_state_batch, memory_swap, None)
        }
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
        #[cfg(test)]
        {
            self.apply_combined_inner(
                request,
                context,
                full_state_batch,
                memory_swap,
                Some(expected_verified),
                |_| Ok(()),
            )
        }
        #[cfg(not(test))]
        {
            self.apply_combined_inner(
                request,
                context,
                full_state_batch,
                memory_swap,
                Some(expected_verified),
            )
        }
    }

    #[cfg(test)]
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
        self.apply_combined_inner(request, context, full_state_batch, memory_swap, None, fault)
    }

    fn apply_combined_inner<M>(
        &self,
        request: TransitionRequest,
        context: &TransitionContext<'_>,
        full_state_batch: DiskWriteBatch,
        memory_swap: M,
        expected_verified: Option<Frontier>,
        #[cfg(test)] mut fault: impl FnMut(FaultPoint) -> Result<(), HeaderChainStoreError>,
    ) -> Result<ApplyResult, HeaderChainStoreError>
    where
        M: FnOnce(),
    {
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        let mut transition_engine = self
            .transition_engine
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
        let before = transition_engine.snapshot();
        if let Some(pin) = before.alarms.migrated_pin_refuted {
            return Err(HeaderChainStoreError::MigratedPinRefuted { pin });
        }
        let event = request.event.idempotency_key();
        let branch = request.event.work_owner().map(|owner| owner.branch);
        let is_idempotent_replay =
            event.is_some_and(|event| transition_engine.metadata().last_transition_id == event);
        if !is_idempotent_replay && request.expected_version != before.state_version {
            return Ok(ApplyResult::Stale(StaleReceipt {
                current_version: before.state_version,
                branch,
            }));
        }
        let durable = if is_idempotent_replay {
            DurableTransitionFacts::None
        } else {
            match &request.event {
                TransitionEvent::InsertHeaders(event) => DurableTransitionFacts::ValidationContext(
                    self.store.validation_context(event.parent_hash)?,
                ),
                TransitionEvent::MigratedPinRefutation(event) => {
                    DurableTransitionFacts::MigratedFinalityPin(
                        self.store
                            .is_migrated_finality_pin(event.pin)?
                            .then_some(event.pin),
                    )
                }
                _ => DurableTransitionFacts::None,
            }
        };
        let transition = match transition_engine.apply(request, &context, durable) {
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
            let actual = transition.change_set().metadata.frontiers.verified_best;
            if expected != actual {
                return Err(HeaderChainStoreError::VerifiedFrontierMismatch { expected, actual });
            }
        }
        if transition.is_no_change() {
            #[cfg(test)]
            fault(FaultPoint::BeforeCommit)?;
            self.store.db.write(full_state_batch)?;
            #[cfg(test)]
            fault(FaultPoint::AfterCommit)?;
            memory_swap();
            #[cfg(test)]
            fault(FaultPoint::AfterMemorySwap)?;
            return Ok(ApplyResult::NoChange(NoChangeReceipt {
                state_version: transition.before().state_version,
                event,
            }));
        }

        let current = transition.change_set().metadata.snapshot();
        let migrated_pin_refuted = transition.change_set().metadata.alarms.migrated_pin_refuted;
        let batch = self
            .store
            .batch_for_combined(transition.change_set(), full_state_batch)?;
        #[cfg(test)]
        fault(FaultPoint::BeforeCommit)?;
        self.store.db.write(batch)?;
        *transition_engine = transition.into_projected_engine();
        #[cfg(test)]
        fault(FaultPoint::AfterCommit)?;
        if let Some(pin) = migrated_pin_refuted {
            return Err(HeaderChainStoreError::MigratedPinRefuted { pin });
        }
        memory_swap();
        #[cfg(test)]
        fault(FaultPoint::AfterMemorySwap)?;
        self.publisher.publish(current);
        #[cfg(test)]
        fault(FaultPoint::AfterPublish)?;
        Ok(ApplyResult::Committed)
    }
}

/// Deterministic state-writer and observer boundaries used by the crash harness.
#[cfg(test)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum FaultPoint {
    BeforeCommit,
    AfterCommit,
    AfterMemorySwap,
    AfterPublish,
}

#[cfg(test)]
impl FaultPoint {
    /// Complete ordered state-writer crash surface used by deterministic recovery tests.
    pub const ALL: [Self; 4] = [
        Self::BeforeCommit,
        Self::AfterCommit,
        Self::AfterMemorySwap,
        Self::AfterPublish,
    ];

    /// Ordered crash surface reached by a transition with no header-chain changes.
    pub const NO_CHANGE: [Self; 3] = [Self::BeforeCommit, Self::AfterCommit, Self::AfterMemorySwap];

    const fn commit_completed(self) -> bool {
        matches!(
            self,
            Self::AfterCommit | Self::AfterMemorySwap | Self::AfterPublish
        )
    }

    const fn memory_swap_completed(self) -> bool {
        matches!(self, Self::AfterMemorySwap | Self::AfterPublish)
    }

    const fn publication_completed(self) -> bool {
        matches!(self, Self::AfterPublish)
    }
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
        #[cfg(test)]
        {
            self.startup_inner(config, |_| Ok(()))
        }
        #[cfg(not(test))]
        {
            self.startup_inner(config)
        }
    }

    #[cfg(test)]
    fn startup_with_fault<F>(
        self,
        config: &EngineConfig,
        fault: F,
    ) -> Result<(HeaderChainRuntime, StartupReport), HeaderChainStoreError>
    where
        F: FnMut(FaultPoint) -> Result<(), HeaderChainStoreError>,
    {
        self.startup_inner(config, fault)
    }

    fn startup_inner(
        self,
        config: &EngineConfig,
        #[cfg(test)] mut fault: impl FnMut(FaultPoint) -> Result<(), HeaderChainStoreError>,
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
            #[cfg(test)]
            fault(FaultPoint::BeforeCommit)?;
            self.db.write(self.recovery_batch(&plan)?)?;
            #[cfg(test)]
            fault(FaultPoint::AfterCommit)?;
        }
        let transition_engine = Arc::new(Mutex::new(load_transition_engine(&self)?));
        let current = plan.metadata.snapshot();
        let report = StartupReport {
            previous,
            current: current.clone(),
            repairs,
            publication_allowed: true,
        };
        let publisher = Publisher::new(current);
        #[cfg(test)]
        fault(FaultPoint::AfterPublish)?;
        drop(writer);
        Ok((
            HeaderChainRuntime {
                store: self,
                publisher,
                leases: Arc::new(Mutex::new(RetainedPathLeaseRegistry::default())),
                transition_engine,
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
                &record,
            )?;
        }
        self.put_value(&mut batch, HEADER_ENGINE_META, METADATA_KEY, &metadata)?;
        self.db.write(batch)?;

        let target = audit_store(&self, integrated_config)?;
        repairs.extend(target.repairs.iter().copied());
        if !target.is_clean() {
            self.db.write(self.recovery_batch(&target)?)?;
        }
        let transition_engine = Arc::new(Mutex::new(load_transition_engine(&self)?));
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
                transition_engine,
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

        let max_nodes = config.limits.max_non_finalized_nodes.get();
        if finalized_path.len().saturating_add(restored_path.len()) > max_nodes {
            if restored_path.len() > max_nodes {
                return Err(TransitionFailure::ResourceStalled.into());
            }
            for chunk in finalized_path.chunks(max_nodes) {
                let chunk = chunk.to_vec();
                let chunk_tip = chunk
                    .last()
                    .map(|header| Frontier::new(header.height, header.hash))
                    .ok_or(HeaderChainStoreError::Incoherent(
                        "oversized reconciliation has an empty finalized chunk",
                    ))?;
                self.reconcile_verified_path(config, chunk)?;
                self.reconcile_finalized(config, chunk_tip)?;
            }
            self.reconcile_verified_path(config, restored_path)?;
        } else {
            let mut authoritative_path = finalized_path;
            authoritative_path.extend(restored_path);
            self.reconcile_verified_path(config, authoritative_path)?;
            self.reconcile_finalized(config, full_state_finalized)?;
        }

        let final_audit = audit_store(&self, config)?;
        repairs.extend(final_audit.repairs.iter().copied());
        if !final_audit.is_clean() {
            self.db.write(self.recovery_batch(&final_audit)?)?;
        }
        let transition_engine = Arc::new(Mutex::new(load_transition_engine(&self)?));
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
                transition_engine,
            },
            report,
        ))
    }

    fn reconcile_verified_path(
        &self,
        config: &EngineConfig,
        authoritative_path: Vec<VerifiedHeaderRef>,
    ) -> Result<(), HeaderChainStoreError> {
        struct Authority(EvidenceId);

        impl FullStateEvidenceAuthority for Authority {
            fn authorizes(&self, evidence: EvidenceId) -> bool {
                evidence == self.0
            }
        }

        let snapshot = self.snapshot()?;
        let mut expected_projection = vec![snapshot.frontiers.finalized];
        expected_projection.extend(
            authoritative_path
                .iter()
                .map(|header| Frontier::new(header.height, header.hash)),
        );
        if self.verified_projection()? == expected_projection {
            return Ok(());
        }
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
        let engine = load_transition_engine(self)?;
        let transition = engine.apply(
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
            DurableTransitionFacts::None,
        )?;
        if !transition.is_no_change() {
            self.db.write(self.batch_for(transition.change_set())?)?;
        }
        Ok(())
    }

    fn reconcile_finalized(
        &self,
        config: &EngineConfig,
        full_state_finalized: Frontier,
    ) -> Result<(), HeaderChainStoreError> {
        struct Authority(EvidenceId);

        impl FullStateEvidenceAuthority for Authority {
            fn authorizes(&self, evidence: EvidenceId) -> bool {
                evidence == self.0
            }
        }

        let snapshot = self.snapshot()?;
        if snapshot.frontiers.finalized == full_state_finalized {
            return Ok(());
        }
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
        let engine = load_transition_engine(self)?;
        let transition = engine.apply(
            TransitionRequest {
                expected_version: snapshot.state_version,
                event: TransitionEvent::FullStateFinalized(FullStateFinalized {
                    full_state_transition_id: evidence,
                    new_finalized: full_state_finalized,
                    verified_path_proof: proof,
                }),
            },
            &context,
            DurableTransitionFacts::None,
        )?;
        if !transition.is_no_change() {
            self.db.write(self.batch_for(transition.change_set())?)?;
        }
        Ok(())
    }

    /// Bootstrap an empty header schema with one already-authenticated anchor.
    ///
    /// Migration calls this only while publication and normal writers are disabled.
    pub fn initialize(
        &self,
        metadata: EngineMetadata,
        anchor: HeaderNode,
    ) -> Result<(), HeaderChainStoreError> {
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
            selected_projection: zakura_header_chain::ProjectionDelta {
                remove_before: None,
                remove_from: None,
                put: vec![metadata.frontiers.finalized],
            },
            verified_projection: zakura_header_chain::ProjectionDelta {
                remove_before: None,
                remove_from: None,
                put: vec![metadata.frontiers.finalized],
            },
            eligibility_changes: Vec::new(),
            aux_changes: Vec::new(),
            finality_append: None,
            metadata: metadata.clone(),
        };
        self.db.write(self.batch_for(&change_set)?)?;
        Ok(())
    }

    fn batch_for(&self, changes: &ChangeSet) -> Result<DiskWriteBatch, HeaderChainStoreError> {
        self.batch_for_combined(changes, DiskWriteBatch::new())
    }

    fn batch_for_combined(
        &self,
        changes: &ChangeSet,
        mut batch: DiskWriteBatch,
    ) -> Result<DiskWriteBatch, HeaderChainStoreError> {
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
                    delivery.as_ref(),
                )?,
                AuxDelta::Delete {
                    header_hash,
                    delivery_id,
                } => self.delete_raw(
                    &mut batch,
                    HEADER_AUX_DELIVERY,
                    HeaderAuxDeliveryKey {
                        header: *header_hash,
                        delivery: *delivery_id,
                    }
                    .as_bytes(),
                )?,
            }
        }

        if let Some(record) = changes.finality_append {
            self.put_value(
                &mut batch,
                HEADER_FINALITY_HISTORY,
                HeaderFinalityKey(record.epoch).as_bytes(),
                &record,
            )?;
        }

        // The singleton logical root is deliberately enqueued last in the same atomic batch.
        self.put_value(
            &mut batch,
            HEADER_ENGINE_META,
            METADATA_KEY,
            &changes.metadata,
        )?;
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
        if plan.repairs.contains(&RecoveryRepair::SelectedProjection) {
            self.replace_projection(&mut batch, HEADER_SELECTED, &plan.selected_projection)?;
        }
        if plan.repairs.contains(&RecoveryRepair::VerifiedProjection) {
            self.replace_projection(&mut batch, HEADER_VERIFIED, &plan.verified_projection)?;
        }
        self.put_value(&mut batch, HEADER_ENGINE_META, METADATA_KEY, &plan.metadata)?;
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
        self.get_value::<EngineMetadata>(HEADER_ENGINE_META, METADATA_KEY)
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

    fn apply_projection(
        &self,
        batch: &mut DiskWriteBatch,
        family: &'static str,
        delta: &zakura_header_chain::ProjectionDelta,
    ) -> Result<(), HeaderChainStoreError> {
        if let Some(remove_before) = delta.remove_before {
            let upper = HeaderHeightKey(remove_before).as_bytes();
            for (key, _) in self.scan_range(family, &[], Some(&upper))? {
                if key.len() != 4 {
                    return Err(HeaderChainStoreError::Incoherent(
                        "invalid projection key width",
                    ));
                }
                self.delete_raw(batch, family, key)?;
            }
        }
        if let Some(remove_from) = delta.remove_from {
            let lower = HeaderHeightKey(remove_from).as_bytes();
            for (key, _) in self.scan_range(family, &lower, None)? {
                if key.len() != 4 {
                    return Err(HeaderChainStoreError::Incoherent(
                        "invalid projection key width",
                    ));
                }
                self.delete_raw(batch, family, key)?;
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

    fn get_value<V: FallibleDiskValue<Error = HeaderChainValueError>>(
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
        self.scan_range(family, &[], None)
    }

    fn scan_range(
        &self,
        family: &'static str,
        lower: &[u8],
        upper: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, HeaderChainStoreError> {
        let cf = self.cf(family)?;
        Ok(self.db.raw_range_cf(&cf, lower, upper)?)
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

    fn put_value<V: FallibleDiskValue<Error = HeaderChainValueError>>(
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

    fn visit_finality_records(
        &self,
        visitor: &mut dyn FnMut(FinalityRecord) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        for (key, value) in self
            .scan_raw(HEADER_FINALITY_HISTORY)
            .map_err(store_error)?
        {
            if key.len() != 8 {
                return Err(StoreError::Incoherent("invalid finality key width"));
            }
            let record = FinalityRecord::decode(&value)
                .map_err(|_| StoreError::Incoherent("invalid finality value"))?;
            if key != record.epoch.get().to_be_bytes() {
                return Err(StoreError::Incoherent("finality key/value mismatch"));
            }
            visitor(record)?;
        }
        Ok(())
    }

    fn finality_history(&self) -> Result<Vec<FinalityRecord>, StoreError> {
        let mut records = Vec::new();
        self.visit_finality_records(&mut |record| {
            records.push(record);
            Ok(())
        })?;
        Ok(records)
    }
}

fn preserve_headers_only_pin(mut record: FinalityRecord) -> FinalityRecord {
    if matches!(record.source, FinalitySource::HeadersOnlyDepth { .. }) {
        record.source = FinalitySource::MigratedHeadersOnly;
    }
    record
}

impl HeaderChainStore {
    pub(crate) fn snapshot(&self) -> Result<EngineSnapshot, StoreError> {
        Ok(self.metadata()?.snapshot())
    }

    pub(crate) fn metadata(&self) -> Result<EngineMetadata, StoreError> {
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

    fn selected_hash(&self, height: block::Height) -> Result<Option<block::Hash>, StoreError> {
        self.projection_hash(HEADER_SELECTED, height)
    }

    fn verified_hash(&self, height: block::Height) -> Result<Option<block::Hash>, StoreError> {
        self.projection_hash(HEADER_VERIFIED, height)
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
            let delivery = AuxDelivery::decode(&value)
                .map_err(|_| StoreError::Incoherent("invalid auxiliary value"))?;
            if delivery.header_hash != hash || key[32..] != delivery.delivery_id.digest() {
                return Err(StoreError::Incoherent("auxiliary key/value mismatch"));
            }
            deliveries.push(delivery);
        }
        deliveries.sort_unstable_by_key(|delivery| delivery.delivery_id);
        Ok(deliveries)
    }

    fn is_migrated_finality_pin(&self, pin: Frontier) -> Result<bool, StoreError> {
        let mut found = false;
        self.visit_finality_records(&mut |record| {
            found |= record.current == pin
                && matches!(record.source, FinalitySource::MigratedHeadersOnly);
            Ok(())
        })?;
        Ok(found)
    }
}

impl StoreAuditRead for HeaderChainStore {
    fn snapshot(&self) -> Result<EngineSnapshot, StoreError> {
        HeaderChainStore::snapshot(self)
    }

    fn metadata(&self) -> Result<EngineMetadata, StoreError> {
        HeaderChainStore::metadata(self)
    }

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

    fn selected_projection(&self) -> Result<Vec<Frontier>, StoreError> {
        self.projection_entries(HEADER_SELECTED)
    }

    fn verified_projection(&self) -> Result<Vec<Frontier>, StoreError> {
        self.projection_entries(HEADER_VERIFIED)
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
            let delivery = AuxDelivery::decode(&value)
                .map_err(|_| StoreError::Incoherent("invalid auxiliary value"))?;
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

    fn visit_finality_history(
        &self,
        visitor: &mut dyn FnMut(FinalityRecord) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        self.visit_finality_records(visitor)
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
    let predecessor_span = u32::try_from(zakura_header_chain::POW_PREDECESSOR_CONTEXT_SPAN)
        .map_err(|_| StoreError::Incoherent("validation context bound does not fit in u32"))?;
    let required = usize::try_from(parent_node.height.0.min(predecessor_span))
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
    fn earliest_deferred(&self) -> Result<Option<DateTime<Utc>>, StoreError> {
        let cf = self.cf(HEADER_DEFERRED).map_err(store_error)?;
        let Some((key, value)) = self
            .db
            .raw_first_cf(&cf)
            .map_err(HeaderChainStoreError::from)
            .map_err(store_error)?
        else {
            return Ok(None);
        };
        if key.len() != 44 || !value.is_empty() {
            return Err(StoreError::Incoherent("invalid deferred-index row"));
        }
        let key = HeaderDeferredKey::try_from_bytes(&key)
            .map_err(|_| StoreError::Incoherent("invalid deferred-index key"))?;
        Utc.timestamp_opt(key.seconds, key.nanoseconds)
            .single()
            .ok_or(StoreError::Incoherent("invalid deferred-index timestamp"))
            .map(Some)
    }

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
mod tests;
