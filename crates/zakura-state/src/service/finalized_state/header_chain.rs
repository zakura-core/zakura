//! Durable adapter for the fork-aware header-chain transition engine.

#![allow(dead_code)] // Constructed by the full-state migration and service wiring in PR-9.

use std::{
    borrow::Cow,
    collections::{BTreeSet, HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::{DateTime, TimeZone, Utc};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{sync::watch, time::Instant};
use zakura_chain::{block, parallel::commitment_aux::BlockCommitmentRoots, parameters::Network};
use zakura_header_chain::{
    audit_store, audit_store_for_trust_anchor_update, ApplyResult, AuxDelivery, AuxDelta,
    BodyWorkAuthority, BodyWorkOwner, ChangeSet, CommittedHeaderChainView, CommittedStallReceipt,
    CounterExhausted, EligibilityReason, EngineConfig, EngineMetadata, EngineMode, EngineSnapshot,
    EvidenceId, FinalityHistoryCheckpoint, FinalityRecord, FinalitySource, Frontier,
    FullStateEvidenceAuthority, FullStateFinalityProvenance, FullStateFinalized, HeaderChainEngine,
    HeaderInsertionFacts, HeaderLocator, HeaderNode, HeaderSyncWorkOwner, HeaderValidationFacts,
    HeaderWorkAuthority, MemHeaderStore, NoChangeReceipt, RecoveryFailure, RecoveryPlan,
    RecoveryRepair, RowLimit, SourceId, StaleReceipt, StateVersion, StoreAuditRead,
    StoreAuditSnapshot, StoreCollection, StoreError, SystemClock, TransitionContext,
    TransitionEffect, TransitionEvent, TransitionFailure, TransitionInput, TransitionRequest,
    UntrustedAuxDeliveryRow, ValidationContextRecord, ValidationLease, VerifiedChainChanged,
    VerifiedChangeCause, VerifiedHeaderRef,
};

use crate::{
    RetainedPathLease, RetainedPathLeaseOutcome, RetainedPathPage, RetainedPathReadOutcome,
    MAX_RETAINED_PATH_LEASES,
};

use super::{
    disk_db::RawVisitError,
    disk_format::{
        header_chain::{
            EligibilityReasonKind, HeaderAuxDeliveryKey, HeaderChildKey, HeaderDeferredKey,
            HeaderEligibilityRootKey, HeaderFinalityKey, HeaderFinalityWitnessKey, HeaderHeightKey,
        },
        header_chain_values::{
            decode_untrusted_aux_delivery, FullStateBodyValidationEvidenceAuthorityDisk,
            HeaderChainValueError, HeaderEligibilityReasonDisk, HeaderFinalityWitnessDisk,
            HeaderNodeDisk, HeaderReconstructionPhaseDisk, HeaderReconstructionProgressDisk,
            HeaderRowCountDisk, HeaderValidationContextDisk,
        },
        FallibleDiskValue, FromDisk, IntoDisk, RawBytes,
    },
    zakura_db::block::ZAKURA_HEADER_HASH_BY_HEIGHT,
    DiskDb, DiskWriteBatch, ReadDisk, WriteDisk, HEADER_AUX_DELIVERY,
    HEADER_BODY_EVIDENCE_AUTHORITY, HEADER_CHILD, HEADER_CONSENSUS_INVALID_BODY_TOMBSTONE,
    HEADER_DEFERRED, HEADER_ELIGIBILITY_ROOT, HEADER_ENGINE_META, HEADER_FINALITY_HISTORY,
    HEADER_FINALITY_WITNESS, HEADER_NODE_BY_HASH, HEADER_SELECTED, HEADER_VALIDATION_CONTEXT,
    HEADER_VERIFIED,
};

const METADATA_KEY: &[u8] = b"";
const FINALITY_HISTORY_CHECKPOINT_KEY: &[u8] = b"finality-history-checkpoint-v1";
const FINALITY_HISTORY_COUNT_KEY: &[u8] = b"finality-history-count-v1";
const FINALITY_WITNESS_COUNT_KEY: &[u8] = b"finality-witness-count-v1";
const TOMBSTONE_COUNT_KEY: &[u8] = b"consensus-invalid-tombstone-count-v1";
const FINALITY_HISTORY_LIMIT: usize = 65_536;
const FINALITY_WITNESS_LIMIT: usize = 2 * FINALITY_HISTORY_LIMIT + 1_000;
const TOMBSTONE_LIMIT: usize = 65_536;
const RECONSTRUCTION_PROGRESS_KEY: &[u8] = b"reconstruction-progress-v1";
const RETAINED_PATH_LEASE_IDLE: Duration = Duration::from_secs(30);

#[cfg(feature = "internal-bench")]
static BENCH_WITNESS_POINT_READS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
#[cfg(feature = "internal-bench")]
static BENCH_WITNESS_ROW_WRITES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
#[cfg(feature = "internal-bench")]
static BENCH_BATCH_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

struct WitnessMutations<'a> {
    store: &'a HeaderChainStore,
    rows: HashMap<Frontier, Option<HeaderFinalityWitnessDisk>>,
    count: u64,
}

impl<'a> WitnessMutations<'a> {
    fn new(store: &'a HeaderChainStore) -> Result<Self, HeaderChainStoreError> {
        let count = store
            .get_value::<HeaderRowCountDisk>(HEADER_ENGINE_META, FINALITY_WITNESS_COUNT_KEY)?
            .map_or(0, |count| count.0);
        if count
            > u64::try_from(FINALITY_WITNESS_LIMIT).map_err(|_| {
                HeaderChainStoreError::Incoherent("finality witness limit does not fit u64")
            })?
        {
            return Err(StoreError::LimitExceeded {
                collection: StoreCollection::FinalityHistory,
                limit: RowLimit::new(FINALITY_WITNESS_LIMIT),
            }
            .into());
        }
        Ok(Self {
            store,
            rows: HashMap::new(),
            count,
        })
    }

    fn row(
        &mut self,
        frontier: Frontier,
    ) -> Result<&mut Option<HeaderFinalityWitnessDisk>, HeaderChainStoreError> {
        if !self.rows.contains_key(&frontier) {
            #[cfg(feature = "internal-bench")]
            BENCH_WITNESS_POINT_READS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let row = self.store.get_value::<HeaderFinalityWitnessDisk>(
                HEADER_FINALITY_WITNESS,
                HeaderFinalityWitnessKey {
                    height: frontier.height,
                    hash: frontier.hash,
                }
                .as_bytes(),
            )?;
            if row.as_ref().is_some_and(|row| {
                row.context.height != frontier.height || row.context.header.hash() != frontier.hash
            }) {
                return Err(HeaderChainStoreError::Incoherent(
                    "finality witness key contradicts its header",
                ));
            }
            self.rows.insert(frontier, row);
        }
        Ok(self
            .rows
            .get_mut(&frontier)
            .expect("the witness mutation exists because this method inserted it"))
    }

    fn insert(
        &mut self,
        frontier: Frontier,
        header: Arc<block::Header>,
    ) -> Result<bool, HeaderChainStoreError> {
        let row = self.row(frontier)?;
        if let Some(row) = row {
            if row.context.header != header {
                return Err(HeaderChainStoreError::Incoherent(
                    "finality witness hash changed its canonical header",
                ));
            }
            return Ok(false);
        }
        *row = Some(HeaderFinalityWitnessDisk {
            context: HeaderValidationContextDisk {
                header,
                height: frontier.height,
            },
            root_references: 0,
            child_references: 0,
        });
        self.count = self
            .count
            .checked_add(1)
            .ok_or(HeaderChainStoreError::Incoherent(
                "finality witness count overflow",
            ))?;
        Ok(true)
    }

    fn parent(&mut self, frontier: Frontier) -> Result<Frontier, HeaderChainStoreError> {
        let row = self
            .row(frontier)?
            .as_ref()
            .ok_or(HeaderChainStoreError::Incoherent(
                "finality witness parent walk reached a missing node",
            ))?;
        Ok(Frontier::new(
            block::Height(frontier.height.0.checked_sub(1).ok_or(
                HeaderChainStoreError::Incoherent("finality witness height underflow"),
            )?),
            row.context.header.previous_block_hash,
        ))
    }

    fn collect_unreferenced_branch(
        &mut self,
        mut cursor: Frontier,
        floor: Frontier,
    ) -> Result<(), HeaderChainStoreError> {
        while cursor.height > floor.height {
            let Some(row) = self.row(cursor)?.as_ref() else {
                return Err(HeaderChainStoreError::Incoherent(
                    "finality witness branch is incomplete",
                ));
            };
            if row.root_references != 0 || row.child_references != 0 {
                break;
            }
            let parent = Frontier::new(
                block::Height(cursor.height.0.checked_sub(1).ok_or(
                    HeaderChainStoreError::Incoherent("finality witness height underflow"),
                )?),
                row.context.header.previous_block_hash,
            );
            *self.row(cursor)? = None;
            self.count = self
                .count
                .checked_sub(1)
                .ok_or(HeaderChainStoreError::Incoherent(
                    "finality witness count underflow",
                ))?;
            if parent.height > floor.height {
                let parent_row =
                    self.row(parent)?
                        .as_mut()
                        .ok_or(HeaderChainStoreError::Incoherent(
                            "finality witness branch lacks its parent",
                        ))?;
                parent_row.child_references = parent_row.child_references.checked_sub(1).ok_or(
                    HeaderChainStoreError::Incoherent("finality witness child reference underflow"),
                )?;
            }
            cursor = parent;
        }
        Ok(())
    }

    fn finish(self, batch: &mut DiskWriteBatch) -> Result<(), HeaderChainStoreError> {
        if self.count
            > u64::try_from(FINALITY_WITNESS_LIMIT).map_err(|_| {
                HeaderChainStoreError::Incoherent("finality witness limit does not fit u64")
            })?
        {
            return Err(StoreError::LimitExceeded {
                collection: StoreCollection::FinalityHistory,
                limit: RowLimit::new(FINALITY_WITNESS_LIMIT),
            }
            .into());
        }
        for (frontier, row) in self.rows {
            #[cfg(feature = "internal-bench")]
            BENCH_WITNESS_ROW_WRITES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let key = HeaderFinalityWitnessKey {
                height: frontier.height,
                hash: frontier.hash,
            }
            .as_bytes();
            if let Some(row) = row {
                self.store
                    .put_value(batch, HEADER_FINALITY_WITNESS, key, &row)?;
            } else {
                self.store.delete_raw(batch, HEADER_FINALITY_WITNESS, key)?;
            }
        }
        self.store.put_value(
            batch,
            HEADER_ENGINE_META,
            FINALITY_WITNESS_COUNT_KEY,
            &HeaderRowCountDisk(self.count),
        )?;
        Ok(())
    }
}

#[cfg(test)]
struct TestHeaderCompletionAuthority<'a>(Option<&'a dyn FullStateEvidenceAuthority>);

#[cfg(test)]
impl FullStateEvidenceAuthority for TestHeaderCompletionAuthority<'_> {
    fn authorizes_full_state(&self, event: &TransitionEvent) -> bool {
        self.0
            .is_some_and(|inner| inner.authorizes_full_state(event))
    }

    fn authorizes_scheduler_retry(&self, retry: &zakura_header_chain::OperatorBodyRetry) -> bool {
        self.0
            .is_some_and(|inner| inner.authorizes_scheduler_retry(retry))
    }

    fn authorizes_header_completion(&self, _insert: &zakura_header_chain::InsertHeaders) -> bool {
        true
    }

    fn authorizes_validation_lease(&self, lease: &ValidationLease) -> bool {
        self.0
            .is_some_and(|inner| inner.authorizes_validation_lease(lease))
    }

    fn authorizes_retention_reference(&self, reference: block::Hash) -> bool {
        self.0
            .is_some_and(|inner| inner.authorizes_retention_reference(reference))
    }
}

struct StateIssuedAuthority<'a> {
    inner: Option<&'a dyn FullStateEvidenceAuthority>,
    validation_leases: &'a [ValidationLease],
    active_retention_references: &'a [block::Hash],
}

impl FullStateEvidenceAuthority for StateIssuedAuthority<'_> {
    fn authorizes_full_state(&self, event: &TransitionEvent) -> bool {
        self.inner
            .is_some_and(|inner| inner.authorizes_full_state(event))
    }

    fn authorizes_scheduler_retry(&self, retry: &zakura_header_chain::OperatorBodyRetry) -> bool {
        self.inner
            .is_some_and(|inner| inner.authorizes_scheduler_retry(retry))
    }

    fn authorizes_header_completion(&self, insert: &zakura_header_chain::InsertHeaders) -> bool {
        self.inner
            .is_some_and(|inner| inner.authorizes_header_completion(insert))
    }

    fn authorizes_validation_lease(&self, lease: &ValidationLease) -> bool {
        self.validation_leases.contains(lease)
    }

    fn authorizes_retention_reference(&self, reference: block::Hash) -> bool {
        self.active_retention_references.contains(&reference)
            || self
                .inner
                .is_some_and(|inner| inner.authorizes_retention_reference(reference))
    }
}

fn combined_retention_references<'a>(
    context_references: &'a [block::Hash],
    active_lease_references: Option<&'a [block::Hash]>,
) -> Cow<'a, [block::Hash]> {
    let Some(active_lease_references) = active_lease_references else {
        return Cow::Borrowed(context_references);
    };
    if context_references.is_empty() {
        return Cow::Borrowed(active_lease_references);
    }

    let mut references = context_references.to_vec();
    references.extend(active_lease_references.iter().copied());
    references.sort_unstable_by_key(|hash| hash.0);
    references.dedup();
    Cow::Owned(references)
}

mod audit_snapshot;
#[cfg(test)]
#[path = "header_chain/coherence.rs"]
mod coherence;
#[cfg(any(test, feature = "header-fuzz"))]
mod fuzz;
pub(in crate::service) mod migration;
#[cfg(any(test, feature = "header-fuzz"))]
pub use fuzz::{replay_recovery_rows_bytes, RecoveryRowsReplaySummary};

/// Selects one usable VCT auxiliary delivery for a retained header.
///
/// The selector excludes deliveries without tree data and rejected deliveries. It prefers
/// authenticated, unauthenticated, and disputed deliveries in that order. The delivery ID breaks
/// ties deterministically.
pub(crate) fn select_vct_auxiliary_delivery(deliveries: Vec<AuxDelivery>) -> Option<AuxDelivery> {
    deliveries
        .into_iter()
        .filter(|delivery| delivery.tree_aux.is_some() && !delivery.is_rejected())
        .min_by_key(|delivery| {
            (
                if delivery.is_authenticated() {
                    0
                } else if delivery.is_unauthenticated() {
                    1
                } else if delivery.is_disputed() {
                    2
                } else {
                    3
                },
                delivery.delivery_id,
            )
        })
}

fn untrusted_aux_row_matches(authoritative: AuxDelivery, row: UntrustedAuxDeliveryRow) -> bool {
    let expected_base = AuxDelivery::new(
        authoritative.delivery_id,
        authoritative.header_hash,
        authoritative.source,
        authoritative.owner,
        authoritative.body_size,
        authoritative.tree_aux,
    );
    row.delivery() == expected_base
        && row.outcome_status_code() == 0
        && row.observation_digests() == [None, None]
        && row.outcome_boundary_hash().is_none()
}

/// Failure at the durable header-chain boundary.
#[derive(Debug, Error)]
pub enum HeaderChainStoreError {
    /// Migration or bootstrap has not initialized the database yet.
    #[error("header-chain metadata is not initialized")]
    Uninitialized,
    /// The decoder found a malformed or internally contradictory durable key or value.
    #[error("incoherent durable header-chain rows: {0}")]
    Incoherent(&'static str),
    /// Stable value encoding failed before RocksDB committed the batch.
    #[error(transparent)]
    Codec(#[from] HeaderChainValueError),
    /// Pure transition planning rejected the request before commit.
    #[error(transparent)]
    Transition(#[from] TransitionFailure),
    /// The writer could not install a committed transition in memory.
    /// The writer must fail closed.
    #[error(transparent)]
    CommittedTransition(#[from] zakura_header_chain::CommittedTransitionError),
    /// A runtime durable read failed before transition planning.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// RocksDB rejected the one atomic write batch.
    #[error("header-chain atomic write failed: {0}")]
    RocksDb(#[from] rocksdb::Error),
    /// A prior panic poisoned the serialized writer lock.
    #[error("header-chain serialized writer lock is poisoned")]
    WriterPoisoned,
    /// Authenticated full state lacked a canonical header during reconstruction.
    #[error("authenticated full state is missing canonical header {0:?}")]
    MissingCanonicalHeader(block::Height),
    /// A staged full-state value disagreed with the header plan derived from the same evidence.
    #[error("staged full-state verified frontier {expected:?} differs from projected header frontier {actual:?}")]
    VerifiedFrontierMismatch {
        /// Exact staged full-state winner.
        expected: Frontier,
        /// Header transition result derived before any write.
        actual: Frontier,
    },
    /// A staged full-state branch lost a required header or parent relation in the projected DAG.
    #[error(
        "staged full-state header {hash:?} is absent or incoherent in the projected header DAG"
    )]
    StagedPathMismatch {
        /// Exact full-state header that the transition did not preserve.
        hash: block::Hash,
    },
    /// A prepared full-state mutation lost its exact serialized header-chain authority.
    #[error(
        "prepared full-state/header transition became stale at durable version {current_version:?}"
    )]
    StaleFullStateTransition {
        /// Current durable version observed instead of committing.
        current_version: StateVersion,
    },
    /// Retention pressure rejected a prepared full-state mutation before it could commit.
    #[error("prepared full-state/header transition was rejected by retention pressure")]
    FullStateResourceStalled {
        /// Durable alarm-only result committed instead of the caller mutation.
        receipt: CommittedStallReceipt,
    },
    /// Exhaustive startup audit or deterministic reconstruction failed.
    #[error(transparent)]
    Recovery(#[from] RecoveryFailure),
    /// An explicit store migration exhausted a monotonic durable counter.
    #[error(transparent)]
    Counter(#[from] CounterExhausted),
    /// Deterministic body validation refuted an imported headers-only trust pin.
    /// The operator must destroy and resync this store.
    #[error(
        "header_chain_migrated_pin_refuted at {pin:?}; delete the migrated header store and resync"
    )]
    MigratedPinRefuted {
        /// Exact preserved pin contradicted by deterministic body validation.
        pin: Frontier,
    },
    /// A test injected a crash at a named durable or publication boundary.
    #[cfg(test)]
    #[error("injected header-chain crash at {0:?}")]
    InjectedCrash(FaultPoint),
}

/// One successful startup audit and optional atomic repair.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartupReport {
    /// Snapshot read before any reconstructible repair.
    pub previous: EngineSnapshot,
    /// Snapshot that the startup audit approved for publication.
    pub current: EngineSnapshot,
    /// Exact reconstructible categories repaired in one batch.
    pub repairs: BTreeSet<RecoveryRepair>,
    /// Only a successful, fully audited startup permits publication.
    pub publication_allowed: bool,
}

/// The sole latest-value publisher for durable header-chain snapshots.
#[derive(Clone, Debug)]
pub struct Publisher {
    sender: watch::Sender<EngineSnapshot>,
    views: watch::Sender<CommittedHeaderChainView>,
    mirrors: Arc<Mutex<Vec<watch::Sender<Option<EngineSnapshot>>>>>,
    view_mirrors: Arc<Mutex<Vec<watch::Sender<Option<CommittedHeaderChainView>>>>>,
}

impl Publisher {
    fn new(snapshot: EngineSnapshot) -> Self {
        record_published_snapshot(&snapshot);
        let (sender, _) = watch::channel(snapshot.clone());
        let (views, _) = watch::channel(CommittedHeaderChainView::new(
            snapshot,
            zakura_header_chain::BodyWorkEpoch::default(),
        ));
        Self {
            sender,
            views,
            mirrors: Arc::new(Mutex::new(Vec::new())),
            view_mirrors: Arc::new(Mutex::new(Vec::new())),
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

    /// Return the latest atomic snapshot and body-work epoch.
    pub fn view(&self) -> CommittedHeaderChainView {
        self.views.borrow().clone()
    }

    /// Subscribe to atomic snapshots and body-work epochs.
    pub fn subscribe_views(&self) -> watch::Receiver<CommittedHeaderChainView> {
        self.views.subscribe()
    }

    /// Mirror committed snapshots into a channel that can predate runtime attachment.
    pub(crate) fn mirror_to(&self, sender: watch::Sender<Option<EngineSnapshot>>) {
        sender.send_replace(Some(self.snapshot()));
        self.mirrors
            .lock()
            .expect("header-chain publisher mirror mutex is never poisoned")
            .push(sender);
    }

    /// Mirror atomic committed views into a channel that can predate runtime attachment.
    pub(crate) fn mirror_views_to(&self, sender: watch::Sender<Option<CommittedHeaderChainView>>) {
        sender.send_replace(Some(self.view()));
        self.view_mirrors
            .lock()
            .expect("header-chain publisher view mirror mutex is never poisoned")
            .push(sender);
    }

    fn publish(&self, snapshot: EngineSnapshot, effect: TransitionEffect) {
        let previous_epoch = self.views.borrow().body_work_epoch;
        let body_work_epoch = if effect.invalidates_body_work() {
            previous_epoch
                .checked_next()
                .expect("a process cannot commit u64::MAX body-work invalidations")
        } else {
            previous_epoch
        };
        let view = CommittedHeaderChainView::new(snapshot.clone(), body_work_epoch);
        record_published_snapshot(&snapshot);
        self.sender.send_replace(snapshot.clone());
        self.views.send_replace(view.clone());
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
        self.view_mirrors
            .lock()
            .expect("header-chain publisher view mirror mutex is never poisoned")
            .retain(|mirror| {
                if mirror.receiver_count() == 0 {
                    false
                } else {
                    mirror.send_replace(Some(view.clone()));
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
    // Metrics expose approximate floating-point generations.
    // The durable counters retain the exact generation values.
    metrics::gauge!("sync.header_chain.generation.header")
        .set(snapshot.header_generation.get() as f64);
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

    tracing::debug!(
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
    config: EngineConfig,
    publisher: Publisher,
    leases: Arc<Mutex<RetainedPathLeaseRegistry>>,
    transition_engine: Arc<Mutex<HeaderChainEngine>>,
}

#[derive(Copy, Clone)]
struct CombinedStateExpectation<'a> {
    verified: Option<Frontier>,
    staged: &'a [VerifiedHeaderRef],
}

impl CombinedStateExpectation<'_> {
    const NONE: Self = Self {
        verified: None,
        staged: &[],
    };
}

fn load_transition_engine(
    store: &HeaderChainStore,
) -> Result<HeaderChainEngine, HeaderChainStoreError> {
    let metadata = store.metadata()?;
    let graph = MemHeaderStore::reconstruct(zakura_header_chain::HeaderGraphReconstruction::new(
        metadata.frontiers.finalized,
        store.load_header_nodes()?,
        store.load_consensus_invalid_body_tombstones()?,
    ))
    .map_err(|_| HeaderChainStoreError::Incoherent("audited node graph is invalid"))?;
    HeaderChainEngine::from_untrusted_durable_state(
        graph,
        metadata,
        store.selected_projection()?,
        store.verified_projection()?,
        store.load_aux_deliveries()?,
    )
    .map_err(|_| HeaderChainStoreError::Incoherent("audited engine state is invalid"))
}

/// Return true when a recovered deferral is due, comparing every entry against one instant.
fn any_deferral_is_due(plan: &zakura_header_chain::RecoveryPlan) -> bool {
    let now = Utc::now();
    plan.deferred_entries.iter().any(|(until, _)| *until <= now)
}

/// Reevaluate due recovered deferrals before constructing a publisher.
///
/// The function uses the normal planner and durable commit path. It leaves the recovered engine
/// unchanged when no deferral is due or when the planner derives no change. It propagates planner
/// failures because the runtime would immediately repeat the due transition. Any retryable
/// planning failure needs an explicit classification and a bounded retry policy. On success, the
/// returned engine matches the durable state that the caller may publish.
fn settle_deferred_before_publication(
    store: &HeaderChainStore,
    config: &EngineConfig,
    has_due_deferred: bool,
) -> Result<HeaderChainEngine, HeaderChainStoreError> {
    let mut engine = load_transition_engine(store)?;
    if !has_due_deferred {
        return Ok(engine);
    }
    let before = engine.snapshot();
    let context = TransitionContext {
        config,
        clock: &SystemClock,
        full_state_authority: None,
        retention_references: &[],
    };
    let transition = engine.plan_transition(
        TransitionInput::ReevaluateDeferred {
            expected_version: before.state_version,
        },
        &context,
    )?;
    if transition.is_no_change() {
        return Ok(engine);
    }

    let migrated_pin_refuted = transition.change_set().metadata.alarms.migrated_pin_refuted;
    let batch = store.batch_for(transition.change_set())?;
    store.db.write(batch)?;
    engine.install_committed_transition(transition)?;
    if let Some(pin) = migrated_pin_refuted {
        return Err(HeaderChainStoreError::MigratedPinRefuted { pin });
    }
    Ok(engine)
}

fn restore_transition_engine_after_staging_error(
    store: &HeaderChainStore,
    engine: &mut HeaderChainEngine,
    original: HeaderChainStoreError,
) -> HeaderChainStoreError {
    match load_transition_engine(store) {
        Ok(restored) => {
            *engine = restored;
            original
        }
        Err(reload) => {
            tracing::error!(
                ?original,
                ?reload,
                "failed to restore the durable header engine after a staged transition error"
            );
            reload
        }
    }
}

/// Read-only coherent queries serialized against durable header transitions.
#[derive(Clone, Debug)]
pub(crate) struct HeaderChainReader {
    store: HeaderChainStore,
    config: Arc<EngineConfig>,
    leases: Arc<Mutex<RetainedPathLeaseRegistry>>,
    transition_engine: Arc<Mutex<HeaderChainEngine>>,
}

/// One selected header and every auxiliary delivery attached to it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SelectedHeaderWithAuxiliaryDeliveries {
    /// Header node from the selected projection.
    pub(crate) header_node: HeaderNode,
    /// Auxiliary deliveries attached to `header_node`.
    pub(crate) auxiliary_deliveries: Vec<AuxDelivery>,
}

/// One atomically read selected header and its direct selected successor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SelectedAuxiliaryWindow {
    /// Engine snapshot that identifies the selected projection generation.
    pub(crate) engine_snapshot: EngineSnapshot,
    /// Selected header whose auxiliary delivery the caller will verify.
    pub(crate) delivery_header: SelectedHeaderWithAuxiliaryDeliveries,
    /// Direct selected successor that supplies the authentication boundary.
    pub(crate) successor_header: Option<SelectedHeaderWithAuxiliaryDeliveries>,
}

/// One selected projection captured with the engine snapshot that produced it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CapturedSelectedProjection {
    /// Engine snapshot that identifies the projection generation and bounds.
    pub(crate) engine_snapshot: EngineSnapshot,
    /// Complete selected projection in ascending height order.
    pub(crate) frontiers: Vec<Frontier>,
}

#[derive(Debug, Default)]
struct RetainedPathLeaseRegistry {
    next_lease_id: u64,
    next_reservation_id: u64,
    by_peer: HashMap<SourceId, CanonicalHeaderPathCursor>,
    reservations: HashMap<SourceId, u64>,
    reference_counts: HashMap<block::Hash, usize>,
    cached_references: Arc<[block::Hash]>,
    references_dirty: bool,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum CanonicalHeaderPathPosition {
    Finalized {
        next: block::Height,
        end: block::Height,
    },
    Retained {
        next: usize,
    },
    Complete,
}

#[derive(Clone, Debug)]
struct CanonicalHeaderPathCursor {
    lease_id: u64,
    peer: SourceId,
    session_id: u64,
    target: Frontier,
    common_ancestor: Frontier,
    scope: HeaderWorkAuthority,
    position: CanonicalHeaderPathPosition,
    last_frontier: Frontier,
    retained_ancestor: Option<block::Hash>,
    retained_path: Arc<[block::Hash]>,
    idle_deadline: Instant,
}

impl CanonicalHeaderPathCursor {
    fn lease(&self) -> RetainedPathLease {
        RetainedPathLease {
            lease_id: self.lease_id,
            peer: self.peer,
            session_id: self.session_id,
            target: self.target,
            common_ancestor: self.common_ancestor,
            scope: self.scope,
            idle_deadline: self.idle_deadline,
        }
    }
}

#[derive(Debug)]
struct RetainedPathLeaseSpec {
    peer: SourceId,
    session_id: u64,
    target: Frontier,
    common_ancestor: Frontier,
    scope: HeaderWorkAuthority,
    position: CanonicalHeaderPathPosition,
    retained_ancestor: Option<block::Hash>,
    retained_path: Arc<[block::Hash]>,
}

#[derive(Copy, Clone, Debug)]
struct CanonicalHeaderPathAdvance {
    expected_after: Frontier,
    position: CanonicalHeaderPathPosition,
    last_frontier: Frontier,
    now: Instant,
}

#[derive(Debug)]
struct RetainedPathReservation {
    leases: Arc<Mutex<RetainedPathLeaseRegistry>>,
    peer: SourceId,
    reservation_id: u64,
    active: bool,
}

impl RetainedPathReservation {
    fn commit(
        mut self,
        spec: RetainedPathLeaseSpec,
        now: Instant,
    ) -> Result<RetainedPathLeaseOutcome, HeaderChainStoreError> {
        let outcome = self
            .leases
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?
            .commit_reservation(self.peer, self.reservation_id, spec, now);
        self.active = false;
        Ok(outcome)
    }
}

impl Drop for RetainedPathReservation {
    fn drop(&mut self) {
        if self.active {
            if let Ok(mut leases) = self.leases.lock() {
                leases.release_reservation(self.peer, self.reservation_id);
            }
        }
    }
}

impl RetainedPathLeaseRegistry {
    fn expire(&mut self, now: Instant) {
        let expired: Vec<_> = self
            .by_peer
            .iter()
            .filter_map(|(peer, cursor)| (cursor.idle_deadline <= now).then_some(*peer))
            .collect();
        for peer in expired {
            self.remove_peer(peer);
        }
    }

    fn add_references(&mut self, cursor: &CanonicalHeaderPathCursor) {
        // The retention algorithm walks from each target to finality.
        // This registry counts the target hash because it protects the full immutable suffix.
        // A path-hash entry would grow the bounded owner list with chain length.
        // That growth would reject ordinary transitions.
        *self.reference_counts.entry(cursor.target.hash).or_default() += 1;
        self.references_dirty = true;
    }

    fn remove_peer(&mut self, peer: SourceId) -> Option<CanonicalHeaderPathCursor> {
        let cursor = self.by_peer.remove(&peer)?;
        let hash = cursor.target.hash;
        let remove = {
            let Some(count) = self.reference_counts.get_mut(&hash) else {
                panic!("every installed lease target has a registry count");
            };
            let Some(next_count) = count.checked_sub(1) else {
                panic!("a lease target reference count cannot underflow");
            };
            *count = next_count;
            *count == 0
        };
        if remove {
            self.reference_counts.remove(&hash);
        }
        self.references_dirty = true;
        Some(cursor)
    }

    fn reserve(&mut self, peer: SourceId, now: Instant) -> Option<u64> {
        self.expire(now);
        if self.by_peer.contains_key(&peer)
            || self.reservations.contains_key(&peer)
            || self.by_peer.len().saturating_add(self.reservations.len())
                >= MAX_RETAINED_PATH_LEASES
        {
            return None;
        }
        let reservation_id = self.next_reservation_id.checked_add(1)?;
        self.next_reservation_id = reservation_id;
        self.reservations.insert(peer, reservation_id);
        Some(reservation_id)
    }

    fn release_reservation(&mut self, peer: SourceId, reservation_id: u64) {
        if self.reservations.get(&peer) == Some(&reservation_id) {
            self.reservations.remove(&peer);
        }
    }

    fn commit_reservation(
        &mut self,
        peer: SourceId,
        reservation_id: u64,
        spec: RetainedPathLeaseSpec,
        now: Instant,
    ) -> RetainedPathLeaseOutcome {
        if peer != spec.peer || self.reservations.get(&peer) != Some(&reservation_id) {
            return RetainedPathLeaseOutcome::Busy;
        }
        self.reservations.remove(&peer);
        if self.by_peer.contains_key(&peer) {
            return RetainedPathLeaseOutcome::Busy;
        }
        let Some(lease_id) = self.next_lease_id.checked_add(1) else {
            return RetainedPathLeaseOutcome::Busy;
        };
        self.next_lease_id = lease_id;
        let cursor = CanonicalHeaderPathCursor {
            lease_id,
            peer: spec.peer,
            session_id: spec.session_id,
            target: spec.target,
            common_ancestor: spec.common_ancestor,
            scope: spec.scope,
            position: spec.position,
            last_frontier: spec.common_ancestor,
            retained_ancestor: spec.retained_ancestor,
            retained_path: spec.retained_path,
            idle_deadline: now + RETAINED_PATH_LEASE_IDLE,
        };
        let lease = cursor.lease();
        self.add_references(&cursor);
        self.by_peer.insert(spec.peer, cursor);
        RetainedPathLeaseOutcome::Acquired(Box::new(lease))
    }

    fn get(
        &mut self,
        peer: SourceId,
        session_id: u64,
        lease_id: u64,
        now: Instant,
    ) -> Option<CanonicalHeaderPathCursor> {
        self.expire(now);
        let cursor = self.by_peer.get(&peer)?;
        if cursor.session_id != session_id || cursor.lease_id != lease_id {
            return None;
        }
        Some(cursor.clone())
    }

    fn advance(
        &mut self,
        peer: SourceId,
        session_id: u64,
        lease_id: u64,
        advance: CanonicalHeaderPathAdvance,
    ) -> bool {
        if self
            .by_peer
            .get(&peer)
            .is_some_and(|cursor| cursor.idle_deadline <= advance.now)
        {
            self.remove_peer(peer);
            return false;
        }
        let Some(cursor) = self.by_peer.get_mut(&peer) else {
            return false;
        };
        if cursor.session_id != session_id
            || cursor.lease_id != lease_id
            || cursor.last_frontier != advance.expected_after
        {
            return false;
        }
        cursor.position = advance.position;
        cursor.last_frontier = advance.last_frontier;
        cursor.idle_deadline = advance.now + RETAINED_PATH_LEASE_IDLE;
        true
    }

    fn release(
        &mut self,
        peer: SourceId,
        session_id: u64,
        lease_id: u64,
        scope: HeaderWorkAuthority,
    ) -> bool {
        let matches = self.by_peer.get(&peer).is_some_and(|cursor| {
            cursor.session_id == session_id && cursor.lease_id == lease_id && cursor.scope == scope
        });
        if matches {
            self.remove_peer(peer);
        }
        matches
    }

    fn active_references(&mut self, now: Instant) -> Arc<[block::Hash]> {
        self.expire(now);
        if self.references_dirty {
            let mut references: Vec<_> = self.reference_counts.keys().copied().collect();
            references.sort_unstable_by_key(|hash| hash.0);
            self.cached_references = references.into();
            self.references_dirty = false;
        }
        self.cached_references.clone()
    }
}

impl HeaderChainReader {
    fn coherent_selected_node(
        &self,
        height: block::Height,
    ) -> Result<Option<HeaderNode>, StoreError> {
        let snapshot = self.store.snapshot()?;
        let selected_hash = self.store.selected_hash(height)?;
        if height < snapshot.frontiers.finalized.height
            || height > snapshot.frontiers.header_best.height
        {
            if selected_hash.is_some() {
                return Err(StoreError::Incoherent(
                    "selected projection contains a row outside its published bounds",
                ));
            }
            return Ok(None);
        }
        let Some(hash) = selected_hash else {
            if height >= snapshot.frontiers.finalized.height
                && height <= snapshot.frontiers.header_best.height
            {
                return Err(StoreError::Incoherent(
                    "selected projection has a gap within its published bounds",
                ));
            }
            return Ok(None);
        };
        let indexed_node = self.store.header_node(hash)?.ok_or(StoreError::Incoherent(
            "selected projection references a missing node",
        ))?;
        if indexed_node.height != height {
            return Err(StoreError::Incoherent(
                "selected projection node height disagrees with its index",
            ));
        }

        let finalized = snapshot.frontiers.finalized;
        if height == finalized.height {
            if hash != finalized.hash {
                return Err(StoreError::Incoherent(
                    "selected projection disagrees with the committed finalized frontier",
                ));
            }
            return Ok(Some(indexed_node));
        }

        let tip = snapshot.frontiers.header_best;
        let mut selected_ancestor =
            self.store
                .header_node(tip.hash)?
                .ok_or(StoreError::Incoherent(
                    "committed selected tip references a missing node",
                ))?;
        if selected_ancestor.height != tip.height {
            return Err(StoreError::Incoherent(
                "committed selected tip height disagrees with its node",
            ));
        }
        while selected_ancestor.height > height {
            let parent_height = block::Height(selected_ancestor.height.0.checked_sub(1).ok_or(
                StoreError::Incoherent("selected path reached a parent below height zero"),
            )?);
            let parent = self
                .store
                .header_node(selected_ancestor.parent_hash)?
                .ok_or(StoreError::Incoherent(
                    "selected path references a missing parent node",
                ))?;
            if parent.height != parent_height {
                return Err(StoreError::Incoherent(
                    "selected path parent height is not contiguous",
                ));
            }
            selected_ancestor = parent;
        }
        if selected_ancestor.height != height || selected_ancestor.hash != hash {
            return Err(StoreError::Incoherent(
                "selected projection node is not on the committed selected path",
            ));
        }
        Ok(Some(indexed_node))
    }

    fn coherent_aux_deliveries(
        &self,
        node: &HeaderNode,
    ) -> Result<Vec<AuxDelivery>, HeaderChainStoreError> {
        self.coherent_aux_deliveries_for(node.hash, &node.aux_delivery_ids)
    }

    fn coherent_aux_deliveries_for(
        &self,
        hash: block::Hash,
        aux_delivery_ids: &[EvidenceId],
    ) -> Result<Vec<AuxDelivery>, HeaderChainStoreError> {
        let deliveries = self
            .transition_engine
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?
            .aux_deliveries(hash)
            .to_vec();
        let durable = self.store.untrusted_aux_deliveries(hash)?;
        let indexed: BTreeSet<_> = aux_delivery_ids.iter().copied().collect();
        let stored: BTreeSet<_> = deliveries
            .iter()
            .map(|delivery| delivery.delivery_id)
            .collect();
        let durable_ids: BTreeSet<_> = durable
            .iter()
            .map(|row| row.delivery().delivery_id)
            .collect();
        if indexed.len() != aux_delivery_ids.len()
            || stored.len() != deliveries.len()
            || durable_ids.len() != durable.len()
            || indexed != stored
            || indexed != durable_ids
            || durable.iter().any(|row| {
                deliveries
                    .iter()
                    .find(|delivery| delivery.delivery_id == row.delivery().delivery_id)
                    .is_none_or(|delivery| !untrusted_aux_row_matches(*delivery, *row))
            })
        {
            return Err(HeaderChainStoreError::Store(StoreError::Incoherent(
                "retained node and auxiliary delivery index disagree",
            )));
        }
        Ok(deliveries)
    }

    fn retained_path_node(
        &self,
        hash: block::Hash,
    ) -> Result<Option<HeaderNodeDisk>, HeaderChainStoreError> {
        let Some(node) = self
            .store
            .get_value::<HeaderNodeDisk>(HEADER_NODE_BY_HASH, hash.0)?
        else {
            return Ok(None);
        };
        if node.hash != hash
            || node.header.hash() != hash
            || node.header.previous_block_hash != node.parent_hash
        {
            return Err(HeaderChainStoreError::Incoherent(
                "retained path node key and header fields disagree",
            ));
        }
        Ok(Some(node))
    }

    fn finalized_frontier(
        &self,
        hash: block::Hash,
    ) -> Result<Option<Frontier>, HeaderChainStoreError> {
        let height_by_hash = self.store.cf("height_by_hash")?;
        let height: Option<block::Height> = self.store.db.zs_get(&height_by_hash, &hash);
        let Some(height) = height else {
            return Ok(None);
        };
        let hash_by_height = self.store.cf("hash_by_height")?;
        let canonical_hash: Option<block::Hash> = self.store.db.zs_get(&hash_by_height, &height);
        if canonical_hash != Some(hash) {
            return Err(StoreError::Incoherent("finalized height/hash indexes disagree").into());
        }
        Ok(Some(Frontier::new(height, hash)))
    }

    fn finalized_header(
        &self,
        frontier: Frontier,
    ) -> Result<Arc<block::Header>, HeaderChainStoreError> {
        let block_header_by_height = self.store.cf("block_header_by_height")?;
        let header: Option<Arc<block::Header>> = self
            .store
            .db
            .zs_get(&block_header_by_height, &frontier.height);
        let header = header.ok_or(StoreError::Incoherent(
            "finalized header path has a missing header",
        ))?;
        if header.hash() != frontier.hash {
            return Err(StoreError::Incoherent(
                "finalized header disagrees with its canonical hash index",
            )
            .into());
        }
        Ok(header)
    }

    fn selected_aux_delivery(
        &self,
        node: &HeaderNode,
    ) -> Result<Option<AuxDelivery>, HeaderChainStoreError> {
        Ok(select_vct_auxiliary_delivery(
            self.coherent_aux_deliveries(node)?,
        ))
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
        if count == 0 {
            return Ok(Vec::new());
        }

        let snapshot = self.store.snapshot()?;
        if start < snapshot.frontiers.finalized.height
            || start > snapshot.frontiers.header_best.height
        {
            self.coherent_selected_node(start)?;
            return Ok(Vec::new());
        }
        let requested_end = block::Height(start.0.saturating_add(count.saturating_sub(1)));
        let end = requested_end.min(snapshot.frontiers.header_best.height);
        let mut selected = self
            .store
            .header_node(snapshot.frontiers.header_best.hash)?
            .ok_or(StoreError::Incoherent(
                "committed selected tip references a missing node",
            ))?;
        if selected.height != snapshot.frontiers.header_best.height {
            return Err(StoreError::Incoherent(
                "committed selected tip height disagrees with its node",
            )
            .into());
        }
        let mut selected_nodes = Vec::new();
        loop {
            if selected.height <= end {
                let projected_hash =
                    self.store
                        .selected_hash(selected.height)?
                        .ok_or(StoreError::Incoherent(
                            "selected projection has a gap within its published bounds",
                        ))?;
                if projected_hash != selected.hash {
                    return Err(StoreError::Incoherent(
                        "selected projection node is not on the committed selected path",
                    )
                    .into());
                }
                selected_nodes.push(selected.clone());
            }
            if selected.height == start {
                break;
            }
            let parent_height = block::Height(selected.height.0.checked_sub(1).ok_or(
                StoreError::Incoherent("selected path reached a parent below height zero"),
            )?);
            let parent =
                self.store
                    .header_node(selected.parent_hash)?
                    .ok_or(StoreError::Incoherent(
                        "selected path references a missing parent node",
                    ))?;
            if parent.height != parent_height {
                return Err(StoreError::Incoherent(
                    "selected path parent height is not contiguous",
                )
                .into());
            }
            selected = parent;
        }
        selected_nodes.reverse();

        let mut roots = Vec::new();
        for node in selected_nodes {
            let height = node.height;
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
        if self.store.header_node(parent_hash)?.is_none() {
            return Ok(None);
        }
        self.store
            .validation_context(parent_hash, self.config.network())
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

    /// Capture full-state data and the durable selected projection from one transition generation.
    pub(crate) fn with_selected_projection<T>(
        &self,
        read_full_state: impl FnOnce() -> T,
    ) -> Result<(T, Vec<Frontier>), HeaderChainStoreError> {
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        let engine = self
            .transition_engine
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        let full_state = read_full_state();
        let snapshot = engine.snapshot();
        let projection = engine.selected_projection().to_vec();
        if projection.first().copied() != Some(snapshot.frontiers.finalized)
            || projection.last().copied() != Some(snapshot.frontiers.header_best)
        {
            return Err(StoreError::Incoherent(
                "selected projection disagrees with its published bounds",
            )
            .into());
        }
        Ok((full_state, projection))
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

    pub(crate) fn selected_hashes(
        &self,
        start: block::Height,
        count: u32,
    ) -> Result<Vec<Frontier>, HeaderChainStoreError> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        let selected_tip = self.store.snapshot()?.frontiers.header_best;
        if start > selected_tip.height {
            return Ok(Vec::new());
        }
        let end = block::Height(
            start
                .0
                .saturating_add(count.saturating_sub(1))
                .min(selected_tip.height.0),
        );
        self.store.projection_range(HEADER_SELECTED, start, end)
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

    /// Reads one selected header and its direct successor under the writer lock.
    ///
    /// The writer lock prevents a concurrent transition from mixing projection entries with
    /// auxiliary deliveries from different engine versions.
    pub(crate) fn selected_auxiliary_window(
        &self,
        height: block::Height,
        hash: block::Hash,
    ) -> Result<Option<SelectedAuxiliaryWindow>, HeaderChainStoreError> {
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        let Some(delivery_header_node) = self.coherent_selected_node(height)? else {
            return Ok(None);
        };
        if delivery_header_node.hash != hash {
            return Ok(None);
        }
        let delivery_auxiliary_deliveries = self.coherent_aux_deliveries(&delivery_header_node)?;
        let successor_header = match height.next() {
            Ok(successor_height) => match self.coherent_selected_node(successor_height)? {
                Some(successor_header_node) => {
                    if successor_header_node.parent_hash != hash {
                        return Err(StoreError::Incoherent(
                            "selected auxiliary successor does not extend the requested header",
                        )
                        .into());
                    }
                    let successor_auxiliary_deliveries =
                        self.coherent_aux_deliveries(&successor_header_node)?;
                    Some(SelectedHeaderWithAuxiliaryDeliveries {
                        header_node: successor_header_node,
                        auxiliary_deliveries: successor_auxiliary_deliveries,
                    })
                }
                None => None,
            },
            Err(_) => None,
        };
        Ok(Some(SelectedAuxiliaryWindow {
            engine_snapshot: self
                .store
                .snapshot()
                .map_err(HeaderChainStoreError::Store)?,
            delivery_header: SelectedHeaderWithAuxiliaryDeliveries {
                header_node: delivery_header_node,
                auxiliary_deliveries: delivery_auxiliary_deliveries,
            },
            successor_header,
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

    pub(crate) fn committed_selected_locator(
        &self,
    ) -> Result<HeaderLocator, HeaderChainStoreError> {
        let engine = self
            .transition_engine
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        let snapshot = engine.snapshot();
        HeaderLocator::for_selected_path(&snapshot, |height| {
            let index = engine
                .selected_projection()
                .binary_search_by_key(&height, |frontier| frontier.height)
                .map_err(|_| StoreError::Incoherent("committed selected projection has a gap"))?;
            let frontier = engine.selected_projection()[index];
            let node = engine
                .graph()
                .header_node(frontier.hash)
                .ok_or(StoreError::Incoherent(
                    "committed selected projection references a missing node",
                ))?;
            if node.height != height || node.hash != frontier.hash {
                return Err(StoreError::Incoherent(
                    "committed selected projection disagrees with its node",
                ));
            }
            Ok(Some(frontier.hash))
        })
        .map_err(HeaderChainStoreError::Store)
    }

    /// Resolve an exact, still-current VCT repair owner to one selected header request.
    pub(crate) fn vct_repair_context(
        &self,
        owner: BodyWorkOwner,
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
        if owner.authority != BodyWorkAuthority::for_snapshot(&snapshot)
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
        scope: HeaderWorkAuthority,
    ) -> Result<RetainedPathLeaseOutcome, HeaderChainStoreError> {
        if locator_hashes.is_empty()
            || locator_hashes.len() > zakura_header_chain::MAX_HEADER_LOCATOR_HASHES
        {
            return Err(HeaderChainStoreError::Store(StoreError::Incoherent(
                "retained path locator count is outside protocol bounds",
            )));
        }
        let reservation_id = self
            .leases
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?
            .reserve(peer, Instant::now());
        let Some(reservation_id) = reservation_id else {
            return Ok(RetainedPathLeaseOutcome::Busy);
        };
        let reservation = RetainedPathReservation {
            leases: self.leases.clone(),
            peer,
            reservation_id,
            active: true,
        };
        let (snapshot, target, mut reverse_path) = {
            let engine = self
                .transition_engine
                .lock()
                .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
            let snapshot = engine.snapshot();
            if scope != HeaderWorkAuthority::for_target(&snapshot, target_tip_hash) {
                return Ok(RetainedPathLeaseOutcome::Busy);
            }
            let Some(target_node) = engine.graph().header_node(target_tip_hash) else {
                return Ok(RetainedPathLeaseOutcome::TargetNotRetained);
            };
            let target = Frontier::new(target_node.height, target_tip_hash);
            let mut reverse_path = vec![target];
            let mut current = target_node;
            while current.height > snapshot.frontiers.finalized.height {
                let Some(parent) = engine.graph().header_node(current.parent_hash) else {
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
            (snapshot, target, reverse_path)
        };
        if reverse_path.last().copied() != Some(snapshot.frontiers.finalized) {
            return Ok(RetainedPathLeaseOutcome::HistoryPruned);
        }
        reverse_path.reverse();
        let mut intersection = None;
        for locator_hash in locator_hashes {
            if let Some(common_index) = reverse_path
                .iter()
                .position(|frontier| frontier.hash == *locator_hash)
            {
                intersection = Some((
                    reverse_path[common_index],
                    CanonicalHeaderPathPosition::Retained { next: 0 },
                    common_index.saturating_add(1),
                    Some(reverse_path[common_index].hash),
                ));
                break;
            }
            if let Some(frontier) = self.finalized_frontier(*locator_hash)? {
                if frontier.height < snapshot.frontiers.finalized.height {
                    let next = frontier.height.next().map_err(|_| {
                        StoreError::Incoherent("canonical header cursor start height overflowed")
                    })?;
                    intersection = Some((
                        frontier,
                        CanonicalHeaderPathPosition::Finalized {
                            next,
                            end: snapshot.frontiers.finalized.height,
                        },
                        1,
                        None,
                    ));
                    break;
                }
            }
        }
        let Some((common_ancestor, mut position, retained_start, retained_ancestor)) = intersection
        else {
            return Ok(RetainedPathLeaseOutcome::NoLocatorIntersection);
        };
        let retained_path: Arc<[block::Hash]> = reverse_path[retained_start..]
            .iter()
            .map(|frontier| frontier.hash)
            .collect();
        if retained_path.is_empty()
            && matches!(position, CanonicalHeaderPathPosition::Retained { .. })
        {
            position = CanonicalHeaderPathPosition::Complete;
        }
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        let current_snapshot = self
            .transition_engine
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?
            .snapshot();
        if current_snapshot.state_version != snapshot.state_version
            || scope != HeaderWorkAuthority::for_target(&current_snapshot, target_tip_hash)
        {
            return Ok(RetainedPathLeaseOutcome::Busy);
        }
        reservation.commit(
            RetainedPathLeaseSpec {
                peer,
                session_id,
                target,
                common_ancestor,
                scope,
                position,
                retained_ancestor,
                retained_path,
            },
            Instant::now(),
        )
    }

    fn next_canonical_path_item(
        &self,
        cursor: &CanonicalHeaderPathCursor,
        position: &mut CanonicalHeaderPathPosition,
        previous: Frontier,
    ) -> Result<Option<(Frontier, Arc<block::Header>, Vec<AuxDelivery>)>, HeaderChainStoreError>
    {
        match *position {
            CanonicalHeaderPathPosition::Complete => Ok(None),
            CanonicalHeaderPathPosition::Finalized { next, end } => {
                if next > end || previous.height.next().ok() != Some(next) {
                    return Err(StoreError::Incoherent(
                        "finalized canonical header cursor has a non-contiguous height",
                    )
                    .into());
                }
                let hash_by_height = self.store.cf("hash_by_height")?;
                let hash: Option<block::Hash> = self.store.db.zs_get(&hash_by_height, &next);
                let hash = hash.ok_or(StoreError::Incoherent(
                    "finalized canonical header cursor has a missing hash",
                ))?;
                let frontier = Frontier::new(next, hash);
                let header = self.finalized_header(frontier)?;
                if header.previous_block_hash != previous.hash {
                    return Err(StoreError::Incoherent(
                        "finalized canonical header cursor has a non-contiguous parent",
                    )
                    .into());
                }
                *position = if next == end {
                    if cursor.retained_path.is_empty() {
                        CanonicalHeaderPathPosition::Complete
                    } else {
                        CanonicalHeaderPathPosition::Retained { next: 0 }
                    }
                } else {
                    CanonicalHeaderPathPosition::Finalized {
                        next: next.next().map_err(|_| {
                            StoreError::Incoherent(
                                "finalized canonical header cursor height overflowed",
                            )
                        })?,
                        end,
                    }
                };
                Ok(Some((frontier, header, Vec::new())))
            }
            CanonicalHeaderPathPosition::Retained { next } => {
                let Some(hash) = cursor.retained_path.get(next).copied() else {
                    return Err(StoreError::Incoherent(
                        "retained canonical header cursor exceeded its immutable suffix",
                    )
                    .into());
                };
                let node = self
                    .retained_path_node(hash)?
                    .ok_or(StoreError::Incoherent(
                        "active canonical header cursor node is absent",
                    ))?;
                if previous.height.next().ok() != Some(node.height)
                    || node.parent_hash != previous.hash
                {
                    return Err(StoreError::Incoherent(
                        "retained canonical header cursor has a non-contiguous item",
                    )
                    .into());
                }
                let deliveries =
                    self.coherent_aux_deliveries_for(node.hash, &node.aux_delivery_ids)?;
                let frontier = Frontier::new(node.height, node.hash);
                *position = if next.saturating_add(1) == cursor.retained_path.len() {
                    CanonicalHeaderPathPosition::Complete
                } else {
                    CanonicalHeaderPathPosition::Retained {
                        next: next.saturating_add(1),
                    }
                };
                Ok(Some((frontier, node.header, deliveries)))
            }
        }
    }

    pub(crate) fn read_retained_path(
        &self,
        peer: SourceId,
        session_id: u64,
        lease_id: u64,
        scope: HeaderWorkAuthority,
        after_hash: block::Hash,
        max_count: u32,
    ) -> Result<RetainedPathReadOutcome, HeaderChainStoreError> {
        if max_count == 0 || max_count > crate::constants::MAX_HEADER_SYNC_HEIGHT_RANGE {
            return Err(HeaderChainStoreError::Store(StoreError::Incoherent(
                "retained path page count is outside protocol bounds",
            )));
        }
        let lease = self
            .leases
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?
            .get(peer, session_id, lease_id, Instant::now());
        let Some(lease) = lease else {
            return Ok(RetainedPathReadOutcome::Unavailable);
        };
        if lease.scope != scope {
            return Ok(RetainedPathReadOutcome::Unavailable);
        }
        if after_hash != lease.last_frontier.hash {
            return Ok(RetainedPathReadOutcome::Unavailable);
        }
        let read_version = self.store.snapshot()?.state_version;
        let page_ancestor = lease.last_frontier;
        let count = usize::try_from(max_count).unwrap_or(usize::MAX);
        let mut headers = Vec::with_capacity(count.min(usize::from(u16::MAX)));
        let mut aux_deliveries = Vec::with_capacity(headers.capacity());
        let mut previous = page_ancestor;
        let mut position = lease.position;
        let page_result: Result<bool, HeaderChainStoreError> = (|| {
            while headers.len() < count {
                let Some((frontier, header, deliveries)) =
                    self.next_canonical_path_item(&lease, &mut position, previous)?
                else {
                    break;
                };
                previous = frontier;
                headers.push(header);
                aux_deliveries.push(deliveries);
            }
            let complete = matches!(position, CanonicalHeaderPathPosition::Complete);
            if complete && previous != lease.target {
                return Err(StoreError::Incoherent(
                    "canonical header cursor completed before its exact target",
                )
                .into());
            }
            Ok(complete)
        })();
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        let current_version = self.store.snapshot()?.state_version;
        if current_version != read_version {
            return Ok(RetainedPathReadOutcome::Unavailable);
        }
        let complete = page_result?;
        let advanced = self
            .leases
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?
            .advance(
                peer,
                session_id,
                lease_id,
                CanonicalHeaderPathAdvance {
                    expected_after: page_ancestor,
                    position,
                    last_frontier: previous,
                    now: Instant::now(),
                },
            );
        if !advanced {
            return Ok(RetainedPathReadOutcome::Unavailable);
        }
        Ok(RetainedPathReadOutcome::Page(Box::new(RetainedPathPage {
            lease_id,
            common_ancestor: page_ancestor,
            target: lease.target,
            scope: lease.scope,
            headers,
            aux_deliveries,
            complete,
        })))
    }

    pub(crate) fn release_retained_path(
        &self,
        peer: SourceId,
        session_id: u64,
        lease_id: u64,
        scope: HeaderWorkAuthority,
    ) -> Result<bool, HeaderChainStoreError> {
        Ok(self
            .leases
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?
            .release(peer, session_id, lease_id, scope))
    }
}

impl HeaderChainRuntime {
    /// Captures the complete selected projection and the engine snapshot that produced it.
    ///
    /// The method rejects an engine whose projection bounds disagree with its snapshot.
    pub(crate) fn capture_selected_projection(
        &self,
    ) -> Result<CapturedSelectedProjection, HeaderChainStoreError> {
        let engine = self
            .transition_engine
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        let engine_snapshot = engine.snapshot();
        let frontiers = engine.selected_projection().to_vec();
        if frontiers.first().copied() != Some(engine_snapshot.frontiers.finalized)
            || frontiers.last().copied() != Some(engine_snapshot.frontiers.header_best)
        {
            return Err(HeaderChainStoreError::Incoherent(
                "selected projection disagrees with its published bounds",
            ));
        }
        Ok(CapturedSelectedProjection {
            engine_snapshot,
            frontiers,
        })
    }

    /// Reads one auxiliary window from the current in-memory selected projection.
    ///
    /// The method returns `None` when the selected projection does not contain the requested
    /// height and hash.
    pub(crate) fn selected_auxiliary_window(
        &self,
        height: block::Height,
        hash: block::Hash,
    ) -> Result<Option<SelectedAuxiliaryWindow>, HeaderChainStoreError> {
        let engine = self
            .transition_engine
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        let Ok(index) = engine
            .selected_projection()
            .binary_search_by_key(&height, |frontier| frontier.height)
        else {
            return Ok(None);
        };
        Self::selected_auxiliary_window_at_projection_index_locked(
            &engine,
            index,
            Frontier::new(height, hash),
        )
    }

    /// Reads one auxiliary window at an index from a previously captured selected projection.
    ///
    /// The method returns `None` when the live projection no longer contains the expected
    /// frontier at `projection_index`. The returned window carries a new engine snapshot so the
    /// caller can detect any generation change.
    pub(crate) fn selected_auxiliary_window_at_projection_index(
        &self,
        projection_index: usize,
        expected_frontier: Frontier,
    ) -> Result<Option<SelectedAuxiliaryWindow>, HeaderChainStoreError> {
        let engine = self
            .transition_engine
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        Self::selected_auxiliary_window_at_projection_index_locked(
            &engine,
            projection_index,
            expected_frontier,
        )
    }

    fn selected_auxiliary_window_at_projection_index_locked(
        engine: &HeaderChainEngine,
        projection_index: usize,
        expected_frontier: Frontier,
    ) -> Result<Option<SelectedAuxiliaryWindow>, HeaderChainStoreError> {
        let Some(current_frontier) = engine.selected_projection().get(projection_index).copied()
        else {
            return Ok(None);
        };
        if current_frontier != expected_frontier {
            return Ok(None);
        }
        let delivery_header_node = engine
            .graph()
            .header_node(expected_frontier.hash)
            .cloned()
            .ok_or(HeaderChainStoreError::Incoherent(
                "selected projection references a missing in-memory node",
            ))?;
        if delivery_header_node.height != expected_frontier.height
            || delivery_header_node.hash != expected_frontier.hash
        {
            return Err(HeaderChainStoreError::Incoherent(
                "selected projection disagrees with its in-memory node",
            ));
        }
        let delivery_auxiliary_deliveries =
            coherent_engine_aux_deliveries(engine, &delivery_header_node)?;
        let successor_header = if let Some(successor_frontier) =
            engine.selected_projection().get(projection_index + 1)
        {
            let expected_successor_height = expected_frontier.height.next().map_err(|_| {
                HeaderChainStoreError::Incoherent("selected auxiliary successor height overflowed")
            })?;
            let successor_header_node = engine
                .graph()
                .header_node(successor_frontier.hash)
                .cloned()
                .ok_or(HeaderChainStoreError::Incoherent(
                    "selected successor references a missing in-memory node",
                ))?;
            if successor_frontier.height != expected_successor_height
                || successor_header_node.height != expected_successor_height
                || successor_header_node.hash != successor_frontier.hash
                || successor_header_node.parent_hash != expected_frontier.hash
            {
                return Err(HeaderChainStoreError::Incoherent(
                    "selected in-memory successor is not contiguous",
                ));
            }
            let successor_auxiliary_deliveries =
                coherent_engine_aux_deliveries(engine, &successor_header_node)?;
            Some(SelectedHeaderWithAuxiliaryDeliveries {
                header_node: successor_header_node,
                auxiliary_deliveries: successor_auxiliary_deliveries,
            })
        } else {
            None
        };
        Ok(Some(SelectedAuxiliaryWindow {
            engine_snapshot: engine.snapshot(),
            delivery_header: SelectedHeaderWithAuxiliaryDeliveries {
                header_node: delivery_header_node,
                auxiliary_deliveries: delivery_auxiliary_deliveries,
            },
            successor_header,
        }))
    }

    pub(in crate::service) fn operator_invalidation_evidence(
        &self,
        target: block::Hash,
        id: zakura_header_chain::OperatorInvalidationId,
    ) -> Result<Option<EvidenceId>, HeaderChainStoreError> {
        let engine = self
            .transition_engine
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        Ok(engine.graph().header_node(target).and_then(|node| {
            node.eligibility
                .direct_reasons
                .iter()
                .find_map(|reason| match reason {
                    EligibilityReason::OperatorInvalid {
                        id: existing,
                        evidence,
                        ..
                    } if *existing == id => Some(*evidence),
                    _ => None,
                })
        }))
    }

    /// Return the sole committed-snapshot publisher.
    pub fn publisher(&self) -> &Publisher {
        &self.publisher
    }

    /// Return a read-only handle whose compound reads share the transition lock.
    pub(crate) fn reader(&self) -> HeaderChainReader {
        HeaderChainReader {
            store: self.store.clone(),
            config: Arc::new(self.config.clone()),
            leases: self.leases.clone(),
            transition_engine: self.transition_engine.clone(),
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
                CombinedStateExpectation::NONE,
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
                CombinedStateExpectation::NONE,
            )
        }
    }

    pub(in crate::service) fn apply_combined_expected<M>(
        &self,
        request: TransitionRequest,
        context: &TransitionContext<'_>,
        full_state_batch: DiskWriteBatch,
        expected_verified: Frontier,
        expected_staged: &[VerifiedHeaderRef],
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
                CombinedStateExpectation {
                    verified: Some(expected_verified),
                    staged: expected_staged,
                },
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
                CombinedStateExpectation {
                    verified: Some(expected_verified),
                    staged: expected_staged,
                },
            )
        }
    }

    /// Atomically apply auxiliary authentication followed by one checkpoint full-state advance.
    ///
    /// The planner creates the checkpoint transition against the projected auxiliary transition.
    /// One RocksDB write commits both header-chain changes and the full-state block batch.
    pub(in crate::service) fn apply_aux_then_checkpoint_combined<M>(
        &self,
        first_request: TransitionRequest,
        first_context: &TransitionContext<'_>,
        checkpoint_request: TransitionRequest,
        checkpoint_context: &TransitionContext<'_>,
        full_state_batch: DiskWriteBatch,
        memory_swap: M,
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
        let lease_references = self
            .leases
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?
            .active_references(Instant::now());

        let first_authority = StateIssuedAuthority {
            inner: first_context.full_state_authority,
            validation_leases: &[],
            active_retention_references: lease_references.as_ref(),
        };
        let first_context = TransitionContext {
            config: first_context.config,
            clock: first_context.clock,
            full_state_authority: Some(&first_authority),
            retention_references: lease_references.as_ref(),
        };
        let checkpoint_parent = match &checkpoint_request.event {
            TransitionEvent::VerifiedChainChanged(event)
                if event.cause == VerifiedChangeCause::CheckpointFinalizedGrow =>
            {
                event.old_tip
            }
            _ => {
                return Err(HeaderChainStoreError::Incoherent(
                    "combined checkpoint transition has the wrong event kind",
                ));
            }
        };
        let checkpoint_headers_are_retained = match &checkpoint_request.event {
            TransitionEvent::VerifiedChainChanged(event) => event
                .new_path
                .iter()
                .all(|header| transition_engine.graph().header_node(header.hash).is_some()),
            _ => false,
        };
        // Header sync normally admits headers before native checkpoint growth promotes them.
        // Only a missing header needs contextual validation and a validation lease.
        let validation_leases = if checkpoint_headers_are_retained {
            Vec::new()
        } else {
            vec![self
                .store
                .validation_context(checkpoint_parent.hash, self.config.network())?]
        };
        let checkpoint_authority = StateIssuedAuthority {
            inner: checkpoint_context.full_state_authority,
            validation_leases: validation_leases.as_slice(),
            active_retention_references: lease_references.as_ref(),
        };
        let checkpoint_context = TransitionContext {
            config: checkpoint_context.config,
            clock: checkpoint_context.clock,
            full_state_authority: Some(&checkpoint_authority),
            retention_references: lease_references.as_ref(),
        };

        let TransitionEvent::AuxEvidence(first_event) = first_request.event else {
            return Err(HeaderChainStoreError::Incoherent(
                "combined auxiliary transition has the wrong event kind",
            ));
        };
        let first = transition_engine.plan_transition(
            TransitionInput::AuxEvidence { event: first_event },
            &first_context,
        )?;
        if first.effect().is_resource_stalled() {
            return Err(HeaderChainStoreError::Incoherent(
                "checkpoint auxiliary authentication exhausted header resources",
            ));
        }
        let batch = self
            .store
            .batch_for_combined(first.change_set(), full_state_batch)?;
        // The runtime uses the engine mutex as its coherent read boundary.
        // The publisher continues to expose the durable snapshot.
        // The writer can stage the first transition without exposing it.
        // The runtime reloads the unchanged durable engine after an error before the atomic write.
        if let Err(error) = transition_engine.install_committed_transition(first) {
            let error = restore_transition_engine_after_staging_error(
                &self.store,
                &mut transition_engine,
                error.into(),
            );
            return Err(error);
        }

        let expected_version = transition_engine.snapshot().state_version;
        validate_full_state_finality_provenance(
            &checkpoint_request.event,
            &transition_engine.snapshot(),
        )?;
        let TransitionEvent::VerifiedChainChanged(checkpoint_event) = checkpoint_request.event
        else {
            return Err(HeaderChainStoreError::Incoherent(
                "combined checkpoint transition has the wrong event kind",
            ));
        };
        let checkpoint = match transition_engine.plan_transition(
            TransitionInput::VerifiedChainChanged {
                expected_version,
                event: checkpoint_event,
                facts: HeaderValidationFacts {
                    validation_leases: validation_leases.to_vec(),
                },
            },
            &checkpoint_context,
        ) {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                let error = restore_transition_engine_after_staging_error(
                    &self.store,
                    &mut transition_engine,
                    error.into(),
                );
                return Err(error);
            }
        };
        if checkpoint.effect().is_resource_stalled() {
            let error = restore_transition_engine_after_staging_error(
                &self.store,
                &mut transition_engine,
                HeaderChainStoreError::Incoherent(
                    "checkpoint full-state advance exhausted header resources",
                ),
            );
            return Err(error);
        }
        let checkpoint_effect = checkpoint.effect();

        let current = checkpoint.snapshot_after_commit();
        let batch = match self
            .store
            .batch_for_combined(checkpoint.change_set(), batch)
        {
            Ok(batch) => batch,
            Err(error) => {
                let error = restore_transition_engine_after_staging_error(
                    &self.store,
                    &mut transition_engine,
                    error,
                );
                return Err(error);
            }
        };
        if let Err(error) = self.store.db.write(batch) {
            let error = restore_transition_engine_after_staging_error(
                &self.store,
                &mut transition_engine,
                error.into(),
            );
            return Err(error);
        }
        if let Err(error) = transition_engine.install_committed_transition(checkpoint) {
            let error = restore_transition_engine_after_staging_error(
                &self.store,
                &mut transition_engine,
                error.into(),
            );
            return Err(error);
        }
        memory_swap();
        self.publisher.publish(current, checkpoint_effect);
        Ok(ApplyResult::Committed)
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
        self.apply_combined_inner(
            request,
            context,
            full_state_batch,
            memory_swap,
            CombinedStateExpectation::NONE,
            fault,
        )
    }

    /// Bind a state-service request to the exact durable facts its event may consume.
    fn build_transition_input(
        &self,
        request: TransitionRequest,
        before: &EngineSnapshot,
        network: &Network,
    ) -> Result<TransitionInput, HeaderChainStoreError> {
        let expected_version = request.expected_version;
        Ok(match request.event {
            TransitionEvent::InsertHeaders(event) => {
                let anchor_changed = event.owner.header_authority().branch.anchor_hash
                    != before.frontiers.finalized.hash;
                let mut validation_leases = Vec::new();
                if self.store.header_node(event.parent_hash)?.is_some() {
                    validation_leases
                        .push(self.store.validation_context(event.parent_hash, network)?);
                }
                if anchor_changed && event.parent_hash != before.frontiers.finalized.hash {
                    validation_leases.push(
                        self.store
                            .validation_context(before.frontiers.finalized.hash, network)?,
                    );
                }
                validation_leases.dedup_by_key(|lease| lease.parent());
                let finality_rebase_history = self.store.finality_rebase_history(
                    event.owner.header_authority().branch.anchor_hash,
                    before.frontiers.finalized,
                    before
                        .header_generation
                        .get()
                        .saturating_sub(event.owner.header_authority().header_generation.get()),
                )?;
                TransitionInput::InsertHeaders {
                    event,
                    facts: HeaderInsertionFacts {
                        validation: HeaderValidationFacts { validation_leases },
                        finality_rebase_history,
                    },
                }
            }
            TransitionEvent::VerifiedChainChanged(event) => {
                let parent = match event.cause {
                    VerifiedChangeCause::Grow | VerifiedChangeCause::CheckpointFinalizedGrow => {
                        event.old_tip
                    }
                    VerifiedChangeCause::Reset => before.frontiers.finalized,
                };
                TransitionInput::VerifiedChainChanged {
                    expected_version,
                    event,
                    facts: HeaderValidationFacts {
                        validation_leases: vec![self
                            .store
                            .validation_context(parent.hash, network)?],
                    },
                }
            }
            TransitionEvent::VerifiedBlockAccepted(event) => {
                TransitionInput::VerifiedBlockAccepted {
                    expected_version,
                    event,
                    facts: HeaderValidationFacts {
                        validation_leases: vec![self
                            .store
                            .validation_context(before.frontiers.finalized.hash, network)?],
                    },
                }
            }
            TransitionEvent::BodyEvidence(event) => TransitionInput::BodyEvidence {
                expected_version,
                event,
            },
            TransitionEvent::BodySupplierDiscovered(event) => {
                TransitionInput::BodySupplierDiscovered {
                    expected_version,
                    event,
                }
            }
            TransitionEvent::OperatorBodyRetry(event) => TransitionInput::OperatorBodyRetry {
                expected_version,
                event,
            },
            TransitionEvent::OperatorInvalidate(event) => TransitionInput::OperatorInvalidate {
                expected_version,
                event,
            },
            TransitionEvent::OperatorReconsider(event) => TransitionInput::OperatorReconsider {
                expected_version,
                event,
            },
            TransitionEvent::FullStateFinalized(event) => TransitionInput::FullStateFinalized {
                expected_version,
                event,
            },
            TransitionEvent::MigratedPinRefutation(event) => {
                let preserved_pin = self
                    .store
                    .is_migrated_finality_pin(event.pin)?
                    .then_some(event.pin);
                TransitionInput::MigratedPinRefutation {
                    expected_version,
                    event,
                    preserved_pin,
                }
            }
            TransitionEvent::AuxEvidence(event) => TransitionInput::AuxEvidence { event },
            TransitionEvent::ReevaluateDeferred => {
                TransitionInput::ReevaluateDeferred { expected_version }
            }
        })
    }

    fn apply_combined_inner<M>(
        &self,
        request: TransitionRequest,
        context: &TransitionContext<'_>,
        full_state_batch: DiskWriteBatch,
        memory_swap: M,
        expectation: CombinedStateExpectation<'_>,
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
        let authoritative_full_state_fork_set = matches!(
            &request.event,
            TransitionEvent::VerifiedChainChanged(_) | TransitionEvent::VerifiedBlockAccepted(_)
        ) && context
            .full_state_authority
            .is_some_and(|authority| authority.authorizes_full_state(&request.event));
        let lease_references = if authoritative_full_state_fork_set {
            None
        } else {
            Some(
                self.leases
                    .lock()
                    .map_err(|_| HeaderChainStoreError::WriterPoisoned)?
                    .active_references(Instant::now()),
            )
        };
        let retention_references = combined_retention_references(
            context.retention_references,
            lease_references.as_deref(),
        );
        #[cfg(test)]
        let test_header_authority = TestHeaderCompletionAuthority(context.full_state_authority);
        #[cfg(test)]
        let full_state_authority = Some(&test_header_authority as &dyn FullStateEvidenceAuthority);
        #[cfg(not(test))]
        let full_state_authority = context.full_state_authority;
        let base_context = TransitionContext {
            config: context.config,
            clock: context.clock,
            full_state_authority,
            retention_references: retention_references.as_ref(),
        };
        let before = transition_engine.snapshot();
        if let Some(pin) = before.alarms.migrated_pin_refuted {
            return Err(HeaderChainStoreError::MigratedPinRefuted { pin });
        }
        if request.expected_version == before.state_version {
            validate_full_state_finality_provenance(&request.event, &before)?;
        }
        let event = request.event.idempotency_key();
        let branch = request
            .event
            .header_sync_owner()
            .map(HeaderSyncWorkOwner::header_authority)
            .map(|authority| authority.branch)
            .or_else(|| request.event.body_owner().map(|owner| owner.branch));
        let input = self.build_transition_input(request, &before, base_context.config.network())?;
        let validation_leases = input
            .header_validation_facts()
            .map(|facts| facts.validation_leases.clone())
            .unwrap_or_default();
        let state_authority = StateIssuedAuthority {
            inner: base_context.full_state_authority,
            validation_leases: &validation_leases,
            active_retention_references: lease_references.as_deref().unwrap_or_default(),
        };
        let transition_context = TransitionContext {
            config: base_context.config,
            clock: base_context.clock,
            full_state_authority: Some(&state_authority),
            retention_references: base_context.retention_references,
        };
        let transition = match transition_engine.plan_transition(input, &transition_context) {
            Ok(plan) => plan,
            Err(TransitionFailure::Stale { current }) => {
                return Ok(ApplyResult::Stale(StaleReceipt {
                    current_version: current,
                    branch,
                }));
            }
            Err(error) => return Err(error.into()),
        };
        let transition_effect = transition.effect();
        let resource_stalled = transition_effect.is_resource_stalled();
        let stall_receipt = resource_stalled.then(|| CommittedStallReceipt {
            state_version: transition.change_set().metadata.state_version,
            alarm_changed: transition.snapshot_before_commit().alarms.resource_stalled
                != transition.change_set().metadata.alarms.resource_stalled,
            attempted_branch: branch,
        });
        if transition_effect.is_header_work_rebased() {
            metrics::counter!("state.header.work.rebase.total", "outcome" => "rebased")
                .increment(1);
            metrics::counter!(
                "state.header.work.rebase.headers.total",
                "outcome" => "rebased"
            )
            .increment(u64::try_from(transition.change_set().put_nodes.len()).unwrap_or(u64::MAX));
        } else if transition_effect.is_header_work_already_applied() {
            metrics::counter!("state.header.work.rebase.total", "outcome" => "already_applied")
                .increment(1);
        }
        if resource_stalled {
            let receipt = stall_receipt.expect("resource-stalled transitions construct a receipt");
            if transition.is_no_change() {
                return Ok(ApplyResult::ResourceStalled(receipt));
            }
            let current = transition.snapshot_after_commit();
            let batch = self.store.batch_for(transition.change_set())?;
            #[cfg(test)]
            fault(FaultPoint::BeforeCommit)?;
            self.store.db.write(batch)?;
            transition_engine.install_committed_transition(transition)?;
            #[cfg(test)]
            fault(FaultPoint::AfterCommit)?;
            self.publisher.publish(current, transition_effect);
            #[cfg(test)]
            fault(FaultPoint::AfterPublish)?;
            return Ok(ApplyResult::ResourceStalled(receipt));
        }
        if !expectation.staged.is_empty() {
            let put_nodes: HashMap<_, _> = transition
                .change_set()
                .put_nodes
                .iter()
                .map(|node| (node.hash, node))
                .collect();
            let deleted: std::collections::HashSet<_> = transition
                .change_set()
                .delete_nodes
                .iter()
                .copied()
                .collect();
            for expected in expectation.staged {
                let projected = if deleted.contains(&expected.hash) {
                    None
                } else if let Some(node) = put_nodes.get(&expected.hash) {
                    Some((*node).clone())
                } else {
                    self.store.header_node(expected.hash)?
                };
                let matches = projected.is_some_and(|node| {
                    node.height == expected.height
                        && node.hash == expected.hash
                        && node.parent_hash == expected.header.previous_block_hash
                });
                if !matches {
                    return Err(HeaderChainStoreError::StagedPathMismatch {
                        hash: expected.hash,
                    });
                }
            }
        }
        if let Some(expected) = expectation.verified {
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
                state_version: transition.snapshot_before_commit().state_version,
                idempotency_key: event,
            }));
        }

        let current = transition.snapshot_after_commit();
        let migrated_pin_refuted = transition.change_set().metadata.alarms.migrated_pin_refuted;
        let batch = self
            .store
            .batch_for_combined(transition.change_set(), full_state_batch)?;
        #[cfg(feature = "internal-bench")]
        BENCH_BATCH_BYTES.store(
            u64::try_from(batch.size_in_bytes()).unwrap_or(u64::MAX),
            std::sync::atomic::Ordering::Relaxed,
        );
        #[cfg(test)]
        fault(FaultPoint::BeforeCommit)?;
        self.store.db.write(batch)?;
        transition_engine.install_committed_transition(transition)?;
        #[cfg(test)]
        fault(FaultPoint::AfterCommit)?;
        if let Some(pin) = migrated_pin_refuted {
            return Err(HeaderChainStoreError::MigratedPinRefuted { pin });
        }
        memory_swap();
        #[cfg(test)]
        fault(FaultPoint::AfterMemorySwap)?;
        self.publisher.publish(current, transition_effect);
        #[cfg(test)]
        fault(FaultPoint::AfterPublish)?;
        Ok(ApplyResult::Committed)
    }
}

fn validate_full_state_finality_provenance(
    event: &TransitionEvent,
    snapshot: &EngineSnapshot,
) -> Result<(), HeaderChainStoreError> {
    let matches = match event {
        TransitionEvent::FullStateFinalized(event) => {
            event.full_state_transition_id
                == zakura_header_chain::full_state_finality_evidence(
                    snapshot.state_version,
                    event.new_finalized,
                    &event.verified_path_proof,
                )
        }
        TransitionEvent::VerifiedChainChanged(event)
            if event.cause == VerifiedChangeCause::CheckpointFinalizedGrow =>
        {
            event.new_path.last().is_none_or(|header| {
                event.full_state_transition_id
                    == zakura_header_chain::checkpoint_finality_evidence(
                        snapshot.state_version,
                        Frontier::new(header.height, header.hash),
                    )
            })
        }
        _ => true,
    };
    if !matches {
        return Err(HeaderChainStoreError::Incoherent(
            "full-state finality provenance does not match the authorized transition",
        ));
    }
    Ok(())
}

/// One end-to-end headers-only finality sample for the internal RocksDB benchmark.
#[cfg(feature = "internal-bench")]
#[derive(Clone, Debug)]
pub struct FinalityWitnessBenchmarkSample {
    /// Finality advance number after the configured depth was reached.
    pub advance: u64,
    /// End-to-end transition latency.
    pub elapsed: Duration,
    /// Logical point reads against witness rows.
    pub witness_point_reads: u64,
    /// Witness rows written or deleted by the batch.
    pub witness_row_writes: u64,
    /// Exact serialized RocksDB write-batch size.
    pub batch_bytes: u64,
    /// Retained finality-history rows after the transition.
    pub history_rows: u64,
    /// Retained witness-DAG rows after the transition.
    pub witness_rows: u64,
}

/// Bounded result from the internal RocksDB finality benchmark.
#[cfg(feature = "internal-bench")]
#[derive(Clone, Debug)]
pub struct FinalityWitnessBenchmarkReport {
    /// Per-advance samples.
    pub samples: Vec<FinalityWitnessBenchmarkSample>,
    /// A reorg that replaces one selected header.
    pub one_block_reorg: FinalityWitnessBenchmarkSample,
    /// A reorg that replaces up to 32 selected headers.
    pub bounded_reorg: FinalityWitnessBenchmarkSample,
    /// Startup audit latency after all advances.
    pub startup_elapsed: Duration,
}

/// Run sequential headers-only finality advances against an ephemeral RocksDB store.
#[cfg(feature = "internal-bench")]
pub fn benchmark_finality_witness(
    genesis: Arc<block::Block>,
    advances: u32,
    depth: u32,
) -> Result<FinalityWitnessBenchmarkReport, HeaderChainStoreError> {
    use std::num::{NonZeroU32, NonZeroU64};

    struct BenchmarkAuthority;

    impl FullStateEvidenceAuthority for BenchmarkAuthority {
        fn authorizes_full_state(&self, _event: &TransitionEvent) -> bool {
            false
        }

        fn authorizes_header_completion(
            &self,
            _insert: &zakura_header_chain::InsertHeaders,
        ) -> bool {
            true
        }
    }

    let network =
        Network::new_regtest(zakura_chain::parameters::testnet::RegtestParameters::default());
    let anchor = Frontier::new(block::Height(0), genesis.hash());
    let mut config = EngineConfig::new(
        EngineMode::HeadersOnly,
        network,
        zakura_header_chain::TrustedAnchor {
            frontier: anchor,
            header: genesis.header.clone(),
        },
        zakura_header_chain::CheckpointSet::default(),
    )
    .map_err(|_| HeaderChainStoreError::Incoherent("benchmark configuration is invalid"))?;
    config.limits.local_finality_depth = NonZeroU32::new(depth).ok_or(
        HeaderChainStoreError::Incoherent("benchmark finality depth must be nonzero"),
    )?;
    let anchor_work =
        genesis
            .header
            .difficulty_threshold
            .to_work()
            .ok_or(HeaderChainStoreError::Incoherent(
                "benchmark anchor work is invalid",
            ))?;
    let anchor_node = HeaderNode::from_durable_parts(
        genesis.header.clone(),
        anchor.hash,
        genesis.header.previous_block_hash,
        anchor.height,
        anchor_work,
        zakura_header_chain::WorkCoordinate::new(anchor.hash, anchor_work.as_u256()),
        zakura_header_chain::HeaderValidationState::Valid,
        Default::default(),
        zakura_header_chain::BodyValidationState::Unknown,
        Vec::new(),
    )
    .map_err(|_| HeaderChainStoreError::Incoherent("benchmark anchor is invalid"))?;
    let metadata = EngineMetadata {
        disk_format: zakura_header_chain::HeaderChainDiskVersion::CURRENT,
        mode: EngineMode::HeadersOnly,
        network_id: config.network().kind(),
        network_policy_digest: config.network_policy_digest(),
        anchor_manifest_digest: config.trust_anchor_digest(),
        work_origin: anchor,
        state_version: StateVersion::new(1),
        header_generation: zakura_header_chain::HeaderGeneration::new(1),
        verified_generation: zakura_header_chain::VerifiedGeneration::new(1),
        finality_epoch: zakura_header_chain::FinalityEpoch::new(0),
        headers_only_migration_epoch: None,
        frontiers: zakura_header_chain::FrontierSet {
            finalized: anchor,
            header_best: anchor,
            verified_best: anchor,
        },
        header_best_score: zakura_header_chain::ChainScore::new(
            zakura_header_chain::SuffixWork::zero(),
            anchor.hash,
        ),
        oldest_retained_height: anchor.height,
        alarms: zakura_header_chain::AlarmSet::default(),
        last_transition: None,
    };
    let db_config = crate::Config::ephemeral();
    let db = DiskDb::new(
        &db_config,
        crate::constants::STATE_DATABASE_KIND,
        &crate::constants::state_database_format_version_in_code(),
        config.network(),
        super::STATE_COLUMN_FAMILIES_IN_CODE
            .iter()
            .map(ToString::to_string),
        false,
    )
    .map_err(|_| HeaderChainStoreError::Incoherent("benchmark RocksDB could not open"))?;
    let store = HeaderChainStore::new(db.clone());
    store.initialize(metadata, anchor_node)?;
    let (runtime, _) = store.startup(&config)?;
    let mut parent = anchor;
    let mut parent_header = genesis.header.clone();
    let mut selected_chain = vec![(anchor, genesis.header.clone())];
    let mut samples = Vec::with_capacity(usize::try_from(advances).unwrap_or(0));
    let authority = BenchmarkAuthority;
    for height in 1..=advances.saturating_add(depth) {
        let before = runtime.publisher().snapshot();
        let lease = runtime.reader().validation_context(parent.hash)?.ok_or(
            HeaderChainStoreError::Incoherent("benchmark parent is not retained"),
        )?;
        let rules = zakura_header_chain::HeaderRules::for_validation_lease(&lease)
            .map_err(|_| HeaderChainStoreError::Incoherent("benchmark rules are invalid"))?;
        let mut header = *parent_header;
        header.previous_block_hash = parent.hash;
        header.time += chrono::Duration::seconds(1);
        header.nonce.0[..4].copy_from_slice(&height.to_be_bytes());
        let header = Arc::new(header);
        let next = Frontier::new(block::Height(height), header.hash());
        let prepared = zakura_header_chain::prepare_headers(
            zakura_header_chain::HeaderBatchInput::new(std::slice::from_ref(&header)),
            lease.parent(),
            &rules,
            &SystemClock,
        )
        .map_err(|_| HeaderChainStoreError::Incoherent("benchmark header did not prepare"))?;
        let owner = zakura_header_chain::HeaderWorkAuthority::for_target(&before, next.hash)
            .bind(
                u64::from(height),
                NonZeroU64::new(u64::from(height)).expect("height is nonzero"),
            )
            .into();
        let request = TransitionRequest {
            expected_version: before.state_version,
            event: TransitionEvent::InsertHeaders(Box::new(zakura_header_chain::InsertHeaders {
                owner,
                source: SourceId::from_digest([0x91; 32]),
                parent_hash: parent.hash,
                target_tip_hash: next.hash,
                completion: zakura_header_chain::TargetCompletion::TargetComplete {
                    common_ancestor: parent,
                },
                batch: prepared,
                aux: Vec::new(),
            })),
        };
        BENCH_WITNESS_POINT_READS.store(0, std::sync::atomic::Ordering::Relaxed);
        BENCH_WITNESS_ROW_WRITES.store(0, std::sync::atomic::Ordering::Relaxed);
        BENCH_BATCH_BYTES.store(0, std::sync::atomic::Ordering::Relaxed);
        let started = std::time::Instant::now();
        runtime.apply(
            request,
            &TransitionContext {
                config: &config,
                clock: &SystemClock,
                full_state_authority: Some(&authority),
                retention_references: &[],
            },
        )?;
        let elapsed = started.elapsed();
        if height > depth {
            samples.push(FinalityWitnessBenchmarkSample {
                advance: u64::from(height - depth),
                elapsed,
                witness_point_reads: BENCH_WITNESS_POINT_READS
                    .load(std::sync::atomic::Ordering::Relaxed),
                witness_row_writes: BENCH_WITNESS_ROW_WRITES
                    .load(std::sync::atomic::Ordering::Relaxed),
                batch_bytes: BENCH_BATCH_BYTES.load(std::sync::atomic::Ordering::Relaxed),
                history_rows: runtime
                    .store
                    .get_value::<HeaderRowCountDisk>(
                        HEADER_ENGINE_META,
                        FINALITY_HISTORY_COUNT_KEY,
                    )?
                    .map_or(0, |count| count.0),
                witness_rows: runtime
                    .store
                    .get_value::<HeaderRowCountDisk>(
                        HEADER_ENGINE_META,
                        FINALITY_WITNESS_COUNT_KEY,
                    )?
                    .map_or(0, |count| count.0),
            });
        }
        parent = next;
        parent_header = header.clone();
        selected_chain.push((next, header));
    }
    let mut reorg_samples = Vec::with_capacity(2);
    for (round, replaced_suffix) in [1_u32, depth.clamp(1, 32)].into_iter().enumerate() {
        let suffix = usize::try_from(replaced_suffix).map_err(|_| {
            HeaderChainStoreError::Incoherent("benchmark reorg depth does not fit usize")
        })?;
        let base_index = selected_chain.len().checked_sub(suffix + 1).ok_or(
            HeaderChainStoreError::Incoherent("benchmark chain is shorter than its reorg"),
        )?;
        let (base, mut fork_parent_header) = selected_chain[base_index].clone();
        let before = runtime.publisher().snapshot();
        let lease = runtime.reader().validation_context(base.hash)?.ok_or(
            HeaderChainStoreError::Incoherent("benchmark reorg parent is not retained"),
        )?;
        let rules = zakura_header_chain::HeaderRules::for_validation_lease(&lease)
            .map_err(|_| HeaderChainStoreError::Incoherent("benchmark reorg rules are invalid"))?;
        let mut fork_headers = Vec::with_capacity(suffix + 1);
        let mut fork_parent = base;
        for offset in 1..=replaced_suffix.saturating_add(1) {
            let height = u32::from(base.height).saturating_add(offset);
            let mut header = *fork_parent_header;
            header.previous_block_hash = fork_parent.hash;
            header.time += chrono::Duration::seconds(1);
            header.nonce.0[0] = 0xd0_u8.saturating_add(u8::try_from(round).unwrap_or(0));
            header.nonce.0[1..5].copy_from_slice(&height.to_be_bytes());
            let header = Arc::new(header);
            fork_parent = Frontier::new(block::Height(height), header.hash());
            fork_parent_header = header.clone();
            fork_headers.push(header);
        }
        let prepared = zakura_header_chain::prepare_headers(
            zakura_header_chain::HeaderBatchInput::new(&fork_headers),
            lease.parent(),
            &rules,
            &SystemClock,
        )
        .map_err(|_| HeaderChainStoreError::Incoherent("benchmark reorg did not prepare"))?;
        let target_height = u64::from(fork_parent.height);
        let owner = zakura_header_chain::HeaderWorkAuthority::for_target(&before, fork_parent.hash)
            .bind(
                target_height,
                NonZeroU64::new(target_height).ok_or(HeaderChainStoreError::Incoherent(
                    "benchmark reorg target height is zero",
                ))?,
            )
            .into();
        let request = TransitionRequest {
            expected_version: before.state_version,
            event: TransitionEvent::InsertHeaders(Box::new(zakura_header_chain::InsertHeaders {
                owner,
                source: SourceId::from_digest([0xd1_u8.saturating_add(round as u8); 32]),
                parent_hash: base.hash,
                target_tip_hash: fork_parent.hash,
                completion: zakura_header_chain::TargetCompletion::TargetComplete {
                    common_ancestor: base,
                },
                batch: prepared,
                aux: Vec::new(),
            })),
        };
        BENCH_WITNESS_POINT_READS.store(0, std::sync::atomic::Ordering::Relaxed);
        BENCH_WITNESS_ROW_WRITES.store(0, std::sync::atomic::Ordering::Relaxed);
        BENCH_BATCH_BYTES.store(0, std::sync::atomic::Ordering::Relaxed);
        let started = std::time::Instant::now();
        runtime.apply(
            request,
            &TransitionContext {
                config: &config,
                clock: &SystemClock,
                full_state_authority: Some(&authority),
                retention_references: &[],
            },
        )?;
        reorg_samples.push(FinalityWitnessBenchmarkSample {
            advance: u64::try_from(round).unwrap_or(0),
            elapsed: started.elapsed(),
            witness_point_reads: BENCH_WITNESS_POINT_READS
                .load(std::sync::atomic::Ordering::Relaxed),
            witness_row_writes: BENCH_WITNESS_ROW_WRITES.load(std::sync::atomic::Ordering::Relaxed),
            batch_bytes: BENCH_BATCH_BYTES.load(std::sync::atomic::Ordering::Relaxed),
            history_rows: runtime
                .store
                .get_value::<HeaderRowCountDisk>(HEADER_ENGINE_META, FINALITY_HISTORY_COUNT_KEY)?
                .map_or(0, |count| count.0),
            witness_rows: runtime
                .store
                .get_value::<HeaderRowCountDisk>(HEADER_ENGINE_META, FINALITY_WITNESS_COUNT_KEY)?
                .map_or(0, |count| count.0),
        });
        selected_chain.truncate(base_index + 1);
        for (offset, header) in fork_headers.into_iter().enumerate() {
            selected_chain.push((
                Frontier::new(
                    block::Height(
                        u32::from(base.height)
                            .saturating_add(u32::try_from(offset).unwrap_or(u32::MAX) + 1),
                    ),
                    header.hash(),
                ),
                header,
            ));
        }
    }
    drop(runtime);
    let started = std::time::Instant::now();
    HeaderChainStore::new(db).startup(&config)?;
    Ok(FinalityWitnessBenchmarkReport {
        samples,
        one_block_reorg: reorg_samples.remove(0),
        bounded_reorg: reorg_samples.remove(0),
        startup_elapsed: started.elapsed(),
    })
}

fn coherent_engine_aux_deliveries(
    engine: &HeaderChainEngine,
    node: &HeaderNode,
) -> Result<Vec<AuxDelivery>, HeaderChainStoreError> {
    let deliveries = engine.aux_deliveries(node.hash).to_vec();
    let indexed: BTreeSet<_> = node.aux_delivery_ids.iter().copied().collect();
    let stored: BTreeSet<_> = deliveries
        .iter()
        .map(|delivery| delivery.delivery_id)
        .collect();
    if indexed.len() != node.aux_delivery_ids.len()
        || stored.len() != deliveries.len()
        || indexed != stored
    {
        return Err(HeaderChainStoreError::Incoherent(
            "in-memory node and auxiliary delivery index disagree",
        ));
    }
    Ok(deliveries)
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
        match self.metadata_row() {
            Ok(metadata) => Ok(metadata.is_some()),
            // Service construction classifies the durable runtime before the block writer
            // migrates released version-one and version-two rows to the current format.
            Err(HeaderChainStoreError::Codec(HeaderChainValueError::UnsupportedDiskFormat(
                1 | 2,
            ))) => Ok(true),
            Err(error) => Err(error),
        }
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
        let plan = audit_store_for_trust_anchor_update(&self, config)?;
        if let Some(pin) = plan.metadata.alarms.migrated_pin_refuted {
            return Err(HeaderChainStoreError::MigratedPinRefuted { pin });
        }
        let previous = plan.snapshot_before_repair.clone();
        let repairs = plan.repairs.clone();
        if !plan.is_clean() {
            #[cfg(test)]
            fault(FaultPoint::BeforeCommit)?;
            self.db.write(self.recovery_batch(&plan)?)?;
            #[cfg(test)]
            fault(FaultPoint::AfterCommit)?;
        }
        let has_due_deferred = any_deferral_is_due(&plan);
        let transition_engine =
            settle_deferred_before_publication(&self, config, has_due_deferred)?;
        let current = transition_engine.snapshot();
        let transition_engine = Arc::new(Mutex::new(transition_engine));
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
                config: config.clone(),
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
        let source = audit_store_for_trust_anchor_update(&self, &headers_only_config)?;
        if let Some(pin) = source.metadata.alarms.migrated_pin_refuted {
            return Err(HeaderChainStoreError::MigratedPinRefuted { pin });
        }
        if source.metadata.frontiers.finalized != full_state_verified {
            return Err(HeaderChainStoreError::Incoherent(
                "integrated migration requires full-state verification through the preserved pin",
            ));
        }
        let previous = source.snapshot_before_repair.clone();
        let mut repairs = source.repairs.clone();
        if !source.is_clean() {
            self.db.write(self.recovery_batch(&source)?)?;
        }

        let history = self.finality_history()?;
        let mut metadata = self.metadata()?;
        metadata.mode = EngineMode::Integrated;
        metadata.headers_only_migration_epoch = Some(metadata.finality_epoch);
        metadata.state_version = metadata.state_version.checked_next()?;
        metadata.header_generation = metadata.header_generation.checked_next()?;
        metadata.verified_generation = metadata.verified_generation.checked_next()?;
        metadata.last_transition = None;

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
        let has_due_deferred = any_deferral_is_due(&target);
        let transition_engine =
            settle_deferred_before_publication(&self, integrated_config, has_due_deferred)?;
        let current = transition_engine.snapshot();
        let transition_engine = Arc::new(Mutex::new(transition_engine));
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
                config: integrated_config.clone(),
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
        let initial = audit_store_for_trust_anchor_update(&self, config)?;
        if let Some(pin) = initial.metadata.alarms.migrated_pin_refuted {
            return Err(HeaderChainStoreError::MigratedPinRefuted { pin });
        }
        let previous = initial.snapshot_before_repair.clone();
        let mut repairs = initial.repairs.clone();
        if !initial.is_clean() {
            self.db.write(self.recovery_batch(&initial)?)?;
        }

        let max_nodes = config.limits.max_non_finalized_nodes.get();
        if finalized_path.len().saturating_add(restored_path.len()) > max_nodes {
            if restored_path.len() > max_nodes {
                return Err(HeaderChainStoreError::Incoherent(
                    "restored verified path exceeds the non-finalized node limit",
                ));
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
        let has_due_deferred = any_deferral_is_due(&final_audit);
        let transition_engine =
            settle_deferred_before_publication(&self, config, has_due_deferred)?;
        let current = transition_engine.snapshot();
        let transition_engine = Arc::new(Mutex::new(transition_engine));
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
                config: config.clone(),
                publisher,
                leases: Arc::new(Mutex::new(RetainedPathLeaseRegistry::default())),
                transition_engine,
            },
            report,
        ))
    }

    /// Resume bounded canonical reconstruction without materializing finalized history.
    pub(in crate::service) fn startup_reconciled_streaming<F, P>(
        self,
        config: &EngineConfig,
        full_state_finalized: Frontier,
        restored_path: Vec<VerifiedHeaderRef>,
        mut canonical_header: F,
        mut report_progress: P,
    ) -> Result<(HeaderChainRuntime, StartupReport), HeaderChainStoreError>
    where
        F: FnMut(block::Height) -> Result<VerifiedHeaderRef, HeaderChainStoreError>,
        P: FnMut(zakura_node_services::sync_lifecycle::HeaderReconstructionProgress),
    {
        use zakura_node_services::sync_lifecycle::{
            HeaderReconstructionProgress, HeaderReconstructionStage,
        };

        let writer = self
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        let initial = audit_store_for_trust_anchor_update(&self, config)?;
        if let Some(pin) = initial.metadata.alarms.migrated_pin_refuted {
            return Err(HeaderChainStoreError::MigratedPinRefuted { pin });
        }
        let previous = initial.snapshot_before_repair.clone();
        let mut repairs = initial.repairs.clone();
        if !initial.is_clean() {
            self.db.write(self.recovery_batch(&initial)?)?;
        }

        let snapshot = self.snapshot()?;
        if snapshot.frontiers.finalized.height > full_state_finalized.height {
            return Err(HeaderChainStoreError::Incoherent(
                "header reconstruction target is below durable finality",
            ));
        }
        let base = snapshot.frontiers.finalized;
        let network = config.network().kind();
        let mut progress = match self.reconstruction_progress()? {
            Some(mut progress) => {
                if progress.network != network
                    || progress.last_committed != snapshot.frontiers.finalized
                    || progress.target.height > full_state_finalized.height
                    || progress.last_committed.height > progress.target.height
                    || (progress.target.height == full_state_finalized.height
                        && progress.target.hash != full_state_finalized.hash)
                {
                    return Err(HeaderChainStoreError::Incoherent(
                        "invalid durable header reconstruction progress",
                    ));
                }
                let expected_next = progress
                    .last_committed
                    .height
                    .next()
                    .unwrap_or(progress.last_committed.height);
                if progress.next_height != expected_next {
                    return Err(HeaderChainStoreError::Incoherent(
                        "header reconstruction progress has a discontinuous next height",
                    ));
                }
                if progress.target.height < full_state_finalized.height {
                    progress.phase = HeaderReconstructionPhaseDisk::FinalizedPath;
                } else {
                    match progress.phase {
                        HeaderReconstructionPhaseDisk::FinalizedPath => {}
                        HeaderReconstructionPhaseDisk::RestoredPath
                        | HeaderReconstructionPhaseDisk::FinalAudit
                            if progress.last_committed == progress.target => {}
                        HeaderReconstructionPhaseDisk::RestoredPath
                        | HeaderReconstructionPhaseDisk::FinalAudit => {
                            return Err(HeaderChainStoreError::Incoherent(
                                "terminal header reconstruction phase precedes its target",
                            ));
                        }
                    }
                }
                progress.target = full_state_finalized;
                progress
            }
            None => HeaderReconstructionProgressDisk {
                network,
                target: full_state_finalized,
                next_height: snapshot
                    .frontiers
                    .finalized
                    .height
                    .next()
                    .unwrap_or(snapshot.frontiers.finalized.height),
                phase: HeaderReconstructionPhaseDisk::FinalizedPath,
                last_committed: snapshot.frontiers.finalized,
            },
        };
        self.write_reconstruction_progress(&progress)?;

        let finalized_total =
            u64::from(full_state_finalized.height.0.saturating_sub(base.height.0));
        let restored_total = u64::try_from(restored_path.len()).unwrap_or(u64::MAX);
        let total = finalized_total.saturating_add(restored_total);
        report_progress(HeaderReconstructionProgress {
            stage: HeaderReconstructionStage::FullStateReconciliation,
            completed: 0,
            total: Some(total),
            target: Some(full_state_finalized),
            last_committed: Some(progress.last_committed),
        });

        if progress.phase == HeaderReconstructionPhaseDisk::FinalizedPath {
            let max_nodes = config.limits.max_non_finalized_nodes.get();
            while progress.last_committed.height < full_state_finalized.height {
                let remaining = full_state_finalized
                    .height
                    .0
                    .saturating_sub(progress.last_committed.height.0);
                let chunk_len = usize::try_from(remaining)
                    .unwrap_or(usize::MAX)
                    .min(max_nodes);
                let mut chunk = Vec::with_capacity(chunk_len);
                let mut expected_parent = progress.last_committed.hash;
                for offset in 0..chunk_len {
                    let offset = u32::try_from(offset).map_err(|_| {
                        HeaderChainStoreError::Incoherent("reconstruction chunk is too large")
                    })?;
                    let height = block::Height(
                        progress
                            .last_committed
                            .height
                            .0
                            .checked_add(offset.saturating_add(1))
                            .ok_or(HeaderChainStoreError::Incoherent(
                                "header reconstruction height overflow",
                            ))?,
                    );
                    let header = canonical_header(height)?;
                    if header.height != height
                        || header.header.previous_block_hash != expected_parent
                    {
                        return Err(HeaderChainStoreError::Incoherent(
                            "canonical reconstruction chunk is discontinuous",
                        ));
                    }
                    expected_parent = header.hash;
                    chunk.push(header);
                }
                let chunk_tip = chunk
                    .last()
                    .map(|header| Frontier::new(header.height, header.hash))
                    .ok_or(HeaderChainStoreError::Incoherent(
                        "header reconstruction produced an empty chunk",
                    ))?;
                self.reconcile_verified_path(config, chunk)?;
                progress.last_committed = chunk_tip;
                progress.next_height = chunk_tip.height.next().unwrap_or(chunk_tip.height);
                self.reconcile_finalized_with_progress(config, chunk_tip, Some(&progress))?;
                report_progress(HeaderReconstructionProgress {
                    stage: HeaderReconstructionStage::FullStateReconciliation,
                    completed: u64::from(chunk_tip.height.0.saturating_sub(base.height.0)),
                    total: Some(total),
                    target: Some(full_state_finalized),
                    last_committed: Some(chunk_tip),
                });
            }
        }

        progress.phase = HeaderReconstructionPhaseDisk::RestoredPath;
        self.write_reconstruction_progress(&progress)?;
        self.reconcile_verified_path(config, restored_path)?;
        progress.phase = HeaderReconstructionPhaseDisk::FinalAudit;
        self.write_reconstruction_progress(&progress)?;
        report_progress(HeaderReconstructionProgress {
            stage: HeaderReconstructionStage::FullStateReconciliation,
            completed: total,
            total: Some(total),
            target: Some(full_state_finalized),
            last_committed: Some(progress.last_committed),
        });

        let final_audit = audit_store(&self, config)?;
        if progress.last_committed != full_state_finalized
            || final_audit.metadata.frontiers.finalized != full_state_finalized
        {
            return Err(HeaderChainStoreError::Incoherent(
                "header reconstruction did not reach its full-state target",
            ));
        }
        repairs.extend(final_audit.repairs.iter().copied());
        if !final_audit.is_clean() {
            self.db.write(self.recovery_batch(&final_audit)?)?;
        }
        self.clear_reconstruction_progress()?;
        let has_due_deferred = any_deferral_is_due(&final_audit);
        let transition_engine =
            settle_deferred_before_publication(&self, config, has_due_deferred)?;
        let current = transition_engine.snapshot();
        let transition_engine = Arc::new(Mutex::new(transition_engine));
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
                config: config.clone(),
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
        struct Authority {
            event: zakura_header_chain::TransitionFingerprint,
            validation_context: [u8; 32],
        }

        impl FullStateEvidenceAuthority for Authority {
            fn authorizes_full_state(&self, event: &TransitionEvent) -> bool {
                event.fingerprint() == Some(self.event)
            }

            fn authorizes_validation_lease(&self, lease: &ValidationLease) -> bool {
                lease.context_digest() == self.validation_context
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
        let event = TransitionEvent::VerifiedChainChanged(VerifiedChainChanged {
            full_state_transition_id: evidence,
            old_tip: snapshot.frontiers.verified_best,
            new_path: authoritative_path,
            cause: VerifiedChangeCause::Reset,
        });
        let validation_context =
            self.validation_context(snapshot.frontiers.finalized.hash, config.network())?;
        let authority = Authority {
            event: event
                .fingerprint()
                .expect("startup reconciliation carries stable evidence"),
            validation_context: validation_context.context_digest(),
        };
        let context = TransitionContext {
            config,
            clock: &SystemClock,
            full_state_authority: Some(&authority),
            retention_references: &[],
        };
        let engine = load_transition_engine(self)?;
        let TransitionEvent::VerifiedChainChanged(event) = event else {
            unreachable!("startup reconciliation constructs VerifiedChainChanged");
        };
        let transition = engine.plan_transition(
            TransitionInput::VerifiedChainChanged {
                expected_version: snapshot.state_version,
                event,
                facts: HeaderValidationFacts {
                    validation_leases: vec![validation_context],
                },
            },
            &context,
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
        self.reconcile_finalized_with_progress(config, full_state_finalized, None)
    }

    fn reconcile_finalized_with_progress(
        &self,
        config: &EngineConfig,
        full_state_finalized: Frontier,
        progress: Option<&HeaderReconstructionProgressDisk>,
    ) -> Result<(), HeaderChainStoreError> {
        struct Authority(zakura_header_chain::TransitionFingerprint);

        impl FullStateEvidenceAuthority for Authority {
            fn authorizes_full_state(&self, event: &TransitionEvent) -> bool {
                event.fingerprint() == Some(self.0)
            }
        }

        let snapshot = self.snapshot()?;
        if snapshot.frontiers.finalized == full_state_finalized {
            if let Some(progress) = progress {
                self.write_reconstruction_progress(progress)?;
            }
            return Ok(());
        }
        let proof = self
            .verified_projection()?
            .into_iter()
            .take_while(|frontier| frontier.height <= full_state_finalized.height)
            .map(|frontier| frontier.hash)
            .collect::<Vec<_>>();
        let evidence = zakura_header_chain::full_state_finality_evidence(
            snapshot.state_version,
            full_state_finalized,
            &proof,
        );
        let event = TransitionEvent::FullStateFinalized(FullStateFinalized {
            full_state_transition_id: evidence,
            new_finalized: full_state_finalized,
            verified_path_proof: proof,
        });
        let authority = Authority(
            event
                .fingerprint()
                .expect("startup finalization carries stable evidence"),
        );
        let context = TransitionContext {
            config,
            clock: &SystemClock,
            full_state_authority: Some(&authority),
            retention_references: &[],
        };
        let engine = load_transition_engine(self)?;
        let TransitionEvent::FullStateFinalized(event) = event else {
            unreachable!("startup finalization constructs FullStateFinalized");
        };
        let transition = engine.plan_transition(
            TransitionInput::FullStateFinalized {
                expected_version: snapshot.state_version,
                event,
            },
            &context,
        )?;
        if !transition.is_no_change() {
            let mut batch = DiskWriteBatch::new();
            if let Some(progress) = progress {
                self.put_value(
                    &mut batch,
                    HEADER_ENGINE_META,
                    RECONSTRUCTION_PROGRESS_KEY,
                    progress,
                )?;
            }
            self.db
                .write(self.batch_for_combined(transition.change_set(), batch)?)?;
        } else if let Some(progress) = progress {
            self.write_reconstruction_progress(progress)?;
        }
        Ok(())
    }

    fn reconstruction_progress(
        &self,
    ) -> Result<Option<HeaderReconstructionProgressDisk>, HeaderChainStoreError> {
        self.get_value(HEADER_ENGINE_META, RECONSTRUCTION_PROGRESS_KEY)
    }

    fn write_reconstruction_progress(
        &self,
        progress: &HeaderReconstructionProgressDisk,
    ) -> Result<(), HeaderChainStoreError> {
        let mut batch = DiskWriteBatch::new();
        self.put_value(
            &mut batch,
            HEADER_ENGINE_META,
            RECONSTRUCTION_PROGRESS_KEY,
            progress,
        )?;
        self.db.write(batch)?;
        Ok(())
    }

    fn clear_reconstruction_progress(&self) -> Result<(), HeaderChainStoreError> {
        let mut batch = DiskWriteBatch::new();
        self.delete_raw(&mut batch, HEADER_ENGINE_META, RECONSTRUCTION_PROGRESS_KEY)?;
        self.db.write(batch)?;
        Ok(())
    }

    /// Bootstrap an empty header schema with one already-authenticated anchor.
    ///
    /// Migration calls this only before enabling publication and normal writers.
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
            put_consensus_invalid_body_tombstones: Vec::new(),
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
            finality_append: Some(match metadata.mode {
                EngineMode::Integrated => FinalityRecord::full_state_with_provenance(
                    metadata.work_origin,
                    metadata.frontiers.finalized,
                    metadata.finality_epoch,
                    FullStateFinalityProvenance::initialization(
                        metadata.state_version,
                        metadata.frontiers.finalized,
                    ),
                ),
                EngineMode::HeadersOnly => FinalityRecord {
                    previous: metadata.work_origin,
                    current: metadata.frontiers.finalized,
                    source: FinalitySource::MigratedHeadersOnly,
                    epoch: metadata.finality_epoch,
                },
            }),
            finality_ancestry: zakura_header_chain::FinalityWitnessProof::default(),
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
        let current_metadata = self.metadata_row()?;
        if let Some(metadata) = current_metadata
            .as_ref()
            .filter(|metadata| metadata.frontiers.finalized != changes.metadata.frontiers.finalized)
        {
            let staged_nodes: HashMap<_, _> = changes
                .put_nodes
                .iter()
                .map(|node| (node.hash, node))
                .collect();
            if let Some((context, outgoing)) = self.incremental_context_slide(
                metadata.frontiers.finalized,
                changes.metadata.frontiers.finalized,
                &staged_nodes,
            )? {
                self.put_value(
                    &mut batch,
                    HEADER_VALIDATION_CONTEXT,
                    context.header.hash().0,
                    &context,
                )?;
                if let Some(hash) = outgoing {
                    self.delete_raw(&mut batch, HEADER_VALIDATION_CONTEXT, hash.0)?;
                }
            } else {
                let old_contexts: Vec<_> =
                    authenticated_context_headers(self, metadata.frontiers.finalized.hash, None)?
                        .into_iter()
                        .map(|context| (context.header.hash(), context))
                        .collect();
                let contexts: Vec<_> = authenticated_context_headers(
                    self,
                    changes.metadata.frontiers.finalized.hash,
                    Some(&staged_nodes),
                )?
                .into_iter()
                .map(|context| (context.header.hash(), context))
                .collect();
                let old_hashes: HashSet<_> = old_contexts.iter().map(|(hash, _)| *hash).collect();
                let new_hashes: HashSet<_> = contexts.iter().map(|(hash, _)| *hash).collect();
                for (hash, _) in old_contexts {
                    if !new_hashes.contains(&hash) {
                        self.delete_raw(&mut batch, HEADER_VALIDATION_CONTEXT, hash.0)?;
                    }
                }
                for (hash, context) in contexts {
                    if !old_hashes.contains(&hash) {
                        self.put_value(&mut batch, HEADER_VALIDATION_CONTEXT, hash.0, &context)?;
                    }
                }
            }
        }

        for hash in &changes.delete_nodes {
            if let Some(node) = self.header_node(*hash).map_err(|_| {
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
                self.delete_reason_rows(&mut batch, &node)?;
                if !matches!(
                    node.body_validation_state,
                    zakura_header_chain::BodyValidationState::ConsensusInvalid { .. }
                ) {
                    self.delete_raw(&mut batch, HEADER_BODY_EVIDENCE_AUTHORITY, hash.0)?;
                }
            }
            for (key, _) in self.scan_prefix(HEADER_CHILD, &hash.0)? {
                self.delete_raw(&mut batch, HEADER_CHILD, key)?;
            }
        }

        for node in &changes.put_nodes {
            if let Some(old) = self.header_node(node.hash).map_err(|_| {
                HeaderChainStoreError::Incoherent("replaced node could not be decoded")
            })? {
                self.delete_deferred_for(&mut batch, &old)?;
                self.delete_reason_rows(&mut batch, &old)?;
            }
            self.put_value(
                &mut batch,
                HEADER_NODE_BY_HASH,
                node.hash.0,
                &HeaderNodeDisk::from_domain(node),
            )?;
            if let Some(authority) =
                FullStateBodyValidationEvidenceAuthorityDisk::from_body_validation_state(
                    node.hash,
                    node.height,
                    &node.body_validation_state,
                )
            {
                self.put_value(
                    &mut batch,
                    HEADER_BODY_EVIDENCE_AUTHORITY,
                    node.hash.0,
                    &authority,
                )?;
            } else {
                self.delete_raw(&mut batch, HEADER_BODY_EVIDENCE_AUTHORITY, node.hash.0)?;
            }
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
            for reason in &node.eligibility.direct_reasons {
                self.put_reason(&mut batch, node.hash, reason)?;
            }
        }

        let finality_advanced = current_metadata.as_ref().is_some_and(|metadata| {
            metadata.frontiers.finalized != changes.metadata.frontiers.finalized
        });
        if current_metadata.is_none()
            || finality_advanced
            || !changes.put_consensus_invalid_body_tombstones.is_empty()
        {
            self.apply_consensus_invalid_tombstones(
                &mut batch,
                changes,
                current_metadata.is_some(),
                finality_advanced,
            )?;
        }

        let selected_bounds = current_metadata.as_ref().map(|metadata| {
            (
                metadata.frontiers.finalized.height,
                metadata.frontiers.header_best.height,
            )
        });
        let verified_bounds = current_metadata.as_ref().map(|metadata| {
            (
                metadata.frontiers.finalized.height,
                metadata.frontiers.verified_best.height,
            )
        });
        self.apply_projection(
            &mut batch,
            HEADER_SELECTED,
            &changes.selected_projection,
            selected_bounds,
        )?;
        self.apply_projection(
            &mut batch,
            HEADER_VERIFIED,
            &changes.verified_projection,
            verified_bounds,
        )?;

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
            let record_key = HeaderFinalityKey(record.epoch);
            let existing =
                self.get_value::<FinalityRecord>(HEADER_FINALITY_HISTORY, record_key.as_bytes())?;
            if existing.is_some_and(|existing| existing != record) {
                return Err(HeaderChainStoreError::Incoherent(
                    "finality epoch changed its immutable record",
                ));
            }
            if existing.is_none() {
                let count = self
                    .get_value::<HeaderRowCountDisk>(
                        HEADER_ENGINE_META,
                        FINALITY_HISTORY_COUNT_KEY,
                    )?
                    .map_or(0, |count| count.0);
                let limit = u64::try_from(FINALITY_HISTORY_LIMIT).map_err(|_| {
                    HeaderChainStoreError::Incoherent("finality history limit does not fit u64")
                })?;
                if count > limit {
                    return Err(StoreError::LimitExceeded {
                        collection: StoreCollection::FinalityHistory,
                        limit: RowLimit::new(FINALITY_HISTORY_LIMIT),
                    }
                    .into());
                }
                let estimated_new_witnesses = match record.source {
                    FinalitySource::HeadersOnlyDepth { .. } => {
                        let history_cf = self.cf(HEADER_FINALITY_HISTORY)?;
                        let newest_root = self
                            .db
                            .raw_last_cf(&history_cf)?
                            .and_then(|(_, value)| FinalityRecord::decode(&value).ok())
                            .and_then(|newest| match newest.source {
                                FinalitySource::HeadersOnlyDepth { selected_tip }
                                | FinalitySource::DiskMigration {
                                    authentication:
                                        zakura_header_chain::DiskMigrationAuthentication::HeadersOnlyDepth {
                                            selected_tip,
                                        },
                                    ..
                                } => Some(selected_tip),
                                _ => None,
                            });
                        let first_new = newest_root
                            .and_then(|root| {
                                changes
                                    .finality_ancestry
                                    .iter()
                                    .position(|entry| entry.frontier == root)
                                    .map(|index| index + 1)
                            })
                            .unwrap_or(0);
                        changes.finality_ancestry[first_new..]
                            .iter()
                            .filter(|entry| entry.frontier.height > record.current.height)
                            .count()
                    }
                    _ => 0,
                };
                let witness_count = self
                    .get_value::<HeaderRowCountDisk>(
                        HEADER_ENGINE_META,
                        FINALITY_WITNESS_COUNT_KEY,
                    )?
                    .map_or(0, |count| count.0);
                let witness_limit = u64::try_from(FINALITY_WITNESS_LIMIT).map_err(|_| {
                    HeaderChainStoreError::Incoherent("finality witness limit does not fit u64")
                })?;
                let witness_excess = witness_count
                    .saturating_add(u64::try_from(estimated_new_witnesses).map_err(|_| {
                        HeaderChainStoreError::Incoherent(
                            "estimated witness count does not fit u64",
                        )
                    })?)
                    .saturating_sub(witness_limit);
                let eviction_count = usize::try_from(u64::from(count == limit).max(witness_excess))
                    .map_err(|_| {
                        HeaderChainStoreError::Incoherent(
                            "finality eviction count does not fit usize",
                        )
                    })?;
                if u64::try_from(eviction_count).map_or(true, |evictions| evictions > count) {
                    return Err(HeaderChainStoreError::Incoherent(
                        "witness budget cannot retain a non-empty finality history",
                    ));
                }
                let history_cf = self.cf(HEADER_FINALITY_HISTORY)?;
                // The published checkpoint names the last epoch a previous eviction removed, so
                // every epoch at or below it is already deleted and the oldest surviving record
                // sits above it. Seeking from there keeps eviction independent of how many
                // evictions preceded it. Starting at the beginning of the column family instead
                // would step over the tombstone left by every previous eviction: the retained
                // window is a few MB, far too small to trigger the compaction that collects
                // them, so that cost grows without bound and eventually dominates every block
                // commit.
                let retained_low = self
                    .get_value::<FinalityHistoryCheckpoint>(
                        HEADER_ENGINE_META,
                        FINALITY_HISTORY_CHECKPOINT_KEY,
                    )?
                    .map_or(0, |checkpoint| checkpoint.epoch.get().saturating_add(1));
                let prefix = self.db.raw_prefix_cf_from(
                    &history_cf,
                    &retained_low.to_be_bytes(),
                    eviction_count.saturating_add(1),
                )?;
                let mut decoded_prefix = Vec::with_capacity(prefix.len());
                for (key, value) in prefix {
                    let row = FinalityRecord::decode(&value).map_err(|_| {
                        HeaderChainStoreError::Incoherent("invalid oldest finality record")
                    })?;
                    if key != row.epoch.get().to_be_bytes() {
                        return Err(HeaderChainStoreError::Incoherent(
                            "oldest finality key/value mismatch",
                        ));
                    }
                    decoded_prefix.push((key, row));
                }
                let mut evicted_records = Vec::with_capacity(eviction_count);
                for (key, evicted) in decoded_prefix.iter().take(eviction_count) {
                    if self.authenticated_canonical_hash(evicted.current.height)?
                        != Some(evicted.current.hash)
                    {
                        return Err(HeaderChainStoreError::Incoherent(
                            "finality checkpoint lacks canonical authentication",
                        ));
                    }
                    self.delete_raw(&mut batch, HEADER_FINALITY_HISTORY, key)?;
                    evicted_records.push(*evicted);
                }
                let prune_frontiers: Vec<_> = (0..eviction_count)
                    .map(|index| {
                        decoded_prefix
                            .get(index + 1)
                            .map_or(record.current, |(_, next)| next.current)
                    })
                    .collect();
                if let Some(last) = evicted_records.last() {
                    self.put_value(
                        &mut batch,
                        HEADER_ENGINE_META,
                        FINALITY_HISTORY_CHECKPOINT_KEY,
                        &FinalityHistoryCheckpoint {
                            epoch: last.epoch,
                            frontier: last.current,
                        },
                    )?;
                }
                let next_count = count
                    .checked_sub(u64::try_from(eviction_count).map_err(|_| {
                        HeaderChainStoreError::Incoherent(
                            "finality eviction count does not fit u64",
                        )
                    })?)
                    .and_then(|count| count.checked_add(1))
                    .ok_or(HeaderChainStoreError::Incoherent(
                        "finality history count overflow",
                    ))?;
                self.apply_finality_evidence(
                    &mut batch,
                    record,
                    &changes.finality_ancestry,
                    &evicted_records,
                    &prune_frontiers,
                )?;
                self.put_value(
                    &mut batch,
                    HEADER_FINALITY_HISTORY,
                    record_key.as_bytes(),
                    &record,
                )?;
                self.put_value(
                    &mut batch,
                    HEADER_ENGINE_META,
                    FINALITY_HISTORY_COUNT_KEY,
                    &HeaderRowCountDisk(next_count),
                )?;
            }
        }

        // The adapter enqueues the singleton logical root last in the atomic batch.
        self.put_value(
            &mut batch,
            HEADER_ENGINE_META,
            METADATA_KEY,
            &changes.metadata,
        )?;
        Ok(batch)
    }

    fn apply_finality_evidence(
        &self,
        batch: &mut DiskWriteBatch,
        record: FinalityRecord,
        ancestry: &[zakura_header_chain::FinalityAncestryHeader],
        evicted: &[FinalityRecord],
        prune_frontiers: &[Frontier],
    ) -> Result<(), HeaderChainStoreError> {
        let mut witnesses = WitnessMutations::new(self)?;

        match record.source {
            FinalitySource::HeadersOnlyDepth { selected_tip }
            | FinalitySource::DiskMigration {
                authentication:
                    zakura_header_chain::DiskMigrationAuthentication::HeadersOnlyDepth { selected_tip },
                ..
            } => {
                let expected_len = usize::try_from(
                    selected_tip
                        .height
                        .0
                        .checked_sub(record.previous.height.0)
                        .ok_or(HeaderChainStoreError::Incoherent(
                            "headers-only finality ancestry retreats",
                        ))?,
                )
                .map_err(|_| {
                    HeaderChainStoreError::Incoherent(
                        "headers-only finality ancestry length does not fit usize",
                    )
                })?;
                if ancestry.len() != expected_len {
                    return Err(HeaderChainStoreError::Incoherent(
                        "headers-only finality ancestry has the wrong length",
                    ));
                }
                let mut parent = record.previous.hash;
                let mut height = record.previous.height.next().map_err(|_| {
                    HeaderChainStoreError::Incoherent(
                        "headers-only finality ancestry height overflow",
                    )
                })?;
                let mut last = record.previous;
                let mut ancestry_frontiers = Vec::with_capacity(ancestry.len());
                for (index, entry) in ancestry.iter().enumerate() {
                    let hash = entry.frontier.hash;
                    if entry.frontier.height != height || entry.header.previous_block_hash != parent
                    {
                        return Err(HeaderChainStoreError::Incoherent(
                            "headers-only finality ancestry is discontinuous",
                        ));
                    }
                    let frontier = entry.frontier;
                    ancestry_frontiers.push(frontier);
                    if entry.frontier.height <= record.current.height {
                        if self
                            .authenticated_canonical_hash(entry.frontier.height)?
                            .is_some_and(|stored| stored != hash)
                        {
                            return Err(HeaderChainStoreError::Incoherent(
                                "headers-only finalized ancestry changed its canonical hash",
                            ));
                        }
                        self.put_raw(
                            batch,
                            ZAKURA_HEADER_HASH_BY_HEIGHT,
                            entry.frontier.height.as_bytes(),
                            hash.0,
                        )?;
                    }
                    parent = hash;
                    last = frontier;
                    if index + 1 < ancestry.len() {
                        height = height.next().map_err(|_| {
                            HeaderChainStoreError::Incoherent(
                                "headers-only finality ancestry height overflow",
                            )
                        })?;
                    }
                }
                if last != selected_tip
                    || ancestry
                        .iter()
                        .find(|entry| entry.frontier.height == record.current.height)
                        .is_none_or(|entry| entry.frontier.hash != record.current.hash)
                {
                    return Err(HeaderChainStoreError::Incoherent(
                        "headers-only finality ancestry does not bind its record",
                    ));
                }

                let history_cf = self.cf(HEADER_FINALITY_HISTORY)?;
                let newest = self
                    .db
                    .raw_last_cf(&history_cf)?
                    .map(|(key, value)| {
                        let newest = FinalityRecord::decode(&value).map_err(|_| {
                            HeaderChainStoreError::Incoherent("invalid newest finality record")
                        })?;
                        if key != newest.epoch.get().to_be_bytes() {
                            return Err(HeaderChainStoreError::Incoherent(
                                "newest finality key/value mismatch",
                            ));
                        }
                        Ok(newest)
                    })
                    .transpose()?;
                let old_root = newest.and_then(|newest| match newest.source {
                    FinalitySource::HeadersOnlyDepth { selected_tip }
                    | FinalitySource::DiskMigration {
                        authentication:
                            zakura_header_chain::DiskMigrationAuthentication::HeadersOnlyDepth {
                                selected_tip,
                            },
                        ..
                    } => Some(selected_tip),
                    FinalitySource::FullState { .. }
                    | FinalitySource::MigratedHeadersOnly
                    | FinalitySource::DiskMigration {
                        authentication: zakura_header_chain::DiskMigrationAuthentication::FullState,
                        ..
                    } => None,
                });
                let common_index = if let Some(old_root) = old_root {
                    if let Some(index) = ancestry_frontiers
                        .iter()
                        .position(|frontier| *frontier == old_root)
                    {
                        Some(index)
                    } else {
                        let mut cursor = old_root;
                        let mut common = None;
                        while cursor.height > record.previous.height {
                            if let Some(index) = ancestry_frontiers
                                .iter()
                                .position(|frontier| *frontier == cursor)
                            {
                                common = Some(index);
                                break;
                            }
                            cursor = witnesses.parent(cursor)?;
                        }
                        if common.is_none() && cursor != record.previous {
                            return Err(HeaderChainStoreError::Incoherent(
                                "historical finality witness does not reach the previous frontier",
                            ));
                        }
                        common
                    }
                } else {
                    None
                };

                for (index, evicted) in evicted.iter().enumerate() {
                    if let FinalitySource::HeadersOnlyDepth { selected_tip }
                    | FinalitySource::DiskMigration {
                        authentication:
                            zakura_header_chain::DiskMigrationAuthentication::HeadersOnlyDepth {
                                selected_tip,
                            },
                        ..
                    } = evicted.source
                    {
                        let root = witnesses.row(selected_tip)?.as_mut().ok_or(
                            HeaderChainStoreError::Incoherent(
                                "evicted finality record lacks its witness root",
                            ),
                        )?;
                        root.root_references = root.root_references.checked_sub(1).ok_or(
                            HeaderChainStoreError::Incoherent(
                                "finality witness root reference underflow",
                            ),
                        )?;
                        witnesses.collect_unreferenced_branch(selected_tip, evicted.current)?;
                    }
                    let prune_frontier = prune_frontiers.get(index).copied().ok_or(
                        HeaderChainStoreError::Incoherent(
                            "finality witness pruning frontier is absent",
                        ),
                    )?;
                    if let Some(row) = witnesses.row(prune_frontier)?.as_ref() {
                        if row.root_references == 0 {
                            *witnesses.row(prune_frontier)? = None;
                            witnesses.count = witnesses.count.checked_sub(1).ok_or(
                                HeaderChainStoreError::Incoherent(
                                    "finality witness count underflow",
                                ),
                            )?;
                        }
                    }
                }

                for (index, (entry, frontier)) in ancestry
                    .iter()
                    .zip(ancestry_frontiers.iter().copied())
                    .enumerate()
                {
                    if entry.frontier.height <= record.current.height
                        || common_index.is_some_and(|common| index <= common)
                    {
                        continue;
                    }
                    let inserted = witnesses.insert(frontier, entry.header.clone())?;
                    if inserted {
                        let parent = Frontier::new(
                            block::Height(entry.frontier.height.0.checked_sub(1).ok_or(
                                HeaderChainStoreError::Incoherent(
                                    "finality witness height underflow",
                                ),
                            )?),
                            entry.header.previous_block_hash,
                        );
                        if parent.height > record.current.height {
                            let parent = witnesses.row(parent)?.as_mut().ok_or(
                                HeaderChainStoreError::Incoherent(
                                    "new finality witness edge lacks its parent",
                                ),
                            )?;
                            parent.child_references = parent
                                .child_references
                                .checked_add(1)
                                .ok_or(HeaderChainStoreError::Incoherent(
                                    "finality witness child reference overflow",
                                ))?;
                        }
                    }
                }
                let root = witnesses.row(selected_tip)?.as_mut().ok_or(
                    HeaderChainStoreError::Incoherent(
                        "headers-only finality proof lacks its selected-tip root",
                    ),
                )?;
                root.root_references = root.root_references.checked_add(1).ok_or(
                    HeaderChainStoreError::Incoherent("finality witness root reference overflow"),
                )?;
            }
            FinalitySource::MigratedHeadersOnly => {
                if !ancestry.is_empty() {
                    return Err(HeaderChainStoreError::Incoherent(
                        "migrated finality unexpectedly carries ancestry",
                    ));
                }
                self.put_raw(
                    batch,
                    ZAKURA_HEADER_HASH_BY_HEIGHT,
                    record.current.height.as_bytes(),
                    record.current.hash.0,
                )?;
            }
            FinalitySource::FullState { .. }
            | FinalitySource::DiskMigration {
                authentication: zakura_header_chain::DiskMigrationAuthentication::FullState,
                ..
            } => {
                if !ancestry.is_empty() {
                    return Err(HeaderChainStoreError::Incoherent(
                        "full-state finality unexpectedly carries headers-only ancestry",
                    ));
                }
            }
        }
        witnesses.finish(batch)
    }

    /// Apply the bounded tombstone lifecycle through the same atomic batch as its source change.
    ///
    /// Ordinary additions use one count read and one point read per distinct hash. Finality and
    /// actual limit pressure may scan the bounded tombstone table because both paths reduce or
    /// rebalance retained resources.
    fn apply_consensus_invalid_tombstones(
        &self,
        batch: &mut DiskWriteBatch,
        changes: &ChangeSet,
        initialized: bool,
        finality_advanced: bool,
    ) -> Result<(), HeaderChainStoreError> {
        let count =
            match self.get_value::<HeaderRowCountDisk>(HEADER_ENGINE_META, TOMBSTONE_COUNT_KEY)? {
                Some(count) => usize::try_from(count.0).map_err(|_| {
                    HeaderChainStoreError::Incoherent("tombstone count does not fit usize")
                })?,
                None if !initialized => 0,
                None => {
                    return Err(HeaderChainStoreError::Incoherent(
                        "consensus-invalid tombstone count is absent",
                    ));
                }
            };
        if count > TOMBSTONE_LIMIT {
            return Err(StoreError::LimitExceeded {
                collection: StoreCollection::ConsensusInvalidBodyTombstones,
                limit: RowLimit::new(TOMBSTONE_LIMIT),
            }
            .into());
        }

        let finalized_height = changes.metadata.frontiers.finalized.height;
        let mut staged = HashMap::new();
        for tombstone in &changes.put_consensus_invalid_body_tombstones {
            if tombstone.height <= finalized_height {
                continue;
            }
            if let Some(previous) = staged.insert(tombstone.hash, tombstone.clone()) {
                if previous != *tombstone {
                    return Err(HeaderChainStoreError::Incoherent(
                        "consensus-invalid tombstone changed in one transition",
                    ));
                }
            }
        }

        let mut additions = Vec::new();
        for tombstone in staged.into_values() {
            match self.get_value::<zakura_header_chain::ConsensusInvalidBodyTombstone>(
                HEADER_CONSENSUS_INVALID_BODY_TOMBSTONE,
                tombstone.hash.0,
            )? {
                Some(existing) if existing != tombstone => {
                    return Err(HeaderChainStoreError::Incoherent(
                        "consensus-invalid tombstone changed",
                    ));
                }
                Some(_) => {}
                None => additions.push(tombstone),
            }
        }

        let prospective_count =
            count
                .checked_add(additions.len())
                .ok_or(HeaderChainStoreError::Incoherent(
                    "consensus-invalid tombstone count overflow",
                ))?;
        if !finality_advanced && prospective_count <= TOMBSTONE_LIMIT {
            for tombstone in &additions {
                self.put_value(
                    batch,
                    HEADER_CONSENSUS_INVALID_BODY_TOMBSTONE,
                    tombstone.hash.0,
                    tombstone,
                )?;
            }
            self.put_value(
                batch,
                HEADER_ENGINE_META,
                TOMBSTONE_COUNT_KEY,
                &HeaderRowCountDisk(u64::try_from(prospective_count).map_err(|_| {
                    HeaderChainStoreError::Incoherent("tombstone count does not fit u64")
                })?),
            )?;
            return Ok(());
        }

        let audit = self.audit_snapshot()?;
        let mut tombstones = HashMap::with_capacity(prospective_count.min(TOMBSTONE_LIMIT));
        audit.visit_consensus_invalid_body_tombstones(
            RowLimit::new(TOMBSTONE_LIMIT),
            &mut |tombstone| {
                tombstones.insert(tombstone.hash, tombstone);
                Ok(())
            },
        )?;
        if tombstones.len() != count {
            return Err(HeaderChainStoreError::Incoherent(
                "consensus-invalid tombstone count mismatch",
            ));
        }

        let finalized_hashes: Vec<_> = tombstones
            .values()
            .filter(|tombstone| tombstone.height <= finalized_height)
            .map(|tombstone| tombstone.hash)
            .collect();
        for hash in finalized_hashes {
            tombstones.remove(&hash);
            self.delete_raw(batch, HEADER_CONSENSUS_INVALID_BODY_TOMBSTONE, hash.0)?;
            if changes.delete_nodes.contains(&hash) || self.header_node(hash)?.is_none() {
                self.delete_raw(batch, HEADER_BODY_EVIDENCE_AUTHORITY, hash.0)?;
            }
        }
        for tombstone in additions {
            tombstones.insert(tombstone.hash, tombstone.clone());
            self.put_value(
                batch,
                HEADER_CONSENSUS_INVALID_BODY_TOMBSTONE,
                tombstone.hash.0,
                &tombstone,
            )?;
        }
        if tombstones.len() > TOMBSTONE_LIMIT {
            let deleted: HashSet<_> = changes.delete_nodes.iter().copied().collect();
            let inserted: HashSet<_> = changes.put_nodes.iter().map(|node| node.hash).collect();
            let mut evictable = Vec::new();
            for tombstone in tombstones.values() {
                let retained = inserted.contains(&tombstone.hash)
                    || !deleted.contains(&tombstone.hash)
                        && self.header_node(tombstone.hash)?.is_some();
                if !retained {
                    evictable.push((tombstone.height, tombstone.hash));
                }
            }
            evictable.sort_unstable_by_key(|(height, hash)| (*height, hash.0));
            let excess = tombstones.len() - TOMBSTONE_LIMIT;
            if evictable.len() < excess {
                return Err(StoreError::LimitExceeded {
                    collection: StoreCollection::ConsensusInvalidBodyTombstones,
                    limit: RowLimit::new(TOMBSTONE_LIMIT),
                }
                .into());
            }
            for (_, hash) in evictable.into_iter().take(excess) {
                tombstones.remove(&hash);
                self.delete_raw(batch, HEADER_CONSENSUS_INVALID_BODY_TOMBSTONE, hash.0)?;
                self.delete_raw(batch, HEADER_BODY_EVIDENCE_AUTHORITY, hash.0)?;
            }
        }

        self.put_value(
            batch,
            HEADER_ENGINE_META,
            TOMBSTONE_COUNT_KEY,
            &HeaderRowCountDisk(u64::try_from(tombstones.len()).map_err(|_| {
                HeaderChainStoreError::Incoherent("tombstone count does not fit u64")
            })?),
        )?;
        Ok(())
    }

    /// Return the exact context-table delta for one authenticated finalized-parent advance.
    fn incremental_context_slide(
        &self,
        previous: Frontier,
        current: Frontier,
        staged_nodes: &HashMap<block::Hash, &HeaderNode>,
    ) -> Result<Option<(HeaderValidationContextDisk, Option<block::Hash>)>, HeaderChainStoreError>
    {
        if current.height.0 != previous.height.0.saturating_add(1) {
            return Ok(None);
        }
        let Some(previous_node) = self.header_node(previous.hash)? else {
            return Ok(None);
        };
        let stored_current = if staged_nodes.contains_key(&current.hash) {
            None
        } else {
            self.header_node(current.hash)?
        };
        let current_node = staged_nodes
            .get(&current.hash)
            .copied()
            .or(stored_current.as_ref());
        let Some(current_node) = current_node else {
            return Ok(None);
        };
        if previous_node.hash != previous.hash
            || previous_node.header.hash() != previous.hash
            || previous_node.height != previous.height
            || current_node.hash != current.hash
            || current_node.header.hash() != current.hash
            || current_node.height != current.height
            || current_node.parent_hash != previous.hash
            || current_node.header.previous_block_hash != previous.hash
        {
            return Ok(None);
        }

        let predecessor_span = u32::try_from(zakura_header_chain::POW_PREDECESSOR_CONTEXT_SPAN)
            .map_err(|_| {
                HeaderChainStoreError::Incoherent("validation context bound does not fit in u32")
            })?;
        let outgoing = if previous.height.0 >= predecessor_span {
            let outgoing_height = block::Height(previous.height.0 - predecessor_span);
            let Some(outgoing_hash) = self.authenticated_canonical_hash(outgoing_height)? else {
                return Ok(None);
            };
            let Some(outgoing_context) = self.get_value::<HeaderValidationContextDisk>(
                HEADER_VALIDATION_CONTEXT,
                outgoing_hash.0,
            )?
            else {
                return Ok(None);
            };
            if outgoing_context.header.hash() != outgoing_hash
                || outgoing_context.height != outgoing_height
            {
                return Ok(None);
            }
            Some(outgoing_hash)
        } else {
            None
        };

        Ok(Some((
            HeaderValidationContextDisk {
                header: previous_node.header,
                height: previous_node.height,
            },
            outgoing,
        )))
    }

    fn recovery_batch(&self, plan: &RecoveryPlan) -> Result<DiskWriteBatch, HeaderChainStoreError> {
        let mut batch = DiskWriteBatch::new();
        if plan.repairs.contains(&RecoveryRepair::InheritedEligibility) {
            for node in &plan.header_nodes {
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
            for (parent, child) in &plan.header_child_edges {
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
        node: &HeaderNode,
    ) -> Result<(), HeaderChainStoreError> {
        for reason in &node.eligibility.direct_reasons {
            let key = HeaderEligibilityRootKey {
                kind: reason_kind(reason),
                root: node.hash,
                evidence: reason_evidence(reason),
            };
            self.delete_raw(batch, HEADER_ELIGIBILITY_ROOT, key.as_bytes())?;
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
        existing_bounds: Option<(block::Height, block::Height)>,
    ) -> Result<(), HeaderChainStoreError> {
        if delta.remove_before.is_some() || delta.remove_from.is_some() {
            let Some((first, last)) = existing_bounds else {
                return Err(HeaderChainStoreError::Incoherent(
                    "projection deletion has no existing bounds",
                ));
            };
            if first > last {
                return Err(HeaderChainStoreError::Incoherent(
                    "existing projection bounds are reversed",
                ));
            }
            if let Some(remove_before) = delta.remove_before {
                if remove_before < first {
                    return Err(HeaderChainStoreError::Incoherent(
                        "projection prefix deletion precedes its existing bounds",
                    ));
                }
                if remove_before > first {
                    let end = block::Height(remove_before.0 - 1).min(last);
                    self.delete_projection_rows(batch, family, first, end)?;
                }
            }
            if let Some(remove_from) = delta.remove_from {
                if remove_from < first {
                    return Err(HeaderChainStoreError::Incoherent(
                        "projection suffix deletion precedes its existing bounds",
                    ));
                }
                if remove_from <= last {
                    self.delete_projection_rows(batch, family, remove_from, last)?;
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

    fn delete_projection_rows(
        &self,
        batch: &mut DiskWriteBatch,
        family: &'static str,
        start: block::Height,
        end: block::Height,
    ) -> Result<(), HeaderChainStoreError> {
        let count = end
            .0
            .checked_sub(start.0)
            .and_then(|count| count.checked_add(1))
            .ok_or(HeaderChainStoreError::Incoherent(
                "projection deletion bounds are reversed",
            ))?;
        if usize::try_from(count)
            .ok()
            .is_none_or(|count| count > zakura_header_chain::MAX_NON_FINALIZED_NODES_V1)
        {
            return Err(HeaderChainStoreError::Incoherent(
                "projection deletion exceeds the retained-node bound",
            ));
        }
        for height in start.0..=end.0 {
            self.delete_raw(
                batch,
                family,
                HeaderHeightKey(block::Height(height)).as_bytes(),
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
        let cf = self.cf(HEADER_FINALITY_HISTORY).map_err(store_error)?;
        self.db
            .raw_visit_cf(&cf, &mut |key, value| {
                if key.len() != 8 {
                    return Err(StoreError::Incoherent("invalid finality key width"));
                }
                let record = FinalityRecord::decode(value)
                    .map_err(|_| StoreError::Incoherent("invalid finality value"))?;
                if key != record.epoch.get().to_be_bytes() {
                    return Err(StoreError::Incoherent("finality key/value mismatch"));
                }
                visitor(record)?;
                Ok(())
            })
            .map_err(|error| match error {
                RawVisitError::RocksDb(error) => {
                    tracing::warn!(?error, "finality history iterator failed");
                    StoreError::Unavailable("finality history iterator failed")
                }
                RawVisitError::Visitor(error) => error,
            })
    }

    fn finality_history(&self) -> Result<Vec<FinalityRecord>, StoreError> {
        let mut records = Vec::new();
        self.visit_finality_records(&mut |record| {
            records.push(record);
            Ok(())
        })?;
        Ok(records)
    }

    fn finality_rebase_history(
        &self,
        original_anchor: block::Hash,
        current_finalized: Frontier,
        max_records: u64,
    ) -> Result<Vec<FinalityRecord>, StoreError> {
        if original_anchor == current_finalized.hash {
            return Ok(Vec::new());
        }
        if max_records == 0 {
            return Ok(Vec::new());
        }

        let metadata = self.metadata()?;
        if metadata.frontiers.finalized != current_finalized {
            return Err(StoreError::Incoherent(
                "finality rebase frontier disagrees with durable metadata",
            ));
        }

        let mut reverse_path = Vec::new();
        let mut expected_current = current_finalized;
        let mut epoch = metadata.finality_epoch.get();
        for _ in 0..max_records {
            let key = HeaderFinalityKey(zakura_header_chain::FinalityEpoch::new(epoch));
            let Some(record) = self
                .get_value::<FinalityRecord>(HEADER_FINALITY_HISTORY, key.as_bytes())
                .map_err(store_error)?
            else {
                if epoch == 0 {
                    break;
                }
                return Err(StoreError::Incoherent(
                    "finality rebase history has a missing epoch",
                ));
            };
            if record.epoch.get() != epoch || record.current != expected_current {
                return Err(StoreError::Incoherent(
                    "finality rebase history is not contiguous",
                ));
            }
            reverse_path.push(record);
            if record.previous.hash == original_anchor {
                reverse_path.reverse();
                return Ok(reverse_path);
            }
            expected_current = record.previous;
            let Some(previous_epoch) = epoch.checked_sub(1) else {
                break;
            };
            epoch = previous_epoch;
        }
        Ok(Vec::new())
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

    #[cfg(test)]
    fn aux_deliveries(&self, hash: block::Hash) -> Result<Vec<AuxDelivery>, StoreError> {
        load_transition_engine(self)
            .map(|engine| engine.aux_deliveries(hash).to_vec())
            .map_err(store_error)
    }

    fn header_node(&self, hash: block::Hash) -> Result<Option<HeaderNode>, StoreError> {
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

    fn untrusted_aux_deliveries(
        &self,
        hash: block::Hash,
    ) -> Result<Vec<UntrustedAuxDeliveryRow>, StoreError> {
        let mut deliveries = Vec::new();
        for (key, value) in self
            .scan_prefix(HEADER_AUX_DELIVERY, &hash.0)
            .map_err(store_error)?
        {
            if key.len() != 64 {
                return Err(StoreError::Incoherent("invalid auxiliary key width"));
            }
            let delivery = decode_untrusted_aux_delivery(&value)
                .map_err(|_| StoreError::Incoherent("invalid auxiliary value"))?;
            if delivery.delivery().header_hash != hash
                || key[32..] != delivery.delivery().delivery_id.digest()
            {
                return Err(StoreError::Incoherent("auxiliary key/value mismatch"));
            }
            deliveries.push(delivery);
        }
        deliveries.sort_unstable_by_key(|delivery| delivery.delivery().delivery_id);
        Ok(deliveries)
    }

    fn selected_hash(&self, height: block::Height) -> Result<Option<block::Hash>, StoreError> {
        self.projection_hash(HEADER_SELECTED, height)
    }

    fn verified_hash(&self, height: block::Height) -> Result<Option<block::Hash>, StoreError> {
        self.projection_hash(HEADER_VERIFIED, height)
    }

    fn validation_context(
        &self,
        parent: block::Hash,
        network: &Network,
    ) -> Result<ValidationLease, StoreError> {
        let metadata = self.metadata()?;
        let parent_node = self
            .header_node(parent)?
            .ok_or(StoreError::Incoherent("validation parent is not retained"))?;
        let parent_frontier = Frontier::new(parent_node.height, parent);
        let mut predecessors = vec![zakura_header_chain::HeaderContextFact {
            frontier: parent_frontier,
            header: parent_node.header.clone(),
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
            network.clone(),
            metadata.anchor_manifest_digest,
        ))
    }

    fn load_header_nodes(&self) -> Result<Vec<HeaderNode>, StoreError> {
        let snapshot = self.audit_snapshot()?;
        let mut nodes = Vec::new();
        snapshot.visit_header_nodes(
            RowLimit::new(zakura_header_chain::MAX_NON_FINALIZED_NODES_V1 + 1),
            &mut |node| {
                nodes.push(node);
                Ok(())
            },
        )?;
        Ok(nodes)
    }

    fn load_consensus_invalid_body_tombstones(
        &self,
    ) -> Result<Vec<zakura_header_chain::ConsensusInvalidBodyTombstone>, StoreError> {
        let snapshot = self.audit_snapshot()?;
        let mut rows = Vec::new();
        snapshot.visit_consensus_invalid_body_tombstones(RowLimit::new(65_536), &mut |row| {
            rows.push(row);
            Ok(())
        })?;
        Ok(rows)
    }

    fn header_child_edges(&self) -> Result<Vec<(block::Hash, block::Hash)>, StoreError> {
        let snapshot = self.audit_snapshot()?;
        let mut rows = Vec::new();
        snapshot.visit_header_child_edges(
            RowLimit::new(zakura_header_chain::MAX_NON_FINALIZED_NODES_V1 + 1),
            &mut |row| {
                rows.push(row);
                Ok(())
            },
        )?;
        Ok(rows)
    }

    pub(in crate::service) fn selected_projection(&self) -> Result<Vec<Frontier>, StoreError> {
        let snapshot = self.audit_snapshot()?;
        let mut rows = Vec::new();
        snapshot.visit_selected_projection(
            RowLimit::new(zakura_header_chain::MAX_NON_FINALIZED_NODES_V1 + 1),
            &mut |row| {
                rows.push(row);
                Ok(())
            },
        )?;
        Ok(rows)
    }

    fn verified_projection(&self) -> Result<Vec<Frontier>, StoreError> {
        let snapshot = self.audit_snapshot()?;
        let mut rows = Vec::new();
        snapshot.visit_verified_projection(
            RowLimit::new(zakura_header_chain::MAX_NON_FINALIZED_NODES_V1 + 1),
            &mut |row| {
                rows.push(row);
                Ok(())
            },
        )?;
        Ok(rows)
    }

    fn deferred_entries(&self) -> Result<Vec<(DateTime<Utc>, block::Hash)>, StoreError> {
        let snapshot = self.audit_snapshot()?;
        let mut rows = Vec::new();
        snapshot.visit_deferred_entries(
            RowLimit::new(zakura_header_chain::MAX_NON_FINALIZED_NODES_V1 + 1),
            &mut |row| {
                rows.push(row);
                Ok(())
            },
        )?;
        Ok(rows)
    }

    fn load_aux_deliveries(&self) -> Result<Vec<UntrustedAuxDeliveryRow>, StoreError> {
        let snapshot = self.audit_snapshot()?;
        let mut rows = Vec::new();
        snapshot.visit_aux_deliveries(
            RowLimit::new(zakura_header_chain::MAX_AUX_DELIVERIES_TOTAL_V1),
            &mut |row| {
                rows.push(row);
                Ok(())
            },
        )?;
        Ok(rows)
    }

    fn authenticated_canonical_hash(
        &self,
        height: block::Height,
    ) -> Result<Option<block::Hash>, StoreError> {
        self.audit_snapshot()?.authenticated_canonical_hash(height)
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

fn authenticated_context_headers(
    store: &HeaderChainStore,
    parent: block::Hash,
    staged_nodes: Option<&HashMap<block::Hash, &HeaderNode>>,
) -> Result<Vec<HeaderValidationContextDisk>, StoreError> {
    let staged_parent = staged_nodes.and_then(|nodes| nodes.get(&parent).copied());
    let stored_parent = if staged_parent.is_none() {
        store.header_node(parent)?
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
            store.header_node(current_hash)?
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

    fn projection_range(
        &self,
        family: &'static str,
        start: block::Height,
        end: block::Height,
    ) -> Result<Vec<Frontier>, HeaderChainStoreError> {
        if start > end {
            return Ok(Vec::new());
        }
        let lower = HeaderHeightKey(start).as_bytes();
        let upper = end
            .next()
            .ok()
            .map(|height| HeaderHeightKey(height).as_bytes());
        let rows = self.scan_range(family, &lower, upper.as_ref().map(AsRef::as_ref))?;
        let mut expected_height = start;
        let mut projection = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            if key.len() != 4 || value.len() != 32 {
                return Err(HeaderChainStoreError::Incoherent(
                    "invalid projection row width",
                ));
            }
            let height = HeaderHeightKey::from_bytes(&key).0;
            if height != expected_height {
                return Err(HeaderChainStoreError::Incoherent(
                    "projection range is not contiguous",
                ));
            }
            let hash =
                block::Hash(value.as_slice().try_into().map_err(|_| {
                    HeaderChainStoreError::Incoherent("invalid projection hash width")
                })?);
            projection.push(Frontier::new(height, hash));
            expected_height = height.next().unwrap_or(height);
        }
        if projection.last().map(|frontier| frontier.height) != Some(end) {
            return Err(HeaderChainStoreError::Incoherent(
                "projection range ended before the requested height",
            ));
        }
        Ok(projection)
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
        EligibilityReason::OperatorInvalid {
            id,
            reason_digest,
            evidence,
        } => {
            hasher.update(id.bytes());
            hasher.update(reason_digest);
            hasher.update(evidence.digest());
        }
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
