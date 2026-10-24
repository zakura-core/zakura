//! A header-chain [`Port`] over the real engine, with in-memory state.
//!
//! [`EnginePort`] follows the production adapter in `zakurad`
//! (`HeaderChainServicePort`) and the state service behind it
//! (`HeaderChainReader` and `HeaderChainRuntime::apply` in zakura-state), for
//! the state a simulation reaches. The engine runs in `Integrated` mode, as in
//! `zakurad`, so only full state can advance finality. A simulation commits no
//! block bodies, so finality stays at the fixture's anchor and every read is
//! above it. Above finality, production reads its database only to mirror the
//! engine, so this adapter reads the engine directly.
//!
//! Each operation runs when its future is first polled, atomically under the
//! engine and lease locks. When and how the run completes it is the
//! operation lifecycle's job, on top of this adapter. `vct_repair_context` is
//! narrower than production: it always answers `Stale`, since a simulation
//! has no repair to resolve. Retained paths follow [`super::lease`].

use std::sync::Arc;

use sha2::{Digest, Sha256};
use zakura_chain::block;
use zakura_header_chain::{
    AlarmSet, AuxDelivery, BodySizeHint, BodyWorkOwner, CheckpointSet, CommittedStallReceipt,
    EngineConfig, EngineMetadata, EngineMode, EngineSnapshot, ErrorSubject, EvidenceId,
    FinalityEpoch, FrontierSet, FullStateEvidenceAuthority, HeaderBatchInput,
    HeaderChainDiskVersion, HeaderChainEngine, HeaderChainError, HeaderContextFact, HeaderFailure,
    HeaderGeneration, HeaderId, HeaderInsertionFacts, HeaderLocator, HeaderRule, HeaderRules,
    HeaderSyncWorkOwner, HeaderValidationCheck, HeaderValidationFacts, HeaderValidationSource,
    HeaderViolation, InsertHeaders, InvalidTransitionEvidence, MemHeaderStore, RuleId, SourceId,
    StateVersion, StoreError, TargetCompletion, TransitionContext, TransitionEvent,
    TransitionFailure, TransitionInput, TrustedAnchor, ValidationLease, VerifiedGeneration,
};
use zakura_node_services::header_chain::{
    AcquirePath, AcquirePathReply, AdapterKey, ApplyHeaderTargetOutcome, ApplyHeaderTargetReply,
    HeaderChainFuture, LocatorReadPermit, Port, PortError, PrepareHeaderTarget,
    PrepareHeaderTargetReply, PreparedHeaderTarget, ReadPath, ReadPathReply, RetainedHeaderPath,
    RetainedHeaderPathPage, SelectedChainMembership, TargetEntry, VctRepairContextReply,
};
use zakura_rt::{
    sync::{watch, Mutex, MutexGuard, PoisonError},
    time::Instant,
};

use super::{
    fixture::{HeaderFixture, ScenarioClock},
    lease::{Capacity, Position, Registry, Spec, MAX_HEADER_SYNC_HEIGHT_RANGE},
};

/// A header-chain port over one in-memory engine. A requester and a
/// publisher sharing one `Arc<EnginePort>` see the same committed state.
#[derive(Debug)]
pub struct EnginePort {
    key: AdapterKey,
    config: EngineConfig,
    engine: Mutex<HeaderChainEngine>,
    leases: Mutex<Registry>,
    /// The committed snapshot. Apply sends on it while it holds the
    /// engine lock, so no consumer may hold a borrow of this watch while it
    /// takes the engine lock, or the two would deadlock.
    selected: watch::Sender<Option<EngineSnapshot>>,
}

impl EnginePort {
    /// A port whose engine holds only `fixture`'s anchor.
    ///
    /// # Panics
    ///
    /// If the fixture's anchor isn't a valid engine anchor, which the Regtest
    /// genesis block always is.
    pub fn new(fixture: &HeaderFixture) -> Self {
        let anchor = fixture.anchor();
        let header = fixture.genesis_header();
        let work = header
            .difficulty_threshold
            .to_work()
            .expect("the Regtest genesis target has valid work");
        let graph = MemHeaderStore::new(anchor, header.clone(), work, work.as_u256())
            .expect("the genesis anchor is coherent");
        let config = EngineConfig::new(
            EngineMode::Integrated,
            fixture.network().clone(),
            TrustedAnchor {
                frontier: anchor,
                header,
            },
            CheckpointSet::default(),
        )
        .expect("the Regtest genesis configuration is coherent");
        // Production initializes every version at 1, never 0.
        let metadata = EngineMetadata {
            disk_format: HeaderChainDiskVersion::CURRENT,
            mode: EngineMode::Integrated,
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
            header_best_score: graph
                .header_chain_score(anchor.hash)
                .expect("the anchor has a score"),
            oldest_retained_height: anchor.height,
            alarms: AlarmSet::default(),
            last_transition: None,
        };
        let engine =
            HeaderChainEngine::from_audited_state(graph, metadata, vec![anchor], vec![anchor], [])
                .expect("a one-node engine is coherent");
        // Production mirrors the current snapshot as soon as it attaches.
        let (selected, _) = watch::channel(Some(engine.snapshot()));
        EnginePort {
            key: AdapterKey::new(),
            config,
            engine: Mutex::new(engine),
            leases: Mutex::new(Registry::default()),
            selected,
        }
    }

    /// The key that seals this port's targets and path handles. The
    /// operation lifecycle checks requests against it.
    pub fn adapter_key(&self) -> &AdapterKey {
        &self.key
    }

    /// The latest committed snapshot.
    pub fn snapshot(&self) -> EngineSnapshot {
        self.engine().snapshot()
    }

    /// Locks the engine. A panic while it was held can't leave it half
    /// changed, since installing a transition is its only mutation and
    /// leaves the engine unchanged on error.
    fn engine(&self) -> MutexGuard<'_, HeaderChainEngine> {
        self.engine.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Locks the lease registry, always after the engine when both are
    /// held. Each registry change completes under the lock.
    fn leases(&self) -> MutexGuard<'_, Registry> {
        self.leases.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// How many retained-path leases are live, and so hold serving capacity.
    pub fn live_leases(&self) -> usize {
        self.leases().live()
    }

    /// The peers whose consumed lease is cached for continuation, in peer
    /// order. The cache holds no capacity and keeps at most
    /// [`MAX_RETAINED_PATH_LEASES`](super::lease::MAX_RETAINED_PATH_LEASES).
    pub fn continued_peers(&self) -> Vec<SourceId> {
        self.leases().continued()
    }

    /// How many acquires continued from a cached cursor instead of walking
    /// the graph. A continuation's lease is the one a walk would build, so
    /// this count is the only place the difference shows. Production keeps
    /// no such count.
    pub fn continuation_hits(&self) -> u64 {
        self.leases().continuation_hits()
    }

    /// Leases the path toward `request`'s target, as
    /// `HeaderChainReader::acquire_retained_path` does above finality.
    fn acquire(&self, request: AcquirePath) -> Result<AcquirePathReply, PortError> {
        let AcquirePath {
            source: peer,
            session_id,
            scope,
            target_tip_hash,
            locator_hashes,
        } = request;
        // Checked before the registry, so a bad request sweeps nothing.
        if locator_hashes.is_empty()
            || locator_hashes.len() > zakura_header_chain::MAX_HEADER_LOCATOR_HASHES
        {
            return Err(unavailable(StoreError::Incoherent(
                "retained path locator count is outside protocol bounds",
            )));
        }
        let now = Instant::now();
        let engine = self.engine();
        let mut leases = self.leases();
        let capacity = match leases.admit(peer, now, Capacity::Bounded) {
            Ok(capacity) => capacity,
            Err(refusal) => return Ok(refusal),
        };
        let snapshot = engine.snapshot();
        if scope.branch.target_tip_hash != target_tip_hash
            || scope.header_generation > snapshot.header_generation
        {
            return Ok(AcquirePathReply::Busy);
        }
        let continuation = leases.take_continuation(peer).filter(|cursor| {
            cursor.session_id == session_id
                && cursor.target.hash == target_tip_hash
                && locator_hashes == [cursor.last_frontier.hash]
                // A continuation can't extend the range its class permits.
                && (capacity == Capacity::General
                    || Capacity::for_range(cursor.last_frontier, cursor.target)
                        == Capacity::Bounded)
        });
        if let Some(cursor) = continuation {
            leases.record_continuation();
            return Ok(self.commit_lease(
                &engine,
                &mut leases,
                peer,
                Spec {
                    session_id,
                    target: cursor.target,
                    common_ancestor: cursor.last_frontier,
                    scope,
                    position: cursor.position,
                    retained_path: cursor.retained_path,
                    capacity,
                },
                now,
            ));
        }
        let finalized = snapshot.frontiers.finalized;
        // At most 13 hashes, so a slice lookup is enough.
        let locators = locator_hashes.as_slice();
        let max_depth = (capacity == Capacity::Bounded).then_some(MAX_HEADER_SYNC_HEIGHT_RANGE);
        // Nothing below the genesis anchor exists, so a target the engine
        // doesn't retain isn't retained anywhere.
        let Some(target_node) = engine.graph().header_node(target_tip_hash) else {
            return Ok(AcquirePathReply::TargetNotRetained);
        };
        let mut path = match ancestry(&engine, target_node, finalized, locators, max_depth)? {
            Ancestry::Path(path) => path,
            Ancestry::Pruned => return Ok(AcquirePathReply::HistoryPruned),
            Ancestry::BeyondReserve => return Ok(leases.beyond_reserve()),
        };
        let target = path[0];
        path.reverse();
        let finalized_end = target.height.min(finalized.height);
        // The anchor is the only finalized header, so it's the only finalized
        // locator that can give the intersection.
        let common_ancestor = path
            .first()
            .copied()
            .filter(|frontier| locators.contains(&frontier.hash))
            .or_else(|| {
                (locators.contains(&finalized.hash) && finalized.height <= finalized_end)
                    .then_some(finalized)
            });
        let Some(common_ancestor) = common_ancestor else {
            return Ok(AcquirePathReply::NoLocatorIntersection);
        };
        if capacity == Capacity::Bounded
            && Capacity::for_range(common_ancestor, target) != Capacity::Bounded
        {
            return Ok(leases.beyond_reserve());
        }
        let retained_path: Arc<[block::Hash]> = path
            .iter()
            .filter(|frontier| {
                frontier.height > common_ancestor.height && frontier.height > finalized_end
            })
            .map(|frontier| frontier.hash)
            .collect();
        let position = if common_ancestor == target {
            Position::Complete
        } else {
            Position::Retained { next: 0 }
        };
        Ok(self.commit_lease(
            &engine,
            &mut leases,
            peer,
            Spec {
                session_id,
                target,
                common_ancestor,
                scope,
                position,
                retained_path,
                capacity,
            },
            now,
        ))
    }

    /// Installs a lease for `spec` once its target is still retained, as
    /// `commit_path_lease` does, and seals its handle with this port's key.
    fn commit_lease(
        &self,
        engine: &HeaderChainEngine,
        leases: &mut Registry,
        peer: SourceId,
        spec: Spec,
        now: Instant,
    ) -> AcquirePathReply {
        let retained = engine
            .graph()
            .header_node(spec.target.hash)
            .map(|node| zakura_header_chain::Frontier::new(node.height, node.hash));
        if retained != Some(spec.target) {
            return AcquirePathReply::TargetNotRetained;
        }
        let (session_id, common_ancestor, target, scope) = (
            spec.session_id,
            spec.common_ancestor,
            spec.target,
            spec.scope,
        );
        match leases.commit(peer, spec, now) {
            Some(lease_id) => {
                AcquirePathReply::Acquired(Box::new(RetainedHeaderPath::from_adapter(
                    &self.key,
                    lease_id,
                    peer,
                    session_id,
                    common_ancestor,
                    target,
                    scope,
                )))
            }
            None => AcquirePathReply::Busy,
        }
    }

    /// Reads one page of a leased path, as `HeaderChainReader::read_retained_path`
    /// and the production adapter do. A served page consumes the lease. A
    /// simulation commits no block bodies and nothing below the anchor, so
    /// every body size and finalized tree root is unknown.
    fn read(
        &self,
        path: RetainedHeaderPath,
        request: ReadPath,
    ) -> Result<ReadPathReply, PortError> {
        let Some((lease_id, peer, session_id)) = path.adapter_identity(&self.key) else {
            return Err(PortError::Unavailable { source: None });
        };
        let max_count = request.max_header_count;
        // Checked before the registry, so a bad request sweeps nothing.
        if max_count == 0 || max_count > MAX_HEADER_SYNC_HEIGHT_RANGE {
            return Err(unavailable(StoreError::Incoherent(
                "retained path page count is outside protocol bounds",
            )));
        }
        let now = Instant::now();
        let engine = self.engine();
        let mut leases = self.leases();
        let Some(cursor) = leases.get(peer, session_id, lease_id, now) else {
            return Ok(ReadPathReply::Unavailable);
        };
        if cursor.scope != path.scope || request.after_hash != cursor.last_frontier.hash {
            return Ok(ReadPathReply::Unavailable);
        }
        // Widening u32 to usize is lossless on supported targets.
        let count = max_count as usize;
        let mut headers = Vec::new();
        let mut aux_deliveries = Vec::new();
        let mut previous = cursor.last_frontier;
        let mut position = cursor.position;
        while headers.len() < count {
            let Position::Retained { next } = position else {
                break;
            };
            let hash = *cursor.retained_path.get(next).ok_or_else(|| {
                unavailable(StoreError::Incoherent(
                    "retained header cursor exceeded its immutable suffix",
                ))
            })?;
            // Retention may have retired a leased losing branch.
            let Some(node) = engine.graph().header_node(hash) else {
                return Ok(ReadPathReply::Unavailable);
            };
            let frontier = zakura_header_chain::Frontier::new(node.height, node.hash);
            if previous.height.next().ok() != Some(frontier.height)
                || node.header.previous_block_hash != previous.hash
            {
                return Err(unavailable(StoreError::Incoherent(
                    "header cursor has a non-contiguous item",
                )));
            }
            position = if next.saturating_add(1) == cursor.retained_path.len() {
                Position::Complete
            } else {
                Position::Retained {
                    next: next.saturating_add(1),
                }
            };
            previous = frontier;
            headers.push(node.header.clone());
            aux_deliveries.push(engine.aux_deliveries(hash).to_vec());
        }
        let complete = position == Position::Complete;
        if complete && previous != cursor.target {
            return Err(unavailable(StoreError::Incoherent(
                "header cursor completed before its exact target",
            )));
        }
        if !leases.finish_page(
            peer,
            session_id,
            lease_id,
            cursor.last_frontier,
            position,
            previous,
            now,
        ) {
            return Ok(ReadPathReply::Unavailable);
        }
        let count = headers.len();
        Ok(ReadPathReply::Page(Box::new(RetainedHeaderPathPage {
            common_ancestor: cursor.last_frontier,
            target: cursor.target,
            scope: cursor.scope,
            headers,
            aux_deliveries,
            finalized_tree_aux: vec![None; count],
            finalized_body_sizes: vec![None; count],
            complete,
        })))
    }

    /// Ends a leased path, as `HeaderChainReader::release_retained_path`
    /// does. A repeat, or a lease that already ended, does nothing.
    fn release(&self, path: RetainedHeaderPath) -> Result<(), PortError> {
        let Some((lease_id, peer, session_id)) = path.adapter_identity(&self.key) else {
            return Err(PortError::Unavailable { source: None });
        };
        self.leases()
            .release(peer, session_id, lease_id, path.scope);
        Ok(())
    }

    /// The selected-path locator, as `HeaderChainReader::committed_selected_locator`
    /// reads it.
    fn locator(&self) -> Result<HeaderLocator, PortError> {
        let engine = self.engine();
        HeaderLocator::for_selected_path(&engine.snapshot(), |height| {
            let projection = engine.selected_projection();
            let index = projection
                .binary_search_by_key(&height, |frontier| frontier.height)
                .map_err(|_| StoreError::Incoherent("committed selected projection has a gap"))?;
            Ok(Some(projection[index].hash))
        })
        .map_err(unavailable)
    }

    /// The validation lease for `parent_hash`, as `HeaderChainStore::validation_context`
    /// builds it, or `None` if the engine doesn't retain the parent. Every
    /// predecessor should be retained, since finality never leaves the genesis
    /// anchor, so a missing one is an incoherence, as in production.
    fn validation_lease(
        &self,
        engine: &HeaderChainEngine,
        parent_hash: block::Hash,
    ) -> Result<Option<ValidationLease>, StoreError> {
        let Some(parent) = engine.graph().header_node(parent_hash) else {
            return Ok(None);
        };
        let parent = zakura_header_chain::Frontier::new(parent.height, parent.hash);
        // Widening u32 to usize is lossless on supported targets.
        let required = (parent.height.0 as usize)
            .saturating_add(1)
            .min(zakura_header_chain::MAX_POW_ADJUSTMENT_BLOCK_SPAN);
        let mut predecessors = Vec::with_capacity(required);
        let mut hash = parent.hash;
        while predecessors.len() < required {
            let node = engine
                .graph()
                .header_node(hash)
                .ok_or(StoreError::Incoherent(
                    "a validation predecessor is missing from the retained graph",
                ))?;
            predecessors.push(HeaderContextFact {
                frontier: zakura_header_chain::Frontier::new(node.height, node.hash),
                header: node.header.clone(),
            });
            hash = node.parent_hash;
        }
        Ok(Some(ValidationLease::new(
            parent,
            predecessors,
            self.config.network().clone(),
            self.config.trust_anchor_digest(),
        )))
    }

    /// Prepares and seals `request`, as the production adapter's
    /// `prepare_header_target` does, with the scenario clock and on the
    /// caller's thread.
    fn prepare(&self, request: PrepareHeaderTarget) -> PrepareHeaderTargetReply {
        let PrepareHeaderTarget {
            source,
            owner,
            common_ancestor,
            target,
            entries,
            completion,
        } = request;
        let entries = entries.into_vec();
        let branch = ErrorSubject::Branch(owner.header_authority().branch);
        let lease = {
            let engine = self.engine();
            self.validation_lease(&engine, common_ancestor.hash)
        };
        // A failed lease read is the adapter's local resource failure.
        let lease = match lease {
            Ok(Some(lease)) if lease.parent() == common_ancestor => lease,
            Ok(_) => return Err(Arc::new(HeaderChainError::stale_target(branch))),
            Err(error) => {
                return Err(Arc::new(HeaderChainError::local_resource(
                    branch,
                    Some(Box::new(error)),
                )))
            }
        };
        let rules = HeaderRules::for_validation_lease(&lease).map_err(|error| {
            Arc::new(HeaderChainError::unknown_anchor(
                branch,
                Some(Box::new(error)),
            ))
        })?;
        let headers: Vec<_> = entries.iter().map(|entry| entry.header.clone()).collect();
        let batch = zakura_header_chain::prepare_headers(
            HeaderBatchInput::new(&headers),
            common_ancestor,
            &rules,
            &ScenarioClock,
        )
        .map_err(|error| Arc::new(classify_preparation_failure(error, &entries, source, owner)))?;
        let mut aux = Vec::with_capacity(entries.len());
        for (entry, prepared) in entries.iter().zip(batch.headers()) {
            let body_size = BodySizeHint::new(entry.body_size).map_err(|error| {
                Arc::new(HeaderChainError::malformed_protocol(
                    ErrorSubject::Header(HeaderId::new(prepared.hash)),
                    RuleId::new("LC-WIRE-13"),
                    source,
                    Some(Box::new(error)),
                ))
            })?;
            aux.push(AuxDelivery::new(
                aux_delivery_id(source, owner, prepared.hash),
                prepared.hash,
                source,
                owner,
                body_size,
                entry.tree_aux,
            ));
        }
        Ok(PreparedHeaderTarget::from_insert(
            &self.key,
            Box::new(InsertHeaders {
                owner,
                source,
                parent_hash: common_ancestor.hash,
                target_tip_hash: target.hash,
                completion,
                batch,
                aux,
            }),
        ))
    }

    /// Applies `target`, as the production adapter and
    /// `HeaderChainRuntime::apply` do for a header insertion, and publishes
    /// the snapshot after a commit.
    fn apply(&self, target: PreparedHeaderTarget) -> ApplyHeaderTargetReply {
        let owner = target.owner();
        let branch = owner.header_authority().branch;
        let stale = || Arc::new(HeaderChainError::stale_target(ErrorSubject::Branch(branch)));
        let insert = target.into_insert(&self.key).map_err(|_| stale())?;
        // A repair completion needs a resolved repair context, which this
        // adapter never gives, so production's re-derivation would mismatch.
        if matches!(
            insert.completion,
            TargetCompletion::SelectedAuxiliaryRepair { .. }
        ) {
            return Err(stale());
        }
        let (source, target_tip_hash) = (insert.source, insert.target_tip_hash);
        let mut engine = self.engine();
        let before = engine.snapshot();
        let mut parents = vec![insert.parent_hash];
        if branch.anchor_hash != before.frontiers.finalized.hash
            && insert.parent_hash != before.frontiers.finalized.hash
        {
            parents.push(before.frontiers.finalized.hash);
        }
        let mut validation_leases = Vec::new();
        for parent in parents {
            match self.validation_lease(&engine, parent) {
                Ok(lease) => validation_leases.extend(lease),
                Err(error) => return Err(local_resource(owner, Some(Box::new(error)))),
            }
        }
        validation_leases.dedup_by_key(|lease| lease.parent());
        let completion = CompletionAuthority(insert.as_ref().clone());
        let authority = IssuedAuthority {
            completion: &completion,
            validation_leases: &validation_leases,
        };
        let context = TransitionContext {
            config: &self.config,
            clock: &ScenarioClock,
            full_state_authority: Some(&authority),
            retention_references: &[],
        };
        let input = TransitionInput::InsertHeaders {
            event: insert,
            facts: HeaderInsertionFacts {
                validation: HeaderValidationFacts {
                    validation_leases: validation_leases.clone(),
                },
                // Finality never moves, so no insertion needs rebasing.
                finality_rebase_history: Vec::new(),
            },
        };
        let transition = match engine.plan_transition(input, &context) {
            Ok(transition) => transition,
            Err(TransitionFailure::Stale { .. }) => return Err(stale()),
            Err(failure) => {
                return Err(classify_apply_failure(
                    failure,
                    source,
                    owner,
                    target_tip_hash,
                ))
            }
        };
        let effect = transition.effect();
        let stalled = effect.is_resource_stalled().then(|| CommittedStallReceipt {
            state_version: transition.change_set().metadata.state_version,
            alarm_changed: transition.snapshot_before_commit().alarms.resource_stalled
                != transition.change_set().metadata.alarms.resource_stalled,
            attempted_branch: Some(branch),
        });
        if transition.is_no_change() {
            return Ok(match stalled {
                Some(receipt) => ApplyHeaderTargetOutcome::ResourceStalled(receipt),
                None => ApplyHeaderTargetOutcome::Applied,
            });
        }
        let after = transition.snapshot_after_commit().clone();
        engine
            .install_committed_transition(transition)
            .map_err(|error| local_resource(owner, Some(Box::new(error))))?;
        // Published after the install, so a receiver never sees an
        // uncommitted snapshot, and still under the engine lock, as
        // production publishes under its writer lock. Dropping the lock first
        // would let a later commit publish before this one, leaving the watch
        // behind the engine for good.
        self.selected.send_replace(Some(after));
        drop(engine);
        Ok(match stalled {
            Some(receipt) => ApplyHeaderTargetOutcome::ResourceStalled(receipt),
            None => ApplyHeaderTargetOutcome::Applied,
        })
    }

    /// Where `hash` sits relative to the selected chain, as
    /// `HeaderChainReader::selected_chain_membership` answers above finality.
    /// Nothing exists below the genesis anchor, so a header the engine
    /// doesn't retain is `NotRetained`.
    fn membership(&self, hash: block::Hash) -> Result<SelectedChainMembership, PortError> {
        let engine = self.engine();
        let finalized = engine.snapshot().frontiers.finalized;
        let Some(node) = engine.graph().header_node(hash) else {
            return Ok(SelectedChainMembership::NotRetained);
        };
        let frontier = zakura_header_chain::Frontier::new(node.height, node.hash);
        if frontier.height < finalized.height {
            return Ok(SelectedChainMembership::NotSelected);
        }
        let projection = engine.selected_projection();
        if projection.first() != Some(&finalized) {
            return Err(unavailable(StoreError::Incoherent(
                "selected projection disagrees with its published bounds",
            )));
        }
        // Widening u32 to usize is lossless on supported targets.
        let offset = (frontier.height.0 - finalized.height.0) as usize;
        Ok(if projection.get(offset) == Some(&frontier) {
            SelectedChainMembership::Selected(frontier)
        } else {
            SelectedChainMembership::NotSelected
        })
    }
}

impl Port for EnginePort {
    fn continuation_locator(
        &self,
        permit: LocatorReadPermit,
    ) -> HeaderChainFuture<'_, Result<Option<HeaderLocator>, PortError>> {
        Box::pin(async move {
            let locator = self.locator().map(Some);
            // The permit is held until the read finishes.
            drop(permit);
            locator
        })
    }

    fn vct_repair_context(
        &self,
        _owner: BodyWorkOwner,
        _height: block::Height,
    ) -> HeaderChainFuture<'_, Result<VctRepairContextReply, PortError>> {
        Box::pin(async { Ok(VctRepairContextReply::Stale) })
    }

    fn acquire_header_path(
        &self,
        request: AcquirePath,
    ) -> HeaderChainFuture<'_, Result<AcquirePathReply, PortError>> {
        Box::pin(async move { self.acquire(request) })
    }

    fn read_header_path(
        &self,
        path: RetainedHeaderPath,
        request: ReadPath,
    ) -> HeaderChainFuture<'_, Result<ReadPathReply, PortError>> {
        Box::pin(async move { self.read(path, request) })
    }

    fn release_header_path(
        &self,
        path: RetainedHeaderPath,
    ) -> HeaderChainFuture<'_, Result<(), PortError>> {
        Box::pin(async move { self.release(path) })
    }

    fn prepare_header_target(
        &self,
        request: PrepareHeaderTarget,
    ) -> HeaderChainFuture<'_, PrepareHeaderTargetReply> {
        Box::pin(async move { self.prepare(request) })
    }

    fn apply_header_target(
        &self,
        target: PreparedHeaderTarget,
    ) -> HeaderChainFuture<'_, ApplyHeaderTargetReply> {
        Box::pin(async move { self.apply(target) })
    }

    fn selected_chain(&self) -> watch::Receiver<Option<EngineSnapshot>> {
        self.selected.subscribe()
    }

    fn selected_chain_contains(
        &self,
        hash: block::Hash,
    ) -> HeaderChainFuture<'_, Result<SelectedChainMembership, PortError>> {
        Box::pin(async move { self.membership(hash) })
    }
}

/// The result of a target-first ancestry walk.
enum Ancestry {
    /// Target first, down to the nearest locator or the finalized anchor.
    Path(Vec<zakura_header_chain::Frontier>),
    /// An ancestor is missing from the retained graph.
    Pruned,
    /// The walk passed a bounded lease's range first.
    BeyondReserve,
}

/// Walks from `target` toward `finalized` until it meets a locator, as
/// `retained_ancestry_to_nearest_locator` does, over the retained graph.
/// `max_depth` bounds the walk for a reserve-slot lease.
fn ancestry(
    engine: &HeaderChainEngine,
    target: &zakura_header_chain::HeaderNode,
    finalized: zakura_header_chain::Frontier,
    locators: &[block::Hash],
    max_depth: Option<u32>,
) -> Result<Ancestry, PortError> {
    let target_height = target.height;
    let mut path = vec![zakura_header_chain::Frontier::new(
        target.height,
        target.hash,
    )];
    let mut current = target;
    while current.height > finalized.height && !locators.contains(&current.hash) {
        if max_depth.is_some_and(|max| target_height.0.saturating_sub(current.height.0) >= max) {
            return Ok(Ancestry::BeyondReserve);
        }
        let Some(parent) = engine.graph().header_node(current.parent_hash) else {
            return Ok(Ancestry::Pruned);
        };
        if parent.height.next().ok() != Some(current.height) {
            return Err(unavailable(StoreError::Incoherent(
                "retained target path has non-contiguous heights",
            )));
        }
        path.push(zakura_header_chain::Frontier::new(
            parent.height,
            parent.hash,
        ));
        current = parent;
    }
    if !locators.contains(&current.hash) && path.last().copied() != Some(finalized) {
        return Ok(Ancestry::Pruned);
    }
    Ok(Ancestry::Path(path))
}

/// Authorizes exactly one header completion, as zakura-state's
/// `PreparedHeaderCompletionAuthority` does.
struct CompletionAuthority(InsertHeaders);

impl FullStateEvidenceAuthority for CompletionAuthority {
    fn authorizes_full_state(&self, _event: &TransitionEvent) -> bool {
        false
    }

    fn authorizes_header_completion(&self, insert: &InsertHeaders) -> bool {
        *insert == self.0
    }
}

/// Adds the validation leases this adapter issued for one apply to
/// `completion`, as zakura-state's `StateIssuedAuthority` does. With no block
/// bodies, full state holds no retention references.
struct IssuedAuthority<'a> {
    completion: &'a CompletionAuthority,
    validation_leases: &'a [ValidationLease],
}

impl FullStateEvidenceAuthority for IssuedAuthority<'_> {
    fn authorizes_full_state(&self, event: &TransitionEvent) -> bool {
        self.completion.authorizes_full_state(event)
    }

    fn authorizes_header_completion(&self, insert: &InsertHeaders) -> bool {
        self.completion.authorizes_header_completion(insert)
    }

    fn authorizes_validation_lease(&self, lease: &ValidationLease) -> bool {
        self.validation_leases.contains(lease)
    }
}

/// `PortError::Unavailable` with `error` as its source, as the production
/// adapter maps a failed state read.
fn unavailable(error: StoreError) -> PortError {
    PortError::Unavailable {
        source: Some(Arc::new(error)),
    }
}

/// A local resource failure on `owner`'s branch.
fn local_resource(
    owner: HeaderSyncWorkOwner,
    source: Option<zakura_chain::BoxError>,
) -> Arc<HeaderChainError> {
    Arc::new(HeaderChainError::local_resource(
        ErrorSubject::Branch(owner.header_authority().branch),
        source,
    ))
}

/// The production adapter's delivery ID for one header from `source` and
/// `owner`, `header_aux_delivery_id` in `zakurad`.
fn aux_delivery_id(source: SourceId, owner: HeaderSyncWorkOwner, hash: block::Hash) -> EvidenceId {
    let mut hasher = Sha256::new();
    hasher.update(b"zakura-header-aux-delivery-v1");
    hasher.update(source.digest());
    hasher.update(owner.session_id().to_le_bytes());
    hasher.update(owner.request_id().get().to_le_bytes());
    hasher.update(hash.0);
    EvidenceId::from_digest(hasher.finalize().into())
}

/// The production adapter's evidence ID for a header that failed `rule`,
/// `header_failure_evidence` in `zakurad`.
fn failure_evidence(
    source: SourceId,
    owner: HeaderSyncWorkOwner,
    hash: block::Hash,
    rule: RuleId,
) -> EvidenceId {
    let mut hasher = Sha256::new();
    hasher.update(b"zakura-header-validation-failure-v1");
    hasher.update(source.digest());
    hasher.update(owner.session_id().to_le_bytes());
    hasher.update(owner.request_id().get().to_le_bytes());
    hasher.update(hash.0);
    hasher.update(rule.as_str().as_bytes());
    EvidenceId::from_digest(hasher.finalize().into())
}

/// Classifies a preparation failure as `classify_header_preparation_failure`
/// in `zakurad` does.
fn classify_preparation_failure(
    error: HeaderFailure,
    entries: &[TargetEntry],
    source: SourceId,
    owner: HeaderSyncWorkOwner,
) -> HeaderChainError {
    match error {
        HeaderFailure::Invalid {
            offset,
            rule,
            reason,
        } => {
            let hash = entries
                .get(offset)
                .expect("the failure offset comes from this exact batch")
                .header
                .hash();
            let rule_id = rule
                .rule_ids()
                .first()
                .copied()
                .expect("every validation stage owns a normative rule");
            HeaderChainError::invalid_header(
                ErrorSubject::Header(HeaderId::new(hash)),
                rule_id,
                failure_evidence(source, owner, hash, rule_id),
                source,
                Some(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    reason,
                ))),
            )
        }
        HeaderFailure::Empty | HeaderFailure::Oversized { .. } => {
            HeaderChainError::malformed_protocol(
                ErrorSubject::Request {
                    source,
                    request_id: owner.request_id(),
                },
                RuleId::new("LC-WIRE-08"),
                source,
                None,
            )
        }
        HeaderFailure::InvalidLease => {
            HeaderChainError::stale_target(ErrorSubject::Branch(owner.header_authority().branch))
        }
        HeaderFailure::ClockRange => HeaderChainError::local_resource(
            ErrorSubject::Branch(owner.header_authority().branch),
            Some(Box::new(HeaderFailure::ClockRange)),
        ),
    }
}

/// Classifies a planner refusal as `classify_header_apply_failure` in
/// `zakurad` does. Only a contextual difficulty or time failure is the
/// supplier's. Every other refusal is local.
fn classify_apply_failure(
    failure: TransitionFailure,
    source: SourceId,
    owner: HeaderSyncWorkOwner,
    target_tip_hash: block::Hash,
) -> Arc<HeaderChainError> {
    match failure {
        TransitionFailure::AuxiliaryLimitExceeded => Arc::new(
            HeaderChainError::auxiliary_capacity(Some(Box::new(failure))),
        ),
        TransitionFailure::InvalidEvidence(InvalidTransitionEvidence::Header(
            HeaderViolation::Validation {
                source: HeaderValidationSource::Prepared,
                check: HeaderValidationCheck::ContextualValidation,
            },
        )) => {
            let rule = HeaderRule::ContextualDifficultyAndTime.rule_ids()[0];
            Arc::new(HeaderChainError::invalid_header(
                ErrorSubject::Branch(owner.header_authority().branch),
                rule,
                failure_evidence(source, owner, target_tip_hash, rule),
                source,
                Some(Box::new(failure)),
            ))
        }
        failure => local_resource(owner, Some(Box::new(failure))),
    }
}
