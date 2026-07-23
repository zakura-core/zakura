//! Feature-gated entry points shared by libFuzzer and deterministic corpus tests.

use std::{
    collections::HashMap,
    num::NonZeroU64,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
};

use chrono::{DateTime, Duration, Utc};
use sha2::{Digest, Sha256};
use zakura_chain::{
    block::{self, genesis::regtest_genesis_block},
    parameters::{testnet::RegtestParameters, Network},
};

use crate::{
    apply_transition, AlarmSet, AuxDelivery, AuxDelta, BodyCommitmentKind, BodyEvidence,
    BodyPayloadMismatch, BodyRuleId, BodyUnavailableSummary, BranchId, ChainScore, CheckpointSet,
    Clock, ConsensusBodyInvalid, EngineConfig, EngineMetadata, EngineMode, EngineSnapshot,
    EvidenceId, FinalityEpoch, FinalityRecord, Frontier, FrontierSet, FullStateEvidenceAuthority,
    FullStateFinalized, HeaderChainDiskVersion, HeaderContextFact, HeaderGeneration, HeaderNode,
    HeaderValidationState, InsertHeaders, MemHeaderStore, OperatorInvalidate,
    OperatorInvalidationId, OperatorReconsider, PreparedHeader, PreparedHeaderBatch,
    ProjectionDelta, SourceId, StateVersion, StoreError, StoreRead, SuffixWork, TargetCompletion,
    TransientBodyFailure, TransientBodyFailureKind, TransitionContext, TransitionFailure,
    TransitionPlan, TransitionRequest, TrustedAnchor, ValidationLease, VerifiedBodyEvidence,
    VerifiedChainChanged, VerifiedChangeCause, VerifiedGeneration, VerifiedHeaderRef, WorkOwner,
    MAX_CANDIDATE_TIPS_V1,
};

/// Deterministic summary of one bounded structured-operation replay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForkReplaySummary {
    /// Number of complete operation bytes consumed.
    pub operations: u16,
    /// Successful durable transitions.
    pub commits: u16,
    /// Expected stale or invalid operations with no durable effect.
    pub refused: u16,
    /// Valid idempotent or informational operations with no durable effect.
    pub no_effects: u16,
    /// Exact candidate-tip-cap pressure checks completed.
    pub pressure_checks: u16,
    /// Same-height insertion-order permutation checks completed.
    pub permutation_checks: u16,
    /// Consecutive exact-branch reset checks completed.
    pub reset_checks: u16,
    /// Historical late-A-after-B-promotion incident checks completed.
    pub incident_checks: u16,
    /// Fixed-anchor 999/1,000/1,001 replacement matrix checks completed.
    pub boundary_checks: u16,
    /// Final authoritative snapshot.
    pub snapshot: EngineSnapshot,
    /// Stable digest of operation outcomes and snapshots.
    pub replay_digest: [u8; 32],
    /// Stable digest of every retained node and projection after the final operation.
    pub retained_digest: [u8; 32],
}

#[derive(Clone)]
struct FuzzStore {
    graph: MemHeaderStore,
    metadata: EngineMetadata,
    selected: Vec<Frontier>,
    verified: Vec<Frontier>,
    branches: [Option<Frontier>; 16],
    finality: Vec<FinalityRecord>,
    aux: Vec<AuxDelivery>,
    config: EngineConfig,
}

impl FuzzStore {
    fn new(mode: EngineMode) -> Self {
        let genesis = regtest_genesis_block();
        let frontier = Frontier::new(block::Height(0), genesis.hash());
        let work = genesis
            .header
            .difficulty_threshold
            .to_work()
            .expect("the regtest genesis target has valid work");
        let graph = MemHeaderStore::new(frontier, genesis.header.clone(), work, work.as_u256())
            .expect("the fixed fuzz anchor is coherent");
        let config = EngineConfig::new(
            mode,
            Network::new_regtest(RegtestParameters::default()),
            TrustedAnchor {
                frontier,
                header: genesis.header.clone(),
            },
            CheckpointSet::default(),
        )
        .expect("the fixed fuzz configuration is coherent");
        let metadata = EngineMetadata {
            disk_format: HeaderChainDiskVersion(1),
            mode,
            network_id: config.network.kind(),
            anchor_manifest_digest: config.trust_anchor_digest(),
            work_origin: frontier,
            state_version: StateVersion::new(0),
            header_generation: HeaderGeneration::new(0),
            verified_generation: VerifiedGeneration::new(0),
            finality_epoch: FinalityEpoch::new(0),
            frontiers: FrontierSet {
                finalized: frontier,
                header_best: frontier,
                verified_best: frontier,
            },
            header_best_score: graph.score(frontier.hash).expect("the anchor has a score"),
            oldest_retained_height: frontier.height,
            alarms: AlarmSet::default(),
            last_transition_id: EvidenceId::from_digest([0xff; 32]),
        };
        Self {
            graph,
            metadata,
            selected: vec![frontier],
            verified: vec![frontier],
            branches: {
                let mut branches = [None; 16];
                branches[0] = Some(frontier);
                branches
            },
            finality: Vec::new(),
            aux: Vec::new(),
            config,
        }
    }

    fn snapshot(&self) -> EngineSnapshot {
        self.metadata.snapshot()
    }

    fn commit(&mut self, plan: &TransitionPlan) {
        self.graph = plan.fuzz_projected().clone();
        self.metadata = plan.change_set().metadata.clone();
        apply_projection(&mut self.selected, &plan.change_set().selected_projection);
        apply_projection(&mut self.verified, &plan.change_set().verified_projection);
        if let Some(record) = plan.change_set().finality_append {
            self.finality.push(record);
        }
        for change in &plan.change_set().aux_changes {
            match change {
                AuxDelta::Put(delivery) => {
                    self.aux
                        .retain(|existing| existing.delivery_id != delivery.delivery_id);
                    self.aux.push(**delivery);
                }
                AuxDelta::Delete(delivery_id) => self
                    .aux
                    .retain(|existing| existing.delivery_id != *delivery_id),
            }
        }
        for tip in &mut self.branches {
            if tip.is_some_and(|frontier| self.graph.node(frontier.hash).is_none()) {
                *tip = None;
            }
        }
    }

    fn lease(&self, parent: Frontier) -> ValidationLease {
        let node = self
            .graph
            .node(parent.hash)
            .expect("an operation selects only a retained parent");
        let mut digest = [0u8; 32];
        digest[..4].copy_from_slice(&parent.height.0.to_le_bytes());
        digest[4..].copy_from_slice(&parent.hash.0[..28]);
        ValidationLease {
            parent,
            predecessors: vec![HeaderContextFact {
                frontier: parent,
                difficulty_threshold: node.header.difficulty_threshold,
                time: node.header.time,
            }],
            trust_anchor_digest: self.config.trust_anchor_digest(),
            context_digest: digest,
        }
    }

    fn insertion(
        &self,
        parent: Frontier,
        count: u32,
        operation: usize,
        branch: u8,
    ) -> TransitionRequest {
        self.insertion_with_validation(
            parent,
            count,
            operation,
            branch,
            HeaderValidationState::Valid,
            false,
        )
    }

    fn insertion_with_validation(
        &self,
        parent: Frontier,
        count: u32,
        operation: usize,
        branch: u8,
        validation: HeaderValidationState,
        hard_work: bool,
    ) -> TransitionRequest {
        let lease = self.lease(parent);
        let evidence = evidence(operation, branch);
        let mut headers = Vec::with_capacity(usize::try_from(count).unwrap_or(8));
        let mut parent_hash = parent.hash;
        for offset in 1..=count {
            let mut header = *regtest_genesis_block().header;
            header.previous_block_hash = parent_hash;
            if hard_work {
                header.difficulty_threshold =
                    zakura_chain::work::difficulty::CompactDifficulty::from_le_bytes(
                        0x1d00_ffff_u32.to_le_bytes(),
                    );
            }
            header.nonce.0[..8].copy_from_slice(&operation_u64(operation).to_le_bytes());
            header.nonce.0[8] = branch;
            header.nonce.0[9..13].copy_from_slice(&offset.to_le_bytes());
            let header = Arc::new(header);
            let hash = header.hash();
            headers.push(PreparedHeader {
                header: header.clone(),
                hash,
                height: block::Height(parent.height.0.saturating_add(offset)),
                block_work: header
                    .difficulty_threshold
                    .to_work()
                    .expect("the fixed target has valid work"),
                validation,
            });
            parent_hash = hash;
        }
        let batch = PreparedHeaderBatch::new(headers, lease.context_digest, evidence)
            .expect("the operation count is nonzero");
        let target = batch.headers().last().expect("the batch is nonempty").hash;
        TransitionRequest {
            expected_version: self.metadata.state_version,
            event: crate::TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
                owner: WorkOwner {
                    state_version: self.metadata.state_version,
                    header_generation: self.metadata.header_generation,
                    verified_generation: None,
                    branch: BranchId::new(self.metadata.frontiers.finalized.hash, target),
                    session_id: u64::try_from(operation).unwrap_or(u64::MAX),
                    request_id: NonZeroU64::new(
                        u64::try_from(operation)
                            .unwrap_or(u64::MAX)
                            .saturating_add(1),
                    )
                    .expect("the request identity is nonzero"),
                },
                source: SourceId::from_digest([branch; 32]),
                parent_hash: parent.hash,
                target_tip_hash: target,
                completion: TargetCompletion::TargetComplete {
                    common_ancestor: parent,
                },
                batch,
                aux: Vec::new(),
            })),
        }
    }

    fn retained_parent(&self, selector: u8) -> Frontier {
        let finalized = self.metadata.frontiers.finalized;
        let best = self.metadata.frontiers.header_best;
        let span = best.height.0.saturating_sub(finalized.height.0);
        let height = finalized
            .height
            .0
            .saturating_add(u32::from(selector) % span.saturating_add(1));
        let hash = self
            .selected
            .iter()
            .find(|frontier| {
                frontier.height == block::Height(height) && self.graph.node(frontier.hash).is_some()
            })
            .map(|frontier| frontier.hash)
            .unwrap_or(finalized.hash);
        Frontier::new(block::Height(height), hash)
    }

    fn branch_parent(&self, key: u8) -> Frontier {
        self.branches[usize::from(key % 16)]
            .filter(|frontier| self.graph.node(frontier.hash).is_some())
            .unwrap_or(self.metadata.frontiers.header_best)
    }

    fn record_branch_tip(&mut self, key: u8, tip: block::Hash) {
        let Some(node) = self.graph.node(tip) else {
            return;
        };
        self.branches[usize::from(key % 16)] = Some(Frontier::new(node.height, tip));
    }

    fn verify_selected_path(&self, operation: usize, branch: u8) -> TransitionRequest {
        let target = self.retained_parent(branch);
        let new_path = self
            .selected
            .iter()
            .copied()
            .filter(|frontier| {
                frontier.height > self.metadata.frontiers.finalized.height
                    && frontier.height <= target.height
            })
            .map(|frontier| {
                let node = self
                    .graph
                    .node(frontier.hash)
                    .expect("selected projections contain retained nodes");
                VerifiedHeaderRef {
                    height: frontier.height,
                    hash: frontier.hash,
                    header: node.header.clone(),
                }
            })
            .collect();
        TransitionRequest {
            expected_version: self.metadata.state_version,
            event: crate::TransitionEvent::VerifiedChainChanged(VerifiedChainChanged {
                full_state_transition_id: evidence(operation, branch),
                old_tip: self.metadata.frontiers.verified_best,
                new_path,
                cause: VerifiedChangeCause::Reset,
            }),
        }
    }

    fn finalize_verified(&self, operation: usize, branch: u8) -> TransitionRequest {
        let index = usize::from(branch) % self.verified.len();
        let new_finalized = self.verified[index];
        TransitionRequest {
            expected_version: self.metadata.state_version,
            event: crate::TransitionEvent::FullStateFinalized(FullStateFinalized {
                full_state_transition_id: evidence(operation, branch),
                new_finalized,
                verified_path_proof: self
                    .verified
                    .iter()
                    .take(index.saturating_add(1))
                    .map(|frontier| frontier.hash)
                    .collect(),
            }),
        }
    }
}

impl StoreRead for FuzzStore {
    fn snapshot(&self) -> Result<EngineSnapshot, StoreError> {
        Ok(self.snapshot())
    }
    fn metadata(&self) -> Result<EngineMetadata, StoreError> {
        Ok(self.metadata.clone())
    }
    fn node(&self, hash: block::Hash) -> Result<Option<HeaderNode>, StoreError> {
        Ok(self.graph.node(hash).cloned())
    }
    fn children(&self, parent: block::Hash) -> Result<Vec<block::Hash>, StoreError> {
        Ok(self.graph.children(parent))
    }
    fn hashes_at_height(&self, height: block::Height) -> Result<Vec<block::Hash>, StoreError> {
        Ok(self.graph.hashes_at_height(height))
    }
    fn selected_hash(&self, height: block::Height) -> Result<Option<block::Hash>, StoreError> {
        Ok(frontier_hash_at(&self.selected, height))
    }
    fn verified_hash(&self, height: block::Height) -> Result<Option<block::Hash>, StoreError> {
        Ok(frontier_hash_at(&self.verified, height))
    }
    fn candidate_tips(&self) -> Result<Vec<(ChainScore, block::Hash)>, StoreError> {
        self.graph
            .eligible_tips()
            .into_iter()
            .map(|tip| {
                self.graph
                    .score(tip.hash)
                    .map(|score| (score, tip.hash))
                    .map_err(|_| StoreError::Incoherent("candidate score is unavailable"))
            })
            .collect()
    }
    fn validation_context(&self, parent: block::Hash) -> Result<ValidationLease, StoreError> {
        let node = self
            .graph
            .node(parent)
            .ok_or(StoreError::Incoherent("validation parent is not retained"))?;
        Ok(self.lease(Frontier::new(node.height, parent)))
    }
    fn aux_deliveries(&self, hash: block::Hash) -> Result<Vec<AuxDelivery>, StoreError> {
        Ok(self
            .aux
            .iter()
            .filter(|delivery| delivery.header_hash == hash)
            .copied()
            .collect())
    }
    fn finality_history(&self) -> Result<Vec<FinalityRecord>, StoreError> {
        Ok(self.finality.clone())
    }
}

struct ManualClock(AtomicI64);

impl ManualClock {
    fn new() -> Self {
        Self(AtomicI64::new(0))
    }

    fn advance(&self, seconds: u32) {
        self.0.fetch_add(i64::from(seconds), Ordering::Relaxed);
    }
}

struct FuzzAuthority;

impl FullStateEvidenceAuthority for FuzzAuthority {
    fn authorizes(&self, _evidence: EvidenceId) -> bool {
        true
    }
}

impl Clock for ManualClock {
    fn now(&self) -> DateTime<Utc> {
        DateTime::from_timestamp(self.0.load(Ordering::Relaxed), 0)
            .expect("the bounded fuzz clock stays in chrono's supported range")
    }
}

/// Replay up to 512 structured operations through the production transition engine.
pub fn replay_fork_transition_bytes(bytes: &[u8]) -> ForkReplaySummary {
    let bounded = &bytes[..bytes.len().min(512)];
    let mode = if bounded
        .first()
        .is_some_and(|byte| decode_fork_operation(*byte).0 & 1 == 1)
    {
        EngineMode::HeadersOnly
    } else {
        EngineMode::Integrated
    };
    let mut store = FuzzStore::new(mode);
    let mut commits = 0u16;
    let mut refused = 0u16;
    let mut no_effects = 0u16;
    let mut pressure_checks = 0u16;
    let mut permutation_checks = 0u16;
    let mut reset_checks = 0u16;
    let mut incident_checks = 0u16;
    let mut boundary_checks = 0u16;
    let mut transcript = Sha256::new();
    let clock = ManualClock::new();
    let authority = FuzzAuthority;
    assert_exhaustive_oracle(&store);

    for (operation, encoded) in bounded.iter().copied().enumerate() {
        if matches!(bounded.first(), Some(b'A' | b'F' | b'I' | b'R' | b'T'))
            && encoded == b'\n'
            && operation.saturating_add(1) == bounded.len()
        {
            no_effects = no_effects.saturating_add(1);
            transcript.update(b"corpus-newline");
            assert_exhaustive_oracle(&store);
            continue;
        }
        if encoded == b'T' {
            let digest = assert_same_height_permutations();
            transcript.update(b"permutation");
            transcript.update(digest);
            no_effects = no_effects.saturating_add(1);
            permutation_checks = permutation_checks.saturating_add(1);
            assert_exhaustive_oracle(&store);
            continue;
        }
        if encoded == b'R' {
            let digest = assert_consecutive_resets();
            transcript.update(b"resets");
            transcript.update(digest);
            no_effects = no_effects.saturating_add(1);
            reset_checks = reset_checks.saturating_add(1);
            assert_exhaustive_oracle(&store);
            continue;
        }
        if encoded == b'I' {
            let digest = assert_incident_recovery();
            transcript.update(b"incident");
            transcript.update(digest);
            no_effects = no_effects.saturating_add(1);
            incident_checks = incident_checks.saturating_add(1);
            assert_exhaustive_oracle(&store);
            continue;
        }
        if encoded == b'F' {
            let digest = assert_fixed_anchor_boundaries();
            transcript.update(b"fixed-anchor-boundaries");
            transcript.update(digest);
            no_effects = no_effects.saturating_add(1);
            boundary_checks = boundary_checks.saturating_add(1);
            assert_exhaustive_oracle(&store);
            continue;
        }
        let (byte, hard_work) = decode_fork_operation(encoded);
        let before = store.snapshot();
        let before_selected = store.selected.clone();
        let before_verified = store.verified.clone();
        let count = u32::from(byte & 0x07).saturating_add(1);
        let branch = byte.rotate_left(3);
        let branch_key = (byte & 0x07).saturating_add((byte & 0x80) >> 4);
        let request = match (byte >> 3) & 0x0f {
            0 => store.insertion_with_validation(
                store.branch_parent(branch_key),
                count,
                operation,
                branch,
                HeaderValidationState::Valid,
                hard_work,
            ),
            1 => store.insertion_with_validation(
                store.retained_parent(branch),
                count,
                operation,
                branch,
                HeaderValidationState::Valid,
                hard_work,
            ),
            2 => {
                let mut request = store.insertion(
                    store.metadata.frontiers.header_best,
                    count,
                    operation,
                    branch,
                );
                request.expected_version =
                    StateVersion::new(store.metadata.state_version.get().saturating_add(1));
                request
            }
            3 => {
                let target = store.retained_parent(branch);
                if target == store.metadata.frontiers.finalized {
                    refused = refused.saturating_add(1);
                    transcript.update([byte, 0]);
                    assert_exhaustive_oracle(&store);
                    continue;
                }
                TransitionRequest {
                    expected_version: store.metadata.state_version,
                    event: crate::TransitionEvent::OperatorInvalidate(OperatorInvalidate {
                        target: target.hash,
                        id: operator_id(operation, branch),
                        operator_reason_digest: [branch.wrapping_add(1); 32],
                        evidence: evidence(operation, branch),
                    }),
                }
            }
            4 => {
                let target = store.retained_parent(branch);
                if target == store.metadata.frontiers.finalized {
                    refused = refused.saturating_add(1);
                    transcript.update([byte, 0]);
                    assert_exhaustive_oracle(&store);
                    continue;
                }
                TransitionRequest {
                    expected_version: store.metadata.state_version,
                    event: crate::TransitionEvent::OperatorReconsider(OperatorReconsider {
                        target: target.hash,
                        id: operator_id(operation.saturating_sub(1), branch),
                        evidence: evidence(operation, branch),
                    }),
                }
            }
            5 => {
                // Crash/reopen oracle: cloning the coherent logical rows must preserve publication.
                let reopened = store.clone();
                assert_eq!(reopened.snapshot(), store.snapshot());
                assert_eq!(retained_digest(&reopened), retained_digest(&store));
                transcript.update(b"reopen");
                assert_exhaustive_oracle(&store);
                continue;
            }
            6 => {
                let target = store.retained_parent(branch);
                if target == store.metadata.frontiers.finalized {
                    refused = refused.saturating_add(1);
                    transcript.update([byte, 0]);
                    assert_exhaustive_oracle(&store);
                    continue;
                }
                TransitionRequest {
                    expected_version: store.metadata.state_version,
                    event: crate::TransitionEvent::BodyEvidence(BodyEvidence::ConsensusInvalid(
                        ConsensusBodyInvalid {
                            hash: target.hash,
                            evidence: evidence(operation, branch),
                            rule: BodyRuleId::new("fuzz.body.invalid"),
                            source: SourceId::from_digest([branch; 32]),
                        },
                    )),
                }
            }
            7 => {
                let target = store.metadata.frontiers.header_best;
                TransitionRequest {
                    expected_version: store.metadata.state_version,
                    event: crate::TransitionEvent::BodyEvidence(BodyEvidence::Transient(
                        TransientBodyFailure {
                            hash: target.hash,
                            evidence: evidence(operation, branch),
                            kind: TransientBodyFailureKind::VerifierUnavailable,
                            availability: BodyUnavailableSummary {
                                started_at: clock.now(),
                                attempts: u32::from(branch).saturating_add(1),
                                suppliers: 1,
                                supplier_set_digest: [branch; 32],
                                alarmed: byte & 0x80 != 0,
                                next_probe_at: clock.now() + Duration::seconds(1),
                            },
                        },
                    )),
                }
            }
            8 => {
                let target = store.retained_parent(branch);
                TransitionRequest {
                    expected_version: store.metadata.state_version,
                    event: crate::TransitionEvent::BodyEvidence(BodyEvidence::Verified(
                        VerifiedBodyEvidence {
                            hash: target.hash,
                            evidence: evidence(operation, branch),
                        },
                    )),
                }
            }
            9 => TransitionRequest {
                expected_version: store.metadata.state_version,
                event: crate::TransitionEvent::ReevaluateDeferred,
            },
            10 => store.insertion_with_validation(
                store.metadata.frontiers.header_best,
                count,
                operation,
                branch,
                HeaderValidationState::DeferredUntil(clock.now() + Duration::seconds(1)),
                hard_work,
            ),
            11 => {
                clock.advance(u32::from(branch).saturating_add(1));
                transcript.update(b"clock");
                transcript.update(clock.now().timestamp().to_le_bytes());
                assert_exhaustive_oracle(&store);
                continue;
            }
            12 => store.verify_selected_path(operation, branch),
            13 => store.finalize_verified(operation, branch),
            14 => TransitionRequest {
                expected_version: store.metadata.state_version,
                event: crate::TransitionEvent::BodyEvidence(BodyEvidence::PayloadMismatch(
                    BodyPayloadMismatch {
                        evidence: evidence(operation, branch),
                        requested: store.metadata.frontiers.header_best.hash,
                        delivered: store.metadata.frontiers.finalized.hash,
                        kind: BodyCommitmentKind::HeaderHash,
                        source: SourceId::from_digest([branch; 32]),
                    },
                )),
            },
            15 => {
                let digest = assert_candidate_eviction_boundary(operation);
                transcript.update(b"pressure");
                transcript.update(digest);
                no_effects = no_effects.saturating_add(1);
                pressure_checks = pressure_checks.saturating_add(1);
                assert_exhaustive_oracle(&store);
                continue;
            }
            _ => {
                // Explicit stale/no-op references are part of the operation language.
                refused = refused.saturating_add(1);
                transcript.update([byte, 0]);
                assert_exhaustive_oracle(&store);
                continue;
            }
        };
        let inserted_target = match &request.event {
            crate::TransitionEvent::InsertHeaders(event) => Some(event.target_tip_hash),
            _ => None,
        };
        let context = TransitionContext {
            config: &store.config,
            clock: &clock,
            full_state_authority: Some(&authority),
            startup_capability: None,
            retention_references: &[],
        };
        match apply_transition(&store, request, &context) {
            Ok(plan) => {
                assert_eq!(plan.before(), &before);
                let no_change = plan.is_no_change();
                let eligibility_changed = !plan.change_set().eligibility_changes.is_empty();
                store.commit(&plan);
                if let Some(target) = inserted_target {
                    store.record_branch_tip(branch_key, target);
                }
                assert_eq!(store.snapshot(), plan.change_set().metadata.snapshot());
                assert_generation_delta(
                    &before,
                    &store.snapshot(),
                    before_selected != store.selected,
                    before_verified != store.verified,
                    eligibility_changed,
                );
                if no_change {
                    no_effects = no_effects.saturating_add(1);
                } else {
                    commits = commits.saturating_add(1);
                }
            }
            Err(TransitionFailure::Stale { .. })
            | Err(TransitionFailure::InvalidEvidence(_))
            | Err(TransitionFailure::Mode)
            | Err(TransitionFailure::StalePreparation)
            | Err(TransitionFailure::ResourceStalled) => {
                assert_eq!(store.snapshot(), before);
                refused = refused.saturating_add(1);
            }
            Err(error) => panic!(
                "structured operation {operation} ({byte:#04x}) produced unexpected local failure: {error}"
            ),
        }
        assert_exhaustive_oracle(&store);
        append_snapshot(&mut transcript, &store.snapshot());
    }

    ForkReplaySummary {
        operations: u16::try_from(bounded.len()).expect("the operation cap fits in u16"),
        commits,
        refused,
        no_effects,
        pressure_checks,
        permutation_checks,
        reset_checks,
        incident_checks,
        boundary_checks,
        snapshot: store.snapshot(),
        replay_digest: transcript.finalize().into(),
        retained_digest: retained_digest(&store),
    }
}

fn decode_fork_operation(byte: u8) -> (u8, bool) {
    match byte {
        b'A' => (4, false),
        b'B' => (9, false),
        b'C' => (1, false),
        b'D' => (9, true),
        _ => (byte, false),
    }
}

fn assert_candidate_eviction_boundary(seed: usize) -> [u8; 32] {
    let mut store = FuzzStore::new(EngineMode::Integrated);
    let clock = ManualClock::new();
    let authority = FuzzAuthority;
    let finalized = store.metadata.frontiers.finalized;
    for index in 0..=MAX_CANDIDATE_TIPS_V1 {
        assert_eq!(
            store.graph.eligible_tips().len(),
            index.max(1),
            "candidate pressure seed {seed} retains every pre-cap tip at step {index}"
        );
        let branch = u8::try_from(index).expect("the candidate-tip bound fits in u8");
        let operation = seed
            .saturating_mul(MAX_CANDIDATE_TIPS_V1.saturating_add(1))
            .saturating_add(index);
        let request = store.insertion(finalized, 1, operation, branch);
        let new_hash = match &request.event {
            crate::TransitionEvent::InsertHeaders(event) => event.target_tip_hash,
            _ => unreachable!("the pressure fixture constructs only header insertions"),
        };
        let expected_evicted = if index < MAX_CANDIDATE_TIPS_V1 {
            None
        } else {
            let new_work = store
                .graph
                .node(finalized.hash)
                .expect("the fixed anchor is retained")
                .block_work;
            store
                .graph
                .eligible_tips()
                .into_iter()
                .map(|tip| {
                    (
                        store
                            .graph
                            .score(tip.hash)
                            .expect("eligible retained tips have scores"),
                        tip.hash,
                    )
                })
                .chain(std::iter::once((
                    ChainScore::new(
                        SuffixWork::zero()
                            .checked_add(new_work)
                            .expect("one fixed-work child cannot overflow"),
                        new_hash,
                    ),
                    new_hash,
                )))
                .min_by_key(|(score, _)| *score)
                .map(|(_, hash)| hash)
        };
        let context = TransitionContext {
            config: &store.config,
            clock: &clock,
            full_state_authority: Some(&authority),
            startup_capability: None,
            retention_references: &[],
        };
        let plan = apply_transition(&store, request, &context)
            .expect("the candidate-tip pressure fixture remains admissible");
        assert_eq!(
            plan.change_set().delete_nodes,
            expected_evicted
                .filter(|hash| *hash != new_hash)
                .into_iter()
                .collect::<Vec<_>>(),
            "candidate pressure seed {seed} step {index} evicts exactly the independently lowest work/hash tip"
        );
        store.commit(&plan);
        if let Some(evicted) = expected_evicted {
            assert!(
                store.graph.node(evicted).is_none(),
                "the independently lowest candidate is absent after pressure"
            );
        }
        assert_exhaustive_oracle(&store);
    }
    assert_eq!(
        store.graph.eligible_tips().len(),
        MAX_CANDIDATE_TIPS_V1,
        "candidate-tip retention ends exactly at the configured cap"
    );
    retained_digest(&store)
}

fn assert_same_height_permutations() -> [u8; 32] {
    let equal_forward = permutation_fixture([(40, 11, false), (41, 12, false)]);
    let equal_reverse = permutation_fixture([(41, 12, false), (40, 11, false)]);
    assert_eq!(
        equal_forward.0, equal_reverse.0,
        "equal-work fork choice is independent of insertion order"
    );
    assert_eq!(
        equal_forward.1,
        [equal_forward.2, equal_forward.3]
            .into_iter()
            .max_by_key(|hash| hash.0)
            .expect("the permutation fixture has two tips"),
        "equal work is resolved by the greatest raw internal tip hash"
    );

    let unequal_forward = permutation_fixture([(50, 21, false), (51, 22, true)]);
    let unequal_reverse = permutation_fixture([(51, 22, true), (50, 21, false)]);
    assert_eq!(
        unequal_forward.0, unequal_reverse.0,
        "unequal-work fork choice is independent of insertion order"
    );
    assert_eq!(
        unequal_forward.1, unequal_forward.3,
        "the harder same-height branch wins independently of raw hash"
    );

    let mut hasher = Sha256::new();
    hasher.update(equal_forward.0);
    hasher.update(unequal_forward.0);
    hasher.finalize().into()
}

fn assert_consecutive_resets() -> [u8; 32] {
    let mut store = FuzzStore::new(EngineMode::Integrated);
    let clock = ManualClock::new();
    let authority = FuzzAuthority;
    let incumbent = insert_fixture_path(&mut store, &clock, &authority, 70, 31, 4);
    let lower = insert_fixture_path(&mut store, &clock, &authority, 71, 32, 1);
    let same_height = insert_fixture_path(&mut store, &clock, &authority, 72, 33, 1);
    let forward = insert_fixture_path(&mut store, &clock, &authority, 73, 34, 5);

    let reset_paths = [&incumbent, &lower, &same_height, &forward];
    for (index, path) in reset_paths.into_iter().enumerate() {
        let before = store.snapshot();
        let new_tip = path.last().expect("each reset fixture path is nonempty");
        let request = TransitionRequest {
            expected_version: before.state_version,
            event: crate::TransitionEvent::VerifiedChainChanged(VerifiedChainChanged {
                full_state_transition_id: evidence(80 + index, new_tip.hash.0[0]),
                old_tip: before.frontiers.verified_best,
                new_path: path.clone(),
                cause: VerifiedChangeCause::Reset,
            }),
        };
        let context = TransitionContext {
            config: &store.config,
            clock: &clock,
            full_state_authority: Some(&authority),
            startup_capability: None,
            retention_references: &[],
        };
        let plan = apply_transition(&store, request, &context)
            .expect("each exact retained reset path is admissible");
        assert_eq!(
            plan.change_set().metadata.frontiers.verified_best,
            Frontier::new(new_tip.height, new_tip.hash),
            "reset selects the exact hash-qualified path rather than inferring by height"
        );
        assert_eq!(
            plan.change_set().metadata.verified_generation,
            before
                .verified_generation
                .checked_next()
                .expect("the bounded reset fixture cannot exhaust its generation"),
            "every exact reset retires the previous verified-work generation"
        );
        assert_eq!(
            plan.change_set().metadata.header_generation,
            before.header_generation,
            "verified reset shape alone does not replace independently selected header work"
        );
        store.commit(&plan);
        assert_eq!(
            store
                .verified
                .iter()
                .map(|frontier| frontier.hash)
                .collect::<Vec<_>>(),
            std::iter::once(store.metadata.frontiers.finalized.hash)
                .chain(path.iter().map(|header| header.hash))
                .collect::<Vec<_>>(),
            "the committed verified projection is the exact reset branch"
        );
        assert_exhaustive_oracle(&store);
    }

    assert!(
        incumbent.last().expect("incumbent is nonempty").height
            > lower.last().expect("lower reset is nonempty").height
    );
    assert_eq!(
        lower.last().expect("lower reset is nonempty").height,
        same_height
            .last()
            .expect("same-height reset is nonempty")
            .height
    );
    assert_ne!(
        lower.last().expect("lower reset is nonempty").hash,
        same_height
            .last()
            .expect("same-height reset is nonempty")
            .hash
    );
    assert!(
        forward.last().expect("forward reset is nonempty").height
            > same_height
                .last()
                .expect("same-height reset is nonempty")
                .height
    );

    let final_tip = store.metadata.frontiers.verified_best;
    let request = store.insertion(final_tip, 1, 90, 35);
    let inserted_tip = match &request.event {
        crate::TransitionEvent::InsertHeaders(event) => {
            assert_eq!(event.parent_hash, final_tip.hash);
            assert_eq!(
                event.completion,
                TargetCompletion::TargetComplete {
                    common_ancestor: final_tip,
                }
            );
            assert_eq!(
                event.batch.headers()[0].header.previous_block_hash,
                final_tip.hash,
                "the first forward request after consecutive resets anchors to the final exact hash"
            );
            event.target_tip_hash
        }
        _ => unreachable!("the next-child fixture constructs one header insertion"),
    };
    let context = TransitionContext {
        config: &store.config,
        clock: &clock,
        full_state_authority: Some(&authority),
        startup_capability: None,
        retention_references: &[],
    };
    let plan =
        apply_transition(&store, request, &context).expect("the exact next child is admissible");
    store.commit(&plan);
    assert_eq!(
        store
            .graph
            .node(inserted_tip)
            .expect("the committed next child is retained")
            .parent_hash,
        final_tip.hash
    );
    assert_exhaustive_oracle(&store);
    retained_digest(&store)
}

fn assert_incident_recovery() -> [u8; 32] {
    let mut store = FuzzStore::new(EngineMode::Integrated);
    let clock = ManualClock::new();
    let authority = FuzzAuthority;
    let anchor = store.metadata.frontiers.finalized;
    let incumbent_a =
        commit_fixture_insertion(&mut store, &clock, &authority, anchor, 5, 100, 0xa1);
    assert_eq!(store.metadata.frontiers.header_best, incumbent_a);

    let late_a = store.insertion(incumbent_a, 1, 101, 0xa2);
    let held_context = TransitionContext {
        config: &store.config,
        clock: &clock,
        full_state_authority: Some(&authority),
        startup_capability: None,
        retention_references: &[],
    };
    let held_a_plan = apply_transition(&store, late_a.clone(), &held_context)
        .expect("A's held insertion is valid before B replaces it");
    assert_eq!(held_a_plan.before(), &store.snapshot());

    let losing_b = commit_fixture_insertion(&mut store, &clock, &authority, anchor, 2, 102, 0xb1);
    assert_eq!(
        store.metadata.frontiers.header_best, incumbent_a,
        "B is retained while it still loses to A"
    );
    let promoted_b =
        commit_fixture_insertion(&mut store, &clock, &authority, losing_b, 4, 103, 0xb2);
    assert_eq!(
        store.metadata.frontiers.header_best, promoted_b,
        "later local work promotes the exact retained B branch"
    );

    let before_late_a = store.snapshot();
    let before_late_a_digest = retained_digest(&store);
    let context = TransitionContext {
        config: &store.config,
        clock: &clock,
        full_state_authority: Some(&authority),
        startup_capability: None,
        retention_references: &[],
    };
    assert!(matches!(
        apply_transition(&store, late_a, &context),
        Err(TransitionFailure::Stale { .. })
    ));
    assert_eq!(store.snapshot(), before_late_a);
    assert_eq!(retained_digest(&store), before_late_a_digest);

    let next_b = commit_fixture_insertion(&mut store, &clock, &authority, promoted_b, 1, 104, 0xb3);
    assert_eq!(store.metadata.frontiers.header_best, next_b);
    assert_eq!(
        store
            .graph
            .node(next_b.hash)
            .expect("B's next child is retained")
            .parent_hash,
        promoted_b.hash
    );

    let reopened = store.clone();
    assert_eq!(reopened.snapshot(), store.snapshot());
    assert_eq!(retained_digest(&reopened), retained_digest(&store));
    assert_eq!(
        reopened.lease(next_b).parent,
        next_b,
        "the reopened exact B tip remains a valid request anchor"
    );
    assert_exhaustive_oracle(&reopened);
    retained_digest(&reopened)
}

fn assert_fixed_anchor_boundaries() -> [u8; 32] {
    let mut hasher = Sha256::new();
    for (fixture, incumbent_depth) in [999, 1_000, 1_001].into_iter().enumerate() {
        for competitor_first in [false, true] {
            let mut store = FuzzStore::new(EngineMode::Integrated);
            let clock = ManualClock::new();
            let authority = FuzzAuthority;
            let anchor = store.metadata.frontiers.finalized;
            let incumbent_operation = 200 + fixture.saturating_mul(2);
            let competitor_operation = incumbent_operation.saturating_add(1);
            let (incumbent, competitor) = if competitor_first {
                let competitor = commit_fixture_insertion(
                    &mut store,
                    &clock,
                    &authority,
                    anchor,
                    incumbent_depth + 1,
                    competitor_operation,
                    0xd2,
                );
                let incumbent = commit_fixture_insertion(
                    &mut store,
                    &clock,
                    &authority,
                    anchor,
                    incumbent_depth,
                    incumbent_operation,
                    0xd1,
                );
                (incumbent, competitor)
            } else {
                let incumbent = commit_fixture_insertion(
                    &mut store,
                    &clock,
                    &authority,
                    anchor,
                    incumbent_depth,
                    incumbent_operation,
                    0xd1,
                );
                assert_eq!(store.metadata.frontiers.header_best, incumbent);
                let competitor = commit_fixture_insertion(
                    &mut store,
                    &clock,
                    &authority,
                    anchor,
                    incumbent_depth + 1,
                    competitor_operation,
                    0xd2,
                );
                (incumbent, competitor)
            };
            assert_ne!(incumbent.hash, competitor.hash);
            assert_eq!(
                store.metadata.frontiers.header_best, competitor,
                "fixed-anchor selection ignores replacement depth and arrival order"
            );
            hasher.update(incumbent_depth.to_le_bytes());
            hasher.update([u8::from(competitor_first)]);
            hasher.update(competitor.hash.0);
        }
    }
    hasher.finalize().into()
}

fn commit_fixture_insertion(
    store: &mut FuzzStore,
    clock: &ManualClock,
    authority: &FuzzAuthority,
    parent: Frontier,
    count: u32,
    operation: usize,
    branch: u8,
) -> Frontier {
    let request = store.insertion(parent, count, operation, branch);
    let target = match &request.event {
        crate::TransitionEvent::InsertHeaders(event) => {
            let target = event
                .batch
                .headers()
                .last()
                .expect("the fixture insertion is nonempty");
            Frontier::new(target.height, target.hash)
        }
        _ => unreachable!("the incident fixture constructs only insertions"),
    };
    let context = TransitionContext {
        config: &store.config,
        clock,
        full_state_authority: Some(authority),
        startup_capability: None,
        retention_references: &[],
    };
    let plan =
        apply_transition(store, request, &context).expect("the fixture insertion is admissible");
    store.commit(&plan);
    assert_exhaustive_oracle(store);
    target
}

fn insert_fixture_path(
    store: &mut FuzzStore,
    clock: &ManualClock,
    authority: &FuzzAuthority,
    operation: usize,
    branch: u8,
    count: u32,
) -> Vec<VerifiedHeaderRef> {
    let request = store.insertion(store.metadata.frontiers.finalized, count, operation, branch);
    let path = match &request.event {
        crate::TransitionEvent::InsertHeaders(event) => event
            .batch
            .headers()
            .iter()
            .map(|header| VerifiedHeaderRef {
                height: header.height,
                hash: header.hash,
                header: header.header.clone(),
            })
            .collect(),
        _ => unreachable!("the reset fixture constructs only header insertions"),
    };
    let context = TransitionContext {
        config: &store.config,
        clock,
        full_state_authority: Some(authority),
        startup_capability: None,
        retention_references: &[],
    };
    let plan =
        apply_transition(store, request, &context).expect("the reset fixture branch is admissible");
    store.commit(&plan);
    assert_exhaustive_oracle(store);
    path
}

fn permutation_fixture(
    operations: [(usize, u8, bool); 2],
) -> ([u8; 32], block::Hash, block::Hash, block::Hash) {
    let mut store = FuzzStore::new(EngineMode::Integrated);
    let clock = ManualClock::new();
    let authority = FuzzAuthority;
    let finalized = store.metadata.frontiers.finalized;
    let mut tips = Vec::new();
    for (operation, branch, hard_work) in operations {
        let request = store.insertion_with_validation(
            finalized,
            2,
            operation,
            branch,
            HeaderValidationState::Valid,
            hard_work,
        );
        let target = match &request.event {
            crate::TransitionEvent::InsertHeaders(event) => event.target_tip_hash,
            _ => unreachable!("the permutation fixture constructs only header insertions"),
        };
        let context = TransitionContext {
            config: &store.config,
            clock: &clock,
            full_state_authority: Some(&authority),
            startup_capability: None,
            retention_references: &[],
        };
        let plan = apply_transition(&store, request, &context)
            .expect("both stable permutation branches are admissible");
        store.commit(&plan);
        assert_exhaustive_oracle(&store);
        tips.push(target);
    }
    (
        retained_digest(&store),
        store.metadata.frontiers.header_best.hash,
        tips[0],
        tips[1],
    )
}

fn assert_exhaustive_oracle(store: &FuzzStore) {
    let finalized = store.metadata.frontiers.finalized;
    let mut nodes: Vec<_> = store.graph.nodes().collect();
    nodes.sort_unstable_by_key(|node| (node.height, node.hash.0));
    assert!(!nodes.is_empty(), "the finalized anchor must be retained");

    let mut independently_eligible: HashMap<block::Hash, bool> = HashMap::new();
    let mut suffix_work = HashMap::new();
    let mut indexed_children: HashMap<block::Hash, Vec<block::Hash>> = HashMap::new();
    for node in &nodes {
        assert_eq!(
            store.graph.node(node.hash),
            Some(*node),
            "the primary hash index must return every retained node"
        );
        assert!(
            store
                .graph
                .hashes_at_height(node.height)
                .contains(&node.hash),
            "the height index must contain every retained node"
        );

        if node.hash == finalized.hash {
            assert_eq!(node.height, finalized.height);
            assert_eq!(node.eligibility.inherited_from, None);
            suffix_work.insert(node.hash, SuffixWork::zero());
        } else {
            let parent = store
                .graph
                .node(node.parent_hash)
                .expect("every non-finalized retained node has a retained parent");
            assert_eq!(
                node.height,
                block::Height(parent.height.0.saturating_add(1)),
                "retained height is exactly parent height plus one"
            );
            let parent_eligible = *independently_eligible
                .get(&parent.hash)
                .expect("parents sort before children by height");
            assert_eq!(
                node.eligibility.inherited_from,
                (!parent_eligible).then_some(parent.hash),
                "inherited eligibility is recomputed from the parent"
            );
            let parent_work = *suffix_work
                .get(&parent.hash)
                .expect("parents sort before children by height");
            suffix_work.insert(
                node.hash,
                parent_work
                    .checked_add(node.block_work)
                    .expect("the production transition already rejected work overflow"),
            );
            indexed_children
                .entry(parent.hash)
                .or_default()
                .push(node.hash);
        }

        let eligible = node.validation == HeaderValidationState::Valid
            && node.eligibility.direct_reasons.is_empty()
            && node.eligibility.inherited_from.is_none();
        assert_eq!(
            node.is_eligible(),
            eligible,
            "node eligibility must equal its independently recomputed facts"
        );
        independently_eligible.insert(node.hash, eligible);
    }

    for node in &nodes {
        let mut expected_children = indexed_children.remove(&node.hash).unwrap_or_default();
        expected_children.sort_unstable_by_key(|hash| hash.0);
        assert_eq!(
            store.graph.children(node.hash),
            expected_children,
            "the child index must exactly match retained parent links"
        );
    }

    let expected_header_best = nodes
        .iter()
        .filter(|node| independently_eligible[&node.hash])
        .map(|node| {
            (
                ChainScore::new(suffix_work[&node.hash], node.hash),
                Frontier::new(node.height, node.hash),
            )
        })
        .max_by_key(|(score, _)| *score)
        .expect("the finalized anchor is independently eligible");
    assert_eq!(
        store.metadata.frontiers.header_best, expected_header_best.1,
        "fork choice must equal independent work/hash ordering"
    );
    assert_eq!(
        store.metadata.header_best_score, expected_header_best.0,
        "published work must equal the independently accumulated score"
    );
    assert_eq!(
        store.selected,
        independent_path(store, expected_header_best.1),
        "the selected projection must exactly match parent links"
    );
    assert_eq!(
        store.verified,
        independent_path(store, store.metadata.frontiers.verified_best),
        "the verified projection must exactly match parent links"
    );
    for tip in store.branches.iter().flatten() {
        assert_eq!(
            store.graph.node(tip.hash).map(|node| node.height),
            Some(tip.height),
            "every named branch tip is an exact retained frontier"
        );
    }
}

fn independent_path(store: &FuzzStore, tip: Frontier) -> Vec<Frontier> {
    let finalized = store.metadata.frontiers.finalized;
    let mut current = tip;
    let mut reversed = Vec::new();
    loop {
        reversed.push(current);
        if current == finalized {
            break;
        }
        let node = store
            .graph
            .node(current.hash)
            .expect("published projection members are retained");
        current = Frontier::new(
            block::Height(current.height.0.saturating_sub(1)),
            node.parent_hash,
        );
    }
    reversed.reverse();
    reversed
}

fn retained_digest(store: &FuzzStore) -> [u8; 32] {
    let mut nodes: Vec<_> = store.graph.nodes().collect();
    nodes.sort_unstable_by_key(|node| (node.height, node.hash.0));
    let mut hasher = Sha256::new();
    hasher.update(b"zakura-header-chain-fuzz-retained-v1");
    for node in nodes {
        hasher.update(node.height.0.to_le_bytes());
        hasher.update(node.hash.0);
        hasher.update(node.parent_hash.0);
        hasher.update(node.work_coordinate().cumulative_work().to_big_endian());
        hasher.update(format!("{:?}", node.validation));
        hasher.update(format!("{:?}", node.eligibility));
        hasher.update(format!("{:?}", node.body));
        for delivery in &node.aux_delivery_ids {
            hasher.update(delivery.digest());
        }
    }
    for frontier in &store.selected {
        hasher.update(b"selected");
        hasher.update(frontier.height.0.to_le_bytes());
        hasher.update(frontier.hash.0);
    }
    for frontier in &store.verified {
        hasher.update(b"verified");
        hasher.update(frontier.height.0.to_le_bytes());
        hasher.update(frontier.hash.0);
    }
    for (key, tip) in store.branches.iter().enumerate() {
        hasher.update(b"branch");
        hasher.update([u8::try_from(key).expect("the branch registry has sixteen entries")]);
        if let Some(tip) = tip {
            hasher.update(tip.height.0.to_le_bytes());
            hasher.update(tip.hash.0);
        }
    }
    hasher.finalize().into()
}

fn apply_projection(path: &mut Vec<Frontier>, delta: &ProjectionDelta) {
    if let Some(remove_from) = delta.remove_from {
        path.retain(|frontier| frontier.height < remove_from);
    }
    path.extend(delta.put.iter().copied());
}

fn frontier_hash_at(path: &[Frontier], height: block::Height) -> Option<block::Hash> {
    path.iter()
        .find(|frontier| frontier.height == height)
        .map(|frontier| frontier.hash)
}

fn evidence(operation: usize, branch: u8) -> EvidenceId {
    let mut digest = [branch; 32];
    digest[..8].copy_from_slice(&operation_u64(operation).to_le_bytes());
    EvidenceId::from_digest(digest)
}

fn operator_id(operation: usize, branch: u8) -> OperatorInvalidationId {
    let mut id = [branch; 16];
    id[..8].copy_from_slice(&operation_u64(operation).to_le_bytes());
    OperatorInvalidationId::new(id)
}

fn operation_u64(operation: usize) -> u64 {
    u64::try_from(operation).expect("the 512-operation cap fits in u64")
}

fn assert_generation_delta(
    before: &EngineSnapshot,
    after: &EngineSnapshot,
    selected_path_changed: bool,
    verified_path_changed: bool,
    eligibility_changed: bool,
) {
    let finalized_changed = before.frontiers.finalized != after.frontiers.finalized;
    let header_changed = finalized_changed || eligibility_changed || selected_path_changed;
    let verified_changed = finalized_changed || verified_path_changed;
    assert_eq!(
        before.header_generation != after.header_generation,
        header_changed,
        "header generation must change exactly with its owned frontiers"
    );
    assert_eq!(
        before.verified_generation != after.verified_generation,
        verified_changed,
        "verified generation must change exactly with the verified path"
    );
}

fn append_snapshot(hasher: &mut Sha256, snapshot: &EngineSnapshot) {
    hasher.update(snapshot.state_version.get().to_le_bytes());
    hasher.update(snapshot.header_generation.get().to_le_bytes());
    hasher.update(snapshot.verified_generation.get().to_le_bytes());
    hasher.update(snapshot.frontiers.finalized.height.0.to_le_bytes());
    hasher.update(snapshot.frontiers.finalized.hash.0);
    hasher.update(snapshot.frontiers.header_best.height.0.to_le_bytes());
    hasher.update(snapshot.frontiers.header_best.hash.0);
    hasher.update(snapshot.frontiers.verified_best.height.0.to_le_bytes());
    hasher.update(snapshot.frontiers.verified_best.hash.0);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn structured_replay_is_bounded_and_deterministic() {
        let bytes: Vec<_> = (0..600)
            .map(|index| u8::try_from(index % 256).expect("the modulo value fits in u8"))
            .collect();
        let first = replay_fork_transition_bytes(&bytes);
        let second = replay_fork_transition_bytes(&bytes);
        assert_eq!(first, second);
        assert_eq!(first.operations, 512);
        assert!(first.commits > 0);
        assert!(first.refused > 0);
    }

    #[test]
    fn candidate_pressure_evicts_exactly_the_lowest_tip_at_the_cap() {
        let first = replay_fork_transition_bytes(b"x");
        let second = replay_fork_transition_bytes(b"x");
        assert_eq!(first, second);
        assert_eq!(first.pressure_checks, 1);
        assert_eq!(first.no_effects, 1);
    }

    #[test]
    fn named_losing_branch_can_be_extended_past_the_incumbent() {
        let incumbent = replay_fork_transition_bytes(&[4]);
        assert_eq!(
            incumbent.snapshot.frontiers.header_best.height,
            block::Height(5)
        );

        let losing_fork = replay_fork_transition_bytes(&[4, 9]);
        assert_eq!(
            losing_fork.snapshot.frontiers.header_best, incumbent.snapshot.frontiers.header_best,
            "the shorter named fork is retained without replacing the incumbent"
        );

        let promoted = replay_fork_transition_bytes(&[4, 9, 1, 1]);
        assert_eq!(
            promoted.snapshot.frontiers.header_best.height,
            block::Height(6),
            "later work extends the exact named losing branch past the incumbent"
        );
        assert_ne!(
            promoted.snapshot.frontiers.header_best.hash,
            incumbent.snapshot.frontiers.header_best.hash
        );
    }

    #[test]
    fn shorter_higher_work_branch_replaces_a_taller_incumbent() {
        let incumbent = replay_fork_transition_bytes(&[4]);
        let replacement = replay_fork_transition_bytes(b"AD");
        assert_eq!(
            replacement.snapshot.frontiers.header_best.height,
            block::Height(2)
        );
        assert!(
            replacement.snapshot.header_best_score.suffix_work
                > incumbent.snapshot.header_best_score.suffix_work,
            "selection follows locally computed cumulative work rather than height"
        );
        assert_ne!(
            replacement.snapshot.frontiers.header_best.hash,
            incumbent.snapshot.frontiers.header_best.hash
        );
    }

    #[test]
    fn same_height_forks_are_insertion_order_independent() {
        let first = replay_fork_transition_bytes(b"T");
        let second = replay_fork_transition_bytes(b"T");
        assert_eq!(first, second);
        assert_eq!(first.permutation_checks, 1);
        assert_eq!(first.no_effects, 1);
    }

    #[test]
    fn consecutive_resets_use_exact_branch_identity() {
        let first = replay_fork_transition_bytes(b"R");
        let second = replay_fork_transition_bytes(b"R");
        assert_eq!(first, second);
        assert_eq!(first.reset_checks, 1);
        assert_eq!(first.no_effects, 1);
    }

    #[test]
    fn aud_incident_late_a_completion_cannot_break_promoted_b() {
        let first = replay_fork_transition_bytes(b"I");
        let second = replay_fork_transition_bytes(b"I");
        assert_eq!(first, second);
        assert_eq!(first.incident_checks, 1);
        assert_eq!(first.no_effects, 1);
    }

    #[test]
    fn fixed_anchor_boundary_matrix_replays_through_transitions() {
        let first = replay_fork_transition_bytes(b"F");
        let second = replay_fork_transition_bytes(b"F");
        assert_eq!(first, second);
        assert_eq!(first.boundary_checks, 1);
        assert_eq!(first.no_effects, 1);
    }

    #[test]
    #[should_panic(expected = "selected projection must exactly match parent links")]
    fn exhaustive_oracle_rejects_a_projection_gap() {
        let mut store = FuzzStore::new(EngineMode::Integrated);
        store.selected.clear();

        assert_exhaustive_oracle(&store);
    }

    #[test]
    fn body_and_deferred_operations_have_their_expected_selection_effects() {
        let body_invalid = replay_fork_transition_bytes(&[10, 48]);
        assert_eq!(body_invalid.commits, 2);
        assert_eq!(
            body_invalid.snapshot.frontiers.header_best.height,
            block::Height(0)
        );

        let body_unavailable = replay_fork_transition_bytes(&[10, 56]);
        assert_eq!(body_unavailable.commits, 2);
        assert_eq!(
            body_unavailable.snapshot.frontiers.header_best.height,
            block::Height(3),
            "transient body availability must not change header eligibility"
        );
        let before_mismatch = replay_fork_transition_bytes(&[10]);
        let body_mismatch = replay_fork_transition_bytes(&[10, 112]);
        assert_eq!(body_mismatch.commits, 1);
        assert_eq!(body_mismatch.no_effects, 1);
        assert_eq!(body_mismatch.snapshot, before_mismatch.snapshot);
        assert_eq!(
            body_mismatch.retained_digest,
            before_mismatch.retained_digest
        );

        let deferred = replay_fork_transition_bytes(&[80]);
        assert_eq!(
            deferred.snapshot.frontiers.header_best.height,
            block::Height(0)
        );
        let admitted = replay_fork_transition_bytes(&[80, 88, 72]);
        assert_eq!(
            admitted.snapshot.frontiers.header_best.height,
            block::Height(1)
        );

        let verified = replay_fork_transition_bytes(&[10, 96]);
        assert_eq!(
            verified.snapshot.frontiers.verified_best.height,
            block::Height(3)
        );
        let contradictory = replay_fork_transition_bytes(&[10, 96, 48]);
        assert_eq!(contradictory.commits, 2);
        assert_eq!(contradictory.refused, 1);
        assert_eq!(
            contradictory.snapshot.frontiers.verified_best,
            verified.snapshot.frontiers.verified_best
        );
        let finalized = replay_fork_transition_bytes(&[10, 96, 104]);
        assert_eq!(
            finalized.snapshot.frontiers.finalized.height,
            block::Height(3)
        );
    }

    #[test]
    fn fork_transition_regression_corpus_replays_green() {
        let corpus: &[(&str, &[u8])] = &[
            (
                "linear_growth",
                include_bytes!("../../fuzz/header-chain/corpus/fork_transitions/linear_growth"),
            ),
            (
                "fork_replacement",
                include_bytes!("../../fuzz/header-chain/corpus/fork_transitions/fork_replacement"),
            ),
            (
                "stale_completion",
                include_bytes!("../../fuzz/header-chain/corpus/fork_transitions/stale_completion"),
            ),
            (
                "operator_cycle",
                include_bytes!("../../fuzz/header-chain/corpus/fork_transitions/operator_cycle"),
            ),
            (
                "crash_reopen",
                include_bytes!("../../fuzz/header-chain/corpus/fork_transitions/crash_reopen"),
            ),
            (
                "body_invalid",
                include_bytes!("../../fuzz/header-chain/corpus/fork_transitions/body_invalid"),
            ),
            (
                "body_unavailable",
                include_bytes!("../../fuzz/header-chain/corpus/fork_transitions/body_unavailable"),
            ),
            (
                "deferred_reevaluation",
                include_bytes!(
                    "../../fuzz/header-chain/corpus/fork_transitions/deferred_reevaluation"
                ),
            ),
            (
                "verified_finality",
                include_bytes!("../../fuzz/header-chain/corpus/fork_transitions/verified_finality"),
            ),
            (
                "body_mismatch",
                include_bytes!("../../fuzz/header-chain/corpus/fork_transitions/body_mismatch"),
            ),
            (
                "evict_pressure",
                include_bytes!("../../fuzz/header-chain/corpus/fork_transitions/evict_pressure"),
            ),
            (
                "aud_01_losing_branch_promotion",
                include_bytes!(
                    "../../fuzz/header-chain/corpus/fork_transitions/aud_01_losing_branch_promotion"
                ),
            ),
            (
                "aud_02_shorter_higher_work",
                include_bytes!(
                    "../../fuzz/header-chain/corpus/fork_transitions/aud_02_shorter_higher_work"
                ),
            ),
            (
                "aud_03_same_height_permutations",
                include_bytes!(
                    "../../fuzz/header-chain/corpus/fork_transitions/aud_03_same_height_permutations"
                ),
            ),
            (
                "aud_04_consecutive_resets",
                include_bytes!(
                    "../../fuzz/header-chain/corpus/fork_transitions/aud_04_consecutive_resets"
                ),
            ),
            (
                "aud_incident_late_a_after_b_promotion",
                include_bytes!(
                    "../../fuzz/header-chain/corpus/fork_transitions/aud_incident_late_a_after_b_promotion"
                ),
            ),
            (
                "fixed_anchor_999_1000_1001",
                include_bytes!(
                    "../../fuzz/header-chain/corpus/fork_transitions/fixed_anchor_999_1000_1001"
                ),
            ),
        ];
        for (name, bytes) in corpus {
            let first = replay_fork_transition_bytes(bytes);
            let second = replay_fork_transition_bytes(bytes);
            assert_eq!(first, second, "{name} must replay deterministically");
            if *name == "aud_01_losing_branch_promotion" {
                assert_eq!(
                    first.snapshot.frontiers.header_best.height,
                    block::Height(6)
                );
            }
            if *name == "aud_02_shorter_higher_work" {
                assert_eq!(
                    first.snapshot.frontiers.header_best.height,
                    block::Height(2)
                );
            }
            if *name == "aud_03_same_height_permutations" {
                assert_eq!(first.permutation_checks, 1);
            }
            if *name == "aud_04_consecutive_resets" {
                assert_eq!(first.reset_checks, 1);
            }
            if *name == "aud_incident_late_a_after_b_promotion" {
                assert_eq!(first.incident_checks, 1);
            }
            if *name == "fixed_anchor_999_1000_1001" {
                assert_eq!(first.boundary_checks, 1);
            }
        }
    }
}
