//! Bounded protocol-model replay used by the header-pursuit fuzz target.

use std::{collections::HashMap, num::NonZeroU64, sync::Arc};

use tokio::time::Instant;
use zakura_chain::{
    block::{self, genesis::regtest_genesis_block},
    work::difficulty::U256,
};
use zakura_header_chain::{
    AlarmSet, BranchId, ChainScore, CompletionDecision, CompletionGate, EngineMode, EngineSnapshot,
    Frontier, FrontierSet, HeaderGeneration, HeaderLocator, PendingOwners, RetiredWork, SourceId,
    StateVersion, SuffixWork, VerifiedGeneration, WorkOwner, WorkScope, MAX_STAGED_TARGETS_V1,
};

use super::{
    scheduler::peer_work::{HeaderTargetPhase, PeerWorkPriority, PeerWorkQueue, QueueWorkResult},
    ActiveHeaderRequest, AdvertisedHeaderTarget, AuxSchema, HeaderSyncRequestId, Status,
    ZakuraPeerId,
};

const MAX_INPUT_BYTES: usize = 512;
const LOGICAL_PEERS: u8 = 20;

/// Sentinel counters for effects forbidden after completion ownership becomes stale.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NoEffectsProbe {
    /// Durable frontier transition attempts.
    pub frontier_transitions: usize,
    /// Forward-coverage mutations.
    pub coverage_updates: usize,
    /// Body-retry mutations.
    pub retry_updates: usize,
    /// VCT-repair mutations.
    pub repair_updates: usize,
    /// Scheduler mutations.
    pub scheduler_updates: usize,
    /// Committed snapshot publications.
    pub publications: usize,
    /// Body-task mutations.
    pub body_task_updates: usize,
    /// Peer attribution mutations.
    pub peer_attributions: usize,
    /// Peer score mutations.
    pub peer_scores: usize,
}

/// Stable counters produced by one pursuit replay.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HeaderPursuitReplaySummary {
    /// Number of complete-target submissions admitted by the ownership gate.
    pub state_submissions: usize,
    /// Number of explicit non-data outcomes retired without punishment.
    pub explicit_outcomes: usize,
    /// Number of stale or mismatched operations with no state effect.
    pub refused_operations: usize,
    /// Number of peer punishments, which must remain zero for modeled outcomes.
    pub peer_punishments: usize,
    /// Prepared completions retained across later operations.
    pub held_completions: usize,
    /// Held completions released through the centralized ownership gate.
    pub released_completions: usize,
    /// Released completions rejected after retirement or scope change.
    pub stale_releases: usize,
    /// Unauthenticated status mutations exercised without changing local authority.
    pub advisory_mutations: usize,
    /// State admission results retained after their write was dispatched.
    pub held_state_results: usize,
    /// Held state admission results released through the ownership gate.
    pub released_state_results: usize,
    /// Released state results rejected after retirement or scope change.
    pub stale_state_results: usize,
    /// Held state results representing local admission failure.
    pub held_state_failures: usize,
    /// Released state results representing local admission failure.
    pub released_state_failures: usize,
    /// Equivalent response-page partition matrices completed.
    pub partition_checks: usize,
    /// Downstream effects admitted by current completions; stale scenarios require all zero.
    pub completion_effects: NoEffectsProbe,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ModelWork {
    Awaiting {
        session_id: u64,
        target: block::Hash,
        scope: WorkScope,
        priority: PeerWorkPriority,
    },
    Active {
        session_id: u64,
        target: block::Hash,
        owner: WorkOwner,
        phase: HeaderTargetPhase,
        ancestor_bound: bool,
    },
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct HeldCompletion {
    peer_key: u8,
    owner: WorkOwner,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum HeldStateResult {
    Applied(HeldCompletion),
    LocalFailure(HeldCompletion),
}

struct PursuitHarness {
    queue: PeerWorkQueue,
    model: HashMap<u8, ModelWork>,
    held: HashMap<u8, HeldCompletion>,
    held_state: HashMap<u8, HeldStateResult>,
    pending: PendingOwners,
    snapshot: EngineSnapshot,
    observed_at: Instant,
    summary: HeaderPursuitReplaySummary,
}

impl PursuitHarness {
    fn new() -> Self {
        let anchor = frontier(10);
        Self {
            queue: PeerWorkQueue::default(),
            model: HashMap::new(),
            held: HashMap::new(),
            held_state: HashMap::new(),
            pending: PendingOwners::default(),
            snapshot: EngineSnapshot {
                mode: EngineMode::Integrated,
                state_version: StateVersion::new(1),
                header_generation: HeaderGeneration::new(1),
                verified_generation: VerifiedGeneration::new(1),
                frontiers: FrontierSet {
                    finalized: anchor,
                    header_best: anchor,
                    verified_best: anchor,
                },
                header_best_score: ChainScore::new(SuffixWork::new(U256::zero()), anchor.hash),
                oldest_retained_height: anchor.height,
                alarms: AlarmSet::default(),
            },
            observed_at: Instant::now(),
            summary: HeaderPursuitReplaySummary::default(),
        }
    }

    fn apply(&mut self, bytes: &[u8]) {
        let opcode = decode_opcode(bytes[0]);
        let peer_key = bytes[1] % LOGICAL_PEERS;
        let marker = bytes[2];
        let flags = bytes[3];
        match opcode {
            0 => self.advertise(peer_key, marker, flags),
            1 => self.start(peer_key, marker, flags),
            2 => self.deliver_page(peer_key, marker, flags),
            3 => self.prepare(peer_key, marker, flags),
            4 => self.complete(peer_key, marker, flags),
            5 => self.explicit_outcome(peer_key, marker, flags),
            6 => self.disconnect(peer_key),
            7 => self.advance_generation(flags),
            8 => self.hold_completion(peer_key, marker, flags),
            9 => self.release_completion(marker),
            10 => self.corrupt_advisory(peer_key, marker, flags),
            11 => self.reset_branch(marker),
            12 => self.dispatch_state(peer_key, marker, flags),
            13 => self.hold_state_result(peer_key, marker, flags),
            14 => self.release_state_result(marker),
            15 => self.hold_state_failure(peer_key, marker, flags),
            16 => self.check_page_partitions(),
            _ => unreachable!("the opcode is reduced modulo seventeen"),
        }
        self.assert_matches_model();
        assert_eq!(
            self.summary.peer_punishments, 0,
            "explicit outcomes and stale work are nonpunitive"
        );
    }

    fn advertise(&mut self, peer_key: u8, marker: u8, flags: u8) {
        let session_id = session(flags);
        let target = target(marker);
        let priority = priority(flags);
        let scope = WorkScope::for_header_target(&self.snapshot, target);
        let actual = self.queue.stage(
            peer(peer_key),
            advertisement(&self.snapshot, self.observed_at, session_id, marker),
            priority,
        );
        let expected = self.model_stage(peer_key, session_id, target, scope, priority);
        assert_eq!(actual, expected, "queue admission diverged from the model");
    }

    fn model_stage(
        &mut self,
        peer_key: u8,
        session_id: u64,
        target: block::Hash,
        scope: WorkScope,
        priority: PeerWorkPriority,
    ) -> QueueWorkResult {
        if let Some(work) = self.model.get_mut(&peer_key) {
            return match work {
                ModelWork::Awaiting {
                    session_id: current_session,
                    target: current_target,
                    scope: current_scope,
                    priority: current_priority,
                } => {
                    *current_session = session_id;
                    *current_target = target;
                    *current_scope = scope;
                    *current_priority = priority;
                    QueueWorkResult::NeedsLocator
                }
                ModelWork::Active { .. } => QueueWorkResult::AlreadyActive,
            };
        }
        if self.model.len() >= MAX_STAGED_TARGETS_V1 {
            let replace = self
                .model
                .iter()
                .filter_map(|(candidate, work)| match work {
                    ModelWork::Awaiting {
                        priority: current, ..
                    } if *current < priority => Some((*candidate, *current)),
                    ModelWork::Awaiting { .. } | ModelWork::Active { .. } => None,
                })
                .min_by(|(left_peer, left_priority), (right_peer, right_priority)| {
                    left_priority
                        .cmp(right_priority)
                        .then_with(|| left_peer.cmp(right_peer))
                })
                .map(|(candidate, _)| candidate);
            let Some(replace) = replace else {
                return QueueWorkResult::AtCapacity;
            };
            self.model.remove(&replace);
        }
        self.model.insert(
            peer_key,
            ModelWork::Awaiting {
                session_id,
                target,
                scope,
                priority,
            },
        );
        QueueWorkResult::NeedsLocator
    }

    fn start(&mut self, peer_key: u8, marker: u8, flags: u8) {
        let supplied_session = session(flags);
        let supplied_target = target(marker);
        let supplied_scope = WorkScope::for_header_target(&self.snapshot, supplied_target);
        let model_match = matches!(
            self.model.get(&peer_key),
            Some(ModelWork::Awaiting {
                session_id,
                target,
                scope,
                ..
            }) if *session_id == supplied_session
                && *target == supplied_target
                && *scope == supplied_scope
        );
        let request = request(
            &self.snapshot,
            self.observed_at,
            peer_key,
            supplied_session,
            marker,
        );
        let owner = request.owner;
        let started = self.queue.start(request);
        assert_eq!(started, model_match, "request admission changed its target");
        if started {
            assert_eq!(
                self.pending.insert(source(peer_key), owner),
                None,
                "one peer has at most one active request owner"
            );
            self.model.insert(
                peer_key,
                ModelWork::Active {
                    session_id: supplied_session,
                    target: supplied_target,
                    owner,
                    phase: HeaderTargetPhase::Receiving,
                    ancestor_bound: false,
                },
            );
        } else {
            self.summary.refused_operations += 1;
        }
    }

    fn deliver_page(&mut self, peer_key: u8, marker: u8, flags: u8) {
        let supplied_session = session(flags);
        let supplied_target = target(marker);
        let returned_ancestor = if flags & 0x20 == 0 {
            self.snapshot.frontiers.finalized
        } else {
            Frontier::new(
                block::Height(self.snapshot.frontiers.finalized.height.0.saturating_add(1)),
                self.snapshot.frontiers.finalized.hash,
            )
        };
        let production_accepts = self.queue.active(&peer(peer_key)).is_some_and(|request| {
            request.target.session_id == supplied_session
                && request.matches_response_page(supplied_target, returned_ancestor)
        });
        let matches = matches!(
            self.model.get(&peer_key),
            Some(ModelWork::Active {
                session_id,
                target,
                phase: HeaderTargetPhase::Receiving,
                ..
            }) if *session_id == supplied_session
                && *target == supplied_target
                && flags & 0x20 == 0
        );
        assert_eq!(
            production_accepts, matches,
            "production page-shape admission diverged from the model"
        );
        if !matches {
            self.summary.refused_operations += 1;
            return;
        }
        let request = self
            .queue
            .active_mut(&peer(peer_key))
            .expect("the production queue is active when the model is active");
        assert_eq!(
            request.target.status.selected_tip_hash, supplied_target,
            "a response page cannot substitute another target"
        );
        request.common_ancestor = Some(returned_ancestor);
        let Some(ModelWork::Active { ancestor_bound, .. }) = self.model.get_mut(&peer_key) else {
            unreachable!("the operation matched an active model request");
        };
        *ancestor_bound = true;
    }

    fn prepare(&mut self, peer_key: u8, marker: u8, flags: u8) {
        let supplied_session = session(flags);
        let supplied_target = target(marker);
        let matches = matches!(
            self.model.get(&peer_key),
            Some(ModelWork::Active {
                session_id,
                target,
                phase: HeaderTargetPhase::Receiving,
                ancestor_bound: true,
                ..
            }) if *session_id == supplied_session && *target == supplied_target
        );
        if !matches {
            self.summary.refused_operations += 1;
            return;
        }
        self.queue
            .active_mut(&peer(peer_key))
            .expect("the production queue is active when the model is active")
            .phase = HeaderTargetPhase::Preparing;
        let Some(ModelWork::Active { phase, .. }) = self.model.get_mut(&peer_key) else {
            unreachable!("the operation matched an active model request");
        };
        *phase = HeaderTargetPhase::Preparing;
    }

    fn complete(&mut self, peer_key: u8, marker: u8, flags: u8) {
        let supplied_session = session(flags);
        let supplied_target = target(marker);
        let Some(ModelWork::Active {
            session_id,
            target,
            owner,
            phase: HeaderTargetPhase::Preparing,
            ancestor_bound: true,
        }) = self.model.get(&peer_key).cloned()
        else {
            self.summary.refused_operations += 1;
            return;
        };
        if session_id != supplied_session || target != supplied_target {
            self.summary.refused_operations += 1;
            return;
        }
        let decision =
            CompletionGate::check(&self.snapshot, &self.pending, source(peer_key), &owner);
        if decision == CompletionDecision::Current {
            self.summary.state_submissions += 1;
            self.record_completion_effects();
        } else {
            self.summary.refused_operations += 1;
        }
        self.retire(peer_key, owner);
    }

    fn explicit_outcome(&mut self, peer_key: u8, marker: u8, flags: u8) {
        let supplied_session = session(flags);
        let supplied_target = target(marker);
        let Some(ModelWork::Active {
            session_id,
            target,
            owner,
            ..
        }) = self.model.get(&peer_key).cloned()
        else {
            self.summary.refused_operations += 1;
            return;
        };
        if session_id != supplied_session || target != supplied_target {
            self.summary.refused_operations += 1;
            return;
        }
        let request_id = HeaderSyncRequestId::new(owner.request_id.get())
            .expect("work owners always contain nonzero request identifiers");
        let request = self
            .queue
            .active(&peer(peer_key))
            .expect("the model and production queue have the same active request");
        assert!(
            request.target.session_id == supplied_session
                && request.accepts_outcome(request_id, supplied_target),
            "production outcome admission diverged from the model"
        );
        self.summary.explicit_outcomes += 1;
        self.retire(peer_key, owner);
    }

    fn disconnect(&mut self, peer_key: u8) {
        if let Some(ModelWork::Active { owner, .. }) = self.model.get(&peer_key).cloned() {
            self.pending.remove(source(peer_key), owner.request_id);
        }
        self.queue.remove(&peer(peer_key));
        self.queue.remove_unstarted(&peer(peer_key));
        self.model.remove(&peer_key);
    }

    fn hold_completion(&mut self, peer_key: u8, marker: u8, flags: u8) {
        let supplied_session = session(flags);
        let supplied_target = target(marker);
        let slot = (flags >> 2).saturating_add(32);
        let Some(ModelWork::Active {
            session_id,
            target,
            owner,
            phase: HeaderTargetPhase::Preparing,
            ancestor_bound: true,
        }) = self.model.get(&peer_key).cloned()
        else {
            self.summary.refused_operations += 1;
            return;
        };
        if session_id != supplied_session
            || target != supplied_target
            || self.held.contains_key(&slot)
            || self
                .held
                .values()
                .any(|completion| completion.owner == owner)
        {
            self.summary.refused_operations += 1;
            return;
        }
        self.held.insert(slot, HeldCompletion { peer_key, owner });
        self.summary.held_completions += 1;
    }

    fn release_completion(&mut self, marker: u8) {
        let slot = marker % 64;
        let Some(completion) = self.held.remove(&slot) else {
            self.summary.refused_operations += 1;
            return;
        };
        self.summary.released_completions += 1;
        let decision = CompletionGate::check(
            &self.snapshot,
            &self.pending,
            source(completion.peer_key),
            &completion.owner,
        );
        if decision == CompletionDecision::Current {
            self.summary.state_submissions += 1;
            self.record_completion_effects();
        } else {
            self.summary.refused_operations += 1;
            self.summary.stale_releases += 1;
        }
        self.retire_if_exact(completion);
    }

    fn corrupt_advisory(&mut self, peer_key: u8, marker: u8, flags: u8) {
        let before = self.snapshot.clone();
        let session_id = session(flags);
        let mut advertised = advertisement(&self.snapshot, self.observed_at, session_id, marker);
        match (flags >> 2) % 4 {
            0 => {
                advertised.status.work_anchor_hash = hash(marker.wrapping_add(1));
                advertised.status.suffix_cumulative_work = U256::MAX;
            }
            1 => advertised.status.selected_tip_height = block::Height(u32::MAX),
            2 => advertised.status.oldest_retained_height = block::Height(u32::MAX),
            3 => advertised.status.max_headers_per_response = 0,
            _ => unreachable!("the advisory mutation is reduced modulo four"),
        }
        let target = advertised.status.selected_tip_hash;
        let scope = advertised.scope;
        if advertised.is_discovery_eligible(&self.snapshot) {
            let priority =
                PeerWorkPriority::from_work_order(advertised.claimed_work_order(&self.snapshot));
            let actual = self.queue.stage(peer(peer_key), advertised, priority);
            let expected = self.model_stage(peer_key, session_id, target, scope, priority);
            assert_eq!(
                actual, expected,
                "advisory mutation changed queue semantics"
            );
        } else {
            self.queue.remove_unstarted(&peer(peer_key));
            if matches!(self.model.get(&peer_key), Some(ModelWork::Awaiting { .. })) {
                self.model.remove(&peer_key);
            }
        }
        assert_eq!(
            self.snapshot, before,
            "unauthenticated advisory fields cannot mutate local authority"
        );
        self.summary.advisory_mutations += 1;
    }

    fn advance_generation(&mut self, flags: u8) {
        let previous = self.snapshot.clone();
        if flags & 1 == 0 {
            self.snapshot.state_version = self
                .snapshot
                .state_version
                .checked_next()
                .expect("at most 128 fuzz operations cannot exhaust state versions");
        } else {
            self.snapshot.header_generation = self
                .snapshot
                .header_generation
                .checked_next()
                .expect("at most 128 fuzz operations cannot exhaust header generations");
        }
        let retired = self.pending.apply_retirement(
            &RetiredWork {
                header_generation_changed: previous.header_generation
                    != self.snapshot.header_generation,
                verified_generation_changed: false,
                owners: Vec::new(),
            },
            &self.snapshot,
        );
        for owner in retired {
            assert!(
                self.queue.remove_owner(owner).is_some(),
                "retired pending owners have exact queue work"
            );
            self.model.retain(
                |_, work| !matches!(work, ModelWork::Active { owner: active, .. } if *active == owner),
            );
        }
    }

    fn reset_branch(&mut self, marker: u8) {
        let previous = self.snapshot.clone();
        let replacement = Frontier::new(
            block::Height(u32::from(marker).max(previous.frontiers.finalized.height.0)),
            hash(marker),
        );
        self.snapshot.state_version = self
            .snapshot
            .state_version
            .checked_next()
            .expect("at most 128 fuzz operations cannot exhaust state versions");
        self.snapshot.header_generation = self
            .snapshot
            .header_generation
            .checked_next()
            .expect("at most 128 fuzz operations cannot exhaust header generations");
        self.snapshot.frontiers.header_best = replacement;
        self.snapshot.header_best_score =
            ChainScore::new(SuffixWork::new(U256::from(marker)), replacement.hash);
        let retired = self.pending.apply_retirement(
            &RetiredWork {
                header_generation_changed: true,
                verified_generation_changed: false,
                owners: Vec::new(),
            },
            &self.snapshot,
        );
        for owner in retired {
            assert!(
                self.queue.remove_owner(owner).is_some(),
                "branch-reset retirement removes exact queue work"
            );
            self.model.retain(
                |_, work| !matches!(work, ModelWork::Active { owner: active, .. } if *active == owner),
            );
        }
    }

    fn dispatch_state(&mut self, peer_key: u8, marker: u8, flags: u8) {
        let supplied_session = session(flags);
        let supplied_target = target(marker);
        let Some(ModelWork::Active {
            session_id,
            target,
            owner,
            phase: HeaderTargetPhase::Preparing,
            ancestor_bound: true,
        }) = self.model.get(&peer_key).cloned()
        else {
            self.summary.refused_operations += 1;
            return;
        };
        if session_id != supplied_session
            || target != supplied_target
            || self
                .held
                .values()
                .any(|completion| completion.owner == owner)
            || CompletionGate::check(&self.snapshot, &self.pending, source(peer_key), &owner)
                != CompletionDecision::Current
        {
            self.summary.refused_operations += 1;
            return;
        }
        self.queue
            .active_mut(&peer(peer_key))
            .expect("the current model request has exact production queue work")
            .phase = HeaderTargetPhase::Applying;
        let Some(ModelWork::Active { phase, .. }) = self.model.get_mut(&peer_key) else {
            unreachable!("the state dispatch matched an active model request");
        };
        *phase = HeaderTargetPhase::Applying;
        self.summary.state_submissions += 1;
    }

    fn hold_state_result(&mut self, peer_key: u8, marker: u8, flags: u8) {
        self.hold_state_completion(peer_key, marker, flags, false);
    }

    fn hold_state_failure(&mut self, peer_key: u8, marker: u8, flags: u8) {
        self.hold_state_completion(peer_key, marker, flags, true);
    }

    fn hold_state_completion(&mut self, peer_key: u8, marker: u8, flags: u8, failure: bool) {
        let supplied_session = session(flags);
        let supplied_target = target(marker);
        let slot = (flags >> 2).saturating_add(32);
        let Some(ModelWork::Active {
            session_id,
            target,
            owner,
            phase: HeaderTargetPhase::Applying,
            ..
        }) = self.model.get(&peer_key).cloned()
        else {
            self.summary.refused_operations += 1;
            return;
        };
        if session_id != supplied_session
            || target != supplied_target
            || self.held_state.contains_key(&slot)
            || self.held_state.values().any(|result| match result {
                HeldStateResult::Applied(completion)
                | HeldStateResult::LocalFailure(completion) => completion.owner == owner,
            })
        {
            self.summary.refused_operations += 1;
            return;
        }
        let completion = HeldCompletion { peer_key, owner };
        let result = if failure {
            self.summary.held_state_failures += 1;
            HeldStateResult::LocalFailure(completion)
        } else {
            HeldStateResult::Applied(completion)
        };
        self.held_state.insert(slot, result);
        self.summary.held_state_results += 1;
    }

    fn release_state_result(&mut self, marker: u8) {
        let slot = marker % 64;
        let Some(result) = self.held_state.remove(&slot) else {
            self.summary.refused_operations += 1;
            return;
        };
        let (completion, applied) = match result {
            HeldStateResult::Applied(completion) => (completion, true),
            HeldStateResult::LocalFailure(completion) => {
                self.summary.released_state_failures += 1;
                (completion, false)
            }
        };
        self.summary.released_state_results += 1;
        let decision = CompletionGate::check(
            &self.snapshot,
            &self.pending,
            source(completion.peer_key),
            &completion.owner,
        );
        if decision == CompletionDecision::Current && applied {
            self.record_completion_effects();
        } else if decision != CompletionDecision::Current {
            self.summary.refused_operations += 1;
            self.summary.stale_state_results += 1;
        }
        self.retire_if_exact(completion);
    }

    fn record_completion_effects(&mut self) {
        self.summary.completion_effects.frontier_transitions += 1;
        self.summary.completion_effects.coverage_updates += 1;
        self.summary.completion_effects.retry_updates += 1;
        self.summary.completion_effects.repair_updates += 1;
        self.summary.completion_effects.scheduler_updates += 1;
        self.summary.completion_effects.publications += 1;
        self.summary.completion_effects.body_task_updates += 1;
        self.summary.completion_effects.peer_attributions += 1;
        self.summary.completion_effects.peer_scores += 1;
    }

    fn check_page_partitions(&mut self) {
        let anchor = self.snapshot.frontiers.header_best;
        let mut parent_hash = anchor.hash;
        let mut entries = Vec::new();
        for index in 0..4_u8 {
            let mut header = *regtest_genesis_block().header;
            header.previous_block_hash = parent_hash;
            header.nonce.0[0] = index.saturating_add(1);
            let header = Arc::new(header);
            parent_hash = header.hash();
            entries.push(super::HeaderEntry {
                header,
                body_size: 0,
                tree_aux: None,
            });
        }
        let target = parent_hash;
        let mut template = request(&self.snapshot, self.observed_at, 0, 1, 42);
        template.target.status.selected_tip_height =
            block::Height(anchor.height.0.saturating_add(4));
        template.target.status.selected_tip_hash = target;
        template.owner.branch.target_tip_hash = target;

        let mut canonical = None;
        for partition in [vec![4], vec![1, 3], vec![2, 2], vec![1, 1, 2]] {
            let mut active = template.clone();
            let mut offset = 0usize;
            for count in partition {
                let returned_ancestor = active.staged_tip().unwrap_or(anchor);
                assert!(
                    active.matches_response_page(target, returned_ancestor),
                    "each continuation page preserves exact staged ancestry"
                );
                active.common_ancestor.get_or_insert(returned_ancestor);
                active.entries.extend(
                    entries[offset..offset.saturating_add(count)]
                        .iter()
                        .cloned(),
                );
                offset = offset.saturating_add(count);
            }
            assert_eq!(offset, entries.len());
            assert_eq!(
                active.staged_tip(),
                Some(Frontier::new(
                    block::Height(anchor.height.0.saturating_add(4)),
                    target,
                ))
            );
            active.phase = HeaderTargetPhase::Preparing;
            let staged_tip = active.staged_tip();
            let projection = (
                active.common_ancestor,
                active.entries,
                staged_tip,
                active.phase,
            );
            match canonical.as_ref() {
                Some(expected) => assert_eq!(
                    &projection, expected,
                    "page boundaries cannot change complete-target admission"
                ),
                None => canonical = Some(projection),
            }
        }
        self.summary.partition_checks += 1;
    }

    fn retire(&mut self, peer_key: u8, owner: WorkOwner) {
        assert_eq!(
            self.pending.remove(source(peer_key), owner.request_id),
            Some(owner),
            "retirement removes the exact pending owner"
        );
        let retired = self
            .queue
            .remove_owner(owner)
            .expect("the queue contains the exact active owner");
        assert_eq!(retired.owner, owner);
        self.model.remove(&peer_key);
    }

    fn retire_if_exact(&mut self, completion: HeldCompletion) {
        let Some(retired) = self.queue.remove_owner(completion.owner) else {
            return;
        };
        assert_eq!(retired.owner, completion.owner);
        assert_eq!(
            self.pending
                .remove(source(completion.peer_key), completion.owner.request_id),
            Some(completion.owner),
            "an exact held owner retires only its own pending entry"
        );
        if matches!(
            self.model.get(&completion.peer_key),
            Some(ModelWork::Active { owner, .. }) if *owner == completion.owner
        ) {
            self.model.remove(&completion.peer_key);
        }
    }

    fn assert_matches_model(&self) {
        assert_eq!(
            self.queue.len(),
            self.model.len(),
            "the production queue and model own the same number of peers"
        );
        assert!(
            self.model.len() <= MAX_STAGED_TARGETS_V1,
            "the bounded queue never exceeds its ownership cap"
        );
        let active_count = self
            .model
            .values()
            .filter(|work| matches!(work, ModelWork::Active { .. }))
            .count();
        assert_eq!(
            self.pending.len(),
            active_count,
            "pending owners exactly match active requests"
        );
        for peer_key in 0..LOGICAL_PEERS {
            match self.model.get(&peer_key) {
                Some(ModelWork::Awaiting {
                    session_id,
                    target,
                    scope,
                    ..
                }) => {
                    assert!(
                        self.queue
                            .awaiting(&peer(peer_key), *session_id, *target, *scope)
                            .is_some(),
                        "the queue's exact awaiting target matches the model"
                    );
                    assert!(self.queue.active(&peer(peer_key)).is_none());
                }
                Some(ModelWork::Active {
                    session_id,
                    target,
                    owner,
                    phase,
                    ancestor_bound,
                }) => {
                    let request = self
                        .queue
                        .active(&peer(peer_key))
                        .expect("the production queue is active when the model is active");
                    assert_eq!(request.target.session_id, *session_id);
                    assert_eq!(request.target.status.selected_tip_hash, *target);
                    assert_eq!(request.owner, *owner);
                    assert_eq!(request.phase, *phase);
                    assert_eq!(request.common_ancestor.is_some(), *ancestor_bound);
                }
                None => {
                    assert!(self.queue.active(&peer(peer_key)).is_none());
                }
            }
        }
    }
}

/// Replay one bounded byte stream through the production queue and ownership gate.
pub fn replay_header_pursuit_bytes(data: &[u8]) -> HeaderPursuitReplaySummary {
    let mut harness = PursuitHarness::new();
    for operation in data[..data.len().min(MAX_INPUT_BYTES)].chunks_exact(4) {
        harness.apply(operation);
    }
    harness.summary
}

fn hash(marker: u8) -> block::Hash {
    block::Hash([marker; 32])
}

fn decode_opcode(byte: u8) -> u8 {
    match byte {
        0..=16 => byte,
        56..=63 => byte - 56,
        72..=80 => byte - 64,
        _ => byte % 17,
    }
}

fn frontier(marker: u8) -> Frontier {
    Frontier::new(block::Height(u32::from(marker)), hash(marker))
}

fn target(marker: u8) -> block::Hash {
    hash(marker)
}

fn peer(peer_key: u8) -> ZakuraPeerId {
    let marker = peer_key
        .checked_add(1)
        .expect("logical peer keys are bounded below u8::MAX");
    ZakuraPeerId::new(vec![marker; 32]).expect("the fixed peer identity respects the wire bound")
}

fn source(peer_key: u8) -> SourceId {
    let marker = peer_key
        .checked_add(1)
        .expect("logical peer keys are bounded below u8::MAX");
    SourceId::from_digest([marker; 32])
}

fn session(flags: u8) -> u64 {
    u64::from((flags & 3) + 1)
}

fn priority(flags: u8) -> PeerWorkPriority {
    match (flags >> 2) % 3 {
        0 => PeerWorkPriority::LowerComparableWork,
        1 => PeerWorkPriority::Normal,
        2 => PeerWorkPriority::HigherComparableWork,
        _ => unreachable!("the priority is reduced modulo three"),
    }
}

fn advertisement(
    snapshot: &EngineSnapshot,
    observed_at: Instant,
    session_id: u64,
    marker: u8,
) -> AdvertisedHeaderTarget {
    AdvertisedHeaderTarget {
        scope: WorkScope::for_header_target(snapshot, target(marker)),
        session_id,
        observed_at,
        status: Status {
            work_anchor_height: block::Height(10),
            work_anchor_hash: hash(10),
            selected_tip_height: block::Height(u32::from(marker)),
            selected_tip_hash: target(marker),
            suffix_cumulative_work: U256::from(u32::from(marker)),
            oldest_retained_height: block::Height(10),
            max_headers_per_response: 1_000,
            max_inflight_requests: 1,
            max_message_bytes: 2_000_000,
            tree_aux_schema_mask: 1,
        },
    }
}

fn request(
    snapshot: &EngineSnapshot,
    observed_at: Instant,
    peer_key: u8,
    session_id: u64,
    marker: u8,
) -> ActiveHeaderRequest {
    let request_value = u64::from(peer_key) + 1;
    let request_id =
        NonZeroU64::new(request_value).expect("logical peer request identifiers are nonzero");
    let target = advertisement(snapshot, observed_at, session_id, marker);
    let owner = WorkOwner {
        state_version: snapshot.state_version,
        header_generation: snapshot.header_generation,
        verified_generation: None,
        branch: BranchId::new(
            snapshot.frontiers.finalized.hash,
            target.status.selected_tip_hash,
        ),
        session_id,
        request_id,
    };
    ActiveHeaderRequest {
        peer: peer(peer_key),
        source: source(peer_key),
        target,
        sent_locator: HeaderLocator::for_continuation(snapshot.frontiers.header_best),
        request_id: HeaderSyncRequestId::new(request_value)
            .expect("logical peer request identifiers are nonzero"),
        owner,
        common_ancestor: None,
        entries: Vec::new(),
        phase: HeaderTargetPhase::Receiving,
        max_header_count: 1_000,
        tree_aux_schema: AuxSchema::None,
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use super::*;

    #[test]
    fn header_pursuit_regression_corpus_replays_green() {
        let corpus = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../fuzz/header-chain/corpus/header_pursuit");
        let mut entries: Vec<_> = fs::read_dir(&corpus)
            .expect("the checked-in pursuit corpus exists")
            .map(|entry| entry.expect("the corpus entry is readable").path())
            .collect();
        entries.sort();
        assert!(!entries.is_empty(), "the pursuit corpus is not empty");
        for path in entries {
            let data = fs::read(&path).expect("the pursuit corpus file is readable");
            let first = replay_header_pursuit_bytes(&data);
            let second = replay_header_pursuit_bytes(&data);
            assert_eq!(
                first,
                second,
                "replay is deterministic for {}",
                path.display()
            );
            assert_eq!(
                first.peer_punishments,
                0,
                "modeled non-data outcomes are nonpunitive for {}",
                path.display()
            );
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .expect("corpus filenames are valid UTF-8");
            match name {
                "exact_completion" => assert_eq!(first.state_submissions, 1),
                "explicit_outcome" => assert_eq!(first.explicit_outcomes, 1),
                "held_completion" => {
                    assert_eq!(first.state_submissions, 1);
                    assert_eq!(first.held_completions, 1);
                    assert_eq!(first.released_completions, 1);
                    assert_eq!(first.stale_releases, 0);
                }
                "stale_held_completion" => {
                    assert_eq!(first.state_submissions, 0);
                    assert_eq!(first.held_completions, 1);
                    assert_eq!(first.released_completions, 1);
                    assert_eq!(first.stale_releases, 1);
                }
                "aud_05_old_network_response" => {
                    assert_eq!(first.state_submissions, 0);
                    assert_eq!(first.held_completions, 1);
                    assert_eq!(first.released_completions, 1);
                    assert_eq!(first.stale_releases, 1);
                    assert_eq!(first.completion_effects, NoEffectsProbe::default());
                }
                "aud_06_late_state_success" => {
                    assert_eq!(first.state_submissions, 1);
                    assert_eq!(first.held_state_results, 1);
                    assert_eq!(first.released_state_results, 1);
                    assert_eq!(first.stale_state_results, 1);
                    assert_eq!(first.completion_effects, NoEffectsProbe::default());
                }
                "aud_07_late_state_failure" => {
                    assert_eq!(first.state_submissions, 1);
                    assert_eq!(first.held_state_results, 1);
                    assert_eq!(first.held_state_failures, 1);
                    assert_eq!(first.released_state_results, 1);
                    assert_eq!(first.released_state_failures, 1);
                    assert_eq!(first.stale_state_results, 1);
                    assert_eq!(first.completion_effects, NoEffectsProbe::default());
                }
                "page_partitions" => assert_eq!(first.partition_checks, 1),
                "disconnected_held_completion" => {
                    assert_eq!(first.state_submissions, 0);
                    assert_eq!(first.held_completions, 1);
                    assert_eq!(first.released_completions, 1);
                    assert_eq!(first.stale_releases, 1);
                }
                "corrupt_advisory" => {
                    assert_eq!(first.state_submissions, 0);
                    assert_eq!(first.advisory_mutations, 1);
                }
                "seventeen_pursuits" | "stale_generation" | "wrong_ancestry" | "wrong_target" => {
                    assert_eq!(first.state_submissions, 0)
                }
                other => panic!("new pursuit corpus seed {other} needs an expected outcome"),
            }
        }
    }

    #[test]
    fn only_exact_prepared_current_targets_reach_state() {
        let valid = [
            0, 0, 42, 0, // advertise session 1, target 42
            1, 0, 42, 0, // start exact request
            2, 0, 42, 0, // bind authenticated ancestry
            3, 0, 42, 0, // prepare the complete target
            4, 0, 42, 0, // pass the gate and submit once
        ];
        assert_eq!(replay_header_pursuit_bytes(&valid).state_submissions, 1);

        let mut stale = valid[..16].to_vec();
        stale.extend([7, 0, 0, 0]);
        stale.extend([4, 0, 42, 0]);
        let summary = replay_header_pursuit_bytes(&stale);
        assert_eq!(summary.state_submissions, 0);
        assert!(summary.refused_operations > 0);
    }

    #[test]
    fn modeled_queue_cap_and_priority_replacement_match_production() {
        let mut input = Vec::new();
        for peer_key in 0..=16_u8 {
            input.extend([
                0,
                peer_key,
                peer_key
                    .checked_add(40)
                    .expect("the bounded peer marker fits in one byte"),
                4,
            ]);
        }
        let at_capacity = replay_header_pursuit_bytes(&input);
        assert_eq!(at_capacity.state_submissions, 0);

        input.extend([0, 17, 57, 8]);
        let replacement = replay_header_pursuit_bytes(&input);
        assert_eq!(replacement.state_submissions, 0);
        assert_eq!(replacement.peer_punishments, 0);
    }

    #[test]
    fn held_completion_is_current_before_retirement_and_stale_after_it() {
        let prepare_and_hold = [
            0, 0, 42, 0, // advertise session 1, target 42
            1, 0, 42, 0, // start exact request
            2, 0, 42, 0, // bind authenticated ancestry
            3, 0, 42, 0, // prepare the complete target
            8, 0, 42, 0, // hold in slot 32
        ];

        let mut current = prepare_and_hold.to_vec();
        current.extend([9, 0, 32, 0]);
        let current = replay_header_pursuit_bytes(&current);
        assert_eq!(current.state_submissions, 1);
        assert_eq!(current.stale_releases, 0);
        assert_ne!(current.completion_effects, NoEffectsProbe::default());

        let mut stale = prepare_and_hold.to_vec();
        stale.extend([7, 0, 0, 0]);
        stale.extend([9, 0, 32, 0]);
        let stale = replay_header_pursuit_bytes(&stale);
        assert_eq!(stale.state_submissions, 0);
        assert_eq!(stale.stale_releases, 1);
        assert_eq!(stale.peer_punishments, 0);

        let mut disconnected = prepare_and_hold.to_vec();
        disconnected.extend([6, 0, 0, 0]);
        disconnected.extend([9, 0, 32, 0]);
        let disconnected = replay_header_pursuit_bytes(&disconnected);
        assert_eq!(disconnected.state_submissions, 0);
        assert_eq!(disconnected.stale_releases, 1);
        assert_eq!(disconnected.peer_punishments, 0);
    }

    #[test]
    fn aud_05_old_network_response_has_no_effects_after_reset() {
        let input = [
            0, 0, 42, 0, // advertise session 1, target 42
            1, 0, 42, 0, // start exact request
            2, 0, 42, 0, // bind authenticated ancestry
            3, 0, 42, 0, // prepare the complete target
            8, 0, 42, 0, // hold in slot 32
            11, 0, 55, 0, // commit another exact branch and retire old work
            9, 0, 32, 0, // release the old response
        ];
        let summary = replay_header_pursuit_bytes(&input);
        assert_eq!(summary.state_submissions, 0);
        assert_eq!(summary.held_completions, 1);
        assert_eq!(summary.released_completions, 1);
        assert_eq!(summary.stale_releases, 1);
        assert_eq!(summary.completion_effects, NoEffectsProbe::default());
        assert_eq!(summary.peer_punishments, 0);
    }

    #[test]
    fn aud_06_late_state_success_cannot_publish_cover_or_schedule() {
        let held_success = [
            0, 0, 42, 0, // advertise session 1, target 42
            1, 0, 42, 0, // start exact request
            2, 0, 42, 0, // bind authenticated ancestry
            3, 0, 42, 0, // prepare the complete target
            12, 0, 42, 0, // dispatch the state write
            13, 0, 42, 0, // hold its success in slot 32
        ];
        let mut current = held_success.to_vec();
        current.extend([
            14, 0, 32, 0, // release the current state success
        ]);
        let current = replay_header_pursuit_bytes(&current);
        assert_eq!(current.state_submissions, 1);
        assert_eq!(current.stale_state_results, 0);
        assert_ne!(current.completion_effects, NoEffectsProbe::default());

        let mut input = held_success.to_vec();
        input.extend([
            11, 0, 55, 0, // observe reconciled branch B and retire A
            14, 0, 32, 0, // release A's old state success
        ]);
        let summary = replay_header_pursuit_bytes(&input);
        assert_eq!(summary.state_submissions, 1);
        assert_eq!(summary.held_state_results, 1);
        assert_eq!(summary.released_state_results, 1);
        assert_eq!(summary.stale_state_results, 1);
        assert_eq!(summary.completion_effects, NoEffectsProbe::default());
        assert_eq!(summary.peer_punishments, 0);
    }

    #[test]
    fn aud_07_late_state_failure_cannot_retry_repair_or_score() {
        let held_failure = [
            0, 0, 42, 0, // advertise session 1, target 42
            1, 0, 42, 0, // start exact request
            2, 0, 42, 0, // bind authenticated ancestry
            3, 0, 42, 0, // prepare the complete target
            12, 0, 42, 0, // dispatch the state write
            15, 0, 42, 0, // hold its local failure in slot 32
        ];
        let mut current = held_failure.to_vec();
        current.extend([
            14, 0, 32, 0, // release the current local failure
        ]);
        let current = replay_header_pursuit_bytes(&current);
        assert_eq!(current.released_state_failures, 1);
        assert_eq!(current.stale_state_results, 0);
        assert_eq!(current.completion_effects, NoEffectsProbe::default());

        let mut input = held_failure.to_vec();
        input.extend([
            11, 0, 55, 0, // observe reconciled branch B and retire A
            14, 0, 32, 0, // release A's old state failure
        ]);
        let summary = replay_header_pursuit_bytes(&input);
        assert_eq!(summary.state_submissions, 1);
        assert_eq!(summary.held_state_results, 1);
        assert_eq!(summary.held_state_failures, 1);
        assert_eq!(summary.released_state_results, 1);
        assert_eq!(summary.released_state_failures, 1);
        assert_eq!(summary.stale_state_results, 1);
        assert_eq!(summary.completion_effects, NoEffectsProbe::default());
        assert_eq!(summary.peer_punishments, 0);
    }

    #[test]
    fn corrupted_advisory_fields_never_change_local_authority() {
        let mut input = Vec::new();
        for mutation in 0..4 {
            input.extend([10, mutation, 42 + mutation, mutation << 2]);
        }
        let summary = replay_header_pursuit_bytes(&input);
        assert_eq!(summary.advisory_mutations, 4);
        assert_eq!(summary.state_submissions, 0);
        assert_eq!(summary.peer_punishments, 0);
    }

    #[test]
    fn complete_target_is_independent_of_page_partition() {
        let summary = replay_header_pursuit_bytes(&[16, 0, 0, 0]);
        assert_eq!(summary.partition_checks, 1);
        assert_eq!(summary.state_submissions, 0);
        assert_eq!(summary.peer_punishments, 0);
    }
}
