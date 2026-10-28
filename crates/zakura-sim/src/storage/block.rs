//! Block storage as an action source.
//!
//! [`BlockStorage`] offers each pending operation of a
//! [`ControlledBlockService`] as `storage:block:<id>:<outcome>` actions, one
//! per outcome its contract allows now, so the `Plan` picks when and how
//! each completes. At every observation it drains the service's facts,
//! checks them and the service's actual owner state against an
//! [`OwnerModel`], logs each fact as [`Observation::Storage`] and the state
//! as [`Observation::StorageState`], and then records what it delivered in
//! its checkpoint. Any failure ends the run with
//! [`HarnessFailure::SourceEvidence`](crate::HarnessFailure).
//!
//! Block operations have no deadlines, so the source sets no boundaries.

use std::sync::{Arc, Weak};

use zakura_header_chain::TransientBodyFailureKind;
use zakura_network::zakura::BlockApplyResult;
use zakura_scheduler::SimInstant;
use zakura_storage::block::{
    controlled::{ControlledBlockService, Fact, FactKind, OpId},
    evidence::{
        OperationStage, OwnerModel, OwnerState, RecordResult, RequestIdentity, ResultIdentity,
    },
    ApplyClass, BlockOutcome, Commitment, RetryKind, Stage, Verdict,
};

use super::{block_owner, hash, hex, peer, point, scope};
use crate::{
    evidence::{Checkpoint, EvidenceId, EvidenceKind, LiveEvidence},
    observe::{
        BlockOperation, BlockStorageState, BodyAvailability, BodyScope, StorageFact,
        StorageRequest, StorageResult, StorageState,
    },
    ActionSource, Boundary, Identity, Observation, ObservationLog, Sim,
};

/// The service name in identities and observations.
pub(crate) const SERVICE: &str = "block";

/// Action indices hold the operation ID times this, plus the outcome's
/// code.
const CODES: u64 = 32;

/// `outcome`'s code in a source-local action index. The match is
/// exhaustive, so a new outcome or verdict can't go without a code.
const fn code(outcome: BlockOutcome) -> u64 {
    match outcome {
        BlockOutcome::Answer => 0,
        BlockOutcome::Fail => 1,
        BlockOutcome::Admit => 2,
        BlockOutcome::Apply => 3,
        BlockOutcome::Serve => 4,
        BlockOutcome::Error => 5,
        BlockOutcome::Verdict(verdict) => match verdict {
            Verdict::Committed => 6,
            Verdict::Duplicate => 7,
            Verdict::PayloadMismatch(commitment) => match commitment {
                Commitment::AuthDataRoot => 8,
                Commitment::ChainHistoryBlockTxAuthCommitment => 9,
                Commitment::FinalSaplingRoot => 10,
            },
            Verdict::ConsensusInvalid => 11,
            Verdict::Retryable(kind) => match kind {
                RetryKind::MissingContext => 12,
                RetryKind::Canceled => 13,
                RetryKind::Storage => 14,
                RetryKind::VerifierUnavailable => 15,
                RetryKind::ResourceExhausted => 16,
            },
            Verdict::Ambiguous => 17,
        },
    }
}

/// The outcome with code `code`, the inverse of [`code`].
fn outcome(code: u64) -> Option<BlockOutcome> {
    let verdict = |verdict| Some(BlockOutcome::Verdict(verdict));
    match code {
        0 => Some(BlockOutcome::Answer),
        1 => Some(BlockOutcome::Fail),
        2 => Some(BlockOutcome::Admit),
        3 => Some(BlockOutcome::Apply),
        4 => Some(BlockOutcome::Serve),
        5 => Some(BlockOutcome::Error),
        6 => verdict(Verdict::Committed),
        7 => verdict(Verdict::Duplicate),
        8 => verdict(Verdict::PayloadMismatch(Commitment::AuthDataRoot)),
        9 => verdict(Verdict::PayloadMismatch(
            Commitment::ChainHistoryBlockTxAuthCommitment,
        )),
        10 => verdict(Verdict::PayloadMismatch(Commitment::FinalSaplingRoot)),
        11 => verdict(Verdict::ConsensusInvalid),
        12 => verdict(Verdict::Retryable(RetryKind::MissingContext)),
        13 => verdict(Verdict::Retryable(RetryKind::Canceled)),
        14 => verdict(Verdict::Retryable(RetryKind::Storage)),
        15 => verdict(Verdict::Retryable(RetryKind::VerifierUnavailable)),
        16 => verdict(Verdict::Retryable(RetryKind::ResourceExhausted)),
        17 => verdict(Verdict::Ambiguous),
        _ => None,
    }
}

/// The operation and outcome that action index `local` names.
pub(crate) fn decode(local: u64) -> (OpId, BlockOutcome) {
    let outcome = outcome(local % CODES).expect("indices come from `enabled`, which codes them");
    (OpId(local / CODES), outcome)
}

/// Block storage driven by the run. Dropping it shuts the service down,
/// which a run does after its log is frozen, so nothing is logged. A drop
/// while the run is live fails the run, since that shutdown is never
/// observed.
pub struct BlockStorage {
    service: Arc<ControlledBlockService>,
    model: OwnerModel,
    /// The last fact delivered to the log, or `None` before the service is
    /// bound at the first observation.
    delivered: Option<u64>,
    checkpoint: Checkpoint,
    /// Binds it to the run that holds its checkpoint.
    id: EvidenceId,
    /// The service in its action identities: `block` for the run's first
    /// storage instance, and `block-<index>` for each later one, as
    /// [`HeaderStorage`](super::HeaderStorage) names its instances.
    label: String,
}

impl BlockStorage {
    /// A source for `service`. Its checkpoint is registered with `sim` now,
    /// so the run fails unless the source observes every boundary, however
    /// it's later wrapped or added.
    ///
    /// # Panics
    ///
    /// If `service` isn't pristine, since then the model can't know its
    /// owner state. That's a fixture bug.
    pub fn new(sim: &mut Sim, service: Arc<ControlledBlockService>) -> Self {
        assert!(
            service.is_pristine(),
            "a storage source needs a pristine service: build it before the run and use it only \
             inside the run"
        );
        let live: Weak<dyn LiveEvidence> = Arc::<ControlledBlockService>::downgrade(&service);
        let checkpoint = Checkpoint::new(
            EvidenceKind::Storage {
                service: SERVICE.to_owned(),
            },
            live,
        );
        let id = sim.register(checkpoint.clone());
        BlockStorage {
            service,
            model: OwnerModel::new(),
            delivered: None,
            checkpoint,
            label: match id.index {
                0 => SERVICE.to_owned(),
                index => format!("{SERVICE}-{index}"),
            },
            id,
        }
    }
}

/// `stage`'s name.
fn stage(stage: Stage) -> String {
    match stage {
        Stage::Pending => "pending",
        Stage::Queued => "queued",
        Stage::Admitted => "admitted",
    }
    .to_owned()
}

/// `class`'s name.
fn class(class: ApplyClass) -> String {
    match class {
        ApplyClass::Checkpoint => "checkpoint",
        ApplyClass::Full => "full",
    }
    .to_owned()
}

/// `result`'s name.
fn apply_result(result: BlockApplyResult) -> String {
    match result {
        BlockApplyResult::Committed => "committed",
        BlockApplyResult::Duplicate => "duplicate",
        BlockApplyResult::Rejected => "rejected",
        BlockApplyResult::Unavailable => "unavailable",
        BlockApplyResult::TimedOut => "timed-out",
    }
    .to_owned()
}

/// `attempt`'s name.
fn attempt(attempt: RecordResult) -> String {
    match attempt {
        RecordResult::Committed => "committed",
        RecordResult::NoChange => "no-change",
        RecordResult::Stale => "stale",
        RecordResult::ResourceStalled => "resource-stalled",
        RecordResult::Refused => "refused",
    }
    .to_owned()
}

/// `kind`'s name.
fn failure(kind: TransientBodyFailureKind) -> String {
    match kind {
        TransientBodyFailureKind::MissingContext => "missing-context",
        TransientBodyFailureKind::Canceled => "canceled",
        TransientBodyFailureKind::Storage => "storage",
        TransientBodyFailureKind::VerifierUnavailable => "verifier-unavailable",
        TransientBodyFailureKind::Timeout => "timeout",
        TransientBodyFailureKind::ResourceExhausted => "resource-exhausted",
    }
    .to_owned()
}

/// `request` in plain fields.
fn request(request: &RequestIdentity) -> StorageRequest {
    match request {
        RequestIdentity::Query {
            query_id,
            from,
            limit,
            scope: seen,
            best_header_tip,
        } => StorageRequest::QueryNeededBlocks {
            query_id: query_id.get(),
            from: from.0,
            limit: *limit,
            scope: BodyScope {
                scope: scope(seen.header),
                verified_generation: seen.verified_generation.get(),
                body_work_epoch: seen.body_work_epoch.get(),
            },
            best_header_tip: best_header_tip.0,
        },
        RequestIdentity::Submit {
            owner,
            source,
            token,
            height,
            hash: submitted,
            parent,
            class: applied,
        } => StorageRequest::SubmitBlock {
            owner: block_owner(*owner),
            source: peer(*source),
            token: *token,
            height: height.map(|height| height.0),
            hash: hash(*submitted),
            parent: hash(*parent),
            class: class(*applied),
        },
        RequestIdentity::Record {
            op,
            expected_version,
            hash: about,
            evidence,
            rule,
            source,
            failure: kind,
            availability,
        } => StorageRequest::RecordBody {
            record: op.name().to_owned(),
            expected_version: expected_version.get(),
            hash: hash(*about),
            evidence: hex(&evidence.digest()),
            rule: rule.as_ref().map(|rule| rule.as_str().to_owned()),
            source: source.map(peer),
            failure: kind.map(failure),
            availability: availability.map(|summary| BodyAvailability {
                started_at: summary.started_at.timestamp_millis(),
                attempts: summary.attempts,
                suppliers: summary.suppliers,
                supplier_set_digest: hex(&summary.supplier_set_digest),
                alarmed: summary.alarmed,
                next_probe_at: summary.next_probe_at.timestamp_millis(),
            }),
        },
        RequestIdentity::Misbehavior {
            peer: reported,
            reason,
        } => StorageRequest::Misbehavior {
            peer: hex(reported.as_bytes()),
            reason: reason.clone(),
        },
        RequestIdentity::Read {
            start_height,
            count,
            max_body_bytes,
        } => StorageRequest::ReadRange {
            start_height: start_height.0,
            count: *count,
            max_body_bytes: *max_body_bytes,
        },
    }
}

/// `result` in plain fields.
fn result(result: &ResultIdentity) -> StorageResult {
    match result {
        ResultIdentity::Admitted => StorageResult::Admitted,
        ResultIdentity::NeededBlocks {
            body_anchor,
            blocks,
        } => StorageResult::NeededBlocks {
            anchor: point(*body_anchor),
            blocks: blocks
                .iter()
                .map(|(height, needed)| point(zakura_header_chain::Frontier::new(*height, *needed)))
                .collect(),
        },
        ResultIdentity::NeededBlocksFailed => StorageResult::NeededBlocksFailed,
        ResultIdentity::Verdict { verdict, result } => StorageResult::Verdict {
            verdict: BlockOutcome::Verdict(*verdict).name(),
            reply: apply_result(*result),
        },
        ResultIdentity::Recorded(attempts) => StorageResult::Recorded {
            attempts: attempts.iter().copied().map(attempt).collect(),
        },
        ResultIdentity::RecordFailed => StorageResult::RecordFailed,
        ResultIdentity::Served { heights, bytes } => StorageResult::Served {
            heights: heights.iter().map(|height| height.0).collect(),
            bytes: *bytes,
        },
        ResultIdentity::ReadFailed => StorageResult::ReadFailed,
    }
}

/// `fact` as storage `instance`'s observation.
fn observation(fact: Fact, instance: u64) -> Observation {
    let fact_kind = match fact.kind {
        FactKind::Submitted {
            id,
            request: asked,
            stage: first,
        } => StorageFact::BlockSubmitted {
            id: id.0,
            request: request(&asked),
            stage: first.map(stage),
        },
        FactKind::Admitted { id } => StorageFact::Admitted { id: id.0 },
        FactKind::Completed(completion) => StorageFact::BlockCompleted {
            id: completion.id.0,
            outcome: completion.outcome.name(),
            result: result(&completion.result),
            delivered: completion.delivered,
            state_version: completion.state_version.get(),
        },
        FactKind::Coalesced { query_id } => StorageFact::Coalesced {
            query_id: query_id.get(),
        },
        FactKind::Deferred { queue } => StorageFact::Deferred {
            queue: queue.iter().map(request).collect(),
        },
        FactKind::TransitionInstalled {
            state_version,
            verified_best,
            epoch_before,
            invalidates_body_work,
            cause,
        } => StorageFact::TransitionInstalled {
            state_version: state_version.get(),
            verified_best: point(verified_best),
            epoch_before: epoch_before.get(),
            invalidates_body_work,
            cause: cause.map(|cause| {
                match cause {
                    zakura_header_chain::VerifiedChangeCause::Grow => "grow",
                    zakura_header_chain::VerifiedChangeCause::CheckpointFinalizedGrow => {
                        "checkpoint-finalized-grow"
                    }
                    zakura_header_chain::VerifiedChangeCause::Reset => "reset",
                }
                .to_owned()
            }),
        },
        FactKind::ViewPublished(view) => StorageFact::ViewPublished {
            state_version: view.state_version.get(),
            epoch: view.epoch.get(),
            hint_revision: view.hint_revision,
            verified_best: point(view.verified_best),
            header_best: point(view.header_best),
        },
        FactKind::ReplyDelivered { id } => StorageFact::ReplyDelivered { id: id.0 },
        FactKind::ReplyDropped { id } => StorageFact::HandleClosed { id: id.0 },
        FactKind::CallerDropped { id } => StorageFact::CallerDropped { id: id.0 },
        FactKind::ReadTaken { id } => StorageFact::ReadTaken { id: id.0 },
        FactKind::ReadDropped { id } => StorageFact::ReadDropped { id: id.0 },
        FactKind::DriverExited { dropped, deferred } => StorageFact::DriverExited {
            dropped: dropped.into_iter().map(|id| id.0).collect(),
            deferred: deferred.iter().map(request).collect(),
        },
        // Mapped like any fact. Shutdown at the end of a run happens after
        // the log is frozen and is never observed.
        FactKind::ShutDown { dropped, deferred } => StorageFact::ShutDown {
            dropped: dropped.into_iter().map(|id| id.0).collect(),
            deferred: deferred.iter().map(request).collect(),
        },
    };
    Observation::Storage {
        service: SERVICE.to_owned(),
        instance,
        seq: fact.seq,
        txn: fact.txn,
        fact: fact_kind,
    }
}

/// `state` in plain fields.
fn state(state: &OwnerState) -> StorageState {
    StorageState::Block(BlockStorageState {
        operations: state
            .operations
            .iter()
            .map(|op| {
                let (name, caller_waiting, replaced) = match op.stage {
                    OperationStage::Pending { caller_waiting } => {
                        ("pending", Some(caller_waiting), None)
                    }
                    OperationStage::Queued => ("queued", None, None),
                    OperationStage::Admitted { replaced } => ("admitted", None, Some(replaced)),
                    OperationStage::Replied => ("replied", None, None),
                };
                BlockOperation {
                    id: op.id.0,
                    request: request(&op.request),
                    stage: name.to_owned(),
                    caller_waiting,
                    replaced,
                    retained: op.retained.map(hash),
                }
            })
            .collect(),
        deferred: state.deferred.iter().map(request).collect(),
    })
}

impl ActionSource for BlockStorage {
    fn enabled(&self, _: SimInstant, out: &mut Vec<(Identity, u64)>) {
        for op in self.service.pending() {
            for outcome in op.enabled {
                out.push((
                    Identity::storage(&self.label, op.id.0, &outcome.name()),
                    op.id.0 * CODES + code(outcome),
                ));
            }
        }
    }

    fn boundaries(&self, _: SimInstant, _: &mut Vec<Boundary>) {}

    fn execute(&mut self, local: u64, _: &mut ObservationLog) {
        let (id, outcome) = decode(local);
        match self.service.complete(id, outcome) {
            // An engine refusal completes as a `Storage` retry, so only an
            // incoherent commit errs, which fixture chains never make.
            Ok(_) => {}
            Err(error) => panic!("`enabled` offered this completion at this step: {error}"),
        }
    }

    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        // As `HeaderStorage::observe`: only the run holding the checkpoint
        // checks what this delivers.
        if log.run() != self.id.run {
            return Err("ForeignRun: storage registered with another run".to_owned());
        }
        let delivered = match self.delivered {
            Some(delivered) => delivered,
            None => {
                self.service
                    .bind_to_run()
                    .map_err(|error| format!("activity before binding: {error}"))?;
                0
            }
        };
        let facts = self.service.take_facts();
        let actual = self.service.owner_state();
        self.model
            .check_boundary(&facts, &actual)
            .map_err(|failure| format!("{failure:?}"))?;
        // After setup, the state is logged only when it may have changed.
        let with_state = self.delivered.is_none() || !facts.is_empty();
        let last = facts.last().map_or(delivered, |fact| fact.seq);
        let mut sent: Vec<Observation> = facts
            .into_iter()
            .map(|fact| observation(fact, self.id.index))
            .collect();
        if with_state {
            sent.push(Observation::StorageState {
                service: SERVICE.to_owned(),
                instance: self.id.index,
                through_seq: actual.through_seq,
                state: state(&actual),
            });
        }
        for obs in &sent {
            log.push(obs.clone());
        }
        self.delivered = Some(last);
        self.checkpoint.record(log.step(), &sent, last);
        Ok(())
    }
}

impl Drop for BlockStorage {
    fn drop(&mut self) {
        // Its shutdown facts are never observed, so a run still live fails.
        self.checkpoint.dropped();
        self.service.shut_down();
    }
}

#[cfg(test)]
mod tests {
    use zakura_storage::block::{ApplyClass, BlockOp, Stage};

    use zakura_storage::block::controlled::OpId;

    use super::{code, decode, outcome, CODES};

    /// Every outcome the contract lists has its own code, which decodes
    /// back to it.
    #[test]
    fn every_contract_outcome_round_trips_through_its_code() {
        let mut codes = Vec::new();
        for op in BlockOp::ALL {
            for stage in [Stage::Pending, Stage::Queued, Stage::Admitted] {
                for class in [ApplyClass::Checkpoint, ApplyClass::Full] {
                    for (listed, _) in op.outcomes(stage, class) {
                        assert!(code(listed) < CODES);
                        assert_eq!(outcome(code(listed)), Some(listed));
                        assert_eq!(decode(7 * CODES + code(listed)), (OpId(7), listed));
                        codes.push(code(listed));
                    }
                }
            }
        }
        codes.sort_unstable();
        codes.dedup();
        assert_eq!(codes, (0..18).collect::<Vec<_>>(), "every code is in use");
    }

    /// Each outcome's name in its action identity, by code. A renamed
    /// outcome changes every plan that picks it.
    #[test]
    fn outcome_names_are_pinned() {
        let names: Vec<String> = (0..CODES)
            .map_while(outcome)
            .map(|listed| listed.name())
            .collect();
        assert_eq!(
            names,
            [
                "answer",
                "fail",
                "admit",
                "apply",
                "serve",
                "error",
                "committed",
                "duplicate",
                "payload-mismatch-auth-data-root",
                "payload-mismatch-chain-history-block-tx-auth-commitment",
                "payload-mismatch-final-sapling-root",
                "consensus-invalid",
                "retryable-missing-context",
                "retryable-canceled",
                "retryable-storage",
                "retryable-verifier-unavailable",
                "retryable-resource-exhausted",
                "ambiguous",
            ]
        );
    }
}
