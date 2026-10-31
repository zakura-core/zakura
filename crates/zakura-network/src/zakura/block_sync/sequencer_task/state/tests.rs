//! Machine tests for [`SequencerState`].
//!
//! Each proptest drives the state with legal input sequences and plays the
//! driver itself: it accepts or refuses submissions and delivers or withholds
//! required evidence. After every step it compares the state with an independent
//! model. The models track heights, epochs, and submission identities. They never
//! call the state's helpers.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    num::NonZeroU64,
};

use chrono::TimeZone;
#[cfg(test)]
use proptest::prelude::*;
use zakura_chain::serialization::ZcashDeserializeInto;
use zakura_header_chain::{
    BodyWorkAuthority, BodyWorkEpoch, BodyWorkOwner, HeaderGeneration, HeaderWorkAuthority,
    SourceId, StateVersion,
};

use super::*;
use crate::zakura::block_sync::{test_work_scope, BlockSyncFrontiers};

/// Highest height any generated input touches.
const MAX_HEIGHT: u32 = 12;

fn to_usize(value: u32) -> usize {
    usize::try_from(value).expect("test heights fit in usize")
}

fn to_u64(value: usize) -> u64 {
    u64::try_from(value).expect("test sizes fit in u64")
}

fn test_block() -> Arc<block::Block> {
    Arc::new(
        // Public block fixture from zakura-test/src/vectors/block-main-0-000-001.txt.
        include_bytes!("mainnet-block-1.bin")
            .as_slice()
            .zcash_deserialize_into()
            .expect("block test vector parses"),
    )
}

fn wall_now() -> DateTime<Utc> {
    Utc.timestamp_opt(1_700_000_000, 0)
        .single()
        .expect("the fixed timestamp is valid")
}

/// A body hash that differs per height and per body-work epoch.
fn hash(height: u32, epoch: u64) -> block::Hash {
    let mut bytes = [0; 32];
    bytes[..4].copy_from_slice(&height.to_le_bytes());
    bytes[4..12].copy_from_slice(&epoch.to_le_bytes());
    bytes[31] = 0xbd;
    block::Hash(bytes)
}

fn source() -> SourceId {
    SourceId::from_digest([1; 32])
}

fn other_source() -> SourceId {
    SourceId::from_digest([9; 32])
}

fn peer() -> ZakuraPeerId {
    ZakuraPeerId::new(vec![1; 32]).expect("test peer ID is valid")
}

/// The authority at body-work `epoch` advanced by `generation` header generations.
fn authority(epoch: u64, generation: u64) -> BodyWorkAuthority {
    let base = test_work_scope();
    BodyWorkAuthority {
        header: HeaderWorkAuthority {
            header_generation: HeaderGeneration::new(base.header_generation.get() + generation),
            ..base.header
        },
        body_work_epoch: BodyWorkEpoch::new(epoch),
        ..base
    }
}

fn owner(authority: BodyWorkAuthority) -> BodyWorkOwner {
    authority.bind(6, NonZeroU64::new(7).expect("seven is nonzero"))
}

fn genesis_frontiers() -> BlockSyncFrontiers {
    BlockSyncFrontiers {
        finalized_height: block::Height(0),
        verified_block_tip: block::Height(0),
        verified_block_hash: block::Hash([0; 32]),
    }
}

fn new_state(submitted_apply_limit: usize) -> SequencerState {
    SequencerState::new(
        Sequencer::new(block::Height(0), submitted_apply_limit),
        genesis_frontiers(),
        Some(authority(0, 0)),
        SeededRetryJitter::new([0; 32]),
    )
}

fn received(block: &Arc<block::Block>, height: u32, epoch: u64) -> ReceivedBody {
    ReceivedBody {
        owner: owner(authority(epoch, 0)),
        source: source(),
        height: block::Height(height),
        hash: hash(height, epoch),
        previous_block_hash: hash(height.saturating_sub(1), epoch),
        body: BufferedBlockBody::Decoded {
            block: block.clone(),
            decoded_attributed_memory_size_bytes: block.attributed_memory_size_bytes(),
        },
        bytes: 100,
        admitted_estimate_bytes: None,
        peer: peer(),
    }
}

/// One submission the driver accepted.
#[derive(Copy, Clone, Debug)]
struct Accepted {
    owner: BodyWorkOwner,
    token: BlockApplyToken,
    height: block::Height,
    hash: block::Hash,
}

/// Play the driver for one submission pass: accept the first `accepts` items and
/// refuse the next one, which ends the pass.
fn submission_pass(
    state: &mut SequencerState,
    now: time::Instant,
    accepts: usize,
) -> Vec<Accepted> {
    let mut accepted = Vec::new();
    let Some(mut pass) = state.begin_submission_pass(now, false) else {
        return accepted;
    };
    while let Some(height) = state.next_submission_height(&mut pass) {
        let Some(item) = state.sequencer.prepare_submit(height) else {
            continue;
        };
        if accepted.len() == accepts {
            let _ = state.submission_refused(item.height, item.token, now, false);
            break;
        }
        let _ = state.submission_accepted(item.height, item.hash, item.decoded_bytes);
        accepted.push(Accepted {
            owner: item.owner,
            token: item.token,
            height: item.height,
            hash: item.hash,
        });
    }
    accepted
}

fn apply_finished(
    accepted: Accepted,
    source: SourceId,
    outcome: BlockApplyOutcome,
    semantic: bool,
) -> SequencerControlInput {
    SequencerControlInput::ApplyFinished {
        owner: Box::new(accepted.owner),
        source,
        token: accepted.token,
        height: accepted.height,
        hash: accepted.hash,
        outcome,
        eligible_sources: BTreeSet::new(),
        persisted_availability: None,
        semantic_completion: semantic.then_some((accepted.owner, StateVersion::new(5))),
        committed_epoch: Some(accepted.owner.authority.body_work_epoch),
    }
}

fn changes_shared_work(operation: &SequencerOperation) -> bool {
    matches!(
        operation,
        SequencerOperation::ResetWorkAbove(_)
            | SequencerOperation::AdvanceWorkFloor(_)
            | SequencerOperation::RefreshWorkAuthority(_)
    )
}

/// Independent model of body sequencing within one body-work epoch.
#[derive(Debug, Default)]
struct SequencingModel {
    /// Delivered heights and the authority each body carried into the pipeline.
    received: BTreeMap<u32, BodyWorkAuthority>,
    /// Heights the driver accepted, in acceptance order.
    accepted: Vec<u32>,
    retry_at: Option<time::Instant>,
    retry_started_at: Option<time::Instant>,
    retry_attempt: u64,
}

impl SequencingModel {
    /// The highest height whose every predecessor above genesis was delivered.
    fn contiguous(&self) -> u32 {
        (1..=MAX_HEIGHT)
            .take_while(|height| self.received.contains_key(height))
            .last()
            .unwrap_or(0)
    }

    fn deliver(&mut self, height: u32, current: BodyWorkAuthority) {
        self.received.entry(height).or_insert(current);
    }

    /// Model one driver pass with an unbounded submission window.
    fn submission_pass(&mut self, now: time::Instant, accepts: usize) {
        let unsubmitted: Vec<u32> = (1..=self.contiguous())
            .filter(|height| !self.accepted.contains(height))
            .collect();
        if unsubmitted.is_empty() {
            self.retry_at = None;
            self.retry_started_at = None;
            self.retry_attempt = 0;
            return;
        }
        self.retry_at = None;
        for (index, height) in unsubmitted.into_iter().enumerate() {
            if index == accepts {
                self.retry_started_at.get_or_insert(now);
                self.retry_attempt += 1;
                self.retry_at = Some(now + Duration::from_millis(100));
                return;
            }
            self.accepted.push(height);
            if self.retry_started_at.take().is_some() {
                self.retry_attempt = 0;
            }
        }
    }

    fn check(&self, state: &SequencerState, expected_epoch: u64) {
        let sequencer = state.sequencer();
        let contiguous = self.contiguous();
        assert_eq!(
            sequencer.floor(),
            block::Height(contiguous),
            "{ORDER_PROPERTY}"
        );
        assert_eq!(
            sequencer.verified_tip(),
            block::Height(0),
            "{ORDER_PROPERTY}"
        );
        for raw in 1..=MAX_HEIGHT {
            let height = block::Height(raw);
            let delivered = self.received.get(&raw);
            assert_eq!(
                sequencer.applying_contains(height),
                delivered.is_some() && raw <= contiguous,
                "{ORDER_PROPERTY}: applying membership at {}",
                raw
            );
            assert_eq!(
                sequencer.reorder_contains(height),
                delivered.is_some() && raw > contiguous,
                "{ORDER_PROPERTY}: reorder membership at {}",
                raw
            );
            if let (Some(authority), Some((owner, ..))) =
                (delivered, sequencer.applying_identity(height))
            {
                assert_eq!(owner.authority, *authority, "{ORDER_PROPERTY}");
                assert_eq!(
                    owner.authority.body_work_epoch,
                    BodyWorkEpoch::new(expected_epoch),
                    "{ORDER_PROPERTY}"
                );
            }
        }
        // The driver sees bodies in height order, without gaps or repeats.
        let in_order: Vec<u32> = (1..).take(self.accepted.len()).collect();
        assert_eq!(&self.accepted, &in_order, "{ORDER_PROPERTY}");
        let accepted = to_u64(self.accepted.len());
        let view = state.view(0, 0, 0);
        assert_eq!(
            view.in_flight_submission_count, accepted,
            "{ORDER_PROPERTY}"
        );
        assert_eq!(
            view.unsubmitted_applying_count,
            u64::from(contiguous) - accepted,
            "{ORDER_PROPERTY}"
        );
        assert_eq!(
            state.submission_retry_at(),
            self.retry_at,
            "{ORDER_PROPERTY}"
        );
        assert_eq!(
            state.submission_retry_attempt(),
            self.retry_attempt,
            "{ORDER_PROPERTY}"
        );
    }
}

#[derive(Clone, Debug)]
enum SequencingStep {
    Deliver(u32),
    RefreshStateVersion(u64),
    RefreshAuthority(u64),
    Submit { accepts: usize, after_ms: u64 },
}

#[cfg(test)]
fn sequencing_step() -> impl Strategy<Value = SequencingStep> {
    let max_accepts = to_usize(MAX_HEIGHT);
    prop_oneof![
        4 => (1..=MAX_HEIGHT).prop_map(SequencingStep::Deliver),
        1 => any::<u64>().prop_map(SequencingStep::RefreshStateVersion),
        1 => (1..8u64).prop_map(SequencingStep::RefreshAuthority),
        2 => (0..=max_accepts, 0..300u64)
            .prop_map(|(accepts, after_ms)| SequencingStep::Submit { accepts, after_ms }),
    ]
}

fn run_sequencing(
    steps: Vec<SequencingStep>,
    mut after_step: impl FnMut(&mut SequencerState),
) -> SequencerReplaySummary {
    let block = test_block();
    let window = to_usize(MAX_HEIGHT);
    let mut state = new_state(window);
    let mut model = SequencingModel::default();
    let mut current = authority(0, 0);
    let mut now = time::Instant::now();
    let mut summary = SequencerReplaySummary::default();
    for step in steps {
        summary.transitions += 1;
        match step {
            SequencingStep::Deliver(height) => {
                // Bodies keep the authority their request was issued under.
                let acceptance = state.accept_body(received(&block, height, 0));
                let expected = if model.received.contains_key(&height) {
                    BodyAcceptance::Redundant
                } else {
                    BodyAcceptance::Buffered
                };
                assert_eq!(acceptance, expected, "{ORDER_PROPERTY}");
                summary.bodies += usize::from(!model.received.contains_key(&height));
                model.deliver(height, current);
            }
            SequencingStep::RefreshStateVersion(version) => {
                let outputs = state.on_control_input(
                    SequencerControlInput::StateVersionChanged(StateVersion::new(version)),
                    wall_now(),
                );
                assert!(!outputs.needs_reaction, "{ORDER_PROPERTY}");
                assert!(outputs.operations.is_empty(), "{ORDER_PROPERTY}");
            }
            SequencingStep::RefreshAuthority(generation) => {
                current = authority(0, generation);
                let outputs = state.on_control_input(
                    SequencerControlInput::WorkAuthorityRefreshed { authority: current },
                    wall_now(),
                );
                assert!(!outputs.needs_reaction, "{ORDER_PROPERTY}");
                assert!(
                    !outputs.operations.iter().any(changes_shared_work),
                    "{ORDER_PROPERTY}"
                );
            }
            SequencingStep::Submit { accepts, after_ms } => {
                now += Duration::from_millis(after_ms);
                let accepted = submission_pass(&mut state, now, accepts);
                for accepted in &accepted {
                    assert_eq!(
                        accepted.hash,
                        hash(accepted.height.0, 0),
                        "{ORDER_PROPERTY}"
                    );
                }
                summary.submitted += accepted.len();
                model.submission_pass(now, accepts);
                summary.backpressure += usize::from(model.retry_at.is_some());
            }
        }
        after_step(&mut state);
        // Neither refresh retires retained bodies or starts a reset epoch.
        assert_eq!(state.reset_epoch(), 0, "{ORDER_PROPERTY}");
        assert_eq!(state.reaction_epoch(), 0, "{ORDER_PROPERTY}");
        model.check(&state, 0);
    }
    summary
}

/// The coarse result of a generated apply completion.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Completion {
    Committed,
    Duplicate,
    ConsensusInvalid { attributed: bool },
    TamperedBody,
    Unavailable,
    TimedOut,
}

#[cfg(test)]
fn completion() -> impl Strategy<Value = Completion> {
    prop_oneof![
        Just(Completion::Committed),
        Just(Completion::Duplicate),
        any::<bool>().prop_map(|attributed| Completion::ConsensusInvalid { attributed }),
        Just(Completion::TamperedBody),
        Just(Completion::Unavailable),
        Just(Completion::TimedOut),
    ]
}

fn outcome(completion: Completion, accepted: Accepted) -> BlockApplyOutcome {
    let evidence = zakura_header_chain::EvidenceId::from_digest([0xe1; 32]);
    let verified = zakura_header_chain::VerifiedBodyEvidence {
        hash: accepted.hash,
        evidence,
    };
    let transient = |kind| zakura_header_chain::TransientBodyFailure {
        hash: accepted.hash,
        evidence,
        kind,
        availability: zakura_header_chain::BodyUnavailableSummary::default(),
    };
    match completion {
        Completion::Committed => BlockApplyOutcome::committed(verified),
        Completion::Duplicate => BlockApplyOutcome::duplicate(verified),
        Completion::ConsensusInvalid { attributed } => {
            BlockApplyOutcome::consensus_invalid(zakura_header_chain::ConsensusBodyInvalid {
                hash: accepted.hash,
                evidence,
                rule: zakura_header_chain::BodyRuleId::new("test.consensus_invalid"),
                source: if attributed { source() } else { other_source() },
            })
        }
        Completion::TamperedBody => {
            BlockApplyOutcome::payload_mismatch(zakura_header_chain::BodyPayloadMismatch {
                evidence,
                requested: accepted.hash,
                delivered: block::Hash([0xee; 32]),
                kind: zakura_header_chain::BodyCommitmentKind::AuthDataRoot,
                source: source(),
            })
        }
        Completion::Unavailable => BlockApplyOutcome::retryable(transient(
            zakura_header_chain::TransientBodyFailureKind::VerifierUnavailable,
        )),
        Completion::TimedOut => BlockApplyOutcome::retryable(transient(
            zakura_header_chain::TransientBodyFailureKind::Timeout,
        )),
    }
}

#[derive(Clone, Debug)]
enum ResetStep {
    DeliverCurrent(u32),
    DeliverRetired(u32),
    SubmitCurrent,
    FinishRetired {
        attempt: usize,
        completion: Completion,
        semantic: bool,
    },
}

#[cfg(test)]
fn reset_step() -> impl Strategy<Value = ResetStep> {
    prop_oneof![
        3 => (1..=MAX_HEIGHT).prop_map(ResetStep::DeliverCurrent),
        1 => (1..=MAX_HEIGHT).prop_map(ResetStep::DeliverRetired),
        1 => Just(ResetStep::SubmitCurrent),
        3 => (any::<usize>(), completion(), any::<bool>()).prop_map(
            |(attempt, completion, semantic)| ResetStep::FinishRetired {
                attempt,
                completion,
                semantic,
            }
        ),
    ]
}

fn run_authority_reset(
    retired_bodies: u32,
    steps: Vec<ResetStep>,
    mut after_retired: impl FnMut(&mut SequencerState),
) -> SequencerReplaySummary {
    let block = test_block();
    // The window holds every retired and current submission, so slots never block.
    let window = to_usize(2 * MAX_HEIGHT);
    let mut state = new_state(window);
    let now = time::Instant::now();
    for height in 1..=retired_bodies {
        let _ = state.accept_body(received(&block, height, 0));
    }
    let retired = submission_pass(&mut state, now, usize::MAX);
    assert_eq!(retired.len(), to_usize(retired_bodies), "{RESET_PROPERTY}");

    // Replace the body-work epoch while every retired apply attempt is in flight.
    let current = authority(1, 0);
    let outputs = state.on_control_input(
        SequencerControlInput::BodyWorkEpochChanged {
            authority: current,
            frontiers: genesis_frontiers(),
        },
        wall_now(),
    );
    assert!(outputs.needs_reaction, "{RESET_PROPERTY}");
    assert_eq!(state.reset_epoch(), 1, "{RESET_PROPERTY}");
    assert_eq!(state.reaction_epoch(), 1, "{RESET_PROPERTY}");
    assert!(
        matches!(
            outputs.operations.as_slice(),
            [
                SequencerOperation::RetainBodyRetryScope(retained),
                SequencerOperation::ResetWorkAbove(block::Height(0)),
                SequencerOperation::RefreshWorkAuthority(refreshed),
            ] if *retained == current && *refreshed == current
        ),
        "{RESET_PROPERTY}"
    );

    let mut model = SequencingModel::default();
    let mut unfinished: BTreeSet<BlockApplyToken> =
        retired.iter().map(|accepted| accepted.token).collect();
    let mut summary = SequencerReplaySummary::default();
    for step in steps {
        summary.transitions += 1;
        match step {
            ResetStep::DeliverCurrent(height) => {
                let _ = state.accept_body(received(&block, height, 1));
                summary.bodies += usize::from(!model.received.contains_key(&height));
                model.deliver(height, current);
            }
            ResetStep::DeliverRetired(height) => {
                let acceptance = state.accept_body(received(&block, height, 0));
                assert_eq!(acceptance, BodyAcceptance::StaleScope, "{RESET_PROPERTY}");
            }
            ResetStep::SubmitCurrent => {
                summary.submitted += submission_pass(&mut state, now, usize::MAX).len();
                model.submission_pass(now, usize::MAX);
            }
            ResetStep::FinishRetired {
                attempt,
                completion,
                semantic,
            } => {
                let Some(accepted) = retired.get(attempt % retired.len().max(1)).copied() else {
                    continue;
                };
                let outputs = state.on_control_input(
                    apply_finished(accepted, source(), outcome(completion, accepted), semantic),
                    wall_now(),
                );
                summary.completions += usize::from(unfinished.remove(&accepted.token));
                after_retired(&mut state);
                // A retired result settles only its own attempt.
                assert!(!outputs.needs_reaction, "{RESET_PROPERTY}");
                let touches_current_work = outputs.operations.iter().any(|operation| {
                    matches!(
                        operation,
                        SequencerOperation::ResetWorkAbove(_)
                            | SequencerOperation::AdvanceWorkFloor(_)
                            | SequencerOperation::DeliverEvidence
                            | SequencerOperation::SendAction(_)
                            | SequencerOperation::DeferBodyRetry { .. }
                            | SequencerOperation::ClearBodyRetry { .. }
                    )
                });
                assert!(!touches_current_work, "{RESET_PROPERTY}");
                assert_eq!(state.pending_evidence_len(), 0, "{RESET_PROPERTY}");
            }
        }
        assert_eq!(state.reset_epoch(), 1, "{RESET_PROPERTY}");
        assert_eq!(state.reaction_epoch(), 1, "{RESET_PROPERTY}");
        model.check_with_retired(&state, unfinished.len());
    }
    summary
}

impl SequencingModel {
    /// [`Self::check`] for the current epoch while `retired` older attempts stay in flight.
    fn check_with_retired(&self, state: &SequencerState, retired: usize) {
        let sequencer = state.sequencer();
        let contiguous = self.contiguous();
        assert_eq!(
            sequencer.floor(),
            block::Height(contiguous),
            "{RESET_PROPERTY}"
        );
        for raw in 1..=MAX_HEIGHT {
            let height = block::Height(raw);
            let delivered = self.received.contains_key(&raw);
            assert_eq!(
                sequencer.applying_contains(height),
                delivered && raw <= contiguous,
                "{RESET_PROPERTY}"
            );
            assert_eq!(
                sequencer.reorder_contains(height),
                delivered && raw > contiguous,
                "{RESET_PROPERTY}"
            );
            if let Some((owner, _, _, applying_hash)) = sequencer.applying_identity(height) {
                assert_eq!(
                    owner.authority.body_work_epoch,
                    BodyWorkEpoch::new(1),
                    "{RESET_PROPERTY}"
                );
                assert_eq!(applying_hash, hash(raw, 1), "{RESET_PROPERTY}");
            }
        }
        let in_order: Vec<u32> = (1..).take(self.accepted.len()).collect();
        assert_eq!(&self.accepted, &in_order, "{RESET_PROPERTY}");
        assert_eq!(
            sequencer.in_flight_submission_count(),
            self.accepted.len() + retired,
            "{RESET_PROPERTY}"
        );
        assert_eq!(
            sequencer.unsubmitted_applying_count(),
            to_usize(contiguous) - self.accepted.len(),
            "{RESET_PROPERTY}"
        );
    }
}

/// The required evidence a model expects, identified by kind and body hash.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum ExpectedEvidence {
    BodyInvalid(block::Hash),
    BodyUnavailable(block::Hash),
    TamperedBody(block::Hash),
}

fn observed_evidence(action: &BlockSyncAction) -> Option<ExpectedEvidence> {
    match action {
        BlockSyncAction::RecordBodyInvalid { invalid, .. } => {
            Some(ExpectedEvidence::BodyInvalid(invalid.hash))
        }
        BlockSyncAction::RecordBodyUnavailable { failure, .. } => {
            Some(ExpectedEvidence::BodyUnavailable(failure.hash))
        }
        BlockSyncAction::Misbehavior {
            reason: BlockSyncMisbehavior::BodyPayloadMismatch(mismatch),
            ..
        } => Some(ExpectedEvidence::TamperedBody(mismatch.requested)),
        _ => None,
    }
}

#[derive(Clone, Debug)]
enum EvidenceStep {
    Finish {
        attempt: usize,
        completion: Completion,
        semantic: bool,
    },
    /// The driver has capacity, so IO delivers the oldest pending evidence.
    Deliver,
    /// The action channel is full, so IO delivers nothing.
    Full,
}

#[cfg(test)]
fn evidence_step() -> impl Strategy<Value = EvidenceStep> {
    prop_oneof![
        3 => (any::<usize>(), completion(), any::<bool>()).prop_map(
            |(attempt, completion, semantic)| EvidenceStep::Finish {
                attempt,
                completion,
                semantic,
            }
        ),
        1 => Just(EvidenceStep::Deliver),
        2 => Just(EvidenceStep::Full),
    ]
}

fn run_required_evidence(
    steps: Vec<EvidenceStep>,
    mut after_step: impl FnMut(&mut SequencerState),
) -> SequencerReplaySummary {
    let block = test_block();
    let window = to_usize(MAX_HEIGHT);
    let mut state = new_state(window);
    for height in 1..=MAX_HEIGHT {
        let _ = state.accept_body(received(&block, height, 0));
    }
    let attempts = submission_pass(&mut state, time::Instant::now(), usize::MAX);
    assert_eq!(attempts.len(), window, "{EVIDENCE_PROPERTY}");

    // Heights whose current attempt is still applying.
    let mut live: BTreeSet<block::Height> = attempts.iter().map(|attempt| attempt.height).collect();
    let mut pending: VecDeque<ExpectedEvidence> = VecDeque::new();
    let mut summary = SequencerReplaySummary::default();
    for step in steps {
        summary.transitions += 1;
        match step {
            EvidenceStep::Finish {
                attempt,
                completion,
                semantic,
            } => {
                let accepted = attempts[attempt % attempts.len()];
                let outputs = state.on_control_input(
                    apply_finished(accepted, source(), outcome(completion, accepted), semantic),
                    wall_now(),
                );
                let height = accepted.height;
                let mut expected = Vec::new();
                let mut misbehavior = false;
                summary.completions += usize::from(live.contains(&height));
                if live.contains(&height) {
                    if !semantic {
                        live.remove(&height);
                    } else {
                        match completion {
                            Completion::Committed => {
                                live.remove(&height);
                            }
                            // A duplicate above the verified tip stays attached.
                            Completion::Duplicate => {}
                            Completion::ConsensusInvalid { attributed } => {
                                if attributed {
                                    expected.push(ExpectedEvidence::BodyInvalid(accepted.hash));
                                    misbehavior = true;
                                }
                                live.retain(|live| *live < height);
                            }
                            Completion::TamperedBody => {
                                expected.push(ExpectedEvidence::TamperedBody(accepted.hash));
                                live.retain(|live| *live < height);
                            }
                            Completion::Unavailable | Completion::TimedOut => {
                                expected.push(ExpectedEvidence::BodyUnavailable(accepted.hash));
                                live.retain(|live| *live < height);
                            }
                        }
                    }
                }
                let delivery_points = outputs
                    .operations
                    .iter()
                    .filter(|operation| matches!(operation, SequencerOperation::DeliverEvidence))
                    .count();
                assert_eq!(delivery_points, expected.len(), "{EVIDENCE_PROPERTY}");
                // Consensus invalidity scores its supplier through a replaceable
                // action. Transport and verifier faults score nobody.
                let replaceable: Vec<_> = outputs
                    .operations
                    .iter()
                    .filter_map(|operation| match operation {
                        SequencerOperation::SendAction(action) => Some(action),
                        _ => None,
                    })
                    .collect();
                assert_eq!(
                    replaceable.len(),
                    usize::from(misbehavior),
                    "{EVIDENCE_PROPERTY}"
                );
                for action in replaceable {
                    assert!(
                        matches!(
                            action,
                            BlockSyncAction::Misbehavior {
                                reason: BlockSyncMisbehavior::ConsensusBodyInvalid(invalid),
                                ..
                            } if invalid.hash == accepted.hash
                        ),
                        "{EVIDENCE_PROPERTY}: unexpected replaceable action {:?}",
                        action
                    );
                }
                summary.evidence_queued += expected.len();
                pending.extend(expected);
            }
            EvidenceStep::Deliver => {
                let delivered = state.take_pending_evidence();
                summary.evidence_delivered += usize::from(!pending.is_empty());
                assert_eq!(
                    delivered.as_ref().and_then(observed_evidence),
                    pending.pop_front(),
                    "{EVIDENCE_PROPERTY}"
                );
            }
            EvidenceStep::Full => {
                summary.backpressure += usize::from(!pending.is_empty());
            }
        }
        after_step(&mut state);
        assert_eq!(
            state.pending_evidence_len(),
            pending.len(),
            "{EVIDENCE_PROPERTY}"
        );
        assert_eq!(
            state.pending_evidence().and_then(observed_evidence),
            pending.front().copied(),
            "{EVIDENCE_PROPERTY}"
        );
        for raw in 1..=MAX_HEIGHT {
            let height = block::Height(raw);
            assert_eq!(
                state.sequencer().applying_contains(height),
                live.contains(&height),
                "{EVIDENCE_PROPERTY}: applying membership at {}",
                raw
            );
        }
    }
    summary
}

#[cfg(test)]
proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Out-of-order bodies submit only as a contiguous prefix, refreshes keep
    /// retained bodies, and refused submissions retry on the model's schedule.
    #[test]
    fn bodies_submit_in_contiguous_order(steps in prop::collection::vec(sequencing_step(), 1..80)) {
        run_sequencing(steps, |_| {});
    }

    /// A retired apply result settles only its own attempt and leaves the
    /// replacement epoch's ordering intact.
    #[test]
    fn retired_results_keep_current_ordering(
        retired_bodies in 1..=MAX_HEIGHT,
        steps in prop::collection::vec(reset_step(), 1..80),
    ) {
        run_authority_reset(retired_bodies, steps, |_| {});
    }

    /// Required evidence stays pending, in order, until IO delivers it.
    #[test]
    fn required_evidence_waits_for_delivery(steps in prop::collection::vec(evidence_step(), 1..60)) {
        run_required_evidence(steps, |_| {});
    }
}

#[test]
fn body_without_committed_authority_is_rejected() {
    let mut state = SequencerState::new(
        Sequencer::new(block::Height(0), 1),
        genesis_frontiers(),
        None,
        SeededRetryJitter::new([0; 32]),
    );
    assert!(matches!(
        state.accept_body(received(&test_block(), 1, 0)),
        BodyAcceptance::StaleScope
    ));
    assert_eq!(state.sequencer().reorder_len(), 0);
    assert_eq!(state.sequencer().applying_len(), 0);
}

#[test]
fn missing_direct_successor_cannot_prove_reset_anchor() {
    let anchor_hash = block::Hash([1; 32]);

    assert!(direct_successor_links_to_anchor(
        Some(anchor_hash),
        anchor_hash
    ));
    assert!(!direct_successor_links_to_anchor(
        Some(block::Hash([2; 32])),
        anchor_hash
    ));
    assert!(!direct_successor_links_to_anchor(None, anchor_hash));
}

/// Worked scenario: bodies 2 and 3 wait for body 1, then all three submit in order.
#[test]
fn predecessor_releases_retained_successors_in_order() {
    let block = test_block();
    let mut state = new_state(4);
    let now = time::Instant::now();
    for height in [3, 2] {
        assert_eq!(
            state.accept_body(received(&block, height, 0)),
            BodyAcceptance::Buffered
        );
    }
    assert!(submission_pass(&mut state, now, usize::MAX).is_empty());

    assert_eq!(
        state.accept_body(received(&block, 1, 0)),
        BodyAcceptance::Buffered
    );
    let heights: Vec<_> = submission_pass(&mut state, now, usize::MAX)
        .into_iter()
        .map(|accepted| accepted.height.0)
        .collect();
    assert_eq!(heights, [1, 2, 3]);
}

/// A full action channel keeps both evidence records pending, oldest first.
#[test]
fn full_action_channel_keeps_pending_evidence() {
    let block = test_block();
    let mut state = new_state(2);
    for height in [1, 2] {
        let _ = state.accept_body(received(&block, height, 0));
    }
    let attempts = submission_pass(&mut state, time::Instant::now(), usize::MAX);
    let [first, second] = attempts[..] else {
        panic!("both bodies are submitted");
    };

    let _ = state.on_control_input(
        apply_finished(first, source(), outcome(Completion::Committed, first), true),
        wall_now(),
    );
    let outputs = state.on_control_input(
        apply_finished(
            second,
            source(),
            outcome(Completion::ConsensusInvalid { attributed: true }, second),
            true,
        ),
        wall_now(),
    );
    assert!(outputs
        .operations
        .iter()
        .any(|operation| matches!(operation, SequencerOperation::DeliverEvidence)));
    assert_eq!(state.pending_evidence_len(), 1);

    let outputs = state.on_control_input(
        apply_finished(
            second,
            source(),
            outcome(Completion::Unavailable, second),
            true,
        ),
        wall_now(),
    );
    assert!(
        !outputs
            .operations
            .iter()
            .any(|operation| matches!(operation, SequencerOperation::DeliverEvidence)),
        "the retired attempt adds no evidence"
    );
    assert_eq!(
        state.pending_evidence().and_then(observed_evidence),
        Some(ExpectedEvidence::BodyInvalid(second.hash))
    );
    assert!(state.take_pending_evidence().is_some());
    assert_eq!(state.pending_evidence_len(), 0);
}

const ORDER_PROPERTY: &str = "block/contiguous-submission";
const RESET_PROPERTY: &str = "block/retired-apply-isolation";
const EVIDENCE_PROPERTY: &str = "block/required-evidence";

/// Applicable transitions in one decoder-v1 sequencer history.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct SequencerReplaySummary {
    /// Generated operations checked against the model.
    pub transitions: usize,
    /// New current bodies delivered by ordering or reset histories.
    pub bodies: usize,
    /// Current submissions accepted by ordering or reset histories.
    pub submitted: usize,
    /// Live apply completions processed by reset or evidence histories.
    pub completions: usize,
    /// Required evidence records produced by live completions.
    pub evidence_queued: usize,
    /// Pending evidence records delivered by the driver.
    pub evidence_delivered: usize,
    /// Refused submission passes or full-channel operations with pending evidence.
    pub backpressure: usize,
}

fn decode_order(data: &[u8]) -> Vec<SequencingStep> {
    data[..data.len().min(512)]
        .chunks(3)
        .map(|chunk| {
            let a = chunk[0];
            let b = chunk.get(1).copied().unwrap_or(0);
            let c = chunk.get(2).copied().unwrap_or(0);
            match a % 4 {
                0 => SequencingStep::Deliver(u32::from(b) % MAX_HEIGHT + 1),
                1 => SequencingStep::RefreshStateVersion(u64::from(b)),
                2 => SequencingStep::RefreshAuthority(u64::from(b) + 1),
                _ => SequencingStep::Submit {
                    accepts: usize::from(b) % (to_usize(MAX_HEIGHT) + 1),
                    after_ms: u64::from(c) * 10,
                },
            }
        })
        .collect()
}

fn decode_completion(byte: u8) -> Completion {
    match byte % 7 {
        0 => Completion::Committed,
        1 => Completion::Duplicate,
        2 => Completion::ConsensusInvalid { attributed: true },
        3 => Completion::ConsensusInvalid { attributed: false },
        4 => Completion::TamperedBody,
        5 => Completion::Unavailable,
        _ => Completion::TimedOut,
    }
}

fn decode_reset(data: &[u8]) -> Vec<ResetStep> {
    data[..data.len().min(511)]
        .chunks(3)
        .map(|chunk| {
            let a = chunk[0];
            let b = chunk.get(1).copied().unwrap_or(0);
            let c = chunk.get(2).copied().unwrap_or(0);
            match a % 4 {
                0 => ResetStep::DeliverCurrent(u32::from(b) % MAX_HEIGHT + 1),
                1 => ResetStep::DeliverRetired(u32::from(b) % MAX_HEIGHT + 1),
                2 => ResetStep::SubmitCurrent,
                _ => ResetStep::FinishRetired {
                    attempt: usize::from(b),
                    completion: decode_completion(c),
                    semantic: c & 128 == 0,
                },
            }
        })
        .collect()
}

fn decode_evidence(data: &[u8]) -> Vec<EvidenceStep> {
    data[..data.len().min(512)]
        .chunks(3)
        .map(|chunk| {
            let a = chunk[0];
            let b = chunk.get(1).copied().unwrap_or(0);
            let c = chunk.get(2).copied().unwrap_or(0);
            match a % 3 {
                0 => EvidenceStep::Finish {
                    attempt: usize::from(b),
                    completion: decode_completion(c),
                    semantic: c & 128 == 0,
                },
                1 => EvidenceStep::Deliver,
                _ => EvidenceStep::Full,
            }
        })
        .collect()
}

/// Replay at most 512 decoder-v1 bytes against the contiguous-submission model.
pub fn replay_sequencer_order_bytes(data: &[u8]) -> SequencerReplaySummary {
    run_sequencing(decode_order(data), |_| {})
}

/// Replay at most 512 decoder-v1 bytes against the retired-apply model.
/// The first byte chooses the number of retired applies; triples choose operations.
pub fn replay_sequencer_reset_bytes(data: &[u8]) -> SequencerReplaySummary {
    let Some((&first, rest)) = data.split_first() else {
        return SequencerReplaySummary::default();
    };
    run_authority_reset(
        u32::from(first) % MAX_HEIGHT + 1,
        decode_reset(rest),
        |_| {},
    )
}

/// Replay at most 512 decoder-v1 bytes against the required-evidence model.
pub fn replay_sequencer_evidence_bytes(data: &[u8]) -> SequencerReplaySummary {
    run_required_evidence(decode_evidence(data), |_| {})
}

#[cfg(test)]
proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]
    #[test]
    fn sequencer_bytes_use_the_existing_models(bytes in prop::collection::vec(any::<u8>(), 0..=512)) {
        replay_sequencer_order_bytes(&bytes);
        replay_sequencer_reset_bytes(&bytes);
        replay_sequencer_evidence_bytes(&bytes);
    }
}

#[cfg(test)]
const ORDER_WITNESS: &[u8] = &[0, 1, 0, 0, 0, 0, 3, 0, 0, 2, 1, 0, 3, 2, 10];
#[cfg(test)]
const RESET_WITNESS: &[u8] = &[0, 0, 0, 0, 2, 0, 0, 1, 0, 0, 3, 0, 2];
#[cfg(test)]
const EVIDENCE_WITNESS: &[u8] = &[0, 0, 2, 2, 0, 0, 1, 0, 0];

#[test]
#[allow(
    clippy::print_stdout,
    reason = "witnesses print applicability counters"
)]
fn sequencer_order_witness_retries_a_refused_contiguous_prefix() {
    let summary = replay_sequencer_order_bytes(ORDER_WITNESS);
    assert_eq!(
        summary,
        SequencerReplaySummary {
            transitions: 5,
            bodies: 2,
            submitted: 2,
            backpressure: 1,
            ..SequencerReplaySummary::default()
        }
    );
    assert_eq!(
        replay_sequencer_order_bytes(&[]),
        SequencerReplaySummary::default()
    );
    println!("tier=Machine target=block_sequencer_order decoder=1 {summary:?}");
}

#[test]
#[allow(
    clippy::print_stdout,
    reason = "witnesses print applicability counters"
)]
fn sequencer_reset_witness_isolates_a_retired_completion() {
    let summary = replay_sequencer_reset_bytes(RESET_WITNESS);
    assert_eq!(
        summary,
        SequencerReplaySummary {
            transitions: 4,
            bodies: 1,
            submitted: 1,
            completions: 1,
            ..SequencerReplaySummary::default()
        }
    );
    assert_eq!(
        replay_sequencer_reset_bytes(&[]),
        SequencerReplaySummary::default()
    );
    println!("tier=Machine target=block_sequencer_reset decoder=1 {summary:?}");
}

#[test]
#[allow(
    clippy::print_stdout,
    reason = "witnesses print applicability counters"
)]
fn sequencer_evidence_witness_retains_evidence_while_full() {
    let summary = replay_sequencer_evidence_bytes(EVIDENCE_WITNESS);
    assert_eq!(
        summary,
        SequencerReplaySummary {
            transitions: 3,
            completions: 1,
            evidence_queued: 1,
            evidence_delivered: 1,
            backpressure: 1,
            ..SequencerReplaySummary::default()
        }
    );
    assert_eq!(
        replay_sequencer_evidence_bytes(&[]),
        SequencerReplaySummary::default()
    );
    println!("tier=Machine target=block_sequencer_evidence decoder=1 {summary:?}");
}

#[test]
#[should_panic(expected = "block/contiguous-submission")]
fn sequencer_order_witness_rejects_lost_retained_body() {
    run_sequencing(decode_order(ORDER_WITNESS), |state| {
        state.sequencer.drop_reorder_from(block::Height(1));
    });
}

#[test]
#[should_panic(expected = "block/retired-apply-isolation")]
fn sequencer_reset_witness_rejects_retired_completion_resetting_current_work() {
    run_authority_reset(1, decode_reset(&RESET_WITNESS[1..]), |state| {
        state.sequencer.reset_to(block::Height(0), false);
    });
}

#[test]
#[should_panic(expected = "block/required-evidence")]
fn sequencer_evidence_witness_rejects_lost_evidence() {
    run_required_evidence(decode_evidence(EVIDENCE_WITNESS), |state| {
        state.pending_evidence.clear();
    });
}
