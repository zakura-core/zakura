//! Machine tests for committed-view authority and body-availability requests.
//!
//! The proptest drives [`ReactorState`] the way `ReactorIO` does: it applies a
//! committed view, runs the hint operation the update chose, and reports each
//! request's dispatch answer before the next input. It checks the state's
//! invariants and compares each update and request only with the view fields
//! the test changed. Named cases pin the supplier, hint, and epoch rules.

use std::{collections::BTreeSet, sync::Arc};

use chrono::{DateTime, TimeZone, Utc};
use proptest::prelude::*;
use zakura_header_chain::{
    AlarmSet, BodySizeHint, BodySizeHintBatch, BodyWorkEpoch, ChainScore, EngineMode, EvidenceId,
    FrontierSet, SuffixWork, VerifiedGeneration,
};

use super::*;

/// The raw fields of one generated committed view.
#[derive(Clone, Debug, PartialEq)]
struct Spec {
    version: u64,
    epoch: u64,
    header_generation: u64,
    verified_generation: u64,
    finalized: u32,
    verified: u32,
    header: u32,
    /// Distinguishes header hashes at one height, as a header reorg does.
    header_branch: u8,
    hint_revision: u64,
    /// First retained hint-batch revision; `None` publishes no batches.
    first_batch: Option<u64>,
    alarm: Option<AlarmSpec>,
}

#[derive(Copy, Clone, Debug, PartialEq)]
struct AlarmSpec {
    attempts: u32,
    suppliers: u32,
    /// Source bytes whose set digest the alarm recorded.
    digest_of: u8,
}

fn wall(seconds: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(1_700_000_000 + seconds, 0)
        .single()
        .expect("the test timestamp is valid")
}

fn source(byte: u8) -> SourceId {
    SourceId::from_digest([byte; 32])
}

/// The eligible sources named by the set bits of `bits`.
fn sources(bits: u8) -> BTreeSet<SourceId> {
    (0..4)
        .filter(|bit| bits & (1 << bit) != 0)
        .map(|bit| source(bit + 1))
        .collect()
}

fn branch_hash(height: u32, branch: u8) -> block::Hash {
    hash(0xC0 ^ branch, height)
}

fn frontier(height: u32, hash: block::Hash) -> Frontier {
    Frontier {
        height: block::Height(height),
        hash,
    }
}

fn committed(spec: &Spec) -> CommittedHeaderChainView {
    let header = frontier(spec.header, branch_hash(spec.header, spec.header_branch));
    let summary = spec.alarm.map(|alarm| BodyUnavailableSummary {
        started_at: wall(0),
        attempts: alarm.attempts,
        suppliers: alarm.suppliers,
        supplier_set_digest: BodyUnavailableSummary::supplier_set_digest(&sources(alarm.digest_of)),
        alarmed: true,
        next_probe_at: wall(1),
    });
    let snapshot = EngineSnapshot {
        mode: EngineMode::Integrated,
        state_version: StateVersion::new(spec.version),
        header_generation: HeaderGeneration::new(spec.header_generation),
        verified_generation: VerifiedGeneration::new(spec.verified_generation),
        frontiers: FrontierSet {
            finalized: frontier(spec.finalized, verified_hash(spec.finalized)),
            header_best: header,
            verified_best: frontier(spec.verified, verified_hash(spec.verified)),
        },
        header_best_score: ChainScore::new(SuffixWork::zero(), header.hash),
        oldest_retained_height: block::Height(spec.finalized),
        alarms: AlarmSet {
            header_best_body_unavailable: summary,
            ..AlarmSet::default()
        },
    };
    let mut view = CommittedHeaderChainView::new(snapshot, BodyWorkEpoch::new(spec.epoch));
    view.body_size_hint_revision = spec.hint_revision;
    view.body_size_hint_batches = spec.first_batch.map(|first| {
        (first..=spec.hint_revision.max(first))
            .map(|revision| BodySizeHintBatch {
                revision,
                updates: Arc::from(vec![(
                    block::Height(spec.verified + 1),
                    verified_hash(spec.verified + 1),
                    BodySizeHint::Unknown,
                )]),
            })
            .collect::<Vec<_>>()
            .into()
    });
    view
}

fn base_spec() -> Spec {
    Spec {
        version: 1,
        epoch: 0,
        header_generation: 0,
        verified_generation: 0,
        finalized: 0,
        verified: 0,
        header: 20,
        header_branch: 0,
        hint_revision: 0,
        first_batch: None,
        alarm: None,
    }
}

/// The IO half of a committed view: run the hint operation the update chose.
fn observe(state: &mut ReactorState, spec: &Spec, now: time::Instant) -> CommittedViewUpdate {
    let update = state.observe_committed_view(committed(spec), now);
    if let HintUpdate::Restart { revision } = update.hints {
        state
            .queries
            .start_hint_refresh(revision, Some((block::Height(1), block::Height(400))));
    }
    update
}

/// Open a forced refill against the state's own authority.
fn committed_refill(state: &mut ReactorState, accept: bool, now: Instant) -> Refill {
    refill(state, true, work(Some(1), 0, 1), accept, now)
}

#[test]
fn authority_change_retires_the_pending_query_and_forces_refill() {
    let now = Instant::now();
    let mut state = reactor_state(0, 20);
    let _ = observe(&mut state, &base_spec(), time::Instant::now());
    expect_accepted(committed_refill(&mut state, true, now));

    let update = observe(
        &mut state,
        &Spec {
            header_generation: 1,
            ..base_spec()
        },
        time::Instant::now(),
    );
    assert!(matches!(
        update.authority,
        Some(AuthorityUpdate::Refreshed(_))
    ));
    assert_eq!(state.queries.pending(), None);
    assert!(update.forces_refill(false));
}

/// A new epoch with missed hint history starts no refresh: the Sequencer
/// resets the queued work that IO would snapshot as the refresh bounds.
#[test]
fn epoch_change_with_missed_hints_starts_no_refresh() {
    let mut state = reactor_state(0, 20);
    let _ = observe(&mut state, &base_spec(), time::Instant::now());

    let update = observe(
        &mut state,
        &Spec {
            epoch: 1,
            hint_revision: 3,
            first_batch: Some(3),
            ..base_spec()
        },
        time::Instant::now(),
    );
    assert!(matches!(
        update.authority,
        Some(AuthorityUpdate::EpochChanged { .. })
    ));
    assert_eq!(update.hints, HintUpdate::Unchanged);
    assert_eq!(state.queries.hint_refresh(), None);
    assert!(update.forces_refill(false));
}

#[test]
fn unchanged_view_keeps_the_pending_query_and_needs_no_refill() {
    let now = Instant::now();
    let mut state = reactor_state(0, 20);
    let _ = observe(&mut state, &base_spec(), time::Instant::now());
    let query = expect_accepted(committed_refill(&mut state, true, now));

    let update = observe(
        &mut state,
        &Spec {
            version: 2,
            ..base_spec()
        },
        time::Instant::now(),
    );
    assert_eq!(update.state_version, Some(StateVersion::new(2)));
    assert_eq!(update.authority, None);
    assert_eq!(update.frontiers, FrontierUpdate::Unchanged);
    assert_eq!(state.queries.pending(), Some(query));
    assert!(!update.forces_refill(false));
}

#[test]
fn far_ahead_headers_hold_an_empty_node() {
    let mut state = ReactorState::new(
        true,
        ReactorProgress::new(
            frontiers(0),
            (block::Height(0), header_hash(0)),
            initial_view(frontiers(0)),
        ),
        BootstrapGate::new(false, None),
        None,
    );
    let now = time::Instant::now();
    let _ = observe(
        &mut state,
        &Spec {
            header: EMPTY_STATE_HEADER_QUIET_MIN_LAG,
            ..base_spec()
        },
        now,
    );
    assert_eq!(
        state.bootstrap.header_quiet_until(),
        Some(now + EMPTY_STATE_HEADER_QUIET_PERIOD)
    );
    assert_eq!(
        state.progress.best_header_tip,
        block::Height(EMPTY_STATE_HEADER_QUIET_MIN_LAG)
    );
}

#[test]
fn refused_alarm_request_is_proposed_again() {
    let mut state = reactor_state(0, 20);
    let alarmed = Spec {
        alarm: Some(AlarmSpec {
            attempts: 3,
            suppliers: 1,
            digest_of: 0b0001,
        }),
        ..base_spec()
    };
    let _ = observe(&mut state, &alarmed, time::Instant::now());
    let eligible = sources(0b0011);

    // A refusal records nothing.
    let first = state
        .propose_supplier_restart(&eligible, wall(5))
        .expect("a second supplier restarts the alarm");
    drop(first);
    let again = state
        .propose_supplier_restart(&eligible, wall(6))
        .expect("the refused restart is proposed again");
    state.alarm_request_accepted(again);
    assert!(state.propose_supplier_restart(&eligible, wall(7)).is_none());

    // A new committed view drops the accepted key.
    let _ = observe(&mut state, &alarmed, time::Instant::now());
    assert!(state.propose_supplier_restart(&eligible, wall(8)).is_some());
}

#[test]
fn only_misbehavior_reports_are_optional_actions() {
    let now = Instant::now();
    let mut state = reactor_state(0, 20);
    let alarmed = Spec {
        alarm: Some(AlarmSpec {
            attempts: 1,
            suppliers: 1,
            digest_of: 0b0001,
        }),
        ..base_spec()
    };
    let _ = observe(&mut state, &alarmed, time::Instant::now());
    let start = match state.open_query(true).0 {
        QueryOpening::NeedsWork => QueryStart::Work(work(Some(1), 0, 1)),
        other => panic!("a forced refill opens, got {other:?}"),
    };
    let QueryPlan::Dispatch(query) = state.propose_query(true, start) else {
        panic!("the committed view authorizes the query");
    };
    let restart = state
        .propose_supplier_restart(&sources(0b0011), wall(1))
        .expect("a second supplier restarts the alarm");
    let retry = state
        .propose_operator_retry(header_hash_of(&alarmed), &sources(0b0001), wall(1))
        .expect("the alarmed header accepts an operator retry");

    for required in [query.action(), restart.action(), retry.action()] {
        assert_eq!(
            ActionClass::of(&required),
            ActionClass::Required,
            "{required:?}"
        );
    }
    let misbehavior = BlockSyncAction::Misbehavior {
        peer: ZakuraPeerId::new(vec![1; 32]).expect("the test peer id is within bounds"),
        reason: crate::zakura::block_sync::BlockSyncMisbehavior::InvalidBlock,
    };
    assert_eq!(ActionClass::of(&misbehavior), ActionClass::Optional);
    let _ = state.queries.refused(query, ActionRefusal::Full, now);
}

/// The alarm restarts its body search only for a supplier it has not tried:
/// more eligible suppliers than it recorded, or as many under another set.
#[test]
fn a_restart_needs_a_supplier_the_alarm_has_not_tried() {
    for (suppliers, digest_of, eligible, restarts) in [
        (1, 0b0001, 0b0011, true),
        (1, 0b0001, 0b0010, true),
        (1, 0b0001, 0b0001, false),
        (2, 0b0011, 0b0001, false),
        (2, 0b0011, 0b0011, false),
        (2, 0b0011, 0b0101, true),
    ] {
        let mut state = reactor_state(0, 20);
        let alarmed = Spec {
            alarm: Some(AlarmSpec {
                attempts: 1,
                suppliers,
                digest_of,
            }),
            ..base_spec()
        };
        let _ = observe(&mut state, &alarmed, time::Instant::now());
        assert_eq!(
            state
                .propose_supplier_restart(&sources(eligible), wall(1))
                .is_some(),
            restarts,
            "{suppliers} suppliers {digest_of:#06b}, eligible {eligible:#06b}"
        );
    }
}

/// A changed hint revision applies the view's batches when they continue from
/// the previous revision, and restarts from state when one is missing. Either
/// retires the pending query, whose sizes may predate the correction. A new
/// epoch does neither: the Sequencer resets the queued work.
#[test]
fn a_hint_change_applies_contiguous_batches_and_restarts_after_a_gap() {
    let now = Instant::now();
    for (epoch, hint_revision, first_batch, expected) in [
        (0, 0, None, HintUpdate::Unchanged),
        (0, 1, Some(1), HintUpdate::Apply { after_revision: 0 }),
        (0, 3, Some(1), HintUpdate::Apply { after_revision: 0 }),
        (0, 3, Some(2), HintUpdate::Restart { revision: 3 }),
        (0, 1, None, HintUpdate::Restart { revision: 1 }),
        (1, 3, Some(3), HintUpdate::Unchanged),
    ] {
        let mut state = reactor_state(0, 20);
        let _ = observe(&mut state, &base_spec(), time::Instant::now());
        let query = expect_accepted(committed_refill(&mut state, true, now));
        let update = observe(
            &mut state,
            &Spec {
                epoch,
                hint_revision,
                first_batch,
                ..base_spec()
            },
            time::Instant::now(),
        );
        assert_eq!(
            update.hints, expected,
            "epoch {epoch}, revision {hint_revision}"
        );
        let kept = epoch == 0 && hint_revision == 0;
        assert_eq!(
            state.queries.pending(),
            kept.then_some(query),
            "epoch {epoch}, revision {hint_revision}"
        );
    }
}

/// A new epoch resets the Sequencer to the epoch's frontiers through its
/// authority update, so the frontier update reports no advance. The same
/// progress within an epoch is an advance.
#[test]
fn a_new_epoch_reports_its_frontiers_through_the_authority() {
    for (epoch, frontiers) in [
        (0, FrontierUpdate::Advanced(verified_frontiers(5))),
        (1, FrontierUpdate::Unchanged),
    ] {
        let mut state = reactor_state(0, 20);
        let _ = observe(&mut state, &base_spec(), time::Instant::now());
        let advanced = Spec {
            epoch,
            verified: 5,
            header: 25,
            ..base_spec()
        };
        let update = observe(&mut state, &advanced, time::Instant::now());
        assert_eq!(update.frontiers, frontiers, "epoch {epoch}");
        if epoch == 1 {
            assert_eq!(
                update.authority,
                Some(AuthorityUpdate::EpochChanged {
                    authority: BodyWorkAuthority::for_view(&committed(&advanced)),
                    frontiers: verified_frontiers(5),
                })
            );
        }
    }
}

// ---- property ----

#[derive(Clone, Debug)]
enum Input {
    View(ViewStep),
    Refill {
        accept: bool,
    },
    Restart {
        bits: u8,
        accept: bool,
    },
    Retry {
        matching: bool,
        bits: u8,
        accept: bool,
    },
}

#[derive(Clone, Debug)]
struct ViewStep {
    version: bool,
    epoch: bool,
    header_generation: bool,
    verified_generation: bool,
    finalized: u32,
    verified: u32,
    header: u32,
    header_branch: Option<u8>,
    /// 0 keeps the revision. 1 publishes a contiguous batch. 2 publishes a gap.
    /// 3 publishes no batches.
    hints: u8,
    alarm: Option<AlarmSpec>,
}

fn alarm() -> impl Strategy<Value = Option<AlarmSpec>> {
    prop::option::of(
        (0u32..4, 0u32..4, 0u8..16).prop_map(|(attempts, suppliers, digest_of)| AlarmSpec {
            attempts,
            suppliers,
            digest_of,
        }),
    )
}

fn view_step() -> impl Strategy<Value = ViewStep> {
    (
        (
            any::<bool>(),
            prop::bool::weighted(0.2),
            prop::bool::weighted(0.2),
        ),
        (prop::bool::weighted(0.2), 0u32..2, 0u32..3, 0u32..3),
        (prop::option::weighted(0.2, 0u8..3), 0u8..4, alarm()),
    )
        .prop_map(
            |(
                (version, epoch, header_generation),
                (verified_generation, finalized, verified, header),
                (header_branch, hints, alarm),
            )| ViewStep {
                version,
                epoch,
                header_generation,
                verified_generation,
                finalized,
                verified,
                header,
                header_branch,
                hints,
                alarm,
            },
        )
}

fn input() -> impl Strategy<Value = Input> {
    prop_oneof![
        3 => view_step().prop_map(Input::View),
        2 => any::<bool>().prop_map(|accept| Input::Refill { accept }),
        2 => (0u8..16, any::<bool>()).prop_map(|(bits, accept)| Input::Restart { bits, accept }),
        2 => (any::<bool>(), 0u8..16, any::<bool>())
            .prop_map(|(matching, bits, accept)| Input::Retry { matching, bits, accept }),
    ]
}

/// A request's deduplication key: (state version, header hash, supplier-set
/// digest).
type Key = (u64, block::Hash, [u8; 32]);

/// The last view the test sent, the requests it accepted against that view,
/// and every request's evidence.
#[derive(Default)]
struct Sent {
    spec: Option<Spec>,
    /// The eligible-source bits of the last accepted restart and retry.
    accepted_restart: Option<u8>,
    accepted_retry: Option<u8>,
    evidence: Vec<((u8, Key), EvidenceId)>,
}

fn header_hash_of(spec: &Spec) -> block::Hash {
    branch_hash(spec.header, spec.header_branch)
}

impl Sent {
    /// The next valid view: the previous one with `step`'s changes.
    fn next_spec(&self, step: &ViewStep) -> Spec {
        let prev = self.spec.clone().unwrap_or_else(base_spec);
        let epoch = prev.epoch + u64::from(step.epoch);
        // Same-epoch verified progress never retreats.
        let verified = if step.epoch {
            step.verified
        } else {
            prev.verified + step.verified
        };
        let finalized = (prev.finalized + step.finalized).min(verified);
        let (hint_revision, first_batch) = match step.hints {
            0 => (prev.hint_revision, prev.first_batch),
            1 => (prev.hint_revision + 1, Some(prev.hint_revision + 1)),
            2 => (prev.hint_revision + 3, Some(prev.hint_revision + 3)),
            _ => (prev.hint_revision + 1, None),
        };
        Spec {
            version: prev.version + u64::from(step.version),
            epoch,
            header_generation: prev.header_generation + u64::from(step.header_generation),
            verified_generation: prev.verified_generation + u64::from(step.verified_generation),
            finalized,
            verified,
            header: (prev.header + step.header).max(verified + 20),
            header_branch: step.header_branch.unwrap_or(prev.header_branch),
            hint_revision,
            first_batch,
            alarm: step.alarm,
        }
    }

    /// One key always maps to one evidence ID, and distinct keys to distinct IDs.
    fn check_evidence(
        &mut self,
        kind: u8,
        key: Key,
        evidence: EvidenceId,
    ) -> Result<(), TestCaseError> {
        for (known, known_evidence) in &self.evidence {
            prop_assert_eq!(*known == (kind, key), *known_evidence == evidence);
        }
        self.evidence.push(((kind, key), evidence));
        Ok(())
    }
}

fn verified_frontiers(verified: u32) -> BlockSyncFrontiers {
    frontiers_of(&Spec {
        verified,
        ..base_spec()
    })
}

/// The verified and finalized frontiers `spec` names.
fn frontiers_of(spec: &Spec) -> BlockSyncFrontiers {
    BlockSyncFrontiers {
        finalized_height: block::Height(spec.finalized),
        verified_block_tip: block::Height(spec.verified),
        verified_block_hash: verified_hash(spec.verified),
    }
}

/// Check a view's update against the fields the test changed, after IO ran its
/// hint operation.
fn check_view(
    state: &ReactorState,
    update: &CommittedViewUpdate,
    prev: Option<&Spec>,
    spec: &Spec,
) -> Result<(), TestCaseError> {
    let current = committed(spec);
    let scope = BodyWorkAuthority::for_view(&current);
    let previous_scope = prev.map(|prev| BodyWorkAuthority::for_view(&committed(prev)));
    let authority_changed = previous_scope != Some(scope);
    let epoch_changed = prev.is_some_and(|prev| prev.epoch != spec.epoch);
    let revision_changed = prev.is_some_and(|prev| prev.hint_revision != spec.hint_revision);
    let frontiers_changed =
        prev.is_some_and(|prev| (prev.verified, prev.finalized) != (spec.verified, spec.finalized));
    let header_changed = prev.is_none_or(|prev| {
        (prev.header, header_hash_of(prev)) != (spec.header, header_hash_of(spec))
    });

    // The update names the previous view's alarm that this view no longer raises.
    let cleared = prev
        .filter(|prev| prev.alarm.is_some())
        .map(header_hash_of)
        .filter(|hash| spec.alarm.is_none() || header_hash_of(spec) != *hash);
    prop_assert_eq!(
        update.alarm_cleared,
        cleared.map(|hash| (previous_scope.expect("an alarm has a view"), hash))
    );
    prop_assert_eq!(
        update.state_version.map(StateVersion::get),
        prev.is_none_or(|prev| prev.version != spec.version)
            .then_some(spec.version)
    );
    match update.authority {
        None => prop_assert!(!authority_changed),
        Some(AuthorityUpdate::EpochChanged {
            authority,
            frontiers,
        }) => {
            prop_assert!(authority_changed && epoch_changed);
            prop_assert_eq!(authority, scope);
            prop_assert_eq!(frontiers, frontiers_of(spec));
        }
        Some(AuthorityUpdate::Refreshed(authority)) => {
            prop_assert!(authority_changed && !epoch_changed);
            prop_assert_eq!(authority, scope);
        }
    }
    match update.frontiers {
        FrontierUpdate::Initial(frontiers) => {
            prop_assert!(prev.is_none());
            prop_assert_eq!(frontiers, frontiers_of(spec));
        }
        FrontierUpdate::Advanced(frontiers) => {
            prop_assert!(frontiers_changed);
            prop_assert_eq!(frontiers, frontiers_of(spec));
        }
        FrontierUpdate::Unchanged => {
            prop_assert!(prev.is_some() && (epoch_changed || !frontiers_changed));
        }
    }
    match update.hints {
        HintUpdate::Unchanged => prop_assert!(epoch_changed || !revision_changed),
        HintUpdate::Apply { after_revision } => {
            prop_assert!(revision_changed);
            prop_assert_eq!(Some(after_revision), prev.map(|prev| prev.hint_revision));
        }
        HintUpdate::Restart { revision } => {
            prop_assert!(revision_changed);
            prop_assert_eq!(revision, spec.hint_revision);
        }
    }
    if authority_changed || revision_changed {
        prop_assert_eq!(state.queries.pending(), None);
    }
    if epoch_changed {
        prop_assert_eq!(state.queries.hint_refresh(), None);
    }
    if authority_changed || header_changed {
        prop_assert!(update.forces_refill(state.queries.hint_refresh().is_some()));
    }
    prop_assert_eq!(state.committed_view(), Some(&current));
    prop_assert_eq!(state.body_work_scope(), Some(scope));
    prop_assert_eq!(state.progress.best_header_tip.0, spec.header);
    prop_assert_eq!(state.progress.best_header_hash, header_hash_of(spec));
    Ok(())
}

proptest! {
    /// Committed views, refills, and body-availability requests keep the
    /// state's invariants. Each update and request reports what the views the
    /// test sent changed, and an accepted request is not proposed again until
    /// the next view.
    #[test]
    fn committed_views_and_requests_keep_every_property(inputs in prop::collection::vec(input(), 1..40)) {
        let now = Instant::now();
        let mut state = reactor_state(0, 20);
        let mut sent = Sent::default();
        for input in inputs {
            match input {
                Input::View(step) => {
                    let spec = sent.next_spec(&step);
                    let update = observe(&mut state, &spec, time::Instant::now());
                    check_view(&state, &update, sent.spec.as_ref(), &spec)?;
                    sent.spec = Some(spec);
                    sent.accepted_restart = None;
                    sent.accepted_retry = None;
                }
                Input::Refill { accept } => {
                    let _ = committed_refill(&mut state, accept, now);
                }
                Input::Restart { bits, accept } => {
                    let actual = state.propose_supplier_restart(&sources(bits), wall(9));
                    let alarm = sent.spec.as_ref().and_then(|spec| spec.alarm);
                    prop_assert!(alarm.is_some() || actual.is_none(), "a restart needs an alarm");
                    prop_assert!(
                        sent.accepted_restart != Some(bits) || actual.is_none(),
                        "an accepted restart is not proposed again"
                    );
                    let Some(request) = actual else { continue };
                    let spec = sent.spec.as_ref().expect("an alarm has a view");
                    let alarm = alarm.expect("checked above");
                    let action = request.action();
                    let BlockSyncAction::RestartBodyAvailability { expected_version, discovery } =
                        action.clone()
                    else {
                        panic!("a supplier restart proposes RestartBodyAvailability");
                    };
                    let key = (
                        spec.version,
                        header_hash_of(spec),
                        BodyUnavailableSummary::supplier_set_digest(&sources(bits)),
                    );
                    prop_assert_eq!(expected_version.get(), key.0);
                    prop_assert_eq!(discovery.hash, key.1);
                    prop_assert_eq!(discovery.availability.supplier_set_digest, key.2);
                    prop_assert_eq!(discovery.availability.suppliers, sources(bits).len() as u32);
                    prop_assert_eq!(discovery.availability.attempts, alarm.attempts);
                    prop_assert_eq!(discovery.availability.started_at, wall(0));
                    prop_assert_eq!(discovery.availability.next_probe_at, wall(9));
                    prop_assert!(discovery.availability.alarmed);
                    sent.check_evidence(0, key, discovery.evidence)?;
                    if accept {
                        state.alarm_request_accepted(request);
                        sent.accepted_restart = Some(bits);
                    } else {
                        // A refusal records nothing, so the same call proposes it again.
                        let again = state.propose_supplier_restart(&sources(bits), wall(9));
                        prop_assert_eq!(
                            format!("{:?}", again.map(|request| request.action())),
                            format!("{:?}", Some(action))
                        );
                    }
                }
                Input::Retry { matching, bits, accept } => {
                    let hash = match (matching, sent.spec.as_ref()) {
                        (true, Some(spec)) => header_hash_of(spec),
                        _ => block::Hash([0xEE; 32]),
                    };
                    let actual = state.propose_operator_retry(hash, &sources(bits), wall(9));
                    let alarmed = sent.spec.as_ref().is_some_and(|spec| spec.alarm.is_some());
                    prop_assert!(
                        (alarmed && matching && bits != 0) || actual.is_none(),
                        "a retry needs the alarmed hash and a supplier"
                    );
                    prop_assert!(
                        sent.accepted_retry != Some(bits) || actual.is_none(),
                        "an accepted retry is not proposed again"
                    );
                    let Some(request) = actual else { continue };
                    let spec = sent.spec.as_ref().expect("an alarm has a view");
                    let action = request.action();
                    let BlockSyncAction::RetryBodyAvailability { expected_version, retry } =
                        action.clone()
                    else {
                        panic!("an operator retry proposes RetryBodyAvailability");
                    };
                    let key = (
                        spec.version,
                        header_hash_of(spec),
                        BodyUnavailableSummary::supplier_set_digest(&sources(bits)),
                    );
                    prop_assert_eq!(expected_version.get(), key.0);
                    prop_assert_eq!(retry.hash, key.1);
                    prop_assert_eq!(retry.availability.supplier_set_digest, key.2);
                    prop_assert_eq!(retry.availability.suppliers, sources(bits).len() as u32);
                    prop_assert_eq!(retry.availability.attempts, 0);
                    prop_assert_eq!(retry.availability.started_at, wall(9));
                    prop_assert_eq!(retry.availability.next_probe_at, wall(9));
                    prop_assert!(!retry.availability.alarmed);
                    sent.check_evidence(1, key, retry.evidence)?;
                    if accept {
                        state.alarm_request_accepted(request);
                        sent.accepted_retry = Some(bits);
                    } else {
                        let again = state.propose_operator_retry(hash, &sources(bits), wall(9));
                        prop_assert_eq!(
                            format!("{:?}", again.map(|request| request.action())),
                            format!("{:?}", Some(action))
                        );
                    }
                }
            }
            state.assert_invariants();
        }
    }
}
