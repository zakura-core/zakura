//! Machine tests for committed-view authority and body-availability requests.
//!
//! The proptest drives [`ReactorState`] the way `ReactorIO` does: it applies a
//! committed view, runs the hint operation the update chose, and reports each
//! request's dispatch answer before the next input. An independent model
//! predicts every update and request from the raw view fields.

use std::{collections::BTreeSet, sync::Arc};

use chrono::{DateTime, TimeZone, Utc};
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
    let scope = state.body_work_scope();
    refill(state, true, work(Some(1), 0, 1), scope, accept, now)
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
    let scope = state.body_work_scope();
    let start = match state.open_query(true) {
        QueryOpening::NeedsWork => QueryStart::Work(work(Some(1), 0, 1)),
        other => panic!("a forced refill opens, got {other:?}"),
    };
    let QueryPlan::Dispatch(query) = state.propose_query(true, start, scope) else {
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
    state.queries.refused(query, now);
}

// ---- model ----

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

/// The independent model: the previous view's raw fields, the pending query,
/// the hint refresh, and the accepted request keys.
struct Model {
    spec: Option<Spec>,
    pending: bool,
    hint_refresh: bool,
    accepted_restart: Option<(u64, block::Hash, [u8; 32])>,
    accepted_retry: Option<(u64, block::Hash, [u8; 32])>,
    evidence: Vec<((u8, u64, block::Hash, [u8; 32]), EvidenceId)>,
}

/// The fields a body-work authority depends on.
fn authority_key(spec: &Spec) -> (u64, u64, u64, u32, u32, u8) {
    (
        spec.epoch,
        spec.header_generation,
        spec.verified_generation,
        spec.finalized,
        spec.header,
        spec.header_branch,
    )
}

fn header_hash_of(spec: &Spec) -> block::Hash {
    branch_hash(spec.header, spec.header_branch)
}

/// The model's prediction for one view.
#[derive(Debug, PartialEq)]
struct Expected {
    alarm_cleared: Option<block::Hash>,
    state_version: Option<u64>,
    /// `Some(true)` for a new epoch, `Some(false)` for a refresh.
    authority: Option<bool>,
    frontiers: FrontierUpdate,
    hints: HintUpdate,
    forces_refill: bool,
}

impl Model {
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

    fn observe(&mut self, spec: &Spec) -> Expected {
        let prev = self.spec.replace(spec.clone());
        self.accepted_restart = None;
        self.accepted_retry = None;
        let alarm_cleared = prev
            .as_ref()
            .filter(|prev| prev.alarm.is_some())
            .map(header_hash_of)
            .filter(|hash| spec.alarm.is_none() || header_hash_of(spec) != *hash);
        let state_version = prev
            .as_ref()
            .is_none_or(|prev| prev.version != spec.version)
            .then_some(spec.version);
        let epoch_changed = prev.as_ref().is_some_and(|prev| prev.epoch != spec.epoch);
        let authority_changed = prev
            .as_ref()
            .is_none_or(|prev| authority_key(prev) != authority_key(spec));
        let authority = authority_changed.then_some(epoch_changed);
        if authority_changed {
            self.pending = false;
        }
        let frontiers = match prev.as_ref() {
            None => FrontierUpdate::Initial(model_frontiers(spec)),
            Some(_) if epoch_changed => FrontierUpdate::Unchanged,
            Some(prev) if (prev.verified, prev.finalized) != (spec.verified, spec.finalized) => {
                FrontierUpdate::Advanced(model_frontiers(spec))
            }
            Some(_) => FrontierUpdate::Unchanged,
        };
        if epoch_changed {
            self.hint_refresh = false;
        }
        let old_revision = prev
            .as_ref()
            .map(|prev| prev.hint_revision)
            .filter(|old| *old != spec.hint_revision);
        let refreshed_pending = old_revision.is_some() && self.pending;
        let hints = match old_revision {
            None => HintUpdate::Unchanged,
            Some(old) if spec.first_batch.is_some_and(|first| first <= old + 1) => {
                HintUpdate::Apply {
                    after_revision: old,
                }
            }
            Some(_) => {
                self.hint_refresh = true;
                HintUpdate::Restart {
                    revision: spec.hint_revision,
                }
            }
        };
        if old_revision.is_some() {
            self.pending = false;
        }
        let header_changed = prev.as_ref().is_none_or(|prev| {
            header_hash_of(prev) != header_hash_of(spec) || prev.header != spec.header
        });
        Expected {
            alarm_cleared,
            state_version,
            authority,
            frontiers,
            hints,
            forces_refill: header_changed
                || authority_changed
                || refreshed_pending
                || (old_revision.is_some() && self.hint_refresh),
        }
    }

    /// The restart key the model expects a proposal for, if any.
    fn restart(&self, bits: u8) -> Option<(u64, block::Hash, [u8; 32])> {
        let spec = self.spec.as_ref()?;
        let alarm = spec.alarm?;
        let eligible = sources(bits);
        let suppliers = eligible.len() as u32;
        let digest = BodyUnavailableSummary::supplier_set_digest(&eligible);
        let recorded = BodyUnavailableSummary::supplier_set_digest(&sources(alarm.digest_of));
        let new_supplier =
            suppliers > alarm.suppliers || (suppliers == alarm.suppliers && digest != recorded);
        let key = (spec.version, header_hash_of(spec), digest);
        (new_supplier && self.accepted_restart != Some(key)).then_some(key)
    }

    /// The operator-retry key the model expects a proposal for, if any.
    fn retry(&self, matching: bool, bits: u8) -> Option<(u64, block::Hash, [u8; 32])> {
        let spec = self.spec.as_ref()?;
        spec.alarm?;
        let eligible = sources(bits);
        let key = (
            spec.version,
            header_hash_of(spec),
            BodyUnavailableSummary::supplier_set_digest(&eligible),
        );
        (matching && !eligible.is_empty() && self.accepted_retry != Some(key)).then_some(key)
    }

    /// Check that one key always maps to one evidence ID, and distinct keys to distinct IDs.
    fn check_evidence(
        &mut self,
        kind: u8,
        key: (u64, block::Hash, [u8; 32]),
        evidence: EvidenceId,
    ) -> Result<(), TestCaseError> {
        let key = (kind, key.0, key.1, key.2);
        for (known, known_evidence) in &self.evidence {
            prop_assert_eq!(*known == key, *known_evidence == evidence);
        }
        self.evidence.push((key, evidence));
        Ok(())
    }
}

fn model_frontiers(spec: &Spec) -> BlockSyncFrontiers {
    BlockSyncFrontiers {
        finalized_height: block::Height(spec.finalized),
        verified_block_tip: block::Height(spec.verified),
        verified_block_hash: verified_hash(spec.verified),
    }
}

fn check_update(
    actual: &CommittedViewUpdate,
    previous: Option<&CommittedHeaderChainView>,
    current: &CommittedHeaderChainView,
    expected: &Expected,
    hint_refresh_active: bool,
) -> Result<(), TestCaseError> {
    prop_assert_eq!(
        actual.alarm_cleared.map(|(_, hash)| hash),
        expected.alarm_cleared
    );
    if let Some((scope, _)) = actual.alarm_cleared {
        let previous = previous.expect("a cleared alarm has a previous view");
        prop_assert_eq!(scope, BodyWorkAuthority::for_view(previous));
    }
    prop_assert_eq!(
        actual.state_version.map(StateVersion::get),
        expected.state_version
    );
    let current_authority = BodyWorkAuthority::for_view(current);
    match (actual.authority, expected.authority) {
        (None, None) => {}
        (
            Some(AuthorityUpdate::EpochChanged {
                authority,
                frontiers,
            }),
            Some(true),
        ) => {
            prop_assert_eq!(authority, current_authority);
            prop_assert_eq!(frontiers, block_sync_frontiers(current));
        }
        (Some(AuthorityUpdate::Refreshed(authority)), Some(false)) => {
            prop_assert_eq!(authority, current_authority);
        }
        (actual, expected) => prop_assert!(false, "authority {actual:?}, expected {expected:?}"),
    }
    prop_assert_eq!(actual.frontiers, expected.frontiers);
    prop_assert_eq!(actual.hints, expected.hints);
    prop_assert_eq!(
        actual.forces_refill(hint_refresh_active),
        expected.forces_refill
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Committed views, refills, and body-availability requests keep the
    /// update, pending query, hint refresh, mirrors, and accepted request keys
    /// equal to the model's after every step.
    #[test]
    fn committed_view_updates_match_model(inputs in prop::collection::vec(input(), 1..40)) {
        let now = Instant::now();
        let mut state = reactor_state(0, 20);
        let mut model = Model {
            spec: None,
            pending: false,
            hint_refresh: false,
            accepted_restart: None,
            accepted_retry: None,
            evidence: Vec::new(),
        };
        for input in inputs {
            match input {
                Input::View(step) => {
                    let spec = model.next_spec(&step);
                    let previous = state.committed_view().cloned();
                    let actual = observe(&mut state, &spec, time::Instant::now());
                    let expected = model.observe(&spec);
                    let current = committed(&spec);
                    check_update(
                        &actual,
                        previous.as_ref(),
                        &current,
                        &expected,
                        state.queries.hint_refresh().is_some(),
                    )?;
                    prop_assert_eq!(state.committed_view(), Some(&current));
                    prop_assert_eq!(state.body_work_scope(), Some(BodyWorkAuthority::for_view(&current)));
                    prop_assert_eq!(state.progress.best_header_tip.0, spec.header);
                    prop_assert_eq!(state.progress.best_header_hash, header_hash_of(&spec));
                }
                Input::Refill { accept } => {
                    if let Refill::Accepted(_) = committed_refill(&mut state, accept, now) {
                        model.pending = true;
                    }
                }
                Input::Restart { bits, accept } => {
                    let actual = state.propose_supplier_restart(&sources(bits), wall(9));
                    let expected = model.restart(bits);
                    prop_assert_eq!(actual.is_some(), expected.is_some(), "restart {:?}", actual);
                    if let (Some(request), Some(key)) = (actual, expected) {
                        let BlockSyncAction::RestartBodyAvailability { expected_version, discovery } =
                            request.action()
                        else {
                            panic!("a supplier restart proposes RestartBodyAvailability");
                        };
                        let alarm = model.spec.as_ref().and_then(|spec| spec.alarm).expect("alarmed");
                        prop_assert_eq!(expected_version.get(), key.0);
                        prop_assert_eq!(discovery.hash, key.1);
                        prop_assert_eq!(discovery.availability.supplier_set_digest, key.2);
                        prop_assert_eq!(discovery.availability.suppliers, sources(bits).len() as u32);
                        prop_assert_eq!(discovery.availability.attempts, alarm.attempts);
                        prop_assert_eq!(discovery.availability.started_at, wall(0));
                        prop_assert_eq!(discovery.availability.next_probe_at, wall(9));
                        prop_assert!(discovery.availability.alarmed);
                        model.check_evidence(0, key, discovery.evidence)?;
                        if accept {
                            state.alarm_request_accepted(request);
                            model.accepted_restart = Some(key);
                        }
                    }
                }
                Input::Retry { matching, bits, accept } => {
                    let hash = match (matching, model.spec.as_ref()) {
                        (true, Some(spec)) => header_hash_of(spec),
                        _ => block::Hash([0xEE; 32]),
                    };
                    let actual = state.propose_operator_retry(hash, &sources(bits), wall(9));
                    let expected = model.retry(matching, bits);
                    prop_assert_eq!(actual.is_some(), expected.is_some(), "retry {:?}", actual);
                    if let (Some(request), Some(key)) = (actual, expected) {
                        let BlockSyncAction::RetryBodyAvailability { expected_version, retry } =
                            request.action()
                        else {
                            panic!("an operator retry proposes RetryBodyAvailability");
                        };
                        prop_assert_eq!(expected_version.get(), key.0);
                        prop_assert_eq!(retry.hash, key.1);
                        prop_assert_eq!(retry.availability.supplier_set_digest, key.2);
                        prop_assert_eq!(retry.availability.suppliers, sources(bits).len() as u32);
                        prop_assert_eq!(retry.availability.attempts, 0);
                        prop_assert_eq!(retry.availability.started_at, wall(9));
                        prop_assert!(!retry.availability.alarmed);
                        model.check_evidence(1, key, retry.evidence)?;
                        if accept {
                            state.alarm_request_accepted(request);
                            model.accepted_retry = Some(key);
                        }
                    }
                }
            }
            prop_assert_eq!(state.queries.pending().is_some(), model.pending);
            prop_assert_eq!(state.queries.hint_refresh().is_some(), model.hint_refresh);
        }
    }
}
