//! Block sync's semantic properties (properties.md, Block sync): which
//! bodies the node may submit, what a stale completion may release, and
//! whether freed submission room wakes waiting work.
//!
//! Every check reads one [`SemanticModel`], which folds the run's log: the
//! node's block-sync events, its block storage facts, and the relays'
//! frames. The model lives in the harness's view, so it outlives each
//! check, and the first check after a step folds that step's observations
//! for all of them. A check never reads the node's own state, so a node
//! that keys its work wrongly can't make its own check agree with it.
//!
//! The model knows the node's submission limit, `submitted_apply_limit()`
//! of the default config, which every block-sync scenario runs.

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
};

use zakura_network::zakura::ZakuraBlockSyncConfig;

use crate::{
    model::Label,
    observe::{
        BlockSyncFact, BlockSyncHolder, StorageFact, StorageRequest, StorageResult, WorkOwner,
    },
    property::{
        CheckCtx, HasView, Kind, LivenessBound, ObligationKey, Property, PropertyId, Source,
        Status, Subject, Unit, Verdict,
    },
    storage::BLOCK_SERVICE,
    Observation, ObservationLog,
};

/// The node submits only bodies it requested.
pub const SUBMIT_ONLY_REQUESTED: &str = "block-sync/submit-only-requested";

/// A stale apply completion never charges or frees a current owner's
/// resources.
pub const STALE_COMPLETION_INERT: &str = "block-sync/stale-completion-inert";

/// Freed submission room wakes an eligible waiting body.
pub const CAPACITY_RELEASE_WAKES: &str = "block-sync/capacity-release-wakes";

/// Reset or epoch change can't hide or multiply retained submissions.
pub const DETACHED_SUBMISSION_BOUNDED: &str = "block-sync/detached-submission-bounded";

/// The sequencer's input queue retains raw envelopes, not decoded bodies.
pub const INPUT_DECODED_BOUND: &str = "block-sync/input-decoded-bound";

/// The issue whose bug [`CAPACITY_RELEASE_WAKES`] witnesses.
pub const CAPACITY_RELEASE_WAKES_ISSUE: u32 = 1523;

/// What every property here reads.
const NEEDS: &[Source] = &[Source::Storage, Source::Frames, Source::View];

/// A view that carries a [`SemanticModel`].
pub trait SemanticView {
    /// The model, shared with every later view.
    fn semantic(&self) -> Rc<RefCell<SemanticModel>>;
}

/// The block-sync semantic properties, for a harness whose view carries the
/// model: [`SUBMIT_ONLY_REQUESTED`] and [`STALE_COMPLETION_INERT`], active;
/// [`CAPACITY_RELEASE_WAKES`], known for #1523; and
/// [`DETACHED_SUBMISSION_BOUNDED`] and [`INPUT_DECODED_BOUND`], drafts.
pub fn semantic_properties<S>() -> Vec<Property<S>>
where
    S: HasView,
    S::View: SemanticView,
{
    vec![
        Property {
            id: PropertyId::new(SUBMIT_ONLY_REQUESTED),
            claim: "the node submits only bodies it requested: each SubmitBlock owner names a \
                    request published for that height, and the body crossed into the node in \
                    that request's open GET_BLOCKS exchange, or in another open exchange with \
                    the hash the owner's work asked for",
            kind: Kind::Safety,
            assumes: &[],
            needs: NEEDS,
            status: Status::Active,
            check: check_submit_only_requested::<S>,
            witness: Some(any_submission),
        },
        Property {
            id: PropertyId::new(STALE_COMPLETION_INERT),
            claim: "while a stale apply completion is pending, the sequencer releases no other \
                    submission that storage still holds",
            kind: Kind::Safety,
            assumes: &[],
            needs: NEEDS,
            status: Status::Active,
            check: check_stale_completion_inert::<S>,
            witness: Some(stale_completion_beside_a_current_submission),
        },
        Property {
            id: PropertyId::new(CAPACITY_RELEASE_WAKES),
            claim: "after a completion frees room under submitted_apply_limit, the sequencer \
                    submits the eligible waiting body before quiescence",
            kind: Kind::Liveness {
                bound: LivenessBound::BeforeQuiescence,
            },
            assumes: &[],
            needs: NEEDS,
            status: Status::Known {
                issue: CAPACITY_RELEASE_WAKES_ISSUE,
            },
            check: check_capacity_release_wakes::<S>,
            witness: Some(room_freed_beside_a_waiting_body),
        },
        Property {
            id: PropertyId::new(DETACHED_SUBMISSION_BOUNDED),
            claim: "attached and detached submissions share the submission limit, and every \
                    submission storage held across a reset keeps an attached or detached holder",
            kind: Kind::Safety,
            assumes: &[],
            needs: NEEDS,
            status: Status::Draft,
            check: check_detached_submission_bounded::<S>,
            witness: Some(reset_while_storage_held),
        },
        Property {
            id: PropertyId::new(INPUT_DECODED_BOUND),
            claim: "the sequencer input queue retains raw envelopes, not decoded block graphs",
            kind: Kind::Bound {
                unit: Unit::AttributedHeap,
            },
            assumes: &[],
            needs: NEEDS,
            status: Status::Draft,
            check: check_input_decoded_bound::<S>,
            witness: Some(queued_at_idle),
        },
    ]
}

/// A violation the model found, for one property to report.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Finding {
    /// The step whose observations showed it.
    pub step: u64,
    /// What the property expected.
    pub expected: String,
    /// What the run did instead.
    pub actual: String,
}

/// Freed room beside an eligible waiting body: [`CAPACITY_RELEASE_WAKES`]'s
/// trigger.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoomFreed {
    /// The step that freed the room.
    pub step: u64,
    /// The release's position among that step's observations.
    pub at: usize,
    /// The waiting body's height.
    pub height: u32,
}

/// A request the node published.
#[derive(Clone, Debug)]
struct Requested {
    /// Its owner's body work epoch.
    epoch: Option<u64>,
    /// Its heights, in order.
    heights: Vec<u32>,
}

/// A body that crossed into the node in an open exchange.
#[derive(Clone, Debug)]
struct Crossed {
    connection: u64,
    /// The exchange's key: its request's first height.
    exchange: u64,
    hash: String,
}

/// A submission storage took.
#[derive(Clone, Debug)]
struct Taken {
    token: u64,
}

/// The run's evidence as the semantic checks read it. See the module doc.
#[derive(Debug, Default)]
pub struct SemanticModel {
    /// Log entries folded so far.
    folded: usize,
    /// The step the entries folded last belong to.
    step: Option<u64>,
    /// What the step being folded changed.
    pending: StepChanges,

    /// Each session's connection.
    sessions: BTreeMap<u64, u64>,
    /// Each published request, by session and request ID.
    requests: BTreeMap<(u64, u64), Requested>,
    /// Each connection's latest peer frame: its number, and its exchange
    /// if a conformant label put it in one.
    last_frame: BTreeMap<u64, (u64, Option<u64>)>,
    /// Each body that crossed in an open exchange, by height.
    crossed: BTreeMap<u32, Vec<Crossed>>,
    /// Each needed-blocks query's epoch, by operation.
    queries: BTreeMap<u64, u64>,
    /// Each `(epoch, height, hash)` a needed-blocks answer listed.
    authorized: BTreeSet<(u64, u32, String)>,

    /// Each submission storage took, by operation.
    taken: BTreeMap<u64, Taken>,
    /// Each token storage took.
    taken_tokens: BTreeSet<u64>,
    /// Tokens storage still holds: taken, neither completed nor dropped.
    storage_held: BTreeSet<u64>,
    /// Tokens whose verdict reached block sync, or that storage dropped.
    finished: BTreeSet<u64>,
    /// Tokens a submission holder ever held.
    ever_held: BTreeSet<u64>,
    /// Tokens storage held once the run reached a newer body-work epoch
    /// than theirs: the submissions a reset must detach, not drop.
    held_at_reset: BTreeSet<u64>,
    /// Each token's held submission holders: attached, then detached.
    submissions: BTreeMap<u64, (u32, u32)>,
    /// Attached submission holders, summed over `submissions`.
    attached_total: u64,
    /// Detached submission holders, summed over `submissions`.
    detached_total: u64,
    /// Each taken token's owner epoch.
    token_epochs: BTreeMap<u64, Option<u64>>,
    /// Each submission holder's body height, by token.
    submission_heights: BTreeMap<u64, u32>,
    /// Each `applying` body's height and owner epoch, by allocation.
    applying: BTreeMap<u64, (u32, Option<u64>)>,
    /// Bodies in the sequencer's input queue, by allocation.
    queued: BTreeSet<u64>,
    /// The latest committed view's body work epoch.
    epoch: Option<u64>,
    /// The latest committed view's verified tip.
    verified_tip: u32,
    /// Whether the sequencer closed.
    closed: bool,
    /// Stale completions block sync received whose holders it still holds.
    stale: BTreeSet<u64>,
    /// Submissions the sequencer held after the last step judged.
    in_flight_at_last_step: u64,

    /// Submissions judged and not yet reported, for
    /// [`SUBMIT_ONLY_REQUESTED`].
    submit_judged: u64,
    /// Findings for [`SUBMIT_ONLY_REQUESTED`], not yet reported.
    submit_findings: Vec<Finding>,
    /// Findings for [`STALE_COMPLETION_INERT`], not yet reported.
    stale_findings: Vec<Finding>,
    /// Findings for [`DETACHED_SUBMISSION_BOUNDED`], not yet reported.
    detached_findings: Vec<Finding>,
    /// Triggers of [`CAPACITY_RELEASE_WAKES`], not yet obliged.
    rooms: Vec<RoomFreed>,

    /// Whether storage took a submission.
    pub saw_submission: bool,
    /// Whether a stale completion reached block sync while a current
    /// submission was attached.
    pub saw_stale_beside_current: bool,
    /// Whether a completion freed room beside an eligible waiting body.
    pub saw_room_freed: bool,
    /// Whether the run reached a newer body-work epoch while storage held a
    /// submission from an older one.
    pub saw_reset_while_held: bool,
    /// Whether virtual time advanced while the input queue held a body.
    pub saw_queued_at_idle: bool,
}

/// What one step changed, judged once the step is folded.
#[derive(Debug, Default)]
struct StepChanges {
    /// Submissions storage took, with their operation, owner, height and
    /// hash.
    submitted: Vec<(u64, WorkOwner, Option<u32>, String)>,
    /// Tokens whose verdict reached block sync.
    delivered: Vec<u64>,
    /// Submission holders released, with the position of the release.
    released: Vec<(BlockSyncHolder, usize)>,
    /// Tokens the sequencer detached.
    detached: BTreeSet<u64>,
    /// Whether storage's held tokens or the sequencer's submission holders
    /// changed.
    storage_changed: bool,
    /// Observations this step, so far.
    count: usize,
}

impl SemanticModel {
    /// Folds every entry of `log` not yet folded, judging each step once
    /// its entries are all folded. The log's last step is complete, since a
    /// check runs only after its step.
    pub fn catch_up(&mut self, log: &ObservationLog) {
        let entries = log.entries();
        while self.folded < entries.len() {
            let entry = &entries[self.folded];
            if self.step != Some(entry.step) {
                if let Some(step) = self.step {
                    self.end_step(step);
                }
                self.step = Some(entry.step);
            }
            self.fold(&entry.obs);
            self.folded += 1;
        }
        if let Some(step) = self.step {
            if self.pending.count > 0 {
                self.end_step(step);
            }
        }
    }

    /// The model of `log` as a whole, for a witness.
    pub fn of(log: &ObservationLog) -> Self {
        let mut model = SemanticModel::default();
        model.catch_up(log);
        model
    }

    /// The node's submission limit.
    fn limit() -> u64 {
        static LIMIT: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
        *LIMIT.get_or_init(|| {
            u64::try_from(ZakuraBlockSyncConfig::default().submitted_apply_limit())
                .expect("the submission limit fits in 64 bits")
        })
    }

    /// Submissions the sequencer holds, attached or detached.
    fn in_flight(&self) -> u64 {
        self.attached_total + self.detached_total
    }

    /// Submissions the sequencer holds attached.
    fn attached(&self) -> u64 {
        self.attached_total
    }

    /// Folds one observation into the model and the step's changes.
    fn fold(&mut self, obs: &Observation) {
        let at = self.pending.count;
        self.pending.count += 1;
        match obs {
            Observation::PeerFrame {
                connection,
                seq,
                label,
                exchange,
                ..
            } => {
                let open = exchange.filter(|_| *label == Label::Conformant);
                self.last_frame.insert(*connection, (*seq, open));
            }
            Observation::PeerBody {
                connection,
                seq,
                height,
                hash,
            } => {
                if let Some((_, Some(exchange))) = self
                    .last_frame
                    .get(connection)
                    .filter(|(frame, _)| frame == seq)
                {
                    self.crossed.entry(*height).or_default().push(Crossed {
                        connection: *connection,
                        exchange: *exchange,
                        hash: hash.clone(),
                    });
                }
            }
            Observation::Storage { service, fact, .. } if service == BLOCK_SERVICE => {
                self.storage(fact);
            }
            Observation::BlockSync { event, .. } => self.block_sync(event, at),
            Observation::TimeAdvanced { .. } if !self.queued.is_empty() => {
                self.saw_queued_at_idle = true;
            }
            _ => {}
        }
    }

    /// Notes that the run reached body-work epoch `epoch`: every
    /// submission storage holds from an older epoch is now one a reset must
    /// detach.
    fn note_epoch(&mut self, epoch: u64) {
        let older: Vec<u64> = self
            .storage_held
            .iter()
            .copied()
            .filter(|token| {
                self.token_epochs
                    .get(token)
                    .copied()
                    .flatten()
                    .is_some_and(|owned| owned < epoch)
            })
            .collect();
        self.saw_reset_while_held |= !older.is_empty();
        self.held_at_reset.extend(older);
    }

    /// Folds a block storage fact.
    fn storage(&mut self, fact: &StorageFact) {
        match fact {
            StorageFact::BlockSubmitted { id, request, .. } => match request {
                StorageRequest::QueryNeededBlocks { scope, .. } => {
                    self.queries.insert(*id, scope.body_work_epoch);
                    self.note_epoch(scope.body_work_epoch);
                }
                StorageRequest::SubmitBlock {
                    owner,
                    token,
                    height,
                    hash,
                    ..
                } => {
                    self.saw_submission = true;
                    self.taken.insert(*id, Taken { token: *token });
                    self.taken_tokens.insert(*token);
                    self.token_epochs.insert(*token, owner.body_work_epoch);
                    self.storage_held.insert(*token);
                    self.pending.storage_changed = true;
                    self.pending
                        .submitted
                        .push((*id, owner.clone(), *height, hash.clone()));
                }
                _ => {}
            },
            StorageFact::BlockCompleted {
                id,
                result,
                delivered,
                ..
            } => {
                if let (Some(epoch), StorageResult::NeededBlocks { blocks, .. }) =
                    (self.queries.get(id), result)
                {
                    for block in blocks {
                        self.authorized
                            .insert((*epoch, block.height, block.hash.clone()));
                    }
                }
                if let Some(taken) = self.taken.get(id) {
                    let token = taken.token;
                    self.storage_held.remove(&token);
                    self.pending.storage_changed = true;
                    if *delivered {
                        self.pending.delivered.push(token);
                    }
                }
            }
            StorageFact::ReplyDelivered { id } => {
                if let Some(taken) = self.taken.get(id) {
                    self.pending.delivered.push(taken.token);
                }
            }
            StorageFact::DriverExited { dropped, .. } | StorageFact::ShutDown { dropped, .. } => {
                for id in dropped {
                    if let Some(taken) = self.taken.get(id) {
                        self.storage_held.remove(&taken.token);
                        self.finished.insert(taken.token);
                        self.pending.storage_changed = true;
                    }
                }
            }
            StorageFact::ViewPublished {
                epoch,
                verified_best,
                ..
            } => {
                self.note_epoch(*epoch);
                self.epoch = Some(*epoch);
                self.verified_tip = verified_best.height;
            }
            _ => {}
        }
    }

    /// Folds a block-sync event, the step's observation `at`.
    fn block_sync(&mut self, event: &BlockSyncFact, at: usize) {
        match event {
            BlockSyncFact::SessionAdmitted {
                connection,
                session_id,
                ..
            } => {
                self.sessions.insert(*session_id, *connection);
            }
            BlockSyncFact::Request {
                owner,
                heights,
                step,
                ..
            } if step == "published" => {
                if let Some(epoch) = owner.body_work_epoch {
                    self.note_epoch(epoch);
                }
                self.requests.insert(
                    (owner.session_id, owner.request_id),
                    Requested {
                        epoch: owner.body_work_epoch,
                        heights: heights.clone(),
                    },
                );
            }
            BlockSyncFact::Holder {
                body,
                holder,
                change,
            } => {
                let retained = change == "retained";
                match holder {
                    BlockSyncHolder::Queued { allocation } => {
                        if retained {
                            self.queued.insert(*allocation);
                        } else {
                            self.queued.remove(allocation);
                        }
                    }
                    BlockSyncHolder::Applying { allocation } => {
                        if retained {
                            self.applying
                                .insert(*allocation, (body.height, body.owner.body_work_epoch));
                        } else {
                            self.applying.remove(allocation);
                        }
                    }
                    BlockSyncHolder::Submitted { token } | BlockSyncHolder::Detached { token } => {
                        let detached = matches!(holder, BlockSyncHolder::Detached { .. });
                        let counts = self.submissions.entry(*token).or_default();
                        let (count, total) = if detached {
                            (&mut counts.1, &mut self.detached_total)
                        } else {
                            (&mut counts.0, &mut self.attached_total)
                        };
                        self.pending.storage_changed = true;
                        if retained {
                            *count += 1;
                            *total += 1;
                            self.submission_heights.insert(*token, body.height);
                            self.ever_held.insert(*token);
                            if detached {
                                self.pending.detached.insert(*token);
                            }
                        } else if *count > 0 {
                            *count -= 1;
                            *total -= 1;
                            self.pending.released.push((holder.clone(), at));
                        }
                        if *counts == (0, 0) {
                            self.submissions.remove(token);
                            self.stale.remove(token);
                        }
                    }
                    BlockSyncHolder::Reorder { .. } => {}
                }
            }
            BlockSyncFact::SequencerClosed => self.closed = true,
            _ => {}
        }
    }

    /// Judges `step`, whose observations are all folded.
    fn end_step(&mut self, step: u64) {
        let changes = std::mem::take(&mut self.pending);
        for (op, owner, height, hash) in &changes.submitted {
            self.submit_judged += 1;
            if let Some(finding) = self.judge_submission(step, *op, owner, *height, hash) {
                self.submit_findings.push(finding);
            }
        }
        self.judge_stale(step, &changes);
        self.judge_room(step, &changes);
        if changes.storage_changed {
            self.judge_detached(step);
        }
        self.in_flight_at_last_step = self.in_flight();
    }

    /// [`SUBMIT_ONLY_REQUESTED`] on submission `op`.
    fn judge_submission(
        &self,
        step: u64,
        op: u64,
        owner: &WorkOwner,
        height: Option<u32>,
        hash: &str,
    ) -> Option<Finding> {
        let finding = |actual: String| Finding {
            step,
            expected: "a SubmitBlock for a body the node requested from that peer, in that \
                       request's open GET_BLOCKS exchange"
                .to_owned(),
            actual,
        };
        let request = (owner.session_id, owner.request_id);
        let Some(height) = height else {
            return Some(finding(format!(
                "submission {op} carries no coinbase height"
            )));
        };
        let Some(requested) = self.requests.get(&request) else {
            return Some(finding(format!(
                "submission {op} at height {height} names session {} request {}, which the node \
                 never published",
                request.0, request.1
            )));
        };
        if requested.epoch != owner.body_work_epoch || !requested.heights.contains(&height) {
            return Some(finding(format!(
                "submission {op} at height {height} names session {} request {} with epoch \
                 {:?}, but that request asked for heights {:?} in epoch {:?}",
                request.0, request.1, owner.body_work_epoch, requested.heights, requested.epoch
            )));
        }
        let crossed = self.crossed.get(&height).map_or(&[][..], Vec::as_slice);
        let first = requested.heights.first().copied().map(u64::from);
        let connection = self.sessions.get(&owner.session_id).copied();
        let own = crossed.iter().any(|body| {
            body.hash == hash && Some(body.connection) == connection && Some(body.exchange) == first
        });
        // First-completion-wins: a body another exchange delivered may end
        // this request's reservation if it's the body this request's work
        // asked for.
        let elsewhere = crossed.iter().any(|body| body.hash == hash)
            && owner
                .body_work_epoch
                .is_some_and(|epoch| self.authorized.contains(&(epoch, height, hash.to_owned())));
        if own || elsewhere {
            return None;
        }
        Some(finding(if crossed.iter().any(|body| body.hash == hash) {
            format!(
                "submission {op} credits body {hash} at height {height} to session {} request {}, \
                 but the body crossed in another exchange, and no needed-blocks answer of epoch \
                 {:?} asked for it",
                request.0, request.1, owner.body_work_epoch
            )
        } else {
            format!(
                "submission {op} carries body {hash} at height {height}, which crossed into the \
                 node in no open GET_BLOCKS exchange"
            )
        }))
    }

    /// [`STALE_COMPLETION_INERT`] on `step`'s changes. A stale completion
    /// is a verdict that reached block sync for a submission the sequencer
    /// had detached, or whose owner's epoch the committed view had left.
    fn judge_stale(&mut self, step: u64, changes: &StepChanges) {
        for token in &changes.delivered {
            self.finished.insert(*token);
            let detached = self
                .submissions
                .get(token)
                .is_some_and(|(_, detached)| *detached > 0);
            let old_epoch = self
                .token_epochs
                .get(token)
                .copied()
                .flatten()
                .zip(self.epoch)
                .is_some_and(|(owned, current)| owned < current);
            if (detached || old_epoch) && self.submissions.contains_key(token) {
                self.stale.insert(*token);
                if self.attached() > 0 {
                    self.saw_stale_beside_current = true;
                }
            }
        }
        if self.closed {
            return;
        }
        for (holder, _) in &changes.released {
            let token = match holder {
                BlockSyncHolder::Submitted { token } | BlockSyncHolder::Detached { token } => {
                    *token
                }
                _ => continue,
            };
            let transferred = changes.detached.contains(&token);
            let own_verdict = self.finished.contains(&token);
            let others: Vec<_> = self.stale.iter().filter(|stale| **stale != token).collect();
            if transferred
                || own_verdict
                || !self.taken_tokens.contains(&token)
                || others.is_empty()
            {
                continue;
            }
            self.stale_findings.push(Finding {
                step,
                expected: "a stale completion releases only its own submission".to_owned(),
                actual: format!(
                    "the sequencer released {holder:?} at height {:?}, whose verdict storage \
                     still holds, while stale completions {others:?} were pending",
                    self.submission_heights.get(&token)
                ),
            });
        }
    }

    /// [`CAPACITY_RELEASE_WAKES`]'s trigger on `step`'s changes: the step
    /// released a submission while the sequencer held the limit's worth,
    /// and an eligible body waits. With no submission attached, the
    /// sequencer's decoded window always admits the next body, so only the
    /// count limit held it back.
    fn judge_room(&mut self, step: u64, changes: &StepChanges) {
        let Some((_, at)) = changes.released.first() else {
            return;
        };
        let limit = Self::limit();
        if self.closed
            || self.in_flight_at_last_step < limit
            || self.in_flight() >= limit
            || self.attached() > 0
        {
            return;
        }
        if let Some(height) = self.eligible() {
            self.saw_room_freed = true;
            self.rooms.push(RoomFreed {
                step,
                at: *at,
                height,
            });
        }
    }

    /// The lowest unsubmitted `applying` body of the current epoch, if every
    /// height between the verified tip and it is `applying` too, so nothing
    /// but room holds it back.
    fn eligible(&self) -> Option<u32> {
        let submitted: BTreeSet<u32> = self
            .submissions
            .iter()
            .filter(|(_, (attached, _))| *attached > 0)
            .filter_map(|(token, _)| self.submission_heights.get(token).copied())
            .collect();
        let applying: BTreeMap<u32, Option<u64>> = self.applying.values().copied().collect();
        let (height, epoch) = applying
            .iter()
            .find(|(height, _)| **height > self.verified_tip && !submitted.contains(height))?;
        let contiguous =
            (self.verified_tip + 1..*height).all(|below| applying.contains_key(&below));
        (contiguous && self.epoch.is_some() && *epoch == self.epoch).then_some(*height)
    }

    /// [`DETACHED_SUBMISSION_BOUNDED`]'s observable halves after `step`.
    fn judge_detached(&mut self, step: u64) {
        let in_flight = self.in_flight();
        let limit = Self::limit();
        if in_flight > limit {
            self.detached_findings.push(Finding {
                step,
                expected: format!("at most {limit} attached and detached submissions"),
                actual: format!("the sequencer holds {in_flight}"),
            });
        }
        if self.closed {
            return;
        }
        let unheld: Vec<_> = self
            .storage_held
            .intersection(&self.held_at_reset)
            // A token whose holder block sync hasn't reported yet lacks none.
            .filter(|token| self.ever_held.contains(token))
            .filter(|token| !self.submissions.contains_key(token))
            .collect();
        if !unheld.is_empty() {
            self.detached_findings.push(Finding {
                step,
                expected: "every submission storage held across a reset keeps its holder"
                    .to_owned(),
                actual: format!("storage holds tokens {unheld:?}, which no holder charges"),
            });
        }
    }
}

/// The model `cx`'s view carries, caught up with the log.
fn caught_up<S>(cx: &CheckCtx<'_, S>) -> Rc<RefCell<SemanticModel>>
where
    S: HasView,
    S::View: SemanticView,
{
    let model = cx.view().semantic();
    model.borrow_mut().catch_up(cx.log);
    model
}

/// Reports `findings`: the first as the verdict, the rest recorded.
fn report<S: HasView>(cx: &mut CheckCtx<'_, S>, findings: Vec<Finding>) -> Option<Verdict> {
    let mut findings = findings.into_iter();
    let first = findings.next()?;
    for finding in findings {
        cx.record_violation(finding.expected, finding.actual);
    }
    Some(cx.violated(first.expected, first.actual))
}

/// Checks [`SUBMIT_ONLY_REQUESTED`] on each submission storage took.
fn check_submit_only_requested<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView,
    S::View: SemanticView,
{
    let model = caught_up(cx);
    let (findings, applicable) = {
        let mut model = model.borrow_mut();
        (
            std::mem::take(&mut model.submit_findings),
            std::mem::take(&mut model.submit_judged) > 0,
        )
    };
    report(cx, findings).unwrap_or(if applicable {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    })
}

/// Checks [`STALE_COMPLETION_INERT`] after each stale completion.
fn check_stale_completion_inert<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView,
    S::View: SemanticView,
{
    let model = caught_up(cx);
    let (findings, applicable) = {
        let mut model = model.borrow_mut();
        (
            std::mem::take(&mut model.stale_findings),
            !model.stale.is_empty(),
        )
    };
    report(cx, findings).unwrap_or(if applicable {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    })
}

/// Opens a [`CAPACITY_RELEASE_WAKES`] obligation for each body that freed
/// room left waiting.
fn check_capacity_release_wakes<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView,
    S::View: SemanticView,
{
    let model = caught_up(cx);
    let rooms = std::mem::take(&mut model.borrow_mut().rooms);
    if rooms.is_empty() {
        return Verdict::NotApplicable;
    }
    for room in rooms {
        let trigger = (room.step == cx.step).then_some(room.at);
        let obligation = cx.obligation(
            trigger,
            Subject::Owner(format!("height {}", room.height)),
            waiting_body_moved,
        );
        cx.oblige(obligation);
    }
    Verdict::Holds
}

/// Whether `obs` discharges the obligation `key` waits on: the waiting body
/// at its height was submitted, or left `applying`, or the sequencer closed.
fn waiting_body_moved(key: &ObligationKey, obs: &Observation) -> bool {
    let Subject::Owner(subject) = &key.subject else {
        return false;
    };
    let Some(height) = subject
        .strip_prefix("height ")
        .and_then(|height| height.parse::<u32>().ok())
    else {
        return false;
    };
    match obs {
        Observation::Storage { service, fact, .. } if service == BLOCK_SERVICE => matches!(
            fact,
            StorageFact::BlockSubmitted {
                request: StorageRequest::SubmitBlock { height: Some(at), .. },
                ..
            } if *at == height
        ),
        Observation::BlockSync { event, .. } => match event {
            BlockSyncFact::Holder {
                body,
                holder,
                change,
            } => {
                body.height == height
                    && match holder {
                        BlockSyncHolder::Submitted { .. } => change == "retained",
                        BlockSyncHolder::Applying { .. } => change == "released",
                        _ => false,
                    }
            }
            BlockSyncFact::SequencerClosed => true,
            _ => false,
        },
        _ => false,
    }
}

/// Checks [`DETACHED_SUBMISSION_BOUNDED`]'s observable halves: the shared
/// count limit, and a holder for every submission storage held across a
/// reset. The byte half needs each submission's decoded bytes, which no
/// observation carries.
fn check_detached_submission_bounded<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView,
    S::View: SemanticView,
{
    let model = caught_up(cx);
    let (findings, applicable) = {
        let mut model = model.borrow_mut();
        (
            std::mem::take(&mut model.detached_findings),
            model.saw_submission,
        )
    };
    report(cx, findings).unwrap_or(if applicable {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    })
}

/// [`INPUT_DECODED_BOUND`] is never applicable: no observation says
/// whether a queued body is decoded, or how many bytes its graph holds.
fn check_input_decoded_bound<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView,
    S::View: SemanticView,
{
    caught_up(cx);
    Verdict::NotApplicable
}

/// [`SUBMIT_ONLY_REQUESTED`]'s trigger history: storage took a submission.
fn any_submission(log: &ObservationLog) -> bool {
    SemanticModel::of(log).saw_submission
}

/// [`STALE_COMPLETION_INERT`]'s trigger history: a stale completion
/// reached block sync while a current submission was attached, the state
/// in which releasing by height frees the wrong one.
fn stale_completion_beside_a_current_submission(log: &ObservationLog) -> bool {
    SemanticModel::of(log).saw_stale_beside_current
}

/// [`CAPACITY_RELEASE_WAKES`]'s trigger history: a completion freed room
/// under the limit beside an eligible waiting body.
fn room_freed_beside_a_waiting_body(log: &ObservationLog) -> bool {
    SemanticModel::of(log).saw_room_freed
}

/// [`DETACHED_SUBMISSION_BOUNDED`]'s trigger history: the run reached a
/// newer body-work epoch while storage held a submission from an older
/// one, which the sequencer had to detach.
fn reset_while_storage_held(log: &ObservationLog) -> bool {
    SemanticModel::of(log).saw_reset_while_held
}

/// [`INPUT_DECODED_BOUND`]'s trigger history: virtual time advanced, which
/// it does only once no task can run, while a body waited in the input
/// queue.
fn queued_at_idle(log: &ObservationLog) -> bool {
    SemanticModel::of(log).saw_queued_at_idle
}
