//! [`REACTOR_QUERY_LIFECYCLE`](super::REACTOR_QUERY_LIFECYCLE)'s model: the
//! effects of the needed-block query lifecycle at the runtime boundary.
//!
//! `Model` and the `block_query` ledger in `zakura-network` check the pure query
//! rules after every generated input. This model checks what they can't see:
//! which queries the real storage took, and the state the real reactor reported.
//!
//! The property's binding (properties.md, Observation bindings):
//!
//! - **Contract.** `QueryState` in `block_sync/reactor/state.rs`. Storage takes
//!   only an accepted query, once, under its exact identity. An accepted query
//!   becomes the pending query and advances the next ID. A refusal keeps both. A
//!   full channel arms a retry. A closed channel ends dispatch and arms nothing.
//!   A reply or failure that names the pending query's ID and authority
//!   retires it, and any other keeps it, whatever its label. A reset, an
//!   authority change and a tip reset retire it.
//! - **Facts.** Each `BlockSyncFact::Reactor` transition and its state, and each
//!   `QueryNeededBlocks` that block storage took. The check compares one
//!   transition with the state the previous transition reported.
//! - **Capture boundary.** `ReactorIO::report_transition`, right after each
//!   `QueryState` change, with no yield in between. Storage facts come from the
//!   simulated block storage, past the action channel.
//! - **Correlation.** The exact query ID, body-work authority, height range and
//!   header tip. A harness runs one tapped node with one block storage, so
//!   neither fact needs a node key.
//! - **Evaluation.** Every completed transition. Storage takes a query in its
//!   own step, so an unaccepted one is judged when no task can run.
//! - **Availability.** The `Sim` tier, through the tap. The check reads
//!   `Source::Storage` and `Source::View`, and a tier without them reports
//!   `NotApplicable`.
//! - **Checker and witness.** `check_reactor_query_lifecycle` and
//!   `query_reached_storage`.

use std::collections::{BTreeMap, BTreeSet};

use super::Finding;
use crate::{
    observe::{BlockSyncReactorState, BodyScope, StorageRequest},
    record::machine::{BodyAuthority, ReactorAction, ReactorQuery, ReactorRefusal},
};

/// A query as block storage took it, in the same terms as a [`ReactorQuery`].
#[derive(Clone, Debug, PartialEq, Eq)]
struct StorageQuery {
    scope: BodyAuthority,
    from: u32,
    limit: u32,
    header_tip: u32,
}

impl StorageQuery {
    /// Whether `query` names exactly this query.
    fn matches(&self, query: &ReactorQuery) -> bool {
        self.scope == query.scope
            && self.from == query.from
            && self.limit == query.limit
            && self.header_tip == query.header_tip
    }
}

/// The queries storage and the action channel have seen, and the query state
/// the reactor last reported.
#[derive(Debug)]
pub struct ReactorQueries {
    /// The pending query's ID and authority in the last reported state.
    reported_pending: Option<(u64, BodyAuthority)>,
    /// The next query's ID in the last reported state.
    reported_next: Option<u64>,
    /// Each accepted query, by ID.
    accepted: BTreeMap<u64, ReactorQuery>,
    /// Queries storage took and the channel hasn't accepted, by ID.
    unaccepted: BTreeMap<u64, StorageQuery>,
    /// Every query ID storage took.
    taken: BTreeSet<u64>,
    /// Findings not yet reported.
    pub(super) findings: Vec<Finding>,

    /// Whether the reactor offered the action channel a query.
    pub saw_proposal: bool,
    /// Whether storage took an accepted query under its exact identity.
    pub saw_query_at_storage: bool,
}

impl Default for ReactorQueries {
    /// A new reactor's state: its first query takes ID 1.
    fn default() -> Self {
        Self {
            reported_pending: None,
            reported_next: Some(1),
            accepted: BTreeMap::new(),
            unaccepted: BTreeMap::new(),
            taken: BTreeSet::new(),
            findings: Vec::new(),
            saw_proposal: false,
            saw_query_at_storage: false,
        }
    }
}

impl ReactorQueries {
    /// Checks one reactor transition against the state it reported, in `step`.
    pub(super) fn transition(
        &mut self,
        step: u64,
        transition: &ReactorAction,
        state: &BlockSyncReactorState,
    ) {
        use ReactorAction as Fact;
        match transition {
            Fact::QueryAccepted { query } => {
                self.saw_proposal = true;
                self.accepted.insert(query.query_id, query.clone());
                if let Some(taken) = self.unaccepted.remove(&query.query_id) {
                    self.judge_storage_identity(step, query, &taken);
                }
                let required = (Some(query.query_id), query.query_id.checked_add(1));
                if (pending_id(state), state.next_query_id) != required {
                    self.finding(
                        step,
                        format!(
                            "after accepting query {}, (pending, next ID) is {required:?}",
                            query.query_id
                        ),
                        format!("the reactor reported {state:?}"),
                    );
                }
            }
            Fact::QueryRefused { query, refusal } => {
                self.saw_proposal = true;
                self.judge_query_state_kept(step, transition, state);
                let closed = *refusal == ReactorRefusal::Closed;
                let holds = if closed {
                    state.dispatch_closed && !state.retry_armed
                } else {
                    state.retry_armed
                };
                if !holds {
                    self.finding(
                        step,
                        format!(
                            "a {refusal:?} refusal of query {} {}",
                            query.query_id,
                            if closed {
                                "closes dispatch and arms no retry"
                            } else {
                                "arms a retry"
                            }
                        ),
                        format!("the reactor reported {state:?}"),
                    );
                }
            }
            Fact::QueryReply {
                query_id, scope, ..
            }
            | Fact::QueryFailed {
                query_id, scope, ..
            } => {
                // The label is the reactor's claim, so the check judges the
                // reply by the identity of the query it names.
                let names_pending =
                    self.reported_pending.as_ref() == Some(&(*query_id, scope.clone()));
                if names_pending {
                    self.judge_query_retired(step, transition, state);
                } else {
                    self.judge_query_state_kept(step, transition, state);
                }
            }
            Fact::AuthorityChanged { .. }
            | Fact::TipReset { .. }
            | Fact::View { reset: true, .. } => {
                if state.pending.is_some() {
                    self.finding(
                        step,
                        format!("{transition:?} retires the pending query"),
                        format!("the reactor reported {state:?}"),
                    );
                }
            }
            Fact::QueryGateChanged | Fact::RetryElapsed | Fact::View { .. } => {}
        }
        self.reported_pending = pending_key(state);
        self.reported_next = state.next_query_id;
    }

    /// Judges a transition that must keep the pending query and the next ID.
    fn judge_query_state_kept(
        &mut self,
        step: u64,
        transition: &ReactorAction,
        state: &BlockSyncReactorState,
    ) {
        let required = (
            self.reported_pending.as_ref().map(|(id, _)| *id),
            self.reported_next,
        );
        if (pending_id(state), state.next_query_id) != required {
            self.finding(
                step,
                format!("{transition:?} keeps (pending, next ID) at {required:?}"),
                format!("the reactor reported {state:?}"),
            );
        }
    }

    /// Judges a reply or failure that names the pending query: it must retire
    /// the query and keep the next ID.
    fn judge_query_retired(
        &mut self,
        step: u64,
        transition: &ReactorAction,
        state: &BlockSyncReactorState,
    ) {
        let required = (None, self.reported_next);
        if (pending_id(state), state.next_query_id) != required {
            self.finding(
                step,
                format!("{transition:?} names the pending query, so it retires it"),
                format!("the reactor reported {state:?}"),
            );
        }
    }

    /// Folds a `QueryNeededBlocks` that block storage took. An accepted
    /// query is judged now, and any other waits for its acceptance, since a
    /// scheduler could run storage before the reactor's report.
    pub(super) fn storage_query(&mut self, step: u64, request: &StorageRequest) {
        let StorageRequest::QueryNeededBlocks {
            query_id,
            from,
            limit,
            scope:
                BodyScope {
                    scope,
                    verified_generation,
                    body_work_epoch,
                },
            best_header_tip,
        } = request
        else {
            return;
        };
        let taken = StorageQuery {
            scope: BodyAuthority {
                header: scope.clone(),
                verified_generation: *verified_generation,
                body_work_epoch: *body_work_epoch,
            },
            from: *from,
            limit: *limit,
            header_tip: *best_header_tip,
        };
        if !self.taken.insert(*query_id) {
            self.finding(
                step,
                "storage takes each needed-block query once".to_owned(),
                format!("storage took query {query_id} again: {taken:?}"),
            );
        }
        match self.accepted.get(query_id).cloned() {
            Some(query) => self.judge_storage_identity(step, &query, &taken),
            None => {
                self.unaccepted.insert(*query_id, taken);
            }
        }
    }

    /// Queries that reached storage although the channel never accepted them,
    /// for a run where no task can run. Takes them, so each is reported once.
    pub(super) fn take_unaccepted(&mut self, step: u64) -> Vec<Finding> {
        std::mem::take(&mut self.unaccepted)
            .into_iter()
            .map(|(query_id, taken)| Finding {
                step,
                expected: "storage takes only a query the action channel accepted".to_owned(),
                actual: format!(
                    "no task can run, and storage took query {query_id}, which the channel never \
                     accepted: {taken:?}"
                ),
            })
            .collect()
    }

    /// Judges that storage took `query` under its exact identity.
    fn judge_storage_identity(&mut self, step: u64, query: &ReactorQuery, taken: &StorageQuery) {
        if taken.matches(query) {
            self.saw_query_at_storage = true;
        } else {
            self.finding(
                step,
                format!(
                    "storage takes query {} with its accepted identity {query:?}",
                    query.query_id
                ),
                format!("storage took {taken:?}"),
            );
        }
    }

    fn finding(&mut self, step: u64, expected: String, actual: String) {
        self.findings.push(Finding {
            step,
            expected,
            actual,
        });
    }
}

/// The pending query's ID in `state`.
fn pending_id(state: &BlockSyncReactorState) -> Option<u64> {
    state.pending.as_ref().map(|query| query.query_id)
}

/// The pending query's ID and authority in `state`.
fn pending_key(state: &BlockSyncReactorState) -> Option<(u64, BodyAuthority)> {
    state
        .pending
        .as_ref()
        .map(|query| (query.query_id, query.scope.clone()))
}

/// Checker tests: each crafts `BlockSyncFact::Reactor` and storage facts and
/// folds them through [`SemanticModel`](super::SemanticModel). They show what
/// the checker judges. They don't show that a simulated run produces these
/// facts.
#[cfg(test)]
mod checker_tests {
    use super::*;
    use crate::{
        domain::block_sync_semantic::SemanticModel,
        observe::{BlockSyncFact, StorageBranch, StorageFact, WorkScope},
        record::machine::ReactorReply,
        storage::BLOCK_SERVICE,
        Observation, ObservationLog,
    };

    use ReactorAction as Fact;

    /// A log built one observation per step.
    #[derive(Default)]
    struct History {
        log: ObservationLog,
        step: u64,
        seq: u64,
    }

    impl History {
        fn push(&mut self, obs: Observation) {
            self.step += 1;
            self.log.begin(self.step);
            self.log.push(obs);
        }

        fn reactor(&mut self, transition: Fact, state: BlockSyncReactorState) -> &mut Self {
            self.seq += 1;
            self.push(Observation::BlockSync {
                instance: 0,
                seq: self.seq,
                storage_seq: 0,
                node: "node".to_owned(),
                event: BlockSyncFact::Reactor { transition, state },
            });
            self
        }

        /// Block storage takes a query with `query`'s identity.
        fn storage(&mut self, query: &ReactorQuery) -> &mut Self {
            self.seq += 1;
            self.push(Observation::Storage {
                service: BLOCK_SERVICE.to_owned(),
                instance: 0,
                seq: self.seq,
                txn: self.seq,
                fact: StorageFact::BlockSubmitted {
                    id: self.seq,
                    request: StorageRequest::QueryNeededBlocks {
                        query_id: query.query_id,
                        from: query.from,
                        limit: query.limit,
                        scope: BodyScope {
                            scope: query.scope.header.clone(),
                            verified_generation: query.scope.verified_generation,
                            body_work_epoch: query.scope.body_work_epoch,
                        },
                        best_header_tip: query.header_tip,
                    },
                    stage: None,
                },
            });
            self
        }

        fn model(&self) -> SemanticModel {
            SemanticModel::of(&self.log)
        }

        fn findings(&self) -> Vec<Finding> {
            self.model().reactor_queries.findings
        }
    }

    fn authority(epoch: u64) -> BodyAuthority {
        BodyAuthority {
            header: WorkScope {
                header_generation: 1,
                branch: StorageBranch {
                    anchor: "genesis".to_owned(),
                    target: "header-tip".to_owned(),
                },
            },
            verified_generation: 1,
            body_work_epoch: epoch,
        }
    }

    fn query(query_id: u64) -> ReactorQuery {
        ReactorQuery {
            query_id,
            scope: authority(1),
            from: 1,
            limit: 8,
            header_tip: 100,
            forced: false,
        }
    }

    /// The state with `pending` and `next`, nothing armed, owed, or closed.
    fn state(pending: Option<u64>, next: u64) -> BlockSyncReactorState {
        BlockSyncReactorState {
            pending: pending.map(query),
            next_query_id: Some(next),
            retry_armed: false,
            retry_forced: false,
            dispatch_closed: false,
            scope: None,
            verified_tip: 0,
            request_floor: 0,
            header_tip: 100,
            reset_epoch: 0,
            reaction_epoch: 0,
        }
    }

    fn armed(pending: Option<u64>, next: u64) -> BlockSyncReactorState {
        BlockSyncReactorState {
            retry_armed: true,
            ..state(pending, next)
        }
    }

    fn closed(pending: Option<u64>, next: u64) -> BlockSyncReactorState {
        BlockSyncReactorState {
            dispatch_closed: true,
            ..state(pending, next)
        }
    }

    fn refused(query: ReactorQuery, refusal: ReactorRefusal) -> Fact {
        Fact::QueryRefused { query, refusal }
    }

    /// Query 1 is accepted and pending.
    fn accepted_first() -> History {
        let mut history = History::default();
        history.reactor(Fact::QueryAccepted { query: query(1) }, state(Some(1), 2));
        history
    }

    #[test]
    fn checker_accepts_an_accepted_query_at_storage_under_its_identity() {
        let mut history = accepted_first();
        history.storage(&query(1));
        let model = history.model();
        assert_eq!(model.reactor_queries.findings, []);
        assert!(model.reactor_queries.saw_query_at_storage);
    }

    #[test]
    fn checker_finds_a_storage_query_with_another_identity() {
        let mut history = accepted_first();
        history.storage(&ReactorQuery {
            limit: 9,
            ..query(1)
        });
        let findings = history.findings();
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert!(findings[0].actual.contains("limit: 9"), "{findings:?}");
        assert!(!history.model().reactor_queries.saw_query_at_storage);
    }

    #[test]
    fn checker_finds_a_storage_query_taken_twice() {
        let mut history = accepted_first();
        history.storage(&query(1)).storage(&query(1));
        assert_eq!(history.findings().len(), 1);
    }

    #[test]
    fn checker_judges_a_storage_query_that_precedes_its_acceptance() {
        let mut history = History::default();
        history
            .storage(&query(1))
            .reactor(Fact::QueryAccepted { query: query(1) }, state(Some(1), 2));
        let mut model = history.model();
        assert_eq!(model.reactor_queries.findings, []);
        assert_eq!(model.reactor_queries.take_unaccepted(9), []);
    }

    /// A storage query the channel never accepted is a fault when no task can
    /// run, whether the reactor reported a refusal or nothing at all.
    #[test]
    fn checker_finds_a_storage_query_without_acceptance() {
        let mut bypass = History::default();
        bypass.storage(&query(1));
        let mut refusal = accepted_first();
        refusal
            .reactor(refused(query(2), ReactorRefusal::Full), armed(Some(1), 2))
            .storage(&query(2));
        for history in [bypass, refusal] {
            let mut model = history.model();
            assert_eq!(model.reactor_queries.findings, []);
            let findings = model.reactor_queries.take_unaccepted(9);
            assert_eq!(findings.len(), 1, "{findings:?}");
        }
    }

    /// An accepted query's reported state names that query and the next ID.
    #[test]
    fn checker_finds_an_accepted_query_whose_pending_state_disagrees() {
        for reported in [state(None, 2), state(Some(1), 1), state(Some(7), 2)] {
            let mut history = History::default();
            history.reactor(Fact::QueryAccepted { query: query(1) }, reported.clone());
            let findings = history.findings();
            assert_eq!(findings.len(), 1, "{reported:?}: {findings:?}");
        }
    }

    #[test]
    fn checker_holds_a_refusal_that_keeps_pending_and_next_id() {
        let mut history = accepted_first();
        history
            .reactor(refused(query(2), ReactorRefusal::Full), armed(Some(1), 2))
            .reactor(Fact::RetryElapsed, state(Some(1), 2))
            .reactor(Fact::QueryAccepted { query: query(2) }, state(Some(2), 3));
        assert_eq!(history.findings(), []);
    }

    /// `refusal-consumes-query-id`'s fact, and a refusal that drops the query.
    #[test]
    fn checker_finds_a_refusal_that_changes_pending_or_next_id() {
        for reported in [armed(Some(1), 3), armed(None, 2)] {
            let mut history = accepted_first();
            history.reactor(refused(query(2), ReactorRefusal::Full), reported.clone());
            let findings = history.findings();
            assert_eq!(findings.len(), 1, "{reported:?}: {findings:?}");
        }
    }

    #[test]
    fn checker_finds_a_full_refusal_that_arms_no_retry() {
        let mut history = accepted_first();
        history.reactor(refused(query(2), ReactorRefusal::Full), state(Some(1), 2));
        assert_eq!(history.findings().len(), 1);
    }

    #[test]
    fn checker_judges_a_closed_refusal() {
        let mut history = accepted_first();
        history.reactor(
            refused(query(2), ReactorRefusal::Closed),
            closed(Some(1), 2),
        );
        assert_eq!(history.findings(), []);

        for reported in [
            state(Some(1), 2),
            BlockSyncReactorState {
                retry_armed: true,
                ..closed(Some(1), 2)
            },
        ] {
            let mut history = accepted_first();
            history.reactor(refused(query(2), ReactorRefusal::Closed), reported.clone());
            assert_eq!(history.findings().len(), 1, "{reported:?}");
        }
    }

    #[test]
    fn checker_finds_a_stale_completion_that_retires_the_pending_query() {
        for stale in [
            Fact::QueryReply {
                query_id: 7,
                scope: authority(1),
                outcome: ReactorReply::Stale,
            },
            Fact::QueryFailed {
                query_id: 7,
                scope: authority(1),
                matched: false,
            },
        ] {
            let mut history = accepted_first();
            history.reactor(stale.clone(), state(Some(1), 2));
            assert_eq!(history.findings(), [], "{stale:?}");

            let mut history = accepted_first();
            history.reactor(stale.clone(), state(None, 2));
            assert_eq!(history.findings().len(), 1, "{stale:?}");
        }
    }

    /// A late reply for a replaced query keeps the replacement, whatever
    /// its label says.
    #[test]
    fn checker_finds_a_mislabeled_late_reply_that_clears_the_replacement() {
        let mut history = accepted_first();
        history.reactor(Fact::QueryAccepted { query: query(2) }, state(Some(2), 3));
        history.reactor(
            Fact::QueryReply {
                query_id: 1,
                scope: authority(1),
                outcome: ReactorReply::Accepted,
            },
            state(None, 3),
        );
        assert_eq!(history.findings().len(), 1);
    }

    /// A reply or failure that names the pending query retires it, whatever
    /// its label says.
    #[test]
    fn checker_finds_a_matching_reply_that_keeps_the_pending_query() {
        for matching in [
            Fact::QueryReply {
                query_id: 1,
                scope: authority(1),
                outcome: ReactorReply::Stale,
            },
            Fact::QueryFailed {
                query_id: 1,
                scope: authority(1),
                matched: false,
            },
        ] {
            let mut history = accepted_first();
            history.reactor(matching.clone(), state(None, 2));
            assert_eq!(history.findings(), [], "{matching:?}");

            let mut history = accepted_first();
            history.reactor(matching.clone(), state(Some(1), 2));
            assert_eq!(history.findings().len(), 1, "{matching:?}");
        }
    }

    /// A reset, an authority change, and a tip reset each retire the query.
    #[test]
    fn checker_finds_a_superseding_transition_that_keeps_the_pending_query() {
        for superseding in [
            Fact::View {
                reset: true,
                tip_advanced: false,
            },
            Fact::AuthorityChanged {
                authority: authority(2),
                epoch_changed: true,
            },
            Fact::TipReset { tip: 0 },
        ] {
            let mut history = accepted_first();
            history.reactor(superseding.clone(), state(None, 2));
            assert_eq!(history.findings(), [], "{superseding:?}");

            let mut history = accepted_first();
            history.reactor(superseding.clone(), state(Some(1), 2));
            assert_eq!(history.findings().len(), 1, "{superseding:?}");
        }
    }
}
