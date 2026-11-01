//! [`REACTOR_QUERY_LIFECYCLE`](super::REACTOR_QUERY_LIFECYCLE)'s model: an
//! independent fold of the needed-block query lifecycle.
//!
//! The property's binding (properties.md, Observation bindings):
//!
//! - **Contract.** `QueryState` in `block_sync/reactor/state.rs`. A refusal
//!   keeps the pending query and the next query ID. A full channel owes a
//!   retry with the refused query's force. A closed channel ends dispatch and
//!   owes nothing.
//! - **Facts.** Each `BlockSyncFact::Reactor` transition and its state, and
//!   each `QueryNeededBlocks` that block storage took. The model derives the
//!   expected state from the transitions alone, and never adopts a reported
//!   state except to resynchronize after a finding.
//! - **Capture boundary.** `ReactorIO::report_transition`, right after each
//!   `QueryState` change, with no yield in between. `QueryGateChanged` reports
//!   the changes that no input-specific transition names. Storage facts come
//!   from the simulated block storage, past the action channel.
//! - **Correlation.** The exact query ID, body-work authority, height range
//!   and header tip. A harness runs one tapped node with one block storage, so
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
    observe::{
        BlockSyncQuery, BlockSyncReactorState, BlockSyncReactorTransition, BodyScope,
        StorageRequest,
    },
    record::machine::{BodyAuthority, ReactorReply},
};

/// A query as block storage took it, in the same terms as a [`BlockSyncQuery`].
#[derive(Clone, Debug, PartialEq, Eq)]
struct StorageQuery {
    scope: BodyAuthority,
    from: u32,
    limit: u32,
    header_tip: u32,
}

impl StorageQuery {
    /// Whether `query` names exactly this query.
    fn matches(&self, query: &BlockSyncQuery) -> bool {
        self.scope == query.scope
            && self.from == query.from
            && self.limit == query.limit
            && self.header_tip == query.header_tip
    }
}

/// The query state the transitions so far require.
#[derive(Debug)]
pub struct ReactorQueries {
    /// The next query's ID, or `None` once the IDs ran out.
    next_id: Option<u64>,
    /// The accepted query awaiting a reply.
    pending: Option<BlockSyncQuery>,
    /// Whether a retry deadline is armed.
    retry_armed: bool,
    /// Whether the next proposal owes a refused or failed query's force.
    retry_forced: bool,
    /// Whether the action channel reported closed.
    dispatch_closed: bool,
    /// The current body-work authority: the latest change, or the first
    /// query's authority before any change.
    authority: Option<BodyAuthority>,
    /// Each accepted query, by ID.
    accepted: BTreeMap<u64, BlockSyncQuery>,
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
    /// Whether the action channel refused a query as full or reserved.
    pub saw_retry_refusal: bool,
    /// Whether the action channel refused a query as closed.
    pub saw_closed_refusal: bool,
}

impl Default for ReactorQueries {
    /// A new reactor's state: its first query takes ID 1.
    fn default() -> Self {
        Self {
            next_id: Some(1),
            pending: None,
            retry_armed: false,
            retry_forced: false,
            dispatch_closed: false,
            authority: None,
            accepted: BTreeMap::new(),
            unaccepted: BTreeMap::new(),
            taken: BTreeSet::new(),
            findings: Vec::new(),
            saw_proposal: false,
            saw_query_at_storage: false,
            saw_retry_refusal: false,
            saw_closed_refusal: false,
        }
    }
}

impl ReactorQueries {
    /// Folds one reactor transition and the state it reported, in `step`.
    pub(super) fn transition(
        &mut self,
        step: u64,
        transition: &BlockSyncReactorTransition,
        state: &BlockSyncReactorState,
    ) {
        use BlockSyncReactorTransition as Fact;
        match transition {
            Fact::QueryAccepted(query) => {
                self.judge_proposal(step, query);
                self.pending = Some(query.clone());
                self.retry_armed = false;
                self.retry_forced = false;
                self.next_id = query.query_id.checked_add(1);
                self.accepted.insert(query.query_id, query.clone());
                if let Some(taken) = self.unaccepted.remove(&query.query_id) {
                    self.judge_storage_identity(step, query, &taken);
                }
            }
            Fact::QueryRefused { query, refusal } => {
                self.judge_proposal(step, query);
                if refusal == "closed" {
                    self.saw_closed_refusal = true;
                    self.dispatch_closed = true;
                    self.retry_armed = false;
                    self.retry_forced = false;
                } else {
                    self.saw_retry_refusal = true;
                    self.retry_forced |= query.forced;
                    self.retry_armed = true;
                }
            }
            Fact::QueryReply {
                query_id,
                scope,
                outcome,
            } => self.reply(step, *query_id, scope, *outcome),
            Fact::QueryFailed {
                query_id,
                scope,
                matched,
            } => self.failure(step, *query_id, scope, *matched),
            Fact::QueryGateChanged => self.gate_changed(step, state),
            Fact::RetryElapsed => self.retry_armed = false,
            Fact::View { reset, .. } => {
                if *reset {
                    self.clear();
                }
            }
            Fact::AuthorityChanged { authority, .. } => {
                self.clear();
                self.authority = Some(authority.clone());
            }
            Fact::TipReset { .. } => self.clear(),
        }
        self.compare(step, transition, state);
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

    /// Judges a query the reactor offered the channel, whatever its answer:
    /// a query needs an open channel, an open retry gate, the next ID, the
    /// current authority, and any force a retry owes.
    fn judge_proposal(&mut self, step: u64, query: &BlockSyncQuery) {
        self.saw_proposal = true;
        let id = query.query_id;
        if self.dispatch_closed {
            self.finding(
                step,
                "a closed action channel ends query dispatch".to_owned(),
                format!("the reactor offered query {id} after the channel closed"),
            );
        }
        if self.retry_armed {
            self.finding(
                step,
                "no query while the retry gate is armed".to_owned(),
                format!("the reactor offered query {id} past the armed retry gate"),
            );
        }
        if Some(id) != self.next_id {
            self.finding(
                step,
                format!("the next query takes ID {:?}", self.next_id),
                format!("the reactor offered query {id}"),
            );
        }
        if self.retry_forced && !query.forced {
            self.finding(
                step,
                "the next query carries the force a refused or failed query owes".to_owned(),
                format!("the reactor offered unforced query {id}"),
            );
        }
        match &self.authority {
            Some(authority) if *authority != query.scope => self.finding(
                step,
                format!("the next query has the current authority {authority:?}"),
                format!("the reactor offered query {id} under {:?}", query.scope),
            ),
            Some(_) => {}
            None => self.authority = Some(query.scope.clone()),
        }
    }

    /// Judges that storage took `query` under its exact identity.
    fn judge_storage_identity(&mut self, step: u64, query: &BlockSyncQuery, taken: &StorageQuery) {
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

    /// Folds a reply, which retires the pending query only under its exact
    /// ID and authority. An owed force survives it.
    fn reply(&mut self, step: u64, query_id: u64, scope: &BodyAuthority, outcome: ReactorReply) {
        let matched = self.matches_pending(query_id, scope);
        let expected = if !matched {
            "stale"
        } else if self.authority.as_ref().is_some_and(|now| now != scope) {
            "stale-authority"
        } else {
            "current"
        };
        let actual = match outcome {
            ReactorReply::Stale => "stale",
            ReactorReply::StaleAuthority => "stale-authority",
            ReactorReply::AnchorAdvanced | ReactorReply::AnchorReset | ReactorReply::Accepted => {
                "current"
            }
        };
        if expected != actual {
            self.finding(
                step,
                format!("the reply to query {query_id} under {scope:?} classifies as {expected}"),
                format!("the reactor classified it as {outcome:?}"),
            );
        }
        if matched {
            self.pending = None;
            self.retry_armed = false;
        }
    }

    /// Folds a failure, which retires the pending query only under its exact
    /// ID and authority, and then owes a retry with that query's force.
    fn failure(&mut self, step: u64, query_id: u64, scope: &BodyAuthority, matched: bool) {
        let expected = self.matches_pending(query_id, scope);
        if expected != matched {
            self.finding(
                step,
                format!("the failure of query {query_id} under {scope:?} matches: {expected}"),
                format!("the reactor reported that it matches: {matched}"),
            );
        }
        if expected {
            if let Some(query) = self.pending.take() {
                self.retry_forced |= query.forced;
            }
            self.retry_armed = true;
        }
    }

    /// Folds an opening or view that changed the query state with no
    /// transition of its own. That change only lifts the retry gate, or
    /// clears the pending query, the gate, and the owed force together.
    fn gate_changed(&mut self, step: u64, state: &BlockSyncReactorState) {
        let lifted_only = state.pending == self.pending_id()
            && state.retry_forced == self.retry_forced
            && !state.retry_armed
            && self.retry_armed;
        let cleared = state.pending.is_none() && !state.retry_armed && !state.retry_forced;
        if lifted_only || cleared {
            if cleared {
                self.clear();
            } else {
                self.retry_armed = false;
            }
        } else {
            self.finding(
                step,
                "a gate change only lifts the retry gate or clears the query state".to_owned(),
                format!(
                    "pending {:?}, armed {}, forced {} became {state:?}",
                    self.pending_id(),
                    self.retry_armed,
                    self.retry_forced
                ),
            );
        }
    }

    /// Whether `(query_id, scope)` identifies the pending query.
    fn matches_pending(&self, query_id: u64, scope: &BodyAuthority) -> bool {
        self.pending
            .as_ref()
            .is_some_and(|pending| pending.query_id == query_id && pending.scope == *scope)
    }

    fn pending_id(&self) -> Option<u64> {
        self.pending.as_ref().map(|query| query.query_id)
    }

    /// Retires the pending query, the retry gate, and any owed force.
    fn clear(&mut self) {
        self.pending = None;
        self.retry_armed = false;
        self.retry_forced = false;
    }

    /// Compares the reported state with the required state. A mismatch is a
    /// finding, and the model resynchronizes, so later findings name later
    /// faults.
    fn compare(
        &mut self,
        step: u64,
        transition: &BlockSyncReactorTransition,
        state: &BlockSyncReactorState,
    ) {
        let required = (
            self.pending_id(),
            self.next_id,
            self.retry_armed,
            self.retry_forced,
            self.dispatch_closed,
        );
        let reported = (
            state.pending,
            state.next_query_id,
            state.retry_armed,
            state.retry_forced,
            state.dispatch_closed,
        );
        if required == reported {
            return;
        }
        self.finding(
            step,
            format!(
                "after {transition:?}, (pending, next ID, armed, forced, closed) is {required:?}"
            ),
            format!("the reactor reported {reported:?}"),
        );
        if self.pending_id() != state.pending {
            self.pending = state
                .pending
                .and_then(|query_id| self.accepted.get(&query_id).cloned());
        }
        self.next_id = state.next_query_id;
        self.retry_armed = state.retry_armed;
        self.retry_forced = state.retry_forced;
        self.dispatch_closed = state.dispatch_closed;
    }

    fn finding(&mut self, step: u64, expected: String, actual: String) {
        self.findings.push(Finding {
            step,
            expected,
            actual,
        });
    }
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
        storage::BLOCK_SERVICE,
        Observation, ObservationLog,
    };

    use BlockSyncReactorTransition as Fact;

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
        fn storage(&mut self, query: &BlockSyncQuery) -> &mut Self {
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

    fn query(query_id: u64, forced: bool) -> BlockSyncQuery {
        BlockSyncQuery {
            query_id,
            scope: authority(1),
            from: 1,
            limit: 8,
            header_tip: 100,
            forced,
        }
    }

    /// The state with `pending` and `next`, nothing armed, owed, or closed.
    fn state(pending: Option<u64>, next: u64) -> BlockSyncReactorState {
        BlockSyncReactorState {
            pending,
            next_query_id: Some(next),
            retry_armed: false,
            retry_forced: false,
            dispatch_closed: false,
            verified_tip: 0,
            request_floor: 0,
            header_tip: 100,
            reset_epoch: 0,
            reaction_epoch: 0,
        }
    }

    fn refused(query: BlockSyncQuery, refusal: &str) -> Fact {
        Fact::QueryRefused {
            query,
            refusal: refusal.to_owned(),
        }
    }

    /// Query 1 is accepted and pending.
    fn accepted_first() -> History {
        let mut history = History::default();
        history.reactor(Fact::QueryAccepted(query(1, false)), state(Some(1), 2));
        history
    }

    #[test]
    fn checker_accepts_an_accepted_query_at_storage_under_its_identity() {
        let mut history = accepted_first();
        history.storage(&query(1, false));
        let model = history.model();
        assert_eq!(model.reactor_queries.findings, []);
        assert!(model.reactor_queries.saw_query_at_storage);
    }

    #[test]
    fn checker_finds_a_storage_query_with_another_identity() {
        let mut history = accepted_first();
        history.storage(&BlockSyncQuery {
            limit: 9,
            ..query(1, false)
        });
        let findings = history.findings();
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert!(findings[0].actual.contains("limit: 9"), "{findings:?}");
        assert!(!history.model().reactor_queries.saw_query_at_storage);
    }

    #[test]
    fn checker_finds_a_storage_query_taken_twice() {
        let mut history = accepted_first();
        history.storage(&query(1, false)).storage(&query(1, false));
        assert_eq!(history.findings().len(), 1);
    }

    #[test]
    fn checker_judges_a_storage_query_that_precedes_its_acceptance() {
        let mut history = History::default();
        history
            .storage(&query(1, false))
            .reactor(Fact::QueryAccepted(query(1, false)), state(Some(1), 2));
        let mut model = history.model();
        assert_eq!(model.reactor_queries.findings, []);
        assert_eq!(model.reactor_queries.take_unaccepted(9), []);
    }

    /// A refused query keeps its ID, so storage taking it unaccepted stays a
    /// fault when no task can run.
    #[test]
    fn checker_finds_a_refused_query_at_storage() {
        let mut history = accepted_first();
        let armed = BlockSyncReactorState {
            retry_armed: true,
            ..state(Some(1), 2)
        };
        history
            .reactor(refused(query(2, false), "full"), armed)
            .storage(&query(2, false));
        let mut model = history.model();
        assert_eq!(model.reactor_queries.findings, []);
        let findings = model.reactor_queries.take_unaccepted(9);
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert!(findings[0].actual.contains("query 2"), "{findings:?}");
    }

    #[test]
    fn checker_holds_a_refusal_that_keeps_pending_and_next_id() {
        let mut history = accepted_first();
        let armed = BlockSyncReactorState {
            retry_armed: true,
            ..state(Some(1), 2)
        };
        history
            .reactor(refused(query(2, false), "full"), armed.clone())
            .reactor(Fact::RetryElapsed, state(Some(1), 2))
            .reactor(Fact::QueryAccepted(query(2, false)), state(Some(2), 3));
        assert_eq!(history.findings(), []);
    }

    /// `refusal-consumes-query-id`'s fact: the refused proposal took the ID.
    #[test]
    fn checker_finds_a_refusal_that_consumes_the_query_id() {
        let mut history = accepted_first();
        let consumed = BlockSyncReactorState {
            retry_armed: true,
            ..state(Some(1), 3)
        };
        history.reactor(refused(query(2, false), "full"), consumed);
        let findings = history.findings();
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert!(findings[0].actual.contains("Some(3)"), "{findings:?}");
    }

    #[test]
    fn checker_finds_a_refusal_that_drops_the_pending_query() {
        let mut history = accepted_first();
        let dropped = BlockSyncReactorState {
            retry_armed: true,
            ..state(None, 2)
        };
        history.reactor(refused(query(2, false), "full"), dropped);
        assert_eq!(history.findings().len(), 1);
    }

    #[test]
    fn checker_finds_a_refusal_that_arms_no_retry() {
        let mut history = accepted_first();
        history.reactor(refused(query(2, false), "full"), state(Some(1), 2));
        assert_eq!(history.findings().len(), 1);
    }

    /// After a forced refusal, the retry must carry the force, and accepting
    /// it discharges the debt.
    #[test]
    fn checker_holds_a_forced_retry_after_a_forced_refusal() {
        let mut history = accepted_first();
        let owed = BlockSyncReactorState {
            retry_armed: true,
            retry_forced: true,
            ..state(Some(1), 2)
        };
        let unarmed = BlockSyncReactorState {
            retry_forced: true,
            ..state(Some(1), 2)
        };
        history
            .reactor(refused(query(2, true), "control-reserve"), owed)
            .reactor(Fact::RetryElapsed, unarmed)
            .reactor(Fact::QueryAccepted(query(2, true)), state(Some(2), 3));
        assert_eq!(history.findings(), []);
    }

    #[test]
    fn checker_finds_a_retry_that_loses_the_owed_force() {
        let mut history = accepted_first();
        let owed = BlockSyncReactorState {
            retry_armed: true,
            retry_forced: true,
            ..state(Some(1), 2)
        };
        let unarmed = BlockSyncReactorState {
            retry_forced: true,
            ..state(Some(1), 2)
        };
        history
            .reactor(refused(query(2, true), "full"), owed)
            .reactor(Fact::RetryElapsed, unarmed)
            .reactor(Fact::QueryAccepted(query(2, false)), state(Some(2), 3));
        let findings = history.findings();
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert!(findings[0].actual.contains("unforced"), "{findings:?}");
    }

    #[test]
    fn checker_finds_a_proposal_past_the_armed_retry_gate() {
        let mut history = accepted_first();
        let armed = BlockSyncReactorState {
            retry_armed: true,
            ..state(Some(1), 2)
        };
        history
            .reactor(refused(query(2, false), "full"), armed)
            .reactor(Fact::QueryAccepted(query(2, false)), state(Some(2), 3));
        let findings = history.findings();
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert!(findings[0].expected.contains("retry gate"), "{findings:?}");
    }

    fn closed(pending: Option<u64>, next: u64) -> BlockSyncReactorState {
        BlockSyncReactorState {
            dispatch_closed: true,
            ..state(pending, next)
        }
    }

    /// A closed channel keeps the pending query, cancels the owed retry and
    /// force, and leaves the reply route open.
    #[test]
    fn checker_holds_a_closed_channel_that_owes_nothing() {
        let mut history = accepted_first();
        let owed = BlockSyncReactorState {
            retry_armed: true,
            retry_forced: true,
            ..state(Some(1), 2)
        };
        let unarmed = BlockSyncReactorState {
            retry_forced: true,
            ..state(Some(1), 2)
        };
        history
            .reactor(refused(query(2, true), "full"), owed)
            .reactor(Fact::RetryElapsed, unarmed)
            .reactor(refused(query(2, true), "closed"), closed(Some(1), 2))
            .reactor(
                Fact::QueryReply {
                    query_id: 1,
                    scope: authority(1),
                    outcome: ReactorReply::Accepted,
                },
                closed(None, 2),
            );
        assert_eq!(history.findings(), []);
        let model = history.model();
        assert!(model.reactor_queries.saw_retry_refusal);
        assert!(model.reactor_queries.saw_closed_refusal);
    }

    #[test]
    fn checker_finds_a_closed_channel_that_arms_a_retry() {
        let mut history = accepted_first();
        let armed = BlockSyncReactorState {
            retry_armed: true,
            ..closed(Some(1), 2)
        };
        history.reactor(refused(query(2, false), "closed"), armed);
        assert_eq!(history.findings().len(), 1);
    }

    #[test]
    fn checker_finds_a_proposal_after_the_channel_closed() {
        let mut history = accepted_first();
        history
            .reactor(refused(query(2, false), "closed"), closed(Some(1), 2))
            .reactor(refused(query(2, false), "closed"), closed(Some(1), 2));
        let findings = history.findings();
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert!(findings[0].actual.contains("after"), "{findings:?}");
    }

    #[test]
    fn checker_finds_a_reply_that_retires_under_a_wrong_identity() {
        let mut history = accepted_first();
        history.reactor(
            Fact::QueryReply {
                query_id: 1,
                scope: authority(2),
                outcome: ReactorReply::Accepted,
            },
            state(None, 2),
        );
        // The classification and the retired pending query each disagree.
        assert_eq!(history.findings().len(), 2);
    }

    #[test]
    fn checker_holds_a_stale_reply_that_leaves_the_pending_query() {
        let mut history = accepted_first();
        history.reactor(
            Fact::QueryReply {
                query_id: 7,
                scope: authority(1),
                outcome: ReactorReply::Stale,
            },
            state(Some(1), 2),
        );
        assert_eq!(history.findings(), []);
    }

    #[test]
    fn checker_owes_a_failed_forced_query_a_forced_retry() {
        let mut history = History::default();
        history
            .reactor(Fact::QueryAccepted(query(1, true)), state(Some(1), 2))
            .reactor(
                Fact::QueryFailed {
                    query_id: 1,
                    scope: authority(1),
                    matched: true,
                },
                BlockSyncReactorState {
                    retry_armed: true,
                    retry_forced: true,
                    ..state(None, 2)
                },
            );
        assert_eq!(history.findings(), []);
        history.reactor(
            Fact::QueryFailed {
                query_id: 1,
                scope: authority(1),
                matched: true,
            },
            BlockSyncReactorState {
                retry_armed: true,
                retry_forced: true,
                ..state(None, 2)
            },
        );
        // The pending query already retired, so the second failure is stale.
        assert_eq!(history.findings().len(), 1);
    }

    /// A reset, an authority change, and a tip reset each supersede the
    /// query state, and a later query takes the next ID.
    #[test]
    fn checker_clears_on_each_superseding_transition() {
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

    #[test]
    fn checker_finds_a_query_under_a_retired_authority() {
        let mut history = accepted_first();
        history
            .reactor(
                Fact::AuthorityChanged {
                    authority: authority(2),
                    epoch_changed: true,
                },
                state(None, 2),
            )
            .reactor(Fact::QueryAccepted(query(2, false)), state(Some(2), 3));
        let findings = history.findings();
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert!(findings[0].expected.contains("authority"), "{findings:?}");
    }

    /// A forced opening lifts the gate alone. A covered range clears the
    /// whole state. Neither makes the next proposal a fault.
    #[test]
    fn checker_holds_each_gate_change_shape() {
        let armed = BlockSyncReactorState {
            retry_armed: true,
            ..state(Some(1), 2)
        };
        let mut history = accepted_first();
        history
            .reactor(refused(query(2, false), "full"), armed)
            .reactor(Fact::QueryGateChanged, state(Some(1), 2))
            .reactor(Fact::QueryGateChanged, state(None, 2))
            .reactor(Fact::QueryAccepted(query(2, false)), state(Some(2), 3));
        assert_eq!(history.findings(), []);
    }

    /// A gate change neither arms a gate nor retires the pending query alone.
    #[test]
    fn checker_finds_a_gate_change_of_another_shape() {
        let mut history = accepted_first();
        history.reactor(
            Fact::QueryGateChanged,
            BlockSyncReactorState {
                retry_armed: true,
                ..state(Some(1), 2)
            },
        );
        let findings = history.findings();
        assert!(
            findings
                .iter()
                .any(|finding| finding.expected.contains("gate change")),
            "{findings:?}"
        );
    }
}
