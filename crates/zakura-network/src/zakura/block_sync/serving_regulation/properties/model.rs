//! Expected ownership, derived from the resource contract rather than production fields.

use std::{collections::VecDeque, time::Duration};

use super::scenario::*;
use crate::zakura::regulation::test_support::RateModel;

#[derive(Clone, Debug)]
struct Request {
    session: usize,
    provisional: bool,
    ledger: bool,
    query_owners: usize,
    query_claimed: bool,
    sent_block: bool,
    sent_terminal: bool,
    transferred: u64,
}

#[derive(Clone, Debug, Default)]
struct Session {
    peer: usize,
    queue: VecDeque<u64>,
    writing: Option<u64>,
}

#[derive(Clone, Debug)]
pub(super) struct Model {
    limit: Limit,
    node_rate: RateModel,
    peer_rates: [RateModel; 2],
    current_sessions: [usize; 2],
    sessions: Vec<Session>,
    requests: [Option<Request>; REQUEST_SLOTS],
    inputs: [Option<usize>; INPUT_SLOTS],
    block_payload_bytes: u64,
}

impl Model {
    pub(super) fn new(limit: Limit, block_payload_bytes: u64) -> Self {
        let config = limit.config();
        let policy = config.get_blocks_regulation;
        Self {
            limit,
            node_rate: RateModel::new(
                policy.node_rate_capacity_bytes,
                policy.node_rate_bytes_per_second,
            ),
            peer_rates: std::array::from_fn(|_| {
                RateModel::new(
                    policy.peer_rate_capacity_bytes,
                    policy.peer_rate_bytes_per_second,
                )
            }),
            current_sessions: [0, 1],
            sessions: vec![
                Session {
                    peer: 0,
                    ..Default::default()
                },
                Session {
                    peer: 1,
                    ..Default::default()
                },
            ],
            requests: std::array::from_fn(|_| None),
            inputs: [None; INPUT_SLOTS],
            block_payload_bytes,
        }
    }

    /// Enabled actions depend only on reference state. Failed admission remains enabled.
    pub(super) fn actions(&self) -> Vec<Action> {
        let mut actions = vec![
            Action::Advance { millis: 0 },
            Action::Advance { millis: 1 },
            Action::Advance { millis: 999 },
            Action::Advance { millis: 1001 },
        ];
        for peer in 0..2 {
            actions.push(Action::Reconnect { peer });
            for request in 0..REQUEST_SLOTS {
                actions.push(Action::Admit { peer, request });
            }
            for input in 0..INPUT_SLOTS {
                actions.push(Action::RetainInput { peer, input });
            }
        }
        for request in 0..REQUEST_SLOTS {
            actions.extend([
                Action::Commit { request },
                Action::ClaimQuery { request },
                Action::CloneQueryLease { request },
                Action::DropQueryLease { request },
                Action::DropLedger { request },
                Action::QueueBlock { request },
                Action::QueueTerminal { request },
            ]);
        }
        for input in 0..INPUT_SLOTS {
            actions.push(Action::DropInput { input });
        }
        for session in 0..self.sessions.len() {
            actions.push(Action::BeginWrite { session });
            for outcome in [WriteEnd::Complete, WriteEnd::Fail, WriteEnd::Cancel] {
                actions.push(Action::EndWrite { session, outcome });
            }
        }
        actions.retain(|action| self.is_enabled(action));
        actions
    }

    /// Replay validity is independent of the generator's choice distribution.
    pub(super) fn is_enabled(&self, action: &Action) -> bool {
        match *action {
            Action::Admit { peer, request } => {
                peer < 2 && self.requests.get(request).is_some_and(Option::is_none)
            }
            Action::Commit { request } => {
                self.request_matches(request, |state| state.provisional && state.ledger)
            }
            Action::ClaimQuery { request } | Action::DropQueryLease { request } => {
                self.request_matches(request, |state| state.query_owners > 0)
            }
            Action::CloneQueryLease { request } => self.request_matches(request, |state| {
                state.query_owners > 0 && state.query_owners < 3
            }),
            Action::DropLedger { request } => self.request_matches(request, |state| state.ledger),
            Action::QueueBlock { request } => self.request_matches(request, |state| {
                !state.provisional && state.ledger && !state.sent_block && !state.sent_terminal
            }),
            Action::QueueTerminal { request } => self.request_matches(request, |state| {
                !state.provisional && state.ledger && !state.sent_terminal
            }),
            Action::BeginWrite { session } => self
                .sessions
                .get(session)
                .is_some_and(|state| state.writing.is_none() && !state.queue.is_empty()),
            Action::EndWrite { session, .. } => self
                .sessions
                .get(session)
                .is_some_and(|state| state.writing.is_some()),
            Action::RetainInput { peer, input } => {
                peer < 2 && self.inputs.get(input).is_some_and(Option::is_none)
            }
            Action::DropInput { input } => self.inputs.get(input).is_some_and(Option::is_some),
            Action::Reconnect { peer } => peer < 2 && self.sessions.len() < 8,
            Action::Advance { millis } => millis <= 10_000,
        }
    }

    fn request_matches(&self, request: usize, predicate: impl FnOnce(&Request) -> bool) -> bool {
        self.requests
            .get(request)
            .and_then(Option::as_ref)
            .is_some_and(predicate)
    }

    /// Reject incompatible replay inputs instead of quietly skipping them.
    pub(super) fn apply(&mut self, action: &Action) -> Outcome {
        assert!(self.is_enabled(action), "invalid replay action: {action:?}");
        let mut outcome = Outcome::Done;
        match *action {
            Action::Admit { peer, request } => {
                let session = self.current_sessions[peer];
                let state = self.snapshot();
                let config = self.limit.config();
                let policy = config.get_blocks_regulation;
                let blocked = [
                    (state.peer_rates[peer] < CHARGE, Limit::PeerRate),
                    (state.node_rate < CHARGE, Limit::NodeRate),
                    (
                        state.session_active[session]
                            >= usize::try_from(config.max_inflight_requests).unwrap(),
                        Limit::PeerActive,
                    ),
                    (
                        state.node_active >= policy.node_active_requests,
                        Limit::NodeActive,
                    ),
                    (
                        state.node_bytes + RESPONSE_CAP > policy.node_outstanding_bytes,
                        Limit::NodeBytes,
                    ),
                    (
                        state.session_bytes[session] + RESPONSE_CAP > policy.peer_outstanding_bytes,
                        Limit::PeerBytes,
                    ),
                ]
                .into_iter()
                .find_map(|(full, limit)| full.then_some(limit));
                if blocked.is_none() {
                    assert!(self.node_rate.reserve(CHARGE));
                    assert!(self.peer_rates[peer].reserve(CHARGE));
                    self.requests[request] = Some(Request {
                        session,
                        provisional: true,
                        ledger: true,
                        query_owners: 0,
                        query_claimed: false,
                        sent_block: false,
                        sent_terminal: false,
                        transferred: 0,
                    });
                }
                outcome = Outcome::Admission(blocked);
            }
            Action::Commit { request } => {
                let state = self.requests[request].as_mut().unwrap();
                state.provisional = false;
                state.query_owners = 1;
            }
            Action::ClaimQuery { request } => {
                let state = self.requests[request].as_mut().unwrap();
                let starts = state.ledger && !state.query_claimed;
                state.query_claimed |= starts;
                outcome = Outcome::Started(starts);
            }
            Action::CloneQueryLease { request } => {
                self.requests[request].as_mut().unwrap().query_owners += 1
            }
            Action::DropQueryLease { request } => {
                self.requests[request].as_mut().unwrap().query_owners -= 1
            }
            Action::DropLedger { request } => {
                self.requests[request].as_mut().unwrap().ledger = false
            }
            Action::QueueBlock { request } | Action::QueueTerminal { request } => {
                let state = self.requests[request].as_mut().unwrap();
                let queue = &mut self.sessions[state.session].queue;
                let queued = queue.len() < QUEUE_DEPTH;
                if queued {
                    let bytes = if matches!(action, Action::QueueBlock { .. }) {
                        state.sent_block = true;
                        self.block_payload_bytes
                    } else {
                        state.sent_terminal = true;
                        9
                    };
                    queue.push_back(bytes);
                    state.transferred += bytes;
                    assert!(state.transferred <= RESPONSE_CAP);
                }
                outcome = Outcome::Queued(queued);
            }
            Action::BeginWrite { session } => {
                self.sessions[session].writing = self.sessions[session].queue.pop_front()
            }
            Action::EndWrite { session, .. } => self.sessions[session].writing = None,
            Action::RetainInput { peer, input } => {
                let session = self.current_sessions[peer];
                let state = self.snapshot();
                let admitted = state.node_pending < 3 && state.session_pending[session] < 2;
                if admitted {
                    self.inputs[input] = Some(session);
                }
                outcome = Outcome::Retained(admitted);
            }
            Action::DropInput { input } => self.inputs[input] = None,
            Action::Reconnect { peer } => {
                let old = self.current_sessions[peer];
                for request in self
                    .requests
                    .iter_mut()
                    .flatten()
                    .filter(|request| request.session == old)
                {
                    request.ledger = false;
                }
                for input in &mut self.inputs {
                    if *input == Some(old) {
                        *input = None;
                    }
                }
                self.current_sessions[peer] = self.sessions.len();
                self.sessions.push(Session {
                    peer,
                    ..Default::default()
                });
            }
            Action::Advance { millis } => {
                let elapsed = Duration::from_millis(millis);
                self.node_rate.advance(elapsed);
                for rate in &mut self.peer_rates {
                    rate.advance(elapsed);
                }
            }
        }
        self.settle_unowned();
        outcome
    }

    fn settle_unowned(&mut self) {
        for state in &mut self.requests {
            let Some(request) = state else { continue };
            if request.ledger || request.query_owners > 0 {
                continue;
            }
            let refund = if request.provisional {
                CHARGE
            } else {
                RESPONSE_CAP - request.transferred
            };
            self.node_rate.refund(refund);
            self.peer_rates[self.sessions[request.session].peer].refund(refund);
            *state = None;
        }
    }

    pub(super) fn snapshot(&self) -> Snapshot {
        let mut state = Snapshot {
            node_rate: self.node_rate.available(),
            peer_rates: std::array::from_fn(|peer| self.peer_rates[peer].available()),
            node_bytes: 0,
            session_bytes: vec![0; self.sessions.len()],
            node_active: 0,
            session_active: vec![0; self.sessions.len()],
            node_pending: 0,
            session_pending: vec![0; self.sessions.len()],
        };
        for request in self.requests.iter().flatten() {
            state.session_bytes[request.session] += RESPONSE_CAP - request.transferred;
            state.session_active[request.session] += 1;
        }
        for (session, output) in self.sessions.iter().enumerate() {
            state.session_bytes[session] +=
                output.queue.iter().sum::<u64>() + output.writing.unwrap_or(0);
        }
        for session in self.inputs.iter().flatten() {
            state.session_pending[*session] += 1;
        }
        state.node_bytes = state.session_bytes.iter().sum();
        state.node_active = state.session_active.iter().sum();
        state.node_pending = state.session_pending.iter().sum();
        state
    }

    /// End all owners through the same actions used by generated histories.
    pub(super) fn cleanup(&mut self) -> Vec<Action> {
        let mut cleanup = Vec::new();
        loop {
            let next = self.actions().into_iter().find(|action| {
                matches!(
                    action,
                    Action::DropLedger { .. }
                        | Action::DropQueryLease { .. }
                        | Action::DropInput { .. }
                        | Action::BeginWrite { .. }
                        | Action::EndWrite {
                            outcome: WriteEnd::Complete,
                            ..
                        }
                )
            });
            let Some(action) = next else { break };
            self.apply(&action);
            cleanup.push(action);
        }
        cleanup
    }
}
