//! Runs ownership actions against the production regulator, encoder, and writer.

use std::{future::Future, pin::Pin};

use futures::FutureExt;
use tokio::sync::oneshot;

use super::{super::*, scenario::*};
use crate::zakura::transport::{worker_framed_channel, FramedWorkerRecv};
use crate::zakura::OrderedSendError;

struct RequestOwners {
    session: usize,
    attempt: Option<AdmissionAttempt>,
    permit: Option<GetBlocksServingPermit>,
    query_leases: Vec<BlockRangeQueryLease>,
    sent_block: bool,
}

struct PendingWrite {
    finish: oneshot::Sender<bool>,
    task: Pin<Box<dyn Future<Output = Result<(), ()>>>>,
}

struct Session {
    account: Option<GetBlocksServingSession>,
    sender: BlockSyncPeerSession,
    receiver: FramedWorkerRecv,
    writing: Option<PendingWrite>,
    // Counter handles keep observations alive without retaining a permit,
    // SessionResources, or the identity account used by cache eviction.
    outstanding: OutstandingByteBudget,
    active: SlotBudget,
    pending: SlotBudget,
}

pub(super) struct Production {
    regulator: GetBlocksServingRegulator,
    current_sessions: [usize; 2],
    sessions: Vec<Session>,
    requests: [Option<RequestOwners>; REQUEST_SLOTS],
    inputs: [Option<(usize, PendingGetBlocksRequest)>; INPUT_SLOTS],
    fixture: Arc<block::Block>,
}

impl Production {
    pub(super) fn new(limit: Limit, fixture: Arc<block::Block>) -> Self {
        let config = limit.config();
        validate_config(&config).expect("the test configuration admits its largest legal request");
        let mut production = Self {
            regulator: GetBlocksServingRegulator::new(config),
            current_sessions: [0, 1],
            sessions: Vec::new(),
            requests: std::array::from_fn(|_| None),
            inputs: std::array::from_fn(|_| None),
            fixture,
        };
        production.connect(0);
        production.connect(1);
        production
    }

    fn connect(&mut self, peer: usize) {
        let identity = ZakuraPeerId::new(vec![u8::try_from(peer + 1).unwrap(); 32]).unwrap();
        let generation = u64::try_from(self.sessions.len()).unwrap();
        let account = self.regulator.session(identity.clone(), generation);
        let (sender, receiver) = worker_framed_channel(QUEUE_DEPTH);
        self.current_sessions[peer] = self.sessions.len();
        self.sessions.push(Session {
            sender: BlockSyncPeerSession::for_test_with_session_id(
                identity,
                generation,
                sender,
                CancellationToken::new(),
            ),
            receiver,
            writing: None,
            outstanding: account.resources.outstanding.clone(),
            active: account.resources.active.clone(),
            pending: account.resources.pending.clone(),
            account: Some(account),
        });
    }

    pub(super) fn apply(&mut self, action: &Action) -> Outcome {
        let outcome = match *action {
            Action::Admit { peer, request } => {
                let session = self.current_sessions[peer];
                match self.sessions[session]
                    .account
                    .as_ref()
                    .unwrap()
                    .try_admit(1)
                {
                    Ok(attempt) => {
                        self.requests[request] = Some(RequestOwners {
                            session,
                            attempt: Some(attempt),
                            permit: None,
                            query_leases: Vec::new(),
                            sent_block: false,
                        });
                        Outcome::Admission(None)
                    }
                    Err(blocked) => Outcome::Admission(Some(match blocked.kind() {
                        BoundKind::PeerRate => Limit::PeerRate,
                        BoundKind::NodeRate => Limit::NodeRate,
                        BoundKind::PeerActive => Limit::PeerActive,
                        BoundKind::NodeActive => Limit::NodeActive,
                        BoundKind::PeerOutstanding => Limit::PeerBytes,
                        BoundKind::NodeOutstanding => Limit::NodeBytes,
                    })),
                }
            }
            Action::Commit { request } => {
                let owners = self.requests[request].as_mut().unwrap();
                let permit = owners.attempt.take().unwrap().commit();
                owners.query_leases.push(permit.query_lease());
                owners.permit = Some(permit);
                Outcome::Done
            }
            Action::ClaimQuery { request } => Outcome::Started(
                self.requests[request].as_ref().unwrap().query_leases[0].try_start(),
            ),
            Action::CloneQueryLease { request } => {
                let query_leases = &mut self.requests[request].as_mut().unwrap().query_leases;
                query_leases.push(query_leases[0].clone());
                Outcome::Done
            }
            Action::DropQueryLease { request } => {
                drop(self.requests[request].as_mut().unwrap().query_leases.pop());
                Outcome::Done
            }
            Action::DropLedger { request } => {
                let owners = self.requests[request].as_mut().unwrap();
                drop(owners.attempt.take());
                drop(owners.permit.take());
                Outcome::Done
            }
            Action::QueueBlock { request } | Action::QueueTerminal { request } => {
                let owners = self.requests[request].as_mut().unwrap();
                let sender = &self.sessions[owners.session].sender;
                let permit = owners.permit.as_mut().unwrap();
                let result = if matches!(action, Action::QueueBlock { .. }) {
                    sender.try_send_regulated_block(self.fixture.clone(), permit)
                } else if owners.sent_block {
                    sender.try_send_regulated_blocks_done(block::Height(1), 1, permit)
                } else {
                    sender.try_send_regulated_message(
                        BlockSyncMessage::RangeUnavailable {
                            start_height: block::Height(1),
                            count: 1,
                        },
                        permit,
                    )
                };
                if result.is_ok() && matches!(action, Action::QueueBlock { .. }) {
                    owners.sent_block = true;
                }
                if let Err(error) = &result {
                    assert!(
                        matches!(error, OrderedSendError::Full),
                        "unexpected send failure: {error:?}"
                    );
                }
                Outcome::Queued(result.is_ok())
            }
            Action::BeginWrite { session } => {
                let output = &mut self.sessions[session];
                let frame = output
                    .receiver
                    .recv()
                    .now_or_never()
                    .expect("the modeled queue is readable")
                    .unwrap();
                let (finish, completion) = oneshot::channel();
                let mut task = Box::pin(frame.write_with(|_frame| async move {
                    match completion.await {
                        Ok(true) => Ok(()),
                        _ => Err(()),
                    }
                }));
                assert!(
                    task.as_mut().now_or_never().is_none(),
                    "the controlled write must retain its frame"
                );
                output.writing = Some(PendingWrite { finish, task });
                Outcome::Done
            }
            Action::EndWrite { session, outcome } => {
                let mut write = self.sessions[session].writing.take().unwrap();
                if outcome != WriteEnd::Cancel {
                    write.finish.send(outcome == WriteEnd::Complete).unwrap();
                    assert_eq!(
                        write.task.as_mut().now_or_never(),
                        Some(if outcome == WriteEnd::Complete {
                            Ok(())
                        } else {
                            Err(())
                        })
                    );
                }
                Outcome::Done
            }
            Action::RetainInput { peer, input } => {
                let session = self.current_sessions[peer];
                let result = self.sessions[session]
                    .account
                    .as_ref()
                    .unwrap()
                    .try_retain_input(block::Height(1), 1);
                let admitted = result.is_ok();
                if let Ok(request) = result {
                    self.inputs[input] = Some((session, request));
                }
                Outcome::Retained(admitted)
            }
            Action::DropInput { input } => {
                drop(self.inputs[input].take());
                Outcome::Done
            }
            Action::Reconnect { peer } => {
                let old = self.current_sessions[peer];
                self.sessions[old].sender.cancel_token().cancel();
                for owners in self
                    .requests
                    .iter_mut()
                    .flatten()
                    .filter(|owners| owners.session == old)
                {
                    drop(owners.attempt.take());
                    drop(owners.permit.take());
                }
                for input in &mut self.inputs {
                    if input.as_ref().is_some_and(|(session, _)| *session == old) {
                        drop(input.take());
                    }
                }
                drop(self.sessions[old].account.take());
                self.connect(peer);
                Outcome::Done
            }
            Action::Advance { .. } => unreachable!("the runner advances the shared Tokio clock"),
        };
        for owners in &mut self.requests {
            if owners.as_ref().is_some_and(|owners| {
                owners.attempt.is_none()
                    && owners.permit.is_none()
                    && owners.query_leases.is_empty()
            }) {
                *owners = None;
            }
        }
        outcome
    }

    pub(super) fn snapshot(&self) -> Snapshot {
        let node = &self.regulator.inner;
        Snapshot {
            node_rate: node.node_rate.available(),
            peer_rates: std::array::from_fn(|peer| {
                self.sessions[self.current_sessions[peer]]
                    .account
                    .as_ref()
                    .unwrap()
                    .peer_rate_available()
            }),
            node_bytes: node.node_outstanding.reserved(),
            session_bytes: self
                .sessions
                .iter()
                .map(|session| session.outstanding.reserved())
                .collect(),
            node_active: node.node_active.reserved(),
            session_active: self
                .sessions
                .iter()
                .map(|session| session.active.reserved())
                .collect(),
            node_pending: node.node_pending.reserved(),
            session_pending: self
                .sessions
                .iter()
                .map(|session| session.pending.reserved())
                .collect(),
        }
    }
}
