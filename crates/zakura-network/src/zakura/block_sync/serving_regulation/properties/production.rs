//! Runs ownership actions against the production regulator, encoder, and writer.

use std::{future::Future, pin::Pin};

use futures::FutureExt;
use tokio::sync::oneshot;

use super::{super::*, scenario::*};
use crate::zakura::transport::{worker_framed_channel, FramedWorkerRecv};

struct RequestOwners {
    session: usize,
    attempt: Option<AdmissionAttempt>,
    permit: Option<GetBlocksServingPermit>,
    query_leases: Vec<BlockRangeReadLease>,
    sent_block: bool,
    lifetime: Option<std::sync::Weak<crate::zakura::regulation::WorkResources>>,
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
    // Counter handles keep observations alive without retaining work ownership.
    active: SlotBudget,
}

pub(super) struct Production {
    regulator: GetBlocksServingRegulator,
    current_sessions: [usize; 2],
    sessions: Vec<Session>,
    requests: [Option<RequestOwners>; REQUEST_SLOTS],
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
            fixture,
        };
        production.connect(0);
        production.connect(1);
        production
    }

    fn connect(&mut self, peer: usize) {
        let identity = ZakuraPeerId::new(vec![u8::try_from(peer + 1).unwrap(); 32]).unwrap();
        let generation = u64::try_from(self.sessions.len()).unwrap();
        let account = self.regulator.session(identity.clone());
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
            active: account.work.peer_budget().clone(),
            account: Some(account),
        });
    }

    pub(super) async fn apply(&mut self, action: &Action) -> Outcome {
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
                            lifetime: Some(attempt.work.weak_resources()),
                            attempt: Some(attempt),
                            permit: None,
                            query_leases: Vec::new(),
                            sent_block: false,
                        });
                        Outcome::Admission(None)
                    }
                    Err(blocked) => Outcome::Admission(Some(match blocked.kind() {
                        WorkBound::Peer => Limit::PeerActive,
                        WorkBound::Node => Limit::NodeActive,
                    })),
                }
            }
            Action::Commit { request } => {
                let owners = self.requests[request].as_mut().unwrap();
                let permit = owners.attempt.take().unwrap().commit();
                owners.lifetime = Some(permit.response.weak_resources());
                owners.query_leases.push(permit.work_lease());
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
                let message = if matches!(action, Action::QueueBlock { .. }) {
                    BlockSyncMessage::Block(self.fixture.clone())
                } else if owners.sent_block {
                    BlockSyncMessage::BlocksDone {
                        start_height: block::Height(1),
                        returned: 1,
                    }
                } else {
                    BlockSyncMessage::RangeUnavailable {
                        start_height: block::Height(1),
                        count: 1,
                    }
                };
                let sender = sender.data_sender();
                // Only the scenario removes queued frames. A full queue stays
                // pending until a later action; queue-wait cancellation has a
                // separate production-task test.
                let queued = sender.capacity() > 0;
                if queued {
                    crate::zakura::block_sync::serving::send_response(&sender, permit, message)
                        .await
                        .expect("the modeled queue has a reserved observation slot");
                    if matches!(action, Action::QueueBlock { .. }) {
                        owners.sent_block = true;
                    }
                }
                Outcome::Queued(queued)
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
                    && owners
                        .lifetime
                        .as_ref()
                        .is_none_or(|owner| owner.upgrade().is_none())
            }) {
                *owners = None;
            }
        }
        outcome
    }

    pub(super) fn snapshot(&self) -> Snapshot {
        let node = &self.regulator.inner;
        let mut session_active = vec![0; self.sessions.len()];
        // Observe each real allocation through a weak handle. Reconnected sessions
        // share a peer counter, so reading that counter twice would double-count.
        for owners in self.requests.iter().flatten() {
            if owners
                .lifetime
                .as_ref()
                .is_some_and(|owner| owner.upgrade().is_some())
            {
                session_active[owners.session] += 1;
            }
        }
        Snapshot {
            node_active: node.node_active.reserved(),
            peer_active: self
                .current_sessions
                .map(|session| self.sessions[session].active.reserved()),
            session_active,
        }
    }
}
