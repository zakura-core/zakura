//! One discovery session's peer routine: its pure [`PeerState`] and the
//! [`PeerIO`] that runs the session's writer.
//!
//! The session's reader records exchange progress in [`SharedPeerState`].
//! The writer asks the state for each step, runs it, and reports its outcome.
//! Only the state decides when the exchange completes, when it refreshes, and
//! whether the session closes its connection. IO consults the connection's
//! admitted service owners and the current session right before it commits
//! to a close.

mod state;
#[cfg(test)]
mod tests;

use std::{sync::Arc, time::Duration};

use zakura_quic::NodeId;
use zakura_rt::sync::{CancellationToken, Mutex, MutexGuard, Notify, PoisonError};

use state::{CloseReason, InitialWait, Ownership, PeerInput, PeerOutput, PeerState, SessionCharge};
#[cfg(test)]
pub(super) use state::{
    ExchangeProgress, DISCOVERY_ANSWER_WRITE_DEADLINE, DISCOVERY_INITIAL_EXCHANGE_TIMEOUT,
};

use super::{
    protocol::{
        DiscoveryMessage, GetPeers, GetServices, ZakuraDiscoveryHandle, ZakuraNodeRecord,
        MAX_DISCOVERY_RECORDS_PER_RESPONSE,
    },
    requester::{AdmittedAnswer, Queries, QueryUnsent},
    service::{DiscoveryPeerSession, SessionSlot},
    serving::AnswersWritten,
};
use crate::zakura::{
    regulation::{CadenceSendError, CadenceSender},
    CloseCause, Service, ZakuraConnId, ZakuraPeerId,
};

/// One session's [`PeerState`], shared by its reader and its writer.
///
/// The reader records the exchange's progress, and the writer drives the
/// session's steps.
#[derive(Clone, Debug, Default)]
pub(super) struct SharedPeerState(Arc<SharedInner>);

#[derive(Debug, Default)]
struct SharedInner {
    state: Mutex<PeerState>,
    /// Wakes the writer's wait for a complete exchange.
    progressed: Notify,
}

impl SharedPeerState {
    /// No holder panics, so the lock is never poisoned.
    fn lock(&self) -> MutexGuard<'_, PeerState> {
        self.0.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records the peer's authentic `Hello`, even one that local policy
    /// dropped.
    pub(super) fn record_hello(&self) {
        self.lock().record_hello();
        self.0.progressed.notify_waiters();
    }

    /// Records an imported answer to this session's query.
    pub(super) fn record_answer(&self, answer: &AdmittedAnswer) {
        self.lock().record_answer(answer);
        self.0.progressed.notify_waiters();
    }

    /// The exchange progress, for tests that check what a handler recorded.
    #[cfg(test)]
    pub(super) fn progress(&self) -> ExchangeProgress {
        self.lock().progress()
    }

    /// Resolves once the peer answered every part of the exchange.
    async fn wait_complete(&self) {
        loop {
            let progressed = self.0.progressed.notified();
            zakura_rt::pin!(progressed);
            progressed.as_mut().enable();
            if self.lock().is_complete() {
                return;
            }
            progressed.await;
        }
    }
}

/// The runtime side of one discovery session's writer: stream IO, waits,
/// and the session's record.
pub(super) struct PeerIO {
    pub(super) handle: ZakuraDiscoveryHandle,
    pub(super) session: DiscoveryPeerSession,
    /// The session's record, which also names the session.
    pub(super) slot: SessionSlot,
    pub(super) state: SharedPeerState,
    /// Paces `Hello` to its row's send interval.
    pub(super) hello: CadenceSender<DiscoveryMessage>,
    /// This session's queries. A query waits until the previous query of its
    /// kind is answered.
    pub(super) queries: Queries,
    /// Resolves once this session wrote every answer it admitted.
    pub(super) answers: AnswersWritten,
    /// The other services that can own this session's connection.
    pub(super) connection_owners: Vec<Arc<dyn Service>>,
    pub(super) peer_node_id: NodeId,
    pub(super) connection_cancel: CancellationToken,
    pub(super) close_cause: CloseCause,
}

impl PeerIO {
    /// Runs the session's steps until the state ends the session.
    pub(super) async fn run(mut self) {
        let mut step = self.state.lock().start();
        loop {
            if let Some(charge) = step.charge {
                self.charge(charge);
            }
            let input = match step.next {
                PeerOutput::Exchange => PeerInput::Exchanged {
                    sent: self.exchange().await.is_ok(),
                },
                PeerOutput::WaitInitial { timeout } => {
                    PeerInput::InitialWaitEnded(self.wait_initial(timeout).await)
                }
                PeerOutput::CheckCurrentSession => PeerInput::CurrentSession {
                    current: self.handle.is_current_session(&self.slot.key).await,
                },
                PeerOutput::CheckSoleOwner => PeerInput::Owners(self.owners().await),
                PeerOutput::Refresh { interval } => PeerInput::Refreshed {
                    sent: self.refresh_after(interval).await,
                },
                PeerOutput::WaitForAnswers { deadline } => {
                    self.wait_for_answers(deadline).await;
                    PeerInput::AnswersSettled
                }
                PeerOutput::MarkShortLived => {
                    self.handle
                        .mark_short_lived_exchange(&self.peer_node_id)
                        .await;
                    PeerInput::ShortLivedMarked
                }
                PeerOutput::End { close } => return self.end(close).await,
            };
            step = self.state.lock().on_input(input);
        }
    }

    fn charge(&self, charge: SessionCharge) {
        match charge {
            SessionCharge::Succeed => self.slot.succeed(),
            SessionCharge::Fail => self.slot.fail(),
            SessionCharge::Retire => self.slot.retire(),
        }
    }

    async fn wait_initial(&self, timeout: Duration) -> InitialWait {
        let cancel = self.session.cancel_token();
        zakura_rt::select! {
            biased;
            _ = cancel.cancelled() => InitialWait::Cancelled,
            _ = self.state.wait_complete() => InitialWait::Complete,
            _ = zakura_rt::time::sleep(timeout) => InitialWait::TimedOut,
        }
    }

    /// Consults the connection's admitted service owners, then whether this
    /// session is still current. Advertised service names never count.
    async fn owners(&self) -> Ownership {
        if peer_has_other_service_owner(
            &self.connection_owners,
            self.session.peer_id(),
            self.slot.key.conn_id,
        ) {
            return Ownership::Shared;
        }
        if self.handle.is_current_session(&self.slot.key).await {
            Ownership::Sole
        } else {
            Ownership::Superseded
        }
    }

    /// Exchanges again once `interval` passes. Returns false once the session
    /// is cancelled or gone.
    async fn refresh_after(&mut self, interval: Duration) -> bool {
        let cancel = self.session.cancel_token();
        zakura_rt::select! {
            biased;
            _ = cancel.cancelled() => false,
            _ = zakura_rt::time::sleep(interval) => self.exchange().await.is_ok(),
        }
    }

    async fn wait_for_answers(&self, deadline: Duration) {
        let cancel = self.session.cancel_token();
        zakura_rt::select! {
            biased;
            _ = cancel.cancelled() => {}
            () = self.answers.wait() => {}
            () = zakura_rt::time::sleep(deadline) => {}
        }
    }

    /// Ends the session's serving and queries and releases its admission,
    /// then closes the connection if `close` names a reason.
    async fn end(self, close: Option<CloseReason>) {
        let close = match close {
            Some(reason) if self.owners().await == Ownership::Sole => Some(reason),
            _ => None,
        };
        self.session.cancel_token().cancel();
        self.queries.retire();
        self.handle.remove_session(&self.slot.key).await;
        if let Some(reason) = close {
            self.close_cause.record(match reason {
                CloseReason::ExchangeTimedOut => "discovery_exchange_timed_out",
                CloseReason::ExchangeComplete => "discovery_exchange_complete",
            });
            self.connection_cancel.cancel();
        }
    }

    /// Gossips the current self-record and asks the peer for records and services.
    ///
    /// Returns `Err(())` once the stream's send side is gone, so the caller
    /// stops the periodic loop.
    pub(super) async fn exchange(&mut self) -> Result<(), ()> {
        if !self.handle.is_current_session(&self.slot.key).await {
            return Err(());
        }

        let record = self
            .handle
            .current_self_record_for_gossip()
            .await
            .map_err(|error| {
                tracing::debug!(
                    ?error,
                    peer = ?self.session.peer_id(),
                    "failed to refresh Zakura discovery self-record"
                );
            })?;
        self.send_hello((*record).clone())?;

        let limit = u16::try_from(
            self.handle
                .peer_sample_limit()
                .await
                .clamp(1, MAX_DISCOVERY_RECORDS_PER_RESPONSE),
        )
        .expect("the largest wire limit fits u16");
        let exclude_node_ids = self.handle.peer_sample_exclusions().await;
        let get_peers = GetPeers {
            limit,
            wanted_services: Vec::new(),
            exclude_node_ids,
        };
        continues(self.queries.send_get_peers(get_peers).await)?;
        let get_services = GetServices {
            wanted_services: Vec::new(),
        };
        continues(self.queries.send_get_services(get_services).await)
    }

    /// Sends `record` as `Hello` if the row's interval has passed and the
    /// previous `Hello` was written. Otherwise the next exchange offers a
    /// fresh record. Returns `Err(())` once the stream is gone.
    fn send_hello(&mut self, record: ZakuraNodeRecord) -> Result<(), ()> {
        let sent = self
            .hello
            .update(DiscoveryMessage::Hello { record })
            .and_then(|()| self.hello.send_due(&self.session.send));
        match sent {
            Ok(_) => Ok(()),
            Err(CadenceSendError::Closed) => Err(()),
            Err(error) => {
                tracing::debug!(
                    ?error,
                    peer = ?self.session.peer_id(),
                    "failed to queue Zakura discovery hello"
                );
                Ok(())
            }
        }
    }
}

/// Whether the session continues after a query: a query that is not due
/// waits for the next exchange.
fn continues(sent: Result<(), QueryUnsent>) -> Result<(), ()> {
    match sent {
        Ok(()) | Err(QueryUnsent::NotDue) => Ok(()),
        Err(QueryUnsent::Closed) => Err(()),
    }
}

/// Whether another admitted service owns `peer_id`'s connection `conn_id`.
pub(super) fn peer_has_other_service_owner(
    connection_owners: &[Arc<dyn Service>],
    peer_id: &ZakuraPeerId,
    conn_id: ZakuraConnId,
) -> bool {
    connection_owners
        .iter()
        .any(|owner| owner.owns_connection_for_peer(peer_id, conn_id))
}
