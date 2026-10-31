//! The exchange policy of one discovery session, as a pure machine.
//!
//! [`PeerState`] records the session's initial exchange progress and decides
//! each step of its writer: when to exchange, how long to wait, when to
//! refresh, and how the session ends. It reads no clock and holds no runtime
//! resource. [`PeerIO`](super::PeerIO) runs each [`PeerOutput`] and answers
//! it with exactly one [`PeerInput`].

use std::time::Duration;

use super::super::{
    protocol::DEFAULT_LIVE_SERVICE_SUMMARY_TTL,
    requester::{AdmittedAnswer, QueryKind},
    wire::SEND_INTERVAL,
};

// The periodic exchange refreshes live service summaries before they expire.
const _: () = assert!(SEND_INTERVAL.as_secs() <= DEFAULT_LIVE_SERVICE_SUMMARY_TTL.as_secs() / 2);

/// Maximum time discovery waits for first-party exchange responses before releasing the session.
pub(in crate::zakura::discovery) const DISCOVERY_INITIAL_EXCHANGE_TIMEOUT: Duration =
    Duration::from_secs(2);
/// How long closing a discovery-only connection waits for this node's admitted
/// answers and the peer's delivered answers and `Hello`. Each is small and needs
/// only free output or a verification slot, so a peer that reads its stream gets
/// them well within this.
pub(in crate::zakura::discovery) const DISCOVERY_ANSWER_WRITE_DEADLINE: Duration =
    Duration::from_secs(2);

/// The parts of the initial exchange that this session has processed.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::zakura::discovery) struct ExchangeProgress {
    /// The peer's authentic `Hello`, imported or dropped by local policy.
    pub(in crate::zakura::discovery) hello: bool,
    /// An imported `Peers` that answered this session's `GetPeers`.
    pub(in crate::zakura::discovery) peers: bool,
    /// Imported `Services` that answered this session's `GetServices`.
    pub(in crate::zakura::discovery) services: bool,
}

impl ExchangeProgress {
    /// Whether the peer answered every part of the exchange.
    fn complete(&self) -> bool {
        self.hello && self.peers && self.services
    }
}

/// How a session's initial exchange ended.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum InitialExchange {
    /// The peer answered every query.
    Complete,
    /// The answers did not all arrive within the timeout.
    TimedOut,
    /// The stream or session ended first.
    Ended,
}

/// Which event ended the initial exchange's wait.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum InitialWait {
    /// The session was cancelled.
    Cancelled,
    /// The exchange completed.
    Complete,
    /// The timeout passed.
    TimedOut,
}

/// Who holds the session's connection, as IO found it.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum Ownership {
    /// Another admitted service owns the connection.
    Shared,
    /// No other service owns the connection, and this session is no longer
    /// current.
    Superseded,
    /// This current session is the connection's only owner.
    Sole,
}

/// A change to this session's record in the service's session states.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum SessionCharge {
    /// Reset the record's failure count.
    Succeed,
    /// Count one failed session.
    Fail,
    /// Retire the record, so the connection reopens no session.
    Retire,
}

/// Why discovery closes its discovery-only connection.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum CloseReason {
    /// The initial exchange completed.
    ExchangeComplete,
    /// The initial exchange timed out.
    ExchangeTimedOut,
}

/// The operation IO runs next.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum PeerOutput {
    /// Gossip `Hello` and send this session's queries.
    Exchange,
    /// Wait until the exchange completes, the session is cancelled, or
    /// `timeout` passes.
    WaitInitial { timeout: Duration },
    /// Check whether this session is still current.
    CheckCurrentSession,
    /// Consult the connection's other admitted service owners, then whether
    /// this session is still current.
    CheckSoleOwner,
    /// Exchange again once `interval` passes, unless the session is cancelled
    /// first.
    Refresh { interval: Duration },
    /// Wait until this session writes every answer it admitted and processes
    /// every answer and `Hello` it read, the session is cancelled, or `deadline`
    /// passes.
    WaitForAnswers { deadline: Duration },
    /// Mark the peer's exchange as short-lived.
    MarkShortLived,
    /// End the session, and close its connection if `close` names a reason.
    End { close: Option<CloseReason> },
}

/// The outcome of the last [`PeerOutput`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum PeerInput {
    /// [`PeerOutput::Exchange`] ran. `sent` is false once the session is gone.
    Exchanged { sent: bool },
    /// [`PeerOutput::WaitInitial`] ended.
    InitialWaitEnded(InitialWait),
    /// [`PeerOutput::CheckCurrentSession`] found the session `current` or not.
    CurrentSession { current: bool },
    /// [`PeerOutput::CheckSoleOwner`] found this ownership.
    Owners(Ownership),
    /// [`PeerOutput::Refresh`] ended. `sent` is false once the session is
    /// cancelled or gone.
    Refreshed { sent: bool },
    /// [`PeerOutput::WaitForAnswers`] ended.
    AnswersSettled,
    /// [`PeerOutput::MarkShortLived`] ran.
    ShortLivedMarked,
}

/// One step of the session: a charge to apply, then the next operation.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct PeerStep {
    /// IO applies this charge to the session's record before `next`.
    pub(super) charge: Option<SessionCharge>,
    pub(super) next: PeerOutput,
}

impl PeerStep {
    fn new(charge: Option<SessionCharge>, next: PeerOutput) -> Self {
        Self { charge, next }
    }

    fn then(next: PeerOutput) -> Self {
        Self::new(None, next)
    }
}

/// The operation the session waits on.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Phase {
    Start,
    Exchanging,
    WaitingInitial,
    /// The wait ended with this classification, pending the session check.
    CheckingInitial(InitialExchange),
    CheckingOwners,
    Refreshing,
    AwaitingAnswers,
    CheckingClose,
    MarkingShortLived,
    Ended,
}

/// One discovery session's exchange progress and completion policy.
#[derive(Debug)]
pub(super) struct PeerState {
    progress: ExchangeProgress,
    /// How the initial exchange ended, once known.
    initial: Option<InitialExchange>,
    phase: Phase,
}

impl Default for PeerState {
    fn default() -> Self {
        Self {
            progress: ExchangeProgress::default(),
            initial: None,
            phase: Phase::Start,
        }
    }
}

impl PeerState {
    /// The session's first step: the initial exchange.
    pub(super) fn start(&mut self) -> PeerStep {
        assert_eq!(self.phase, Phase::Start, "a session starts once");
        self.phase = Phase::Exchanging;
        PeerStep::then(PeerOutput::Exchange)
    }

    /// Records the peer's authentic `Hello`. Local policy may have dropped
    /// it, for example because it expired, and it still counts.
    pub(super) fn record_hello(&mut self) {
        self.progress.hello = true;
    }

    /// Records an imported answer to this session's query.
    pub(super) fn record_answer(&mut self, answer: &AdmittedAnswer) {
        match answer.kind() {
            QueryKind::Peers => self.progress.peers = true,
            QueryKind::Services => self.progress.services = true,
        }
    }

    /// Whether the peer answered every part of the exchange.
    pub(super) fn is_complete(&self) -> bool {
        self.progress.complete()
    }

    /// The exchange progress, for tests that check what a handler recorded.
    #[cfg(test)]
    pub(super) fn progress(&self) -> ExchangeProgress {
        self.progress
    }

    /// Applies the outcome of the last output and returns the next step.
    ///
    /// IO answers each output with its own input, so any other pairing is a
    /// bug in IO.
    pub(super) fn on_input(&mut self, input: PeerInput) -> PeerStep {
        match (self.phase, input) {
            (Phase::Exchanging, PeerInput::Exchanged { sent: true }) => {
                self.phase = Phase::WaitingInitial;
                PeerStep::then(PeerOutput::WaitInitial {
                    timeout: DISCOVERY_INITIAL_EXCHANGE_TIMEOUT,
                })
            }
            (Phase::Exchanging, PeerInput::Exchanged { sent: false })
            | (Phase::WaitingInitial, PeerInput::InitialWaitEnded(InitialWait::Cancelled))
            | (Phase::CheckingInitial(_), PeerInput::CurrentSession { current: false }) => {
                self.initial_ended()
            }
            (Phase::WaitingInitial, PeerInput::InitialWaitEnded(wait)) => {
                let initial = if wait == InitialWait::Complete {
                    InitialExchange::Complete
                } else {
                    InitialExchange::TimedOut
                };
                self.phase = Phase::CheckingInitial(initial);
                PeerStep::then(PeerOutput::CheckCurrentSession)
            }
            (Phase::CheckingInitial(initial), PeerInput::CurrentSession { current: true }) => {
                self.initial = Some(initial);
                self.phase = Phase::CheckingOwners;
                // The answers can wait behind the peer's serving queue and
                // this node's verification slots, so a slow exchange is not
                // the peer's fault. It continues like a completed one, and is
                // settled when its session ends.
                let charge =
                    (initial == InitialExchange::Complete).then_some(SessionCharge::Succeed);
                PeerStep::new(charge, PeerOutput::CheckSoleOwner)
            }
            (
                Phase::CheckingOwners | Phase::CheckingClose,
                PeerInput::Owners(Ownership::Shared),
            ) => {
                self.phase = Phase::Refreshing;
                PeerStep::then(PeerOutput::Refresh {
                    interval: SEND_INTERVAL,
                })
            }
            (
                Phase::CheckingOwners | Phase::CheckingClose,
                PeerInput::Owners(Ownership::Superseded),
            )
            | (Phase::Refreshing, PeerInput::Refreshed { sent: false }) => self.release(),
            (Phase::CheckingOwners, PeerInput::Owners(Ownership::Sole)) => {
                // Closing drops every unwritten answer, and the peer's
                // exchange would end without it.
                self.phase = Phase::AwaitingAnswers;
                PeerStep::then(PeerOutput::WaitForAnswers {
                    deadline: DISCOVERY_ANSWER_WRITE_DEADLINE,
                })
            }
            (Phase::Refreshing, PeerInput::Refreshed { sent: true }) => {
                self.phase = Phase::CheckingOwners;
                PeerStep::then(PeerOutput::CheckSoleOwner)
            }
            (Phase::AwaitingAnswers, PeerInput::AnswersSettled) => {
                self.phase = Phase::CheckingClose;
                PeerStep::then(PeerOutput::CheckSoleOwner)
            }
            (Phase::CheckingClose, PeerInput::Owners(Ownership::Sole)) => {
                self.phase = Phase::MarkingShortLived;
                PeerStep::then(PeerOutput::MarkShortLived)
            }
            (Phase::MarkingShortLived, PeerInput::ShortLivedMarked) => {
                self.phase = Phase::Ended;
                // The close's answer wait can finish a timed-out exchange.
                let close = if self.progress.complete() {
                    CloseReason::ExchangeComplete
                } else {
                    CloseReason::ExchangeTimedOut
                };
                // A retired session is never charged, so a timed-out exchange
                // that closes its connection settles here.
                PeerStep::new(
                    Some(SessionCharge::Retire),
                    PeerOutput::End { close: Some(close) },
                )
            }
            (phase, input) => {
                unreachable!(
                    "discovery IO answers each output with its own input: {phase:?} got {input:?}"
                )
            }
        }
    }

    /// Ends a session whose initial exchange broke. Charging the session
    /// stops endless reopens that break the stream first.
    fn initial_ended(&mut self) -> PeerStep {
        self.initial = Some(InitialExchange::Ended);
        self.phase = Phase::Ended;
        PeerStep::new(Some(SessionCharge::Fail), PeerOutput::End { close: None })
    }

    /// Ends a session that leaves its connection open.
    ///
    /// A timed-out session that completed late resets the count, and one that
    /// never completed is charged, so a silent peer cannot reopen without
    /// bound.
    fn release(&mut self) -> PeerStep {
        self.phase = Phase::Ended;
        let charge = (self.initial == Some(InitialExchange::TimedOut)).then(|| {
            if self.progress.complete() {
                SessionCharge::Succeed
            } else {
                SessionCharge::Fail
            }
        });
        PeerStep::new(charge, PeerOutput::End { close: None })
    }
}
