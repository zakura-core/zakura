//! Fence a session's request writers before its receiver goes away.
//!
//! A session can outlive its receiver: the transport keeps a session alive
//! while any application handle or queued write remains. A reactor can
//! therefore retire or replace a session while the old session's requests are
//! still queued or being written. A queued request can be skipped safely if
//! its first byte has not been written. Once the first byte is written, the
//! peer may answer, and no receiver owns that answer. The session's streams
//! must then reset, so the answer is never charged to the peer as unsolicited
//! or matched to a newer session.
//!
//! Each session has its own QUIC streams and its own cancellation token, a
//! child of the connection's. Cancelling the session token resets every member
//! stream with a neutral code, and the peer reads the reset as a clean end of
//! that session. A newer session opens fresh streams, so a late answer on a
//! reset stream can never reach it. The connection and its other services
//! keep running.
//!
//! [`WriterFence`] is one session's fence. Each request opens an [`Exchange`]
//! with one owner. The owner hands cloneable [`ExchangeWriter`]s to the code
//! that publishes and writes the request. One lock orders retirement,
//! publication, and the writer's first-byte claim, so exactly one of two
//! things happens:
//!
//! - The write claims its first byte first. The exchange has started, so
//!   retirement cancels the session.
//! - Retirement comes first. The claim fails, and the transport skips the
//!   frame without writing a byte.
//!
//! The reset assigns the peer no fault: the peer did nothing wrong.
//!
//! [`Reservations::reserve_fenced`](super::Reservations::reserve_fenced) makes
//! each reservation own its exchange. The ending's claim ends the exchange, so
//! a reactor that keeps its reservations in the session needs no other code.
//!
//! Ported from #978's `ResponseScope`, whose logic is unchanged.
//!
//! # Properties and their tests
//!
//! | Property | Test |
//! | --- | --- |
//! | After retirement, no exchange opens, publishes, or starts | `retirement_fences_prepared_and_queued_writes_without_resetting` |
//! | Retirement and a first byte have one winner | `retirement_and_first_write_have_one_winner` |
//! | Retirement waits for a publication in progress | `publication_is_complete_before_retirement_returns` |
//! | A failed work claim leaves the session running | `a_failed_work_claim_leaves_the_session_running` |
//! | Dropping a started, unended exchange resets the session | `owner_drop_fences_unstarted_writes_and_resets_started_exchanges` |
//! | Only an ending releases a started exchange | `only_endings_release_started_exchanges` |
//! | Every writer of an old or reset fence stays fenced, and no reset reaches the connection | `exchange_histories_keep_old_writers_fenced` |
//! | A following frame is written only after its request started | `a_following_frame_needs_its_request_started` |
//! | Retirement during a following frame's write resets the session | `retirement_during_a_following_write_resets_the_session` |

use std::sync::{Arc, OnceLock};

use zakura_rt::sync::{CancellationToken, Mutex, MutexGuard, PoisonError};
use zakura_rt::time::Instant;

use crate::zakura::{
    transport::{FrameWriteClaim, GuardedFrameSlot},
    Frame, FramedSend,
};

#[cfg(test)]
mod tests;

/// One session's fence over its request writers.
#[derive(Clone, Debug)]
pub(crate) struct WriterFence(Arc<Fence>);

#[derive(Debug)]
struct Fence {
    state: Mutex<FenceState>,
    /// The session's token. Cancelling it resets the session's streams.
    session: CancellationToken,
}

#[derive(Debug, Default)]
struct FenceState {
    retired: bool,
    started: usize,
    /// Following frames claimed and not yet written.
    following: usize,
}

#[derive(Debug, PartialEq)]
enum Phase {
    Opened,
    Queued,
    Started,
    Ended,
    /// Ended before the request's first byte was written.
    Abandoned,
    Dropped,
}

#[derive(Debug)]
struct ExchangeState {
    fence: WriterFence,
    // Always lock the fence before the phase, including in `Drop`.
    phase: Mutex<Phase>,
}

/// One request's exchange, with one owner.
///
/// Dropping it after its first byte is written, without an ending, resets the
/// session: no receiver remains for the peer's answer.
#[derive(Debug)]
pub(crate) struct Exchange(Arc<ExchangeState>);

/// Publishes and starts an exchange's request. A writer cannot end the
/// exchange.
#[derive(Clone, Debug)]
pub(crate) struct ExchangeWriter(Arc<ExchangeState>);

impl WriterFence {
    /// A fence over the session that `session` cancels.
    pub(crate) fn new(session: CancellationToken) -> Self {
        Self(Arc::new(Fence {
            state: Mutex::new(FenceState::default()),
            session,
        }))
    }

    fn lock(&self) -> MutexGuard<'_, FenceState> {
        self.0.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Open an exchange before taking local work for it. `None` once the
    /// fence retired or the session ended.
    pub(crate) fn open(&self) -> Option<Exchange> {
        let state = self.lock();
        if state.retired || self.0.session.is_cancelled() {
            return None;
        }
        Some(Exchange(Arc::new(ExchangeState {
            fence: self.clone(),
            phase: Mutex::new(Phase::Opened),
        })))
    }

    /// Fence every unstarted write. Reset the session if an exchange started
    /// and has no ending, or a following frame is being written.
    ///
    /// Returns whether the session is still running.
    pub(crate) fn retire(&self) -> bool {
        let mut state = self.lock();
        state.retired = true;
        if state.started != 0 || state.following != 0 {
            self.reset_unfinished();
        }
        !self.0.session.is_cancelled()
    }

    /// End the session with a neutral stream reset. The connection stays.
    fn reset_unfinished(&self) {
        if !self.0.session.is_cancelled() {
            tracing::debug!("resetting a Zakura session with an unfinished exchange");
        }
        self.0.session.cancel();
    }
}

impl ExchangeState {
    /// Lock the fence, then the phase.
    fn lock(&self) -> (MutexGuard<'_, FenceState>, MutexGuard<'_, Phase>) {
        let state = self.fence.lock();
        let phase = self.phase.lock().unwrap_or_else(PoisonError::into_inner);
        (state, phase)
    }
}

impl Exchange {
    /// A writer for this exchange's request.
    pub(crate) fn writer(&self) -> ExchangeWriter {
        ExchangeWriter(self.0.clone())
    }

    /// End the exchange. Call it only after the ending passed validation.
    /// Later calls change nothing.
    ///
    /// An ending that arrives before the request's first byte abandons the
    /// exchange: the request and every frame queued to follow it are skipped.
    pub(crate) fn end(&mut self) {
        let (mut state, mut phase) = self.0.lock();
        match *phase {
            Phase::Started => {
                state.started -= 1;
                *phase = Phase::Ended;
            }
            Phase::Opened | Phase::Queued => *phase = Phase::Abandoned,
            Phase::Ended | Phase::Abandoned | Phase::Dropped => {}
        }
    }
}

impl ExchangeWriter {
    /// Publish the request, atomically with respect to retirement.
    ///
    /// `publish` runs under the fence's lock. It must not reenter this fence
    /// or drop an exchange of it.
    pub(crate) fn publish(&self, publish: impl FnOnce()) -> bool {
        let (state, mut phase) = self.0.lock();
        if state.retired || self.0.fence.0.session.is_cancelled() || *phase != Phase::Opened {
            return false;
        }
        *phase = Phase::Queued;
        publish();
        true
    }

    /// Claim the request's first byte. `claim` runs under the fence's lock and
    /// may take the caller's own work claim.
    ///
    /// A failed `claim` starts nothing and leaves the session running.
    pub(crate) fn try_start(&self, claim: impl FnOnce() -> bool) -> bool {
        let (mut state, mut phase) = self.0.lock();
        if state.retired
            || self.0.fence.0.session.is_cancelled()
            || *phase != Phase::Queued
            || !claim()
        {
            return false;
        }
        state.started = state
            .started
            .checked_add(1)
            .expect("each started exchange owns a distinct allocation");
        *phase = Phase::Started;
        true
    }

    /// Claim a later frame's write, if the request's first byte was written,
    /// the exchange is still owned, and the fence has neither retired nor
    /// seen its session end. The claim holds the fence until
    /// [`Self::follow_written`], so retirement during the write resets the
    /// session rather than let the frame reach a replacement session.
    fn try_follow(&self) -> bool {
        let (mut state, phase) = self.0.lock();
        if state.retired
            || self.0.fence.0.session.is_cancelled()
            || !matches!(*phase, Phase::Started | Phase::Ended)
        {
            return false;
        }
        state.following = state
            .following
            .checked_add(1)
            .expect("each following write holds a distinct queued frame");
        true
    }

    /// Release the claim of a following frame whose write finished.
    fn follow_written(&self) {
        let mut state = self.0.fence.lock();
        debug_assert!(state.following > 0, "a written frame was claimed");
        state.following = state.following.saturating_sub(1);
    }
}

impl Drop for Exchange {
    fn drop(&mut self) {
        let (mut state, mut phase) = self.0.lock();
        if *phase == Phase::Started {
            // Reset before the last record of the started exchange goes.
            self.0.fence.reset_unfinished();
            state.started -= 1;
        }
        *phase = Phase::Dropped;
    }
}

/// A transport write claim for a fenced request.
///
/// For a reactor without its own work ledger. A reactor with one wraps its
/// own claim in [`ExchangeWriter::try_start`] instead.
#[derive(Debug)]
struct FencedWrite(ExchangeWriter);

impl FrameWriteClaim for FencedWrite {
    fn try_start(&self) -> bool {
        self.0.try_start(|| true)
    }

    fn written(&self) {}
}

/// A transport write claim for a later frame of an exchange. See
/// [`ExchangeWriter::try_follow`].
#[derive(Debug)]
struct FollowingWrite {
    writer: ExchangeWriter,
    written_at: Option<Arc<OnceLock<Instant>>>,
}

impl FrameWriteClaim for FollowingWrite {
    fn try_start(&self) -> bool {
        self.writer.try_follow()
    }

    fn written(&self) {
        self.writer.follow_written();
        if let Some(written_at) = &self.written_at {
            let _ = written_at.set(Instant::now());
        }
    }
}

/// Why a fenced request was not queued.
#[derive(Copy, Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum FencedSendError {
    /// The fence retired, the session ended, or the exchange was already
    /// published.
    #[error("the exchange is fenced")]
    Fenced,
    /// The stream's writer has gone.
    #[error("the stream is closed")]
    Closed,
}

impl FramedSend {
    /// Wait for a queue slot, then publish `frame` under `writer`'s fence with
    /// a claim that starts the exchange at the first byte.
    ///
    /// If the fence retires before the first byte, the transport skips the
    /// frame.
    pub(crate) async fn send_fenced(
        &self,
        frame: Frame,
        writer: &ExchangeWriter,
    ) -> Result<(), FencedSendError> {
        // Waiting never reports `Full`, and every framed channel supports
        // guarded slots, so any error means the writer has gone.
        let slot = self
            .reserve_guarded()
            .await
            .map_err(|_| FencedSendError::Closed)?;
        slot.send_fenced(frame, writer)
    }

    /// Wait for a queue slot, then queue `frame` to follow the request of
    /// `writer`'s exchange, such as a subscription update after its `Open`.
    ///
    /// The transport writes `frame` only if the request's first byte was
    /// written first and the fence has not retired since, so retirement skips
    /// `frame` whether or not it skipped the request.
    pub(crate) async fn send_following(
        &self,
        frame: Frame,
        writer: &ExchangeWriter,
    ) -> Result<(), FencedSendError> {
        let slot = self
            .reserve_guarded()
            .await
            .map_err(|_| FencedSendError::Closed)?;
        slot.send_following(frame, writer)
    }
}

impl GuardedFrameSlot<'_> {
    /// Publish `frame` into this reserved slot under `writer`'s fence, as
    /// [`FramedSend::send_fenced`] does without waiting.
    pub(crate) fn send_fenced(
        self,
        frame: Frame,
        writer: &ExchangeWriter,
    ) -> Result<(), FencedSendError> {
        let slot = self;
        let mut queued = false;
        if !writer.publish(|| {
            queued = slot.send_request(frame, Arc::new(FencedWrite(writer.clone())));
        }) {
            return Err(FencedSendError::Fenced);
        }
        if queued {
            Ok(())
        } else {
            Err(FencedSendError::Closed)
        }
    }

    /// Queue `frame` into this reserved slot to follow the request of
    /// `writer`'s exchange, as [`FramedSend::send_following`] does without
    /// waiting.
    pub(crate) fn send_following(
        self,
        frame: Frame,
        writer: &ExchangeWriter,
    ) -> Result<(), FencedSendError> {
        self.send_following_with_receipt(frame, writer, None)
    }

    /// Queue a following frame and optionally record when its full write completes.
    pub(crate) fn send_following_with_receipt(
        self,
        frame: Frame,
        writer: &ExchangeWriter,
        written_at: Option<Arc<OnceLock<Instant>>>,
    ) -> Result<(), FencedSendError> {
        let claim = FollowingWrite {
            writer: writer.clone(),
            written_at,
        };
        if self.send_request(frame, Arc::new(claim)) {
            Ok(())
        } else {
            Err(FencedSendError::Closed)
        }
    }
}
