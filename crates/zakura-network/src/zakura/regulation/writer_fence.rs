//! Fence a session's request writers before its receiver goes away.
//!
//! A session can outlive its receiver: the transport keeps a session alive
//! while any application handle or queued write remains. A reactor can
//! therefore retire or replace a session while the old session's requests are
//! still queued or being written. A queued request can be skipped safely if
//! its first byte has not been written. Once the first byte is written, the
//! peer may answer, and no receiver owns that answer. The connection must then
//! close, so the answer is never charged to the peer as unsolicited or matched
//! to a newer session.
//!
//! [`WriterFence`] is one session's fence. Each request opens an [`Exchange`]
//! with one owner. The owner hands cloneable [`ExchangeWriter`]s to the code
//! that publishes and writes the request. One lock orders retirement,
//! publication, and the writer's first-byte claim, so exactly one of two
//! things happens:
//!
//! - The write claims its first byte first. The exchange has started, so
//!   retirement closes the connection.
//! - Retirement comes first. The claim fails, and the transport skips the
//!   frame without writing a byte.
//!
//! The close records the local cause `unfinished_exchange`. It assigns the
//! peer no fault: the peer did nothing wrong.
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
//! | After retirement, no exchange opens, publishes, or starts | `retirement_fences_prepared_and_queued_writes_without_closing` |
//! | Retirement and a first byte have one winner | `retirement_and_first_write_have_one_winner` |
//! | Retirement waits for a publication in progress | `publication_is_complete_before_retirement_returns` |
//! | A failed work claim leaves the connection reusable | `a_failed_work_claim_leaves_the_connection_reusable` |
//! | Dropping a started, unended exchange closes the connection | `owner_drop_fences_unstarted_writes_and_closes_started_exchanges` |
//! | Only an ending releases a started exchange | `only_endings_release_started_exchanges` |
//! | Every writer of an old or closed fence stays fenced | `exchange_histories_keep_old_writers_fenced` |

use std::sync::{Arc, Mutex, PoisonError};

use tokio_util::sync::CancellationToken;

use super::{shared_allocation_bytes, ConnectionResponseMemory, ResponseMemoryPermit};
use crate::zakura::{transport::FrameWriteClaim, CloseCause, Frame, FramedSend};

#[cfg(test)]
mod tests;

/// The close cause a fence records.
pub(crate) const UNFINISHED_EXCHANGE: &str = "unfinished_exchange";

/// One session's fence over its request writers.
#[derive(Clone, Debug)]
pub(crate) struct WriterFence(Arc<Fence>);

#[derive(Debug)]
struct Fence {
    state: Mutex<FenceState>,
    connection: CancellationToken,
    close_cause: CloseCause,
    memory: Option<ConnectionResponseMemory>,
    _setup: Option<ResponseMemoryPermit>,
}

// Includes the shared fence and first-use platform lock storage.
// Cold allocation tests check this allowance.
const FENCE_SETUP_BYTES: u64 = 512;
// The phase mutex can allocate storage on first use independently of its Arc.
// Cold allocation tests include this storage rather than treating the Arc's
// inline layout as the whole allocation.
const EXCHANGE_LOCK_BYTES: u64 = 128;

/// A local reason an exchange could not be opened. Never a peer violation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum ExchangeOpenError {
    /// The session retired or its connection closed.
    #[error("the exchange fence retired")]
    Retired,
    /// The connection or node has insufficient metadata capacity.
    #[error("response metadata capacity is full")]
    MemoryFull,
    /// Additional metadata needs a fence constructed with a shared memory pool.
    #[error("the exchange fence has no metadata pool")]
    Unfunded,
}

#[derive(Debug, Default)]
struct FenceState {
    retired: bool,
    started: usize,
}

#[derive(Debug, PartialEq)]
enum Phase {
    Opened,
    Queued,
    Started,
    Ended,
    Dropped,
}

#[derive(Debug)]
struct ExchangeState {
    fence: WriterFence,
    // Always lock the fence before the phase, including in `Drop`.
    phase: Mutex<Phase>,
    _memory: Option<ResponseMemoryPermit>,
}

/// One request's exchange, with one owner.
///
/// Dropping it after its first byte is written, without an ending, closes the
/// connection: no receiver remains for the peer's answer.
#[derive(Debug)]
pub(crate) struct Exchange(Arc<ExchangeState>);

/// Publishes and starts an exchange's request. A writer cannot end the
/// exchange.
#[derive(Clone, Debug)]
pub(crate) struct ExchangeWriter(Arc<ExchangeState>);

impl WriterFence {
    /// A fence that closes `connection`, recording its cause in `close_cause`.
    pub(crate) fn new(connection: CancellationToken, close_cause: CloseCause) -> Self {
        Self(Arc::new(Fence {
            state: Mutex::new(FenceState::default()),
            connection,
            close_cause,
            memory: None,
            _setup: None,
        }))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, FenceState> {
        self.0.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Fund the fence before allocating it. Services and replacement sessions
    /// on a connection must pass clones of the same memory handle.
    pub(crate) fn try_with_memory(
        connection: &CancellationToken,
        close_cause: &CloseCause,
        memory: ConnectionResponseMemory,
    ) -> Result<Self, ExchangeOpenError> {
        let setup = memory
            .try_reserve(FENCE_SETUP_BYTES)
            .ok_or(ExchangeOpenError::MemoryFull)?;
        let fence = Fence {
            state: Mutex::new(FenceState::default()),
            connection: connection.clone(),
            close_cause: close_cause.clone(),
            memory: Some(memory),
            _setup: Some(setup),
        };
        drop(fence.state.lock().unwrap_or_else(PoisonError::into_inner));
        Ok(Self(Arc::new(fence)))
    }

    /// Open before taking local work. Returns `None` after retirement or when
    /// a funded fence has no room. Use `try_open` to distinguish these cases.
    pub(crate) fn open(&self) -> Option<Exchange> {
        self.try_open().ok()
    }

    /// Open an exchange, charging its shared allocation when the fence is funded.
    pub(crate) fn try_open(&self) -> Result<Exchange, ExchangeOpenError> {
        self.try_open_with_retained_memory(0, 0)
            .map(|(exchange, _)| exchange)
    }

    /// Reserve the exchange, caller metadata and retained container growth as
    /// one allocation plan before publishing work. The separate permit must
    /// follow the container. Exchange metadata follows every writer clone.
    pub(crate) fn try_open_with_retained_memory(
        &self,
        metadata_bytes: u64,
        retained_bytes: u64,
    ) -> Result<(Exchange, Option<ResponseMemoryPermit>), ExchangeOpenError> {
        let state = self.lock();
        if state.retired || self.0.connection.is_cancelled() {
            return Err(ExchangeOpenError::Retired);
        }
        let (memory, retained) = if let Some(pool) = &self.0.memory {
            let bytes = Self::admission_bytes(metadata_bytes, retained_bytes)
                .ok_or(ExchangeOpenError::MemoryFull)?;
            let mut memory = pool
                .try_reserve(bytes)
                .ok_or(ExchangeOpenError::MemoryFull)?;
            let retained = (retained_bytes > 0).then(|| memory.split_off(retained_bytes));
            (Some(memory), retained)
        } else if metadata_bytes == 0 && retained_bytes == 0 {
            (None, None)
        } else {
            return Err(ExchangeOpenError::Unfunded);
        };
        let phase = Mutex::new(Phase::Opened);
        drop(phase.lock().unwrap_or_else(PoisonError::into_inner));
        Ok((
            Exchange(Arc::new(ExchangeState {
                fence: self.clone(),
                phase,
                _memory: memory,
            })),
            retained,
        ))
    }

    /// Total charge for an exchange, its caller metadata and retained growth.
    pub(crate) fn admission_bytes(metadata_bytes: u64, retained_bytes: u64) -> Option<u64> {
        shared_allocation_bytes::<ExchangeState>()
            .checked_add(EXCHANGE_LOCK_BYTES)?
            .checked_add(metadata_bytes)?
            .checked_add(retained_bytes)
    }

    /// Fence every unstarted write. Close the connection if an exchange
    /// started and has no ending.
    ///
    /// Returns whether the connection can host a replacement session.
    pub(crate) fn retire(&self) -> bool {
        let mut state = self.lock();
        state.retired = true;
        if state.started != 0 {
            self.close_unfinished();
        }
        !self.0.connection.is_cancelled()
    }

    fn close_unfinished(&self) {
        self.0.close_cause.record(UNFINISHED_EXCHANGE);
        self.0.connection.cancel();
    }
}

impl ExchangeState {
    /// Lock the fence, then the phase.
    fn lock(
        &self,
    ) -> (
        std::sync::MutexGuard<'_, FenceState>,
        std::sync::MutexGuard<'_, Phase>,
    ) {
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
    pub(crate) fn end(&mut self) {
        let (mut state, mut phase) = self.0.lock();
        if *phase == Phase::Started {
            state.started -= 1;
        }
        *phase = Phase::Ended;
    }
}

impl ExchangeWriter {
    /// Publish the request, atomically with respect to retirement.
    ///
    /// `publish` runs under the fence's lock. It must not reenter this fence
    /// or drop an exchange of it.
    pub(crate) fn publish(&self, publish: impl FnOnce()) -> bool {
        let (state, mut phase) = self.0.lock();
        if state.retired || self.0.fence.0.connection.is_cancelled() || *phase != Phase::Opened {
            return false;
        }
        *phase = Phase::Queued;
        publish();
        true
    }

    /// Claim the request's first byte. `claim` runs under the fence's lock and
    /// may take the caller's own work claim.
    ///
    /// A failed `claim` starts nothing and leaves the connection reusable.
    pub(crate) fn try_start(&self, claim: impl FnOnce() -> bool) -> bool {
        let (mut state, mut phase) = self.0.lock();
        if state.retired
            || self.0.fence.0.connection.is_cancelled()
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
}

impl Drop for Exchange {
    fn drop(&mut self) {
        let (mut state, mut phase) = self.0.lock();
        if *phase == Phase::Started {
            // Close before the last record of the started exchange goes.
            self.0.fence.close_unfinished();
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

/// Why a fenced request was not queued.
#[derive(Copy, Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum FencedSendError {
    /// The fence retired, the connection closed, or the exchange was already
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
}
