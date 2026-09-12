//! Fence requester publication and first writes before replacing a receiver.

use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc, Mutex, MutexGuard,
};

use tokio_util::sync::CancellationToken;

use super::response_memory::{
    shared_allocation_bytes, ConnectionResponseMemory, ResponseMemoryPermit,
};
use crate::zakura::CloseCause;

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ResponseAdmissionError {
    Retired,
    MemoryFull,
}

/// One receiver incarnation. Retiring it prevents further publication or starts.
/// Started exchanges still require their message-specific ending or connection close.
#[derive(Clone, Debug)]
pub(crate) struct ResponseScope(Arc<Scope>);

#[derive(Debug)]
struct Scope {
    state: Mutex<ScopeState>,
    connection_cancel: CancellationToken,
    close_cause: CloseCause,
    memory: ConnectionResponseMemory,
    _setup: ResponseMemoryPermit,
}

// Includes the shared scope and platform mutex storage initialized before use.
const SCOPE_SETUP_BYTES: u64 = 512;

#[derive(Debug, Default)]
struct ScopeState {
    retired: bool,
    started: usize,
}

impl Scope {
    fn lock(&self) -> MutexGuard<'_, ScopeState> {
        self.state
            .lock()
            .expect("response scope mutex is never poisoned")
    }
}

const PREPARED: u8 = 0;
const QUEUED: u8 = 1;
const STARTED: u8 = 2;
const FINISHED: u8 = 3;
const ABANDONED: u8 = 4;

#[derive(Debug)]
struct Authorization {
    scope: ResponseScope,
    // The scope lock serializes every access. Inline atomic storage avoids a
    // second mutex that can allocate when first locked on some platforms.
    phase: AtomicU8,
    _memory: ResponseMemoryPermit,
}

/// Unique owner of an exchange through its validated ending. Dropping a started
/// exchange closes its connection locally because the response cannot be drained.
#[derive(Debug)]
pub(crate) struct ResponseAuthorization(Arc<Authorization>);

/// Writer access to an authorization. This does not own or complete the response.
#[derive(Clone, Debug)]
pub(crate) struct ResponseWritePermission(Arc<Authorization>);

impl ResponseScope {
    pub(crate) fn try_with_memory(
        connection_cancel: &CancellationToken,
        close_cause: &CloseCause,
        memory: ConnectionResponseMemory,
    ) -> Result<Self, ResponseAdmissionError> {
        let setup = memory
            .try_reserve(SCOPE_SETUP_BYTES)
            .ok_or(ResponseAdmissionError::MemoryFull)?;
        let retired = connection_cancel.is_cancelled();
        let scope = Scope {
            state: Mutex::new(ScopeState {
                retired,
                started: 0,
            }),
            connection_cancel: connection_cancel.clone(),
            close_cause: close_cause.clone(),
            memory,
            _setup: setup,
        };
        // Some targets allocate a mutex on its first lock. Initialize it while
        // setup is funded, before another thread can race that initialization.
        drop(scope.lock());
        Ok(Self(Arc::new(scope)))
    }

    /// Admit the exchange and retained container growth atomically. The caller
    /// transfers the separate permit to that storage before publishing work.
    pub(crate) fn authorize_with_retained_memory(
        &self,
        metadata_bytes: u64,
        retained_bytes: u64,
    ) -> Result<(ResponseAuthorization, Option<ResponseMemoryPermit>), ResponseAdmissionError> {
        let state = self.0.lock();
        if state.retired || self.0.connection_cancel.is_cancelled() {
            return Err(ResponseAdmissionError::Retired);
        }
        let bytes = shared_allocation_bytes::<Authorization>()
            .checked_add(metadata_bytes)
            .and_then(|bytes| bytes.checked_add(retained_bytes))
            .ok_or(ResponseAdmissionError::MemoryFull)?;
        let mut memory = self
            .0
            .memory
            .try_reserve(bytes)
            .ok_or(ResponseAdmissionError::MemoryFull)?;
        let retained = (retained_bytes > 0).then(|| memory.split_off(retained_bytes));
        Ok((
            ResponseAuthorization(Arc::new(Authorization {
                scope: self.clone(),
                phase: AtomicU8::new(PREPARED),
                _memory: memory,
            })),
            retained,
        ))
    }

    pub(crate) fn memory(&self) -> ConnectionResponseMemory {
        self.0.memory.clone()
    }

    /// Fence old publishers and writers before removing or replacing their session.
    /// Returns whether this connection can host a replacement receiver.
    pub(crate) fn retire(&self) -> bool {
        let mut state = self
            .0
            .state
            .lock()
            .expect("response scope mutex is never poisoned");
        state.retired = true;
        if state.started != 0 {
            self.close_unfinished();
        }
        !self.0.connection_cancel.is_cancelled()
    }

    fn close_unfinished(&self) {
        self.0
            .close_cause
            .record("unfinished_response_authorization");
        self.0.connection_cancel.cancel();
    }
}

impl ResponseAuthorization {
    pub(crate) fn write_permission(&self) -> ResponseWritePermission {
        ResponseWritePermission(self.0.clone())
    }

    /// Call only after validating the message's terminal identity and counts.
    pub(crate) fn finish(&mut self) {
        let mut state = self
            .0
            .scope
            .0
            .state
            .lock()
            .expect("response scope mutex is never poisoned");
        if self.0.phase.load(Ordering::Relaxed) == STARTED {
            state.started -= 1;
        }
        self.0.phase.store(FINISHED, Ordering::Relaxed);
    }
}

impl ResponseWritePermission {
    /// Publish expectations and queue the write atomically against retirement.
    /// The callback must not reenter this scope or drop an authorization in it.
    pub(crate) fn publish(&self, publish: impl FnOnce()) -> bool {
        let state = self
            .0
            .scope
            .0
            .state
            .lock()
            .expect("response scope mutex is never poisoned");
        if state.retired
            || self.0.scope.0.connection_cancel.is_cancelled()
            || self.0.phase.load(Ordering::Relaxed) != PREPARED
        {
            return false;
        }
        self.0.phase.store(QUEUED, Ordering::Relaxed);
        publish();
        true
    }

    /// Claim immediately before the first byte, under the caller's work lock.
    /// A failed work claim spends no authorization and creates no started exchange.
    pub(crate) fn try_start(&self, claim: impl FnOnce() -> bool) -> bool {
        let mut state = self
            .0
            .scope
            .0
            .state
            .lock()
            .expect("response scope mutex is never poisoned");
        if state.retired
            || self.0.scope.0.connection_cancel.is_cancelled()
            || self.0.phase.load(Ordering::Relaxed) != QUEUED
            || !claim()
        {
            return false;
        }
        state.started = state
            .started
            .checked_add(1)
            .expect("each started exchange owns a distinct allocation");
        self.0.phase.store(STARTED, Ordering::Relaxed);
        true
    }
}

impl Drop for ResponseAuthorization {
    fn drop(&mut self) {
        let mut state = self
            .0
            .scope
            .0
            .state
            .lock()
            .expect("response scope mutex is never poisoned");
        if self.0.phase.load(Ordering::Relaxed) == STARTED {
            // Close before releasing the last record of unfinished authorization.
            self.0.scope.close_unfinished();
            state.started -= 1;
        }
        self.0.phase.store(ABANDONED, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests;
