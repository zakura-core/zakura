//! Fence requester publication and first writes before replacing a receiver.

use std::sync::{Arc, Mutex};

use tokio_util::sync::CancellationToken;

use crate::zakura::CloseCause;

/// One receiver incarnation. Retiring it prevents further publication or starts.
/// Started exchanges still require their message-specific ending or connection close.
#[derive(Clone, Debug)]
pub(crate) struct ResponseScope(Arc<Scope>);

#[derive(Debug)]
struct Scope {
    state: Mutex<ScopeState>,
    connection_cancel: CancellationToken,
    close_cause: CloseCause,
}

#[derive(Debug, Default)]
struct ScopeState {
    retired: bool,
    started: usize,
}

#[derive(Debug, PartialEq)]
enum Phase {
    Prepared,
    Queued,
    Started,
    Finished,
    Abandoned,
}

#[derive(Debug)]
struct Authorization {
    scope: ResponseScope,
    // Always lock the scope before this phase, including terminal and Drop paths.
    phase: Mutex<Phase>,
}

/// Unique owner of an exchange through its validated ending. Dropping a started
/// exchange closes its connection locally because the response cannot be drained.
#[derive(Debug)]
pub(crate) struct ResponseAuthorization(Arc<Authorization>);

/// Writer access to an authorization. This does not own or complete the response.
#[derive(Clone, Debug)]
pub(crate) struct ResponseWritePermission(Arc<Authorization>);

impl ResponseScope {
    pub(crate) fn new(connection_cancel: CancellationToken, close_cause: CloseCause) -> Self {
        Self(Arc::new(Scope {
            state: Mutex::new(ScopeState::default()),
            connection_cancel,
            close_cause,
        }))
    }

    /// Prepare before taking local work or allocating message-specific expectations.
    pub(crate) fn authorize(&self) -> Option<ResponseAuthorization> {
        let state = self
            .0
            .state
            .lock()
            .expect("response scope mutex is never poisoned");
        if state.retired || self.0.connection_cancel.is_cancelled() {
            return None;
        }
        Some(ResponseAuthorization(Arc::new(Authorization {
            scope: self.clone(),
            phase: Mutex::new(Phase::Prepared),
        })))
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
        let mut phase = self
            .0
            .phase
            .lock()
            .expect("response phase mutex is never poisoned");
        if *phase == Phase::Started {
            state.started -= 1;
        }
        *phase = Phase::Finished;
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
        let mut phase = self
            .0
            .phase
            .lock()
            .expect("response phase mutex is never poisoned");
        if state.retired
            || self.0.scope.0.connection_cancel.is_cancelled()
            || *phase != Phase::Prepared
        {
            return false;
        }
        *phase = Phase::Queued;
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
        let mut phase = self
            .0
            .phase
            .lock()
            .expect("response phase mutex is never poisoned");
        if state.retired
            || self.0.scope.0.connection_cancel.is_cancelled()
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

impl Drop for ResponseAuthorization {
    fn drop(&mut self) {
        let mut state = self
            .0
            .scope
            .0
            .state
            .lock()
            .expect("response scope mutex is never poisoned");
        let mut phase = self
            .0
            .phase
            .lock()
            .expect("response phase mutex is never poisoned");
        if *phase == Phase::Started {
            // Close before releasing the last record of unfinished authorization.
            self.0.scope.close_unfinished();
            state.started -= 1;
        }
        *phase = Phase::Abandoned;
    }
}

#[cfg(test)]
mod tests;
