//! Each peer's current session, shared between a service and its reactor.
//!
//! The service publishes a session when the transport admits it and removes it
//! when it ends. The reactor reads a snapshot and reconciles its own state.
//! Changes coalesce into one watch notification, so reconnect churn cannot
//! build a queue.
//!
//! The table owns each session's [`WriterFence`]. Replacing or removing a
//! session retires its fence under the table's lock, before the entry changes.
//! That puts #978's lock order (table, then fence) and its rule "retire before
//! erasing the record" in one place. If the old session has a started exchange
//! without an ending, retiring its fence closes the connection; a replacement
//! on that same connection is then refused.
//!
//! Generalized from #945's `CurrentSessions`.
//!
//! # Properties and their tests
//!
//! | Property | Test |
//! | --- | --- |
//! | One session per peer; a stale key never removes a newer session | `a_stale_key_cannot_remove_a_newer_session`, `operation_sequences_keep_one_fenced_session_per_peer` |
//! | Changes coalesce, and a change during reconciliation stays visible | `churn_coalesces_and_changes_during_reconciliation_stay_visible` |
//! | Replacing or removing retires the old fence first | `replacement_fences_the_old_publication_and_queued_first_write`, `removing_a_session_fences_its_writers_before_erasing_it` |
//! | A same-connection replacement is refused exactly when the old fence closed the connection | `replacement_closes_a_started_exchange_even_after_its_write_finished`, `an_ended_exchange_or_a_new_connection_allows_replacement`, the proptest |

use std::{
    collections::HashMap,
    sync::{Mutex, PoisonError},
};

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::WriterFence;
use crate::zakura::{ZakuraConnId, ZakuraPeerId};

#[cfg(test)]
mod tests;

/// A session's identity: its connection and its local session id.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct SessionKey {
    pub(crate) conn_id: ZakuraConnId,
    pub(crate) session_id: u64,
}

/// A peer's current session.
#[derive(Debug)]
pub(crate) struct Current<S> {
    pub(crate) key: SessionKey,
    /// The session's cancellation token. Cancelling it retires every member.
    pub(crate) cancel: CancellationToken,
    pub(crate) fence: WriterFence,
    pub(crate) session: S,
}

/// The outcome of [`SessionTable::replace`].
#[derive(Debug)]
pub(crate) enum Replaced<S> {
    /// The peer had no session.
    Inserted,
    /// The old session was fenced and cancelled.
    Replaced(Current<S>),
    /// The old session had a started exchange without an ending on the same
    /// connection. Its fence closed the connection, and the new session was
    /// cancelled.
    Refused,
}

/// Each peer's current session, with coalesced change notifications.
#[derive(Debug)]
pub(crate) struct SessionTable<S> {
    current: Mutex<HashMap<ZakuraPeerId, Current<S>>>,
    changed: watch::Sender<()>,
}

impl<S> Default for SessionTable<S> {
    fn default() -> Self {
        Self {
            current: Mutex::new(HashMap::new()),
            changed: watch::channel(()).0,
        }
    }
}

impl<S: Clone> SessionTable<S> {
    /// Watch for changes. A notification means "read a snapshot".
    pub(crate) fn subscribe(&self) -> watch::Receiver<()> {
        self.changed.subscribe()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<ZakuraPeerId, Current<S>>> {
        self.current.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Make `current` the peer's session.
    ///
    /// Under the table lock, retire the old session's fence, then cancel and
    /// replace it.
    pub(crate) fn replace(&self, peer: ZakuraPeerId, current: Current<S>) -> Replaced<S> {
        let mut table = self.lock();
        let replaced = match table.remove(&peer) {
            None => Replaced::Inserted,
            Some(old) => {
                let reusable = old.fence.retire();
                if !reusable && old.key.conn_id == current.key.conn_id {
                    current.cancel.cancel();
                    // The connection is closing. The old session's own
                    // teardown removes it.
                    table.insert(peer, old);
                    return Replaced::Refused;
                }
                old.cancel.cancel();
                Replaced::Replaced(old)
            }
        };
        table.insert(peer, current);
        self.changed.send_replace(());
        replaced
    }

    /// Remove the peer's session if `key` still names it. Retire its fence
    /// and cancel it first.
    ///
    /// A stale key, from a session that was already replaced, removes nothing.
    pub(crate) fn remove(&self, peer: &ZakuraPeerId, key: SessionKey) -> Option<Current<S>> {
        let mut table = self.lock();
        if table.get(peer).is_none_or(|current| current.key != key) {
            return None;
        }
        let removed = table
            .remove(peer)
            .expect("the entry was just found under the same lock");
        removed.fence.retire();
        removed.cancel.cancel();
        self.changed.send_replace(());
        Some(removed)
    }

    /// The peer's current session.
    pub(crate) fn get(&self, peer: &ZakuraPeerId) -> Option<(SessionKey, S)> {
        self.lock()
            .get(peer)
            .map(|current| (current.key, current.session.clone()))
    }

    /// Every current session, cloned out of the lock.
    ///
    /// Marks `seen` as seen first, so a change during reconciliation wakes
    /// the reactor again.
    pub(crate) fn snapshot(
        &self,
        seen: &mut watch::Receiver<()>,
    ) -> Vec<(ZakuraPeerId, SessionKey, S)> {
        seen.borrow_and_update();
        self.lock()
            .iter()
            .map(|(peer, current)| (peer.clone(), current.key, current.session.clone()))
            .collect()
    }
}
