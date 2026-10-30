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

use zakura_rt::{
    collections::HashMap,
    sync::{watch, CancellationToken, Mutex, MutexGuard, PoisonError},
};

use super::WriterFence;
#[cfg(any(test, zakura_sim))]
use crate::zakura::{diag, ConnTap, RemoveCause, ServicePeerDirection};
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
    /// What the service installed it from, or `None` for a table that
    /// reports no changes (M6a-7b).
    #[cfg(any(test, zakura_sim))]
    pub(crate) installed: Option<Install>,
}

/// What a service installed a session from, and the connection's tap its
/// table changes are reported through (M6a-7b).
#[cfg(any(test, zakura_sim))]
#[derive(Clone, Debug)]
pub(crate) struct Install {
    pub(crate) facts: diag::InstallFacts,
    pub(crate) tap: Option<std::sync::Arc<ConnTap>>,
}

/// A change to a table's entries, which its reporter hears under the
/// table's lock, as the change is made (M6a-7b).
#[cfg(any(test, zakura_sim))]
#[derive(Clone, Copy, Debug)]
pub(crate) enum TableChange<'a> {
    /// A session was put in the table.
    Installed {
        key: SessionKey,
        install: Option<&'a Install>,
    },
    /// A session was taken out of it.
    Removed {
        key: SessionKey,
        install: Option<&'a Install>,
        cause: RemoveCause,
    },
    /// A `remove` with a stale key, whose session the table no longer
    /// holds, removed nothing.
    RemoveIgnored {
        key: SessionKey,
        removal: &'a Removal,
    },
}

/// Why a `remove` runs, and the remover's connection tap, which a removal
/// that finds nothing to remove reports through (M6a-7b).
#[cfg(any(test, zakura_sim))]
#[derive(Clone, Debug)]
pub(crate) struct Removal {
    pub(crate) cause: RemoveCause,
    pub(crate) tap: Option<std::sync::Arc<ConnTap>>,
}

#[cfg(any(test, zakura_sim))]
impl Removal {
    /// A removal for `cause`, by the connection whose tap is `tap`.
    pub(crate) fn new(cause: RemoveCause, tap: Option<&std::sync::Arc<ConnTap>>) -> Self {
        Self {
            cause,
            tap: tap.cloned(),
        }
    }
}

/// How a table reports its changes. It must not block, yield or take a
/// `zakura_rt` lock, since it runs under the table's.
#[cfg(any(test, zakura_sim))]
pub(crate) struct Reporter(Box<dyn Fn(TableChange<'_>) + Send + Sync>);

#[cfg(any(test, zakura_sim))]
impl std::fmt::Debug for Reporter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Reporter")
    }
}

/// The outcome of [`SessionTable::replace`].
#[derive(Debug)]
pub(crate) enum Replacement<S> {
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
    /// The owning service's allocation ID and what hears each change, once
    /// a service sets them (M6a-7b).
    #[cfg(any(test, zakura_sim))]
    reporter: std::sync::OnceLock<(u64, Reporter)>,
}

impl<S> Default for SessionTable<S> {
    fn default() -> Self {
        Self {
            current: Mutex::new(HashMap::new()),
            changed: watch::channel(()).0,
            #[cfg(any(test, zakura_sim))]
            reporter: std::sync::OnceLock::new(),
        }
    }
}

#[cfg(any(test, zakura_sim))]
impl<S> SessionTable<S> {
    /// Has `report` hear each later change to the table, under its lock,
    /// for the service with allocation ID `service` (M6a-7b). Only the
    /// first call sets one.
    pub(crate) fn report_with(
        &self,
        service: u64,
        report: impl Fn(TableChange<'_>) + Send + Sync + 'static,
    ) {
        let _ = self.reporter.set((service, Reporter(Box::new(report))));
    }

    /// The owning service's allocation ID, once it set its reporter.
    pub(crate) fn service(&self) -> Option<u64> {
        self.reporter.get().map(|(service, _)| *service)
    }

    /// Hands `change` to the reporter, if one is set, inside an owner
    /// callback, so a reporter that reaches the table panics instead of
    /// deadlocking.
    fn report(&self, change: TableChange<'_>) {
        if let Some((_, Reporter(report))) = self.reporter.get() {
            diag::owner_callback("a session table", || report(change));
        }
    }
}

#[cfg(any(test, zakura_sim))]
impl SessionTable<ServicePeerDirection> {
    /// The table's boundary read: each current session, by connection and
    /// session ID, with what it was installed from, peeked under the
    /// table's lock with no scheduling point, through a std `Weak`, so it
    /// reads `Gone` once the table drops (M6a-7b). It's `Held` while a
    /// task holds the lock.
    pub(crate) fn read(table: &std::sync::Arc<Self>) -> diag::ComponentRead {
        let table = diag::weak(table);
        Box::new(move || {
            diag::upgrade(&table, |table| {
                diag::refuse_reentry("a session table");
                diag::peek(&table.current, |current| {
                    let mut entries: Vec<diag::SessionEntry> = current
                        .iter()
                        .map(|(peer, current)| diag::SessionEntry {
                            peer: peer.clone(),
                            conn_id: current.key.conn_id,
                            session_id: current.key.session_id,
                            direction: current.session,
                            installed: current.installed.as_ref().map(|install| install.facts),
                        })
                        .collect();
                    entries.sort_unstable_by_key(|entry| (entry.conn_id, entry.session_id));
                    diag::OwnerState::HeaderSessions(entries)
                })
            })
        })
    }
}

impl<S: Clone> SessionTable<S> {
    /// Watch for changes. A notification means "read a snapshot".
    pub(crate) fn subscribe(&self) -> watch::Receiver<()> {
        self.changed.subscribe()
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<ZakuraPeerId, Current<S>>> {
        #[cfg(any(test, zakura_sim))]
        diag::refuse_reentry("a session table");
        self.current.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Make `current` the peer's session.
    ///
    /// Under the table lock, retire the old session's fence, then cancel and
    /// replace it.
    pub(crate) fn replace(&self, peer: ZakuraPeerId, current: Current<S>) -> Replacement<S> {
        let mut table = self.lock();
        let replaced = match table.remove(&peer) {
            None => Replacement::Inserted,
            Some(old) => {
                let reusable = old.fence.retire();
                if !reusable && old.key.conn_id == current.key.conn_id {
                    current.cancel.cancel();
                    // The connection is closing. The old session's own
                    // teardown removes it.
                    table.insert(peer, old);
                    return Replacement::Refused;
                }
                old.cancel.cancel();
                #[cfg(any(test, zakura_sim))]
                self.report(TableChange::Removed {
                    key: old.key,
                    install: old.installed.as_ref(),
                    cause: RemoveCause::Replaced,
                });
                Replacement::Replaced(old)
            }
        };
        #[cfg(any(test, zakura_sim))]
        let (key, install) = (current.key, current.installed.clone());
        table.insert(peer, current);
        #[cfg(any(test, zakura_sim))]
        self.report(TableChange::Installed {
            key,
            install: install.as_ref(),
        });
        self.changed.send_replace(());
        replaced
    }

    /// Remove the peer's session if `key` still names it. Retire its fence
    /// and cancel it first.
    ///
    /// A stale key, from a session that was already replaced, removes nothing.
    /// Test and simulator builds take the `removal` they report the change,
    /// or the stale key, with.
    pub(crate) fn remove(
        &self,
        peer: &ZakuraPeerId,
        key: SessionKey,
        #[cfg(any(test, zakura_sim))] removal: Removal,
    ) -> Option<Current<S>> {
        let mut table = self.lock();
        #[cfg(any(test, zakura_sim))]
        if table.get(peer).is_none_or(|current| current.key != key) {
            self.report(TableChange::RemoveIgnored {
                key,
                removal: &removal,
            });
        }
        if table.get(peer)?.key != key {
            return None;
        }
        let removed = table.remove(peer)?;
        #[cfg(any(test, zakura_sim))]
        self.report(TableChange::Removed {
            key,
            install: removed.installed.as_ref(),
            cause: removal.cause,
        });
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
