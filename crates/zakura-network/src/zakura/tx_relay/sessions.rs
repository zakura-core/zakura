//! The record of live `tx-relay` sessions, which the legacy adapter reads to
//! leave a peer's transactions to `tx-relay`.

use std::sync::Arc;

use zakura_rt::{collections::HashMap, sync::Mutex};

use crate::zakura::{ZakuraConnId, ZakuraPeerId};

/// Which peers have a live `tx-relay` session, each session named by its
/// peer, connection and session key.
///
/// `tx-relay` publishes an entry when a session opens. Only that session
/// removes it, by dropping its [`LiveSession`], so a newer session to the
/// same peer is never removed by an older one's cleanup. Clones share one
/// record.
#[derive(Clone, Debug, Default)]
pub struct TxRelaySessions {
    live: Arc<Mutex<HashMap<ZakuraPeerId, Vec<SessionName>>>>,
}

/// One session's name within its peer's entries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SessionName {
    conn_id: ZakuraConnId,
    session: u64,
}

impl TxRelaySessions {
    /// Publishes a live session for `peer` and returns the handle that owns
    /// the entry. The entry stays until that handle drops.
    pub(crate) fn open(
        &self,
        peer: ZakuraPeerId,
        conn_id: ZakuraConnId,
        session: u64,
    ) -> LiveSession {
        let name = SessionName { conn_id, session };
        self.lock().entry(peer.clone()).or_default().push(name);
        LiveSession {
            record: self.clone(),
            peer,
            name,
        }
    }

    /// Whether `peer` has at least one live session.
    pub(crate) fn is_live(&self, peer: &ZakuraPeerId) -> bool {
        self.lock().contains_key(peer)
    }

    /// Removes exactly `name` from `peer`'s entries, and the peer once it has
    /// none left.
    fn close(&self, peer: &ZakuraPeerId, name: SessionName) {
        let mut live = self.lock();
        let Some(names) = live.get_mut(peer) else {
            return;
        };
        names.retain(|live| *live != name);
        if names.is_empty() {
            live.remove(peer);
        }
    }

    /// Locks the record.
    fn lock(&self) -> zakura_rt::sync::MutexGuard<'_, HashMap<ZakuraPeerId, Vec<SessionName>>> {
        self.live
            .lock()
            .expect("the session record is never poisoned: no code panics while holding it")
    }
}

/// One published session. Dropping it removes that entry and no other.
#[derive(Debug)]
pub(crate) struct LiveSession {
    record: TxRelaySessions,
    peer: ZakuraPeerId,
    name: SessionName,
}

impl Drop for LiveSession {
    fn drop(&mut self) {
        self.record.close(&self.peer, self.name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A test peer id from one byte.
    fn peer(byte: u8) -> ZakuraPeerId {
        ZakuraPeerId::new(vec![byte; 32]).expect("32-byte node id is valid")
    }

    #[test]
    fn only_the_owning_session_removes_its_entry() {
        let record = TxRelaySessions::default();
        let old = record.open(peer(1), 1, 10);
        let new = record.open(peer(1), 2, 11);
        assert!(record.is_live(&peer(1)));
        assert!(!record.is_live(&peer(2)));

        drop(old);
        assert!(record.is_live(&peer(1)), "the newer session is still live");

        drop(new);
        assert!(!record.is_live(&peer(1)));
    }
}
