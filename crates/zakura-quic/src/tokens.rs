//! Address-validation tokens (WIRE-14, RFC 9000 §8.1.3).
//!
//! A listener gives a dialer whose address it validated a few tokens in
//! NEW_TOKEN frames. A later dial from the same IP that presents one is
//! validated at its first datagram, so the listener isn't held to three
//! times what it received and ADM-3 sends it no Retry. `quinn-proto` makes
//! and checks the tokens, binding each to the dialer's IP. This module holds
//! the two pieces it leaves to the application, both bounded:
//!
//! - [`PeerTokens`], the dialer's store. `quinn-proto` keys its store by TLS
//!   server name, which every Zakura dial leaves at one fixed value (TLS-6),
//!   so the store is scoped to the dialed node instead.
//! - [`UsedTokens`], the listener's log of tokens already presented, so each
//!   is accepted once.

use std::{
    collections::VecDeque,
    sync::Arc,
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use quinn_proto::{TokenLog, TokenReuseError, TokenStore};
use zakura_rt::{
    collections::HashMap,
    sync::{Mutex, PoisonError},
};

use crate::key::NodeId;

/// How long a listener accepts a token after issuing it. Its token key lives
/// only as long as its endpoint, so a restarted listener refuses older
/// tokens anyway.
pub(crate) const TOKEN_LIFETIME: Duration = Duration::from_secs(24 * 60 * 60);

/// Tokens a listener sends each dialer whose address it validated. Two cover
/// a redial whose first attempt fails after it spent a token.
pub(crate) const TOKENS_SENT: u32 = 2;

/// The most nodes a dialer keeps tokens for. Past it, the node it stored
/// tokens for longest ago loses them.
const MAX_PEERS: usize = 1_024;

/// The most tokens a dialer keeps for one node; a newer one replaces the
/// oldest.
const MAX_TOKENS_PER_PEER: usize = TOKENS_SENT as usize;

/// The most presented tokens a listener remembers. When it's full of tokens
/// still within their lifetime, the listener accepts no more tokens until
/// some expire: a dialer whose token is refused is only unvalidated.
const MAX_USED_TOKENS: usize = 32_768;

/// A dialer's tokens, by the node that issued them.
#[derive(Default)]
pub(crate) struct PeerTokens {
    inner: Mutex<Stored>,
}

#[derive(Default)]
struct Stored {
    tokens: HashMap<NodeId, VecDeque<Bytes>>,
    /// Nodes in the order their tokens were first stored, for eviction.
    order: VecDeque<NodeId>,
}

impl PeerTokens {
    /// The store a dial to `peer` hands `quinn-proto`.
    pub(crate) fn for_peer(self: &Arc<Self>, peer: NodeId) -> Arc<dyn TokenStore> {
        Arc::new(PeerStore {
            tokens: self.clone(),
            peer,
        })
    }

    fn insert(&self, peer: NodeId, token: Bytes) {
        let mut stored = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let Stored { tokens, order } = &mut *stored;
        if !tokens.contains_key(&peer) {
            if tokens.len() >= MAX_PEERS {
                if let Some(oldest) = order.pop_front() {
                    tokens.remove(&oldest);
                }
            }
            order.push_back(peer);
        }
        let queue = tokens.entry(peer).or_default();
        if queue.len() >= MAX_TOKENS_PER_PEER {
            queue.pop_front();
        }
        queue.push_back(token);
    }

    fn take(&self, peer: NodeId) -> Option<Bytes> {
        let mut stored = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let Stored { tokens, order } = &mut *stored;
        let queue = tokens.get_mut(&peer)?;
        // The newest token has the most lifetime left.
        let token = queue.pop_back();
        if queue.is_empty() {
            tokens.remove(&peer);
            order.retain(|node| *node != peer);
        }
        token
    }
}

/// [`PeerTokens`] seen by one dial: the server name is ignored, since every
/// dial uses the same one.
struct PeerStore {
    tokens: Arc<PeerTokens>,
    peer: NodeId,
}

impl TokenStore for PeerStore {
    fn insert(&self, _server_name: &str, token: Bytes) {
        self.tokens.insert(self.peer, token);
    }

    fn take(&self, _server_name: &str) -> Option<Bytes> {
        self.tokens.take(self.peer)
    }
}

/// A listener's log of the tokens dialers have presented, until each
/// expires.
#[derive(Default)]
pub(crate) struct UsedTokens {
    /// Each presented token's nonce, with the time it stops being valid.
    used: Mutex<HashMap<u128, SystemTime>>,
}

impl UsedTokens {
    /// Records `nonce` as presented at `now`. Refuses a nonce already
    /// recorded, and any nonce while the log is full of unexpired ones.
    fn check_and_insert_at(
        &self,
        nonce: u128,
        expires: SystemTime,
        now: SystemTime,
    ) -> Result<(), TokenReuseError> {
        let mut used = self.used.lock().unwrap_or_else(PoisonError::into_inner);
        if used.contains_key(&nonce) {
            return Err(TokenReuseError);
        }
        if used.len() >= MAX_USED_TOKENS {
            used.retain(|_, expiry| *expiry > now);
            if used.len() >= MAX_USED_TOKENS {
                return Err(TokenReuseError);
            }
        }
        used.insert(nonce, expires);
        Ok(())
    }
}

impl TokenLog for UsedTokens {
    fn check_and_insert(
        &self,
        nonce: u128,
        issued: SystemTime,
        lifetime: Duration,
    ) -> Result<(), TokenReuseError> {
        // An expiry past what `SystemTime` holds never comes; keep it.
        let expires = issued.checked_add(lifetime).unwrap_or(issued);
        self.check_and_insert_at(nonce, expires, zakura_rt::time::utc_now())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeSecretKey;

    fn node(byte: u8) -> NodeId {
        NodeSecretKey::from_bytes(&[byte; 32]).public()
    }

    #[test]
    fn a_token_goes_back_only_to_the_node_that_issued_it() {
        let tokens = Arc::new(PeerTokens::default());
        let (a, b) = (tokens.for_peer(node(1)), tokens.for_peer(node(2)));
        a.insert("same name", Bytes::from_static(b"from a"));
        assert_eq!(b.take("same name"), None);
        assert_eq!(a.take("same name"), Some(Bytes::from_static(b"from a")));
        assert_eq!(a.take("same name"), None, "a token is used once");
    }

    #[test]
    fn a_node_keeps_its_newest_tokens() {
        let tokens = Arc::new(PeerTokens::default());
        let store = tokens.for_peer(node(1));
        for token in [&b"1"[..], b"2", b"3"] {
            store.insert("", Bytes::copy_from_slice(token));
        }
        assert_eq!(store.take(""), Some(Bytes::from_static(b"3")));
        assert_eq!(store.take(""), Some(Bytes::from_static(b"2")));
        assert_eq!(store.take(""), None);
    }

    #[test]
    fn the_store_forgets_the_oldest_node_past_its_bound() {
        let tokens = Arc::new(PeerTokens::default());
        let first = NodeSecretKey::generate().public();
        tokens.insert(first, Bytes::from_static(b"first"));
        for _ in 0..MAX_PEERS {
            tokens.insert(NodeSecretKey::generate().public(), Bytes::new());
        }
        let stored = tokens.inner.lock().unwrap();
        assert_eq!(stored.tokens.len(), MAX_PEERS);
        assert_eq!(stored.order.len(), MAX_PEERS);
        assert!(!stored.tokens.contains_key(&first));
    }

    #[test]
    fn a_presented_token_is_refused_the_second_time() {
        let log = UsedTokens::default();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let expires = now + TOKEN_LIFETIME;
        assert!(log.check_and_insert_at(7, expires, now).is_ok());
        assert!(log.check_and_insert_at(7, expires, now).is_err());
        assert!(log.check_and_insert_at(8, expires, now).is_ok());
    }

    #[test]
    fn a_full_log_accepts_only_once_tokens_expire() {
        let log = UsedTokens::default();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let soon = now + Duration::from_secs(1);
        for nonce in 0..MAX_USED_TOKENS as u128 {
            assert!(log.check_and_insert_at(nonce, soon, now).is_ok());
        }
        let fresh = u128::MAX;
        assert!(log
            .check_and_insert_at(fresh, now + TOKEN_LIFETIME, now)
            .is_err());
        let later = soon + Duration::from_secs(1);
        assert!(log
            .check_and_insert_at(fresh, later + TOKEN_LIFETIME, later)
            .is_ok());
        assert_eq!(log.used.lock().unwrap().len(), 1);
    }
}
