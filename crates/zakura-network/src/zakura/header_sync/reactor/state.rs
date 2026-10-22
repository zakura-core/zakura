//! Logical header-sync reactor state.
//!
//! The types here hold plain values. They own no session, channel, future,
//! permit, or timer, and they read no clock. [`super::HeaderSyncReactor`]
//! owns those runtime resources and runs the operations this state records.

use zakura_header_chain::HeaderWorkAuthority;
use zakura_rt::collections::HashMap;

use super::{AdvertisedHeaderTarget, ZakuraPeerId};

/// The identity of one continuation-locator read.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct LocatorKey {
    /// The peer whose awaiting target the read serves.
    pub(super) peer: ZakuraPeerId,
    /// Distinguishes this read from every other read for `peer`.
    pub(super) query: u64,
}

/// How a locator request for one peer proceeds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum LocatorProposal {
    /// The peer's read in flight also serves this request.
    Joined,
    /// State recorded a new read, which IO must start or refuse.
    Start(LocatorKey),
}

/// What a settled locator read does to its peer's awaiting target.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum LocatorDisposition {
    /// No read with this key is pending, or the peer awaits no target.
    Stale,
    /// The read cannot serve the awaiting target, so the target needs a fresh read.
    Refetch,
    /// The read serves the awaiting target.
    Ready,
}

/// One pending continuation-locator read.
#[derive(Copy, Clone, Debug)]
struct LocatorQuery {
    query: u64,
    /// The authority whose selected path the read follows.
    scope: HeaderWorkAuthority,
}

/// Continuation-locator reads in flight, at most one per peer.
///
/// A peer's read stays pending until its storage operation settles, even when
/// the peer's session or awaiting target changes. A later request for the same
/// peer joins the pending read instead of starting another.
#[derive(Debug, Default)]
pub(super) struct LocatorQueries {
    pending: HashMap<ZakuraPeerId, LocatorQuery>,
    /// The query number of the next recorded read.
    next_query: u64,
}

impl LocatorQueries {
    /// The number of pending reads.
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.pending.len()
    }

    /// Record a read for `peer` under `scope`, unless `peer` already has one pending.
    pub(super) fn propose(
        &mut self,
        peer: &ZakuraPeerId,
        scope: HeaderWorkAuthority,
    ) -> LocatorProposal {
        if self.pending.contains_key(peer) {
            return LocatorProposal::Joined;
        }
        let query = self.next_query;
        self.next_query = self.next_query.wrapping_add(1);
        self.pending
            .insert(peer.clone(), LocatorQuery { query, scope });
        LocatorProposal::Start(LocatorKey {
            peer: peer.clone(),
            query,
        })
    }

    /// Remove the read that `key` names, and return its authority.
    ///
    /// Another read for the same peer stays pending.
    pub(super) fn remove_exact(&mut self, key: &LocatorKey) -> Option<HeaderWorkAuthority> {
        let pending = self.pending.get(&key.peer)?;
        if pending.query != key.query {
            return None;
        }
        let scope = pending.scope;
        self.pending.remove(&key.peer);
        Some(scope)
    }

    /// Settle the read that `key` names, and classify its effect on `awaiting`,
    /// the peer's current awaiting target.
    ///
    /// Locators follow the local selected path, so tip churn under the same
    /// header generation and finality anchor can reuse one read, even across a
    /// session replacement. A generation or reanchor change needs a fresh read,
    /// and so does a failed read that belonged to a replaced tip.
    pub(super) fn settle(
        &mut self,
        key: &LocatorKey,
        awaiting: Option<&AdvertisedHeaderTarget>,
        found: bool,
    ) -> LocatorDisposition {
        let Some(query_scope) = self.remove_exact(key) else {
            return LocatorDisposition::Stale;
        };
        let Some(target) = awaiting else {
            return LocatorDisposition::Stale;
        };
        let same_selected_path_authority = target.scope.header_generation
            == query_scope.header_generation
            && target.scope.branch.anchor_hash == query_scope.branch.anchor_hash;
        let superseded_tip = target.status.selected_tip_hash != query_scope.branch.target_tip_hash;
        if !same_selected_path_authority || (!found && superseded_tip) {
            LocatorDisposition::Refetch
        } else {
            LocatorDisposition::Ready
        }
    }
}
