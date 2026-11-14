//! `header-sync/state-publication-consistent`'s served-page rule: every page
//! a publisher hands its writer (`PageQueued`) is what storage read for it.
//!
//! Each publisher page job leases a path and reads one page from its start
//! (`read_header_path`), then builds the page from what it read. Storage's
//! read result keeps each header's hash and its parent's, view-side
//! ([`ResultIdentity::Page`](zakura_storage::header::evidence::ResultIdentity)),
//! and the read's own state is the reference, so a reorganization after the
//! read is fine. A queued page names the lease its read used, and each
//! lease serves one read, so the page joins its session's completed page
//! read under that lease. Then:
//!
//! - **What was read.** The page's headers are the read's, in order.
//!   That the read is a path from its start is the storage owner model's
//!   check, which fails the run at the read's completion, before its page
//!   can be queued. A schema-1 page may stop before a header storage has
//!   no auxiliary record for, but no harness subscription asks for schema 1,
//!   so the page must be the whole read.
//! - **Where it starts.** A page declares its read's start, hash and
//!   height, as its common ancestor. The start's height is the result's
//!   last height less its count.
//!
//! A page with no completed read under its lease breaks too. Whether its
//! subscriber takes the page, toward the target its `Open` asked for, is
//! the subscriber's own `receive_page` to judge: a mirror that refuses one
//! reports it, which [`super::MirrorRefusals`] breaks. So a page still in flight when its link
//! closes or the run ends is judged here only against its read.

use std::collections::HashMap;

use zakura_chain::block;
use zakura_network::zakura::{observe::ReactorEvent, ZakuraPeerId};

use crate::{
    observe::{StorageFact, StorageRequest, StorageResult},
    storage::hash,
};

/// A completed page read, waiting for the page its job queues.
#[derive(Debug)]
struct Read {
    /// Where the read started, as the log writes hashes.
    after: String,
    /// The start's height, the last header's less the count read. `None` if
    /// the result's last height is below its count.
    after_height: Option<u32>,
    /// Each read header's hash, as the log writes hashes.
    headers: Vec<String>,
}

/// The served-page check's state, which [`super::Publication`] keeps.
#[derive(Debug, Default)]
pub(super) struct ServedPages {
    /// Each submitted page read with a lease this port issued, by operation,
    /// with its peer, session, lease and start.
    reading: HashMap<u64, (String, u64, u64, String)>,
    /// Each completed page read's links, by operation, until its completion
    /// fact is read.
    links: HashMap<u64, Vec<(block::Hash, block::Hash)>>,
    /// Each completed page read whose page isn't queued yet, by peer,
    /// session and lease.
    reads: HashMap<(String, u64, u64), Read>,
}

impl ServedPages {
    /// Notes the links of each page read that completed in the observation
    /// whose storage facts come next, by operation.
    pub(super) fn page_reads(&mut self, reads: &[(u64, Vec<(block::Hash, block::Hash)>)]) {
        self.links
            .extend(reads.iter().map(|(op, links)| (*op, links.clone())));
    }

    /// Notes one of the node's storage facts: a page read's submission, or
    /// its completion with a page delivered to its job.
    pub(super) fn storage(&mut self, fact: &StorageFact) {
        match fact {
            StorageFact::Submitted {
                id,
                request:
                    StorageRequest::ReadHeaderPath {
                        lease: Some(lease),
                        after,
                        ..
                    },
                ..
            } => {
                self.reading.insert(
                    *id,
                    (
                        lease.peer.clone(),
                        lease.session_id,
                        lease.lease_id,
                        after.clone(),
                    ),
                );
            }
            StorageFact::Completed {
                id,
                delivered,
                result,
                ..
            } => {
                let reading = self.reading.remove(id);
                let links = self.links.remove(id);
                if let (
                    true,
                    StorageResult::Page { last, count, .. },
                    Some((peer, session_id, lease, after)),
                    Some(links),
                ) = (*delivered, result, reading, links)
                {
                    self.reads.insert(
                        (peer, session_id, lease),
                        Read {
                            after,
                            after_height: last.height.checked_sub(*count),
                            headers: links.into_iter().map(|(header, _)| hash(header)).collect(),
                        },
                    );
                }
            }
            _ => {}
        }
    }

    /// Judges `event` if it's a `PageQueued`, for the session of the peer
    /// whose storage name `peer` gives, returning what it broke. `None` if
    /// it isn't one.
    pub(super) fn queued(
        &mut self,
        event: &ReactorEvent,
        peer: impl Fn(&ZakuraPeerId) -> String,
    ) -> Option<Vec<String>> {
        let ReactorEvent::PageQueued {
            peer: queued_for,
            session_id,
            subscription_id,
            common_ancestor,
            headers,
            lease,
            ..
        } = event
        else {
            return None;
        };
        let peer = peer(queued_for);
        let mut breaks = Vec::new();
        let page = format!("session {session_id} subscription {subscription_id}'s page");
        let Some(read) = self.reads.remove(&(peer, *session_id, *lease)) else {
            breaks.push(format!(
                "ServedPage: {page} names lease {lease}, under which no page read for the \
                 session completed"
            ));
            return Some(breaks);
        };
        let declared = hash(common_ancestor.hash);
        if declared != read.after || Some(common_ancestor.height.0) != read.after_height {
            breaks.push(format!(
                "ServedPage: {page} declares ancestor {common_ancestor:?}, not its read's start \
                 {} at {:?}",
                read.after, read.after_height
            ));
        }
        let served: Vec<String> = headers.iter().copied().map(hash).collect();
        if served.is_empty() || served != read.headers {
            breaks.push(format!(
                "ServedPage: {page} serves {served:?}, not its read's {:?}",
                read.headers
            ));
        }
        Some(breaks)
    }
}

#[cfg(test)]
mod tests;
