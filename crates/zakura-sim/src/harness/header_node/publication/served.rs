//! `header-sync/state-publication-consistent`'s served-page rule: every page
//! a publisher hands its writer (`PageQueued`) is what storage read for it,
//! toward the target its subscriber asked for.
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
//!   last height less its count. A later page of the subscription declares
//!   the cursor its previous page left, the last header and its height. A
//!   first page starts at one of its `Open`'s locators, not always the
//!   first, or at the target itself if the `Open` named it among them,
//!   since its subscriber already holds it. Storage starts a path only at a
//!   locator it's given, which its owner model checks, so this catches a
//!   publisher that asked from other locators.
//! - **Toward what.** Every page carries the initial target of its
//!   subscription's `Open`, as the subscriber built it. Each mirror's own
//!   reactor reports that `Open` ([`super::SubscriberOpens`]), so its
//!   target, the target's height and its locators come from the far end of
//!   the link, not from the node under test.
//! - **As its subscriber takes it.** Until the subscription reaches its
//!   target, at its `Open` or with a marked page, a page passes neither the
//!   target nor one of the `Open`'s locators before its last header, ends
//!   at a locator only if it's the target, reaches the target's height
//!   only at the target, and carries its marker exactly when it ends at the
//!   target. After that, a page follows the node's tip past the target and
//!   carries no marker. These are the path and target checks of the
//!   subscriber's `receive_page`, restated against its `Open`. Its credit,
//!   size and schema checks aren't restated.
//!
//! A page with no completed read under its lease breaks too, and so does
//! one whose subscription has no recorded `Open`.

use std::collections::HashMap;

use zakura_chain::block;
use zakura_header_chain::Frontier;
use zakura_network::zakura::{observe::ReactorEvent, ZakuraPeerId};

use crate::{
    observe::{StorageFact, StorageRequest, StorageResult},
    storage::hash,
};

/// A subscription, by its subscriber's name, session and ID.
pub(super) type SubscriptionKey = (String, u64, u64);

/// A subscription's `Open`, as its subscriber built it.
#[derive(Clone, Debug)]
pub(super) struct Opened {
    /// Its initial target.
    pub(super) target: block::Hash,
    /// The target's height, which the `Open` names only by hash.
    pub(super) target_height: block::Height,
    /// Its locators, nearest first.
    pub(super) locators: Vec<block::Hash>,
}

/// Where a subscription's pages have left it.
#[derive(Debug)]
struct Progress {
    /// The last page's last header and its height.
    cursor: Frontier,
    /// Whether a marked page reached the target.
    reached: bool,
}

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
    /// Where each subscription's pages have left it.
    progress: HashMap<SubscriptionKey, Progress>,
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
    /// whose storage name `peer` gives, against the `Open` that `opened`
    /// gives for the subscriber's peer ID, session and subscription,
    /// returning what it broke. `None` if it isn't one.
    pub(super) fn queued(
        &mut self,
        event: &ReactorEvent,
        peer: impl Fn(&ZakuraPeerId) -> String,
        opened: impl Fn(&ZakuraPeerId, u64, u64) -> Option<Opened>,
    ) -> Option<Vec<String>> {
        let ReactorEvent::PageQueued {
            peer: queued_for,
            session_id,
            subscription_id,
            common_ancestor,
            headers,
            initial_target,
            reaches_target,
            lease,
        } = event
        else {
            return None;
        };
        let peer = peer(queued_for);
        let mut breaks = Vec::new();
        let page = format!("session {session_id} subscription {subscription_id}'s page");
        let Some(read) = self.reads.remove(&(peer.clone(), *session_id, *lease)) else {
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
        let open = opened(queued_for, *session_id, *subscription_id);
        match &open {
            Some(open) if open.target == *initial_target => {}
            Some(open) => breaks.push(format!(
                "ServedPage: {page} carries initial target {initial_target}, not its \
                 subscription's `Open` target {}",
                open.target
            )),
            None => breaks.push(format!(
                "ServedPage: {page} carries initial target {initial_target}, for a \
                 subscription whose subscriber recorded no `Open`"
            )),
        }
        let key = (peer, *session_id, *subscription_id);
        let progress = self.progress.get(&key);
        if let Some(progress) = progress.filter(|progress| *common_ancestor != progress.cursor) {
            breaks.push(format!(
                "ServedPage: {page} declares ancestor {common_ancestor:?}, not the cursor \
                 {:?} its previous page left",
                progress.cursor
            ));
        }
        let mut reached = progress.is_some_and(|progress| progress.reached);
        if let Some(open) = &open {
            let held = open.locators.contains(&open.target);
            reached |= held;
            if progress.is_none() {
                let start = if held {
                    (*common_ancestor != Frontier::new(open.target_height, open.target))
                        .then_some("isn't the target its `Open` named among its locators")
                } else {
                    (!open.locators.contains(&common_ancestor.hash))
                        .then_some("isn't one of its `Open`'s locators")
                };
                if let Some(start) = start {
                    breaks.push(format!(
                        "ServedPage: {page} is its subscription's first, from \
                         {common_ancestor:?}, which {start}"
                    ));
                }
            }
            if let Some(refusal) =
                refusal(open, reached, *common_ancestor, headers, *reaches_target)
            {
                breaks.push(format!(
                    "ServedPage: {page} {refusal}, which its subscriber refuses"
                ));
            }
        }
        // A page holds at most a few thousand headers, so the count converts.
        let sent = u32::try_from(headers.len()).unwrap_or(u32::MAX);
        let cursor = headers.last().map_or(*common_ancestor, |last| {
            Frontier::new(
                block::Height(common_ancestor.height.0.saturating_add(sent)),
                *last,
            )
        });
        self.progress.insert(
            key,
            Progress {
                cursor,
                reached: reached || *reaches_target,
            },
        );
        Some(breaks)
    }
}

/// Why `open`'s subscriber refuses a page from `ancestor` of `headers` with
/// target marker `marker`, where `reached` says whether the `Open` or an
/// earlier marked page reached the target: the path and target checks of
/// the subscriber's `receive_page`, restated. `None` if it takes the page,
/// or the page is empty, which the read comparison breaks.
fn refusal(
    open: &Opened,
    reached: bool,
    ancestor: Frontier,
    headers: &[block::Hash],
    marker: bool,
) -> Option<String> {
    let (&last, before) = headers.split_last()?;
    if reached {
        return marker.then(|| "marks a target its subscription already reached".to_owned());
    }
    for header in before {
        if *header == open.target {
            return Some(format!("passes its target {header} before its last header"));
        }
        if open.locators.contains(header) {
            return Some(format!(
                "serves {header}, one of its `Open`'s locators short of its target"
            ));
        }
    }
    // A page holds at most a few thousand headers, so the count converts.
    let height = ancestor
        .height
        .0
        .saturating_add(u32::try_from(headers.len()).unwrap_or(u32::MAX));
    let reaches = last == open.target;
    if !reaches && open.locators.contains(&last) {
        return Some(format!(
            "ends at {last}, one of its `Open`'s locators short of its target"
        ));
    }
    if !reaches && height >= open.target_height.0 {
        return Some(format!(
            "ends at {last} at height {height}, at or above its target's {:?}",
            open.target_height
        ));
    }
    if reaches && height != open.target_height.0 {
        return Some(format!(
            "ends at its target at height {height}, not its target's {:?}",
            open.target_height
        ));
    }
    (marker != reaches).then(|| format!("ends at {last} with its target marker {marker}"))
}

#[cfg(test)]
mod tests;
