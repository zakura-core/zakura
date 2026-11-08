//! `header-sync/commit-only-authorized`'s commit-contents half: what each
//! apply's and operator reset's engine run did to the engine's whole
//! durable state, as the controlled port recorded it just before and just
//! after the run.
//!
//! - **Insert set.** Every header an apply inserts, whatever its result, is
//!   one of the hashes of the insertion it submitted. No run drops a held
//!   header, since no fixture reaches retention. A reset inserts nothing.
//! - **Failure.** An apply whose result is a failure leaves the durable
//!   state exactly as it was. One that only raises a resource-stall alarm
//!   inserts nothing.
//! - **Success.** After any other apply, and after every reset, each held
//!   header is eligible exactly when neither it nor an ancestor stands
//!   invalidated by the port's `Reset` facts and not reconsidered. The
//!   stored selected projection is the ancestry from the finalized anchor
//!   to its own tip, each entry the parent of the next by its header, and
//!   the verified projection is unchanged, since header applies and resets
//!   never verify bodies. Which tip is selected is
//!   `expected-chain-selected`'s.

use std::collections::{HashMap, HashSet};

use zakura_chain::block;
use zakura_header_chain::{DurableState, HeaderNode};
use zakura_storage::header::{controlled::EngineRun, evidence::ResultIdentity};

use crate::{
    observe::{StorageFact, StorageRequest},
    storage::hash,
};

/// The commit-contents rule's state.
#[derive(Debug, Default)]
pub(super) struct Contents {
    /// Each apply operation's submitted insertion, by its hashes in order,
    /// or `None` if the port couldn't open it.
    payloads: HashMap<u64, Option<Vec<String>>>,
    /// The headers the port's `Reset` facts left invalidated, oldest first.
    invalidated: Vec<String>,
    breaks: Vec<String>,
}

impl Contents {
    /// Notes storage's `fact`: an apply's submitted insertion, or a reset's
    /// invalidation or reconsideration.
    pub(super) fn storage(&mut self, fact: &StorageFact) {
        match fact {
            StorageFact::Submitted {
                id,
                request: StorageRequest::ApplyHeaderTarget { hashes, .. },
                ..
            } => {
                self.payloads.insert(*id, hashes.clone());
            }
            StorageFact::Reset { kind, target, .. } if kind == "invalidate" => {
                self.invalidated.push(target.clone());
            }
            StorageFact::Reset { target, .. } => {
                self.invalidated.retain(|held| held != target);
            }
            _ => {}
        }
    }

    /// Judges one engine `run`.
    pub(super) fn run(&mut self, run: &EngineRun) {
        let EngineRun {
            apply,
            before,
            after,
        } = run;
        let what = match apply {
            Some((id, _)) => format!("apply operation {}", id.0),
            None => "a reset".to_owned(),
        };
        let mut found = Vec::new();
        let mut dropped: Vec<String> = before
            .graph
            .nodes
            .keys()
            .filter(|held| !after.graph.nodes.contains_key(held))
            .map(|held| hash(*held))
            .collect();
        dropped.sort_unstable();
        if !dropped.is_empty() {
            found.push(format!("dropped held headers {dropped:?}"));
        }
        let mut inserted: Vec<String> = after
            .graph
            .nodes
            .keys()
            .filter(|new| !before.graph.nodes.contains_key(new))
            .map(|new| hash(*new))
            .collect();
        inserted.sort_unstable();
        let success = match apply {
            Some((id, result)) => {
                let payload = self.payloads.get(&id.0).cloned().flatten();
                let stray: Vec<&String> = inserted
                    .iter()
                    .filter(|new| !payload.as_ref().is_some_and(|sent| sent.contains(new)))
                    .collect();
                if !stray.is_empty() {
                    found.push(format!(
                        "inserted {stray:?}, which its submitted insertion {payload:?} doesn't \
                         hold"
                    ));
                }
                match result {
                    ResultIdentity::Applied => true,
                    ResultIdentity::ResourceStalled => {
                        if !inserted.is_empty() {
                            found.push(format!(
                                "only raised a resource-stall alarm but inserted {inserted:?}"
                            ));
                        }
                        true
                    }
                    failed => {
                        if before != after {
                            found.push(format!(
                                "returned {failed:?} but changed the engine's durable state"
                            ));
                        }
                        false
                    }
                }
            }
            None => {
                if !inserted.is_empty() {
                    found.push(format!("inserted {inserted:?}"));
                }
                true
            }
        };
        if success {
            self.judge_state(before, after, &mut found);
        }
        for detail in found {
            self.breaks
                .push(format!("CommitOnlyAuthorized: {what} {detail}"));
        }
    }

    /// Judges the durable state `after` a successful run against the reset
    /// model and the projection's own ancestry, and against `before` for
    /// the verified projection.
    fn judge_state(&self, before: &DurableState, after: &DurableState, found: &mut Vec<String>) {
        let nodes = &after.graph.nodes;
        let invalidated: HashSet<&str> = self.invalidated.iter().map(String::as_str).collect();
        let mut wrong: Vec<String> = nodes
            .values()
            .filter(|node| node.is_eligible() == stands_invalidated(node, nodes, &invalidated))
            .map(|node| hash(node.hash))
            .collect();
        wrong.sort_unstable();
        if !wrong.is_empty() {
            found.push(format!(
                "left {wrong:?} eligible while invalidated, or ineligible though nothing \
                 invalidated them"
            ));
        }
        let projection = &after.selected_projection;
        if projection.first() != Some(&after.graph.finalized_frontier) {
            found.push(format!(
                "left the selected projection starting at {:?}, not the finalized anchor {:?}",
                projection.first(),
                after.graph.finalized_frontier
            ));
        }
        for pair in projection.windows(2) {
            let links = nodes.get(&pair[1].hash).is_some_and(|child| {
                child.header.previous_block_hash == pair[0].hash
                    && child.header.hash() == pair[1].hash
                    && pair[1].height.0 == pair[0].height.0.saturating_add(1)
            });
            if !links {
                found.push(format!(
                    "left the selected projection holding {:?} after {:?}, which isn't its \
                     held child",
                    pair[1], pair[0]
                ));
            }
        }
        if before.verified_projection != after.verified_projection {
            found.push(format!(
                "moved the verified projection from {:?} to {:?}",
                before.verified_projection, after.verified_projection
            ));
        }
    }

    /// The breaks found since the last call.
    pub(super) fn take_breaks(&mut self) -> Vec<String> {
        std::mem::take(&mut self.breaks)
    }
}

/// Whether `node` or one of its held ancestors in `nodes` is in
/// `invalidated`.
fn stands_invalidated(
    node: &HeaderNode,
    nodes: &HashMap<block::Hash, HeaderNode>,
    invalidated: &HashSet<&str>,
) -> bool {
    let mut at = Some(node);
    // Each step moves to a strictly lower height, so the walk ends.
    while let Some(node) = at {
        if invalidated.contains(hash(node.hash).as_str()) {
            return true;
        }
        at = nodes
            .get(&node.header.previous_block_hash)
            .filter(|parent| parent.height < node.height);
    }
    false
}
