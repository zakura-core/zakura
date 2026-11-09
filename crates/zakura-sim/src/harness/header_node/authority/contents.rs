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
//! - **Success.** After any other apply, and after every reset, the graph
//!   is what its stored headers alone give: each node's key, hash, parent,
//!   height, block work and cumulative work, and the children, height and
//!   eligible-tip indexes rebuilt from the nodes. A node held before and
//!   after changes only in its eligibility (and its auxiliary delivery IDs,
//!   which Rule 11 leaves aside). Its direct reasons stay as they were,
//!   except that a reset's invalidation adds its one operator reason to its
//!   target and a reconsideration removes it, and a new node has none. Each
//!   node inherits from its parent exactly when the parent is ineligible.
//!   Each held header is
//!   eligible exactly when neither it nor an ancestor stands invalidated by
//!   the port's `Reset` facts and not reconsidered. The stored selected
//!   projection is the ancestry from the finalized anchor to the engine's
//!   selected frontier, each entry the parent of the next by its header,
//!   and the verified projection is unchanged, since header applies and
//!   resets never verify bodies. Which tip is selected is
//!   `expected-chain-selected`'s.
//! - **Read.** A locator read leaves the durable state exactly as it was.

use std::collections::{HashMap, HashSet, VecDeque};

use chrono::{DateTime, Utc};

use sha2::{Digest, Sha256};
use zakura_chain::block;
use zakura_header_chain::{
    BodyValidationState, DurableState, EligibilityReason, EngineMetadata, HeaderNode,
    HeaderValidationState, OperatorInvalidationId,
};
use zakura_storage::header::{
    controlled::{EngineRun, RunKind},
    evidence::ResultIdentity,
};

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
    /// The headers the judged reset runs left invalidated, oldest first.
    invalidated: Vec<String>,
    /// The drain's `Reset` facts not yet matched to their runs, oldest
    /// first: whether each invalidated, and its target.
    resets: VecDeque<(bool, String)>,
    breaks: Vec<String>,
}

impl Contents {
    /// Notes storage's `fact`: an apply's submitted insertion, or a reset's
    /// invalidation or reconsideration, which its run then applies.
    pub(super) fn storage(&mut self, fact: &StorageFact) {
        match fact {
            StorageFact::Submitted {
                id,
                request: StorageRequest::ApplyHeaderTarget { hashes, .. },
                ..
            } => {
                self.payloads.insert(*id, hashes.clone());
            }
            StorageFact::Reset { kind, target, .. } => {
                self.resets
                    .push_back((kind == "invalidate", target.clone()));
            }
            _ => {}
        }
    }

    /// Judges one engine `run`.
    pub(super) fn run(&mut self, run: &EngineRun) {
        let EngineRun {
            kind,
            clock,
            before,
            after,
        } = run;
        let what = match kind {
            RunKind::Apply(id, _) => format!("apply operation {}", id.0),
            RunKind::Reset => "a reset".to_owned(),
            RunKind::Locator(id) => {
                // Its result is `reset-invalidates-work`'s locator-result
                // rule's, judged against `before`.
                if before != after {
                    self.breaks.push(format!(
                        "CommitOnlyAuthorized: locator read {} changed the engine's durable \
                         state",
                        id.0
                    ));
                }
                return;
            }
        };
        let mut found = Vec::new();
        // Each reset run is its oldest unmatched `Reset` fact's, so the
        // invalidated set changes at the run that changed it.
        let reset = match kind {
            RunKind::Reset => {
                let reset = self.resets.pop_front();
                match &reset {
                    Some((true, target)) => self.invalidated.push(target.clone()),
                    Some((false, target)) => self.invalidated.retain(|held| held != target),
                    None => found.push("ran with no Reset fact".to_owned()),
                }
                reset
            }
            _ => None,
        };
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
        let success = match kind {
            RunKind::Apply(id, result) => {
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
                    ResultIdentity::Applied => {
                        // An applied target holds every submitted header,
                        // whether it inserted it or already held it.
                        let mut missing: Vec<String> = payload
                            .iter()
                            .flatten()
                            .filter(|sent| {
                                !after.graph.nodes.keys().any(|key| hash(*key) == **sent)
                            })
                            .cloned()
                            .collect();
                        missing.sort_unstable();
                        if !missing.is_empty() {
                            found.push(format!(
                                "returned Applied without holding submitted headers {missing:?}"
                            ));
                        }
                        true
                    }
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
            _ => {
                if !inserted.is_empty() {
                    found.push(format!("inserted {inserted:?}"));
                }
                true
            }
        };
        if success {
            self.judge_state(*clock, before, after, reset.as_ref(), &mut found);
        }
        for detail in found {
            self.breaks
                .push(format!("CommitOnlyAuthorized: {what} {detail}"));
        }
    }

    /// Judges the durable state `after` a successful run, field by field:
    /// every field of the engine, its graph and its metadata is destructured
    /// with no `..`, so a field added later fails to compile here until it's
    /// classified. Each is derived from the stored headers and the reset
    /// model, or must be unchanged from `before`, or is left to a named
    /// owner. `reset` is the run's reset, if it's one: whether it
    /// invalidated, and its target.
    fn judge_state(
        &self,
        clock: DateTime<Utc>,
        before: &DurableState,
        after: &DurableState,
        reset: Option<&(bool, String)>,
        found: &mut Vec<String>,
    ) {
        let DurableState {
            graph,
            metadata,
            selected_projection,
            verified_projection,
            // Auxiliary delivery evidence is Rule 11's.
            aux_delivery_index: _,
        } = after;
        let zakura_header_chain::GraphState {
            // An in-memory change counter, like the engine's own revision.
            graph_revision: _,
            finalized_frontier,
            preferred_header_tip,
            nodes,
            // Rebuilt from the nodes by `structure`.
            children: _,
            heights: _,
            eligible_header_tips: _,
            consensus_invalid_body_tombstones,
        } = graph;
        let EngineMetadata {
            disk_format,
            mode,
            network_id,
            network_policy_digest,
            anchor_manifest_digest,
            work_origin,
            // Advances on every commit; its own rule is the planner's.
            state_version: _,
            // `reset-invalidates-work`'s insertion and reset rules.
            header_generation: _,
            verified_generation,
            finality_epoch,
            headers_only_migration_epoch,
            frontiers,
            header_best_score,
            oldest_retained_height,
            // A resource stall's alarm, the only change no fixture judges.
            alarms: _,
            // The committed transition's identity.
            last_transition: _,
        } = metadata;
        let old = &before.metadata;
        // No header apply or reset in these fixtures finalizes, verifies a
        // body, migrates, prunes or changes the engine's identity.
        let unchanged = [
            ("the disk format", disk_format == &old.disk_format),
            ("the mode", mode == &old.mode),
            ("the network", network_id == &old.network_id),
            (
                "the network policy",
                network_policy_digest == &old.network_policy_digest,
            ),
            (
                "the anchor manifest",
                anchor_manifest_digest == &old.anchor_manifest_digest,
            ),
            ("the work origin", work_origin == &old.work_origin),
            (
                "the verified generation",
                verified_generation == &old.verified_generation,
            ),
            ("the finality epoch", finality_epoch == &old.finality_epoch),
            (
                "the migration epoch",
                headers_only_migration_epoch == &old.headers_only_migration_epoch,
            ),
            (
                "the finalized frontier",
                frontiers.finalized == old.frontiers.finalized,
            ),
            (
                "the verified frontier",
                frontiers.verified_best == old.frontiers.verified_best,
            ),
            (
                "the oldest retained height",
                oldest_retained_height == &old.oldest_retained_height,
            ),
            (
                "the graph's finalized frontier",
                finalized_frontier == &before.graph.finalized_frontier,
            ),
            (
                "the body tombstones",
                consensus_invalid_body_tombstones
                    == &before.graph.consensus_invalid_body_tombstones,
            ),
            (
                "the verified projection",
                verified_projection == &before.verified_projection,
            ),
        ];
        for (field, same) in unchanged {
            if !same {
                found.push(format!("changed {field}"));
            }
        }
        found.extend(structure(graph));
        // A node held before and after changes only in its eligibility, which
        // the model below judges, and its auxiliary delivery IDs, Rule 11's.
        // Its validation stays too: only a `ReevaluateDeferred` event promotes
        // a held deferred header, the controlled port never sends one, and
        // re-admitting a held header keeps its node.
        let mut changed: Vec<String> = before
            .graph
            .nodes
            .iter()
            .filter(|(key, old)| {
                nodes.get(key).is_some_and(|new| {
                    let mut new = new.clone();
                    new.eligibility = old.eligibility.clone();
                    new.aux_delivery_ids = old.aux_delivery_ids.clone();
                    new != **old
                })
            })
            .map(|(key, _)| hash(*key))
            .collect();
        changed.sort_unstable();
        if !changed.is_empty() {
            found.push(format!(
                "changed held nodes {changed:?} in more than their eligibility"
            ));
        }
        // A new header is valid unless its time is more than two hours past
        // the run's clock, and then deferred until two hours before its time
        // (the protocol's full-validator time rule).
        let mut misdated: Vec<String> = nodes
            .iter()
            .filter(|(key, node)| {
                !before.graph.nodes.contains_key(key)
                    && node.validation != admitted(node.header.time, clock)
            })
            .map(|(key, _)| hash(*key))
            .collect();
        misdated.sort_unstable();
        if !misdated.is_empty() {
            found.push(format!(
                "inserted {misdated:?} with a validation the clock {clock} doesn't give"
            ));
        }
        // A header apply admits a header with no body knowledge.
        let mut known: Vec<String> = nodes
            .iter()
            .filter(|(key, node)| {
                !before.graph.nodes.contains_key(key)
                    && node.body_validation_state != BodyValidationState::Unknown
            })
            .map(|(key, _)| hash(*key))
            .collect();
        known.sort_unstable();
        if !known.is_empty() {
            found.push(format!("inserted {known:?} with body state no body gave"));
        }
        found.extend(reasons(before, nodes, reset));
        // Each node inherits ineligibility from its parent exactly when the
        // parent is ineligible. The finalized anchor inherits nothing.
        let mut inherits: Vec<String> = nodes
            .values()
            .filter(|node| {
                let parent = (node.hash != finalized_frontier.hash)
                    .then(|| nodes.get(&node.parent_hash))
                    .flatten();
                node.eligibility.inherited_from
                    != parent
                        .filter(|parent| !parent.is_eligible())
                        .map(|parent| parent.hash)
            })
            .map(|node| hash(node.hash))
            .collect();
        inherits.sort_unstable();
        if !inherits.is_empty() {
            found.push(format!(
                "left {inherits:?} inheriting from other than an ineligible parent"
            ));
        }
        // Eligible exactly when no held ancestor, itself included, stands
        // invalidated by a reset or waits on a deferred validation.
        let invalidated: HashSet<&str> = self.invalidated.iter().map(String::as_str).collect();
        let mut wrong: Vec<String> = nodes
            .values()
            .filter(|node| node.is_eligible() == excluded(node, nodes, &invalidated))
            .map(|node| hash(node.hash))
            .collect();
        wrong.sort_unstable();
        if !wrong.is_empty() {
            found.push(format!(
                "left {wrong:?} eligible though excluded, or ineligible though nothing \
                 excludes them"
            ));
        }
        // Ending at the engine's own selected frontier, with its own score, is
        // consistency, not fork choice, which is `expected-chain-selected`'s.
        let selected = frontiers.header_best;
        if selected_projection.last() != Some(&selected) {
            found.push(format!(
                "left the selected projection ending at {:?}, not the selected frontier \
                 {selected:?}",
                selected_projection.last()
            ));
        }
        // Integrated mode prefers the selected tip on equal work; headers-only
        // keeps no preference.
        if *preferred_header_tip
            != (*mode == zakura_header_chain::EngineMode::Integrated).then_some(selected)
        {
            found.push(format!(
                "left the preferred tip {preferred_header_tip:?}, not the selected frontier"
            ));
        }
        let score = nodes
            .get(&selected.hash)
            .zip(nodes.get(&finalized_frontier.hash))
            .and_then(|(tip, anchor)| {
                tip.work_coordinate()
                    .cumulative_work()
                    .checked_sub(anchor.work_coordinate().cumulative_work())
            });
        if header_best_score.tip_hash != selected.hash
            || Some(header_best_score.suffix_work.as_u256()) != score
        {
            found.push(format!(
                "left the selected score {header_best_score:?}, not the selected tip's work \
                 above the anchor"
            ));
        }
        if selected_projection.first() != Some(finalized_frontier) {
            found.push(format!(
                "left the selected projection starting at {:?}, not the finalized anchor \
                 {finalized_frontier:?}",
                selected_projection.first()
            ));
        }
        for pair in selected_projection.windows(2) {
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
    }

    /// The breaks found since the last call.
    pub(super) fn take_breaks(&mut self) -> Vec<String> {
        std::mem::take(&mut self.breaks)
    }
}

/// The ways `graph`'s nodes and indexes disagree with what its stored
/// headers alone give: each node's hash, parent, height and block work, and
/// the children, height and eligible-tip indexes rebuilt from the nodes.
fn structure(graph: &zakura_header_chain::GraphState) -> Vec<String> {
    let nodes = &graph.nodes;
    let mut wrong: Vec<String> = nodes
        .iter()
        .filter(|(key, node)| {
            let height = match nodes.get(&node.header.previous_block_hash) {
                Some(parent) => parent.height.next().ok(),
                // Only the finalized anchor's parent isn't held.
                None => (node.hash == graph.finalized_frontier.hash)
                    .then_some(graph.finalized_frontier.height),
            };
            // A node's cumulative work is its parent's plus its own. The
            // finalized anchor's is its trusted origin.
            let coordinate = match nodes.get(&node.header.previous_block_hash) {
                Some(parent) => parent.work_coordinate().checked_add(node.block_work).ok(),
                None => Some(node.work_coordinate()),
            };
            node.hash != **key
                || node.header.hash() != **key
                || node.parent_hash != node.header.previous_block_hash
                || Some(node.height) != height
                || Some(node.block_work) != node.header.difficulty_threshold.to_work()
                || Some(node.work_coordinate()) != coordinate
        })
        .map(|(key, _)| hash(*key))
        .collect();
    wrong.sort_unstable();
    let mut found = Vec::new();
    if !wrong.is_empty() {
        found.push(format!(
            "left {wrong:?} with a hash, parent, height, block work or cumulative work its \
             headers don't give"
        ));
    }
    let mut children: HashMap<block::Hash, HashSet<block::Hash>> = HashMap::new();
    let mut heights: HashMap<block::Height, HashSet<block::Hash>> = HashMap::new();
    for node in nodes.values() {
        // The finalized anchor's parent isn't held, so it isn't indexed.
        if nodes.contains_key(&node.parent_hash) {
            children
                .entry(node.parent_hash)
                .or_default()
                .insert(node.hash);
        }
        heights.entry(node.height).or_default().insert(node.hash);
    }
    let stored_children: HashMap<_, _> = graph
        .children
        .iter()
        .filter(|(_, kids)| !kids.is_empty())
        .map(|(parent, kids)| (*parent, kids.clone()))
        .collect();
    let stored_heights: HashMap<_, _> = graph
        .heights
        .iter()
        .filter(|(_, at)| !at.is_empty())
        .map(|(height, at)| (*height, at.clone()))
        .collect();
    if stored_children != children {
        found.push("left a children index its nodes don't give".to_owned());
    }
    if stored_heights != heights {
        found.push("left a height index its nodes don't give".to_owned());
    }
    let tips: HashSet<block::Hash> = nodes
        .values()
        .filter(|node| {
            node.is_eligible()
                && !children.get(&node.hash).is_some_and(|kids| {
                    kids.iter()
                        .any(|kid| nodes.get(kid).is_some_and(HeaderNode::is_eligible))
                })
        })
        .map(|node| node.hash)
        .collect();
    if graph.eligible_header_tips != tips {
        found.push("left an eligible-tip index its nodes don't give".to_owned());
    }
    found
}

/// The ways `nodes`' direct eligibility reasons differ from `before`'s
/// after a run whose reset, if any, is `reset`. Each node keeps its reasons,
/// and a new node has none, except the reset's target: an invalidation adds
/// exactly one operator reason, with the ID and digest
/// [`operator_identity`] gives the target, and a reconsideration removes
/// exactly that reason. An operator reason's evidence names the full-state
/// path, which the durable state doesn't hold, so it isn't judged.
fn reasons(
    before: &DurableState,
    nodes: &HashMap<block::Hash, HeaderNode>,
    reset: Option<&(bool, String)>,
) -> Vec<String> {
    let mut found = Vec::new();
    for (key, node) in nodes {
        let old = before
            .graph
            .nodes
            .get(key)
            .map(|old| old.eligibility.direct_reasons.clone())
            .unwrap_or_default();
        let new = &node.eligibility.direct_reasons;
        let target = reset.filter(|(_, target)| *target == hash(*key));
        let right = match target {
            None => *new == old,
            Some((invalidate, _)) => {
                let (id, digest) = operator_identity(*key);
                let operator = |reason: &EligibilityReason| {
                    matches!(
                        reason,
                        EligibilityReason::OperatorInvalid { id: held, reason_digest, .. }
                            if *held == id && *reason_digest == digest
                    )
                };
                if *invalidate {
                    let added: Vec<&EligibilityReason> = new.difference(&old).collect();
                    old.is_subset(new) && added.len() == 1 && operator(added[0])
                } else {
                    let removed: Vec<&EligibilityReason> = old.difference(new).collect();
                    new.is_subset(&old) && removed.len() == 1 && operator(removed[0])
                }
            }
        };
        if !right {
            found.push(format!(
                "left {} with direct reasons {new:?}, from {old:?}",
                hash(*key)
            ));
        }
    }
    found.sort_unstable();
    found
}

/// The operator invalidation ID and reason digest for `target`, derived on
/// its own as zakura-state's `operator_identity` defines them.
fn operator_identity(target: block::Hash) -> (OperatorInvalidationId, [u8; 32]) {
    let id_digest: [u8; 32] = Sha256::new()
        .chain_update(b"zakura-operator-invalidation-id-v1")
        .chain_update(target.0)
        .finalize()
        .into();
    let mut id = [0; 16];
    id.copy_from_slice(&id_digest[..16]);
    let id = OperatorInvalidationId::new(id);
    let digest = Sha256::new()
        .chain_update(b"zakura-operator-invalidation-v1")
        .chain_update(target.0)
        .chain_update(id.bytes())
        .finalize()
        .into();
    (id, digest)
}

/// The validation header admission gives a header timed `time` at `clock`:
/// `DeferredUntil` two hours before `time` when that's still ahead of
/// `clock`, else `Valid`, as the protocol's full-validator time rule sets it.
/// Header times are whole seconds.
fn admitted(time: DateTime<Utc>, clock: DateTime<Utc>) -> HeaderValidationState {
    const TWO_HOURS: i64 = 2 * 60 * 60;
    let deadline = DateTime::<Utc>::from_timestamp(time.timestamp().saturating_sub(TWO_HOURS), 0);
    match deadline {
        Some(deadline) if deadline > clock => HeaderValidationState::DeferredUntil(deadline),
        _ => HeaderValidationState::Valid,
    }
}

/// Whether `node` or one of its held ancestors in `nodes` is in
/// `invalidated` or waits on a deferred validation, either of which makes it
/// ineligible.
fn excluded(
    node: &HeaderNode,
    nodes: &HashMap<block::Hash, HeaderNode>,
    invalidated: &HashSet<&str>,
) -> bool {
    let mut at = Some(node);
    // Each step moves to a strictly lower height, so the walk ends.
    while let Some(node) = at {
        if invalidated.contains(hash(node.hash).as_str())
            || node.validation != HeaderValidationState::Valid
        {
            return true;
        }
        at = nodes
            .get(&node.header.previous_block_hash)
            .filter(|parent| parent.height < node.height);
    }
    false
}
