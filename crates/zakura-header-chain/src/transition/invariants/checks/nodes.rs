//! Node hash, parent, work, and eligibility leaf checks.

use crate::graph::HeaderGraphView;
use crate::HeaderNode;
use zakura_chain::block;

use super::super::InvariantViolation;

pub(crate) fn verify_node<G: HeaderGraphView>(
    graph: &G,
    node: &HeaderNode,
    work_origin: block::Hash,
) -> Result<(), InvariantViolation> {
    if node.header.hash() != node.hash {
        return Err(InvariantViolation::NodeHash(node.hash));
    }
    if !graph
        .view_header_hashes_at_height(node.height)
        .contains(&node.hash)
    {
        return Err(InvariantViolation::Index(node.hash));
    }
    if node.work_coordinate().origin_hash() != work_origin {
        return Err(InvariantViolation::Work(node.hash));
    }
    if node.hash == graph.view_finalized_frontier().hash {
        if node.eligibility.inherited_from.is_some() {
            return Err(InvariantViolation::Eligibility(node.hash));
        }
        return Ok(());
    }
    let parent = graph
        .view_header_node(node.parent_hash)
        .ok_or(InvariantViolation::Parent(node.hash))?;
    if parent.height.next().ok() != Some(node.height)
        || !graph.view_header_children(parent.hash).contains(&node.hash)
    {
        return Err(InvariantViolation::Parent(node.hash));
    }
    if parent.work_coordinate().checked_add(node.block_work).ok() != Some(node.work_coordinate()) {
        return Err(InvariantViolation::Work(node.hash));
    }
    if node.eligibility.inherited_from != (!parent.is_eligible()).then_some(parent.hash) {
        return Err(InvariantViolation::Eligibility(node.hash));
    }
    Ok(())
}
