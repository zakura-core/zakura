//! Protected frontier retention leaf checks.

use crate::graph::HeaderGraphView;
use crate::PlanCandidate;

use super::super::InvariantViolation;

pub(crate) fn verify_protected<G: HeaderGraphView>(
    graph: &G,
    plan: &PlanCandidate,
) -> Result<(), InvariantViolation> {
    for frontier in [
        plan.change_set.metadata.frontiers.finalized,
        plan.change_set.metadata.frontiers.header_best,
        plan.change_set.metadata.frontiers.verified_best,
    ] {
        if graph.view_header_node(frontier.hash).is_none()
            || plan.change_set.delete_nodes.contains(&frontier.hash)
        {
            return Err(InvariantViolation::Protected(frontier.hash));
        }
    }
    Ok(())
}
