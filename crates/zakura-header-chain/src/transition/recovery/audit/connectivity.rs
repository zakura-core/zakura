//! Finalized-rooted connectivity checks.

use std::collections::HashSet;

use crate::{Frontier, HeaderNode};

use super::super::contracts::AuditViolation;

pub(super) fn check_finalized_connectivity(
    nodes: &[HeaderNode],
    finalized: Frontier,
    violations: &mut Vec<AuditViolation>,
) {
    let mut connected = HashSet::from([finalized.hash]);
    for node in nodes {
        if node.hash == finalized.hash {
            continue;
        }
        if connected.contains(&node.parent_hash) {
            connected.insert(node.hash);
        } else {
            violations.push(AuditViolation::ProtectedPath(node.hash));
        }
    }
}
