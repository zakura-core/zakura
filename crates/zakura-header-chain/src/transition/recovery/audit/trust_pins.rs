//! Settled and checkpoint trust-pin eligibility checks.

use sha2::Digest as _;

use crate::{BodyValidationState, EligibilityReason, EngineConfig, Frontier, HeaderNode};

use super::super::contracts::AuditViolation;

pub(super) fn check_trust_pins(
    nodes: &[HeaderNode],
    finalized: Frontier,
    config: &EngineConfig,
    violations: &mut Vec<AuditViolation>,
) {
    let settled = config.settled_manifest().pin_for_network(&config.network);
    for node in nodes {
        for reason in &node.eligibility.direct_reasons {
            let valid = match reason {
                EligibilityReason::SettledUpgradeConflict { height, expected } => settled
                    .is_some_and(|pin| {
                        *height == node.height
                            && pin.activation == Frontier::new(*height, *expected)
                            && node.hash != *expected
                    }),
                EligibilityReason::CheckpointConflict { height, expected } => config
                    .local_checkpoints()
                    .hash(*height)
                    .is_some_and(|configured| {
                        configured == *expected && *height == node.height && node.hash != *expected
                    }),
                EligibilityReason::FinalityConflict {
                    finalized: reason_finalized,
                } => {
                    *reason_finalized == finalized
                        && node.height <= reason_finalized.height
                        && node.hash != reason_finalized.hash
                }
                EligibilityReason::ConsensusBodyInvalid { evidence, rule } => matches!(
                    &node.body_validation_state,
                    BodyValidationState::ConsensusInvalid {
                        evidence: body_evidence,
                        rule: body_rule,
                    } if body_evidence == evidence && body_rule == rule
                ),
                EligibilityReason::OperatorInvalid {
                    id, reason_digest, ..
                } => {
                    let mut hasher = sha2::Sha256::new();
                    hasher.update(b"zakura-operator-invalidation-v1");
                    hasher.update(node.hash.0);
                    hasher.update(id.bytes());
                    let digest: [u8; 32] = hasher.finalize().into();
                    *reason_digest == digest
                }
            };
            if !valid {
                violations.push(AuditViolation::EligibilityRoot(node.hash));
            }
        }
        let expected = if settled.is_some_and(|pin| pin.activation.height == node.height) {
            settled.map(|pin| (pin.activation.hash, true))
        } else {
            config
                .local_checkpoints()
                .hash(node.height)
                .map(|hash| (hash, false))
        };
        let Some((expected, settled_reason)) = expected else {
            continue;
        };
        let reason = node
            .eligibility
            .direct_reasons
            .iter()
            .any(|reason| match reason {
                EligibilityReason::SettledUpgradeConflict {
                    height,
                    expected: hash,
                } if settled_reason => *height == node.height && *hash == expected,
                EligibilityReason::CheckpointConflict {
                    height,
                    expected: hash,
                } if !settled_reason => *height == node.height && *hash == expected,
                _ => false,
            });
        if (node.hash == expected && reason) || (node.hash != expected && !reason) {
            violations.push(AuditViolation::TrustPin(node.height, node.hash));
        }
    }
}
