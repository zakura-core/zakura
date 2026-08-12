//! Exhaustive startup audit and deterministic reconstruction planning.

mod contracts;
mod model;
mod reconstruction;
mod repair;
mod source_audit;

#[cfg(test)]
mod tests;

use chrono::{DateTime, Utc};

use crate::EngineConfig;

pub use contracts::{
    AuditViolation, RecoveryFailure, RecoveryPlan, RecoveryRepair, StoreAuditRead,
    ValidationContextRecord,
};

use model::load_store_image;
use reconstruction::derive_state;
use repair::classify_and_plan;
use source_audit::audit_authoritative;

/// Audit authoritative rows and derive only reconstructible repairs.
pub fn audit_store<S: StoreAuditRead>(
    store: &S,
    config: &EngineConfig,
) -> Result<RecoveryPlan, RecoveryFailure> {
    audit_store_at_with_policy(store, config, Utc::now(), false)
}

/// Audit authoritative rows using an injected consensus-local recovery time.
pub fn audit_store_at<S: StoreAuditRead>(
    store: &S,
    config: &EngineConfig,
    now: DateTime<Utc>,
) -> Result<RecoveryPlan, RecoveryFailure> {
    audit_store_at_with_policy(store, config, now, false)
}

/// Audit every authoritative row against the current configuration.
/// The audit plans an atomic trust-anchor-manifest rebind when only that digest differs.
///
/// The startup compatibility path permits release checkpoint extensions.
/// The path rejects mode, network, disk format, bootstrap origin, checkpoint, and source-row
/// mismatches.
pub fn audit_store_for_trust_anchor_update<S: StoreAuditRead>(
    store: &S,
    config: &EngineConfig,
) -> Result<RecoveryPlan, RecoveryFailure> {
    audit_store_at_with_policy(store, config, Utc::now(), true)
}

fn audit_store_at_with_policy<S: StoreAuditRead>(
    store: &S,
    config: &EngineConfig,
    now: DateTime<Utc>,
    allow_trust_anchor_update: bool,
) -> Result<RecoveryPlan, RecoveryFailure> {
    // Phase 1: load exhaustive durable rows
    let image = load_store_image(store, config, allow_trust_anchor_update)?;
    // Phase 2: fail closed on authoritative contradictions
    let audited = audit_authoritative(store, image, config, now)?;
    // Phase 3: reconstruct derived views from audited source
    let derived = derive_state(&audited, config, now)?;
    // Phase 4: classify reconstructible repairs and assemble the plan
    classify_and_plan(store, audited, derived, config)
}
