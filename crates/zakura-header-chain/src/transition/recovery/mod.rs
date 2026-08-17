//! Exhaustive startup audit and deterministic reconstruction planning.

mod audit;
mod contracts;
mod phases;
mod reconstruction;
mod repair;

#[cfg(test)]
mod tests;

use chrono::{DateTime, Utc};

use crate::EngineConfig;

pub use contracts::{
    AuditViolation, RecoveryFailure, RecoveryPlan, RecoveryRepair, StoreAuditRead,
    StoreAuditSnapshot, ValidationContextRecord,
};

use audit::audit_authoritative;
use phases::load_pre_audit_store_rows;
use reconstruction::reconstruct_derived_views;
use repair::classify_and_plan;

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
    let snapshot = store.audit_snapshot()?;
    // Phase 1: load exhaustive durable rows
    let rows = load_pre_audit_store_rows(&snapshot, config, allow_trust_anchor_update)?;
    // Phase 2: fail closed on authoritative contradictions
    let audited = audit_authoritative(&snapshot, rows, config, now)?;
    // Phase 3: reconstruct derived views from audited source
    let derived = reconstruct_derived_views(&audited, config, now)?;
    // Phase 4: classify reconstructible repairs and assemble the plan
    classify_and_plan(&snapshot, audited, derived, config)
}
