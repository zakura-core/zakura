//! Draft Stage 0 constants for the first draft of the memory budget.
//!
//! Every constant in this module stands in for a droplet measurement specified
//! in `03-delivery.md §1` (Stage 0). Once Stage 0 runs complete on the fleet,
//! the measured values replace these constants before the PR leaves draft.

use zakura_chain::{
    block::MAX_BLOCK_BYTES, parameters::checkpoint::constants::MAX_CHECKPOINT_BYTE_COUNT,
};

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;

/// Draft non-P2P baseline RSS (`base` in `B = (budget - base - sidecar) / ρ`).
///
/// Covers everything outside the v2 P2P ledger: RocksDB, non-finalized state,
/// verifiers, RPC, and the async/rayon runtimes. State reads are a fixed term
/// in the ledger instead (`02 §4`).
///
/// Source: conservative constant (~4.60 GiB) drawn from issue `#1970`'s ~4.6 GiB
/// sandblast peak. Replaced by Stage 0 (`03 §1`: Mainnet checkpoint sync to tip
/// plus 1 h at tip on 8, 16, and 32 GiB droplets).
pub const DRAFT_NON_P2P_BASE_BYTES: u64 = 4_710 * MIB;

/// Numerator of the draft allocator RSS-to-logical-bytes ratio `ρ = 6/5 = 1.20`.
///
/// Source: conservative constant above 1 matching the 15–25% allocator/fragmentation
/// overhead noted in `01-audit-and-ledger.md §3` item 9. Replaced by Stage 0
/// (`03 §1`: saturation run on a 16 GiB droplet).
pub const DRAFT_RHO_NUMERATOR: u64 = 6;

/// Denominator of the draft allocator RSS-to-logical-bytes ratio `ρ = 6/5 = 1.20`.
///
/// See [`DRAFT_RHO_NUMERATOR`] and `03 §1`.
pub const DRAFT_RHO_DENOMINATOR: u64 = 5;

/// Draft peak RSS reserved for a managed `zcashd` sidecar (`sidecar` in
/// `B = (budget - base - sidecar) / ρ`). Zero when managed `zcashd` is off.
///
/// Source: conservative constant (`4 GiB`) until Stage 0 (`03 §1`: one managed
/// `zcashd-compat` run) measures the `zcashd` process's peak RSS.
pub const DRAFT_MANAGED_ZCASHD_SIDECAR_BYTES: u64 = 4 * GIB;

/// Most bytes block-sync admission charges one maximal block before it decodes:
/// `wire + max(4 × wire, decoded)` (`block_sync/admission.rs`, whose
/// `DESERIALIZED_MEM_FACTOR` is the 4) at `MAX_BLOCK_BYTES`, or
/// `5 × 2,000,000 B`.
pub const ADMISSION_CHARGE_PER_MAX_BLOCK: u64 = 5 * MAX_BLOCK_BYTES;

/// Most bytes block-sync admission charges one checkpoint range before it
/// decodes (`decision-12`).
///
/// The checkpoint generator closes a range once its serialized bytes reach
/// `MAX_CHECKPOINT_BYTE_COUNT`, counting the block that crosses it, so a
/// range holds under `MAX_CHECKPOINT_BYTE_COUNT + MAX_BLOCK_BYTES`. At
/// admission's five bytes per wire byte that is
/// `(33,554,432 + 2,000,000) × 5 = 177,772,160 B` (~170 MiB). Block sync's
/// commit window covers one range (`block_sync::CommitWindow`).
pub const WORST_CHECKPOINT_RANGE_CHARGE_BYTES: u64 = (MAX_CHECKPOINT_BYTE_COUNT + MAX_BLOCK_BYTES)
    * (ADMISSION_CHARGE_PER_MAX_BLOCK / MAX_BLOCK_BYTES);

/// Breakdown of how the pure P2P budget `B` was derived from the host/configured
/// memory budget (`02 §2`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BudgetDerivation {
    /// Total process memory budget from `[memory] budget` or the host/cgroup probe.
    pub host_budget_bytes: u64,
    /// Non-P2P baseline subtracted (`base`).
    pub base_bytes: u64,
    /// Managed `zcashd` sidecar baseline subtracted (`sidecar`; 0 if disabled).
    pub sidecar_bytes: u64,
    /// Numerator of `ρ`.
    pub rho_numerator: u64,
    /// Denominator of `ρ`.
    pub rho_denominator: u64,
    /// Resulting logical-byte P2P budget `B = (budget - base - sidecar) / ρ`.
    pub p2p_budget_b: u64,
}

/// Derive the logical-byte P2P budget `B = (budget - base - sidecar) / ρ` using
/// pure integer arithmetic (`02 §2`, Stage 0 draft constants in `03 §1`).
pub const fn derive_p2p_budget(host_budget_bytes: u64, managed_zcashd: bool) -> BudgetDerivation {
    let sidecar_bytes = if managed_zcashd {
        DRAFT_MANAGED_ZCASHD_SIDECAR_BYTES
    } else {
        0
    };
    let net = host_budget_bytes
        .saturating_sub(DRAFT_NON_P2P_BASE_BYTES)
        .saturating_sub(sidecar_bytes);
    let scaled =
        (net as u128).saturating_mul(DRAFT_RHO_DENOMINATOR as u128) / (DRAFT_RHO_NUMERATOR as u128);
    let p2p_budget_b = if scaled > u64::MAX as u128 {
        u64::MAX
    } else {
        scaled as u64
    };
    BudgetDerivation {
        host_budget_bytes,
        base_bytes: DRAFT_NON_P2P_BASE_BYTES,
        sidecar_bytes,
        rho_numerator: DRAFT_RHO_NUMERATOR,
        rho_denominator: DRAFT_RHO_DENOMINATOR,
        p2p_budget_b,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The range charge follows admission's per-block charge (fix-62).
    #[test]
    fn the_range_charge_sums_admissions_charge() {
        use crate::zakura::block_sync::DESERIALIZED_MEM_FACTOR;

        assert_eq!(
            ADMISSION_CHARGE_PER_MAX_BLOCK,
            (1 + DESERIALIZED_MEM_FACTOR) * MAX_BLOCK_BYTES
        );
        assert_eq!(WORST_CHECKPOINT_RANGE_CHARGE_BYTES, 177_772_160);
    }
}
