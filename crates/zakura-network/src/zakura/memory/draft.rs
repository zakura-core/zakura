//! Draft Stage 0 constants for the first draft of the memory budget.
//!
//! Every constant in this module stands in for a droplet measurement specified
//! in `03-delivery.md §1` (Stage 0). Once Stage 0 runs complete on the fleet,
//! the measured values replace these constants before the PR leaves draft.

use zakura_chain::{
    block::{Height, MAX_BLOCK_BYTES},
    parameters::{Network, NetworkKind},
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
const ADMISSION_CHARGE_PER_MAX_BLOCK: u64 = 5 * MAX_BLOCK_BYTES;

/// Draft worst 401-block checkpoint window charge on Mainnet (`decision-12`).
///
/// Admission's charge summed over 401 maximal blocks:
/// `401 × 5 × 2,000,000 = 4,010,000,000 B` (~3.73 GiB), the most any 401
/// blocks can charge before they decode.
///
/// Source: conservative bound; replaced by Stage 0 (`03 §1`: offline scan of
/// the Mainnet chain, summing admission's per-block charge).
pub const DRAFT_WORST_CHECKPOINT_WINDOW_BYTES_MAINNET: u64 = 401 * ADMISSION_CHARGE_PER_MAX_BLOCK;

/// Draft worst 401-block checkpoint window charge on Testnet (`decision-12`).
///
/// Testnet has the same block size limit as Mainnet, and its checkpoints
/// cover blocks up to that limit, so the unmeasured bound equals Mainnet's.
///
/// Source: conservative bound; replaced by Stage 0 (`03 §1`: offline scan of
/// the Testnet chain, summing admission's per-block charge).
pub const DRAFT_WORST_CHECKPOINT_WINDOW_BYTES_TESTNET: u64 = 401 * ADMISSION_CHARGE_PER_MAX_BLOCK;

/// Draft worst 401-block checkpoint window charge on Regtest (`decision-12`).
///
/// Regtest has no checkpoint beyond genesis, so it never verifies a
/// checkpoint window. The consensus reserve then covers the semantic commit
/// window alone.
///
/// Source: Regtest's checkpoint list (`03 §1` measures only the networks with
/// checkpoints).
pub const DRAFT_WORST_CHECKPOINT_WINDOW_BYTES_REGTEST: u64 = 0;

/// Draft worst 401-block checkpoint window charge on `network` (`decision-12`,
/// `03 §1`).
///
/// A network whose checkpoint list ends at genesis, such as a configured
/// test network without checkpoints, has no checkpoint window.
pub fn draft_worst_checkpoint_window_bytes(network: &Network) -> u64 {
    if network.checkpoint_list().max_height() == Height(0) {
        return DRAFT_WORST_CHECKPOINT_WINDOW_BYTES_REGTEST;
    }
    match network.kind() {
        NetworkKind::Mainnet => DRAFT_WORST_CHECKPOINT_WINDOW_BYTES_MAINNET,
        NetworkKind::Testnet => DRAFT_WORST_CHECKPOINT_WINDOW_BYTES_TESTNET,
        NetworkKind::Regtest => DRAFT_WORST_CHECKPOINT_WINDOW_BYTES_REGTEST,
    }
}

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

/// Convenience helper returning only `B` from [`derive_p2p_budget`].
pub const fn resolved_p2p_budget_bytes(host_budget_bytes: u64, managed_zcashd: bool) -> u64 {
    derive_p2p_budget(host_budget_bytes, managed_zcashd).p2p_budget_b
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The draft window follows admission's per-block charge (fix-62).
    #[test]
    fn the_draft_window_sums_admissions_charge() {
        use crate::zakura::block_sync::DESERIALIZED_MEM_FACTOR;

        assert_eq!(
            ADMISSION_CHARGE_PER_MAX_BLOCK,
            (1 + DESERIALIZED_MEM_FACTOR) * MAX_BLOCK_BYTES
        );
        assert_eq!(DRAFT_WORST_CHECKPOINT_WINDOW_BYTES_MAINNET, 4_010_000_000);
    }
}
