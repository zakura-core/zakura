//! Draft Stage 0 constants for the first draft of the memory budget.
//!
//! Every constant in this module stands in for a droplet measurement specified
//! in `03-delivery.md §1` (Stage 0). Once Stage 0 runs complete on the fleet,
//! the measured values replace these constants before the PR leaves draft.

use zakura_chain::block::MAX_BLOCK_BYTES;

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;

/// Draft non-P2P baseline RSS (`base` in `B = (budget - base - sidecar) / ρ`).
///
/// Covers everything outside the v2 P2P ledger: RocksDB, non-finalized state,
/// state reads (`ReadMemoryBudget`), verifiers, RPC, and the async/rayon
/// runtimes.
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

/// Draft worst 401-block checkpoint window charge (`decision-12`).
///
/// Computed as `401 × max(4 × wire, decoded)` at `MAX_BLOCK_BYTES` (`2,000,000 B`):
/// `401 × 4 × 2,000,000 = 3,208,000,000 B` (~2.99 GiB).
///
/// Source: `goal-impl.md` Draft values table; replaced by Stage 0 (`03 §1`:
/// offline scan of the real Mainnet and Testnet chains).
pub const DRAFT_WORST_CHECKPOINT_WINDOW_BYTES: u64 = 401 * 4 * (MAX_BLOCK_BYTES as u64);

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
