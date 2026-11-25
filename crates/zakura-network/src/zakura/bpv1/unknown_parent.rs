//! Checks on `HeaderMeta` whose parent header is unknown.
//!
//! Headerchain answers `MissingParent` after a hash lookup, before it checks
//! proof of work. Without these checks, a made-up header with an unknown
//! parent costs its sender only bytes. The checks need no parent, and they
//! run cheapest first:
//!
//! 1. The binding's coinbase txid reaches the header's Merkle root.
//! 2. The coinbase height (BIP 34) lies above `top` and at most
//!    [`PROPAGATION_DEPTH`] above it. Block sync owns every other height.
//! 3. The target is at most the highest target that difficulty adjustment
//!    allows that far above the best header ([`TargetBound`]).
//! 4. The header hash meets the target, and the Equihash solution is valid.
//!
//! A fake then costs about as much work as a real block that far ahead.
//! The order departs from `bpv1-10` (headerchain before the commitment) only
//! for metadata whose parent is missing.

use zakura_chain::{
    block::{Header, Height},
    parameters::{Network, NetworkUpgrade, MAX_POW_AVERAGING_WINDOW},
    transparent,
    work::difficulty::{ExpandedDifficulty, ParameterDifficulty as _, U256},
};
use zakura_header_chain::{
    validate_compact_target, validate_hash_filter, PowPolicy, ValidationLease,
};

use super::{
    blocks::MetaEnvelope, close::Bpv1CloseRow, commitment::verify_txid_path,
    wire::PROPAGATION_DEPTH,
};

/// `PoWMaxAdjustDown * 100` in the Zcash specification: one block's target is
/// at most this many percent above its averaging window's mean target.
const MAX_ADJUST_DOWN_PERCENT: u64 = 32;

/// The outcome of checking metadata whose parent header is unknown.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum UnknownParentVerdict {
    /// Every check passed: the metadata waits for its parent.
    Park,
    /// The block's height lies where block sync owns it. The sender is not at
    /// fault.
    OutOfRange,
    /// A check failed, and the sender's session closes with this row.
    ///
    /// A binding that does not match its header closes with row 4. A target or
    /// proof of work that fails closes with row 3.
    Invalid(Bpv1CloseRow),
}

/// The highest target that difficulty adjustment allows above the best header.
///
/// A block's target is at most `1 + PoWMaxAdjustDown` times the mean target of
/// its averaging window, so a header `k` blocks above the best header has a
/// target at most `1.32^k` times the highest target in the best header's
/// window. The bound reads the widest window that any upgrade uses, so it holds
/// across the NU7 change of `PoWAveragingWindow` (ZIP 218).
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TargetBound {
    /// Height of the best header.
    pub best_height: u32,
    /// Highest target among the best header and the predecessors in its
    /// widest averaging window.
    pub max_target: ExpandedDifficulty,
}

impl TargetBound {
    /// Reads the bound from the validation lease for a child of the best
    /// header.
    ///
    /// Returns `None` while the chain is no longer than the widest averaging
    /// window, where `ThresholdBits` is the proof-of-work limit, or when a
    /// lease target does not expand.
    pub fn from_lease(lease: &ValidationLease) -> Option<Self> {
        let best_height = lease.parent().height.0;
        if usize::try_from(best_height).ok()? < MAX_POW_AVERAGING_WINDOW {
            return None;
        }
        let mut max_target = None;
        for fact in lease.predecessors().get(..MAX_POW_AVERAGING_WINDOW)? {
            let target = fact.header.difficulty_threshold.to_expanded()?;
            max_target = max_target.max(Some(target));
        }
        Some(Self {
            best_height,
            max_target: max_target?,
        })
    }

    /// The highest target allowed at `height`, capped at `pow_limit`, or
    /// `None` at or below the best header.
    pub fn limit_at(
        &self,
        height: u32,
        pow_limit: ExpandedDifficulty,
    ) -> Option<ExpandedDifficulty> {
        let steps = height.checked_sub(self.best_height).filter(|&k| k > 0)?;
        let limit = U256::from(pow_limit);
        let mut bound = U256::from(self.max_target);
        let hundred = U256::from(100u64);
        let percent = U256::from(MAX_ADJUST_DOWN_PERCENT);
        // Each step computes `floor(bound * 1.32)` exactly as
        // `bound + floor(bound * 32 / 100)`, split so no product overflows. The
        // floor keeps the bound at or above every integer target the rule
        // allows. The bound grows by about a third each step, so the loop
        // reaches the limit within a few hundred steps.
        for _ in 0..steps {
            let growth = (bound / hundred) * percent + (bound % hundred) * percent / hundred;
            let next = bound.saturating_add(growth);
            if next == bound {
                break;
            }
            bound = next;
            if bound >= limit {
                break;
            }
        }
        Some(ExpandedDifficulty::from(bound.min(limit)))
    }
}

/// Whether check 3 applies at `height`.
///
/// The Testnet minimum-difficulty rule allows the proof-of-work limit after a
/// long gap between blocks, so no bound holds where that rule is active.
pub fn target_bound_applies(network: &Network, height: u32) -> bool {
    NetworkUpgrade::minimum_difficulty_spacing_for_height(network, Height(height)).is_none()
}

/// Runs checks 1 and 2 on `header` and `envelope`'s binding.
///
/// Returns the coinbase height when both pass, else the verdict that ends the
/// check. Both checks are cheap.
pub fn check_binding_and_height(
    header: &Header,
    envelope: &MetaEnvelope,
    top: u32,
) -> Result<u32, UnknownParentVerdict> {
    let invalid_binding = UnknownParentVerdict::Invalid(Bpv1CloseRow::Row4BadMetaOrCommitment);
    let coinbase = verify_txid_path(header, &envelope.binding, envelope.coding.tx_count)
        .map_err(|_| invalid_binding)?;
    let height = match coinbase.inputs().first() {
        Some(transparent::Input::Coinbase { height, .. }) => height.0,
        _ => return Err(invalid_binding),
    };
    if height <= top || height > top.saturating_add(PROPAGATION_DEPTH) {
        return Err(UnknownParentVerdict::OutOfRange);
    }
    Ok(height)
}

/// Runs checks 3 and 4 on `header` at the coinbase `height`.
///
/// `bound` is `None` when headerchain has no context for check 3; checks 1, 2
/// and 4 still bound what a fake costs. Check 4 verifies an Equihash solution,
/// so callers run this off the driver.
pub fn check_proof_of_work(
    network: &Network,
    header: &Header,
    height: u32,
    bound: Option<TargetBound>,
) -> UnknownParentVerdict {
    let invalid_header = UnknownParentVerdict::Invalid(Bpv1CloseRow::Row3InvalidHeader);
    let Ok(target) = validate_compact_target(header, network) else {
        return invalid_header;
    };
    // A best header at or above `height` bounds nothing. Header sync moved it
    // there during the checks, so check 4 decides, and the table retries the
    // admission that may now find the header.
    let limit = bound
        .filter(|_| target_bound_applies(network, height))
        .and_then(|bound| bound.limit_at(height, network.target_difficulty_limit()));
    if limit.is_some_and(|limit| target > limit) {
        return invalid_header;
    }
    // A network that cannot waive proof of work validates it, as headerchain does.
    let policy = PowPolicy::for_network(network).unwrap_or_else(|_| PowPolicy::validating(network));
    if !policy.is_authenticated_custom_waiver()
        && validate_hash_filter(header.hash(), target).is_err()
    {
        return invalid_header;
    }
    if policy.validate_solution(header).is_err() {
        return invalid_header;
    }
    UnknownParentVerdict::Park
}
