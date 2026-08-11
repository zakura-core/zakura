//! Header-time authentication for peer-supplied verified-commitment-trees (vct) metadata.
//!
//! The committer authenticates the auxiliary delivery it is about to fold, in the same
//! RocksDB batch as the finalized block. That is the trust boundary and this module does not
//! move it. What it adds is *timing*: during fast sync the header chain runs far ahead of the
//! bodies, so a wrong root sits in the DAG unnoticed until the committer finally reaches its
//! height. This sweep walks the selected path as headers arrive, authenticates each delivery
//! against the one-header-later commitment that already proves it, and evicts a delivery that
//! fails — arming metadata repair immediately instead of at commit.
//!
//! The cryptographic kernel is [`verify_supplied_roots_from_parts`], written by Roman Akhtariev
//! for `main`'s ahead-of-body header-root authentication lane (zakura#346, #352, #455). It is
//! reused unchanged; this module only supplies the fork-aware inputs `main` did not have and
//! records the verdict as header-chain auxiliary evidence instead of a height-keyed disk row.

use std::cmp::Ordering;

use zakura_chain::{
    block::{Commitment, CommitmentError, Height},
    history_tree::HistoryTree,
    parallel::{
        commitment_aux::BlockCommitmentRoots,
        commitment_aux_verify::{verify_supplied_roots_from_parts, SuppliedRootsError},
    },
    parameters::{Network, NetworkUpgrade},
};
use zakura_header_chain::{ApplyResult, AuxDelivery, Frontier};

use crate::{
    error::VctCommitFailure,
    service::{
        finalized_state::{
            FinalizedState, NextVctBlock, VctAuthenticationProof, VctAuxRejection, VctAuxWindow,
        },
        write::{vct_write::VctWriteManager, HeaderChainWriter, VctAuxWindowRead},
    },
};

/// How many selected heights one sweep may authenticate before yielding the state writer.
///
/// A sweep runs on the thread that also commits blocks, so it yields after a bounded number of
/// heights and the next sweep picks up where it stopped. The bound is a height count rather
/// than a deadline so the work one sweep does is a function of the chain, not of machine load.
/// Roughly one large header batch, which is what one sweep is normally asked to cover.
const MAX_HEIGHTS_PER_SWEEP: u32 = 2_048;

/// A contiguous run of selected heights whose auxiliary roots are verified and folded.
struct VerifiedRun {
    /// Highest selected header whose delivery is verified and folded into `history_tree`.
    frontier: Frontier,
    /// ZIP-221 MMR folded through `frontier`, positioned exactly as `frontier`'s successor
    /// needs it.
    history_tree: HistoryTree,
}

/// Authenticates selected auxiliary deliveries as their successor headers arrive.
#[derive(Default)]
pub(super) struct VctAuthSweeper {
    /// Verified prefix above the committed body tip, absent until the first sweep anchors.
    verified: Option<VerifiedRun>,
    /// Height whose delivery this sweep evicted and has not yet re-verified.
    evicted: Option<Height>,
}

impl VctAuthSweeper {
    /// Authenticate a bounded run of selected deliveries above the committed body tip.
    ///
    /// Every step is best-effort: an unreadable window, an absent delivery, or a stale
    /// transition stops this sweep without advancing, and the next one retries. The committer
    /// re-verifies whatever it commits regardless, so a sweep that never runs is only slower,
    /// never wrong.
    pub(super) fn sweep(
        &mut self,
        finalized_state: &FinalizedState,
        writer: &HeaderChainWriter,
        repair: &mut VctWriteManager,
    ) {
        let network = finalized_state.network();
        let Some((body_tip, body_tip_hash)) = finalized_state.db.tip() else {
            self.forget(repair);
            return;
        };
        // Outside the fast path the committer rebuilds every note-commitment tree from bodies,
        // so peer metadata carries no authority and there is nothing to authenticate early.
        let Ok(first) = body_tip.next() else {
            self.forget(repair);
            return;
        };
        if !finalized_state.vct_requires_exact_roots(first) {
            self.forget(repair);
            return;
        }

        let reader = writer.runtime.reader();
        // The run stays usable while its frontier is still the selected header at its height:
        // the selected path is a chain, so a still-selected frontier proves its whole verified
        // ancestry is still selected. Anything else — the committer passing the frontier, or a
        // reorg under it — rebuilds from the committed tip.
        let anchored = self.verified.as_ref().is_some_and(|run| {
            run.frontier.height >= body_tip
                && reader.selected_hash(run.frontier.height).ok().flatten()
                    == Some(run.frontier.hash)
        });
        if !anchored && !self.anchor(finalized_state, &reader, &network, body_tip, body_tip_hash) {
            self.forget(repair);
            return;
        }

        let mut run = self
            .verified
            .take()
            .expect("an unanchored sweep returned above");
        let mut budget = MAX_HEIGHTS_PER_SWEEP;
        let mut cursor = run.frontier.height.next().ok().and_then(|height| {
            reader
                .selected_hash(height)
                .ok()
                .flatten()
                .map(|hash| (height, hash))
        });

        while let Some((height, hash)) = cursor {
            if budget == 0 {
                break;
            }
            budget -= 1;
            if !finalized_state.vct_requires_exact_roots(height) {
                break;
            }
            // A misplaced running tree would panic the fold, and this runs on the thread that
            // commits blocks. Re-check the invariant the anchor established rather than trust
            // it across an arbitrary number of transitions.
            if !fold_is_safe(&network, height, &run.history_tree) {
                tracing::warn!(
                    ?height,
                    "VCT: header-time authentication stopped on a misplaced running history tree"
                );
                break;
            }

            let window = match writer.vct_aux_window(height, hash) {
                Ok(VctAuxWindowRead::Ready(window)) => *window,
                // No selected delivery carries roots for this height yet. The committer's own
                // stall path already asks for a re-delivery when it reaches the hole.
                Ok(VctAuxWindowRead::Missing { .. }) => break,
                Err(error) => {
                    tracing::warn!(
                        ?error,
                        ?height,
                        "VCT: header-time authentication stopped on an incoherent auxiliary read"
                    );
                    break;
                }
            };
            // Without the successor header there is no commitment that proves these roots.
            let Some(successor) = window.successor.clone() else {
                break;
            };
            let (Some(current_roots), Some(successor_roots)) = (
                supplied_roots(&window.current),
                successor.delivery.as_ref().and_then(supplied_roots),
            ) else {
                break;
            };

            match verify_supplied_roots_from_parts(
                &network,
                run.history_tree.clone(),
                [
                    (window.current_header.as_ref(), &current_roots),
                    (successor.header.as_ref(), &successor_roots),
                ],
            ) {
                Ok(verified) => {
                    if !promote(writer, &network, &window, &successor) {
                        break;
                    }
                    run.history_tree = verified.history_tree().clone();
                    run.frontier = Frontier::new(height, hash);
                    metrics::counter!("state.vct.aux.sweep.authenticated.count").increment(1);
                    cursor = Some((successor.height, successor.hash));
                }
                Err((failed_height, error)) => {
                    self.evict(writer, repair, &window, &successor, failed_height, &error);
                    break;
                }
            }
        }

        if self
            .evicted
            .is_some_and(|evicted| evicted <= run.frontier.height)
        {
            self.clear_eviction(repair);
        }
        metrics::gauge!("state.vct.aux.sweep.frontier.height")
            .set(f64::from(run.frontier.height.0));
        self.verified = Some(run);
    }

    /// Start a new verified run at the committed body tip, or report that it is not usable.
    fn anchor(
        &mut self,
        finalized_state: &FinalizedState,
        reader: &crate::service::finalized_state::header_chain::HeaderChainReader,
        network: &Network,
        body_tip: Height,
        body_tip_hash: zakura_chain::block::Hash,
    ) -> bool {
        if reader.selected_hash(body_tip).ok().flatten() != Some(body_tip_hash) {
            // The selected projection has not caught up with the committed body tip, so there
            // is no branch to sweep yet.
            return false;
        }
        let history_tree = match finalized_state.db.try_history_tree() {
            Ok(history_tree) => (*history_tree).clone(),
            Err(error) => {
                tracing::warn!(
                    ?error,
                    "VCT: header-time authentication could not read the committed history tree"
                );
                return false;
            }
        };
        let Ok(first) = body_tip.next() else {
            return false;
        };
        if !fold_is_safe(network, first, &history_tree) {
            tracing::warn!(
                ?body_tip,
                "VCT: header-time authentication found the committed history tree misplaced"
            );
            return false;
        }

        self.verified = Some(VerifiedRun {
            frontier: Frontier::new(body_tip, body_tip_hash),
            history_tree,
        });
        true
    }

    /// Record one failed delivery, mark it rejected, and ask for replacement metadata now.
    fn evict(
        &mut self,
        writer: &HeaderChainWriter,
        repair: &mut VctWriteManager,
        window: &VctAuxWindow,
        successor: &NextVctBlock,
        failed_height: Height,
        error: &SuppliedRootsError,
    ) {
        let height = window
            .current
            .tree_aux
            .map_or(failed_height, |aux| aux.height);
        let Some((failure, rejection)) =
            attribute(window, height, successor.height, failed_height, error)
        else {
            tracing::warn!(
                ?height,
                ?failed_height,
                %error,
                "VCT: header-time authentication failed without an attributable delivery"
            );
            return;
        };
        let attribution = match rejection {
            VctAuxRejection::Current => "current",
            VctAuxRejection::Successor => "successor",
            VctAuxRejection::Ambiguous => "ambiguous",
            VctAuxRejection::None => "none",
        };
        metrics::counter!(
            "state.vct.aux.sweep.rejected.count",
            "attribution" => attribution
        )
        .increment(1);
        tracing::warn!(
            ?height,
            ?failed_height,
            attribution,
            %error,
            "VCT: header-time authentication rejected supplied auxiliary metadata"
        );

        match writer.reject_vct_aux(window, rejection, failure) {
            Ok(Some(ApplyResult::Committed | ApplyResult::NoChange(_))) => {
                let evicted = match rejection {
                    // An ambiguous boundary rejects both deliveries, and repair restarts at
                    // the lower of the two.
                    VctAuxRejection::Current | VctAuxRejection::Ambiguous => height,
                    VctAuxRejection::Successor => successor.height,
                    VctAuxRejection::None => return,
                };
                self.evicted = Some(evicted);
                repair.request_sweep_repair(evicted);
            }
            Ok(Some(result)) => {
                tracing::debug!(?result, "VCT: header-time rejection did not commit");
            }
            Ok(None) => {}
            Err(error) => {
                tracing::error!(
                    ?error,
                    "VCT: header-time authentication could not persist a rejection"
                );
            }
        }
    }

    /// Drop the verified run and any repair need it owns.
    fn forget(&mut self, repair: &mut VctWriteManager) {
        self.verified = None;
        self.clear_eviction(repair);
    }

    fn clear_eviction(&mut self, repair: &mut VctWriteManager) {
        if self.evicted.take().is_some() {
            repair.clear_sweep_repair();
        }
    }
}

/// Record one verified delivery as authenticated by its exact one-header-later boundary.
///
/// Returns whether the sweep may advance past this height. Advancing without a durable
/// authentication mark would leave the delivery replaceable by a later lower-identity
/// delivery, which would silently invalidate the roots already folded into the running tree.
fn promote(
    writer: &HeaderChainWriter,
    network: &Network,
    window: &VctAuxWindow,
    successor: &NextVctBlock,
) -> bool {
    // Below Heartwood no successor commitment authenticates a history root, so there is no
    // boundary to record. Those roots are pinned directly by their own header and never enter
    // the MMR, so an unauthenticated delivery there cannot change what the tree folds.
    let authenticates_history = matches!(
        successor.header.commitment(network, successor.height),
        Ok(Commitment::ChainHistoryRoot(_) | Commitment::ChainHistoryBlockTxAuthCommitment(_))
    );
    let Some(boundary_auth_data_root) = successor.auth_data_root.filter(|_| authenticates_history)
    else {
        return true;
    };

    let proof = VctAuthenticationProof::Successor {
        current_delivery_id: window.current.delivery_id,
        current_header_hash: window.current.header_hash,
        boundary_hash: successor.hash,
        boundary_auth_data_root,
    };
    match writer.authenticate_vct_aux(window, proof) {
        // `None` means the delivery is already authenticated, which pins selection just as
        // well as a fresh mark.
        Ok(None) | Ok(Some(ApplyResult::Committed | ApplyResult::NoChange(_))) => true,
        Ok(Some(result)) => {
            tracing::debug!(
                ?result,
                "VCT: header-time authentication did not commit; retrying on the next sweep"
            );
            false
        }
        Err(error) => {
            tracing::warn!(
                ?error,
                "VCT: header-time authentication could not persist a promotion"
            );
            false
        }
    }
}

/// Attribute a sweep verification failure to the exact delivery that can be blamed for it.
///
/// Returns `None` when no auxiliary delivery is provably at fault, which leaves both
/// deliveries alone: a wrong rejection is permanent and would evict metadata that no peer
/// needs to replace.
fn attribute(
    window: &VctAuxWindow,
    height: Height,
    successor_height: Height,
    failed_height: Height,
    error: &SuppliedRootsError,
) -> Option<(VctCommitFailure, VctAuxRejection)> {
    if failed_height == height {
        // The tree folded so far is already confirmed by this header's predecessor, so the
        // only inputs left at this height come from the current delivery.
        return match error {
            // This check reads no delivery field: it re-proves the parent roots, which were
            // confirmed one step earlier. A mismatch means the run itself is inconsistent.
            SuppliedRootsError::InvalidHeaderCommitment(
                CommitmentError::InvalidChainHistoryRoot { .. },
            )
            | SuppliedRootsError::MissingHistoryTreeRoot => None,
            SuppliedRootsError::InvalidHeaderCommitment(error) if !blames_delivery(error) => None,
            _ => {
                let failure = VctCommitFailure::CurrentRoots;
                Some((failure, window.classify_failure(failure)))
            }
        };
    }
    if failed_height != successor_height {
        return None;
    }

    match error {
        // The boundary commitment mixes the current delivery's folded roots with the
        // successor's authorizing-data root and cannot separate them, which is exactly the
        // ambiguity the committer already resolves by rejecting both.
        SuppliedRootsError::InvalidHeaderCommitment(
            CommitmentError::InvalidChainHistoryRoot { .. }
            | CommitmentError::InvalidChainHistoryBlockTxAuthCommitment { .. },
        ) => {
            let failure = VctCommitFailure::SuccessorBoundary;
            Some((failure, window.classify_failure(failure)))
        }
        // Every other successor failure is a pre-activation pin on the successor's own
        // fields, which no other delivery can influence. The committer never sees this case
        // because its successor item carries no roots.
        SuppliedRootsError::InvalidHeaderCommitment(error) if blames_delivery(error) => {
            let unauthenticated = successor_delivery_is_unauthenticated(window);
            unauthenticated.then_some((
                VctCommitFailure::SuccessorBoundary,
                VctAuxRejection::Successor,
            ))
        }
        _ => None,
    }
}

/// Whether a commitment failure is caused only by the supplied auxiliary fields.
///
/// The remaining variants report a malformed or unparsable header commitment, which the
/// header chain validated before retaining the header and no delivery can cause.
fn blames_delivery(error: &CommitmentError) -> bool {
    matches!(
        error,
        CommitmentError::InvalidFinalSaplingRoot { .. }
            | CommitmentError::InvalidPreSaplingSaplingTxCount { .. }
            | CommitmentError::InvalidPreNu5OrchardRoot { .. }
            | CommitmentError::InvalidPreNu5OrchardTxCount { .. }
            | CommitmentError::InvalidPreNu6_3IronwoodRoot { .. }
            | CommitmentError::InvalidPreNu6_3IronwoodTxCount { .. }
            | CommitmentError::InvalidChainHistoryBlockTxAuthCommitment { .. }
    )
}

fn successor_delivery_is_unauthenticated(window: &VctAuxWindow) -> bool {
    window
        .successor
        .as_ref()
        .and_then(|successor| successor.delivery)
        .is_some_and(|delivery| {
            delivery.authentication == zakura_header_chain::AuxAuthentication::Unauthenticated
        })
}

fn supplied_roots(delivery: &AuxDelivery) -> Option<BlockCommitmentRoots> {
    let aux = delivery.tree_aux?;
    Some(BlockCommitmentRoots {
        height: aux.height,
        sapling_root: aux.sapling_root,
        orchard_root: aux.orchard_root,
        ironwood_root: aux.ironwood_root,
        sapling_tx: aux.sapling_tx_count,
        orchard_tx: aux.orchard_tx_count,
        ironwood_tx: aux.ironwood_tx_count,
        auth_data_root: aux.auth_data_root,
    })
}

/// Whether folding `height` into `tree` matches the placement [`HistoryTree::push_from_parts`]
/// asserts on.
///
/// The fold panics on a misplaced tree, and it runs on the thread that commits blocks, so
/// every caller checks first and declines rather than risking the node.
fn fold_is_safe(network: &Network, height: Height, tree: &HistoryTree) -> bool {
    let Some(heartwood) = NetworkUpgrade::Heartwood.activation_height(network) else {
        return tree.as_ref().is_none();
    };

    match height.cmp(&heartwood) {
        Ordering::Less => tree.as_ref().is_none(),
        Ordering::Equal => true,
        Ordering::Greater => tree.as_ref().is_some_and(|tree| {
            height
                .previous()
                .is_ok_and(|previous| tree.current_height() == previous)
        }),
    }
}

#[cfg(test)]
mod tests;
