//! Header-time authentication for peer-supplied verified-commitment-trees (vct) metadata.
//!
//! The committer authenticates each auxiliary delivery before it folds the delivery.
//! The committer writes the authentication state and finalized block in one RocksDB batch.
//! This module preserves that trust boundary.
//!
//! During fast sync, the header chain runs ahead of the block bodies.
//! The sweep authenticates each selected delivery against its successor commitment.
//! The sweep records attributable failures as rejections.
//! The sweep records ambiguous failures as disputes.
//! The sweep requests replacement metadata immediately after either failure.
//!
//! Roman Akhtariev wrote [`verify_supplied_roots_from_parts`] for the ahead-of-body
//! authentication lane on `main` (zakura#346, #351, #352, #455).
//! This module reuses that verification function.
//! This module supplies fork-aware inputs and records header-chain auxiliary evidence.

use std::{
    cmp::Ordering,
    time::{Duration, Instant},
};

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
            FinalizedState, VctAuthenticationProof, VctAuxiliaryFailureAttribution,
            VctAuxiliaryWindow, VctSuccessorWitness,
        },
        write::{
            vct_failure_repair_trigger,
            vct_write_retry::{VctRepairTrigger, VctWriteRetryManager},
            HeaderChainWriter, VctAuxiliaryWindowRead,
        },
    },
};

/// The sweep authenticates at most this many selected heights before it yields.
const MAX_HEIGHTS_PER_SWEEP: u32 = 128;

/// The sweep consumes at most this much writer time before it yields.
const MAX_SWEEP_TIME: Duration = Duration::from_millis(5);

/// A contiguous selected prefix whose auxiliary roots the sweep has verified and folded.
struct VerifiedSelectedPrefix {
    /// Highest selected header whose delivery is verified and folded into `history_tree`.
    frontier: Frontier,
    /// ZIP-221 MMR folded through `frontier`, positioned exactly as `frontier`'s successor
    /// needs it.
    history_tree: HistoryTree,
}

/// Authenticates selected auxiliary deliveries as their successor headers arrive.
#[derive(Default)]
pub(super) struct VctAuthenticationSweeper {
    /// Verified prefix above the committed body tip, absent until the first sweep anchors.
    verified_selected_prefix: Option<VerifiedSelectedPrefix>,
}

impl VctAuthenticationSweeper {
    /// Authenticates a bounded prefix of selected deliveries above the committed body tip.
    ///
    /// The sweep keeps its durable repair need when a transient condition stops progress.
    /// The committer re-verifies every delivery before it commits the matching block.
    pub(super) fn sweep(
        &mut self,
        finalized_state: &FinalizedState,
        writer: &HeaderChainWriter,
        repair_manager: &mut VctWriteRetryManager,
        mut should_yield: impl FnMut() -> bool,
    ) {
        let sweep_started_at = Instant::now();
        let network = finalized_state.network();
        let Some((committed_body_tip_height, committed_body_tip_hash)) = finalized_state.db.tip()
        else {
            self.reset_verified_prefix();
            return;
        };
        // Outside the fast path, the committer rebuilds every note-commitment tree from
        // block bodies. Peer metadata carries no authority on that path.
        let Ok(first_uncommitted_height) = committed_body_tip_height.next() else {
            self.reset_verified_prefix();
            return;
        };
        if !finalized_state.vct_requires_exact_roots(first_uncommitted_height) {
            self.reset_verified_prefix();
            return;
        }

        let captured_projection = match writer.runtime.capture_selected_projection() {
            Ok(captured_projection) => captured_projection,
            Err(error) => {
                tracing::warn!(
                    ?error,
                    "VCT: header-time authentication could not capture the selected path"
                );
                self.reset_verified_prefix();
                return;
            }
        };
        let selected_frontier_at_height = |height: Height| {
            captured_projection
                .frontiers
                .binary_search_by_key(&height, |frontier| frontier.height)
                .ok()
                .map(|projection_index| captured_projection.frontiers[projection_index])
        };
        // A selected frontier proves that its ancestors remain selected because the projection
        // forms one chain. The sweep rebuilds the prefix after the committer passes its
        // frontier or a reorganization replaces its ancestry.
        let prefix_remains_anchored =
            self.verified_selected_prefix
                .as_ref()
                .is_some_and(|verified_prefix| {
                    verified_prefix.frontier.height >= committed_body_tip_height
                        && selected_frontier_at_height(verified_prefix.frontier.height)
                            == Some(verified_prefix.frontier)
                });
        if !prefix_remains_anchored
            && !self.anchor_verified_prefix(
                finalized_state,
                &network,
                selected_frontier_at_height(committed_body_tip_height),
                committed_body_tip_height,
                committed_body_tip_hash,
            )
        {
            self.reset_verified_prefix();
            return;
        }

        let mut verified_prefix = self
            .verified_selected_prefix
            .take()
            .expect("an unanchored sweep returned above");
        let mut remaining_height_budget = MAX_HEIGHTS_PER_SWEEP;
        let Ok(verified_frontier_index) = captured_projection
            .frontiers
            .binary_search_by_key(&verified_prefix.frontier.height, |frontier| frontier.height)
        else {
            self.reset_verified_prefix();
            return;
        };
        let mut projection_index = verified_frontier_index + 1;

        while let Some(selected_frontier) =
            captured_projection.frontiers.get(projection_index).copied()
        {
            if remaining_height_budget == 0
                || sweep_started_at.elapsed() >= MAX_SWEEP_TIME
                || should_yield()
            {
                break;
            }
            let selected_height = selected_frontier.height;
            let selected_hash = selected_frontier.hash;
            remaining_height_budget -= 1;
            if !finalized_state.vct_requires_exact_roots(selected_height) {
                break;
            }
            // A misplaced running tree would panic the fold, and this runs on the thread that
            // commits blocks. Re-check the invariant the anchor established rather than trust
            // it across an arbitrary number of transitions.
            if !history_tree_accepts_height(
                &network,
                selected_height,
                &verified_prefix.history_tree,
            ) {
                tracing::warn!(
                    height = ?selected_height,
                    "VCT: header-time authentication stopped on a misplaced running history tree"
                );
                break;
            }

            let auxiliary_window = match writer
                .vct_auxiliary_window_at_projection_index(projection_index, selected_frontier)
            {
                Ok(VctAuxiliaryWindowRead::Ready(auxiliary_window)) => *auxiliary_window,
                // No selected delivery carries roots for this height. The committer
                // also requests a replacement when it reaches this height.
                Ok(VctAuxiliaryWindowRead::Missing { height }) => {
                    repair_manager
                        .request_sweep_repair(height, VctRepairTrigger::MissingRootObserved);
                    break;
                }
                Err(error) => {
                    tracing::warn!(
                        ?error,
                        height = ?selected_height,
                        "VCT: header-time authentication stopped on an incoherent auxiliary read"
                    );
                    break;
                }
            };
            // Without the successor header there is no commitment that proves these roots.
            let Some(successor_witness) = auxiliary_window.successor.clone() else {
                if let Some(height) = auxiliary_window.successor_height {
                    repair_manager
                        .request_sweep_repair(height, VctRepairTrigger::MissingRootObserved);
                }
                break;
            };
            if deliveries_share_dispute_evidence(&auxiliary_window, &successor_witness) {
                repair_manager
                    .request_sweep_repair(selected_height, VctRepairTrigger::MissingRootObserved);
                break;
            }
            let (Some(current_delivery_roots), Some(successor_delivery_roots)) = (
                supplied_roots(&auxiliary_window.delivery),
                successor_witness.delivery.as_ref().and_then(supplied_roots),
            ) else {
                repair_manager
                    .request_sweep_repair(selected_height, VctRepairTrigger::MissingRootObserved);
                break;
            };

            if auxiliary_window.engine_snapshot.header_generation
                != captured_projection.engine_snapshot.header_generation
            {
                break;
            }

            match verify_supplied_roots_from_parts(
                &network,
                verified_prefix.history_tree.clone(),
                [
                    (
                        auxiliary_window.delivery_header.as_ref(),
                        &current_delivery_roots,
                    ),
                    (successor_witness.header.as_ref(), &successor_delivery_roots),
                ],
            ) {
                Ok(verified) => {
                    if !persist_delivery_authentication(
                        writer,
                        &network,
                        &auxiliary_window,
                        &successor_witness,
                    ) {
                        break;
                    }
                    verified_prefix.history_tree = verified.history_tree().clone();
                    verified_prefix.frontier = Frontier::new(selected_height, selected_hash);
                    metrics::counter!("state.vct.aux.sweep.authenticated.count").increment(1);
                    projection_index += 1;
                }
                Err((failed_height, error)) => {
                    self.record_verification_failure(
                        writer,
                        repair_manager,
                        &auxiliary_window,
                        &successor_witness,
                        failed_height,
                        &error,
                    );
                    break;
                }
            }
        }

        if repair_manager
            .sweep_repair_height()
            .is_some_and(|height| height <= verified_prefix.frontier.height)
        {
            repair_manager.clear_sweep_repair();
        }
        metrics::gauge!("state.vct.aux.sweep.frontier.height")
            .set(f64::from(verified_prefix.frontier.height.0));
        self.verified_selected_prefix = Some(verified_prefix);
    }

    /// Anchors a new verified selected prefix at the committed body tip.
    ///
    /// The method returns `false` when the selected projection or history tree cannot provide a
    /// safe anchor.
    fn anchor_verified_prefix(
        &mut self,
        finalized_state: &FinalizedState,
        network: &Network,
        selected_frontier_at_body_tip: Option<Frontier>,
        committed_body_tip_height: Height,
        committed_body_tip_hash: zakura_chain::block::Hash,
    ) -> bool {
        if selected_frontier_at_body_tip
            != Some(Frontier::new(
                committed_body_tip_height,
                committed_body_tip_hash,
            ))
        {
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
        let Ok(first_uncommitted_height) = committed_body_tip_height.next() else {
            return false;
        };
        if !history_tree_accepts_height(network, first_uncommitted_height, &history_tree) {
            tracing::warn!(
                body_tip = ?committed_body_tip_height,
                "VCT: header-time authentication found the committed history tree misplaced"
            );
            return false;
        }

        self.verified_selected_prefix = Some(VerifiedSelectedPrefix {
            frontier: Frontier::new(committed_body_tip_height, committed_body_tip_hash),
            history_tree,
        });
        true
    }

    /// Records attributable failure evidence and requests replacement metadata.
    fn record_verification_failure(
        &mut self,
        writer: &HeaderChainWriter,
        repair_manager: &mut VctWriteRetryManager,
        auxiliary_window: &VctAuxiliaryWindow,
        successor_witness: &VctSuccessorWitness,
        failed_height: Height,
        error: &SuppliedRootsError,
    ) {
        let delivery_height = auxiliary_window
            .delivery
            .tree_aux
            .map_or(failed_height, |auxiliary_data| auxiliary_data.height);
        let Some((failure, attribution)) = attribute_verification_failure(
            auxiliary_window,
            delivery_height,
            successor_witness.height,
            failed_height,
            error,
        ) else {
            tracing::warn!(
                ?delivery_height,
                ?failed_height,
                %error,
                "VCT: header-time authentication failed without an attributable delivery"
            );
            return;
        };
        let attribution_label = attribution.attribution_label();
        metrics::counter!(
            "state.vct.aux.sweep.verification_failure.count",
            "attribution" => attribution_label
        )
        .increment(1);
        tracing::warn!(
            ?delivery_height,
            ?failed_height,
            attribution = attribution_label,
            %error,
            "VCT: header-time authentication attributed invalid auxiliary metadata"
        );

        match writer.record_vct_auxiliary_failure(auxiliary_window, attribution, failure) {
            Ok(Some(apply_result @ (ApplyResult::Committed | ApplyResult::NoChange(_)))) => {
                let Some(repair_height) =
                    attribution.repair_height(delivery_height, Some(successor_witness.height))
                else {
                    return;
                };
                let trigger = vct_failure_repair_trigger(&apply_result)
                    .expect("committed or idempotent evidence has a trigger");
                repair_manager.request_sweep_repair(repair_height, trigger);
            }
            Ok(Some(apply_result)) => {
                tracing::debug!(
                    ?apply_result,
                    "VCT: auxiliary failure evidence did not commit"
                );
            }
            Ok(None) => {}
            Err(error) => {
                tracing::error!(
                    ?error,
                    "VCT: header-time authentication could not persist failure evidence"
                );
            }
        }
    }

    /// Drops the volatile verified prefix without clearing durable repair state.
    fn reset_verified_prefix(&mut self) {
        self.verified_selected_prefix = None;
    }
}

fn deliveries_share_dispute_evidence(
    auxiliary_window: &VctAuxiliaryWindow,
    successor_witness: &VctSuccessorWitness,
) -> bool {
    let current = auxiliary_window.delivery;
    let Some(successor) = successor_witness.delivery else {
        return false;
    };
    current.is_disputed()
        && successor.is_disputed()
        && current.observation_ids() == successor.observation_ids()
}

/// Records one verified delivery as authenticated by its successor boundary.
///
/// The function returns whether the sweep may advance past this height. The durable
/// authentication state prevents a later delivery from replacing roots that the sweep already
/// folded into the history tree.
fn persist_delivery_authentication(
    writer: &HeaderChainWriter,
    network: &Network,
    auxiliary_window: &VctAuxiliaryWindow,
    successor_witness: &VctSuccessorWitness,
) -> bool {
    // Below Heartwood no successor commitment authenticates a history root, so there is no
    // boundary to record. Those roots are pinned directly by their own header and never enter
    // the MMR, so an unauthenticated delivery there cannot change what the tree folds.
    let authenticates_history = matches!(
        successor_witness
            .header
            .commitment(network, successor_witness.height),
        Ok(Commitment::ChainHistoryRoot(_) | Commitment::ChainHistoryBlockTxAuthCommitment(_))
    );
    let Some(boundary_auth_data_root) = successor_witness
        .auth_data_root
        .filter(|_| authenticates_history)
    else {
        return true;
    };

    let proof = VctAuthenticationProof::Successor {
        delivery_id: auxiliary_window.delivery.delivery_id,
        delivery_header_hash: auxiliary_window.delivery.header_hash,
        boundary_hash: successor_witness.hash,
        boundary_auth_data_root,
    };
    match writer.authenticate_vct_aux(auxiliary_window, proof) {
        // `None` means the delivery is already authenticated, which pins selection just as
        // well as a fresh mark.
        Ok(None) | Ok(Some(ApplyResult::Committed | ApplyResult::NoChange(_))) => true,
        Ok(Some(apply_result)) => {
            tracing::debug!(
                ?apply_result,
                "VCT: header-time authentication did not commit; retrying on the next sweep"
            );
            false
        }
        Err(error) => {
            tracing::warn!(
                ?error,
                "VCT: header-time authentication could not persist authentication evidence"
            );
            false
        }
    }
}

/// Attributes a sweep verification failure to the exact delivery that caused it.
///
/// The function returns `None` when the evidence does not identify a faulty delivery.
fn attribute_verification_failure(
    auxiliary_window: &VctAuxiliaryWindow,
    delivery_height: Height,
    successor_height: Height,
    failed_height: Height,
    error: &SuppliedRootsError,
) -> Option<(VctCommitFailure, VctAuxiliaryFailureAttribution)> {
    if failed_height == delivery_height {
        // The tree folded so far is already confirmed by this header's predecessor, so the
        // only inputs left at this height come from the current delivery.
        return match error {
            // This check reads no delivery field: it re-proves the parent roots, which were
            // confirmed one step earlier. A mismatch means the run itself is inconsistent.
            SuppliedRootsError::InvalidHeaderCommitment(
                CommitmentError::InvalidChainHistoryRoot { .. },
            )
            | SuppliedRootsError::MissingHistoryTreeRoot => None,
            SuppliedRootsError::InvalidHeaderCommitment(error)
                if !commitment_error_implicates_delivery(error) =>
            {
                None
            }
            _ => {
                let failure = VctCommitFailure::CurrentRoots;
                Some((failure, auxiliary_window.attribute_failure(failure)))
            }
        };
    }
    if failed_height != successor_height {
        return None;
    }

    match error {
        // The boundary commitment combines the current delivery's folded roots with the
        // successor delivery's authorizing-data root. The failure cannot identify which delivery
        // is invalid, so the writer disputes both deliveries.
        SuppliedRootsError::InvalidHeaderCommitment(
            CommitmentError::InvalidChainHistoryRoot { .. }
            | CommitmentError::InvalidChainHistoryBlockTxAuthCommitment { .. },
        ) => {
            let failure = VctCommitFailure::SuccessorBoundary;
            Some((failure, auxiliary_window.attribute_failure(failure)))
        }
        // Every other successor failure is a pre-activation pin on the successor's own
        // fields, which no other delivery can influence. The committer never sees this
        // case because its successor item carries no roots.
        SuppliedRootsError::InvalidHeaderCommitment(error)
            if commitment_error_implicates_delivery(error) =>
        {
            successor_delivery_is_untrusted(auxiliary_window).then_some((
                VctCommitFailure::SuccessorBoundary,
                VctAuxiliaryFailureAttribution::SuccessorDelivery,
            ))
        }
        _ => None,
    }
}

/// Returns whether supplied auxiliary fields caused the commitment failure.
///
/// The remaining variants report a malformed or unparsable header commitment, which the
/// header chain validated before retaining the header and no delivery can cause.
fn commitment_error_implicates_delivery(error: &CommitmentError) -> bool {
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

fn successor_delivery_is_untrusted(auxiliary_window: &VctAuxiliaryWindow) -> bool {
    auxiliary_window
        .successor
        .as_ref()
        .and_then(|successor| successor.delivery)
        .is_some_and(|delivery| delivery.is_unauthenticated() || delivery.is_disputed())
}

fn supplied_roots(delivery: &AuxDelivery) -> Option<BlockCommitmentRoots> {
    let auxiliary_data = delivery.tree_aux?;
    Some(BlockCommitmentRoots {
        height: auxiliary_data.height,
        sapling_root: auxiliary_data.sapling_root,
        orchard_root: auxiliary_data.orchard_root,
        ironwood_root: auxiliary_data.ironwood_root,
        sapling_tx: auxiliary_data.sapling_tx_count,
        orchard_tx: auxiliary_data.orchard_tx_count,
        ironwood_tx: auxiliary_data.ironwood_tx_count,
        auth_data_root: auxiliary_data.auth_data_root,
    })
}

/// Returns whether `tree` has the position that [`HistoryTree::push_from_parts`] requires for
/// `height`.
///
/// The fold panics on a misplaced tree, and it runs on the thread that commits blocks, so
/// every caller checks first and declines rather than risking the node.
fn history_tree_accepts_height(network: &Network, height: Height, tree: &HistoryTree) -> bool {
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
