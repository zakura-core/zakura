//! Verification for supplied per-block commitment roots.

use std::sync::Arc;

use thiserror::Error;

use crate::{
    block::{
        self, merkle::AuthDataRoot, ChainHistoryBlockTxAuthCommitmentHash, Commitment,
        CommitmentError, Header, Height,
    },
    history_tree::{HistoryTree, HistoryTreeBlockParts, HistoryTreeError},
    ironwood, orchard,
    parallel::commitment_aux::BlockCommitmentRoots,
    parameters::{Network, NetworkUpgrade},
    sapling,
};

/// Result of verifying supplied header-sync commitment roots from header parts.
#[derive(Clone, Debug)]
pub struct SuppliedRootsVerification {
    /// The history tree after folding only roots confirmed by this delivery.
    pub tree: HistoryTree,
    /// Last height whose supplied roots were confirmed and folded.
    pub confirmed_tip: Option<Height>,
}

/// A supplied-root verification failure.
#[derive(Debug, Error)]
pub enum SuppliedRootsError {
    /// A header commitment did not match its supplied auxiliary data.
    #[error("invalid header commitment: {0}")]
    InvalidHeaderCommitment(#[from] CommitmentError),

    /// The supplied auxiliary roots could not extend the history tree.
    #[error("invalid history tree update: {0}")]
    HistoryTree(#[from] Arc<HistoryTreeError>),
}

/// Verify supplied per-block roots against the checkpoint-committed header chain, folding them
/// into the ZIP-221 MMR **from parts** (no block bodies).
///
/// `items` are `(header, roots)` in ascending, contiguous height order, each one height above
/// `tree`'s current tip (`tree` is the running header-frontier history tree). Returns the
/// advanced tree, or `(height, error)` for the first block whose header commitment rejects the
/// roots folded so far.
///
/// A block's commitment binds the history tree as of its *parent*, so the root supplied for
/// height `H` is confirmed when `H + 1` is processed. Over a contiguous range `[start..=end]`
/// this confirms `[start..=end - 1]`; the next range's first header confirms `end`.
/// Therefore the final item is header-checked but not folded, and the returned tree is positioned
/// at the last confirmed entry.
pub fn verify_supplied_roots_from_parts<'a, I>(
    network: &Network,
    mut tree: HistoryTree,
    items: I,
) -> Result<SuppliedRootsVerification, (Height, SuppliedRootsError)>
where
    I: IntoIterator<Item = (&'a Header, &'a BlockCommitmentRoots)>,
{
    let items = items.into_iter().collect::<Vec<_>>();
    let mut confirmed_tip = None;

    for (index, (header, roots)) in items.iter().enumerate() {
        let height = roots.height;

        header_commitment_is_valid_for_chain_history(
            header,
            height,
            network,
            &tree,
            roots.auth_data_root,
        )
        .map_err(|error| (height, error))?;

        verify_supplied_sapling_root_below_heartwood_from_header(
            network,
            header,
            height,
            &roots.sapling_root,
        )
        .map_err(|error| (height, error))?;
        verify_supplied_orchard_root_below_nu5(network, height, &roots.orchard_root)
            .map_err(|error| (height, error))?;
        verify_supplied_ironwood_root_below_nu6_3(network, height, &roots.ironwood_root)
            .map_err(|error| (height, error))?;

        // Header H + 1 authenticates roots for H, so the final item is only a boundary check.
        if index + 1 == items.len() {
            continue;
        }

        tree.push_from_parts(
            network,
            HistoryTreeBlockParts {
                header,
                height,
                sapling_root: &roots.sapling_root,
                orchard_root: &roots.orchard_root,
                ironwood_root: &roots.ironwood_root,
                sapling_tx: roots.sapling_tx,
                orchard_tx: roots.orchard_tx,
                ironwood_tx: roots.ironwood_tx,
            },
        )
        .map_err(Arc::new)
        .map_err(SuppliedRootsError::from)
        .map_err(|error| (height, error))?;
        confirmed_tip = Some(height);
    }

    Ok(SuppliedRootsVerification {
        tree,
        confirmed_tip,
    })
}

/// Header-driven commitment check against `history_tree`, the history tree as
/// of the parent.
pub fn header_commitment_is_valid_for_chain_history(
    header: &block::Header,
    height: block::Height,
    network: &Network,
    history_tree: &HistoryTree,
    auth_data_root: AuthDataRoot,
) -> Result<(), SuppliedRootsError> {
    // Header-sync receives auxiliary roots alongside each header, but a
    // header's chain-history commitment authenticates the history tree built
    // from the *previous* block's auxiliary roots. So this function checks the
    // current header against the tree already folded by the receive pipeline.
    //
    // In a contiguous range, header H + 1 confirms the supplied roots for H.
    // The caller folds roots only after this check succeeds, which keeps
    // unconfirmed roots out of the returned history tree.
    match header.commitment(network, height)? {
        Commitment::PreSaplingReserved(_)
        | Commitment::FinalSaplingRoot(_)
        | Commitment::ChainHistoryActivationReserved => Ok(()),
        Commitment::ChainHistoryRoot(actual_history_tree_root) => {
            // Heartwood through NU6_2 commits directly to the parent history
            // tree root, so the current header confirms the roots that were
            // folded into `history_tree` before this block.
            let history_tree_root = history_tree
                .hash()
                .expect("the previous block history tree exists because current header has a ChainHistoryRoot");
            if actual_history_tree_root == history_tree_root {
                Ok(())
            } else {
                Err(CommitmentError::InvalidChainHistoryRoot {
                    actual: actual_history_tree_root.into(),
                    expected: history_tree_root.into(),
                }
                .into())
            }
        }
        Commitment::ChainHistoryBlockTxAuthCommitment(actual_hash_block_commitments) => {
            // NU6_3 onward commits to both the parent history tree root and
            // this block's auth data root. That still preserves the one-block
            // lag for supplied note commitment roots: `history_tree` is the
            // parent tree, while `auth_data_root` belongs to the current
            // header's block.
            let history_tree_root = history_tree
                .hash()
                .or_else(|| {
                    (NetworkUpgrade::Heartwood.activation_height(network) == Some(height))
                        .then_some(block::CHAIN_HISTORY_ACTIVATION_RESERVED.into())
                })
                .expect(
                    "the previous block history tree exists because current header has a ChainHistoryBlockTxAuthCommitment",
                );
            let hash_block_commitments = ChainHistoryBlockTxAuthCommitmentHash::from_commitments(
                &history_tree_root,
                &auth_data_root,
            );

            if actual_hash_block_commitments == hash_block_commitments {
                Ok(())
            } else {
                Err(CommitmentError::InvalidChainHistoryBlockTxAuthCommitment {
                    actual: actual_hash_block_commitments.into(),
                    expected: hash_block_commitments.into(),
                }
                .into())
            }
        }
    }
}

/// Verifies a supplied Sapling root for a *pre-Heartwood* block directly against the header.
pub fn verify_supplied_sapling_root_below_heartwood_from_header(
    network: &Network,
    header: &Header,
    height: Height,
    sapling_root: &sapling::tree::Root,
) -> Result<(), SuppliedRootsError> {
    let expected = match header.commitment(network, height)? {
        Commitment::FinalSaplingRoot(header_root) => header_root,
        Commitment::PreSaplingReserved(_) => sapling::tree::NoteCommitmentTree::default().root(),
        _ => return Ok(()),
    };

    if sapling_root != &expected {
        return Err(CommitmentError::InvalidFinalSaplingRoot {
            expected: <[u8; 32]>::from(expected),
            actual: <[u8; 32]>::from(*sapling_root),
        }
        .into());
    }

    Ok(())
}

/// Verifies a supplied Orchard root for a pre-NU5 block.
pub fn verify_supplied_orchard_root_below_nu5(
    network: &Network,
    height: Height,
    orchard_root: &orchard::tree::Root,
) -> Result<(), SuppliedRootsError> {
    if let Some(nu5_height) = NetworkUpgrade::Nu5.activation_height(network) {
        if height >= nu5_height {
            return Ok(());
        }
    }

    let expected = orchard::tree::NoteCommitmentTree::default().root();
    if orchard_root != &expected {
        return Err(CommitmentError::InvalidPreNu5OrchardRoot {
            expected: <[u8; 32]>::from(expected),
            actual: <[u8; 32]>::from(*orchard_root),
        }
        .into());
    }

    Ok(())
}

/// Verifies a supplied Ironwood root for a pre-Ironwood (pre-`Nu6_3`) block.
pub fn verify_supplied_ironwood_root_below_nu6_3(
    network: &Network,
    height: Height,
    ironwood_root: &ironwood::tree::Root,
) -> Result<(), SuppliedRootsError> {
    if let Some(nu6_3_height) = NetworkUpgrade::Nu6_3.activation_height(network) {
        if height >= nu6_3_height {
            return Ok(());
        }
    }

    let expected = ironwood::tree::NoteCommitmentTree::default().root();
    if ironwood_root != &expected {
        return Err(CommitmentError::InvalidPreNu6_3IronwoodRoot {
            expected: <[u8; 32]>::from(expected),
            actual: <[u8; 32]>::from(*ironwood_root),
        }
        .into());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        block::Block,
        parameters::{
            testnet::{ConfiguredActivationHeights, RegtestParameters},
            Network::Mainnet,
        },
        serialization::ZcashDeserializeInto,
    };

    /// Build an empty [`HistoryTree`] (the genesis block is pre-Heartwood).
    fn empty_history_tree() -> HistoryTree {
        let genesis = Arc::new(
            zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
                .zcash_deserialize_into::<Block>()
                .expect("genesis deserializes"),
        );
        HistoryTree::from_block(
            &Mainnet,
            genesis,
            &Default::default(),
            &Default::default(),
            &Default::default(),
        )
        .expect("empty history tree for a pre-Heartwood block")
    }

    fn mainnet_block_at(height: u32) -> Arc<Block> {
        let (blocks, _) = Mainnet.block_sapling_roots_map();
        Arc::new(
            blocks
                .get(&height)
                .expect("test vector block exists")
                .zcash_deserialize_into::<Block>()
                .expect("block deserializes"),
        )
    }

    fn mainnet_sapling_root_at(height: u32) -> sapling::tree::Root {
        let (_, sapling_roots) = Mainnet.block_sapling_roots_map();
        sapling::tree::Root::try_from(**sapling_roots.get(&height).expect("root vector exists"))
            .expect("valid root")
    }

    fn empty_ironwood_root() -> ironwood::tree::Root {
        ironwood::tree::NoteCommitmentTree::default().root()
    }

    fn non_empty_orchard_root() -> orchard::tree::Root {
        let empty = orchard::tree::NoteCommitmentTree::default().root();
        let wrong = orchard::tree::Root::try_from([0u8; 32])
            .expect("zero is a valid pallas base field element");
        assert_ne!(
            wrong, empty,
            "the negative cases need a root distinct from the empty-tree root"
        );
        wrong
    }

    fn non_empty_ironwood_root() -> ironwood::tree::Root {
        let empty = empty_ironwood_root();
        let wrong = ironwood::tree::Root::try_from([0u8; 32])
            .expect("zero is a valid pallas base field element");
        assert_ne!(
            wrong, empty,
            "the negative cases need a root distinct from the empty-tree root"
        );
        wrong
    }

    fn roots_from_block(
        block: &Block,
        sapling_root: sapling::tree::Root,
        orchard_root: orchard::tree::Root,
    ) -> BlockCommitmentRoots {
        let height = block
            .coinbase_height()
            .expect("test block has a coinbase height");
        BlockCommitmentRoots {
            height,
            sapling_root,
            orchard_root,
            ironwood_root: empty_ironwood_root(),
            sapling_tx: block.sapling_transactions_count(),
            orchard_tx: block.orchard_transactions_count(),
            ironwood_tx: block.ironwood_transactions_count(),
            auth_data_root: block.auth_data_root(),
        }
    }

    #[test]
    fn pins_sapling_root_below_heartwood_to_header_or_empty() {
        let empty = sapling::tree::NoteCommitmentTree::default().root();
        let sapling_root = mainnet_sapling_root_at(419_200);
        let different_sapling_root = mainnet_sapling_root_at(419_201);
        assert_ne!(
            empty, different_sapling_root,
            "the pre-Sapling negative case needs a non-empty root"
        );
        assert_ne!(
            sapling_root, different_sapling_root,
            "the negative cases need two distinct roots"
        );

        let pre_sapling_block = mainnet_block_at(1);
        verify_supplied_sapling_root_below_heartwood_from_header(
            &Mainnet,
            &pre_sapling_block.header,
            Height(1),
            &empty,
        )
        .expect("the empty-tree root is accepted before Sapling");
        let error = verify_supplied_sapling_root_below_heartwood_from_header(
            &Mainnet,
            &pre_sapling_block.header,
            Height(1),
            &different_sapling_root,
        )
        .expect_err("a non-empty Sapling root must be rejected before Sapling");
        assert!(
            matches!(
                error,
                SuppliedRootsError::InvalidHeaderCommitment(
                    CommitmentError::InvalidFinalSaplingRoot { .. }
                )
            ),
            "rejection uses the final Sapling root error, got: {error:?}"
        );

        let sapling_block = mainnet_block_at(419_200);
        verify_supplied_sapling_root_below_heartwood_from_header(
            &Mainnet,
            &sapling_block.header,
            Height(419_200),
            &sapling_root,
        )
        .expect("the header's final Sapling root is accepted before Heartwood");
        let error = verify_supplied_sapling_root_below_heartwood_from_header(
            &Mainnet,
            &sapling_block.header,
            Height(419_200),
            &different_sapling_root,
        )
        .expect_err("a Sapling root different from the header root must be rejected");
        assert!(
            matches!(
                error,
                SuppliedRootsError::InvalidHeaderCommitment(
                    CommitmentError::InvalidFinalSaplingRoot { .. }
                )
            ),
            "rejection uses the final Sapling root error, got: {error:?}"
        );

        let heartwood_block = mainnet_block_at(903_000);
        verify_supplied_sapling_root_below_heartwood_from_header(
            &Mainnet,
            &heartwood_block.header,
            Height(903_000),
            &different_sapling_root,
        )
        .expect("at Heartwood the root is authenticated by the MMR, not pinned here");
    }

    #[test]
    fn pins_orchard_root_to_empty_below_nu5_and_defers_above() {
        let nu5 = NetworkUpgrade::Nu5
            .activation_height(&Mainnet)
            .expect("mainnet has NU5");
        let empty = orchard::tree::NoteCommitmentTree::default().root();
        let wrong = non_empty_orchard_root();

        let pre_nu5 = Height(nu5.0 - 1);
        verify_supplied_orchard_root_below_nu5(&Mainnet, pre_nu5, &empty)
            .expect("the empty-tree root is accepted below NU5");
        let error = verify_supplied_orchard_root_below_nu5(&Mainnet, pre_nu5, &wrong)
            .expect_err("a non-empty orchard root must be rejected below NU5");
        assert!(
            matches!(
                error,
                SuppliedRootsError::InvalidHeaderCommitment(
                    CommitmentError::InvalidPreNu5OrchardRoot { .. }
                )
            ),
            "rejection uses the dedicated pre-NU5 orchard error, got: {error:?}"
        );

        verify_supplied_orchard_root_below_nu5(&Mainnet, nu5, &wrong)
            .expect("at NU5 the root is authenticated by the MMR, not pinned here");
    }

    #[test]
    fn pins_orchard_root_to_empty_when_nu5_is_unconfigured() {
        let network = Network::new_regtest(RegtestParameters {
            activation_heights: ConfiguredActivationHeights {
                nu5: None,
                ..Default::default()
            },
            ..Default::default()
        });
        let empty = orchard::tree::NoteCommitmentTree::default().root();
        let wrong = non_empty_orchard_root();

        verify_supplied_orchard_root_below_nu5(&network, Height(1), &empty)
            .expect("the empty-tree root is accepted when NU5 is unconfigured");
        verify_supplied_orchard_root_below_nu5(&network, Height(1), &wrong)
            .expect_err("a non-empty orchard root must be rejected when NU5 is unconfigured");
    }

    #[test]
    fn pins_ironwood_root_to_empty_below_nu6_3_and_defers_above() {
        let network = Network::new_regtest(RegtestParameters {
            activation_heights: ConfiguredActivationHeights {
                nu6_3: Some(1_000),
                ..Default::default()
            },
            ..Default::default()
        });
        let empty = empty_ironwood_root();
        let wrong = non_empty_ironwood_root();

        let pre_nu6_3 = Height(999);
        verify_supplied_ironwood_root_below_nu6_3(&network, pre_nu6_3, &empty)
            .expect("the empty-tree root is accepted below Nu6_3");
        let error = verify_supplied_ironwood_root_below_nu6_3(&network, pre_nu6_3, &wrong)
            .expect_err("a non-empty ironwood root must be rejected below Nu6_3");
        assert!(
            matches!(
                error,
                SuppliedRootsError::InvalidHeaderCommitment(
                    CommitmentError::InvalidPreNu6_3IronwoodRoot { .. }
                )
            ),
            "rejection uses the dedicated pre-Nu6_3 ironwood error, got: {error:?}"
        );

        verify_supplied_ironwood_root_below_nu6_3(&network, Height(1_000), &wrong)
            .expect("at Nu6_3 the root is authenticated by the MMR, not pinned here");
    }

    #[test]
    fn verifies_real_roots_and_reports_confirmed_tip_with_one_block_lag() {
        let activation = NetworkUpgrade::Heartwood
            .activation_height(&Mainnet)
            .expect("mainnet has Heartwood")
            .0;

        let act_block = mainnet_block_at(activation);
        let next_block = mainnet_block_at(activation + 1);
        let act_root = mainnet_sapling_root_at(activation);
        let next_root = mainnet_sapling_root_at(activation + 1);
        let empty_orchard_root = orchard::tree::NoteCommitmentTree::default().root();

        let act_roots = roots_from_block(&act_block, act_root, empty_orchard_root);
        let next_roots = roots_from_block(&next_block, next_root, empty_orchard_root);
        let items = vec![
            (act_block.header.as_ref(), &act_roots),
            (next_block.header.as_ref(), &next_roots),
        ];

        let verified = verify_supplied_roots_from_parts(&Mainnet, empty_history_tree(), items)
            .expect("real roots verify against the headers");

        assert_eq!(
            verified.confirmed_tip,
            Some(Height(activation)),
            "a two-header range only confirms the first header's roots"
        );
        assert_eq!(
            verified.tree.hash(),
            HistoryTree::from_block(
                &Mainnet,
                act_block,
                &act_root,
                &empty_orchard_root,
                &empty_ironwood_root(),
            )
            .expect("activation block builds a history tree")
            .hash(),
            "the returned tree is folded through the confirmed root tip"
        );
    }

    #[test]
    fn rejects_wrong_root_at_successor_height() {
        let activation = NetworkUpgrade::Heartwood
            .activation_height(&Mainnet)
            .expect("mainnet has Heartwood")
            .0;

        let act_block = mainnet_block_at(activation);
        let next_block = mainnet_block_at(activation + 1);
        let act_root = mainnet_sapling_root_at(activation);
        let next_root = mainnet_sapling_root_at(activation + 1);
        let empty_orchard_root = orchard::tree::NoteCommitmentTree::default().root();
        assert_ne!(act_root, next_root, "test needs two distinct roots");

        let bad_act_roots = roots_from_block(&act_block, next_root, empty_orchard_root);
        let next_roots = roots_from_block(&next_block, next_root, empty_orchard_root);
        let items = vec![
            (act_block.header.as_ref(), &bad_act_roots),
            (next_block.header.as_ref(), &next_roots),
        ];

        let (fail_height, _error) =
            verify_supplied_roots_from_parts(&Mainnet, empty_history_tree(), items)
                .expect_err("a wrong root must be rejected");
        assert_eq!(
            fail_height,
            Height(activation + 1),
            "a wrong root at H is detected at H+1"
        );
    }
}
