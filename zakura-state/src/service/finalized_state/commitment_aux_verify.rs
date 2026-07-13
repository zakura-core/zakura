//! Read-only verification of supplied per-block note-commitment roots against the
//! checkpoint-committed block headers, via the ZIP-221 ChainHistory MMR.
//!
//! This is the "verify" component of the verified-commitment-trees design
//! (`docs/design/verified-commitment-trees.md`). Given a sequence of per-block
//! Sapling/Orchard/Ironwood roots (from a fixture today, an untrusted peer later), confirm
//! they reconstruct a history tree consistent with the header commitments.

#![cfg_attr(not(test), allow(dead_code))]

use std::sync::Arc;

use zakura_chain::{
    block::{merkle::AuthDataRoot, Block, Header, Height},
    history_tree::HistoryTree,
    ironwood, orchard,
    parallel::commitment_aux_verify::{
        header_commitment_is_valid_for_chain_history, SuppliedRootsError,
    },
    parameters::{Network, NetworkUpgrade},
    sapling,
};

use zakura_chain::block::{Commitment, CommitmentError};

use crate::{service::check, ValidateContextError};

/// One block-sized step in supplied commitment-root verification.
#[derive(Clone, Debug)]
pub(crate) struct CommitmentRootVerification {
    pub(crate) block: Option<Arc<Block>>,
    pub(crate) header: Arc<Header>,
    pub(crate) height: Height,
    pub(crate) roots: Option<(
        sapling::tree::Root,
        orchard::tree::Root,
        ironwood::tree::Root,
    )>,
    pub(crate) precomputed_auth_data_root: Option<AuthDataRoot>,
    pub(crate) skip_parent_check: bool,
}

impl CommitmentRootVerification {
    /// Verify this block's parent-history commitment, then fold the supplied
    /// per-block roots into the running history tree for the next block.
    pub(crate) fn with_roots(
        block: Arc<Block>,
        sapling_root: sapling::tree::Root,
        orchard_root: orchard::tree::Root,
        ironwood_root: ironwood::tree::Root,
        precomputed_auth_data_root: Option<AuthDataRoot>,
        skip_parent_check: bool,
    ) -> Self {
        let height = block
            .coinbase_height()
            .expect("checkpoint-verified blocks have a coinbase height");
        CommitmentRootVerification {
            header: block.header.clone(),
            height,
            block: Some(block),
            roots: Some((sapling_root, orchard_root, ironwood_root)),
            precomputed_auth_data_root,
            skip_parent_check,
        }
    }

    /// Verify this block's parent-history commitment without folding in roots.
    ///
    /// This confirms the roots already accumulated in the running tree, which is useful
    /// for the final one-block lag: the roots at height `H` are checked by height `H + 1`.
    pub(crate) fn header_only(
        header: Arc<Header>,
        height: Height,
        precomputed_auth_data_root: Option<AuthDataRoot>,
    ) -> Self {
        CommitmentRootVerification {
            block: None,
            header,
            height,
            roots: None,
            precomputed_auth_data_root,
            skip_parent_check: false,
        }
    }
}

/// Verifies a supplied Sapling root for a *pre-Heartwood* block directly against the
/// block header.
///
/// The ZIP-221 history MMR does not exist below Heartwood, so `block_commitment_is_valid_for_chain_history`
/// is a no-op there and cannot authenticate the supplied roots. This fills that gap:
/// - Sapling..Heartwood: the header's `FinalSaplingRoot` commits the Sapling root
///   directly, so the supplied root must equal it.
/// - Pre-Sapling: the Sapling tree is empty, so the supplied root must be the empty-tree root.
pub(crate) fn verify_supplied_sapling_root_below_heartwood(
    network: &Network,
    block: &Block,
    sapling_root: &sapling::tree::Root,
) -> Result<(), ValidateContextError> {
    let expected = match block.commitment(network)? {
        Commitment::FinalSaplingRoot(header_root) => header_root,
        Commitment::PreSaplingReserved(_) => sapling::tree::NoteCommitmentTree::default().root(),
        // Heartwood activation and later are authenticated by the MMR path.
        _ => return Ok(()),
    };

    if sapling_root != &expected {
        return Err(ValidateContextError::InvalidBlockCommitment(
            CommitmentError::InvalidFinalSaplingRoot {
                expected: <[u8; 32]>::from(expected),
                actual: <[u8; 32]>::from(*sapling_root),
            },
        ));
    }

    Ok(())
}

/// Verifies a supplied Orchard root for a pre-NU5 block.
///
/// Blocks before NU5 do not commit to Orchard roots, so the MMR cannot
/// authenticate them. The supplied root must therefore be the empty-tree root.
pub(crate) fn verify_supplied_orchard_root_below_nu5(
    network: &Network,
    height: Height,
    orchard_root: &orchard::tree::Root,
) -> Result<(), ValidateContextError> {
    // At/above NU5 the ZIP-221 V2 MMR commits to the Orchard root, so it is
    // authenticated there, not here.
    if let Some(nu5_height) = NetworkUpgrade::Nu5.activation_height(network) {
        if height >= nu5_height {
            return Ok(());
        }
    }

    let expected = orchard::tree::NoteCommitmentTree::default().root();
    if orchard_root != &expected {
        return Err(ValidateContextError::InvalidBlockCommitment(
            CommitmentError::InvalidPreNu5OrchardRoot {
                expected: <[u8; 32]>::from(expected),
                actual: <[u8; 32]>::from(*orchard_root),
            },
        ));
    }

    Ok(())
}

/// Verifies a supplied Ironwood root for a pre-Ironwood (pre-`Nu6_3`) block.
///
/// `Nu6_3` is the first network upgrade whose `HistoryTree` leaf commits to an
/// Ironwood root (the `IronwoodOnward`/V3 leaf); below it, no header commits to an
/// Ironwood root and the Ironwood tree is provably empty (no Ironwood actions are
/// allowed), so the supplied root must be the empty-tree root.
pub(crate) fn verify_supplied_ironwood_root_below_nu6_3(
    network: &Network,
    height: Height,
    ironwood_root: &ironwood::tree::Root,
) -> Result<(), ValidateContextError> {
    // At/above Nu6_3 the ZIP-221 V3 MMR commits to the Ironwood root, so it is
    // authenticated there, not here.
    if let Some(nu6_3_height) = NetworkUpgrade::Nu6_3.activation_height(network) {
        if height >= nu6_3_height {
            return Ok(());
        }
    }

    let expected = ironwood::tree::NoteCommitmentTree::default().root();
    if ironwood_root != &expected {
        return Err(ValidateContextError::InvalidBlockCommitment(
            CommitmentError::InvalidPreNu6_3IronwoodRoot {
                expected: <[u8; 32]>::from(expected),
                actual: <[u8; 32]>::from(*ironwood_root),
            },
        ));
    }

    Ok(())
}

/// Verifies that `items` (blocks in ascending height order, with supplied
/// Sapling/Orchard/Ironwood roots when they should be folded in) reconstruct a ZIP-221
/// history MMR consistent with the block header commitments, starting from `tree`
/// (the parent block's history tree).
///
/// Returns the final history tree on success, or `(height, error)` for the first
/// block whose header commitment rejects the roots folded in so far.
///
/// # Lag
///
/// A block's commitment commits to the history tree as of its *parent*, so the root
/// supplied for height `H` is only confirmed when height `H + 1` is processed. Over a
/// contiguous range `[start..=end]` this therefore confirms the roots at
/// `[start..=end - 1]`; pass the block at `end + 1` to confirm the root at `end`.
pub(crate) fn verify_commitment_roots<I>(
    network: &Network,
    mut history_tree: HistoryTree,
    blocks_to_verify: I,
) -> Result<HistoryTree, (Height, ValidateContextError)>
where
    I: IntoIterator<Item = CommitmentRootVerification>,
{
    for block_verify in blocks_to_verify {
        let CommitmentRootVerification {
            block,
            header,
            height,
            roots,
            precomputed_auth_data_root,
            skip_parent_check,
        } = block_verify;

        // Validate this block's header commitment against the current (parent) tree,
        // i.e. against every root already folded in.
        // We allow the caller to control skipping this check
        // in case the caller has already verified the parent tree
        // For example, a block execution loop is:
        // 1. Verify block X against block X - 1 history tree
        // 2. Wait for block X + 1 body to verify against block X history tree
        //    * This is so that we do not commit block X before we have verified its roots.
        // 3. Verify block X + 1 against block X history tree
        //
        // Note that, when we are processing block X + 1 step 1, we are ovrlapping
        // with step 3 of the prior iteration so verification can be skipped in that case
        // for perf reasons.
        if !skip_parent_check {
            if let Some(block) = &block {
                // This block + history tree up to and including the previous block.
                check::block_commitment_is_valid_for_chain_history(
                    block.clone(),
                    network,
                    &history_tree,
                    precomputed_auth_data_root,
                )
                .map_err(|error| (height, error))?;
            } else {
                let auth_data_root = precomputed_auth_data_root
                    .expect("header-only VCT witnesses have a stored precomputed auth-data root");
                header_commitment_is_valid_for_chain_history(
                    &header,
                    height,
                    network,
                    &history_tree,
                    auth_data_root,
                )
                .map_err(|error| match error {
                    SuppliedRootsError::InvalidHeaderCommitment(error) => {
                        ValidateContextError::InvalidBlockCommitment(error)
                    }
                    SuppliedRootsError::HistoryTree(error) => {
                        ValidateContextError::HistoryTreeError(error)
                    }
                })
                .map_err(|error| (height, error))?;
            }
        }

        let Some((sapling_root, orchard_root, ironwood_root)) = roots else {
            continue;
        };

        let block = block.expect("verification items with supplied roots have a block body");
        verify_supplied_sapling_root_below_heartwood(network, &block, &sapling_root)
            .map_err(|error| (height, error))?;
        verify_supplied_orchard_root_below_nu5(network, height, &orchard_root)
            .map_err(|error| (height, error))?;
        verify_supplied_ironwood_root_below_nu6_3(network, height, &ironwood_root)
            .map_err(|error| (height, error))?;

        // Fold this block's supplied roots into the running MMR (builds the leaf
        // from the block body tx-counts + the roots).
        history_tree
            .push(network, block, &sapling_root, &orchard_root, &ironwood_root)
            .map_err(Arc::new)
            .map_err(ValidateContextError::from)
            .map_err(|error| (height, error))?;
    }

    Ok(history_tree)
}

#[cfg(test)]
mod tests {
    use super::*;

    use zakura_chain::{
        block::Block,
        parameters::{
            testnet::{ConfiguredActivationHeights, RegtestParameters},
            Network::Mainnet,
            NetworkUpgrade,
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

    /// A distinct, valid Orchard root that is *not* the empty-tree root, for the
    /// negative cases. Zero is a valid Pallas base field element, and the empty
    /// Orchard tree root is an uncommitted-leaf hash, so the two differ.
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

    fn verification_item(
        block: Arc<Block>,
        sapling_root: sapling::tree::Root,
        orchard_root: orchard::tree::Root,
    ) -> CommitmentRootVerification {
        CommitmentRootVerification::with_roots(
            block,
            sapling_root,
            orchard_root,
            empty_ironwood_root(),
            None,
            false,
        )
    }

    #[test]
    fn commitment_root_verification_constructors_set_expected_fields() {
        let block = mainnet_block_at(1);
        let sapling_root = sapling::tree::NoteCommitmentTree::default().root();
        let orchard_root = orchard::tree::NoteCommitmentTree::default().root();
        let ironwood_root = empty_ironwood_root();

        let with_roots = CommitmentRootVerification::with_roots(
            block.clone(),
            sapling_root,
            orchard_root,
            ironwood_root,
            None,
            true,
        );
        assert!(Arc::ptr_eq(
            with_roots.block.as_ref().expect("roots item has a block"),
            &block
        ));
        assert!(Arc::ptr_eq(&with_roots.header, &block.header));
        assert_eq!(with_roots.height, block.coinbase_height().unwrap());
        assert_eq!(
            with_roots.roots,
            Some((sapling_root, orchard_root, ironwood_root))
        );
        assert_eq!(with_roots.precomputed_auth_data_root, None);
        assert!(with_roots.skip_parent_check);

        let height = block.coinbase_height().unwrap();
        let header_only = CommitmentRootVerification::header_only(
            block.header.clone(),
            height,
            Some(block.auth_data_root()),
        );
        assert!(header_only.block.is_none());
        assert!(Arc::ptr_eq(&header_only.header, &block.header));
        assert_eq!(header_only.height, height);
        assert_eq!(header_only.roots, None);
        assert_eq!(
            header_only.precomputed_auth_data_root,
            Some(block.auth_data_root())
        );
        assert!(!header_only.skip_parent_check);
    }

    /// Below Heartwood the supplied Sapling root is authenticated directly by the
    /// header commitment (or pinned to empty before Sapling). At/above Heartwood,
    /// the MMR path authenticates it instead, so this direct check accepts.
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
        verify_supplied_sapling_root_below_heartwood(&Mainnet, &pre_sapling_block, &empty)
            .expect("the empty-tree root is accepted before Sapling");
        let error = verify_supplied_sapling_root_below_heartwood(
            &Mainnet,
            &pre_sapling_block,
            &different_sapling_root,
        )
        .expect_err("a non-empty Sapling root must be rejected before Sapling");
        assert!(
            matches!(
                error,
                ValidateContextError::InvalidBlockCommitment(
                    CommitmentError::InvalidFinalSaplingRoot { .. }
                )
            ),
            "rejection uses the final Sapling root error, got: {error:?}"
        );

        let sapling_block = mainnet_block_at(419_200);
        verify_supplied_sapling_root_below_heartwood(&Mainnet, &sapling_block, &sapling_root)
            .expect("the header's final Sapling root is accepted before Heartwood");
        let error = verify_supplied_sapling_root_below_heartwood(
            &Mainnet,
            &sapling_block,
            &different_sapling_root,
        )
        .expect_err("a Sapling root different from the header root must be rejected");
        assert!(
            matches!(
                error,
                ValidateContextError::InvalidBlockCommitment(
                    CommitmentError::InvalidFinalSaplingRoot { .. }
                )
            ),
            "rejection uses the final Sapling root error, got: {error:?}"
        );

        let heartwood_block = mainnet_block_at(903_000);
        verify_supplied_sapling_root_below_heartwood(
            &Mainnet,
            &heartwood_block,
            &different_sapling_root,
        )
        .expect("at Heartwood the root is authenticated by the MMR, not pinned here");
    }

    /// Below NU5 the supplied Orchard root must equal the empty-tree root (no header
    /// commits to it there), and any other root is rejected. At/above NU5 the MMR
    /// authenticates it, so this check accepts unconditionally.
    #[test]
    fn pins_orchard_root_to_empty_below_nu5_and_defers_above() {
        let nu5 = NetworkUpgrade::Nu5
            .activation_height(&Mainnet)
            .expect("mainnet has NU5");
        let empty = orchard::tree::NoteCommitmentTree::default().root();
        let wrong = non_empty_orchard_root();

        // Below NU5: the empty root is accepted, a non-empty root is rejected.
        let pre_nu5 = Height(nu5.0 - 1);
        verify_supplied_orchard_root_below_nu5(&Mainnet, pre_nu5, &empty)
            .expect("the empty-tree root is accepted below NU5");
        let error = verify_supplied_orchard_root_below_nu5(&Mainnet, pre_nu5, &wrong)
            .expect_err("a non-empty orchard root must be rejected below NU5");
        assert!(
            matches!(
                error,
                ValidateContextError::InvalidBlockCommitment(
                    CommitmentError::InvalidPreNu5OrchardRoot { .. }
                )
            ),
            "rejection uses the dedicated pre-NU5 orchard error, got: {error:?}"
        );

        // Pre-Sapling/Heartwood (well below NU5) is also pinned to empty.
        verify_supplied_orchard_root_below_nu5(&Mainnet, Height(1), &empty)
            .expect("the empty-tree root is accepted at low heights");
        verify_supplied_orchard_root_below_nu5(&Mainnet, Height(1), &wrong)
            .expect_err("a non-empty orchard root must be rejected at low heights");

        // At and above NU5 the MMR path authenticates the root, so even a non-empty
        // root is accepted here (it is checked elsewhere).
        verify_supplied_orchard_root_below_nu5(&Mainnet, nu5, &wrong)
            .expect("at NU5 the root is authenticated by the MMR, not pinned here");
        verify_supplied_orchard_root_below_nu5(&Mainnet, Height(nu5.0 + 1), &wrong)
            .expect("above NU5 the root is authenticated by the MMR, not pinned here");
    }

    #[test]
    fn pins_orchard_root_to_empty_when_nu5_is_unconfigured() {
        let network = zakura_chain::parameters::Network::new_regtest(RegtestParameters {
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
        let error = verify_supplied_orchard_root_below_nu5(&network, Height(1), &wrong)
            .expect_err("a non-empty orchard root must be rejected when NU5 is unconfigured");
        assert!(
            matches!(
                error,
                ValidateContextError::InvalidBlockCommitment(
                    CommitmentError::InvalidPreNu5OrchardRoot { .. }
                )
            ),
            "rejection uses the dedicated pre-NU5 orchard error, got: {error:?}"
        );
    }

    /// A distinct, valid Ironwood root that is *not* the empty-tree root, for the
    /// negative cases.
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

    /// Below `Nu6_3` the supplied Ironwood root must equal the empty-tree root (no
    /// header commits to it there), and any other root is rejected. At/above
    /// `Nu6_3` the MMR authenticates it, so this check accepts unconditionally.
    #[test]
    fn pins_ironwood_root_to_empty_below_nu6_3_and_defers_above() {
        let network = zakura_chain::parameters::Network::new_regtest(RegtestParameters {
            activation_heights: ConfiguredActivationHeights {
                nu6_3: Some(1_000),
                ..Default::default()
            },
            ..Default::default()
        });
        let nu6_3 = Height(1_000);
        let empty = empty_ironwood_root();
        let wrong = non_empty_ironwood_root();

        // Below Nu6_3: the empty root is accepted, a non-empty root is rejected.
        let pre_nu6_3 = Height(nu6_3.0 - 1);
        verify_supplied_ironwood_root_below_nu6_3(&network, pre_nu6_3, &empty)
            .expect("the empty-tree root is accepted below Nu6_3");
        let error = verify_supplied_ironwood_root_below_nu6_3(&network, pre_nu6_3, &wrong)
            .expect_err("a non-empty ironwood root must be rejected below Nu6_3");
        assert!(
            matches!(
                error,
                ValidateContextError::InvalidBlockCommitment(
                    CommitmentError::InvalidPreNu6_3IronwoodRoot { .. }
                )
            ),
            "rejection uses the dedicated pre-Nu6_3 ironwood error, got: {error:?}"
        );

        // Pre-Sapling/Heartwood (well below Nu6_3) is also pinned to empty.
        verify_supplied_ironwood_root_below_nu6_3(&network, Height(1), &empty)
            .expect("the empty-tree root is accepted at low heights");
        verify_supplied_ironwood_root_below_nu6_3(&network, Height(1), &wrong)
            .expect_err("a non-empty ironwood root must be rejected at low heights");

        // At and above Nu6_3 the MMR path authenticates the root, so even a
        // non-empty root is accepted here (it is checked elsewhere).
        verify_supplied_ironwood_root_below_nu6_3(&network, nu6_3, &wrong)
            .expect("at Nu6_3 the root is authenticated by the MMR, not pinned here");
        verify_supplied_ironwood_root_below_nu6_3(&network, Height(nu6_3.0 + 1), &wrong)
            .expect("above Nu6_3 the root is authenticated by the MMR, not pinned here");
    }

    #[test]
    fn pins_ironwood_root_to_empty_when_nu6_3_is_unconfigured() {
        let empty = empty_ironwood_root();
        let wrong = non_empty_ironwood_root();

        verify_supplied_ironwood_root_below_nu6_3(&Mainnet, Height(1), &empty)
            .expect("the empty-tree root is accepted when Nu6_3 is unconfigured");
        let error = verify_supplied_ironwood_root_below_nu6_3(&Mainnet, Height(1), &wrong)
            .expect_err("a non-empty ironwood root must be rejected when Nu6_3 is unconfigured");
        assert!(
            matches!(
                error,
                ValidateContextError::InvalidBlockCommitment(
                    CommitmentError::InvalidPreNu6_3IronwoodRoot { .. }
                )
            ),
            "rejection uses the dedicated pre-Nu6_3 ironwood error, got: {error:?}"
        );
    }

    /// The verifier confirms real Sapling roots over the Heartwood activation and its
    /// next block (the V1 `ChainHistoryRoot` path), and rejects a wrong root at the
    /// *next* block (the one-block lag).
    #[test]
    fn verifies_real_roots_with_header_only_successor_and_rejects_a_wrong_root() {
        let activation = NetworkUpgrade::Heartwood
            .activation_height(&Mainnet)
            .expect("mainnet has Heartwood")
            .0;

        let act_block = mainnet_block_at(activation);
        let next_block = mainnet_block_at(activation + 1);
        let act_root = mainnet_sapling_root_at(activation);
        let next_root = mainnet_sapling_root_at(activation + 1);
        let empty_orchard_root = orchard::tree::NoteCommitmentTree::default().root();

        // Positive: the real roots reconstruct a tree the next block's header commits to.
        let ok_items = vec![
            verification_item(act_block.clone(), act_root, empty_orchard_root),
            CommitmentRootVerification::header_only(
                next_block.header.clone(),
                Height(activation + 1),
                Some(next_block.auth_data_root()),
            ),
        ];
        verify_commitment_roots(&Mainnet, empty_history_tree(), ok_items)
            .expect("real roots verify against the headers");

        // Negative + lag: a wrong root at the activation height (here, the next
        // block's root, which is a valid but different root) is only caught when the
        // following block's commitment is checked.
        assert_ne!(act_root, next_root, "test needs two distinct roots");
        let bad_items = vec![
            verification_item(act_block, next_root, empty_orchard_root),
            CommitmentRootVerification::header_only(
                next_block.header.clone(),
                Height(activation + 1),
                Some(next_block.auth_data_root()),
            ),
        ];
        let (fail_height, _error) =
            verify_commitment_roots(&Mainnet, empty_history_tree(), bad_items)
                .expect_err("a wrong root must be rejected");
        assert_eq!(
            fail_height.0,
            activation + 1,
            "a wrong root at H is detected at H+1 (the lag)"
        );
    }

    /// Real NU5/V2-range verification over the POC range (1,707,211..=1,717,210),
    /// exercising the actual [`verify_commitment_roots`] on production data.
    ///
    /// Gated by env vars so it stays out of normal CI. Requires two read-only forks
    /// of the RUNBOOK 1.707M master snapshot:
    /// - `VCT_SEED_DB`: an *unsynced* `cp -al` fork (its tip history tree at height
    ///   1,707,210 is the seed — mid-NU5-epoch, so no activation boundary to handle).
    /// - `VCT_ARCHIVE_DB`: an archive fork synced to >= 1,717,211 (provides the blocks
    ///   and per-height roots).
    ///
    /// Run:
    /// ```text
    /// VCT_SEED_DB=<unsynced-fork> VCT_ARCHIVE_DB=<synced-fork> \
    ///   cargo test -p zakura-state --lib commitment_aux_verify -- --ignored --nocapture
    /// ```
    #[ignore]
    #[test]
    #[allow(clippy::print_stderr)] // intentional progress output for a manual run
    fn verifies_real_nu5_range_over_synced_forks() {
        use std::path::PathBuf;

        use crate::{
            constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
            service::finalized_state::{ZakuraDb, STATE_COLUMN_FAMILIES_IN_CODE},
            Config,
        };

        let (Some(seed_dir), Some(archive_dir)) = (
            std::env::var_os("VCT_SEED_DB"),
            std::env::var_os("VCT_ARCHIVE_DB"),
        ) else {
            eprintln!("skipping: set VCT_SEED_DB (unsynced fork) and VCT_ARCHIVE_DB (synced fork)");
            return;
        };

        let open = |dir: PathBuf| -> ZakuraDb {
            let config = Config {
                cache_dir: dir,
                ephemeral: false,
                ..Default::default()
            };
            ZakuraDb::new(
                &config,
                STATE_DATABASE_KIND,
                &state_database_format_version_in_code(),
                &Mainnet,
                true, // skip format upgrades
                STATE_COLUMN_FAMILIES_IN_CODE
                    .iter()
                    .map(ToString::to_string),
                true, // read-only
            )
            .expect("opening the finalized state database should succeed")
        };

        let seed_db = open(PathBuf::from(seed_dir));
        let archive_db = open(PathBuf::from(archive_dir));

        let start = 1_707_211u32;
        let end = 1_717_210u32;

        // Seed: the history tree at 1,707,210 (the unsynced fork's tip).
        let seed = (*seed_db.history_tree()).clone();
        assert_eq!(
            seed_db.finalized_tip_height().map(|h| h.0),
            Some(start - 1),
            "VCT_SEED_DB must be the unsynced 1,707,210 master fork"
        );
        assert!(
            archive_db.finalized_tip_height().map(|h| h.0).unwrap_or(0) > end,
            "VCT_ARCHIVE_DB must be synced to at least {}",
            end + 1
        );

        // Build (block, sapling_root, orchard_root) for [start..=end+1]; the +1 block
        // confirms the in-range root at `end` via the one-block lag.
        let item_at = |h: u32| -> CommitmentRootVerification {
            let block = archive_db
                .block(Height(h).into())
                .expect("archive fork has the block");
            let sapling_root = archive_db
                .sapling_tree_by_height(&Height(h))
                .expect("archive fork has the per-height Sapling tree")
                .root();
            let orchard_root = archive_db
                .orchard_tree_by_height(&Height(h))
                .expect("archive fork has the per-height Orchard tree")
                .root();
            verification_item(block, sapling_root, orchard_root)
        };
        let items: Vec<_> = (start..=end + 1).map(item_at).collect();

        // Positive: every supplied root in the range is confirmed by the V2 headers.
        verify_commitment_roots(&Mainnet, seed.clone(), items.clone())
            .expect("real NU5 roots verify against the headers");
        eprintln!("VCT NU5 positive: {} blocks verified", items.len());

        // Negative + lag: corrupt one root mid-range with a distinct valid root (the
        // range's first root, certainly different after thousands of sandblast blocks);
        // expect rejection at H+1.
        let bad_offset = 5_000usize;
        let bad_height = start + bad_offset as u32;
        let wrong_root = items[0].roots.expect("test verification item has roots").0;
        let mut bad_items = items;
        assert_ne!(
            bad_items[bad_offset]
                .roots
                .expect("test verification item has roots")
                .0,
            wrong_root,
            "need a distinct wrong root"
        );
        bad_items[bad_offset]
            .roots
            .as_mut()
            .expect("test verification item has roots")
            .0 = wrong_root;
        let (fail_height, _error) = verify_commitment_roots(&Mainnet, seed, bad_items)
            .expect_err("a wrong NU5 root must be rejected");
        assert_eq!(
            fail_height.0,
            bad_height + 1,
            "a wrong root at H is detected at H+1 (the lag)"
        );
        eprintln!(
            "VCT NU5 negative: wrong root at {bad_height} rejected at {}",
            fail_height.0
        );
    }

    /// Validates the exact tree-aux records an archive database would serve for arbitrary ranges.
    ///
    /// This check needs only one read-only database and is intended for diagnosing a serving node.
    /// It verifies contiguous encoded heights and compares every served root, transaction count,
    /// and auth-data root with the database's block and per-height tree data.
    ///
    /// Run:
    /// ```text
    /// VCT_ARCHIVE_DB=<archive-db> \
    /// VCT_RANGES=187401-188400,200401-201400 \
    ///   cargo test -p zakura-state --lib validates_served_vct_ranges_from_read_only_db \
    ///   -- --ignored --nocapture
    /// ```
    #[ignore]
    #[test]
    #[allow(clippy::print_stderr)] // intentional progress output for a manual diagnostic
    fn validates_served_vct_ranges_from_read_only_db() {
        use std::path::PathBuf;

        use crate::{
            constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
            service::finalized_state::{
                commitment_aux::serve_block_roots, ZakuraDb, STATE_COLUMN_FAMILIES_IN_CODE,
            },
            Config,
        };

        let (Some(archive_dir), Ok(ranges)) = (
            std::env::var_os("VCT_ARCHIVE_DB"),
            std::env::var("VCT_RANGES"),
        ) else {
            eprintln!("skipping: set VCT_ARCHIVE_DB (archive state) and VCT_RANGES (START-END)");
            return;
        };

        let config = Config {
            cache_dir: PathBuf::from(archive_dir),
            ephemeral: false,
            ..Default::default()
        };
        let archive_db = ZakuraDb::new(
            &config,
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &Mainnet,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            true,
        )
        .expect("opening the archive database read-only should succeed");

        let ranges: Vec<_> = ranges
            .split(',')
            .map(str::trim)
            .filter(|range| !range.is_empty())
            .map(|range| {
                let (start, end) = range
                    .split_once('-')
                    .unwrap_or_else(|| panic!("range {range:?} must use START-END syntax"));
                let start: u32 = start
                    .parse()
                    .unwrap_or_else(|_| panic!("invalid start height in range {range:?}"));
                let end: u32 = end
                    .parse()
                    .unwrap_or_else(|_| panic!("invalid end height in range {range:?}"));
                assert!(start <= end, "range start must not exceed end: {range:?}");
                (start, end)
            })
            .collect();
        assert!(!ranges.is_empty(), "VCT_RANGES did not contain any ranges");

        let root_at = |height: Height| {
            serve_block_roots(&archive_db, height..=height)
                .into_iter()
                .next()
                .unwrap_or_else(|| panic!("served root is missing at {height:?}"))
        };
        let verification_item_at = |height: Height| {
            let roots = root_at(height);
            assert_eq!(roots.height, height, "served root height at {height:?}");
            let block = archive_db
                .block(height.into())
                .unwrap_or_else(|| panic!("archive body is missing at {height:?}"));
            assert_eq!(
                roots.sapling_tx,
                block.sapling_transactions_count(),
                "Sapling transaction count at {height:?}"
            );
            assert_eq!(
                roots.orchard_tx,
                block.orchard_transactions_count(),
                "Orchard transaction count at {height:?}"
            );
            assert_eq!(
                roots.ironwood_tx,
                block.ironwood_transactions_count(),
                "Ironwood transaction count at {height:?}"
            );
            assert_eq!(
                roots.auth_data_root,
                block.auth_data_root(),
                "auth-data root at {height:?}"
            );
            CommitmentRootVerification::with_roots(
                block,
                roots.sapling_root,
                roots.orchard_root,
                roots.ironwood_root,
                Some(roots.auth_data_root),
                false,
            )
        };

        let mut checked = 0usize;
        for &(start, end) in &ranges {
            let range = format!("{start}-{end}");
            let roots = serve_block_roots(&archive_db, Height(start)..=Height(end));
            let expected_len = usize::try_from(end - start + 1)
                .expect("a u32 range length fits in usize on supported targets");
            assert_eq!(
                roots.len(),
                expected_len,
                "served roots must fully cover range {range:?}"
            );

            for (offset, roots) in roots.into_iter().enumerate() {
                let offset =
                    u32::try_from(offset).expect("the bounded diagnostic range offset fits in u32");
                let height = Height(
                    start
                        .checked_add(offset)
                        .expect("the validated range end fits in u32"),
                );
                assert_eq!(
                    roots.height, height,
                    "served root height is misaligned in range {range:?}"
                );

                let block = archive_db
                    .block(height.into())
                    .unwrap_or_else(|| panic!("archive body is missing at {height:?}"));
                let sapling = archive_db
                    .sapling_tree_by_height(&height)
                    .unwrap_or_else(|| panic!("Sapling tree is missing at {height:?}"));
                let orchard = archive_db
                    .orchard_tree_by_height(&height)
                    .unwrap_or_else(|| panic!("Orchard tree is missing at {height:?}"));
                let ironwood = archive_db
                    .ironwood_tree_by_height(&height)
                    .map(|tree| tree.root())
                    .unwrap_or_else(|| ironwood::tree::NoteCommitmentTree::default().root());

                assert_eq!(
                    roots.sapling_root,
                    sapling.root(),
                    "Sapling root at {height:?}"
                );
                assert_eq!(
                    roots.orchard_root,
                    orchard.root(),
                    "Orchard root at {height:?}"
                );
                assert_eq!(roots.ironwood_root, ironwood, "Ironwood root at {height:?}");
                assert_eq!(
                    roots.sapling_tx,
                    block.sapling_transactions_count(),
                    "Sapling transaction count at {height:?}"
                );
                assert_eq!(
                    roots.orchard_tx,
                    block.orchard_transactions_count(),
                    "Orchard transaction count at {height:?}"
                );
                assert_eq!(
                    roots.ironwood_tx,
                    block.ironwood_transactions_count(),
                    "Ironwood transaction count at {height:?}"
                );
                assert_eq!(
                    roots.auth_data_root,
                    block.auth_data_root(),
                    "auth-data root at {height:?}"
                );
                checked += 1;
            }

            eprintln!("validated served tree-aux records for {start}..={end}");
        }

        eprintln!("validated {checked} served tree-aux records");

        let heartwood = NetworkUpgrade::Heartwood
            .activation_height(&Mainnet)
            .expect("Heartwood has a mainnet activation height")
            .0;
        for &(start, end) in ranges.iter().filter(|(start, _)| *start < heartwood) {
            let items = (start..=end).map(|height| verification_item_at(Height(height)));
            verify_commitment_roots(&Mainnet, empty_history_tree(), items).unwrap_or_else(
                |(height, error)| panic!("pre-Heartwood roots failed at {height:?}: {error}"),
            );
            eprintln!("validated direct pre-Heartwood commitments for {start}..={end}");
        }

        let mut epoch_ends = std::collections::BTreeMap::new();
        for &(start, end) in ranges.iter().filter(|(start, _)| *start >= heartwood) {
            let upgrade = NetworkUpgrade::current(&Mainnet, Height(start));
            assert_eq!(
                NetworkUpgrade::current(&Mainnet, Height(end)),
                upgrade,
                "diagnostic range {start}..={end} must not cross a network upgrade"
            );
            let activation = upgrade
                .activation_height(&Mainnet)
                .expect("an active mainnet upgrade has an activation height")
                .0;
            epoch_ends
                .entry((activation, upgrade))
                .and_modify(|max_end: &mut u32| *max_end = (*max_end).max(end))
                .or_insert(end);
        }

        for ((activation, upgrade), end) in epoch_ends {
            let activation_item = verification_item_at(Height(activation));
            let activation_block = activation_item
                .block
                .expect("served root verification items contain block bodies");
            let (sapling_root, orchard_root, ironwood_root) = activation_item
                .roots
                .expect("served root verification items contain roots");
            let history_tree = HistoryTree::from_block(
                &Mainnet,
                activation_block,
                &sapling_root,
                &orchard_root,
                &ironwood_root,
            )
            .expect("network-upgrade activation starts a history-tree epoch");
            let confirm_end = end
                .checked_add(1)
                .expect("diagnostic range end has a successor");
            let items =
                (activation + 1..=confirm_end).map(|height| verification_item_at(Height(height)));

            verify_commitment_roots(&Mainnet, history_tree, items).unwrap_or_else(
                |(height, error)| {
                    panic!(
                        "{upgrade:?} MMR linkage failed at {height:?} while validating through \
                         {confirm_end}: {error}"
                    )
                },
            );
            eprintln!(
                "validated {upgrade:?} MMR linkage for {activation}..={end} \
                 (confirmed by {confirm_end})"
            );
        }
    }
}
