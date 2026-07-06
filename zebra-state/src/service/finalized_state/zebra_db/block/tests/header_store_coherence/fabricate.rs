//! Pure header, body, and branch-universe fabrication for the coherence harness.
//!
//! Headers are fabricated (not mined): each header's `difficulty_threshold` is
//! computed with the same [`AdjustedDifficulty`] logic the validator uses, so the
//! real DAA runs on every branch. Work divergence between branches comes from
//! block spacing: faster-than-target spacing drives thresholds below the
//! difficulty limit (more work per header), slower-than-target spacing drifts
//! them back up (less work per header). The testnet minimum-difficulty rule is
//! irrelevant here — it only applies above `TESTNET_MINIMUM_DIFFICULTY_START_HEIGHT`
//! (299,188), far above these chains.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use zebra_chain::{
    block::{self, Block, Height},
    parallel::commitment_aux::BlockCommitmentRoots,
    parameters::{Network, NetworkUpgrade},
    transparent,
    work::difficulty::{CompactDifficulty, ParameterDifficulty, PartialCumulativeWork, Work},
};

use super::super::common::{
    commit_header_range, mainnet_block, no_extra_checkpoint_test_network, root_at,
    state_with_genesis_config,
};
use crate::{
    service::check::difficulty::{AdjustedDifficulty, POW_ADJUSTMENT_BLOCK_SPAN},
    Config,
};

/// A difficulty context window: `(difficulty_threshold, time)` pairs in reverse
/// height order, starting from the previous block — the exact shape consumed by
/// [`AdjustedDifficulty::new_from_header_time`] and produced by
/// `ZebraDb::recent_header_context`.
pub(crate) type DifficultyContext = Vec<(CompactDifficulty, DateTime<Utc>)>;

/// Block spacing for fabricated headers, relative to the network target spacing.
#[derive(Copy, Clone, Debug)]
pub(crate) enum Spacing {
    /// A sixteenth of target spacing: the DAA lowers thresholds
    /// (more work per header).
    Fast,
    /// Four times target spacing: the DAA raises thresholds toward the
    /// difficulty limit (less work per header).
    Slow,
}

impl Spacing {
    fn duration(self, network: &Network, height: Height) -> Duration {
        let target = NetworkUpgrade::target_spacing_for_height(network, height);
        let duration = match self {
            Spacing::Fast => target / 16,
            Spacing::Slow => target * 4,
        };
        // Header times serialize as whole seconds; sub-second precision would
        // be lost on the store round-trip and desync the difficulty context.
        Duration::seconds(duration.num_seconds().max(1))
    }
}

/// A fabricated header and everything the op alphabet needs to commit it.
#[derive(Clone, Debug)]
pub(crate) struct FabHeader {
    pub height: Height,
    pub hash: block::Hash,
    pub header: Arc<block::Header>,
    /// The header's proof-of-work amount, from its fabricated difficulty threshold.
    pub work: Work,
    /// Provisional commitment roots for this height.
    pub roots: BlockCommitmentRoots,
    /// Advertised body size committed alongside the header (0 = unknown).
    pub body_size: u32,
}

/// Fabricates a linked run of headers on top of `anchor`.
///
/// `context` must be the difficulty context at the anchor (anchor first). Each
/// header gets the validator-expected difficulty threshold for its fabricated
/// time, so the run passes `check::header_is_valid_for_recent_chain` when
/// committed on a store whose context at the anchor matches `context`.
pub(crate) fn fabricate_headers(
    network: &Network,
    anchor: (Height, block::Hash),
    mut context: DifficultyContext,
    spacings: &[Spacing],
    nonce_seed: u8,
) -> Vec<FabHeader> {
    let template = mainnet_block(1);
    let (mut previous_height, mut previous_hash) = anchor;
    let mut nonce_tag = nonce_seed;

    spacings
        .iter()
        .map(|spacing| {
            let candidate_height = previous_height
                .next()
                .expect("test header height remains in range");
            let previous_time = context
                .first()
                .expect("anchor difficulty context is available")
                .1;
            let candidate_time = previous_time + spacing.duration(network, candidate_height);
            let expected_difficulty = AdjustedDifficulty::new_from_header_time(
                candidate_time,
                previous_height,
                network,
                context.iter().copied(),
            )
            .expected_difficulty_threshold();

            let mut header = *template.header;
            header.previous_block_hash = previous_hash;
            header.time = candidate_time;
            header.difficulty_threshold = expected_difficulty;
            header.nonce.0[0] = header.nonce.0[0].wrapping_add(nonce_tag);
            nonce_tag = nonce_tag.wrapping_add(1);

            let header = Arc::new(header);
            let hash = block::Hash::from(&*header);
            previous_hash = hash;
            previous_height = candidate_height;
            context.insert(0, (header.difficulty_threshold, header.time));
            context.truncate(POW_ADJUSTMENT_BLOCK_SPAN);

            FabHeader {
                height: candidate_height,
                hash,
                header,
                work: expected_difficulty
                    .to_work()
                    .expect("fabricated difficulty threshold always converts to work"),
                roots: root_at(candidate_height),
                body_size: 0,
            }
        })
        .collect()
}

/// Extends a difficulty context with fabricated headers (oldest to newest).
pub(crate) fn extend_context(
    mut context: DifficultyContext,
    headers: &[FabHeader],
) -> DifficultyContext {
    for fab in headers {
        context.insert(0, (fab.header.difficulty_threshold, fab.header.time));
    }
    context.truncate(POW_ADJUSTMENT_BLOCK_SPAN);
    context
}

/// Fabricates a full block whose header is `fab.header` and whose single
/// coinbase transaction commits to `fab.height`, so `CheckpointVerifiedBlock`
/// derives the right height. The body-commit batch does no merkle or parent
/// validation, so a template body with a rewritten coinbase height suffices.
pub(crate) fn fabricate_body(fab: &FabHeader) -> Arc<Block> {
    let template = mainnet_block(1);
    let mut block = Block::clone(&template);

    let mut tx = block.transactions.remove(0);
    let input = match Arc::make_mut(&mut tx) {
        zebra_chain::transaction::Transaction::V1 { inputs, .. } => &mut inputs[0],
        _ => panic!("mainnet block 1 has a V1 coinbase transaction"),
    };
    match input {
        transparent::Input::Coinbase { height, .. } => *height = fab.height,
        _ => panic!("mainnet block 1 transaction 0 is a coinbase"),
    }
    block.transactions.insert(0, tx);
    block.header = fab.header.clone();

    Arc::new(block)
}

/// Sums the work of a fabricated header run.
pub(crate) fn total_work(headers: &[FabHeader]) -> PartialCumulativeWork {
    let mut total = PartialCumulativeWork::zero();
    for fab in headers {
        total += fab.work;
    }
    total
}

/// A branch of fabricated headers off a known parent.
#[derive(Clone, Debug)]
pub(crate) struct BranchDef {
    /// The `(height, hash)` of the header this branch builds on.
    pub fork_parent: (Height, block::Hash),
    pub headers: Vec<FabHeader>,
}

/// Height of the trunk header the main fork branches build on.
pub(crate) const FORK_HEIGHT: u32 = 50;
/// Number of trunk headers above genesis.
pub(crate) const TRUNK_LEN: usize = 60;

/// Branch indexes into [`Universe::branches`].
pub(crate) const BRANCH_A: usize = 0;
pub(crate) const BRANCH_B: usize = 1;
pub(crate) const BRANCH_B_EXT: usize = 2;
pub(crate) const BRANCH_C: usize = 3;

/// The fixed, deterministic block-tree every scenario and proptest case runs over.
///
/// Work divergence comes from the real DAA reacting to block spacing. The DAA
/// response is slow — the median timespan compares medians ~17 blocks apart,
/// damped 4× and bounded to −16%/+32%, and the median-time-past lag hides a
/// spacing change for its first ~6 blocks — so per-header work drifts only
/// ~2%/block. Branches must be tens of headers long for the work drift to beat
/// branch-length differences:
///
/// - `trunk`: 60 fast-spacing headers over genesis (the DAA is live and
///   thresholds are well below the limit at the fork point, height 50).
/// - Branch `A` (index 0): 26 fast headers off trunk@50 — the high-work fork.
/// - Branch `B` (index 1): 30 slow headers off trunk@50 — longer than `A` but
///   lower total work, so height order and work order disagree.
/// - Branch `B_ext` (index 2): `B`'s first 4 headers plus a fast continuation,
///   long enough to carry strictly more work than `A` (the flipped work
///   balance scenario s02 needs).
/// - Branch `C` (index 3): 5 fast headers off `A`'s second header — a nested fork.
pub(crate) struct Universe {
    pub network: Network,
    pub genesis: Arc<Block>,
    pub trunk: Vec<FabHeader>,
    pub branches: Vec<BranchDef>,
}

impl Universe {
    pub fn new() -> Self {
        let genesis = mainnet_block(0);
        let network = no_extra_checkpoint_test_network(genesis.hash());
        let genesis_anchor = (Height(0), genesis.hash());
        let genesis_context: DifficultyContext =
            vec![(genesis.header.difficulty_threshold, genesis.header.time)];

        let trunk = fabricate_headers(
            &network,
            genesis_anchor,
            genesis_context.clone(),
            &[Spacing::Fast; TRUNK_LEN],
            0x10,
        );

        let fork_index = FORK_HEIGHT as usize - 1;
        let fork_parent = (trunk[fork_index].height, trunk[fork_index].hash);
        let fork_context = extend_context(genesis_context, &trunk[..=fork_index]);

        // The DAA must be live at the fork point, or fast/slow spacing cannot
        // produce work divergence between the branches.
        let limit_work = network
            .target_difficulty_limit()
            .to_compact()
            .to_work()
            .expect("difficulty limit converts to work");
        assert!(
            trunk[fork_index].work > limit_work,
            "trunk fast spacing must drive thresholds below the difficulty limit \
             before the fork point"
        );

        let branch_a = BranchDef {
            fork_parent,
            headers: fabricate_headers(
                &network,
                fork_parent,
                fork_context.clone(),
                &[Spacing::Fast; 26],
                0x40,
            ),
        };

        let branch_b = BranchDef {
            fork_parent,
            headers: fabricate_headers(
                &network,
                fork_parent,
                fork_context.clone(),
                &[Spacing::Slow; 30],
                0x80,
            ),
        };

        // B's first 4 headers plus a fast continuation, extended until the
        // whole branch carries strictly more work than A, plus margin.
        let b_prefix = branch_b.headers[..4].to_vec();
        let b_prefix_tip = (b_prefix[3].height, b_prefix[3].hash);
        let b_prefix_context = extend_context(fork_context.clone(), &b_prefix);
        let a_work = total_work(&branch_a.headers);
        let mut continuation_len = 1;
        let branch_b_ext = loop {
            assert!(
                continuation_len <= 64,
                "branch B_ext should out-work branch A within 64 fast headers"
            );
            let continuation = fabricate_headers(
                &network,
                b_prefix_tip,
                b_prefix_context.clone(),
                &vec![Spacing::Fast; continuation_len],
                0xC0,
            );
            let mut headers = b_prefix.clone();
            headers.extend(continuation);
            if total_work(&headers) > a_work {
                // Two margin headers so split-range deliveries also out-work A.
                let continuation = fabricate_headers(
                    &network,
                    b_prefix_tip,
                    b_prefix_context.clone(),
                    &vec![Spacing::Fast; continuation_len + 2],
                    0xC0,
                );
                let mut headers = b_prefix;
                headers.extend(continuation);
                break BranchDef {
                    fork_parent,
                    headers,
                };
            }
            continuation_len += 1;
        };

        let c_parent = (branch_a.headers[1].height, branch_a.headers[1].hash);
        let c_context = extend_context(fork_context, &branch_a.headers[..2]);
        let branch_c = BranchDef {
            fork_parent: c_parent,
            headers: fabricate_headers(&network, c_parent, c_context, &[Spacing::Fast; 5], 0xE0),
        };

        let universe = Universe {
            network,
            genesis,
            trunk,
            branches: vec![branch_a, branch_b, branch_b_ext, branch_c],
        };
        universe.assert_work_orderings();
        universe
    }

    /// The invariants the branch construction promises to every scenario.
    fn assert_work_orderings(&self) {
        let a = &self.branches[BRANCH_A];
        let b = &self.branches[BRANCH_B];
        let b_ext = &self.branches[BRANCH_B_EXT];

        assert!(
            b.headers.len() > a.headers.len(),
            "branch B must be longer than branch A"
        );
        assert!(
            total_work(&a.headers) > total_work(&b.headers),
            "branch A must carry more work than the longer branch B \
             (A: {:?}, B: {:?})",
            total_work(&a.headers),
            total_work(&b.headers),
        );
        assert!(
            total_work(&b_ext.headers) > total_work(&a.headers),
            "branch B_ext must carry more work than branch A \
             (B_ext: {:?}, A: {:?})",
            total_work(&b_ext.headers),
            total_work(&a.headers),
        );
        assert_eq!(
            b.headers[..4]
                .iter()
                .map(|fab| fab.hash)
                .collect::<Vec<_>>(),
            b_ext.headers[..4]
                .iter()
                .map(|fab| fab.hash)
                .collect::<Vec<_>>(),
            "branch B_ext must re-use branch B's first four headers"
        );
    }

    /// The trunk header at `height` (heights start at 1).
    pub fn trunk_at(&self, height: u32) -> &FabHeader {
        &self.trunk[height as usize - 1]
    }
}

/// Proves the fabricated universe passes the real contextual validation
/// (`check::header_is_valid_for_recent_chain`) on every branch: the whole
/// trunk, and each branch on a store holding the chain up to its fork parent.
#[test]
fn fabricated_universe_commits_cleanly() {
    let _init_guard = zebra_test::init();
    let universe = Universe::new();

    let headers_of = |fabs: &[FabHeader]| -> Vec<Arc<block::Header>> {
        fabs.iter().map(|fab| fab.header.clone()).collect()
    };

    // The whole trunk, in one range from the genesis anchor.
    let state = state_with_genesis_config(
        &universe.network,
        universe.genesis.clone(),
        Config::ephemeral(),
    );
    commit_header_range(
        &state,
        universe.genesis.hash(),
        &headers_of(&universe.trunk),
    );
    let trunk_tip = universe.trunk.last().expect("trunk is non-empty");
    assert_eq!(
        state.best_header_tip(),
        Some((trunk_tip.height, trunk_tip.hash)),
    );

    // Each branch, on a fresh store holding the chain up to its fork parent.
    for branch_index in [BRANCH_A, BRANCH_B, BRANCH_B_EXT, BRANCH_C] {
        let branch = &universe.branches[branch_index];
        let state = state_with_genesis_config(
            &universe.network,
            universe.genesis.clone(),
            Config::ephemeral(),
        );

        commit_header_range(
            &state,
            universe.genesis.hash(),
            &headers_of(&universe.trunk[..FORK_HEIGHT as usize]),
        );
        if branch_index == BRANCH_C {
            // C forks off A's second header, so A's prefix must be present.
            let a_prefix = &universe.branches[BRANCH_A].headers[..2];
            commit_header_range(
                &state,
                universe.branches[BRANCH_A].fork_parent.1,
                &headers_of(a_prefix),
            );
        }

        commit_header_range(&state, branch.fork_parent.1, &headers_of(&branch.headers));

        let branch_tip = branch.headers.last().expect("branches are non-empty");
        assert_eq!(
            state.best_header_tip(),
            Some((branch_tip.height, branch_tip.hash)),
            "branch {branch_index} commits to the tip",
        );
    }
}
