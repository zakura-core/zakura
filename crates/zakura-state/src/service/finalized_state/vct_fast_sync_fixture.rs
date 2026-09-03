//! A VCT fast-synced node fixture for tests that need the absent band `[U, H)`.
//!
//! A verified-commitment-trees fast-synced node writes no per-height note commitment tree below
//! its checkpoint handoff, which is what removes `z_gettreestate` and the `getblock` tree sizes
//! for that band. Reproducing that state takes a synthetic valid-commitment chain committed
//! twice: once through the legacy path, which stores every per-height tree and is the golden
//! reference, and once through the VCT fast path, which stores none below the handoff. This
//! module builds both and wires the fast database behind a real [`ReadStateService`].
//!
//! It lives inside the finalized state because installing a fixture root source and constructing
//! a read service are both private to this crate. The `proptest-impl` feature exports it, so
//! integration tests above `zakura-state` can drive real RPC handlers over the absent band
//! instead of asserting on a mocked read service.

#![allow(dead_code)]

use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};

use proptest::{
    strategy::{Strategy, ValueTree},
    test_runner::TestRunner,
};
use tempfile::NamedTempFile;

use zakura_chain::{
    block::{Block, Height},
    orchard,
    parallel::tree::NoteCommitmentTrees,
    parameters::{
        testnet::{ConfiguredActivationHeights, ParametersBuilder},
        Network, NetworkUpgrade,
    },
    sapling, LedgerState,
};
use zakura_node_services::sync_lifecycle::{
    HeaderRuntimeDetachedReason, HeaderRuntimeStatus, LifecycleEpoch,
};

use crate::{
    request::CheckpointVerifiedBlock,
    service::{
        chain_tip::{ChainTipBlock, ChainTipSender, LatestChainTip},
        check,
        finalized_state::{
            commitment_aux, DiskWriteBatch, FinalizedState, FrontierArtifact, FrontierEntry,
            VctSuccessorWitness,
        },
        non_finalized_state::NonFinalizedState,
        watch_receiver::WatchReceiver,
        HeaderChainSubscriptions, ReadStateService, VctRootRepairStatus,
    },
    Config,
};

/// The nontrivial spacing used by the fixture's published frontier grid.
const FRONTIER_GRID_SPACING: u32 = 5;

/// Per-height commitment roots the fixture root source answers from, standing in for the
/// authenticated roots a real fast-synced node takes from its header chain.
type FixtureRoots = HashMap<
    u32,
    (
        sapling::tree::Root,
        orchard::tree::Root,
        zakura_chain::ironwood::tree::Root,
    ),
>;

/// A synthetic chain that a node can fast-sync, together with the legacy trees to check
/// derived answers against.
///
/// Generating the chain is the expensive part, so one fixture builds any number of nodes
/// through [`Self::node`].
pub struct VctFastSyncedChain {
    network: Network,
    blocks: Vec<Arc<Block>>,
    upgrade: Height,
    handoff: Height,
    roots: FixtureRoots,
    handoff_trees: NoteCommitmentTrees,
    legacy: FinalizedState,
}

/// A fast-synced node: its read service, its chain tip, and the database and grid artifact
/// they read through.
///
/// The artifact file and the tip sender are owned here because the read service outlives
/// neither: a dropped temporary file removes the grid the node anchors derivation on, and a
/// dropped sender closes the chain tip channel.
pub struct VctFastSyncedNode {
    /// The read service a caller wraps in the same `Buffer` an RPC server would.
    pub read_state: ReadStateService,

    /// The node's chain tip, for RPC handlers that resolve relative heights.
    pub latest_chain_tip: LatestChainTip,

    /// The fast-synced database, for assertions about what it did and did not store.
    pub state: FinalizedState,

    _chain_tip_sender: ChainTipSender,
    _frontier_grid: Option<NamedTempFile>,
}

impl VctFastSyncedChain {
    /// Generates a chain whose upgrades all activate early enough to leave a shielded absent
    /// band, and commits it through the legacy path to collect the golden trees.
    ///
    /// The chain is deterministic: proptest's fixed-seed runner draws it once, so a failure
    /// reproduces rather than depending on the case a property test happened to pick.
    pub fn generate() -> Self {
        let network = ParametersBuilder::default()
            .with_activation_heights(ConfiguredActivationHeights {
                before_overwinter: Some(1),
                overwinter: Some(10),
                sapling: Some(15),
                blossom: Some(20),
                heartwood: Some(25),
                canopy: Some(30),
                nu5: Some(35),
                nu6: Some(40),
                nu6_1: Some(45),
                nu6_2: Some(47),
                nu6_3: Some(48),
                nu7: Some(50),
            })
            .expect("the fixture activation heights are ordered")
            .extend_funding_streams()
            .to_network()
            .expect("the fixture network parameters are valid");

        let nu5_height = NetworkUpgrade::Nu5
            .activation_height(&network)
            .expect("NU5 activation height is configured");
        let upgrade = NetworkUpgrade::Heartwood
            .activation_height(&network)
            .expect("Heartwood activation height is configured");

        // Four blocks past NU5 so the handoff, its successor witness, and a probe height below
        // the handoff all have Orchard commitments to derive.
        let block_count =
            usize::try_from(nu5_height.0 + 4).expect("the fixture activation height fits in usize");
        let blocks = generate_valid_commitment_chain(&network, block_count);
        let handoff = Height(
            u32::try_from(blocks.len() - 1).expect("the fixture chain is shorter than Height::MAX"),
        );

        // The fixture root source answers from Heartwood onward, matching the VCT property
        // tests. Every height below the handoff still lands in the absent band, so the probe
        // height needs a replay from the frontier grid either way.
        let seed = NetworkUpgrade::Heartwood
            .activation_height(&network)
            .expect("Heartwood activation height is configured")
            .0
            - 1;

        let mut legacy = FinalizedState::new(&Config::ephemeral(), &network)
            .expect("opening an ephemeral database succeeds");
        let mut roots = FixtureRoots::new();
        let mut handoff_trees = None;

        for (index, block) in blocks.iter().enumerate() {
            let height = u32::try_from(index).expect("the fixture chain fits in a u32 height");
            let (_hash, trees) = legacy
                .commit_finalized_direct(
                    CheckpointVerifiedBlock::from(block.clone()).into(),
                    None,
                    None,
                    "vct fixture legacy",
                )
                .expect("the legacy commit of a valid chain succeeds");

            if height > seed {
                roots.insert(
                    height,
                    (
                        trees.sapling.root(),
                        trees.orchard.root(),
                        trees.ironwood.root(),
                    ),
                );
            }
            if height == handoff.0 {
                handoff_trees = Some(trees);
            }
        }

        Self {
            network,
            blocks,
            upgrade,
            handoff,
            roots,
            handoff_trees: handoff_trees.expect("the handoff block produced trees"),
            legacy,
        }
    }

    /// The network the fixture chain was generated for.
    pub fn network(&self) -> &Network {
        &self.network
    }

    /// The checkpoint handoff `H`: the first height a fast-synced node stores trees for.
    pub fn handoff_height(&self) -> Height {
        self.handoff
    }

    /// The top of the absent band `[U, H)`.
    ///
    /// The fixture grid's last entry is below this height, so answering here always exercises a
    /// replay from a published anchor rather than a direct grid hit.
    pub fn absent_band_height(&self) -> Height {
        self.handoff
            .previous()
            .expect("the handoff is above genesis")
    }

    /// The block at `height` on the fixture chain.
    pub fn block(&self, height: Height) -> Arc<Block> {
        self.blocks[usize::try_from(height.0).expect("a fixture height fits in usize")].clone()
    }

    /// The Sapling tree a legacy archive node stores at `height`, which is what a fast-synced
    /// node has to reproduce.
    pub fn legacy_sapling_tree(&self, height: Height) -> Arc<sapling::tree::NoteCommitmentTree> {
        self.legacy
            .db
            .sapling_tree_by_height(&height)
            .expect("a legacy archive node stores every per-height tree")
    }

    /// The Orchard tree a legacy archive node stores at `height`.
    pub fn legacy_orchard_tree(&self, height: Height) -> Arc<orchard::tree::NoteCommitmentTree> {
        self.legacy
            .db
            .orchard_tree_by_height(&height)
            .expect("a legacy archive node stores every per-height tree")
    }

    /// Fast-syncs the chain under `config` and wires the result behind a read service.
    ///
    /// `config.historical_frontier_artifact` is overwritten with a sparse multi-entry grid: this
    /// network has no embedded one, and without a grid derivation stays idle whatever else the
    /// config says. The grid spans genesis through the handoff at nontrivial spacing, including
    /// multiple entries in the positive-`U` absent band.
    pub fn node(&self, config: Config) -> VctFastSyncedNode {
        let frontier_grid = self.write_frontier_grid(None);

        self.build_node(config, Some(frontier_grid))
    }

    /// Builds a node whose nearest published entry is well-framed but has the wrong Orchard root.
    #[cfg(test)]
    fn node_with_corrupt_nearest_frontier(&self, config: Config) -> VctFastSyncedNode {
        let corrupt = self
            .frontier_grid_heights()
            .last()
            .copied()
            .expect("the fixture grid has entries");
        let frontier_grid = self.write_frontier_grid(Some(corrupt));

        self.build_node(config, Some(frontier_grid))
    }

    /// Fast-syncs the chain under `config` with no frontier grid at all.
    ///
    /// Derivation has nothing to anchor on, so a node built this way reports the absent band as
    /// unavailable however capable it otherwise is. It is the control for tests that assert the
    /// band is served: everything but the grid is identical.
    pub fn node_without_frontier_grid(&self, config: Config) -> VctFastSyncedNode {
        self.build_node(config, None)
    }

    fn build_node(
        &self,
        config: Config,
        frontier_grid: Option<NamedTempFile>,
    ) -> VctFastSyncedNode {
        let config = Config {
            historical_frontier_artifact: frontier_grid
                .as_ref()
                .map(|grid| grid.path().to_path_buf()),
            ..config
        };

        let mut state = FinalizedState::new(&config, &self.network)
            .expect("opening an ephemeral database succeeds");

        for (index, block) in self.blocks.iter().enumerate() {
            let height = Height(
                u32::try_from(index).expect("the fixture chain is shorter than Height::MAX"),
            );

            if height == self.upgrade {
                // The current binary writes the roots index and U marker on legacy commits too.
                // Remove the pre-upgrade index rows and move the marker to model a database
                // created by an older binary, while preserving its per-height trees below U.
                let mut batch = DiskWriteBatch::new();
                batch.delete_range_commitment_roots_by_height(&state.db, &Height(0), &self.upgrade);
                batch.update_vct_upgrade_marker(&state.db, self.upgrade);
                state
                    .db
                    .write_batch(batch)
                    .expect("installing the fixture upgrade boundary succeeds");

                state.enable_vct_fast_source(
                    Box::new(commitment_aux::FixtureSource::new(
                        self.roots.clone(),
                        commitment_aux::FinalFrontiers {
                            height: self.handoff,
                            sapling: self.handoff_trees.sapling.clone(),
                            orchard: self.handoff_trees.orchard.clone(),
                            sprout: self.handoff_trees.sprout.clone(),
                            ironwood: self.handoff_trees.ironwood.clone(),
                        },
                    )),
                    false,
                );
            }

            // The fast path defers a tip root until its successor is buffered, so every block
            // but the last carries the witness for the block above it.
            let successor = self
                .blocks
                .get(index + 1)
                .map(|successor| successor_witness(successor.clone()));

            state
                .commit_finalized_direct(
                    CheckpointVerifiedBlock::from(block.clone()).into(),
                    None,
                    successor,
                    "vct fixture fast",
                )
                .expect("the verified fast commit of a valid chain succeeds");
        }

        let tip = ChainTipBlock::from(CheckpointVerifiedBlock::from(
            self.blocks
                .last()
                .expect("the fixture chain is not empty")
                .clone(),
        ));
        let (chain_tip_sender, latest_chain_tip, _chain_tip_change) =
            ChainTipSender::new(tip, &self.network);

        VctFastSyncedNode {
            read_state: read_service_over(&state),
            latest_chain_tip,
            state,
            _chain_tip_sender: chain_tip_sender,
            _frontier_grid: frontier_grid,
        }
    }

    /// Returns the fixture's uniformly spaced grid heights below the handoff.
    fn frontier_grid_heights(&self) -> Vec<Height> {
        (0..self.handoff.0)
            .step_by(
                usize::try_from(FRONTIER_GRID_SPACING).expect("the fixture spacing fits in usize"),
            )
            .map(Height)
            .collect()
    }

    /// Writes a sparse grid from the legacy reference, optionally corrupting one Orchard entry.
    fn write_frontier_grid(&self, corrupt: Option<Height>) -> NamedTempFile {
        let artifact = FrontierArtifact {
            spacing: FRONTIER_GRID_SPACING,
            last_checkpoint: self.handoff,
            entries: self
                .frontier_grid_heights()
                .into_iter()
                .map(|height| {
                    let mut orchard = self
                        .legacy
                        .db
                        .orchard_tree_by_height(&height)
                        .expect("the legacy fixture stores every Orchard frontier");
                    if corrupt == Some(height) {
                        Arc::make_mut(&mut orchard)
                            .append(halo2::pasta::pallas::Base::from(1u64))
                            .expect("the small fixture Orchard tree is not full");
                    }

                    FrontierEntry {
                        height,
                        sapling: self
                            .legacy
                            .db
                            .sapling_tree_by_height(&height)
                            .expect("the legacy fixture stores every Sapling frontier"),
                        orchard,
                        ironwood: self
                            .legacy
                            .db
                            .ironwood_tree_by_height(&height)
                            .expect("the legacy fixture stores every Ironwood frontier"),
                    }
                })
                .collect(),
        };

        let file = NamedTempFile::new().expect("a temporary artifact file is created");
        std::fs::write(file.path(), artifact.encode(&self.network))
            .expect("the genesis frontier grid writes");

        file
    }
}

/// Draws a deterministic chain of blocks whose note commitments are internally consistent, so
/// the legacy commit path produces the trees a fast-synced node must reproduce.
fn generate_valid_commitment_chain(network: &Network, block_count: usize) -> Vec<Arc<Block>> {
    let strategy =
        LedgerState::genesis_strategy(Some(network.clone()), None::<NetworkUpgrade>, None, false)
            .prop_flat_map(move |ledger| {
                Block::partial_chain_strategy(
                    ledger,
                    block_count,
                    check::utxo::transparent_coinbase_spend,
                    true,
                )
            });

    strategy
        .new_tree(&mut TestRunner::deterministic())
        .expect("the fixture chain strategy draws a value")
        .current()
        .0
}

fn successor_witness(block: Arc<Block>) -> VctSuccessorWitness {
    VctSuccessorWitness::from_header(
        block.header.clone(),
        block
            .coinbase_height()
            .expect("generated blocks have a coinbase height"),
        block.auth_data_root(),
    )
}

/// Wires `state` behind a read service with an empty non-finalized state and no header chain,
/// which is what a fast-synced archive node looks like below its handoff.
fn read_service_over(state: &FinalizedState) -> ReadStateService {
    let (_non_finalized_sender, non_finalized_receiver) =
        tokio::sync::watch::channel(NonFinalizedState::new(&state.network()));
    let (_repair_sender, repair_receiver) =
        tokio::sync::watch::channel(VctRootRepairStatus::default());
    let (_snapshot_sender, snapshot_receiver) = tokio::sync::watch::channel(None);
    let (_view_sender, view_receiver) = tokio::sync::watch::channel(None);
    let (_runtime_status_sender, runtime_status_receiver) =
        tokio::sync::watch::channel(HeaderRuntimeStatus::Detached {
            epoch: LifecycleEpoch::INITIAL,
            reason: HeaderRuntimeDetachedReason::AwaitingSemanticHandoff,
        });
    let (_reader_sender, reader_receiver) = tokio::sync::watch::channel(None);

    ReadStateService::new(
        state,
        None,
        Arc::new(OnceLock::new()),
        WatchReceiver::new(non_finalized_receiver),
        repair_receiver,
        HeaderChainSubscriptions {
            snapshots: snapshot_receiver,
            views: view_receiver,
            runtime_status: runtime_status_receiver,
            reader: reader_receiver,
        },
        crate::service::load_historical_frontier_artifact(
            &state.network(),
            state.db.config(),
            state.db.vct_synced_below().is_some(),
        )
        .expect("the fixture grid loads")
        .discard_if_before_vct_handoff(state.db.config(), &state.db),
    )
}

#[cfg(test)]
mod tests {
    use crate::service::read::historical_tree::derive_historical_frontiers_measured;

    use super::*;

    #[test]
    fn positive_upgrade_grid_anchors_and_skips_a_rejected_nearest_entry() {
        let _init_guard = zakura_test::init();
        let chain = VctFastSyncedChain::generate();
        let probe = chain.absent_band_height();
        let grid_heights = chain.frontier_grid_heights();
        let nearest = *grid_heights
            .last()
            .expect("the fixture grid has a nearest entry");
        let previous = grid_heights[grid_heights.len() - 2];

        assert!(chain.upgrade > Height(0));
        assert!(previous >= chain.upgrade);

        let node = chain.node(Config::ephemeral());
        assert_eq!(node.state.db.vct_upgrade_height(), Some(chain.upgrade));
        assert!(node
            .state
            .db
            .sapling_tree_by_height(
                &chain
                    .upgrade
                    .previous()
                    .expect("the fixture upgrade is above genesis")
            )
            .is_some());
        assert!(node.state.vct_tree_absent(probe));

        let derivation = derive_historical_frontiers_measured(
            &node.state.db,
            &node.read_state.historical_trees,
            probe,
            u64::MAX,
        )
        .expect("the nearest valid grid entry anchors the replay");
        assert_eq!(derivation.replayed_blocks, u64::from(probe.0 - nearest.0));
        assert_eq!(
            derivation.frontiers.sapling.root(),
            chain.legacy_sapling_tree(probe).root()
        );
        assert_eq!(
            derivation.frontiers.orchard.root(),
            chain.legacy_orchard_tree(probe).root()
        );

        let corrupt_node = chain.node_with_corrupt_nearest_frontier(Config::ephemeral());
        let derivation = derive_historical_frontiers_measured(
            &corrupt_node.state.db,
            &corrupt_node.read_state.historical_trees,
            probe,
            u64::MAX,
        )
        .expect("a rejected nearest entry falls back to the previous valid entry");
        assert_eq!(
            derivation.replayed_blocks,
            u64::from(probe.0 - previous.0),
            "the replay cost identifies the previous grid entry as the selected anchor"
        );
        assert_eq!(
            derivation.frontiers.sapling.root(),
            chain.legacy_sapling_tree(probe).root()
        );
        assert_eq!(
            derivation.frontiers.orchard.root(),
            chain.legacy_orchard_tree(probe).root()
        );
    }
}
