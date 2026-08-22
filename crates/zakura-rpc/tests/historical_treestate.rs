//! Integration tests for historical treestate serving on a VCT fast-synced node.
//!
//! A verified-commitment-trees fast-synced node stores no per-height note commitment tree below
//! its checkpoint handoff. That band is what broke `z_gettreestate` and the `getblock` verbosity
//! 1 tree sizes for wallets syncing against a fast-synced archive snapshot, and restoring them
//! is what [#703](https://github.com/zakura-core/zakura/pull/703) is for.
//!
//! These tests drive the real RPC handlers over a real read service backed by a fast-synced
//! database, so they cover the whole path a wallet takes: RPC method, read request, absent-band
//! detection, replay from the frontier grid, and the root check that decides whether a derived
//! frontier may be served. The oracle is a legacy archive node that committed the same chain
//! without skipping anything, so "restored" means "says what a node that never skipped says".

use tower::buffer::Buffer;

use zakura_chain::{block::Height, chain_sync_status::MockSyncStatus, parameters::Network};
use zakura_network::address_book_peers::MockAddressBookPeers;
use zakura_node_services::BoxError;
use zakura_rpc::methods::{GetBlockResponse, RpcImpl, RpcServer};
use zakura_state::{Config, PruningConfig, StorageMode, VctFastSyncedChain, VctFastSyncedNode};
use zakura_test::mock_service::MockService;

/// The RPC handlers under test, over a real read service and mocks for everything else.
type TestRpc = RpcImpl<
    MockService<
        zakura_node_services::mempool::Request,
        zakura_node_services::mempool::Response,
        zakura_test::mock_service::PanicAssertion,
        BoxError,
    >,
    MockService<
        zakura_state::Request,
        zakura_state::Response,
        zakura_test::mock_service::PanicAssertion,
        BoxError,
    >,
    Buffer<zakura_state::ReadStateService, zakura_state::ReadRequest>,
    zakura_state::LatestChainTip,
    MockAddressBookPeers,
    MockService<
        zakura_consensus::Request,
        zakura_chain::block::Hash,
        zakura_test::mock_service::PanicAssertion,
        BoxError,
    >,
    MockSyncStatus,
>;

/// Wires `node`'s read service behind the RPC handlers a wallet calls.
///
/// Everything a treestate query does not touch is mocked: these tests are about what the state
/// can answer below the handoff, not about mempool, mining, or peer handling.
fn rpc_over(node: &VctFastSyncedNode, network: &Network) -> (TestRpc, tokio::task::JoinHandle<()>) {
    let (_last_warn_error_log_tx, last_warn_error_log_rx) = tokio::sync::watch::channel(None);

    RpcImpl::new(
        network.clone(),
        Default::default(),
        Default::default(),
        "0.0.1",
        "historical treestate integration test",
        MockService::build().for_unit_tests(),
        MockService::build().for_unit_tests(),
        Buffer::new(node.read_state.clone(), 1),
        MockService::build().for_unit_tests(),
        MockSyncStatus::default(),
        node.latest_chain_tip.clone(),
        MockAddressBookPeers::default(),
        last_warn_error_log_rx,
        None,
    )
}

/// `z_gettreestate` must answer inside the absent band with the trees a legacy archive node
/// stored, rather than the typed archive-mode error or a `null` treestate.
///
/// The `null` case is the dangerous one: a client following the lightwalletd contract reads an
/// absent tree as the *empty* tree, so it would derive a birthday anchor asserting an empty
/// commitment tree deep in the chain with nothing raising an error.
#[tokio::test(flavor = "multi_thread")]
async fn z_gettreestate_is_restored_across_the_absent_band() {
    let _init_guard = zakura_test::init();

    let chain = VctFastSyncedChain::generate();
    let network = chain.network().clone();
    let probe = chain.absent_band_height();

    let node = chain.node(Config::ephemeral());
    assert!(
        node.state.vct_tree_absent(probe),
        "the probe height must be inside the absent band for this test to mean anything"
    );

    let (rpc, _queue) = rpc_over(&node, &network);

    let treestate = rpc
        .z_get_treestate(probe.0.to_string())
        .await
        .expect("a deriving archive node answers z_gettreestate below the handoff");

    assert_eq!(treestate.height(), probe);
    assert_eq!(treestate.hash(), chain.block(probe).hash());

    let sapling = chain.legacy_sapling_tree(probe);
    let sapling_commitments = treestate.sapling().commitments();
    assert_eq!(
        sapling_commitments.final_root().as_deref(),
        Some(sapling.root().bytes_in_display_order().as_slice()),
        "the derived Sapling root must match the root a legacy archive node stored"
    );
    assert_eq!(
        sapling_commitments.final_state().as_deref(),
        Some(sapling.to_rpc_bytes().as_slice()),
        "the derived Sapling frontier must serialize exactly as the legacy node's does"
    );

    let orchard = chain.legacy_orchard_tree(probe);
    let orchard_commitments = treestate.orchard().commitments();
    assert_eq!(
        orchard_commitments.final_root().as_deref(),
        Some(orchard.root().bytes_in_display_order().as_slice()),
        "the derived Orchard root must match the root a legacy archive node stored"
    );
    assert_eq!(
        orchard_commitments.final_state().as_deref(),
        Some(orchard.to_rpc_bytes().as_slice()),
        "the derived Orchard frontier must serialize exactly as the legacy node's does"
    );
}

/// `getblock` with verbosity 1 must answer inside the absent band with the tree sizes a legacy
/// archive node stored.
///
/// This is the call that stops a wallet in practice: lightwalletd-style clients fetch the
/// `trees` sizes once per block, and before this the whole band was a hard RPC error.
#[tokio::test(flavor = "multi_thread")]
async fn getblock_verbosity_1_is_restored_across_the_absent_band() {
    let _init_guard = zakura_test::init();

    let chain = VctFastSyncedChain::generate();
    let network = chain.network().clone();
    let probe = chain.absent_band_height();

    let node = chain.node(Config::ephemeral());
    assert!(
        node.state.vct_tree_absent(probe),
        "the probe height must be inside the absent band for this test to mean anything"
    );

    let (rpc, _queue) = rpc_over(&node, &network);

    let GetBlockResponse::Object(block) = rpc
        .get_block(probe.0.to_string(), Some(1))
        .await
        .expect("a deriving archive node answers getblock verbosity 1 below the handoff")
    else {
        panic!("verbosity 1 returns a block object")
    };

    assert_eq!(block.height(), Some(probe));

    let sapling = chain.legacy_sapling_tree(probe);
    let orchard = chain.legacy_orchard_tree(probe);
    assert_eq!(
        block.trees().sapling(),
        sapling.count(),
        "the derived Sapling tree size must match the legacy archive node's"
    );
    assert_eq!(
        block.trees().orchard(),
        orchard.count(),
        "the derived Orchard tree size must match the legacy archive node's"
    );

    // `getblock` takes its final Sapling root from `getblockheader`, which reads the same absent
    // per-height tree, so verbosity 1 also covers the verbose header path.
    let mut expected_final_sapling_root: [u8; 32] = sapling.root().into();
    expected_final_sapling_root.reverse();
    assert_eq!(
        block.final_sapling_root(),
        Some(expected_final_sapling_root),
        "the derived final Sapling root must match the legacy archive node's"
    );
}

/// Without a frontier grid there is nothing to anchor a replay on, so the band stays
/// unavailable.
///
/// This is the control that makes the two tests above mean something: the node here is
/// identical in every other respect, so what they prove is the derivation seam rather than
/// some other source of trees.
#[tokio::test(flavor = "multi_thread")]
async fn nodes_without_a_frontier_grid_still_refuse_the_absent_band() {
    let _init_guard = zakura_test::init();

    let chain = VctFastSyncedChain::generate();
    let network = chain.network().clone();
    let probe = chain.absent_band_height();

    let node = chain.node_without_frontier_grid(Config::ephemeral());
    assert!(
        node.state.vct_tree_absent(probe),
        "the probe height must be inside the absent band for this test to mean anything"
    );

    let (rpc, _queue) = rpc_over(&node, &network);

    assert_absent_band_is_refused(&rpc, probe).await;
}

/// A pruned node still cannot serve the band, and says so.
///
/// Replay reads block bodies a pruned node does not guarantee it retains, so both RPCs report
/// the typed archive-mode error. This is the control that keeps the tests above honest: without
/// it they would pass against a build that answered the band from some other source.
#[tokio::test(flavor = "multi_thread")]
async fn pruned_nodes_still_refuse_the_absent_band() {
    let _init_guard = zakura_test::init();

    let chain = VctFastSyncedChain::generate();
    let network = chain.network().clone();
    let probe = chain.absent_band_height();

    let node = chain.node(Config {
        storage_mode: StorageMode::Pruned(PruningConfig::default()),
        ..Config::ephemeral()
    });
    assert!(
        node.state.vct_tree_absent(probe),
        "the probe height must be inside the absent band for this test to mean anything"
    );

    let (rpc, _queue) = rpc_over(&node, &network);

    assert_absent_band_is_refused(&rpc, probe).await;
}

/// Both restored RPCs must report the typed archive-mode error, which names fast sync as the
/// cause, rather than a `null` treestate or an empty tree.
async fn assert_absent_band_is_refused(rpc: &TestRpc, probe: Height) {
    let treestate_error = rpc
        .z_get_treestate(probe.0.to_string())
        .await
        .expect_err("this node cannot serve historical treestates")
        .to_string();
    assert!(
        treestate_error.contains("fast-synced"),
        "z_gettreestate names the cause, got: {treestate_error}"
    );

    let block_error = rpc
        .get_block(probe.0.to_string(), Some(1))
        .await
        .expect_err("this node cannot serve historical tree sizes")
        .to_string();
    assert!(
        block_error.contains("fast-synced"),
        "getblock verbosity 1 names the cause, got: {block_error}"
    );
}
