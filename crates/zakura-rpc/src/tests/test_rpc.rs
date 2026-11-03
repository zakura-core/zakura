//! One [`RpcImpl`] type shared by the unit tests.
//!
//! `RpcImpl` and the RPC servers are generic over their services, so every
//! distinct combination of mock services a test passes compiles its own copy
//! of all the RPC methods. [`TestRpcImpl::new_for_tests`] boxes the services
//! and wraps the chain tip, so tests share one compiled copy.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::{sync::mpsc, task::JoinHandle};
use tower::{util::BoxCloneSyncService, BoxError};

use zakura_chain::{
    block,
    chain_sync_status::MockSyncStatus,
    chain_tip::{mock::MockChainTip, ChainTip, NoChainTip},
    parameters::Network,
    transaction,
};
use zakura_consensus::router::service_trait::BlockVerifierService;
use zakura_network::address_book_peers::MockAddressBookPeers;
use zakura_node_services::mempool::{self, MempoolService};
use zakura_state::{LatestChainTip, ReadState as ReadStateService, State as StateService};

use crate::{
    config,
    methods::{LoggedLastEvent, RpcImpl},
    MinedBlockEvent,
};

/// The boxed mempool service used by [`TestRpcImpl`].
pub(crate) type TestMempool = BoxCloneSyncService<mempool::Request, mempool::Response, BoxError>;

/// The boxed state service used by [`TestRpcImpl`].
pub(crate) type TestState =
    BoxCloneSyncService<zakura_state::Request, zakura_state::Response, BoxError>;

/// The boxed read-only state service used by [`TestRpcImpl`].
pub(crate) type TestReadState =
    BoxCloneSyncService<zakura_state::ReadRequest, zakura_state::ReadResponse, BoxError>;

/// The boxed block verifier router used by [`TestRpcImpl`].
pub(crate) type TestBlockVerifier =
    BoxCloneSyncService<zakura_consensus::Request, block::Hash, BoxError>;

/// The RPC implementation built by the unit tests.
pub(crate) type TestRpcImpl = RpcImpl<
    TestMempool,
    TestState,
    TestReadState,
    TestChainTip,
    MockAddressBookPeers,
    TestBlockVerifier,
    MockSyncStatus,
>;

impl TestRpcImpl {
    /// Boxes the test's services and calls [`RpcImpl::new`].
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_for_tests(
        network: Network,
        mining_config: config::mining::Config,
        debug_force_finished_sync: bool,
        build_version: impl ToString,
        user_agent: impl ToString,
        mempool: impl MempoolService,
        state: impl StateService,
        read_state: impl ReadStateService,
        block_verifier_router: impl BlockVerifierService,
        sync_status: MockSyncStatus,
        latest_chain_tip: impl Into<TestChainTip>,
        address_book: MockAddressBookPeers,
        last_warn_error_log_rx: LoggedLastEvent,
        mined_block_sender: Option<mpsc::UnboundedSender<MinedBlockEvent>>,
    ) -> (Self, JoinHandle<()>) {
        RpcImpl::new(
            network,
            mining_config,
            debug_force_finished_sync,
            build_version.to_string(),
            user_agent.to_string(),
            BoxCloneSyncService::new(mempool),
            BoxCloneSyncService::new(state),
            BoxCloneSyncService::new(read_state),
            BoxCloneSyncService::new(block_verifier_router),
            sync_status,
            latest_chain_tip.into(),
            address_book,
            last_warn_error_log_rx,
            mined_block_sender,
        )
    }
}

/// The chain tip types the unit tests pass to [`TestRpcImpl`].
///
/// [`ChainTip`] can't be a trait object, so this enum stands in for one.
#[derive(Clone, Debug)]
pub(crate) enum TestChainTip {
    Mock(MockChainTip),
    Latest(LatestChainTip),
    None(NoChainTip),
}

impl From<MockChainTip> for TestChainTip {
    fn from(tip: MockChainTip) -> Self {
        Self::Mock(tip)
    }
}

impl From<LatestChainTip> for TestChainTip {
    fn from(tip: LatestChainTip) -> Self {
        Self::Latest(tip)
    }
}

impl From<NoChainTip> for TestChainTip {
    fn from(tip: NoChainTip) -> Self {
        Self::None(tip)
    }
}

/// Calls `$method` on whichever chain tip `$tip` holds.
macro_rules! delegate {
    ($tip:expr, $method:ident($($arg:expr),*)) => {
        match $tip {
            TestChainTip::Mock(tip) => tip.$method($($arg),*),
            TestChainTip::Latest(tip) => tip.$method($($arg),*),
            TestChainTip::None(tip) => tip.$method($($arg),*),
        }
    };
}

impl ChainTip for TestChainTip {
    fn best_tip_height(&self) -> Option<block::Height> {
        delegate!(self, best_tip_height())
    }

    fn best_tip_hash(&self) -> Option<block::Hash> {
        delegate!(self, best_tip_hash())
    }

    fn best_tip_height_and_hash(&self) -> Option<(block::Height, block::Hash)> {
        delegate!(self, best_tip_height_and_hash())
    }

    fn best_tip_block_time(&self) -> Option<DateTime<Utc>> {
        delegate!(self, best_tip_block_time())
    }

    fn best_tip_height_and_block_time(&self) -> Option<(block::Height, DateTime<Utc>)> {
        delegate!(self, best_tip_height_and_block_time())
    }

    fn best_tip_mined_transaction_ids(&self) -> Arc<[transaction::Hash]> {
        delegate!(self, best_tip_mined_transaction_ids())
    }

    async fn best_tip_changed(&mut self) -> Result<(), BoxError> {
        match self {
            TestChainTip::Mock(tip) => tip.best_tip_changed().await,
            TestChainTip::Latest(tip) => tip.best_tip_changed().await,
            TestChainTip::None(tip) => tip.best_tip_changed().await,
        }
    }

    fn mark_best_tip_seen(&mut self) {
        delegate!(self, mark_best_tip_seen())
    }

    fn estimate_network_chain_tip_height(
        &self,
        network: &Network,
        now: DateTime<Utc>,
    ) -> Option<block::Height> {
        delegate!(self, estimate_network_chain_tip_height(network, now))
    }

    fn estimate_distance_to_network_chain_tip(
        &self,
        network: &Network,
    ) -> Option<(block::HeightDiff, block::Height)> {
        delegate!(self, estimate_distance_to_network_chain_tip(network))
    }

    fn is_at_or_near_network_tip(&self, network: &Network) -> bool {
        delegate!(self, is_at_or_near_network_tip(network))
    }
}
