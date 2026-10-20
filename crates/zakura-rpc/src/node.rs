//! The RPC servers wired to the node's production service types.
//!
//! [`RpcImpl`], the RPC servers, and the indexer server are generic over their
//! services, so Rust compiles them in whichever crate first names concrete
//! service types. Without this module that is `zakurad`, the last crate in the
//! build, so all of the RPC code and the tokio, hyper and tonic code it uses
//! was compiled after everything else had finished.
//!
//! The functions here are not generic: they name the production types and
//! instantiate the RPC code once, in this crate, where it compiles in parallel
//! with the crates that depend on it. Tests keep using the generic APIs with
//! mock services.

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use futures::{future::BoxFuture, FutureExt};
use tokio::{sync::mpsc, task::JoinHandle};
use tower::{buffer::Buffer, BoxError};

use zakura_chain::{block, chain_sync_status::ChainSyncStatus, parameters::Network};
use zakura_consensus::RouterError;
use zakura_network::AddressBook;
use zakura_node_services::mempool::{self, MempoolTxSubscriber};
use zakura_state::{LatestChainTip, ReadStateService};

use crate::{
    config, indexer,
    methods::{LoggedLastEvent, RpcImpl},
    server::RpcServer,
    MinedBlockEvent, PendingBlockRegistry,
};

/// The node's buffered mempool service.
pub type NodeMempool =
    Buffer<mempool::Request, BoxFuture<'static, Result<mempool::Response, BoxError>>>;

/// The node's buffered state service.
pub type NodeState =
    Buffer<zakura_state::Request, BoxFuture<'static, Result<zakura_state::Response, BoxError>>>;

/// The node's buffered block verifier router.
pub type NodeBlockVerifier =
    Buffer<zakura_consensus::Request, BoxFuture<'static, Result<block::Hash, RouterError>>>;

/// The node's shared address book.
pub type NodeAddressBook = Arc<Mutex<AddressBook>>;

/// The RPC implementation served by the node.
pub type NodeRpcImpl = RpcImpl<
    NodeMempool,
    NodeState,
    ReadStateService,
    LatestChainTip,
    NodeAddressBook,
    NodeBlockVerifier,
    NodeSyncStatus,
>;

/// The node's chain sync status.
///
/// The syncer's status type is defined in `zakurad`, so the RPC code holds it
/// behind a trait object. It is only checked when serving mining requests.
#[derive(Clone)]
pub struct NodeSyncStatus(Arc<dyn ChainSyncStatus + Send + Sync>);

impl NodeSyncStatus {
    /// Wraps the syncer's status for the RPC services.
    pub fn new(sync_status: impl ChainSyncStatus + Send + Sync + 'static) -> Self {
        Self(Arc::new(sync_status))
    }
}

impl ChainSyncStatus for NodeSyncStatus {
    fn is_close_to_tip(&self) -> bool {
        self.0.is_close_to_tip()
    }
}

/// The services the node's RPC implementation calls.
pub struct NodeRpcServices {
    /// The buffered mempool service.
    pub mempool: NodeMempool,
    /// The buffered state service.
    pub state: NodeState,
    /// The read-only state service.
    pub read_state: ReadStateService,
    /// The buffered block verifier router.
    pub block_verifier_router: NodeBlockVerifier,
    /// The syncer's chain sync status.
    pub sync_status: NodeSyncStatus,
    /// The latest chain tip.
    pub latest_chain_tip: LatestChainTip,
    /// The peer address book.
    pub address_book: NodeAddressBook,
}

/// Creates the node's RPC implementation and spawns its transaction queue.
///
/// See [`RpcImpl::new`], [`RpcImpl::with_pending_blocks`] and
/// [`RpcImpl::with_end_of_support_height`] for the arguments.
#[allow(clippy::too_many_arguments)]
pub fn new_rpc_impl(
    network: Network,
    mining_config: config::mining::Config,
    debug_force_finished_sync: bool,
    build_version: String,
    user_agent: String,
    services: NodeRpcServices,
    last_warn_error_log_rx: LoggedLastEvent,
    mined_block_sender: Option<mpsc::UnboundedSender<MinedBlockEvent>>,
    pending_blocks: PendingBlockRegistry,
    end_of_support_height: Option<block::Height>,
) -> (NodeRpcImpl, JoinHandle<()>) {
    let NodeRpcServices {
        mempool,
        state,
        read_state,
        block_verifier_router,
        sync_status,
        latest_chain_tip,
        address_book,
    } = services;

    let (rpc_impl, rpc_tx_queue_handle) = RpcImpl::new(
        network,
        mining_config,
        debug_force_finished_sync,
        build_version,
        user_agent,
        mempool,
        state,
        read_state,
        block_verifier_router,
        sync_status,
        latest_chain_tip,
        address_book,
        last_warn_error_log_rx,
        mined_block_sender,
    );

    let rpc_impl = rpc_impl
        .with_pending_blocks(pending_blocks)
        .with_end_of_support_height(end_of_support_height);

    (rpc_impl, rpc_tx_queue_handle)
}

/// Starts the node's public RPC server. See [`RpcServer::start`].
pub fn start_server(
    rpc: &NodeRpcImpl,
    conf: config::rpc::Config,
) -> BoxFuture<'static, Result<JoinHandle<Result<(), BoxError>>, BoxError>> {
    RpcServer::start(rpc.clone(), conf).boxed()
}

/// Starts the node's admin RPC server. See [`RpcServer::start_admin`].
pub fn start_admin_server(
    rpc: &NodeRpcImpl,
    conf: config::rpc::Config,
) -> BoxFuture<'static, Result<JoinHandle<Result<(), BoxError>>, BoxError>> {
    RpcServer::start_admin(rpc.clone(), conf).boxed()
}

/// Starts the node's indexer gRPC server. See [`indexer::server::init`].
pub fn start_indexer_server(
    listen_addr: SocketAddr,
    tls: Option<config::rpc::IndexerTlsConfig>,
    read_state: ReadStateService,
    latest_chain_tip: LatestChainTip,
    mempool_change: MempoolTxSubscriber,
) -> BoxFuture<'static, Result<(JoinHandle<Result<(), BoxError>>, SocketAddr), BoxError>> {
    indexer::server::init(
        listen_addr,
        tls,
        read_state,
        latest_chain_tip,
        mempool_change,
    )
    .boxed()
}
