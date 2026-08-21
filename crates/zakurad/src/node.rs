//! In-process Zakura node lifecycle and application client.

use std::sync::Arc;

use abscissa_core::config::Override;
use color_eyre::{eyre::eyre, Report};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt as _;
use tower::{buffer::Buffer, util::BoxService};
use zakura_chain::chain_tip::ChainTip as _;

use crate::{commands::StartCmd, components::tokio::run_until_shutdown, config::ZakuradConfig};

pub use zakura_chain::{
    block::{Block, Hash as BlockHash, Height as BlockHeight},
    transaction::{Transaction, UnminedTx, UnminedTxId},
};
pub use zakura_network::zakura::CustomService;
pub use zakura_state::{ChainTipChange, HashOrHeight as BlockId, TipAction};

type MempoolService = Buffer<
    BoxService<
        zakura_node_services::mempool::Request,
        zakura_node_services::mempool::Response,
        zakura_node_services::BoxError,
    >,
    zakura_node_services::mempool::Request,
>;

type BlockVerifierService = Buffer<
    BoxService<zakura_consensus::Request, zakura_chain::block::Hash, zakura_consensus::RouterError>,
    zakura_consensus::Request,
>;

/// A cloneable application client for a running node.
#[derive(Clone)]
pub struct NodeClient {
    read_state: zakura_state::ReadStateService,
    latest_chain_tip: zakura_state::LatestChainTip,
    chain_tip_change: ChainTipChange,
    sync_status: crate::components::sync::SyncStatus,
    mempool: MempoolService,
    block_verifier: BlockVerifierService,
    mined_block_sender: mpsc::Sender<(BlockHash, BlockHeight)>,
}

impl NodeClient {
    pub(crate) fn new(
        read_state: zakura_state::ReadStateService,
        latest_chain_tip: zakura_state::LatestChainTip,
        chain_tip_change: ChainTipChange,
        sync_status: crate::components::sync::SyncStatus,
        mempool: MempoolService,
        block_verifier: BlockVerifierService,
        mined_block_sender: mpsc::Sender<(BlockHash, BlockHeight)>,
    ) -> Self {
        Self {
            read_state,
            latest_chain_tip,
            chain_tip_change,
            sync_status,
            mempool,
            block_verifier,
            mined_block_sender,
        }
    }

    /// Returns the current best chain tip, if the state is not empty.
    pub fn tip(&self) -> Option<(BlockHeight, BlockHash)> {
        self.latest_chain_tip.best_tip_height_and_hash()
    }

    /// Returns a shared handle for state queries.
    pub fn read_state(&self) -> zakura_state::ReadStateService {
        self.read_state.clone()
    }

    /// Returns the shared finalized database handle.
    pub fn database(&self) -> zakura_state::ZakuraDb {
        self.read_state.db().clone()
    }

    /// Waits until the synchronizer is likely within its recent-tip window.
    pub async fn wait_until_close_to_tip(&self) -> Result<(), Report> {
        self.sync_status
            .clone()
            .wait_until_close_to_tip()
            .await
            .map_err(|error| eyre!("sync status stopped: {error}"))
    }

    /// Returns a block from the best chain by hash or height.
    pub async fn block(
        &self,
        hash_or_height: impl Into<BlockId>,
    ) -> Result<Option<Arc<Block>>, Report> {
        let response = self
            .read_state
            .clone()
            .oneshot(zakura_state::ReadRequest::Block(hash_or_height.into()))
            .await
            .map_err(|error| eyre!("state block query failed: {error}"))?;

        match response {
            zakura_state::ReadResponse::Block(block) => Ok(block),
            response => Err(eyre!("state returned an unexpected response: {response:?}")),
        }
    }

    /// Verifies and queues a transaction in the mempool.
    pub async fn submit_transaction(
        &self,
        transaction: impl Into<UnminedTx>,
    ) -> Result<UnminedTxId, Report> {
        let transaction = transaction.into();
        let transaction_id = transaction.id();
        let response = self
            .mempool
            .clone()
            .oneshot(zakura_node_services::mempool::Request::Queue(vec![
                transaction.into(),
            ]))
            .await
            .map_err(|error| eyre!("mempool request failed: {error}"))?;
        let zakura_node_services::mempool::Response::Queued(mut results) = response else {
            return Err(eyre!(
                "mempool returned an unexpected response: {response:?}"
            ));
        };
        if results.len() != 1 {
            return Err(eyre!(
                "mempool returned {} results for one transaction",
                results.len()
            ));
        }

        results
            .pop()
            .expect("one result exists because its length was checked")
            .map_err(|error| {
                eyre!("mempool rejected the transaction before verification: {error}")
            })?
            .await
            .map_err(|error| eyre!("mempool stopped while verifying the transaction: {error}"))?
            .map_err(|error| eyre!("transaction verification failed: {error}"))?;

        Ok(transaction_id)
    }

    /// Returns all transactions currently in the mempool.
    pub async fn mempool_transactions(&self) -> Result<Vec<UnminedTx>, Report> {
        let response = self
            .mempool
            .clone()
            .oneshot(zakura_node_services::mempool::Request::FullTransactions)
            .await
            .map_err(|error| eyre!("mempool request failed: {error}"))?;
        let zakura_node_services::mempool::Response::FullTransactions { transactions, .. } =
            response
        else {
            return Err(eyre!(
                "mempool returned an unexpected response: {response:?}"
            ));
        };

        Ok(transactions
            .into_iter()
            .map(|transaction| transaction.transaction)
            .collect())
    }

    /// Verifies and commits a block to the node's state.
    pub async fn submit_block(&self, block: impl Into<Arc<Block>>) -> Result<BlockHash, Report> {
        let block = block.into();
        let height = block
            .coinbase_height()
            .ok_or_else(|| eyre!("submitted block has no coinbase height"))?;
        let hash = self
            .block_verifier
            .clone()
            .oneshot(zakura_consensus::Request::Commit(block))
            .await
            .map_err(|error| eyre!("block verification failed: {error}"))?;
        self.mined_block_sender
            .try_send((hash, height))
            .map_err(|error| {
                eyre!("block was committed but could not be queued for gossip: {error}")
            })?;
        Ok(hash)
    }

    /// Returns an independent listener for best chain tip changes.
    pub fn subscribe_chain_tip(&self) -> ChainTipChange {
        self.chain_tip_change.clone()
    }
}

/// A running in-process Zakura node.
///
/// Dropping this handle requests shutdown without waiting for cleanup. Use
/// [`Node::shutdown`] to wait for graceful shutdown.
pub struct Node {
    client: NodeClient,
    shutdown: CancellationToken,
    task: Option<JoinHandle<Result<(), Report>>>,
}

impl Node {
    /// Returns an application client that remains valid while the node is running.
    pub fn client(&self) -> NodeClient {
        self.client.clone()
    }

    /// Waits until the node stops or fails.
    pub async fn wait(mut self) -> Result<(), Report> {
        self.join().await
    }

    /// Requests graceful shutdown and waits for it to finish.
    pub async fn shutdown(mut self) -> Result<(), Report> {
        self.shutdown.cancel();
        self.join().await
    }

    async fn join(&mut self) -> Result<(), Report> {
        self.task
            .take()
            .expect("node task exists until it is joined")
            .await
            .map_err(|error| eyre!("embedded node task failed: {error}"))?
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

/// Starts a Zakura node on the current Tokio runtime and returns after its services are ready.
pub async fn spawn(config: ZakuradConfig) -> Result<Node, Report> {
    spawn_with_services(config, Vec::new()).await
}

/// Starts a Zakura node with custom p2p services and returns after its services are ready.
pub async fn spawn_with_services(
    config: ZakuradConfig,
    custom_services: Vec<CustomService>,
) -> Result<Node, Report> {
    let shutdown = CancellationToken::new();
    let shutdown_on_drop = shutdown.clone().drop_guard();
    let (ready_tx, ready_rx) = oneshot::channel();
    let mut task = tokio::spawn(run_node(
        config,
        custom_services,
        shutdown.clone(),
        Some(ready_tx),
    ));

    let client = tokio::select! {
        biased;
        result = &mut task => {
            result
                .map_err(|error| eyre!("embedded node task failed during startup: {error}"))??;
            return Err(eyre!("embedded node stopped during startup"));
        }
        ready = ready_rx => ready.map_err(|_| eyre!("embedded node stopped during startup"))?,
    };

    Ok(Node {
        client,
        shutdown: shutdown_on_drop.disarm(),
        task: Some(task),
    })
}

/// Runs a Zakura node on the current Tokio runtime until shutdown or failure.
///
/// tracing, metrics, Rayon setup, and panic hooks are the embedding
/// application's responsibility.
pub async fn run(config: ZakuradConfig, shutdown: CancellationToken) -> Result<(), Report> {
    run_with_services(config, Vec::new(), shutdown).await
}

/// Runs a Zakura node with custom p2p services
pub async fn run_with_services(
    config: ZakuradConfig,
    custom_services: Vec<CustomService>,
    shutdown: CancellationToken,
) -> Result<(), Report> {
    run_node(config, custom_services, shutdown, None).await
}

async fn run_node(
    config: ZakuradConfig,
    custom_services: Vec<CustomService>,
    shutdown: CancellationToken,
    ready: Option<oneshot::Sender<NodeClient>>,
) -> Result<(), Report> {
    if shutdown.is_cancelled() {
        return Ok(());
    }

    let command = StartCmd::default();
    let config = command
        .override_config(config)
        .map_err(|error| eyre!("invalid embedded node configuration: {error}"))?;
    let node_shutdown = CancellationToken::new();
    // One-way latch: cancelled means shutdown must await the root future's cleanup.
    let shutdown_cleanup_required = CancellationToken::new();
    let node = command.start(
        config.into(),
        custom_services,
        node_shutdown.clone(),
        shutdown_cleanup_required.clone(),
        ready,
    );
    run_until_shutdown(
        node,
        shutdown.cancelled_owned(),
        node_shutdown,
        shutdown_cleanup_required,
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::{
        net::UdpSocket,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    };

    use super::*;
    use tokio::time::timeout;
    use zakura_network::zakura::{Peer, Service, Stream, ZakuraConnId, ZakuraPeerId};

    #[derive(Debug)]
    struct RegistrationProbe(Arc<AtomicBool>);

    impl Service for RegistrationProbe {
        fn name(&self) -> &'static str {
            "registration-probe"
        }

        fn streams(&self) -> &[Stream] {
            self.0.store(true, Ordering::Relaxed);
            &[]
        }

        fn add_peer(&self, _peer: Peer) {}

        fn remove_peer(&self, _peer: &ZakuraPeerId, _conn_id: ZakuraConnId) {}
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn embedded_node_client_submits_and_reads_blocks() {
        let _guard = zakura_test::init();
        let native_socket = UdpSocket::bind("127.0.0.1:0").expect("test UDP port is available");
        let native_addr = native_socket
            .local_addr()
            .expect("test UDP socket has an address");
        drop(native_socket);
        let identity_dir = tempfile::tempdir().expect("temporary identity directory is created");

        let mut config = ZakuradConfig::default();
        config.network.network = zakura_chain::parameters::Network::new_regtest(Default::default());
        config.network.listen_addr = "127.0.0.1:0".parse().expect("valid test address");
        config.network.p2p_stack = zakura_network::P2pStack::Dual;
        config.network.initial_mainnet_peers.clear();
        config.network.cache_dir = zakura_network::CacheDir::disabled();
        config.network.identity_dir = identity_dir.path().to_owned();
        config.network.zakura.listen_addr = Some(native_addr);
        config.network.zakura.bootstrap_peers.clear();
        config.state = zakura_state::Config::ephemeral();
        config.sync.debug_skip_regtest_genesis_self_seed = true;
        let service_registered = Arc::new(AtomicBool::new(false));
        let node = timeout(
            Duration::from_secs(30),
            spawn_with_services(
                config,
                vec![CustomService {
                    service: Arc::new(RegistrationProbe(service_registered.clone())),
                    provides: Vec::new(),
                    seeks: Vec::new(),
                }],
            ),
        )
        .await
        .expect("node starts within the test timeout")
        .expect("node starts successfully");

        assert!(service_registered.load(Ordering::Relaxed));
        assert!(UdpSocket::bind(native_addr).is_err());
        let client = node.client();
        assert_eq!(client.tip(), None);
        assert_eq!(client.database().tip(), None);
        let mut tip_changes = client.subscribe_chain_tip();
        let genesis = zakura_chain::block::genesis::regtest_genesis_block();
        let genesis_hash = timeout(
            Duration::from_secs(30),
            client.submit_block(genesis.clone()),
        )
        .await
        .expect("genesis submission completes within the test timeout")
        .expect("genesis is accepted");

        assert_eq!(genesis_hash, genesis.hash());
        let tip_action = timeout(Duration::from_secs(30), tip_changes.wait_for_tip_change())
            .await
            .expect("tip changes within the test timeout")
            .expect("tip change listener remains open");
        assert_eq!(
            match tip_action {
                TipAction::Grow { block } => block.hash,
                TipAction::Reset { hash, .. } => hash,
            },
            genesis_hash
        );
        assert_eq!(client.tip(), Some((BlockHeight(0), genesis_hash)));
        assert!(client
            .mempool_transactions()
            .await
            .expect("mempool query succeeds")
            .is_empty());
        assert_eq!(
            client
                .block(BlockHeight(0))
                .await
                .expect("block query succeeds"),
            Some(genesis)
        );

        node.shutdown().await.expect("node shuts down successfully");

        let socket = timeout(Duration::from_secs(30), async {
            loop {
                if let Ok(socket) = UdpSocket::bind(native_addr) {
                    break socket;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("shutdown releases the Zakura endpoint");
        drop(socket);
    }
}
