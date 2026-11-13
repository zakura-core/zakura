//! The pool core, run by zakurad behind `[mempool] implementation = "core"`.
//!
//! The core itself is [`zakura_mempool::io::PoolCore`], which knows nothing
//! of tokio, the peer set or the verifier. This module connects it to the
//! node:
//!
//! - [`verifier`]: the core's verifications, through the transaction
//!   verifier, and the verifier's own pool handle, which answers output
//!   lookups at once from the published pool.
//! - [`adapter`]: the `mempool::Request` service every other component
//!   already calls, served on top of the port, with the fetches it needs.
//! - [`follower`]: the tasks that follow the published pool: the announcer
//!   and the indexer's change stream.
//! - [`activation`]: whether the pool is on, decided as today's pool decides.
//!
//! The mempool handle other components take keeps its type, so nothing
//! outside `start.rs` knows which pool runs.

use std::{
    collections::HashSet,
    net::IpAddr,
    sync::{atomic::AtomicBool, Arc},
};

use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tower::{buffer::Buffer, timeout::Timeout, util::BoxService};

use zakura_chain::parameters::Network;
use zakura_mempool::io::PoolCore;
use zakura_network::{self as zn, PeerSocketAddr};
use zakura_node_services::{
    mempool::{
        port::{pool_port, ChainTransitions},
        MempoolTxSubscriber, Request, Response,
    },
    task::{spawn_buffer, spawn_erased},
};
use zakura_state::{ChainTipChange, LatestChainTip};

use crate::components::sync::SyncStatus;

use super::{
    downloads::{
        MAX_INBOUND_CONCURRENCY, TRANSACTION_DOWNLOAD_TIMEOUT, TRANSACTION_VERIFY_TIMEOUT,
    },
    gossip::MEMPOOL_CHANGE_CHANNEL_CAPACITY,
    Config, Outbound, ReadState, TxVerifier,
};
use crate::BoxError;

mod activation;
mod adapter;
mod follower;
mod verifier;

/// The mempool service type every component takes.
pub(crate) type MempoolService = Buffer<
    Request,
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<Response, BoxError>> + Send>>,
>;

/// What [`start`] gives `start.rs`.
pub(crate) struct CoreParts {
    /// The `mempool::Request` service, for every component that takes the
    /// mempool.
    pub mempool: MempoolService,
    /// The transaction verifier's pool handle. It answers output lookups at
    /// once, from the published pool.
    pub verifier_pool: MempoolService,
    /// The indexer's change stream.
    pub subscriber: MempoolTxSubscriber,
    /// The core's task.
    pub core_task: JoinHandle<Result<(), BoxError>>,
    /// The tasks that decide whether the pool is on and follow what it
    /// publishes.
    pub follower_task: JoinHandle<Result<(), BoxError>>,
}

/// The peers the pool never charges: zcashd-compat sidecars and configured
/// unconditional peers, as today.
#[derive(Clone, Debug, Default)]
pub(crate) struct Exempt {
    /// The canonical IPs of zcashd-compat sidecars.
    pub zcashd_compat: HashSet<IpAddr>,
    /// Configured peers exempt from mempool bans, cooldowns and disconnects.
    pub unconditional: zn::UnconditionalPeers,
}

impl Exempt {
    /// Whether the pool must not charge `peer`.
    pub fn contains(&self, peer: PeerSocketAddr) -> bool {
        self.zcashd_compat.contains(&peer.ip().to_canonical())
            || self.unconditional.contains(peer.ip())
    }
}

/// Starts the pool core and everything that connects it to the node.
#[allow(clippy::too_many_arguments)]
pub(crate) fn start(
    config: &Config,
    expose_peer_addresses: bool,
    outbound: Outbound,
    read_state: ReadState,
    chain_transitions: &ChainTransitions,
    tx_verifier: TxVerifier,
    sync_status: SyncStatus,
    latest_chain_tip: LatestChainTip,
    chain_tip_change: ChainTipChange,
    misbehavior_sender: mpsc::Sender<(PeerSocketAddr, u32)>,
    exempt: Exempt,
) -> Result<CoreParts, BoxError> {
    let activation = activation::Activation::new(
        config,
        sync_status,
        latest_chain_tip,
        chain_tip_change.clone(),
    );
    let (active_tx, active_rx) = tokio::sync::watch::channel(false);
    let Assembled {
        mempool,
        verifier_pool,
        subscriber,
        core,
        follower,
        ..
    } = assemble(
        config,
        expose_peer_addresses,
        outbound,
        read_state,
        chain_transitions,
        tx_verifier,
        chain_tip_change.network().clone(),
        activation.current_flag(),
        misbehavior_sender,
        exempt,
        active_rx,
    )?;
    let core_task = spawn_erased(async move {
        core.run().await;
        Ok::<(), BoxError>(())
    });
    let follower_task = spawn_erased(async move {
        tokio::select! {
            result = follower.run() => result,
            result = activation.run(active_tx) => result,
        }
    });

    Ok(CoreParts {
        mempool,
        verifier_pool,
        subscriber,
        core_task,
        follower_task,
    })
}

/// The pool core and its connections, built but not running.
pub(crate) struct Assembled {
    /// The `mempool::Request` service.
    pub mempool: MempoolService,
    /// The transaction verifier's pool handle.
    pub verifier_pool: MempoolService,
    /// The indexer's change stream.
    pub subscriber: MempoolTxSubscriber,
    /// The core, to run.
    pub core: PoolCore<zakura_consensus::error::TransactionError, verifier::ConsensusVerifier>,
    /// The announcer and change stream, to run.
    pub follower: follower::Follower,
}

/// Builds the pool core and its connections. The core is on while `active`
/// holds `true`, and the verifier starts cooldowns only while `current`
/// holds `true`. [`start`] decides both as today's pool does.
#[allow(clippy::too_many_arguments)]
pub(crate) fn assemble(
    config: &Config,
    expose_peer_addresses: bool,
    outbound: Outbound,
    read_state: ReadState,
    chain_transitions: &ChainTransitions,
    tx_verifier: TxVerifier,
    network: Network,
    current: Arc<AtomicBool>,
    misbehavior_sender: mpsc::Sender<(PeerSocketAddr, u32)>,
    exempt: Exempt,
    active: watch::Receiver<bool>,
) -> Result<Assembled, BoxError> {
    let (start, events) = chain_transitions.subscribe()?;
    let (handle, inbox) = pool_port();

    let adapter_read_state = read_state.clone();
    let verifier = verifier::ConsensusVerifier::new(
        Timeout::new(tx_verifier, TRANSACTION_VERIFY_TIMEOUT),
        read_state,
        config.max_transaction_bytes,
        network,
        current,
    );
    let core = PoolCore::new(
        config,
        start,
        events,
        chain_transitions.generation(),
        verifier,
        inbox,
        active,
    );

    let (changes, _) = tokio::sync::broadcast::channel(MEMPOOL_CHANGE_CHANNEL_CAPACITY);
    let subscriber = MempoolTxSubscriber::new(changes.clone());
    let follower = follower::Follower::new(
        handle.reader(),
        outbound.clone(),
        changes,
        MEMPOOL_CHANGE_CHANNEL_CAPACITY,
    );

    let adapter = adapter::CoreMempool::new(
        handle.clone(),
        Timeout::new(outbound.clone(), TRANSACTION_DOWNLOAD_TIMEOUT),
        outbound,
        adapter_read_state,
        expose_peer_addresses,
        misbehavior_sender,
        Arc::new(exempt),
    );
    let mempool = spawn_buffer(BoxService::new(adapter), MAX_INBOUND_CONCURRENCY);
    let verifier_pool = spawn_buffer(
        BoxService::new(verifier::VerifierPool::new(handle.reader())),
        MAX_INBOUND_CONCURRENCY,
    );

    Ok(Assembled {
        mempool,
        verifier_pool,
        subscriber,
        core,
        follower,
    })
}
