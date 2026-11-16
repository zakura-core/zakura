//! `start` subcommand - entry point for starting a Zakura node
//!
//! ## Application Structure
//!
//! A Zakura node consists of the following major services and tasks:
//!
//! Peers:
//!  * Peer Connection Pool Service
//!    * primary external interface for outbound requests from this node to remote peers
//!    * accepts requests from services and tasks in this node, and sends them to remote peers
//!  * Peer Discovery Service
//!    * maintains a list of peer addresses, and connection priority metadata
//!    * discovers new peer addresses from existing peer connections
//!    * initiates new outbound peer connections in response to demand from tasks within this node
//!  * Peer Cache Service
//!    * Reads previous peer cache on startup, and adds it to the configured DNS seed peers
//!    * Periodically updates the peer cache on disk from the latest address book state
//!
//! Blocks & Mempool Transactions:
//!  * Consensus Service
//!    * handles all validation logic for the node
//!    * verifies blocks using zakura-chain, then stores verified blocks in zakura-state
//!    * verifies mempool and block transactions using zakura-chain and zakura-script,
//!      and returns verified mempool transactions for mempool storage
//!  * Inbound Service
//!    * primary external interface for inbound peer requests to this node
//!    * handles requests from peers for network data, chain data, and mempool transactions
//!    * spawns download and verify tasks for each gossiped block
//!    * sends gossiped transactions to the mempool service
//!
//! Blocks:
//!  * Sync Task
//!    * runs in the background and continuously queries the network for
//!      new blocks to be verified and added to the local state
//!    * spawns download and verify tasks for each crawled block
//!  * State Service
//!    * contextually verifies blocks
//!    * handles in-memory storage of multiple non-finalized chains
//!    * handles permanent storage of the best finalized chain
//!  * Old State Version Cleanup Task
//!    * deletes outdated state versions
//!  * Block Gossip Task
//!    * runs in the background and continuously queries the state for
//!      newly committed blocks to be gossiped to peers
//!  * Progress Task
//!    * logs progress towards the chain tip
//!
//! Block Mining:
//!  * Internal Miner Task
//!    * if the user has configured Zakura to mine blocks, spawns tasks to generate new blocks,
//!      and submits them for verification. This automatically shares these new blocks with peers.
//!
//! Mempool Transactions:
//!  * Mempool Service
//!    * activates when the syncer is near the chain tip
//!    * spawns download and verify tasks for each crawled or gossiped transaction
//!    * handles in-memory storage of unmined transactions
//!  * Queue Checker Task
//!    * runs in the background, polling the mempool to store newly verified transactions
//!  * Transaction Gossip Task
//!    * runs in the background and gossips newly added mempool transactions
//!      to peers
//!
//! Remote Procedure Calls:
//!  * JSON-RPC Service
//!    * answers RPC client requests using the State Service and Mempool Service
//!    * submits client transactions to the node's mempool
//!
//! Zakura also has diagnostic support:
//! * [metrics](https://github.com/ZcashFoundation/zebra/blob/main/book/src/user/metrics.md)
//! * [tracing](https://github.com/ZcashFoundation/zebra/blob/main/book/src/user/tracing.md)
//!
//! Some of the diagnostic features are optional, and need to be enabled at compile-time.

pub(crate) mod zakura;

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::Path,
    sync::Arc,
};

use eyre::{eyre, Report};
use futures::{
    future::BoxFuture,
    stream::{FuturesUnordered, StreamExt},
    FutureExt,
};
use tokio::{
    pin, select,
    sync::{mpsc, oneshot, watch},
    task::{AbortHandle, JoinHandle},
};
use tokio_util::sync::CancellationToken;
use tower::{load_shed::LoadShed, timeout::Timeout, util::BoxService, ServiceExt};
use tracing::Instrument;

use zakura_chain::block::{self, genesis::regtest_genesis_block};
use zakura_consensus::router::BackgroundTaskHandles;
use zakura_jsonl_trace::{JsonlEventEmitter, JsonlTracer};
use zakura_network::types::PeerServices;
use zakura_node_services::task::{spawn_buffer, spawn_erased};
use zakura_rpc::EarlyMinedBlockChannel;
use zakura_state::StorageMode;

use zakura::{
    drive_block_sync_actions, query_block_sync_frontiers, zakura_header_sync_driver_startup,
    BlocksyncThroughputProbe, BlocksyncThroughputSummary,
};

use crate::{
    application::{build_version, user_agent, LAST_WARN_ERROR_LOG_SENDER},
    components::{
        health,
        inbound::{self, InboundSetupData, MAX_INBOUND_RESPONSE_TIME},
        mempool::{self, Mempool},
        sync::{self, show_block_chain_progress, VERIFICATION_PIPELINE_SCALING_MULTIPLIER},
        tokio::{new_runtime, RuntimeRun},
        zcashd_compat, ChainSync, Inbound,
    },
    config::ZakuradConfig,
};

struct BlockSyncDriverExitGuard {
    fatal_events: mpsc::UnboundedSender<()>,
    shutdown: CancellationToken,
}

impl Drop for BlockSyncDriverExitGuard {
    fn drop(&mut self) {
        if !self.shutdown.is_cancelled() {
            let _ = self.fatal_events.send(());
        }
    }
}

fn supervise_block_sync_driver(
    driver: impl std::future::Future<Output = ()>,
    fatal_events: mpsc::UnboundedSender<()>,
    shutdown: CancellationToken,
) -> impl std::future::Future<Output = ()> {
    let exit_guard = BlockSyncDriverExitGuard {
        fatal_events,
        shutdown,
    };
    async move {
        let _exit_guard = exit_guard;
        driver.await;
    }
}

#[cfg(feature = "internal-miner")]
use crate::components;

/// Start the application (default command)
#[derive(Debug, Default, clap::Parser)]
pub struct StartCmd {
    /// Filter strings which override the config file and defaults
    #[clap(help = "tracing filters which override the zakura.toml config")]
    filters: Vec<String>,

    /// Enable zcashd-compat mode and apply zcashd-compat RPC guardrails.
    #[clap(long)]
    zcashd_compat: bool,

    /// Continue startup even when zcashd-compat preflight detects minimum hardware shortfalls.
    #[clap(long = "unsafe-low-specs")]
    unsafe_low_specs: bool,
}

/// Warns if Linux TCP slow-start-after-idle is enabled, which significantly
/// reduces single-peer throughput for block propagation.
///
/// Use `--help` to inspect the available troubleshooting flags.
#[cfg(target_os = "linux")]
fn check_tcp_slow_start_after_idle() {
    const PATH: &str = "/proc/sys/net/ipv4/tcp_slow_start_after_idle";

    let raw = match std::fs::read_to_string(PATH) {
        Ok(raw) => raw,
        Err(error) => {
            debug!(
                ?error,
                path = PATH,
                "could not read TCP sysctl, skipping check"
            );
            return;
        }
    };

    if raw.trim() == "0" {
        return;
    }

    warn!(
        setting = "net.ipv4.tcp_slow_start_after_idle",
        "TCP slow-start-after-idle is enabled, which resets TCP's congestion window \
         between block requests and significantly reduces single-peer throughput for \
         block propagation. \
         Hint: set `net.ipv4.tcp_slow_start_after_idle=0` via sysctl. \
         See https://zebra.zfnd.org/user/troubleshooting.html#linux-tcp-tuning-for-block-propagation"
    );
}

fn use_zakura_block_sync(config: &zakura_network::Config) -> bool {
    config.v2_p2p()
}

/// The tasks spawned by the `start` command, which are all aborted when this is dropped.
#[derive(Default)]
struct NodeTasks {
    /// Tasks that are only aborted on drop.
    tracked: Vec<AbortHandle>,
    /// Tasks whose exit stops the node.
    supervised: Vec<AbortHandle>,
    /// The exit statuses of the supervised tasks.
    supervised_exits: FuturesUnordered<BoxFuture<'static, Result<(), Report>>>,
}

impl NodeTasks {
    /// Aborts `task` when the node tasks are dropped.
    fn track<T>(&mut self, task: &JoinHandle<T>) {
        self.tracked.push(task.abort_handle());
    }

    /// Stops the node when `task` exits, with the status `exit_status` gives its output.
    ///
    /// `name` identifies the task in logs and panic messages. The task is also aborted by
    /// [`Self::abort_supervised`], and when the node tasks are dropped.
    fn supervise<T: Send + 'static>(
        &mut self,
        name: &'static str,
        task: JoinHandle<T>,
        exit_status: impl FnOnce(T) -> Result<(), Report> + Send + 'static,
    ) {
        self.supervised.push(task.abort_handle());
        self.supervised_exits.push(
            async move {
                let output = task.await.unwrap_or_else(|error| {
                    panic!("unexpected panic in the {name} task: {error:?}")
                });
                exit_status(output).map(|()| info!("{name} task exited"))
            }
            .boxed(),
        );
    }

    /// Stops the node without an error when the server `task` exits, and logs the server's result.
    fn supervise_server(
        &mut self,
        name: &'static str,
        task: JoinHandle<Result<(), tower::BoxError>>,
    ) {
        self.supervise(name, task, move |server_result| {
            info!(?server_result, "{name} server stopped");
            Ok(())
        });
    }

    /// Waits for a supervised task to exit, and returns its exit status.
    ///
    /// Returns `None` if every supervised task's exit has already been returned.
    ///
    /// # Panics
    ///
    /// If the task panicked or was aborted.
    async fn next_supervised_exit(&mut self) -> Option<Result<(), Report>> {
        self.supervised_exits.next().await
    }

    /// Aborts the supervised tasks.
    fn abort_supervised(&self) {
        self.supervised.iter().for_each(AbortHandle::abort);
    }
}

impl Drop for NodeTasks {
    fn drop(&mut self) {
        self.tracked
            .iter()
            .chain(&self.supervised)
            .for_each(AbortHandle::abort);
    }
}

/// The exit status of a task that can fail with an error other than a [`Report`].
fn error_exit_status<E: Into<tower::BoxError>>(result: Result<(), E>) -> Result<(), Report> {
    result.map_err(|error| eyre!(error.into()))
}

#[cfg(not(target_os = "linux"))]
fn check_tcp_slow_start_after_idle() {}

impl StartCmd {
    /// Extra time Zakura waits for the zcashd-compat supervisor task beyond the
    /// child's `shutdown_grace_period`. The supervisor's `terminate_child` waits
    /// the full grace period before its SIGKILL last resort, so the outer wait
    /// must be strictly longer or aborting the task races the graceful path.
    const ZCASHD_COMPAT_SHUTDOWN_TIMEOUT_MARGIN: std::time::Duration =
        std::time::Duration::from_secs(30);

    fn validate_consensus_config(config: &ZakuradConfig) -> Result<(), Report> {
        config
            .consensus
            .validate()
            .map_err(|error| eyre!("invalid consensus configuration: {error}"))
    }

    fn validate_debug_blocksync_throughput_config(config: &ZakuradConfig) -> Result<(), Report> {
        let Some(target_height) = config.sync.debug_blocksync_throughput_target_height else {
            return Ok(());
        };

        if !config.network.v2_p2p() {
            return Err(eyre!(
                "sync.debug_blocksync_throughput_target_height requires the Zakura P2P v2 \
                 stack; set network.p2p_stack to \"zakura\" or \"dual\""
            ));
        }

        if target_height > block::Height::MAX.0 {
            return Err(eyre!(
                "sync.debug_blocksync_throughput_target_height={target_height} exceeds the maximum supported block height {}",
                block::Height::MAX.0
            ));
        }

        Ok(())
    }

    fn log_blocksync_throughput_summary(summary: BlocksyncThroughputSummary) {
        let elapsed_secs = summary.elapsed.as_secs_f64().max(f64::EPSILON);
        let blocks_per_second = summary.completed_blocks as f64 / elapsed_secs;
        let bytes_per_second = summary.completed_bytes as f64 / elapsed_secs;

        info!(
            target_height = ?summary.target_height,
            verified_block_tip = ?summary.verified_block_tip,
            completed_blocks = summary.completed_blocks,
            completed_bytes = summary.completed_bytes,
            elapsed_ms = u64::try_from(summary.elapsed.as_millis()).unwrap_or(u64::MAX),
            blocks_per_second,
            bytes_per_second,
            "Zakura block-sync throughput probe reached target height"
        );
    }

    /// Returns the Zakura P2P address supervised zcashd should `-connect` to.
    ///
    /// Uses `zcashd_compat.p2p_connect_addr` when set, otherwise Zakura's bound
    /// legacy P2P listener, substituting loopback for unspecified addresses so
    /// zcashd gets a dialable target on the same host.
    fn zcashd_compat_p2p_connect_addr(
        config: &ZakuradConfig,
        local_listener: SocketAddr,
    ) -> SocketAddr {
        if let Some(addr) = config.zcashd_compat.p2p_connect_addr {
            return addr;
        }

        if local_listener.ip().is_unspecified() {
            SocketAddr::from(([127, 0, 0, 1], local_listener.port()))
        } else {
            local_listener
        }
    }

    /// Returns the services advertised to legacy P2P peers in every storage mode.
    ///
    /// Pruned nodes advertise block service so peers can discover and reconnect to them.
    /// Requests for block bodies outside their retention window still return `notfound`.
    fn advertised_services(config: &ZakuradConfig) -> PeerServices {
        let mut services = PeerServices::NODE_NETWORK;
        if matches!(config.state.storage_mode, StorageMode::Pruned(_)) {
            services |= PeerServices::NODE_PRUNED;
        }
        services
    }

    fn zcashd_compat_default_block_gossip_peer_ips() -> Vec<IpAddr> {
        vec![
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V6(Ipv6Addr::LOCALHOST),
        ]
    }

    /// Returns the supervisor shutdown timeout when zcashd-compat `zcashd` supervision is active.
    ///
    /// This is the configured `shutdown_grace_period` plus a fixed margin, so the
    /// supervisor task always gets to finish its own SIGTERM → grace → SIGKILL
    /// sequence before Zakura gives up on the task.
    fn zcashd_compat_supervisor_shutdown_timeout(
        config: &ZakuradConfig,
    ) -> Option<std::time::Duration> {
        (config.zcashd_compat.enabled && config.zcashd_compat.manage_zcashd).then_some(
            config
                .zcashd_compat
                .shutdown_grace_period
                .saturating_add(Self::ZCASHD_COMPAT_SHUTDOWN_TIMEOUT_MARGIN),
        )
    }

    pub(crate) async fn start(
        &self,
        config: Arc<ZakuradConfig>,
        custom_services: Vec<zakura_network::zakura::CustomService>,
        ready: Option<tokio::sync::oneshot::Sender<crate::node::NodeServices>>,
        shutdown: CancellationToken,
        shutdown_cleanup_required: CancellationToken,
    ) -> Result<(), Report> {
        check_tcp_slow_start_after_idle();

        let is_regtest = config.network.network.is_regtest();

        let config = if is_regtest {
            Arc::new(ZakuradConfig {
                mempool: mempool::Config {
                    debug_enable_at_height: Some(0),
                    ..config.mempool
                },
                ..Arc::unwrap_or_clone(config)
            })
        } else {
            config
        };

        if !config.zcashd_compat.enabled && !config.zcashd_compat.block_gossip_peer_ips.is_empty() {
            return Err(eyre!(
                "zcashd_compat.block_gossip_peer_ips requires zcashd_compat.enabled = true"
            ));
        }

        let zcashd_compat_block_gossip_peer_ips = if config.zcashd_compat.enabled {
            if config.zcashd_compat.block_gossip_peer_ips.is_empty() {
                Self::zcashd_compat_default_block_gossip_peer_ips()
            } else {
                config.zcashd_compat.block_gossip_peer_ips.clone()
            }
        } else {
            Vec::new()
        };

        Self::validate_consensus_config(&config)?;
        Self::validate_debug_blocksync_throughput_config(&config)?;
        config.rpc.validate().map_err(|error| eyre!(error))?;
        let backend_verifications = config
            .sync
            .backend_verification_total(&config.network.network)
            .map_err(|error| eyre!(error))?;
        info!(
            backend_verifications,
            "checked the backend verification bound"
        );

        if config.zcashd_compat.enabled {
            zcashd_compat::run_preflight(&config, self.unsafe_low_specs)?;
        }

        let resolved_zcashd_path = if config.zcashd_compat.enabled
            && config.zcashd_compat.manage_zcashd
        {
            let zcashd_compat_config = config.zcashd_compat.clone();
            let state_cache_dir = config.state.cache_dir.clone();
            Some(
                tokio::task::spawn_blocking(move || {
                    zcashd_compat::resolve_zcashd_binary_path(
                        &zcashd_compat_config,
                        &state_cache_dir,
                    )
                })
                .await
                .map_err(|err| eyre!("failed to join managed zcashd binary resolver: {err}"))??,
            )
        } else {
            None
        };

        info!("initializing node state");

        // Surface a misconfigured storage mode as a clean startup error, before the
        // (potentially slow) database open panics deep inside the state service.
        config
            .state
            .validate_storage_mode(&config.network.network)
            .map_err(|error| eyre!("invalid state storage configuration: {error}"))?;

        let (_, max_checkpoint_height) = zakura_consensus::router::init_checkpoint_list(
            config.consensus.clone(),
            &config.network.network,
        );

        if config.network.v2_p2p() {
            // Report boundaries from this binary's effective network configuration.
            for (upgrade, metric) in [
                (
                    zakura_chain::parameters::NetworkUpgrade::Sapling,
                    "sync.report.sapling.height",
                ),
                (
                    zakura_chain::parameters::NetworkUpgrade::Nu6_3,
                    "sync.report.ironwood.height",
                ),
            ] {
                if let Some(height) = upgrade.activation_height(&config.network.network) {
                    metrics::gauge!(metric).set(f64::from(height.0));
                }
            }
            metrics::gauge!("sync.report.checkpoint.height")
                .set(f64::from(max_checkpoint_height.0));
            for metric in [
                "sync.block.payload.received.bytes",
                "state.vct.fast.block.count",
                "state.vct.legacy.block.count",
            ] {
                metrics::counter!(metric).increment(0);
            }
            #[cfg(feature = "sync-metrics")]
            metrics::counter!("sync.block.payload.committed.bytes").increment(0);
        }

        info!("opening database, this may take a few minutes");

        let state_config = config.mirrored_state_config();

        let (
            zakura_state::InitializedState {
                state: mut state_service,
                read_state: read_only_state_service,
                latest_chain_tip,
                chain_tip_change,
                block_announcements,
            },
            header_chain_body_evidence,
        ) = zakura_state::init_with_header_chain_body_evidence(
            state_config,
            &config.network.network,
            max_checkpoint_height,
            config.sync.checkpoint_verify_concurrency_limit
                * (VERIFICATION_PIPELINE_SCALING_MULTIPLIER + 1),
        )
        .boxed()
        .await
        .map_err(|error| eyre!("state initialization failed: {error}"))?;

        state_service
            .ready()
            .await
            .map_err(|error| eyre!("state service startup handoff failed: {error}"))?;

        info!("logging database metrics on startup");
        read_only_state_service.log_db_metrics();

        let (blocksync_throughput_probe, mut blocksync_throughput_completion_rx) =
            if let Some(target_height) = config.sync.debug_blocksync_throughput_target_height {
                let target_height = block::Height(target_height);
                let initial_frontiers = query_block_sync_frontiers(
                    read_only_state_service.clone(),
                    latest_chain_tip.clone(),
                )
                .boxed()
                .await
                .unwrap_or(zakura_network::zakura::BlockSyncFrontiers {
                    finalized_height: block::Height(0),
                    verified_block_tip: block::Height(0),
                    verified_block_hash: config.network.network.genesis_hash(),
                });
                info!(
                    ?target_height,
                    ?initial_frontiers,
                    "Zakura block-sync throughput probe enabled"
                );
                let (probe, completion_rx) =
                    BlocksyncThroughputProbe::new(initial_frontiers, target_height);
                (Some(probe), Some(completion_rx))
            } else {
                (None, None)
            };

        let state = spawn_buffer(state_service, Self::state_buffer_bound(&config));

        let zakura_bootstrap_snapshots = config
            .network
            .v2_p2p()
            .then(|| read_only_state_service.subscribe_header_chain_snapshots());
        let zakura_bootstrap_runtime_status = config
            .network
            .v2_p2p()
            .then(|| read_only_state_service.subscribe_header_runtime_status());
        let legacy_bootstrap_needed = zakura_bootstrap_runtime_status
            .as_ref()
            .is_some_and(|status| !status.borrow().is_ready());
        let zakura_block_sync_handoff = if legacy_bootstrap_needed {
            zakura::SyncCoordinator::new_legacy_bootstrap()
        } else {
            zakura::SyncCoordinator::new()
        };

        let zakura_header_sync_driver_startup = if config.network.v2_p2p() {
            Some(
                zakura_header_sync_driver_startup(
                    state.clone(),
                    read_only_state_service.clone(),
                    header_chain_body_evidence.clone(),
                    &config.network.network,
                    &zakura_block_sync_handoff,
                )
                .boxed()
                .await?,
            )
        } else {
            None
        };

        let mut node_tasks = NodeTasks::default();
        // Cancel detached tasks before dropping the state services, whose teardown blocks.
        let _shutdown_on_drop = shutdown.clone().drop_guard();

        if let Some(task) = crate::components::tracing::raw_endpoint::init(
            config.tracing.raw_endpoint_addr,
            config.network.zakura.trace_dir.clone(),
            shutdown.clone(),
        )
        .await?
        {
            node_tasks.track(&task);
        }

        info!("initializing network");
        // The service that our node uses to respond to requests by peers. The
        // load_shed middleware ensures that we reduce the size of the peer set
        // in response to excess load.
        //
        // # Security
        //
        // This layer stack is security-sensitive, modifying it can cause hangs,
        // or enable denial of service attacks.
        //
        // See `zakura_network::Connection::drive_peer_request()` for details.
        let (setup_tx, setup_rx) = oneshot::channel();
        let zcashd_compat_pruning_retention = config
            .zcashd_compat
            .enabled
            .then(|| config.state.pruning_config())
            .flatten()
            .map(|pruning| pruning.tx_retention);
        let pending_blocks = zakura_rpc::PendingBlockRegistry::default();
        let early_relay = config
            .network
            .compact_block_relay
            .then(|| zakura_consensus::EarlyRelay::new(latest_chain_tip.clone()));
        let compact_relay = config.network.compact_block_relay.then(|| {
            inbound::compact::CompactRelay::new(
                config.network.network.clone(),
                tower::util::BoxCloneSyncService::new(read_only_state_service.clone()),
            )
        });
        let inbound = LoadShed::new(spawn_buffer(
            Timeout::new(
                Inbound::new(
                    config.sync.full_verify_concurrency_limit,
                    config.network.expose_peer_addresses,
                    zcashd_compat_pruning_retention,
                    zcashd_compat_block_gossip_peer_ips.clone(),
                    setup_rx,
                )
                .with_pending_blocks(pending_blocks.clone())
                .with_early_relay(early_relay.clone())
                .with_compact_relay(compact_relay),
                MAX_INBOUND_RESPONSE_TIME,
            ),
            inbound::downloads::MAX_INBOUND_CONCURRENCY,
        ));

        let advertised_services = Self::advertised_services(&config);

        let (peer_set, address_book, misbehavior, zakura_endpoint, block_adverts) =
            zakura_network::init_with_zakura(
                config.network.clone(),
                inbound,
                latest_chain_tip.clone(),
                user_agent(),
                advertised_services,
                zcashd_compat_block_gossip_peer_ips.clone(),
                zakura_header_sync_driver_startup,
                custom_services,
            )
            .await
            .map_err(|error| eyre!(error))?;

        let mut header_sync_fatal_events = match zakura_endpoint.as_ref() {
            Some(endpoint) => endpoint.take_header_sync_fatal_events().await,
            None => None,
        };
        let mut block_sync_fatal_events = None;

        // Not added to node_tasks, because it must outlive start() being dropped to shutdown the
        // endpoint
        let zakura_endpoint_shutdown_task = if let Some(endpoint) = zakura_endpoint.clone() {
            shutdown_cleanup_required.cancel();

            let shutdown = shutdown.clone();
            Some(spawn_erased(async move {
                shutdown.cancelled().await;
                endpoint.shutdown().await;
            }))
        } else {
            None
        };

        // Start health server if configured (after sync_status is available)

        info!("initializing verifiers");
        let (tx_verifier_setup_tx, tx_verifier_setup_rx) = oneshot::channel();
        let (block_verifier_router, tx_verifier, consensus_task_handles, max_checkpoint_height) =
            zakura_consensus::router::init_with_read_state(
                config.consensus.clone(),
                &config.network.network,
                state.clone(),
                read_only_state_service.clone(),
                tx_verifier_setup_rx,
                early_relay.clone(),
            )
            .boxed()
            .await;
        node_tasks.track(&consensus_task_handles.state_checkpoint_verify_handle);

        if let Some(endpoint) = zakura_endpoint.clone() {
            let trace = endpoint.trace();
            if endpoint.header_sync().is_some() {
                if let (Some(block_sync), Some(block_actions)) = (
                    endpoint.block_sync(),
                    endpoint.take_block_sync_actions().await,
                ) {
                    let (block_sync_fatal_tx, block_sync_fatal_rx) = mpsc::unbounded_channel();
                    block_sync_fatal_events = Some(block_sync_fatal_rx);
                    let block_driver_shutdown = shutdown.clone();
                    let block_driver_task = spawn_erased(
                        supervise_block_sync_driver(
                            drive_block_sync_actions(
                                block_actions,
                                endpoint.supervisor(),
                                Some(endpoint.clone()),
                                block_sync.clone(),
                                latest_chain_tip.clone(),
                                read_only_state_service.clone(),
                                Some(tower::util::BoxCloneService::new(state.clone())),
                                Some(header_chain_body_evidence.clone()),
                                block_verifier_router.clone(),
                                max_checkpoint_height,
                                config.sync.checkpoint_verify_concurrency_limit,
                                config.sync.full_verify_concurrency_limit,
                                config.sync.zakura_block_apply_concurrency_limit,
                                trace.clone(),
                                blocksync_throughput_probe.clone(),
                                zakura_block_sync_handoff.clone(),
                                shutdown.clone().cancelled_owned(),
                            ),
                            block_sync_fatal_tx,
                            block_driver_shutdown,
                        )
                        .in_current_span(),
                    );
                    endpoint.push_block_sync_task(block_driver_task).await;
                }
            }
        }

        info!("initializing syncer");
        let (mut syncer, sync_status) = ChainSync::new(
            &config,
            max_checkpoint_height,
            peer_set.clone(),
            block_verifier_router.clone(),
            state.clone(),
            latest_chain_tip.clone(),
            misbehavior.clone(),
        );

        // The mempool and `getblocktemplate` trace the path from a new tip to a mining template.
        // They share one writer.
        let template_path_trace = JsonlEventEmitter::new(
            config
                .network
                .zakura
                .trace_dir
                .clone()
                .map(JsonlTracer::spawn)
                .unwrap_or_else(JsonlTracer::noop),
            zakura_jsonl_trace::node_id(),
        );

        info!("initializing mempool");
        let (mempool, mempool_transaction_subscriber) = Mempool::new(
            &config.mempool,
            config.network.expose_peer_addresses,
            peer_set.clone(),
            state.clone(),
            tower::util::BoxCloneService::new(read_only_state_service.clone()),
            tx_verifier,
            sync_status.clone(),
            latest_chain_tip.clone(),
            chain_tip_change.clone(),
            misbehavior.clone(),
            zcashd_compat_block_gossip_peer_ips.clone(),
            config.network.unconditional_peers.clone(),
        );
        let mempool = mempool.with_trace(template_path_trace.clone());
        let mempool_verification_finished = mempool.verification_finished();
        let mempool = spawn_buffer(
            BoxService::new(mempool),
            mempool::downloads::MAX_INBOUND_CONCURRENCY,
        );

        if tx_verifier_setup_tx.send(mempool.clone()).is_err() {
            warn!("error setting up the transaction verifier with a handle to the mempool service");
        };

        info!("fully initializing inbound peer request handler");
        // Fully start the inbound service as soon as possible
        let setup_data = InboundSetupData {
            address_book: address_book.clone(),
            block_download_peer_set: peer_set.clone(),
            block_verifier: block_verifier_router.clone(),
            mempool: mempool.clone(),
            state: state.clone(),
            latest_chain_tip: latest_chain_tip.clone(),
            network: config.network.network.clone(),
            misbehavior,
        };
        setup_tx
            .send(setup_data)
            .map_err(|_| eyre!("could not send setup data to inbound service"))?;
        // And give it time to clear its queue
        tokio::task::yield_now().await;

        // Create a channel to send mined blocks to the gossip task
        let submit_block_channel = EarlyMinedBlockChannel::new();

        // Launch RPC server
        let (rpc_impl, rpc_tx_queue_handle) = zakura_rpc::node::new_rpc_impl(
            config.network.network.clone(),
            config.mining.clone(),
            config.rpc.debug_force_finished_sync,
            build_version().to_string(),
            user_agent(),
            zakura_rpc::node::NodeRpcServices {
                mempool: mempool.clone(),
                state: state.clone(),
                read_state: read_only_state_service.clone(),
                block_verifier_router: block_verifier_router.clone(),
                sync_status: zakura_rpc::node::NodeSyncStatus::new(sync_status.clone()),
                latest_chain_tip: latest_chain_tip.clone(),
                address_book: address_book.clone(),
            },
            LAST_WARN_ERROR_LOG_SENDER.subscribe(),
            Some(submit_block_channel.sender()),
            pending_blocks,
            sync::end_of_support::end_of_support_height(&config.network.network),
        );
        let rpc_impl = rpc_impl
            .with_template_trace(template_path_trace)
            .with_local_services(config.network.handshake_services(advertised_services));
        node_tasks.supervise("rpc transaction queue", rpc_tx_queue_handle, Ok);

        let node_services = ready.as_ref().map(|_| crate::node::NodeServices {
            read_state: read_only_state_service.clone(),
            latest_chain_tip: latest_chain_tip.clone(),
            chain_tip_change: chain_tip_change.clone(),
            sync_status: sync_status.clone(),
            mempool: mempool.clone(),
        });

        if config.rpc.listen_addr.is_some() {
            let rpc_task_handle = zakura_rpc::node::start_server(&rpc_impl, config.rpc.clone())
                .await
                .expect("server should start");
            node_tasks.supervise_server("rpc", rpc_task_handle);
        }

        if config.rpc.admin_listen_addr.is_some() {
            let admin_rpc_task_handle =
                zakura_rpc::node::start_admin_server(&rpc_impl, config.rpc.clone())
                    .await
                    .expect("admin server should start");
            node_tasks.supervise_server("admin rpc", admin_rpc_task_handle);
        }

        let zcashd_compat_shutdown_timeout =
            Self::zcashd_compat_supervisor_shutdown_timeout(&config);
        let zcashd_compat_supervisor_config =
            if let Some(resolved_zcashd_path) = resolved_zcashd_path {
                let local_listener = address_book
                    .lock()
                    .expect("unexpected panic in address book mutex guard")
                    .local_listener_socket_addr();
                let supervisor_config = zcashd_compat::SupervisorConfig::new(
                    &config.zcashd_compat,
                    resolved_zcashd_path,
                    &config.state.cache_dir,
                    config.network.network.kind(),
                    Self::zcashd_compat_p2p_connect_addr(&config, local_listener),
                );

                info!(
                    connect = %supervisor_config.zakura_p2p_addr,
                    "zcashd-compat source enabled"
                );

                Some(supervisor_config)
            } else {
                if config.zcashd_compat.enabled {
                    zcashd_compat::set_supervision_config_disabled_metrics();
                    info!("zcashd-compat source enabled: zcashd supervision disabled");
                }

                None
            };

        // TODO: Add a shutdown signal and start the server with `serve_with_incoming_shutdown()` if
        //       any related unit tests sometimes crash with memory errors
        match config.rpc.indexer_listen_addr {
            #[cfg(feature = "indexer-rpc")]
            Some(indexer_listen_addr) => {
                info!("spawning indexer RPC server");
                let (indexer_rpc_task_handle, _listen_addr) =
                    zakura_rpc::node::start_indexer_server(
                        indexer_listen_addr,
                        config.rpc.indexer_tls.clone(),
                        read_only_state_service.clone(),
                        latest_chain_tip.clone(),
                        mempool_transaction_subscriber.clone(),
                    )
                    .await
                    .map_err(|err| eyre!(err))?;

                node_tasks.supervise_server("indexer rpc", indexer_rpc_task_handle);
            }
            #[cfg(not(feature = "indexer-rpc"))]
            Some(_) => {
                return Err(eyre!(
                    "rpc.indexer_listen_addr is configured, but this zakurad was built \
                     without the `indexer-rpc` feature"
                ));
            }
            None => {
                #[cfg(feature = "indexer-rpc")]
                warn!("configure an indexer_listen_addr to start the indexer RPC server");
            }
        }

        // Start concurrent tasks which don't add load to other tasks
        info!("spawning block gossip task");
        let block_gossip_task_handle = spawn_erased(
            sync::gossip_best_tip_block_hashes(
                sync_status.clone(),
                latest_chain_tip.clone(),
                block_announcements,
                block_adverts.clone(),
                read_only_state_service.clone(),
                Some(submit_block_channel.receiver()),
            )
            .in_current_span(),
        );
        node_tasks.supervise(
            "chain tip block gossip",
            block_gossip_task_handle,
            error_exit_status,
        );

        if let Some(early_relay) = early_relay {
            info!("spawning early-relay block gossip task");
            let early_gossip_task_handle = spawn_erased(
                sync::gossip_early_relay_blocks(sync_status.clone(), early_relay, block_adverts)
                    .in_current_span(),
            );
            node_tasks.track(&early_gossip_task_handle);
        }

        info!("spawning mempool queue checker task");
        let mempool_queue_checker_task_handle =
            mempool::QueueChecker::spawn(mempool.clone(), mempool_verification_finished);
        node_tasks.supervise(
            "mempool queue checker",
            mempool_queue_checker_task_handle,
            error_exit_status,
        );

        info!("spawning mempool transaction gossip task");
        let tx_gossip_task_handle = spawn_erased(
            mempool::run_mempool_transaction_id_gossip(
                mempool_transaction_subscriber.subscribe(),
                peer_set.clone(),
                mempool.clone(),
            )
            .in_current_span(),
        );
        node_tasks.supervise(
            "transaction gossip",
            tx_gossip_task_handle,
            error_exit_status,
        );

        info!("spawning delete old databases task");
        let mut old_databases_task_handle = zakura_state::check_and_delete_old_state_databases(
            &config.state,
            &config.network.network,
        );
        node_tasks.track(&old_databases_task_handle);

        info!("spawning progress logging task");
        let (chain_tip_metrics_sender, chain_tip_metrics_receiver) =
            health::ChainTipMetrics::channel();
        let progress_task_handle = spawn_erased(
            show_block_chain_progress(
                config.network.network.clone(),
                latest_chain_tip.clone(),
                sync_status.clone(),
                chain_tip_metrics_sender,
            )
            .in_current_span(),
        );
        // The progress task runs forever, unless it panics.
        node_tasks.supervise("chain progress", progress_task_handle, |never| never);

        // Start health server if configured
        info!("initializing health endpoints");
        let (health_task_handle, _) = health::init(
            config.health.clone(),
            config.network.network.clone(),
            chain_tip_metrics_receiver,
            sync_status.clone(),
            address_book.clone(),
        )
        .boxed()
        .await;
        node_tasks.track(&health_task_handle);

        // Spawn never ending end of support task.
        info!("spawning end of support checking task");
        let end_of_support_task_handle = spawn_erased(
            sync::end_of_support::start(config.network.network.clone(), latest_chain_tip.clone())
                .in_current_span(),
        );
        node_tasks.supervise("end of support", end_of_support_task_handle, |result| {
            result
        });

        // Give the inbound service more time to clear its queue,
        // then start concurrent tasks that can add load to the inbound service
        // (by opening more peer connections, so those peers send us requests)
        tokio::task::yield_now().await;

        // The crawler only activates immediately in tests that use mempool debug mode
        info!("spawning mempool crawler task");
        let mempool_crawler_task_handle = mempool::Crawler::spawn(
            &config.mempool,
            peer_set,
            mempool.clone(),
            sync_status.clone(),
            chain_tip_change.clone(),
        );
        node_tasks.supervise(
            "mempool crawler",
            mempool_crawler_task_handle,
            error_exit_status,
        );

        info!("spawning syncer task");
        // In regtest, commit the genesis block directly (bypassing the syncer's genesis
        // download, which requires a connected peer). Then run the syncer normally so
        // that multi-hop block propagation works: gossiped blocks that arrive out of
        // order (e.g. only the latest tip hash was gossiped) will be recovered by the
        // syncer using block locators within REGTEST_SYNC_RESTART_DELAY (2 seconds).
        //
        // `debug_skip_regtest_genesis_self_seed` opts a node out of this shortcut so it
        // downloads genesis from a peer instead, exercising the production
        // genesis-bootstrap path (e.g. a Zakura-only node fetching genesis over Zakura).
        if is_regtest
            && !config.sync.debug_skip_regtest_genesis_self_seed
            && !syncer
                .state_contains(config.network.network.genesis_hash())
                .await?
        {
            let genesis_hash = block_verifier_router
                .clone()
                .oneshot(zakura_consensus::Request::Commit(
                    regtest_genesis_block().into(),
                ))
                .await
                .expect("should validate Regtest genesis block");

            assert_eq!(
                genesis_hash,
                config.network.network.genesis_hash(),
                "validated block hash should match network genesis hash"
            )
        }
        let syncer_task_handle = if use_zakura_block_sync(&config.network) {
            info!(
                "legacy-compatible genesis fetch will complete before Zakura block sync takes \
                 ownership at height 1"
            );
            // Only dual-stack nodes (Zakura + legacy peers) fall back to legacy ChainSync on a
            // Zakura stall; a Zakura-only node has no legacy peers to drive body sync. The
            // fallback resumes legacy ChainSync as the body-sync driver while the Zakura
            // reactors stay alive as a serving/advertising bridge for zakura-only peers.
            let legacy_fallback = config.network.v2_p2p() && config.network.legacy_p2p();
            spawn_erased(
                syncer
                    .bootstrap_genesis_then_pause(
                        read_only_state_service.clone(),
                        zakura_bootstrap_snapshots
                            .expect("Zakura block sync has a durable snapshot receiver"),
                        zakura_bootstrap_runtime_status
                            .expect("Zakura block sync has a header-runtime status receiver"),
                        legacy_fallback,
                        zakura_block_sync_handoff.clone(),
                    )
                    .in_current_span(),
            )
        } else {
            spawn_erased(syncer.sync().in_current_span())
        };
        node_tasks.supervise("syncer", syncer_task_handle, |result| result);

        // And finally, spawn the internal Zcash miner, if it is compiled in and enabled.
        #[cfg(feature = "internal-miner")]
        if config.mining.is_internal_miner_enabled() {
            info!("spawning Zcash miner");
            let miner_task_handle = components::miner::spawn_init(&config.metrics, rpc_impl);
            node_tasks.supervise("miner", miner_task_handle, |result| result);
        }

        info!("spawned initial Zakura tasks");

        let (zcashd_compat_shutdown_tx, zcashd_compat_shutdown_rx) = watch::channel(false);
        let mut zcashd_compat_task_handle =
            if let Some(supervisor_config) = zcashd_compat_supervisor_config {
                spawn_erased(
                    zcashd_compat::run_supervisor(supervisor_config, zcashd_compat_shutdown_rx)
                        .in_current_span(),
                )
            } else {
                spawn_erased(std::future::pending().in_current_span())
            };
        // The supervisor may own a child after this point, so shutdown must
        // await cleanup. Do not add a yield between the spawn and this marker.
        shutdown_cleanup_required.cancel();

        if let Some((ready, services)) = ready.zip(node_services) {
            let _ = ready.send(services);
        }

        // startup tasks
        let BackgroundTaskHandles {
            mut state_checkpoint_verify_handle,
        } = consensus_task_handles;

        let state_checkpoint_verify_handle_fused = (&mut state_checkpoint_verify_handle).fuse();
        pin!(state_checkpoint_verify_handle_fused);

        let old_databases_task_handle_fused = (&mut old_databases_task_handle).fuse();
        pin!(old_databases_task_handle_fused);

        let writer_health = read_only_state_service.clone();
        let writer_failure = writer_health.wait_for_writer_failure();
        tokio::pin!(writer_failure);

        // Wait for tasks to finish
        let mut zcashd_compat_task_finished = false;
        let exit_status = {
            let zcashd_compat_task_handle_fused = (&mut zcashd_compat_task_handle).fuse();
            pin!(zcashd_compat_task_handle_fused);

            loop {
                let mut exit_when_task_finishes = true;

                let result = select! {
                _ = shutdown.cancelled() => Ok(()),

                failure = &mut writer_failure => {
                    tracing::error!(%failure, "block writer failed; terminating the node");
                    shutdown.cancel();
                    Err(eyre!(failure))
                },

                header_sync_fatal_event = async {
                    match header_sync_fatal_events.as_mut() {
                        Some(events) => events.recv().await,
                        None => std::future::pending().await,
                    }
                } => match header_sync_fatal_event {
                    Some(event) => Self::handle_header_sync_fatal_event(event, &shutdown),
                    None => {
                        header_sync_fatal_events = None;
                        exit_when_task_finishes = false;
                        Ok(())
                    }
                },

                block_sync_fatal_event = async {
                    match block_sync_fatal_events.as_mut() {
                        Some(events) => events.recv().await,
                        None => std::future::pending().await,
                    }
                } => match block_sync_fatal_event {
                    Some(()) => Self::handle_block_sync_driver_exit(&shutdown),
                    None => {
                        block_sync_fatal_events = None;
                        exit_when_task_finishes = false;
                        Ok(())
                    }
                },

                // An ongoing task exited. There is always a syncer task.
                Some(supervised_exit) = node_tasks.next_supervised_exit() => supervised_exit,

                blocksync_throughput_result = async {
                    blocksync_throughput_completion_rx
                        .as_mut()
                        .expect("throughput completion branch is only enabled when receiver exists")
                        .await
                }, if blocksync_throughput_completion_rx.is_some() => {
                    let summary = blocksync_throughput_result
                        .map_err(|_| eyre!("Zakura block-sync throughput probe completion sender dropped before target height"))?;
                    Self::log_blocksync_throughput_summary(summary);
                    Ok(())
                }

                // We also expect the state checkpoint verify task to finish.
                state_checkpoint_verify_result = &mut state_checkpoint_verify_handle_fused => {
                    state_checkpoint_verify_result
                        .unwrap_or_else(|_| panic!(
                            "unexpected panic checking previous state followed the best chain"));

                    exit_when_task_finishes = false;
                    Ok(())
                }

                // And the old databases task should finish while Zakura is running.
                old_databases_result = &mut old_databases_task_handle_fused => {
                    old_databases_result
                        .unwrap_or_else(|_| panic!(
                            "unexpected panic deleting old database directories"));

                    exit_when_task_finishes = false;
                    Ok(())
                }

                    zcashd_compat_result = &mut zcashd_compat_task_handle_fused => {
                        zcashd_compat_task_finished = true;
                        exit_when_task_finishes =
                            Self::zcashd_compat_supervisor_should_exit(zcashd_compat_result);
                        Ok(())
                    },
                };

                // Stop Zakura if a task finished and returned an error,
                // or if an ongoing task exited.
                if let Err(err) = result {
                    break Err(err);
                }

                if exit_when_task_finishes {
                    break Ok(());
                }
            }
        };

        info!("exiting Zakura: asking other tasks to stop");
        shutdown.cancel();
        zakura_chain::shutdown::set_shutting_down();

        // ongoing tasks
        node_tasks.abort_supervised();
        health_task_handle.abort();
        if let Some(zakura_endpoint_shutdown_task) = zakura_endpoint_shutdown_task {
            zakura_endpoint_shutdown_task
                .await
                .expect("unexpected panic in the Zakura endpoint shutdown task");
        }
        if zcashd_compat_task_finished {
            debug!("zcashd-compat supervisor task already exited before shutdown");
        } else if let Some(zcashd_compat_shutdown_timeout) = zcashd_compat_shutdown_timeout {
            info!(
                ?zcashd_compat_shutdown_timeout,
                "requesting zcashd-compat supervisor shutdown"
            );
            if zcashd_compat_shutdown_tx.send(true).is_err() {
                warn!("zcashd-compat supervisor shutdown request was not delivered");
            }
            if tokio::time::timeout(
                zcashd_compat_shutdown_timeout,
                &mut zcashd_compat_task_handle,
            )
            .await
            .is_err()
            {
                warn!(
                    ?zcashd_compat_shutdown_timeout,
                    "zcashd-compat supervisor did not finish before shutdown timeout; \
                     abandoning child process handle"
                );
                // The supervisor spawns zcashd without kill_on_drop, so this
                // abort abandons an already-signalled child rather than
                // SIGKILLing it mid-flush.
                zcashd_compat_task_handle.abort();
            }
        } else {
            debug!("aborting zcashd-compat supervisor task without managed child shutdown");
            zcashd_compat_task_handle.abort();
        }

        // startup tasks
        state_checkpoint_verify_handle.abort();
        old_databases_task_handle.abort();

        info!(
            "exiting Zakura: all tasks have been asked to stop, waiting for remaining tasks to finish"
        );

        Self::finish_shutdown(exit_status, writer_health.writer_failure())
    }

    fn finish_shutdown(
        exit_status: Result<(), Report>,
        writer_failure: Option<zakura_state::BoxError>,
    ) -> Result<(), Report> {
        // Another ready task can win the select after the writer publishes its failure.
        exit_status?;
        if let Some(failure) = writer_failure {
            tracing::error!(%failure, "block writer failed during shutdown");
            Err(eyre!(failure))
        } else {
            Ok(())
        }
    }

    /// Returns `false` so Zakura keeps running if zcashd-compat supervision exits unexpectedly.
    fn zcashd_compat_supervisor_should_exit(
        zcashd_compat_result: Result<Result<(), Report>, tokio::task::JoinError>,
    ) -> bool {
        zcashd_compat::set_supervision_unexpectedly_disabled_metrics();

        match zcashd_compat_result {
            Ok(Ok(())) => {
                warn!(
                    "zcashd-compat supervisor task exited unexpectedly in supervision mode; continuing without zcashd supervision"
                );
            }
            Ok(Err(err)) => {
                warn!(
                    ?err,
                    "zcashd-compat supervisor task failed in supervision mode; continuing without zcashd supervision"
                );
            }
            Err(join_err) => {
                warn!(
                    ?join_err,
                    "zcashd-compat supervisor task panicked in supervision mode; continuing without zcashd supervision"
                );
            }
        }

        false
    }

    fn handle_header_sync_fatal_event(
        event: zakura_network::zakura::HeaderSyncFatalEvent,
        shutdown: &CancellationToken,
    ) -> Result<(), Report> {
        shutdown.cancel();
        Err(eyre!(event))
    }

    fn handle_block_sync_driver_exit(shutdown: &CancellationToken) -> Result<(), Report> {
        shutdown.cancel();
        Err(eyre!(
            "critical Zakura block-sync driver exited unexpectedly"
        ))
    }

    /// Returns the bound for the state service buffer,
    /// based on the configurations of the services that use the state concurrently.
    fn state_buffer_bound(config: &ZakuradConfig) -> usize {
        // Ignore the checkpoint verify limit, because it is very large.
        // Mempool read traffic bypasses this buffer.
        //
        // TODO: do we also need to account for concurrent use across services?
        //       we could multiply the maximum by 3/2, or add a fixed constant
        [
            config.sync.download_concurrency_limit,
            config.sync.full_verify_concurrency_limit,
            inbound::downloads::MAX_INBOUND_CONCURRENCY,
        ]
        .into_iter()
        .max()
        .unwrap()
    }
}

/// Returns an error for a config that `start` can't run with.
///
/// The message keeps the "I/O operation failed" prefix that `zakurad` has always printed
/// for these errors, so existing log searches keep matching.
fn config_error(error: impl std::fmt::Display) -> Report {
    eyre!("I/O operation failed: {error}")
}

impl StartCmd {
    /// Creates the async runtime, runs the node on it until a shutdown signal or a fatal
    /// error, then stops the runtime.
    ///
    /// Returns the node's error after logging it.
    pub(crate) fn run(&self, config: Arc<ZakuradConfig>) -> Result<(), Report> {
        let runtime = new_runtime(config.mirrored_state_config().read_worker_count());

        info!("Starting zakurad");

        runtime.run_with_graceful_shutdown(|shutdown, shutdown_cleanup_required| {
            self.start(
                config,
                Vec::new(),
                None,
                shutdown,
                shutdown_cleanup_required,
            )
        })?;

        info!("stopping zakurad");

        Ok(())
    }

    /// Process the given command line options, overriding settings from
    /// a configuration file using explicit flags taken from command-line
    /// arguments, then checks the resulting config.
    pub(crate) fn override_config(
        &self,
        mut config: ZakuradConfig,
    ) -> Result<ZakuradConfig, Report> {
        if !self.filters.is_empty() {
            config.tracing.filter = Some(self.filters.join(","));
        }

        // `--zcashd-compat` is a one-way override that enables zcashd-compat mode.
        // The actual zcashd-compat guardrails are applied below using
        // `config.zcashd_compat.enabled` so CLI and config-file activation share one path.
        if self.zcashd_compat {
            config.zcashd_compat.enabled = true;
        }

        if !config.zcashd_compat.enabled && !config.zcashd_compat.block_gossip_peer_ips.is_empty() {
            return Err(config_error(
                "zcashd_compat.block_gossip_peer_ips requires zcashd_compat.enabled = true",
            ));
        }

        if config.zcashd_compat.enabled {
            if !config.network.legacy_p2p() {
                return Err(config_error(
                    "zcashd-compat P2P sidecar mode requires the legacy Zcash P2P stack, \
                     because zcashd syncs from Zakura over it; set network.p2p_stack to \
                     \"legacy\" or \"dual\"",
                ));
            }

            if config.zcashd_compat.manage_zcashd {
                zcashd_compat::reject_peer_selection_extra_args(
                    &config.zcashd_compat.zcashd_extra_args,
                )
                .map_err(config_error)?;
            }

            if config.zcashd_compat.manage_zcashd {
                match zcashd_compat::effective_zcashd_source(&config.zcashd_compat) {
                    Ok(zcashd_compat::ZcashdBinarySource::Path(path))
                        if !zcashd_compat::is_command_resolvable(Path::new(&path)) =>
                    {
                        return Err(config_error(format!(
                            "zcashd-compat mode could not resolve zcashd_path={}",
                            path.display()
                        )));
                    }
                    Ok(_) => {}
                    Err(err) => return Err(config_error(err)),
                }
            }
        }

        Self::validate_consensus_config(&config).map_err(config_error)?;
        Self::validate_debug_blocksync_throughput_config(&config).map_err(config_error)?;
        config.rpc.validate().map_err(config_error)?;

        Ok(config)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod zakura_header_sync_driver_tests;
