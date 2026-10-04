//! In-process Zakura node lifecycle.

use abscissa_core::config::Override;
use color_eyre::Report;
use tokio_util::sync::CancellationToken;

use crate::{commands::StartCmd, components::tokio::run_until_shutdown, config::ZakuradConfig};

pub use zakura_network::zakura::CustomService;

/// Initialized services for an embedding application.
#[derive(Clone)]
pub struct NodeServices {
    /// Queries against the current best chain.
    pub read_state: zakura_state::ReadStateService,
    /// The current best chain tip.
    pub latest_chain_tip: zakura_state::LatestChainTip,
    /// An independent listener for best chain tip changes.
    pub chain_tip_change: zakura_state::ChainTipChange,
    /// The synchronizer's recent-tip status.
    pub sync_status: crate::components::sync::SyncStatus,
    /// Buffered access to the node's mempool.
    pub mempool: tower::buffer::Buffer<
        tower::util::BoxService<
            zakura_node_services::mempool::Request,
            zakura_node_services::mempool::Response,
            crate::BoxError,
        >,
        zakura_node_services::mempool::Request,
    >,
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
    run_with_services_inner(config, custom_services, shutdown, None).await
}

/// Runs a node and sends its service handles after all initial tasks have started.
///
/// The receiver closes without a value if startup fails or is cancelled. Dropping
/// the receiver does not stop the node; use `shutdown` to request cleanup.
pub async fn run_with_services_ready(
    config: ZakuradConfig,
    custom_services: Vec<CustomService>,
    shutdown: CancellationToken,
    ready: tokio::sync::oneshot::Sender<NodeServices>,
) -> Result<(), Report> {
    run_with_services_inner(config, custom_services, shutdown, Some(ready)).await
}

async fn run_with_services_inner(
    config: ZakuradConfig,
    custom_services: Vec<CustomService>,
    shutdown: CancellationToken,
    ready: Option<tokio::sync::oneshot::Sender<NodeServices>>,
) -> Result<(), Report> {
    if shutdown.is_cancelled() {
        return Ok(());
    }

    let command = StartCmd::default();
    let config = command.override_config(config)?;
    let node_shutdown = CancellationToken::new();
    // One-way latch: cancelled means shutdown must await the root future's cleanup.
    let shutdown_cleanup_required = CancellationToken::new();
    let node = command.start(
        config.into(),
        custom_services,
        ready,
        node_shutdown.clone(),
        shutdown_cleanup_required.clone(),
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
    async fn embedded_node_registers_custom_service_and_shuts_down_on_drop() {
        let _guard = zakura_test::init();
        let native_socket = UdpSocket::bind("127.0.0.1:0").expect("test UDP port is available");
        let native_addr = native_socket
            .local_addr()
            .expect("test UDP socket has an address");
        drop(native_socket);
        let identity_dir = tempfile::tempdir().expect("temporary identity directory is created");

        let mut config = ZakuradConfig::default();
        config.network.listen_addr = "127.0.0.1:0".parse().expect("valid test address");
        config.network.p2p_stack = zakura_network::P2pStack::Dual;
        config.network.initial_mainnet_peers.clear();
        config.network.cache_dir = zakura_network::CacheDir::disabled();
        config.network.identity_dir = identity_dir.path().to_owned();
        config.network.zakura.listen_addr = Some(native_addr);
        config.network.zakura.bootstrap_peers.clear();
        config.state = zakura_state::Config::ephemeral();
        let service_registered = Arc::new(AtomicBool::new(false));
        let mut node = Box::pin(run_with_services(
            config,
            vec![CustomService {
                service: Arc::new(RegistrationProbe(service_registered.clone())),
                provides: Vec::new(),
                seeks: Vec::new(),
            }],
            CancellationToken::new(),
        ));
        timeout(Duration::from_secs(30), async {
            loop {
                tokio::select! {
                    result = &mut node => panic!("node exited before starting: {result:?}"),
                    _ = tokio::time::sleep(Duration::from_millis(25)) => {}
                }
                if service_registered.load(Ordering::Relaxed)
                    && UdpSocket::bind(native_addr).is_err()
                {
                    break;
                }
            }
        })
        .await
        .expect("node registers its custom service and starts its Zakura endpoint");

        drop(node);

        let socket = timeout(Duration::from_secs(30), async {
            loop {
                if let Ok(socket) = UdpSocket::bind(native_addr) {
                    break socket;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("dropping the node future releases its Zakura endpoint");
        drop(socket);
    }
}
