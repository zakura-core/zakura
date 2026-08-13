//! In-process Zakura node lifecycle.

use abscissa_core::config::Override;
use color_eyre::Report;
use tokio_util::sync::CancellationToken;

use crate::{commands::StartCmd, components::tokio::run_until_shutdown, config::ZakuradConfig};

pub use zakura_network::zakura::CustomService;

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
    use std::{net::UdpSocket, time::Duration};

    use super::*;
    use tokio::time::timeout;

    #[tokio::test]
    async fn dropping_node_future_shuts_down_zakura_endpoint() {
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
        let mut node = Box::pin(run_with_services(
            config,
            Vec::new(),
            CancellationToken::new(),
        ));
        timeout(Duration::from_secs(30), async {
            loop {
                tokio::select! {
                    result = &mut node => panic!("node exited before starting: {result:?}"),
                    _ = tokio::time::sleep(Duration::from_millis(25)) => {}
                }
                if UdpSocket::bind(native_addr).is_err() {
                    break;
                }
            }
        })
        .await
        .expect("node starts its Zakura endpoint");
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
