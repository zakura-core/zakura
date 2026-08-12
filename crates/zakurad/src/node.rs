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
    let cleanup_ready = CancellationToken::new();
    let node = command.start(
        config.into(),
        custom_services,
        node_shutdown.clone(),
        cleanup_ready.clone(),
    );

    run_until_shutdown(
        node,
        shutdown.cancelled_owned(),
        node_shutdown,
        cleanup_ready,
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::{net::TcpListener, time::Duration};

    use super::*;
    use tokio::{net::TcpStream, time::timeout};

    #[tokio::test]
    async fn node_starts_and_stops_on_cancellation() {
        let _guard = zakura_test::init();
        let listener = TcpListener::bind("127.0.0.1:0").expect("test port is available");
        let health_addr = listener.local_addr().expect("test listener has an address");
        drop(listener);

        let mut config = ZakuradConfig::default();
        config.network.listen_addr = "127.0.0.1:0".parse().expect("valid test address");
        config.network.p2p_stack = zakura_network::P2pStack::Legacy;
        config.network.initial_mainnet_peers.clear();
        config.state = zakura_state::Config::ephemeral();
        config.health.listen_addr = Some(health_addr);
        let shutdown = CancellationToken::new();
        let node = run_with_services(config, Vec::new(), shutdown.clone());

        tokio::pin!(node);
        timeout(Duration::from_secs(30), async {
            tokio::select! {
                result = &mut node => panic!("node exited before starting: {result:?}"),
                _ = async {
                    while TcpStream::connect(health_addr).await.is_err() {
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                } => {}
            }
        })
        .await
        .expect("node starts its health endpoint");

        tokio::select! {
            result = &mut node => panic!("node exited after starting: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
        shutdown.cancel();
        timeout(Duration::from_secs(30), &mut node)
            .await
            .expect("node shuts down after cancellation")
            .expect("node shuts down cleanly");
    }
}
