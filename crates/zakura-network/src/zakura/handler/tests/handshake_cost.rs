//! An ignored loopback workload for the droplet CPU measurement.

use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use crate::zakura::{TransportEvent, TransportObserver};

/// Counts production refusals after authenticated TLS completes.
#[derive(Debug, Default)]
struct Refusals(AtomicUsize);

impl TransportObserver for Refusals {
    fn observe(&self, event: TransportEvent) {
        if matches!(
            event,
            TransportEvent::Handshake {
                stage: "hello",
                validation: Some(ZakuraValidationError::WrongChain),
                ..
            }
        ) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Run this test alone in release mode under `/usr/bin/time` on the droplet.
/// Divide process user plus system CPU by the printed refusal count. That
/// includes both TLS peers, endpoint creation, observation, and cleanup, so
/// it bounds server cost from above rather than isolating server CPU.
#[tokio::test]
#[ignore = "measures failed-handshake CPU on the droplet"]
#[allow(
    clippy::print_stdout,
    reason = "the droplet measurement reads the attempt and refusal counts"
)]
async fn measure_failed_control_handshake_cpu() -> Result<(), BoxError> {
    const ATTEMPTS: usize = 256;
    let limits = ZakuraLocalLimits::from_config(&Config::default());
    let server = LocalEndpointFactory::with_limits(&limits)
        .endpoint(50_000)
        .await?;
    let supervisor = ZakuraSupervisorHandle::new(DEFAULT_ZAKURA_MAX_CONNS_PER_IP);
    let refusals = Arc::new(Refusals::default());
    let handler = ZakuraProtocolHandler::new(
        supervisor.clone(),
        Network::Mainnet,
        ZakuraHandshakeConfig::for_network(&Network::Mainnet),
        limits.clone(),
    )
    .with_local_node_id(server.local_id())
    .with_transport_observer(refusals.clone());
    server.serve(handler)?;
    let address = LocalEndpointFactory::node_addr(&server).await;
    let mut wrong_chain = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
    wrong_chain.chain_id[0] ^= 1;
    let mut clients = Vec::with_capacity(ATTEMPTS);
    let started = std::time::Instant::now();
    for attempt in 0..ATTEMPTS {
        // A fresh endpoint has no TLS session tickets from earlier attempts.
        let client = LocalEndpointFactory::with_limits(&limits)
            .endpoint(50_001 + u64::try_from(attempt)?)
            .await?;
        let connection = client.connect(address.clone(), &[P2P_V2_ALPN]).await?;
        let peer = ZakuraPeerId::new(client.local_id().as_bytes().to_vec())?;
        assert!(run_native_initiator_handshake_without_trace(
            &connection,
            &limits,
            &wrong_chain,
            &peer,
        )
        .await
        .is_err());
        assert_eq!(refusals.0.load(Ordering::Relaxed), attempt + 1);
        clients.push(client);
        // Ten attempts per second remain below the production 16/s cadence.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(supervisor.registered_ids().await.is_empty());
    println!(
        "failed_handshake_cpu attempts={} wrong_chain_refusals={} wall_seconds={:.6}",
        ATTEMPTS,
        refusals.0.load(Ordering::Relaxed),
        started.elapsed().as_secs_f64(),
    );
    for client in clients {
        client.shutdown().await;
    }
    server.shutdown().await;
    Ok(())
}
