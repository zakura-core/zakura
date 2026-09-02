use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use iroh::{
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler, Router},
    Watcher as _,
};
use tokio::sync::{oneshot, Notify};

use crate::zakura::{
    run_native_initiator_control, run_native_responder_control, testkit::LocalEndpointFactory,
    NativeHandshakeNegotiated, TokioControlHandshakeClock, ZakuraHandlerError, ZakuraLocalLimits,
    ZakuraPeerId, ZakuraProtocolHandler, ZakuraSupervisorHandle, P2P_V2_ALPN,
};
use crate::Config;

use super::{
    runner::{report, RunReport},
    scenario::HandshakeScenario,
    trace::TraceRecorder,
};

const TEST_ALPN: &[u8] = b"/zakura/testkit/handshake-properties/0";
type RoleResult = Result<NativeHandshakeNegotiated, ZakuraHandlerError>;

#[derive(Debug)]
struct Responder {
    limits: ZakuraLocalLimits,
    config: crate::zakura::ZakuraHandshakeConfig,
    result: Arc<Mutex<Option<oneshot::Sender<RoleResult>>>>,
    release: Arc<Notify>,
    trace: TraceRecorder,
}

impl ProtocolHandler for Responder {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let result = async {
            let remote = connection.remote_node_id()?;
            let remote = ZakuraPeerId::new(remote.as_bytes().to_vec())?;
            let (mut send, mut recv) = connection.accept_bi().await?;
            run_native_responder_control(
                &mut send,
                &mut recv,
                &self.limits,
                &self.config,
                &remote,
                [2; 32],
                &TokioControlHandshakeClock,
                &self.trace,
            )
            .await
        }
        .await;
        if let Some(result_tx) = self
            .result
            .lock()
            .expect("the real-Iroh result mutex is not poisoned")
            .take()
        {
            let _ = result_tx.send(result);
        }
        self.release.notified().await;
        Ok(())
    }
}

pub(super) async fn run_real_iroh(scenario: &HandshakeScenario) -> RunReport {
    let (initiator_config, initiator_limits, responder_config, responder_limits) =
        scenario.policies();
    let trace = TraceRecorder::for_scenario(scenario);
    let (result_tx, result_rx) = oneshot::channel();
    let release = Arc::new(Notify::new());
    let server = LocalEndpointFactory::with_transport_config(responder_limits.transport_config())
        .endpoint(9_300)
        .await
        .expect("the real-Iroh responder endpoint binds");
    let router = Router::builder(server)
        .accept(
            TEST_ALPN,
            Responder {
                limits: responder_limits,
                config: responder_config,
                result: Arc::new(Mutex::new(Some(result_tx))),
                release: release.clone(),
                trace: trace.clone(),
            },
        )
        .spawn();
    let client = LocalEndpointFactory::with_transport_config(initiator_limits.transport_config())
        .endpoint(9_301)
        .await
        .expect("the real-Iroh initiator endpoint binds");
    let server_address = router.endpoint().node_addr().initialized().await;
    client
        .add_node_addr(server_address.clone())
        .expect("the real-Iroh initiator learns the responder address");
    let connection = tokio::time::timeout(
        Duration::from_secs(5),
        client.connect(server_address, TEST_ALPN),
    )
    .await
    .expect("the real-Iroh connection completes before its deadline")
    .expect("the real-Iroh initiator connects");
    let local_id = ZakuraPeerId::new(client.node_id().as_bytes().to_vec())
        .expect("the real Iroh node id is a valid peer id");
    let (mut send, mut recv) = tokio::time::timeout(Duration::from_secs(5), connection.open_bi())
        .await
        .expect("the real-Iroh control stream opens before its deadline")
        .expect("the real-Iroh control stream opens");
    let initiator = run_native_initiator_control(
        &mut send,
        &mut recv,
        &initiator_limits,
        &initiator_config,
        &local_id,
        [1; 32],
        &TokioControlHandshakeClock,
        &trace,
    )
    .await;
    release.notify_one();
    let responder = tokio::time::timeout(Duration::from_secs(5), result_rx)
        .await
        .expect("the real-Iroh responder finishes before its deadline")
        .expect("the real-Iroh responder reports its result");
    connection.close(0u32.into(), b"handshake test complete");
    client.close().await;
    router
        .shutdown()
        .await
        .expect("the real-Iroh responder shuts down");
    let trace = trace.snapshot();
    report(initiator, responder, trace, None)
}

pub(super) async fn check_pending_handshake_isolation() {
    let mut limits = ZakuraLocalLimits::from_config(&Config::default());
    limits.max_pending_handshakes = 2;
    limits.control_timeout = Duration::from_secs(5);
    let server = LocalEndpointFactory::with_transport_config(limits.transport_config())
        .endpoint(9_400)
        .await
        .expect("the permit-isolation responder binds");
    let handler = ZakuraProtocolHandler::new(
        ZakuraSupervisorHandle::new(4),
        Config::default().network,
        crate::zakura::ZakuraHandshakeConfig::for_network(&Config::default().network),
        limits.clone(),
    )
    .with_endpoint(server.clone());
    let router = Router::builder(server)
        .accept(P2P_V2_ALPN, handler.clone())
        .spawn();
    let server_address = router.endpoint().node_addr().initialized().await;

    let stalled = LocalEndpointFactory::with_transport_config(limits.transport_config())
        .endpoint(9_401)
        .await
        .expect("the stalled initiator binds");
    stalled
        .add_node_addr(server_address.clone())
        .expect("the stalled initiator learns the responder address");
    let stalled_connection = stalled
        .connect(server_address.clone(), P2P_V2_ALPN)
        .await
        .expect("the stalled initiator connects");
    let (_stalled_send, _stalled_recv) = stalled_connection
        .open_bi()
        .await
        .expect("the stalled initiator opens a control stream");
    wait_for_pending_permits(&handler, 1).await;

    let healthy = LocalEndpointFactory::with_transport_config(limits.transport_config())
        .endpoint(9_402)
        .await
        .expect("the healthy initiator binds");
    healthy
        .add_node_addr(server_address.clone())
        .expect("the healthy initiator learns the responder address");
    let healthy_connection = healthy
        .connect(server_address, P2P_V2_ALPN)
        .await
        .expect("the healthy initiator connects while one peer stalls");
    let (mut send, mut recv) = healthy_connection
        .open_bi()
        .await
        .expect("the healthy initiator opens a control stream");
    let healthy_id = ZakuraPeerId::new(healthy.node_id().as_bytes().to_vec())
        .expect("the healthy Iroh identity is a valid peer id");
    let negotiated = run_native_initiator_control(
        &mut send,
        &mut recv,
        &limits,
        &crate::zakura::ZakuraHandshakeConfig::for_network(&Config::default().network),
        &healthy_id,
        [3; 32],
        &TokioControlHandshakeClock,
        &crate::zakura::NoopControlHandshakeObserver,
    )
    .await;
    assert!(
        negotiated.is_ok(),
        "a stalled peer must not block a healthy peer while one permit remains: {negotiated:?}"
    );
    wait_for_pending_permits(&handler, 1).await;

    healthy_connection.close(0u32.into(), b"healthy handshake complete");
    healthy.close().await;
    stalled_connection.close(0u32.into(), b"release stalled handshake");
    stalled.close().await;
    wait_for_pending_permits(&handler, 2).await;
    router
        .shutdown()
        .await
        .expect("the permit-isolation responder shuts down");
}

async fn wait_for_pending_permits(handler: &ZakuraProtocolHandler, expected: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while handler.available_pending_handshake_permits() != expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "pending handshake permits did not reach {expected}; current count is {}",
            handler.available_pending_handshake_permits(),
        )
    });
}
