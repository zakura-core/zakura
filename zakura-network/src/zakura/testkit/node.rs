//! In-process Zakura test node built from the production handler.

use std::{fmt, net::SocketAddr, sync::Arc, time::Duration};

use iroh::{endpoint::TransportConfig, protocol::Router, NodeAddr, NodeId};
use tokio::{
    sync::{mpsc, Mutex},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use zakura_jsonl_trace::JsonlTracer;

use super::{InboundRecorder, LocalEndpointFactory, WaitError};
use crate::{
    zakura::{
        discovery::build_discovery_handle, service_registry, spawn_block_sync_reactor,
        spawn_header_sync_reactor, BlockSyncAction, BlockSyncFrontiers, BlockSyncHandle,
        BlockSyncStartup, DiscoveryService, HeaderSyncAction, HeaderSyncFrontiers,
        HeaderSyncHandle, HeaderSyncStartup, Service, ZakuraBlockSyncConfig, ZakuraDiscoveryHandle,
        ZakuraEndpoint, ZakuraHandshakeConfig, ZakuraHeaderSyncConfig, ZakuraLocalLimits,
        ZakuraPeerId, ZakuraProtocolHandler, ZakuraServiceId, ZakuraSupervisorHandle, ZakuraTrace,
        P2P_V2_ALPN,
    },
    BoxError, Config,
};
use zakura_chain::{block, parameters::Network};

/// A running in-process Zakura node for integration tests.
#[derive(Debug)]
pub struct ZakuraTestNode {
    seed: u64,
    endpoint: ZakuraEndpoint,
    discovery: ZakuraDiscoveryHandle,
    limits: ZakuraLocalLimits,
    recorder: InboundRecorder,
    dial_tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
    _tracer: JsonlTracer,
}

impl ZakuraTestNode {
    /// Create a node builder using `seed` as the deterministic identity.
    pub fn builder(seed: u64) -> ZakuraTestNodeBuilder {
        ZakuraTestNodeBuilder::new(seed)
    }

    /// Deterministic seed used by this node.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Current Iroh node address.
    pub async fn node_addr(&self) -> NodeAddr {
        self.endpoint.node_addr().await
    }

    /// Active supervisor handle.
    pub fn supervisor(&self) -> ZakuraSupervisorHandle {
        self.endpoint.supervisor()
    }

    /// Clone the underlying endpoint for test-only external drivers.
    #[cfg(test)]
    pub(crate) fn endpoint(&self) -> ZakuraEndpoint {
        self.endpoint.clone()
    }

    /// Active header-sync handle, if this test node was spawned with native
    /// header sync enabled.
    pub fn header_sync(&self) -> Option<HeaderSyncHandle> {
        self.endpoint.header_sync()
    }

    /// Active block-sync handle, if this test node was spawned with stream-6
    /// block sync enabled.
    pub fn block_sync(&self) -> Option<BlockSyncHandle> {
        self.endpoint.block_sync()
    }

    /// Take the native header-sync action receiver for an externally driven
    /// test node.
    pub async fn take_header_sync_actions(&self) -> Option<mpsc::Receiver<HeaderSyncAction>> {
        self.endpoint.take_header_sync_actions().await
    }

    /// Take the stream-6 block-sync action receiver for an externally driven
    /// test node.
    pub async fn take_block_sync_actions(&self) -> Option<mpsc::Receiver<BlockSyncAction>> {
        self.endpoint.take_block_sync_actions().await
    }

    /// Local limits used by this node.
    pub fn limits(&self) -> &ZakuraLocalLimits {
        &self.limits
    }

    /// Bounded inbound recorder.
    pub fn recorder(&self) -> InboundRecorder {
        self.recorder.clone()
    }

    /// Native discovery runtime handle backing this node's discovery service.
    pub fn discovery(&self) -> ZakuraDiscoveryHandle {
        self.discovery.clone()
    }

    /// Spawn this node's discovery candidate dialer (book-driven outbound dials).
    pub fn spawn_discovery_dialer(&self) -> JoinHandle<()> {
        tokio::spawn(crate::zakura::discovery::run_native_discovery_dialer(
            self.endpoint.clone(),
            self.discovery.clone(),
            self.limits.clone(),
        ))
    }

    /// Insert `peer` as a trusted static discovery candidate (loopback allowed)
    /// and teach iroh its route, so the candidate dialer can connect to it.
    pub async fn insert_static_discovery_candidate(
        &self,
        peer: &ZakuraTestNode,
    ) -> Result<NodeId, BoxError> {
        let node_addr = peer.node_addr().await;
        let node_id = node_addr.node_id;
        self.endpoint.add_node_addr(node_addr.clone())?;
        self.discovery.insert_static_candidate(node_addr).await?;
        Ok(node_id)
    }

    /// Start a native dial to `peer` and wait until this node registers it.
    pub async fn connect_native(
        &self,
        peer: &ZakuraTestNode,
        timeout: Duration,
    ) -> Result<(), BoxError> {
        let peer_addr = peer.node_addr().await;
        self.connect_native_to_addr(peer_addr, timeout).await
    }

    /// Start a native dial to an explicit [`NodeAddr`] and wait until this node
    /// registers it. Lets tests advertise a specific direct-address list (for
    /// example a decoy address ahead of the reachable one).
    pub async fn connect_native_to_addr(
        &self,
        peer_addr: NodeAddr,
        timeout: Duration,
    ) -> Result<(), BoxError> {
        self.endpoint.add_node_addr(peer_addr.clone())?;
        let mut handle = self.endpoint.spawn_native_dial(peer_addr.clone());
        let peer_id = peer_addr.node_id.as_bytes().to_vec();
        let mut peer_set_rx = self.supervisor().subscribe();

        let result = tokio::time::timeout(timeout, async {
            tokio::select! {
                registered = wait_for_peer_registration(&mut peer_set_rx, peer_id.as_slice()) => {
                    registered
                }
                joined = &mut handle => {
                    joined
                        .map_err(|error| -> BoxError { format!("native Zakura dial task failed: {error}").into() })?;
                    Err("native Zakura dial task ended before serving the connection".into())
                }
            }
        })
        .await;

        match result {
            Ok(Ok(())) => {
                self.dial_tasks.lock().await.push(handle);
                Ok(())
            }
            Ok(Err(error)) => {
                handle.abort();
                Err(error)
            }
            Err(_) => {
                handle.abort();
                Err(Box::new(WaitError::new(
                    "native Zakura peer registration",
                    timeout,
                )))
            }
        }
    }

    /// Shut the node down and abort outstanding dial tasks.
    pub async fn shutdown(&self) {
        self.endpoint.shutdown().await;
        let mut tasks = self.dial_tasks.lock().await;
        for task in tasks.drain(..) {
            task.abort();
        }
    }
}

async fn wait_for_peer_registration(
    peer_set_rx: &mut tokio::sync::watch::Receiver<Vec<ZakuraPeerId>>,
    peer_id: &[u8],
) -> Result<(), BoxError> {
    loop {
        if peer_set_rx
            .borrow()
            .iter()
            .any(|id| id.as_bytes() == peer_id)
        {
            return Ok(());
        }

        peer_set_rx.changed().await.map_err(|_| -> BoxError {
            "Zakura peer-set watcher closed before registration".into()
        })?;
    }
}

/// Builder for [`ZakuraTestNode`].
pub struct ZakuraTestNodeBuilder {
    seed: u64,
    limits: ZakuraLocalLimits,
    max_connections_per_ip: usize,
    transport_config: Option<TransportConfig>,
    legacy_upgrade: bool,
    tracer: JsonlTracer,
    service: Option<Arc<dyn Service>>,
    service_factory: Option<Box<dyn FnOnce(ZakuraSupervisorHandle) -> Arc<dyn Service> + Send>>,
    discovery_direct_addrs: Vec<SocketAddr>,
    extra_advertised_services: Vec<ZakuraServiceId>,
    header_sync: Option<TestHeaderSyncStartup>,
    header_sync_config: ZakuraHeaderSyncConfig,
    header_sync_request_timeout: Option<Duration>,
    supported_capabilities: Option<u64>,
    block_sync_config: ZakuraBlockSyncConfig,
}

#[derive(Clone, Debug)]
struct TestHeaderSyncStartup {
    network: Network,
    anchor: (block::Height, block::Hash),
    frontiers: HeaderSyncFrontiers,
    best_header_tip: Option<(block::Height, block::Hash)>,
    verified_block_tip_hash: block::Hash,
}

impl fmt::Debug for ZakuraTestNodeBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZakuraTestNodeBuilder")
            .field("seed", &self.seed)
            .field("limits", &self.limits)
            .field("max_connections_per_ip", &self.max_connections_per_ip)
            .field("transport_config", &self.transport_config.is_some())
            .field("legacy_upgrade", &self.legacy_upgrade)
            .field("tracer", &self.tracer)
            .field(
                "service",
                &(self.service.is_some() || self.service_factory.is_some()),
            )
            .field("header_sync", &self.header_sync.is_some())
            .finish()
    }
}

impl ZakuraTestNodeBuilder {
    /// Create a node builder.
    pub fn new(seed: u64) -> Self {
        let mut limits = ZakuraLocalLimits::from_config(&Config::default());
        limits.max_connections = 16;
        limits.max_pending_handshakes = 8;
        limits.max_open_streams = 16;
        limits.max_inbound_queue_depth = 64;
        let config = Config::default();
        Self {
            seed,
            limits,
            max_connections_per_ip: config.zakura.max_connections_per_ip(),
            transport_config: None,
            legacy_upgrade: false,
            tracer: JsonlTracer::noop(),
            service: None,
            service_factory: None,
            discovery_direct_addrs: Vec::new(),
            extra_advertised_services: Vec::new(),
            header_sync: None,
            header_sync_config: ZakuraHeaderSyncConfig::default(),
            header_sync_request_timeout: None,
            supported_capabilities: None,
            block_sync_config: ZakuraBlockSyncConfig::default(),
        }
    }

    /// Advertise these direct addresses in this node's discovery self-record.
    pub fn discovery_direct_addrs(mut self, direct_addrs: Vec<SocketAddr>) -> Self {
        self.discovery_direct_addrs = direct_addrs;
        self
    }

    /// Advertise an additional service id in this node's discovery self-record.
    pub fn add_advertised_service(mut self, service: ZakuraServiceId) -> Self {
        self.extra_advertised_services.push(service);
        self
    }

    /// Override local limits.
    pub fn limits(mut self, limits: ZakuraLocalLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Override the per-IP connection cap enforced by this node's supervisor.
    ///
    /// Defaults to the production [`ZakuraConfig`](crate::zakura::ZakuraConfig) cap so that
    /// security and integration tests built on the default node exercise the
    /// real per-IP admission gate instead of silently admitting many same-IP
    /// peers. Multi-peer loopback harnesses — where every node shares
    /// `127.0.0.1`, so the per-IP cap collapses to a single bucket — raise this
    /// to restore cluster ergonomics.
    pub fn max_connections_per_ip(mut self, max_connections_per_ip: usize) -> Self {
        self.max_connections_per_ip = max_connections_per_ip;
        self
    }

    /// Mutate the transport configuration used by the endpoint factory.
    pub fn transport(mut self, configure: impl FnOnce(&mut TransportConfig)) -> Self {
        let mut transport = self.limits.transport_config();
        configure(&mut transport);
        self.transport_config = Some(transport);
        self
    }

    /// Reserve the legacy-upgrade test hook. Native-only remains the default.
    pub fn enable_legacy_upgrade(mut self, enable: bool) -> Self {
        self.legacy_upgrade = enable;
        self
    }

    /// Reserve the JSONL tracer hook used by the trace-introspection plan.
    pub fn tracer(mut self, tracer: JsonlTracer) -> Self {
        self.tracer = tracer;
        self
    }

    /// Install a custom service instead of the default recorder.
    pub fn service(mut self, service: Arc<dyn Service>) -> Self {
        self.service = Some(service);
        self
    }

    /// Install a custom service that needs this node's supervisor.
    pub fn service_from_supervisor(
        mut self,
        factory: impl FnOnce(ZakuraSupervisorHandle) -> Arc<dyn Service> + Send + 'static,
    ) -> Self {
        self.service_factory = Some(Box::new(factory));
        self
    }

    /// Enable the production header-sync adapter on this test node and
    /// expose its action receiver for an external test driver.
    pub fn header_sync_driver(
        mut self,
        network: Network,
        anchor: (block::Height, block::Hash),
        frontiers: HeaderSyncFrontiers,
        best_header_tip: Option<(block::Height, block::Hash)>,
    ) -> Self {
        self.header_sync = Some(TestHeaderSyncStartup {
            network,
            anchor,
            frontiers,
            best_header_tip,
            verified_block_tip_hash: anchor.1,
        });
        self
    }

    /// Override the header-sync reactor configuration used by the test node.
    pub fn header_sync_config(mut self, config: ZakuraHeaderSyncConfig) -> Self {
        self.header_sync_config = config;
        self
    }

    /// Override the header-sync request timeout used by the test node.
    pub fn header_sync_request_timeout(mut self, request_timeout: Duration) -> Self {
        self.header_sync_request_timeout = Some(request_timeout);
        self
    }

    /// Restrict the capability mask advertised by this test node.
    pub fn supported_capabilities(mut self, supported_capabilities: u64) -> Self {
        self.supported_capabilities = Some(supported_capabilities);
        self
    }

    /// Override the block-sync config used with [`Self::header_sync_driver`].
    pub fn block_sync_config(mut self, config: ZakuraBlockSyncConfig) -> Self {
        self.block_sync_config = config;
        self
    }

    /// Spawn the node.
    pub async fn spawn(self) -> Result<ZakuraTestNode, BoxError> {
        if self.legacy_upgrade {
            return Err(
                "ZakuraTestNode legacy-upgrade mode is reserved until connect_via_upgrade is implemented"
                    .into(),
            );
        }

        let transport = self
            .transport_config
            .unwrap_or_else(|| self.limits.transport_config());
        let endpoint = LocalEndpointFactory::with_transport_config(transport)
            .endpoint(self.seed)
            .await?;
        let supervisor = ZakuraSupervisorHandle::new(self.max_connections_per_ip);
        let recorder = InboundRecorder::new(usize::from(self.limits.max_inbound_queue_depth));
        let base_service = if let Some(factory) = self.service_factory {
            factory(supervisor.clone())
        } else {
            self.service.unwrap_or_else(|| Arc::new(recorder.clone()))
        };
        let network = Config::default().network;
        let handshake_config = ZakuraHandshakeConfig::for_network(&network);
        let mut advertised_services = crate::zakura::discovery::default_advertised_services();
        advertised_services.extend(self.extra_advertised_services.clone());
        let discovery = build_discovery_handle(
            LocalEndpointFactory::secret_key(self.seed),
            self.discovery_direct_addrs.clone(),
            advertised_services,
            &handshake_config,
            self.limits.max_connections,
            0,
            supervisor.subscribe(),
        )?;

        let mut header_sync_handle = None;
        let mut header_sync_actions = None;
        let mut block_sync_handle = None;
        let mut block_sync_actions = None;
        let mut header_sync_tasks = Vec::new();
        let header_sync = if let Some(header_sync) = self.header_sync {
            let TestHeaderSyncStartup {
                network,
                anchor,
                frontiers,
                best_header_tip,
                verified_block_tip_hash,
            } = header_sync;
            let mut startup = HeaderSyncStartup::new(
                network,
                anchor,
                frontiers,
                best_header_tip,
                self.header_sync_config.clone(),
                self.limits.max_frame_bytes,
            );
            if let Some(request_timeout) = self.header_sync_request_timeout {
                startup.request_timeout = request_timeout;
            }
            startup.range_state_actions_enabled = true;
            startup.inbound_new_block_acceptance_enabled = true;
            startup.status_refresh_interval = Duration::from_millis(200);
            let shutdown = CancellationToken::new();
            startup.shutdown = shutdown.clone();
            startup.trace = ZakuraTrace::new(self.tracer.clone(), seed_label(self.seed));

            let (handle, actions, task) = spawn_header_sync_reactor(startup)?;
            header_sync_tasks.push(task);
            header_sync_actions = Some((shutdown, actions));
            header_sync_handle = Some(handle.clone());

            let mut startup = BlockSyncStartup::new(
                BlockSyncFrontiers {
                    finalized_height: frontiers.finalized_height,
                    verified_block_tip: frontiers.verified_block_tip,
                    verified_block_hash: verified_block_tip_hash,
                },
                best_header_tip.unwrap_or(anchor),
                handle.subscribe_tip(),
                self.block_sync_config.clone(),
            );
            let shutdown = header_sync_actions
                .as_ref()
                .expect("header sync actions were just initialized")
                .0
                .clone();
            startup.shutdown = shutdown;
            startup.trace = ZakuraTrace::new(self.tracer.clone(), seed_label(self.seed));
            let (block_handle, actions, task) = spawn_block_sync_reactor(startup);
            header_sync_tasks.push(task);
            block_sync_actions = Some(actions);
            block_sync_handle = Some(block_handle.clone());

            Some(handle)
        } else {
            // Recorder-only nodes use the header-sync passthrough so tests can
            // inspect header-sync frames without spawning the reactor.
            None
        };
        let discovery_service = if let Some(header_sync) = header_sync.as_ref() {
            Arc::new(DiscoveryService::with_sync_services(
                discovery.clone(),
                header_sync.clone(),
                block_sync_handle.clone(),
            )) as Arc<dyn Service>
        } else {
            Arc::new(DiscoveryService::new(discovery.clone())) as Arc<dyn Service>
        };
        let registry = service_registry(
            &supervisor,
            header_sync,
            block_sync_handle.clone(),
            self.block_sync_config.clone(),
            base_service,
            discovery_service,
        )?;
        let mut handler = ZakuraProtocolHandler::new_with_registry_and_trace(
            supervisor.clone(),
            network.clone(),
            handshake_config,
            self.limits.clone(),
            registry,
            ZakuraTrace::new(self.tracer.clone(), seed_label(self.seed)),
        );
        if let Some(supported_capabilities) = self.supported_capabilities {
            handler = handler.with_supported_capabilities(supported_capabilities);
        }
        let router = Router::builder(endpoint)
            .accept(P2P_V2_ALPN, handler.clone())
            .spawn();
        let endpoint = if let (Some(header_handle), Some(block_handle), Some((shutdown, actions))) =
            (header_sync_handle, block_sync_handle, header_sync_actions)
        {
            ZakuraEndpoint::from_parts_with_sync_services(
                router,
                supervisor,
                handler,
                header_handle,
                block_handle,
                shutdown,
                header_sync_tasks,
                Some(actions),
                block_sync_actions,
            )
        } else {
            ZakuraEndpoint::from_parts(router, supervisor, handler)
        };

        Ok(ZakuraTestNode {
            seed: self.seed,
            endpoint,
            discovery,
            limits: self.limits,
            recorder,
            dial_tasks: Arc::new(Mutex::new(Vec::new())),
            _tracer: self.tracer,
        })
    }
}

fn seed_label(seed: u64) -> String {
    format!("{seed:02}")
}

impl Drop for ZakuraTestNode {
    fn drop(&mut self) {
        if let Ok(mut tasks) = self.dial_tasks.try_lock() {
            for task in tasks.drain(..) {
                task.abort();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::TEST_NET_TIMEOUT;
    use super::*;
    use crate::zakura::DEFAULT_ZAKURA_MAX_CONNS_PER_IP;

    #[tokio::test]
    async fn legacy_upgrade_builder_fails_loudly() {
        let error = ZakuraTestNode::builder(1)
            .enable_legacy_upgrade(true)
            .spawn()
            .await
            .expect_err("legacy-upgrade hook is reserved, not silently ignored");

        assert!(error.to_string().contains("connect_via_upgrade"));
    }

    #[test]
    fn default_test_node_uses_production_per_ip_cap() {
        // `handler::tests::inbound_accept_enforces_per_ip_cap` drives the real
        // native admission path. This test only needs to guard the test-builder
        // default that selects that production cap.
        let builder = ZakuraTestNode::builder(10000);

        assert_eq!(
            builder.max_connections_per_ip, DEFAULT_ZAKURA_MAX_CONNS_PER_IP,
            "the default test node must inherit the production per-IP cap",
        );
    }

    #[tokio::test]
    async fn per_ip_cap_opt_out_admits_multiple_same_ip_peers() {
        // Multi-peer loopback harnesses (clusters, gossip meshes) intentionally
        // admit several same-IP peers and do not exercise the per-IP gate. The
        // explicit builder opt-out restores that ergonomics on top of the
        // production-faithful default.
        let peer1 = ZakuraTestNode::builder(9101)
            .spawn()
            .await
            .expect("first loopback peer spawns");
        let peer2 = ZakuraTestNode::builder(9102)
            .spawn()
            .await
            .expect("second loopback peer spawns");
        let node = ZakuraTestNode::builder(9103)
            .max_connections_per_ip(8)
            .spawn()
            .await
            .expect("opt-out test node spawns");

        node.connect_native(&peer1, TEST_NET_TIMEOUT)
            .await
            .expect("first same-IP peer registers with raised per-IP cap");
        node.connect_native(&peer2, TEST_NET_TIMEOUT)
            .await
            .expect("second same-IP peer registers with raised per-IP cap");

        node.shutdown().await;
        peer1.shutdown().await;
        peer2.shutdown().await;
    }

    // Pin a peer's advertised addresses to its IPv4 loopback path so same-host
    // dials share one source IP (test nodes also bind an IPv6 loopback socket).
    fn ipv4_loopback_addr(peer_addr: &NodeAddr) -> NodeAddr {
        let addr = NodeAddr::new(peer_addr.node_id).with_direct_addresses(
            peer_addr
                .direct_addresses()
                .copied()
                .filter(|addr| addr.is_ipv4() && addr.ip().is_loopback()),
        );
        assert!(
            addr.direct_addresses().next().is_some(),
            "test peer must advertise an IPv4 loopback direct address",
        );
        addr
    }

    #[tokio::test]
    async fn outbound_dial_charges_confirmed_path_not_advertised_decoy() {
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};

        // Regression for the per-IP cap bypass in `serve_native_dial_connection`:
        // it used to charge the connection to the first *advertised* direct
        // address instead of the path iroh actually confirmed. A record could
        // then list an unreachable decoy first and escape the per-IP cap while
        // being served over a different (shared) address.
        let peer1 = ZakuraTestNode::builder(9201)
            .spawn()
            .await
            .expect("peer1 spawns");
        let peer2 = ZakuraTestNode::builder(9202)
            .spawn()
            .await
            .expect("peer2 spawns");
        let node = ZakuraTestNode::builder(9203)
            .max_connections_per_ip(1)
            .spawn()
            .await
            .expect("dialer spawns");

        // Advertise peer1 with an unreachable decoy (RFC 5737 TEST-NET-1,
        // guaranteed non-routable) ahead of its real loopback address. The dial
        // is served over loopback, so the connection must be charged to
        // 127.0.0.1, not the decoy.
        let peer1_loopback = ipv4_loopback_addr(&peer1.node_addr().await);
        let decoy = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)), 1);
        let decoy_first = NodeAddr::new(peer1_loopback.node_id).with_direct_addresses(
            std::iter::once(decoy).chain(peer1_loopback.direct_addresses().copied()),
        );
        node.connect_native_to_addr(decoy_first, TEST_NET_TIMEOUT)
            .await
            .expect("peer1 registers over its reachable loopback path despite the decoy");

        // With the connection correctly charged to the confirmed loopback IP,
        // the per-IP cap of 1 must turn away a second distinct identity from
        // 127.0.0.1. Before the fix, peer1 was charged to the decoy IP, leaving
        // the loopback bucket empty and wrongly admitting peer2.
        let excess = node
            .connect_native_to_addr(
                ipv4_loopback_addr(&peer2.node_addr().await),
                TEST_NET_TIMEOUT,
            )
            .await;
        assert!(
            excess.is_err(),
            "second same-loopback identity must be rejected by the per-IP cap, proving the \
             first dial was charged to the confirmed path and not the advertised decoy",
        );

        node.shutdown().await;
        peer1.shutdown().await;
        peer2.shutdown().await;
    }
}
