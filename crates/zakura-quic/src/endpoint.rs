//! The endpoint: bind, serve, dial and shut down (API-2, SPEC §6 to §8).

use std::{
    collections::HashMap,
    fmt,
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, PoisonError, Weak,
    },
    time::Duration,
};

use futures::{future::BoxFuture, stream::FuturesUnordered, StreamExt as _};
use quinn_proto::{ClientConfig, TransportConfig, VarInt};
use tokio::sync::{mpsc, oneshot, watch};
use zakura_rt::time::Instant;

use crate::{
    config::{QuicBindConfig, QuicConfig},
    congestion::{CongestionProbe, InstrumentedFactory},
    conn::{Conn, ConnObserver, Hooks},
    driver::{
        endpoint::{EndpointCmd, EndpointTask, SHUTDOWN_DRAIN},
        ConnRef, Connecting, Handshake,
    },
    error::{BindError, ConnectError},
    inputs::EndpointInputs,
    key::{NodeAddr, NodeId, NodeSecretKey},
    socket::{SocketBuffers, SocketFactory, UdpFactory, UdpIo},
    sys::{canonical_addr, local_interface_ips},
    tls::{RuntimeUtc, TlsConfig},
};

/// How often a wildcard-bound endpoint lists the host's interface addresses to
/// notice a network change (SOCK-12).
const INTERFACE_POLL: Duration = Duration::from_secs(5);

/// What to do with a connection attempt before any handshake work (ADM-2).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Admit {
    /// Start the handshake.
    Accept,
    /// Send `CONNECTION_REFUSED`.
    Refuse,
    /// Send a stateless Retry, which validates the source address.
    Retry,
    /// Send nothing.
    Ignore,
}

/// A connection attempt, as [`Acceptor::admit`] sees it (ADM-1).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IncomingInfo {
    /// The canonical source address (SOCK-11).
    pub remote: SocketAddr,
    /// Whether the source address is validated, for example by a Retry token.
    pub validated: bool,
    /// QUIC handshakes in progress on this endpoint (ADM-7).
    pub pending_total: usize,
    /// QUIC handshakes in progress from `remote`'s IP (ADM-7).
    pub pending_from_ip: usize,
}

/// The application side of the accept loop (API-3).
pub trait Acceptor: Send + Sync + 'static {
    /// Decides a connection attempt. Runs on the endpoint task before any
    /// crypto work and must not block (ADM-4).
    fn admit(&self, incoming: &IncomingInfo) -> Admit;

    /// ALPNs to offer, in preference order (WIRE-2).
    fn alpns(&self) -> Vec<Vec<u8>>;

    /// Serves one authenticated connection (ADM-9).
    fn handle(&self, conn: Conn) -> BoxFuture<'static, ()>;

    /// Whether an IP is banned. A connection that migrates to a banned IP
    /// closes (PATH-3).
    fn is_banned(&self, _ip: IpAddr) -> bool {
        false
    }
}

/// Per-socket statistics.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct SocketStats {
    /// The bound address.
    pub local_addr: SocketAddr,
    /// Requested and effective buffer sizes (SOCK-3).
    pub buffers: SocketBuffers,
    /// Rebinds after a fatal socket error (SOCK-9).
    pub rebinds: u64,
    /// Whether the kernel accepted segmented sends at bind time (SOCK-13).
    pub gso: bool,
    /// Receive system calls (OBS-12).
    pub recv_calls: u64,
    /// Datagrams received; above `recv_calls` when GRO batches them.
    pub datagrams_received: u64,
    /// Datagrams dropped because their connection's queue or the socket's
    /// queue was full (SPEC §6a).
    pub queue_drops: u64,
    /// Datagrams dropped because they exceeded the advertised
    /// `max_udp_payload_size` (SPEC §6a).
    pub oversize_drops: u64,
    /// Whether the socket failed twice within 60 s and its endpoint task
    /// stopped (SOCK-9). Dials from it fail from then on.
    pub stopped: bool,
}

/// One bound socket's identity and counters, shared with its endpoint task.
#[derive(Debug)]
pub(crate) struct SocketSlot {
    pub(crate) local_addr: SocketAddr,
    pub(crate) buffers: SocketBuffers,
    pub(crate) gso: bool,
    pub(crate) rebinds: AtomicU64,
    pub(crate) inode: AtomicU64,
    pub(crate) recv_calls: AtomicU64,
    pub(crate) datagrams_received: AtomicU64,
    pub(crate) queue_drops: AtomicU64,
    pub(crate) oversize_drops: AtomicU64,
    pub(crate) stopped: std::sync::atomic::AtomicBool,
}

/// State the handles and every endpoint task share. It holds no command
/// sender, so it never keeps an endpoint task alive.
pub(crate) struct Shared {
    pub(crate) id: NodeId,
    pub(crate) config: QuicConfig,
    pub(crate) max_bidi_streams: u32,
    pub(crate) tls: TlsConfig,
    pub(crate) pending: PendingTable,
    pub(crate) connections: ConnectionSlots,
    pub(crate) hooks: Arc<Hooks>,
    pub(crate) shutdown: watch::Sender<bool>,
    pub(crate) network: watch::Sender<u64>,
    pub(crate) network_changes: AtomicU64,
    /// The injected randomness. Applied where each config is built: socket
    /// endpoint configs, `serve` and every dial.
    pub(crate) inputs: EndpointInputs,
}

impl Shared {
    /// One socket's endpoint config, with the injected seed, reset key and
    /// CID generator.
    pub(crate) fn endpoint_config(&self) -> quinn_proto::EndpointConfig {
        let mut config = self.config.endpoint_config();
        self.inputs.apply_endpoint(&mut config);
        config
    }

    /// A connection's transport config, with its own instrumented controller
    /// (OBS-12).
    pub(crate) fn connection_transport(
        &self,
        probe: Arc<CongestionProbe>,
        gso: bool,
    ) -> Arc<TransportConfig> {
        let mut transport = self.config.transport_config(self.max_bidi_streams, gso);
        transport.congestion_controller_factory(Arc::new(InstrumentedFactory::new(
            self.config.congestion_factory(),
            probe,
        )));
        Arc::new(transport)
    }
}

/// A QUIC endpoint: one endpoint task per bound socket (SOCK-1).
///
/// Cloning shares the endpoint. Dropping the last clone closes every
/// connection and frees the sockets (API-7).
#[derive(Clone)]
pub struct QuicEndpoint {
    inner: Arc<Inner>,
}

struct Inner {
    shared: Arc<Shared>,
    sockets: Vec<SocketHandle>,
}

struct SocketHandle {
    cmds: mpsc::UnboundedSender<EndpointCmd>,
    slot: Arc<SocketSlot>,
    /// The endpoint task, so `shutdown` can abort one that doesn't stop.
    task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl fmt::Debug for QuicEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuicEndpoint")
            .field("id", &self.inner.shared.id)
            .field("local_addrs", &self.local_addrs())
            .finish()
    }
}

impl QuicEndpoint {
    /// Binds one socket per address in `bind` (API-2).
    pub fn bind(
        secret: NodeSecretKey,
        bind: &QuicBindConfig,
        config: &QuicConfig,
    ) -> Result<Self, BindError> {
        // A `u32` fits in `usize` on every supported target.
        let factory = UdpFactory {
            recv_buffer_bytes: config.recv_buffer_bytes as usize,
            send_buffer_bytes: config.send_buffer_bytes as usize,
            offload: config.gso,
        };
        Self::bind_with(secret, bind, config, factory, EndpointInputs::default())
    }

    /// Binds one socket per address in `bind` through `factory`, with the
    /// randomness in `inputs` (API-2).
    ///
    /// `factory` also builds every SOCK-9 replacement. Only a socket with an
    /// inode gets the SOCK-7 poller, and only one with buffer sizes gets the
    /// SOCK-5 gauges. [`Self::bind`] is this with OS sockets and
    /// `EndpointInputs::default()`, so production nodes use `bind`.
    pub fn bind_with<F: SocketFactory>(
        secret: NodeSecretKey,
        bind: &QuicBindConfig,
        config: &QuicConfig,
        factory: F,
        inputs: EndpointInputs,
    ) -> Result<Self, BindError> {
        config.validate()?;
        if bind.addrs.is_empty() {
            return Err(BindError::NoAddress);
        }
        // ADM-12: with no slot, every dial and accept would fail, and a dial
        // capped at zero addresses would have nothing to try.
        if bind.max_connections == 0 {
            return Err(BindError::NoConnectionSlots);
        }
        let shared = Arc::new(Shared {
            id: secret.public(),
            config: config.clone(),
            max_bidi_streams: bind.max_bidi_streams,
            tls: TlsConfig::with_inputs(&secret, &inputs)?,
            pending: PendingTable::default(),
            connections: ConnectionSlots::new(
                bind.max_connections,
                bind.dial_reserve,
                bind.max_closing_connections,
            ),
            hooks: Arc::new(Hooks::default()),
            shutdown: watch::Sender::new(false),
            network: watch::Sender::new(0),
            network_changes: AtomicU64::new(0),
            inputs,
        });
        let factory = Arc::new(factory);
        let mut bound = Vec::with_capacity(bind.addrs.len());
        for addr in &bind.addrs {
            let socket = factory.bind(*addr).map_err(|source| BindError::Socket {
                addr: *addr,
                source,
            })?;
            bound.push(socket);
        }
        let mut sockets = Vec::with_capacity(bound.len());
        for socket in bound {
            // SOCK-5.
            if let Some(buffers) = socket.buffers {
                metrics::gauge!("zakura.quic.socket.recv_buffer_bytes")
                    .set(buffers.recv_effective as f64);
                metrics::gauge!("zakura.quic.socket.send_buffer_bytes")
                    .set(buffers.send_effective as f64);
            }
            let slot = Arc::new(SocketSlot {
                local_addr: socket.local_addr,
                buffers: socket.buffers.unwrap_or_default(),
                gso: socket.io.gso(),
                rebinds: AtomicU64::new(0),
                inode: AtomicU64::new(socket.inode.unwrap_or(0)),
                recv_calls: AtomicU64::new(0),
                datagrams_received: AtomicU64::new(0),
                queue_drops: AtomicU64::new(0),
                oversize_drops: AtomicU64::new(0),
                stopped: std::sync::atomic::AtomicBool::new(false),
            });
            let (cmds, cmds_rx) = mpsc::unbounded_channel();
            let task = tokio::spawn(
                EndpointTask::new(
                    shared.clone(),
                    slot.clone(),
                    factory.clone(),
                    socket.io,
                    cmds_rx,
                )
                .run(),
            );
            // SOCK-7: an injected socket has no inode to poll.
            #[cfg(target_os = "linux")]
            if socket.inode.is_some() {
                tokio::spawn(kernel_drop_poller(
                    Arc::downgrade(&slot),
                    config.kernel_drop_poll_interval(),
                ));
            }
            sockets.push(SocketHandle {
                cmds,
                slot,
                task: std::sync::Mutex::new(Some(task)),
            });
        }
        // SOCK-12: a socket bound to a specific address keeps sending from it,
        // so only a wildcard bind needs to follow interface changes.
        if sockets
            .iter()
            .any(|socket| socket.slot.local_addr.ip().is_unspecified())
        {
            tokio::spawn(watch_interfaces(
                Arc::downgrade(&shared),
                local_interface_ips,
                INTERFACE_POLL,
            ));
        }
        Ok(Self {
            inner: Arc::new(Inner { shared, sockets }),
        })
    }

    /// This endpoint's node ID.
    pub fn local_id(&self) -> NodeId {
        self.inner.shared.id
    }

    /// The bound socket addresses, canonical (SOCK-10, SOCK-11).
    pub fn local_addrs(&self) -> Vec<SocketAddr> {
        self.inner
            .sockets
            .iter()
            .map(|socket| canonical_addr(socket.slot.local_addr))
            .collect()
    }

    /// Per-socket statistics.
    pub fn socket_stats(&self) -> Vec<SocketStats> {
        self.inner
            .sockets
            .iter()
            .map(|socket| {
                let slot = &socket.slot;
                SocketStats {
                    local_addr: slot.local_addr,
                    buffers: slot.buffers,
                    rebinds: slot.rebinds.load(Ordering::Relaxed),
                    gso: slot.gso,
                    recv_calls: slot.recv_calls.load(Ordering::Relaxed),
                    datagrams_received: slot.datagrams_received.load(Ordering::Relaxed),
                    queue_drops: slot.queue_drops.load(Ordering::Relaxed),
                    oversize_drops: slot.oversize_drops.load(Ordering::Relaxed),
                    stopped: slot.stopped.load(Ordering::Relaxed),
                }
            })
            .collect()
    }

    /// Live QUIC connections the endpoint holds across its sockets:
    /// handshakes, dial attempts and established connections (ADM-12).
    pub fn connection_count(&self) -> usize {
        self.inner.shared.connections.live()
    }

    /// Closed connections the endpoint keeps until they drain (ADM-13).
    pub fn closing_connection_count(&self) -> usize {
        self.inner.shared.connections.closing()
    }

    /// Whether the table has a free live connection slot right now (ADM-12).
    /// Callers that dial in a loop check this first, so a full table doesn't
    /// make them spin on `ConnectionLimit`. A dial needs one slot per address
    /// it tries, so a multi-address dial can still fail with that error.
    pub fn has_dial_capacity(&self) -> bool {
        self.inner.shared.connections.can_dial()
    }

    /// Whether discovery can reserve an address attempt while leaving one
    /// live slot below the inbound refusal threshold (ADM-12).
    pub fn has_discovery_capacity(&self) -> bool {
        self.inner.shared.connections.live() < self.inner.shared.connections.discovery_limit()
    }

    /// Network changes the endpoint has passed to its connections (SOCK-12).
    pub fn network_changes(&self) -> u64 {
        self.inner.shared.network_changes.load(Ordering::Relaxed)
    }

    /// The validated transport configuration.
    pub fn config(&self) -> &QuicConfig {
        &self.inner.shared.config
    }

    /// Installs a callback for per-connection samples (OBS-3).
    pub fn set_conn_observer(&self, observer: ConnObserver) {
        self.inner.shared.hooks.set_observer(observer);
    }

    /// Starts accepting connections for `acceptor` (API-2).
    ///
    /// Until this runs, the endpoint only dials: quinn-proto drops inbound
    /// attempts because no server config exists.
    pub fn serve(&self, acceptor: impl Acceptor) -> Result<(), BindError> {
        let acceptor: Arc<dyn Acceptor> = Arc::new(acceptor);
        let alpns = acceptor.alpns();
        let crypto = self.inner.shared.tls.server_config(alpns.clone())?;
        let mut server = self.inner.shared.inputs.server_config(Arc::new(crypto));
        self.inner.shared.config.apply_server_limits(&mut server);
        // Retry tokens carry their issue time, so they read the runtime's UTC.
        server.time_source(Arc::new(RuntimeUtc));
        let server = Arc::new(server);

        let ban = acceptor.clone();
        self.inner
            .shared
            .hooks
            .set_ban(Arc::new(move |ip| ban.is_banned(ip)));
        for socket in &self.inner.sockets {
            let _ = socket.cmds.send(EndpointCmd::Serve {
                server: server.clone(),
                acceptor: acceptor.clone(),
            });
        }

        // OBS-10.
        tracing::info!(
            target: "zakura_quic",
            id = %self.inner.shared.id,
            addrs = ?self.local_addrs(),
            buffers = ?self.socket_stats().iter().map(|s| s.buffers).collect::<Vec<_>>(),
            gso = ?self.socket_stats().iter().map(|s| s.gso).collect::<Vec<_>>(),
            congestion_controller = ?self.inner.shared.config.congestion_controller,
            alpns = ?alpns.iter().map(|alpn| String::from_utf8_lossy(alpn).into_owned()).collect::<Vec<_>>(),
            "zakura-quic endpoint serving",
        );
        Ok(())
    }

    /// Dials `addr`, offering `alpns` in preference order (SPEC §8). The
    /// server picks one; [`Conn::alpn`] returns it. Offering several versions
    /// of a protocol lets a node reach peers on older versions in one
    /// handshake. An empty list fails with [`ConnectError::AlpnMismatch`].
    pub async fn connect(&self, addr: NodeAddr, alpns: &[&[u8]]) -> Result<Conn, ConnectError> {
        self.connect_with_limit(addr, alpns, self.inner.shared.connections.max())
            .await
    }

    /// Dials a discovery candidate without taking the dial reserve or the
    /// last slot available to inbound connections (ADM-12).
    ///
    /// Like [`Self::connect`], but reserves all address attempts below this
    /// lower ceiling before sending any packet. A ceiling of zero disables
    /// discovery dials and returns [`ConnectError::ConnectionLimit`].
    pub async fn connect_discovery(
        &self,
        addr: NodeAddr,
        alpns: &[&[u8]],
    ) -> Result<Conn, ConnectError> {
        self.connect_with_limit(addr, alpns, self.inner.shared.connections.discovery_limit())
            .await
    }

    /// Reserves a dial's address attempts together under `limit` before any packet.
    async fn connect_with_limit(
        &self,
        addr: NodeAddr,
        alpns: &[&[u8]],
        limit: usize,
    ) -> Result<Conn, ConnectError> {
        let shared = &self.inner.shared;
        if alpns.is_empty() {
            return Err(ConnectError::AlpnMismatch);
        }
        // DIAL-1.
        if addr.id == shared.id {
            return Err(ConnectError::SelfDial);
        }
        if *shared.shutdown.borrow() {
            return Err(ConnectError::Endpoint(
                quinn_proto::ConnectError::EndpointStopping,
            ));
        }

        // DIAL-2: canonicalize, deduplicate in order, and keep dialable families.
        let mut targets: Vec<(SocketAddr, &SocketHandle)> = Vec::new();
        for raw in addr.direct {
            let target = canonical_addr(raw);
            if targets.iter().any(|(seen, _)| *seen == target) {
                continue;
            }
            if let Some(socket) = self.socket_for(target) {
                targets.push((target, socket));
            }
        }
        if targets.is_empty() {
            return Err(ConnectError::NoUsableAddress);
        }

        // ADM-12: either every address fits before the first packet, or none
        // is tried. Bound the target set by this dial's ceiling so an idle
        // endpoint can fit it whenever that ceiling is nonzero.
        targets.truncate(limit);
        if targets.is_empty() {
            return Err(ConnectError::ConnectionLimit);
        }
        let slots = shared
            .connections
            .claim_dial_slots(targets.len(), limit)
            .ok_or(ConnectError::ConnectionLimit)?;

        let crypto = Arc::new(shared.tls.client_config(addr.id, alpns)?);

        // DIAL-3: one handshake per address, `dial_stagger` apart.
        let stagger = shared.config.dial_stagger();
        let deadline = shared.config.handshake_timeout();
        let mut attempts = FuturesUnordered::new();
        for (index, ((target, socket), slot)) in targets.into_iter().zip(slots).enumerate() {
            let crypto = crypto.clone();
            // Bounded by the 8 addresses discovery admits per record; the cast can't truncate.
            let delay = stagger * index as u32;
            attempts.push(async move {
                if !delay.is_zero() {
                    zakura_rt::time::sleep(delay).await;
                }
                metrics::counter!("zakura.quic.dial.attempts").increment(1);
                let started = Instant::now();
                let result = self.dial_once(socket, crypto, target, deadline, slot).await;
                record_handshake(&result, started);
                (target, result)
            });
        }

        let mut best: Option<ConnectError> = None;
        while let Some((target, result)) = attempts.next().await {
            match result {
                Ok((conn, handshake)) => {
                    // Dropping `attempts` drops every other attempt, which
                    // closes those connections with code 0.
                    drop(attempts);
                    return finish_dial(conn, handshake, addr.id, target);
                }
                Err(error) => {
                    if best.as_ref().is_none_or(|best| error.rank() > best.rank()) {
                        best = Some(error);
                    }
                }
            }
        }
        let error =
            best.expect("at least one attempt ran because the capped targets were non-empty");
        if matches!(error, ConnectError::AlpnMismatch) {
            metrics::counter!("zakura.quic.dial.alpn_mismatch").increment(1);
        }
        Err(error)
    }

    async fn dial_once(
        &self,
        socket: &SocketHandle,
        crypto: Arc<quinn_proto::crypto::rustls::QuicClientConfig>,
        target: SocketAddr,
        deadline: Option<Duration>,
        slot: Arc<ConnectionSlot>,
    ) -> Result<(ConnRef, Handshake), ConnectError> {
        let probe = Arc::new(CongestionProbe::default());
        let mut client = ClientConfig::new(crypto);
        self.inner.shared.inputs.apply_client(&mut client);
        client.transport_config(
            self.inner
                .shared
                .connection_transport(probe.clone(), socket.slot.gso),
        );
        let (reply, connecting) = oneshot::channel();
        socket
            .cmds
            .send(EndpointCmd::Connect {
                config: client,
                remote: target,
                probe,
                slot,
                reply,
            })
            .map_err(|_| ConnectError::Endpoint(quinn_proto::ConnectError::EndpointStopping))?;
        let connecting = connecting
            .await
            .map_err(|_| ConnectError::Endpoint(quinn_proto::ConnectError::EndpointStopping))??;
        establish(connecting, deadline).await
    }

    /// Stops accepting, closes every connection, waits up to 3 s for them to
    /// drain and releases the sockets (API-7).
    ///
    /// Other handles stay valid, but they can no longer dial or serve.
    pub async fn shutdown(&self) {
        self.inner.shared.shutdown.send_replace(true);
        let mut done = Vec::new();
        for socket in &self.inner.sockets {
            let (reply, stopped) = oneshot::channel();
            if socket.cmds.send(EndpointCmd::Shutdown { reply }).is_ok() {
                done.push(stopped);
            }
        }
        // The endpoint tasks enforce the 3 s drain; the extra second covers
        // their exit.
        let all = futures::future::join_all(done);
        if zakura_rt::time::timeout(SHUTDOWN_DRAIN + Duration::from_secs(1), all)
            .await
            .is_ok()
        {
            return;
        }
        // A task that missed the bound is aborted. Dropping it closes its
        // socket, so `shutdown` never returns with the socket still bound.
        tracing::warn!(target: "zakura_quic", "an endpoint task didn't stop within the drain bound; aborting it");
        for socket in &self.inner.sockets {
            let task = socket
                .task
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            if let Some(task) = task {
                task.abort();
                let _ = zakura_rt::time::timeout(Duration::from_secs(1), task).await;
            }
        }
    }

    /// Dials `target` with a hand-built client crypto config (SEC-2 tests).
    #[cfg(test)]
    pub(crate) async fn dial_with_crypto(
        &self,
        crypto: quinn_proto::crypto::rustls::QuicClientConfig,
        target: SocketAddr,
    ) -> Result<(ConnRef, Handshake), ConnectError> {
        let socket = self
            .socket_for(target)
            .expect("tests dial an address family the endpoint bound");
        let deadline = self.inner.shared.config.handshake_timeout();
        let slot = self
            .inner
            .shared
            .connections
            .claim_dial()
            .ok_or(ConnectError::ConnectionLimit)?;
        self.dial_once(socket, Arc::new(crypto), target, deadline, slot)
            .await
    }

    #[cfg(test)]
    pub(crate) fn shared(&self) -> &Arc<Shared> {
        &self.inner.shared
    }

    /// Picks the socket to dial `target` from: same family, and loopback for
    /// loopback targets when one exists.
    fn socket_for(&self, target: SocketAddr) -> Option<&SocketHandle> {
        let family: Vec<&SocketHandle> = self
            .inner
            .sockets
            .iter()
            .filter(|socket| socket.slot.local_addr.is_ipv4() == target.is_ipv4())
            .collect();
        let reaches = |socket: &SocketHandle| {
            let local = socket.slot.local_addr.ip();
            local.is_unspecified() || local.is_loopback() == target.ip().is_loopback()
        };
        family
            .iter()
            .find(|socket| reaches(socket))
            .or_else(|| family.first())
            .copied()
    }
}

fn finish_dial(
    conn: ConnRef,
    handshake: Handshake,
    expected: NodeId,
    target: SocketAddr,
) -> Result<Conn, ConnectError> {
    let close = |reason: &'static [u8]| {
        let _ = conn.send(crate::driver::ConnCmd::Close {
            code: VarInt::from_u32(0),
            reason: bytes::Bytes::from_static(reason),
        });
    };
    // The verifier already enforced TLS-7; this guards against a verifier bug.
    if handshake.remote_id != Some(expected) {
        close(b"identity");
        return Err(ConnectError::WrongIdentity);
    }
    let Some(alpn) = handshake.alpn else {
        close(b"alpn");
        return Err(ConnectError::AlpnMismatch);
    };
    Ok(Conn::new(conn, expected, target, alpn))
}

/// Waits for a handshake, closing the connection at the deadline (ADM-6,
/// DIAL-4).
async fn establish(
    connecting: Connecting,
    deadline: Option<Duration>,
) -> Result<(ConnRef, Handshake), ConnectError> {
    let established = connecting.established();
    match deadline {
        // Dropping the attempt at the deadline closes the connection.
        Some(deadline) => match zakura_rt::time::timeout(deadline, established).await {
            Ok(result) => result.map_err(ConnectError::from_handshake),
            Err(_) => Err(ConnectError::HandshakeTimeout),
        },
        None => established.await.map_err(ConnectError::from_handshake),
    }
}

/// Finishes one inbound handshake and hands the connection to the acceptor.
pub(crate) async fn inbound_handshake(
    connecting: Connecting,
    deadline: Option<Duration>,
    pending: PendingGuard,
    acceptor: Arc<dyn Acceptor>,
) {
    let started = Instant::now();
    let remote = connecting.remote;
    let shared = connecting.conn.shared.clone();
    let result = establish(connecting, deadline).await;
    record_handshake(&result, started);
    let Ok((conn, handshake)) = result else {
        // A failed or timed-out attempt keeps its pending slot until the
        // connection task has closed it, so a peer that keeps the task busy
        // can't free the slot early (ADM-7).
        wait_closed(&shared).await;
        return;
    };
    let (Some(remote_id), Some(alpn)) = (handshake.remote_id, handshake.alpn) else {
        let _ = conn.send(crate::driver::ConnCmd::Close {
            code: VarInt::from_u32(0),
            reason: bytes::Bytes::from_static(b"identity"),
        });
        drop(conn);
        wait_closed(&shared).await;
        return;
    };
    // `handle` runs before the pending count drops, so an acceptor that moves
    // the connection into its own accounting there leaves no gap (ADM-7).
    let handled = acceptor.handle(Conn::new(conn, remote_id, remote, alpn));
    drop(pending);
    handled.await;
}

/// Waits until the connection task closes the connection, for at most the
/// shutdown drain bound in case the task is gone.
async fn wait_closed(shared: &crate::driver::ConnShared) {
    let mut closed = shared.closed.subscribe();
    let _ = zakura_rt::time::timeout(SHUTDOWN_DRAIN, closed.wait_for(Option::is_some)).await;
}

/// Applies the acceptor's rules, then this crate's optional limits (ADM-3).
///
/// The acceptor owns rules 1, 2 and 4 (bans and Zakura's connection limits).
/// The endpoint owns rules 3 and 5 because their keys live in `QuicConfig`.
pub(crate) fn decide(config: &QuicConfig, acceptor: &dyn Acceptor, info: &IncomingInfo) -> Admit {
    let decision = acceptor.admit(info);
    if decision != Admit::Accept {
        return decision;
    }
    // Rule 3.
    if config
        .max_pending_per_ip
        .is_some_and(|limit| info.pending_from_ip >= limit as usize)
    {
        return Admit::Refuse;
    }
    // Rule 5.
    if !info.validated
        && config
            .retry_threshold
            .is_some_and(|threshold| info.pending_total >= threshold as usize)
    {
        return Admit::Retry;
    }
    Admit::Accept
}

/// Counts a handshake result, and records its duration when it completed.
fn record_handshake<T>(result: &Result<T, ConnectError>, started: Instant) {
    let Some(counter) = handshake_counter(result) else {
        return;
    };
    metrics::counter!(counter).increment(1);
    if result.is_ok() {
        metrics::histogram!("zakura.quic.handshake.duration_seconds")
            .record(started.elapsed().as_secs_f64());
    }
}

/// The handshake counter a dial or accept result belongs to, or `None` when
/// no handshake happened: `ConnectionLimit` is this node's own cap refusing
/// the attempt before any packet (ADM-12), so it isn't a failed handshake.
fn handshake_counter<T>(result: &Result<T, ConnectError>) -> Option<&'static str> {
    match result {
        Ok(_) => Some("zakura.quic.handshake.completed"),
        Err(ConnectError::HandshakeTimeout) => Some("zakura.quic.handshake.timed_out"),
        Err(ConnectError::ConnectionLimit) => None,
        Err(_) => Some("zakura.quic.handshake.failed"),
    }
}

/// Tells every connection when the host's interface addresses change
/// (SOCK-12).
///
/// A wildcard socket replies from the address each peer used. When that
/// address disappears, sends from it fail until the connection forgets it and
/// lets the kernel pick a new source.
pub(crate) async fn watch_interfaces(
    shared: Weak<Shared>,
    list_ips: impl Fn() -> Vec<IpAddr> + Send + 'static,
    interval: Duration,
) {
    let mut known = list_ips();
    let mut ticker = zakura_rt::time::interval(interval);
    ticker.set_missed_tick_behavior(zakura_rt::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let Some(shared) = shared.upgrade() else {
            return;
        };
        if *shared.shutdown.borrow() {
            return;
        }
        let current = list_ips();
        if current == known {
            continue;
        }
        tracing::info!(target: "zakura_quic", from = ?known, to = ?current, "interface addresses changed");
        known = current;
        shared.network_changes.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("zakura.quic.network_changes").increment(1);
        shared.network.send_modify(|generation| *generation += 1);
    }
}

/// Exports the socket's `/proc/net/udp` drop count as a counter (SOCK-7).
#[cfg(target_os = "linux")]
async fn kernel_drop_poller(slot: Weak<SocketSlot>, interval: Duration) {
    let mut ticker = zakura_rt::time::interval(interval);
    let mut baseline: Option<(u64, u64)> = None;
    loop {
        ticker.tick().await;
        let Some(slot) = slot.upgrade() else {
            return;
        };
        let current_inode = slot.inode.load(Ordering::Relaxed);
        if current_inode == 0 {
            continue;
        }
        let Some(drops) = crate::sys::kernel_drops(current_inode, slot.local_addr.is_ipv6()) else {
            continue;
        };
        let previous = match baseline {
            Some((seen_inode, seen_drops)) if seen_inode == current_inode => seen_drops,
            _ => 0,
        };
        metrics::counter!("zakura.quic.socket.kernel_drops")
            .increment(drops.saturating_sub(previous));
        baseline = Some((current_inode, drops));
    }
}

/// Handshakes in progress, in total and per source IP (ADM-7).
#[derive(Clone, Default)]
pub(crate) struct PendingTable(Arc<Mutex<PendingCounts>>);

#[derive(Default)]
struct PendingCounts {
    total: usize,
    by_ip: HashMap<IpAddr, usize>,
}

impl PendingTable {
    pub(crate) fn total(&self) -> usize {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).total
    }

    pub(crate) fn for_ip(&self, ip: IpAddr) -> usize {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .by_ip
            .get(&ip)
            .copied()
            .unwrap_or(0)
    }

    pub(crate) fn enter(&self, ip: IpAddr) -> PendingGuard {
        let mut counts = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        counts.total += 1;
        *counts.by_ip.entry(ip).or_default() += 1;
        PendingGuard {
            table: self.clone(),
            ip,
        }
    }
}

/// Releases one pending handshake on drop.
pub(crate) struct PendingGuard {
    table: PendingTable,
    ip: IpAddr,
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        let mut counts = self.table.0.lock().unwrap_or_else(PoisonError::into_inner);
        counts.total = counts.total.saturating_sub(1);
        if let Some(count) = counts.by_ip.get_mut(&self.ip) {
            *count -= 1;
            if *count == 0 {
                counts.by_ip.remove(&self.ip);
            }
        }
    }
}

/// QUIC connections held across every socket of one endpoint: live ones
/// (ADM-12) and closed ones still draining (ADM-13).
pub(crate) struct ConnectionSlots {
    counts: Arc<SlotCounts>,
    max: usize,
    max_inbound: usize,
    max_closing: usize,
}

#[derive(Default)]
struct SlotCounts {
    live: AtomicUsize,
    closing: AtomicUsize,
}

impl ConnectionSlots {
    /// Allows `max` live connections, of which inbound attempts may take all
    /// but `dial_reserve`, and `max_closing` closed ones.
    pub(crate) fn new(max: usize, dial_reserve: usize, max_closing: usize) -> Self {
        Self {
            counts: Arc::default(),
            max,
            max_inbound: max.saturating_sub(dial_reserve),
            max_closing,
        }
    }

    /// Live connections held right now, dial attempts in flight included.
    pub(crate) fn live(&self) -> usize {
        self.counts.live.load(Ordering::Acquire)
    }

    /// Closed connections held right now.
    pub(crate) fn closing(&self) -> usize {
        self.counts.closing.load(Ordering::Acquire)
    }

    /// The most live connections the table ever holds, so the most addresses
    /// one dial can claim slots for.
    pub(crate) fn max(&self) -> usize {
        self.max
    }

    /// The discovery ceiling leaves the dial reserve and one inbound slot free.
    fn discovery_limit(&self) -> usize {
        self.max_inbound.saturating_sub(1)
    }

    /// Whether a dial attempt would get a slot right now.
    pub(crate) fn can_dial(&self) -> bool {
        self.live() < self.max
    }

    /// Claims a live slot for an inbound attempt, or `None` when only the dial
    /// reserve is left. Closed connections don't count (ADM-13).
    pub(crate) fn claim_inbound(&self) -> Option<Arc<ConnectionSlot>> {
        self.claim(self.max_inbound)
    }

    /// Claims a live slot for one dial attempt, or `None` at the cap.
    #[cfg(test)]
    pub(crate) fn claim_dial(&self) -> Option<Arc<ConnectionSlot>> {
        self.claim(self.max)
    }

    /// Reserves every address attempt atomically, or leaves the count unchanged.
    fn claim_dial_slots(&self, attempts: usize, limit: usize) -> Option<Vec<Arc<ConnectionSlot>>> {
        if !try_add(&self.counts.live, attempts, limit) {
            return None;
        }
        Some((0..attempts).map(|_| self.new_slot()).collect())
    }

    /// Claims a live slot while fewer than `limit` are held.
    fn claim(&self, limit: usize) -> Option<Arc<ConnectionSlot>> {
        if !try_add(&self.counts.live, 1, limit) {
            return None;
        }
        Some(self.new_slot())
    }

    /// Tracks one already reserved live slot until its connection drains.
    fn new_slot(&self) -> Arc<ConnectionSlot> {
        metrics::gauge!("zakura.quic.connections.held").increment(1.0);
        Arc::new(ConnectionSlot {
            counts: self.counts.clone(),
            max_closing: self.max_closing,
            closing: AtomicBool::new(false),
        })
    }
}

/// Adds `additional` to `count` only if the result fits `limit`. Concurrent
/// dials and endpoint tasks reserve their slots in one atomic step. A plain
/// CAS loop: `fetch_update` is deprecated on newer toolchains and `try_update`
/// doesn't exist on older ones.
fn try_add(count: &AtomicUsize, additional: usize, limit: usize) -> bool {
    let mut held = count.load(Ordering::Acquire);
    loop {
        let Some(next) = held.checked_add(additional).filter(|next| *next <= limit) else {
            return false;
        };
        match count.compare_exchange_weak(held, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return true,
            Err(current) => held = current,
        }
    }
}

/// One connection's slot, shared by its table entry and its connection task.
/// It counts as live until the connection closes, then as closing, and frees
/// when the last holder drops it: the table entry leaves only once
/// `quinn-proto` reports the connection drained (ADM-11).
pub(crate) struct ConnectionSlot {
    counts: Arc<SlotCounts>,
    max_closing: usize,
    closing: AtomicBool,
}

impl ConnectionSlot {
    /// Moves the slot from live to closing. Returns `false`, leaving it live,
    /// when the closing allowance is full: the caller then drops the
    /// connection's state at once instead of draining it (ADM-13). Only the
    /// connection task calls this, once.
    ///
    /// `shrink` runs after the endpoint switches to the closing queue limit
    /// and before the live slot frees, so the caller can drop the datagrams
    /// queued under the live limit before the cheaper charge applies.
    pub(crate) fn begin_closing(&self, shrink: impl FnOnce()) -> bool {
        if self.is_closing() {
            return true;
        }
        if !try_add(&self.counts.closing, 1, self.max_closing) {
            return false;
        }
        self.closing.store(true, Ordering::Release);
        shrink();
        self.counts.live.fetch_sub(1, Ordering::AcqRel);
        metrics::gauge!("zakura.quic.connections.held").decrement(1.0);
        metrics::gauge!("zakura.quic.connections.closing").increment(1.0);
        true
    }

    /// Whether the connection has closed and moved to the closing allowance.
    pub(crate) fn is_closing(&self) -> bool {
        self.closing.load(Ordering::Acquire)
    }
}

impl Drop for ConnectionSlot {
    fn drop(&mut self) {
        if self.is_closing() {
            self.counts.closing.fetch_sub(1, Ordering::AcqRel);
            metrics::gauge!("zakura.quic.connections.closing").decrement(1.0);
        } else {
            self.counts.live.fetch_sub(1, Ordering::AcqRel);
            metrics::gauge!("zakura.quic.connections.held").decrement(1.0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Concurrent address batches cannot cross the discovery ceiling, and a
    /// refused batch leaves no partial claim. Inbound and maintained dials
    /// retain their separate capacity, and dropping every claim refunds it.
    #[test]
    fn concurrent_discovery_reservations_preserve_inbound_capacity() {
        let slots = ConnectionSlots::new(8, 2, 0);
        let barrier = std::sync::Barrier::new(9);
        let observed = std::thread::scope(|scope| {
            for _ in 0..8 {
                let (slots, barrier) = (&slots, &barrier);
                scope.spawn(move || {
                    barrier.wait();
                    let held = slots.claim_dial_slots(2, slots.discovery_limit());
                    barrier.wait();
                    barrier.wait();
                    drop(held);
                });
            }
            barrier.wait();
            barrier.wait();
            let live = slots.live();
            let overflow_refused = slots.claim_dial_slots(usize::MAX, usize::MAX).is_none();
            let inbound = [
                slots.claim_inbound(),
                slots.claim_inbound(),
                slots.claim_inbound(),
            ];
            let maintained = slots.claim_dial_slots(2, slots.max());
            let observed = (
                live,
                overflow_refused,
                inbound.iter().flatten().count(),
                maintained.as_ref().map_or(0, Vec::len),
                slots.live(),
            );
            drop((inbound, maintained));
            // Release workers before asserting, so a regression cannot leave
            // the test blocked at a barrier.
            barrier.wait();
            observed
        });
        // Two whole batches fit under the ceiling of five. A third
        // batch must fail without retaining even its first slot.
        let (discovery_slots, overflow_refused, inbound_slots, maintained_slots, full_count) =
            observed;
        assert_eq!(discovery_slots, 4);
        assert!(overflow_refused);
        assert_eq!(inbound_slots, 2);
        assert_eq!(maintained_slots, 2);
        assert_eq!(full_count, 8);
        assert_eq!(slots.live(), 0);
    }

    /// A dial refused at this node's own cap never reached the peer, so it
    /// isn't counted as a failed handshake (ADM-12).
    #[test]
    fn a_local_cap_refusal_is_not_a_handshake_result() {
        let counter = |result: Result<(), ConnectError>| handshake_counter(&result);
        assert_eq!(counter(Err(ConnectError::ConnectionLimit)), None);
        assert_eq!(counter(Ok(())), Some("zakura.quic.handshake.completed"));
        assert_eq!(
            counter(Err(ConnectError::HandshakeTimeout)),
            Some("zakura.quic.handshake.timed_out")
        );
        assert_eq!(
            counter(Err(ConnectError::Refused)),
            Some("zakura.quic.handshake.failed")
        );
    }
}
