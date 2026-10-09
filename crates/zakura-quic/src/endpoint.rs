//! The endpoint: bind, serve, dial and shut down (API-2, SPEC §6 to §8).

use std::{
    collections::HashMap,
    fmt, io,
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, PoisonError, Weak,
    },
    time::{Duration, Instant},
};

use futures::{future::BoxFuture, stream::FuturesUnordered, StreamExt as _};
use noq::{PathId, Runtime, VarInt};
use tokio::{sync::watch, task::JoinSet};

use crate::{
    admission::{self, Admission, ConnectionAttempt, Reservation},
    config::{QuicBindConfig, QuicConfig},
    conn::{self, BanCheck, Conn, ConnObserver, OpenPaths},
    error::{BindError, ConnectError},
    key::{NodeAddr, NodeId, NodeSecretKey},
    socket::{bind_udp, RebindOnError, SocketBuffers},
    sys::{canonical_addr, local_interface_ips},
    tls::{self, TlsConfig},
};

/// How long [`QuicEndpoint::shutdown`] waits for connections to drain (API-7).
const SHUTDOWN_DRAIN: Duration = Duration::from_secs(3);
/// A second socket failure within this window stops the endpoint (SOCK-9).
const REBIND_WINDOW: Duration = Duration::from_secs(60);
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
    /// QUIC handshakes in progress from `remote`'s IP, plus its failed attempts
    /// and closed inbound connections whose state may remain (ADM-7, ADM-8).
    pub pending_from_ip: usize,
}

/// The application side of the accept loop (API-3).
pub trait Acceptor: Send + Sync + 'static {
    /// Decides a connection attempt. Runs before any crypto work and must not
    /// block (ADM-4).
    fn admit(&self, incoming: &IncomingInfo) -> Admit;

    /// ALPNs to offer, in preference order (WIRE-2).
    fn alpns(&self) -> Vec<Vec<u8>>;

    /// Serves one authenticated connection (ADM-9).
    fn handle(&self, conn: Conn) -> BoxFuture<'static, ()>;

    /// Whether an IP is banned. Peer-opened paths and migrations from a banned
    /// IP close (PATH-2, PATH-3).
    fn is_banned(&self, _ip: IpAddr) -> bool {
        false
    }
}

/// Per-socket statistics.
#[derive(Clone, Copy, Debug)]
pub struct SocketStats {
    /// The bound address.
    pub local_addr: SocketAddr,
    /// Requested and effective buffer sizes (SOCK-3).
    pub buffers: SocketBuffers,
    /// Rebinds after a fatal socket error (SOCK-9).
    pub rebinds: u64,
}

/// A QUIC endpoint: one noq endpoint per bound socket (SOCK-1).
///
/// Cloning shares the endpoint.
#[derive(Clone)]
pub struct QuicEndpoint {
    inner: Arc<Inner>,
}

impl fmt::Debug for QuicEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuicEndpoint")
            .field("id", &self.inner.id)
            .field("local_addrs", &self.local_addrs())
            .finish()
    }
}

struct Inner {
    id: NodeId,
    config: QuicConfig,
    tls: TlsConfig,
    transport: Arc<noq::TransportConfig>,
    sockets: Vec<SocketSlot>,
    pending: PendingTable,
    admission: Admission,
    shutdown: watch::Sender<bool>,
    serve: Mutex<Option<Arc<dyn Acceptor>>>,
    observer: Mutex<Option<ConnObserver>>,
    network_changes: Arc<AtomicU64>,
}

struct SocketSlot {
    /// `None` once [`QuicEndpoint::shutdown`] has released the socket (API-7).
    endpoint: Mutex<Option<noq::Endpoint>>,
    local_addr: SocketAddr,
    buffers: SocketBuffers,
    rebinds: Arc<AtomicU64>,
}

/// Dropping the last `QuicEndpoint` handle closes every connection and frees
/// the sockets, as dropping an Iroh endpoint did. Dropping `shutdown` also
/// ends the accept loops, socket supervisors and drop pollers.
impl Drop for Inner {
    fn drop(&mut self) {
        for endpoint in self.sockets.iter().filter_map(SocketSlot::endpoint) {
            endpoint.set_server_config(None);
            endpoint.close(VarInt::from_u32(0), b"");
        }
    }
}

impl SocketSlot {
    fn endpoint(&self) -> Option<noq::Endpoint> {
        self.endpoint
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl QuicEndpoint {
    /// Binds one socket per address in `bind` (API-2).
    pub fn bind(
        secret: NodeSecretKey,
        bind: &QuicBindConfig,
        config: &QuicConfig,
    ) -> Result<Self, BindError> {
        config.validate()?;
        if bind.addrs.is_empty() {
            return Err(BindError::NoAddress);
        }
        if bind.max_inbound_connections == 0
            || bind.max_inbound_connections > bind.max_connections
            || bind.max_connections > tokio::sync::Semaphore::MAX_PERMITS
            || bind.max_draining_connections > tokio::sync::Semaphore::MAX_PERMITS
        {
            return Err(BindError::ConnectionLimits);
        }
        let runtime: Arc<dyn Runtime> = Arc::new(noq::TokioRuntime);
        let (shutdown, _) = watch::channel(false);
        let mut sockets = Vec::with_capacity(bind.addrs.len());
        for addr in &bind.addrs {
            sockets.push(bind_slot(*addr, config, &runtime, &shutdown)?);
        }
        let network_changes = Arc::new(AtomicU64::new(0));
        // SOCK-12: a socket bound to a specific address keeps sending from it,
        // so only a wildcard bind needs to follow interface changes.
        if sockets
            .iter()
            .any(|slot| slot.local_addr.ip().is_unspecified())
        {
            tokio::spawn(watch_interfaces(
                sockets.iter().filter_map(SocketSlot::endpoint).collect(),
                local_interface_ips,
                INTERFACE_POLL,
                network_changes.clone(),
                shutdown.subscribe(),
            ));
        }
        let inner = Inner {
            id: secret.public(),
            config: config.clone(),
            tls: TlsConfig::new(&secret),
            transport: Arc::new(config.transport_config(bind.max_bidi_streams)),
            sockets,
            pending: PendingTable::default(),
            admission: Admission::new(
                bind.max_connections,
                bind.max_inbound_connections,
                bind.max_draining_connections,
            ),
            shutdown,
            serve: Mutex::new(None),
            observer: Mutex::new(None),
            network_changes,
        };
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// This endpoint's node ID.
    pub fn local_id(&self) -> NodeId {
        self.inner.id
    }

    /// The bound socket addresses, canonical (SOCK-10, SOCK-11).
    pub fn local_addrs(&self) -> Vec<SocketAddr> {
        self.inner
            .sockets
            .iter()
            .map(|slot| canonical_addr(slot.local_addr))
            .collect()
    }

    /// Per-socket statistics.
    pub fn socket_stats(&self) -> Vec<SocketStats> {
        self.inner
            .sockets
            .iter()
            .map(|slot| SocketStats {
                local_addr: slot.local_addr,
                buffers: slot.buffers,
                rebinds: slot.rebinds.load(Ordering::Relaxed),
            })
            .collect()
    }

    /// Network changes the endpoint has passed to noq (SOCK-12).
    pub fn network_changes(&self) -> u64 {
        self.inner.network_changes.load(Ordering::Relaxed)
    }

    /// The validated transport configuration.
    pub fn config(&self) -> &QuicConfig {
        &self.inner.config
    }

    /// Installs a callback for per-connection samples (OBS-3).
    pub fn set_conn_observer(&self, observer: ConnObserver) {
        *self
            .inner
            .observer
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(observer);
    }

    /// Starts accepting connections for `acceptor` (API-2).
    ///
    /// Until this runs, the endpoint only dials: noq drops inbound attempts
    /// because no server config exists.
    pub fn serve(&self, acceptor: impl Acceptor) -> Result<(), BindError> {
        let acceptor: Arc<dyn Acceptor> = Arc::new(acceptor);
        let alpns = acceptor.alpns();
        let crypto = self.inner.tls.server_config(alpns.clone())?;
        let mut server = noq::ServerConfig::with_crypto(Arc::new(crypto));
        server.transport_config(self.inner.transport.clone());
        self.inner.config.apply_server_limits(&mut server);

        *self
            .inner
            .serve
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(acceptor.clone());
        for endpoint in self.inner.sockets.iter().filter_map(SocketSlot::endpoint) {
            endpoint.set_server_config(Some(server.clone()));
            tokio::spawn(accept_loop(
                Arc::downgrade(&self.inner),
                self.shutdown_signal(),
                endpoint,
                acceptor.clone(),
            ));
        }

        // OBS-10.
        tracing::info!(
            target: "zakura_quic",
            id = %self.inner.id,
            addrs = ?self.local_addrs(),
            buffers = ?self.socket_stats().iter().map(|s| s.buffers).collect::<Vec<_>>(),
            congestion_controller = ?self.inner.config.congestion_controller,
            alpns = ?alpns.iter().map(|alpn| String::from_utf8_lossy(alpn).into_owned()).collect::<Vec<_>>(),
            "zakura-quic endpoint serving",
        );
        Ok(())
    }

    /// Dials `addr` and negotiates `alpn` (SPEC §8).
    pub async fn connect(&self, addr: NodeAddr, alpn: &[u8]) -> Result<Conn, ConnectError> {
        // DIAL-1.
        if addr.id == self.inner.id {
            return Err(ConnectError::SelfDial);
        }
        if *self.inner.shutdown.borrow() {
            return Err(ConnectError::Endpoint(noq::ConnectError::EndpointStopping));
        }

        // DIAL-2: canonicalize, deduplicate in order, and keep dialable families.
        let mut targets: Vec<(SocketAddr, noq::Endpoint)> = Vec::new();
        for raw in addr.direct {
            let target = canonical_addr(raw);
            if targets.iter().any(|(seen, _)| *seen == target) {
                continue;
            }
            if let Some(endpoint) = self.socket_for(target) {
                targets.push((target, endpoint));
            }
        }
        if targets.is_empty() {
            return Err(ConnectError::NoUsableAddress);
        }

        let mut client =
            noq::ClientConfig::new(Arc::new(self.inner.tls.client_config(addr.id, alpn)?));
        client.transport_config(self.inner.transport.clone());

        // DIAL-3: one handshake per address, `dial_stagger` apart.
        let stagger = self.inner.config.dial_stagger();
        let deadline = self.inner.config.handshake_timeout();
        let mut attempts = FuturesUnordered::new();
        for (index, (target, endpoint)) in targets.into_iter().enumerate() {
            let client = client.clone();
            // Bounded by the 8 addresses discovery admits per record; the cast can't truncate.
            let delay = stagger * index as u32;
            attempts.push(async move {
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                metrics::counter!("zakura.quic.dial.attempts").increment(1);
                let started = Instant::now();
                let result = dial_once(self, &endpoint, client, target, deadline).await;
                record_handshake(&result, started);
                (target, result)
            });
        }

        let mut best: Option<ConnectError> = None;
        while let Some((target, result)) = attempts.next().await {
            match result {
                Ok(connection) => {
                    // Dropping `attempts` drops every other `Connecting`, which
                    // closes those connections with code 0.
                    drop(attempts);
                    return self.finish_dial(connection, addr.id, target);
                }
                Err(error) => {
                    if best.as_ref().is_none_or(|best| error.rank() > best.rank()) {
                        best = Some(error);
                    }
                }
            }
        }
        let error = best.expect("at least one attempt ran because targets was non-empty");
        if matches!(error, ConnectError::AlpnMismatch) {
            metrics::counter!("zakura.quic.dial.alpn_mismatch").increment(1);
        }
        Err(error)
    }

    /// Stops accepting, closes every connection, waits up to 3 s for them to
    /// drain and releases the sockets (API-7).
    ///
    /// Other handles stay valid, but they can no longer dial or serve.
    pub async fn shutdown(&self) {
        self.inner.shutdown.send_replace(true);
        let endpoints: Vec<noq::Endpoint> = self
            .inner
            .sockets
            .iter()
            .filter_map(SocketSlot::endpoint)
            .collect();
        for endpoint in &endpoints {
            endpoint.set_server_config(None);
            endpoint.close(VarInt::from_u32(0), b"");
        }
        let drain = futures::future::join_all(endpoints.iter().map(noq::Endpoint::wait_idle));
        if tokio::time::timeout(SHUTDOWN_DRAIN, drain).await.is_err() {
            tracing::debug!(target: "zakura_quic", "connections didn't drain within 3 s");
        }
        // Step 4: the socket closes once noq's driver drops its last handle.
        for slot in &self.inner.sockets {
            slot.endpoint
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
        }
    }

    /// Counts every accepted or dialed transport until noq reports it drained.
    /// Construction is serialized separately so concurrent sockets cannot each
    /// spend the same last slot. Counts may only decrease during that check.
    fn open_connections(&self) -> usize {
        self.inner
            .sockets
            .iter()
            .filter_map(SocketSlot::endpoint)
            .map(|socket| socket.open_connections())
            .fold(0usize, usize::saturating_add)
    }

    /// Whether a dial would now pass the ADM-11 checks rather than fail with
    /// [`ConnectError::Capacity`]. Callers can skip a dial instead of
    /// retrying one that would fail locally. Another dial may still take the
    /// last slot first.
    pub fn has_dial_capacity(&self) -> bool {
        self.inner.admission.has_dial_room(self.open_connections())
    }

    /// Picks the socket to dial `target` from: same family, and loopback for
    /// loopback targets when one exists.
    fn socket_for(&self, target: SocketAddr) -> Option<noq::Endpoint> {
        let family: Vec<&SocketSlot> = self
            .inner
            .sockets
            .iter()
            .filter(|slot| slot.local_addr.is_ipv4() == target.is_ipv4())
            .collect();
        let reaches = |slot: &SocketSlot| {
            let local = slot.local_addr.ip();
            local.is_unspecified() || local.is_loopback() == target.ip().is_loopback()
        };
        family
            .iter()
            .find(|slot| reaches(slot))
            .or_else(|| family.first())
            .and_then(|slot| slot.endpoint())
    }

    fn finish_dial(
        &self,
        connection: noq::Connection,
        expected: NodeId,
        target: SocketAddr,
    ) -> Result<Conn, ConnectError> {
        // The verifier already enforced TLS-7; this guards against a verifier bug.
        let remote_id = tls::remote_node_id(&connection);
        if remote_id != Some(expected) {
            connection.close(VarInt::from_u32(0), b"identity");
            return Err(ConnectError::WrongIdentity);
        }
        let Some(alpn) = tls::negotiated_alpn(&connection) else {
            connection.close(VarInt::from_u32(0), b"alpn");
            return Err(ConnectError::AlpnMismatch);
        };
        Ok(self.register_conn(connection, expected, target, alpn))
    }

    fn register_conn(
        &self,
        connection: noq::Connection,
        remote_id: NodeId,
        admitted: SocketAddr,
        alpn: Vec<u8>,
    ) -> Conn {
        let paths = OpenPaths::new();
        let ban = self.ban_check();
        let observer = self
            .inner
            .observer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        tokio::spawn(conn::monitor(
            connection.clone(),
            remote_id,
            admitted.ip(),
            paths.clone(),
            ban,
            observer,
        ));
        Conn::new(connection, remote_id, admitted, alpn, paths)
    }

    /// The live noq endpoints, for tests that drive endpoint tasks directly.
    #[cfg(test)]
    pub(crate) fn noq_endpoints(&self) -> Vec<noq::Endpoint> {
        self.inner
            .sockets
            .iter()
            .filter_map(SocketSlot::endpoint)
            .collect()
    }

    /// The `pending_total` and `pending_from_ip` an attempt from `ip` would see
    /// now, for tests.
    #[cfg(test)]
    pub(crate) fn pending(&self, ip: IpAddr) -> (usize, usize) {
        (self.inner.pending.total(), self.inner.pending.for_ip(ip))
    }

    /// Live and draining owner permits held, for tests.
    #[cfg(test)]
    pub(crate) fn held_owners(&self) -> (usize, usize) {
        self.inner.admission.held()
    }

    fn ban_check(&self) -> Option<BanCheck> {
        let acceptor = self
            .inner
            .serve
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()?;
        Some(Arc::new(move |ip| acceptor.is_banned(ip)))
    }

    fn shutdown_signal(&self) -> watch::Receiver<bool> {
        self.inner.shutdown.subscribe()
    }
}

fn bind_slot(
    addr: SocketAddr,
    config: &QuicConfig,
    runtime: &Arc<dyn Runtime>,
    shutdown: &watch::Sender<bool>,
) -> Result<SocketSlot, BindError> {
    let socket_error = |source| BindError::Socket { addr, source };
    let bound = bind_udp(
        addr,
        config.recv_buffer_bytes as usize,
        config.send_buffer_bytes as usize,
    )
    .map_err(socket_error)?;
    let (failures, failure_rx) = tokio::sync::mpsc::unbounded_channel();
    let socket =
        RebindOnError::wrap(runtime, bound.socket, failures.clone()).map_err(socket_error)?;
    let endpoint = noq::Endpoint::new_with_abstract_socket(
        config.endpoint_config(),
        None,
        socket,
        runtime.clone(),
    )
    .map_err(socket_error)?;

    // SOCK-5.
    metrics::gauge!("zakura.quic.socket.recv_buffer_bytes")
        .set(bound.buffers.recv_effective as f64);
    metrics::gauge!("zakura.quic.socket.send_buffer_bytes")
        .set(bound.buffers.send_effective as f64);

    let rebinds = Arc::new(AtomicU64::new(0));
    let inode = Arc::new(AtomicU64::new(bound.inode.unwrap_or(0)));
    tokio::spawn(socket_supervisor(
        endpoint.clone(),
        bound.local_addr,
        config.clone(),
        runtime.clone(),
        failures,
        failure_rx,
        rebinds.clone(),
        inode.clone(),
        shutdown.subscribe(),
    ));
    #[cfg(target_os = "linux")]
    tokio::spawn(kernel_drop_poller(
        inode,
        bound.local_addr.is_ipv6(),
        config.kernel_drop_poll_interval(),
        shutdown.subscribe(),
    ));

    Ok(SocketSlot {
        endpoint: Mutex::new(Some(endpoint)),
        local_addr: bound.local_addr,
        buffers: bound.buffers,
        rebinds,
    })
}

/// Rebinds once after a fatal socket error, or stops the endpoint (SOCK-9).
#[allow(clippy::too_many_arguments)]
async fn socket_supervisor(
    endpoint: noq::Endpoint,
    local_addr: SocketAddr,
    config: QuicConfig,
    runtime: Arc<dyn Runtime>,
    failures: tokio::sync::mpsc::UnboundedSender<io::Error>,
    mut failure_rx: tokio::sync::mpsc::UnboundedReceiver<io::Error>,
    rebinds: Arc<AtomicU64>,
    inode: Arc<AtomicU64>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut last_rebind: Option<Instant> = None;
    loop {
        let error = tokio::select! {
            error = failure_rx.recv() => match error {
                Some(error) => error,
                None => return,
            },
            _ = shutdown.wait_for(|stop| *stop) => return,
        };
        tracing::warn!(target: "zakura_quic", %local_addr, %error, "UDP socket failed; rebinding");
        if last_rebind.is_some_and(|at| at.elapsed() < REBIND_WINDOW) {
            tracing::error!(target: "zakura_quic", %local_addr, %error, "UDP socket failed twice within 60 s; stopping the endpoint");
            endpoint.close(VarInt::from_u32(0), b"socket failed");
            return;
        }
        let rebound = bind_udp(
            local_addr,
            config.recv_buffer_bytes as usize,
            config.send_buffer_bytes as usize,
        )
        .and_then(|bound| {
            let new_inode = bound.inode;
            let socket = RebindOnError::wrap(&runtime, bound.socket, failures.clone())?;
            endpoint.rebind_abstract(socket)?;
            Ok(new_inode)
        });
        match rebound {
            Ok(new_inode) => {
                last_rebind = Some(Instant::now());
                rebinds.fetch_add(1, Ordering::Relaxed);
                inode.store(new_inode.unwrap_or(0), Ordering::Relaxed);
                metrics::counter!("zakura.quic.socket.rebinds").increment(1);
            }
            Err(error) => {
                tracing::error!(target: "zakura_quic", %local_addr, %error, "rebind failed; stopping the endpoint");
                endpoint.close(VarInt::from_u32(0), b"socket failed");
                return;
            }
        }
    }
}

/// Every path may recover in place after a network change, so noq clears its
/// local address and pings it. Zakura never opens replacement paths (DIAL-7).
#[derive(Debug)]
struct RecoverInPlace;

impl noq::NetworkChangeHint for RecoverInPlace {
    fn is_path_recoverable(&self, _path_id: PathId, _network_path: noq::FourTuple) -> bool {
        true
    }
}

/// Tells noq when the host's interface addresses change (SOCK-12).
///
/// noq pins each path to the local address that first received its packets.
/// When that address disappears, every send on the path fails until the path
/// idles out. A notified connection forgets the address and pings its peer
/// from whichever address the kernel now picks.
pub(crate) async fn watch_interfaces(
    endpoints: Vec<noq::Endpoint>,
    list_ips: impl Fn() -> Vec<IpAddr> + Send + 'static,
    interval: Duration,
    changes: Arc<AtomicU64>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut known = list_ips();
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = shutdown.wait_for(|stop| *stop) => return,
        }
        let current = list_ips();
        if current == known {
            continue;
        }
        tracing::info!(target: "zakura_quic", from = ?known, to = ?current, "interface addresses changed");
        known = current;
        changes.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("zakura.quic.network_changes").increment(1);
        let hint: Arc<dyn noq::NetworkChangeHint + Send + Sync> = Arc::new(RecoverInPlace);
        for endpoint in &endpoints {
            endpoint.handle_network_change(Some(hint.clone()));
        }
    }
}

/// Exports the socket's `/proc/net/udp` drop count as a counter (SOCK-7).
#[cfg(target_os = "linux")]
async fn kernel_drop_poller(
    inode: Arc<AtomicU64>,
    ipv6: bool,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(interval);
    let mut baseline: Option<(u64, u64)> = None;
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = shutdown.wait_for(|stop| *stop) => return,
        }
        let current_inode = inode.load(Ordering::Relaxed);
        if current_inode == 0 {
            continue;
        }
        let Some(drops) = crate::sys::kernel_drops(current_inode, ipv6) else {
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

/// Accepts connection attempts on one socket (SPEC §7).
///
/// The loop holds the endpoint weakly, so dropping every `QuicEndpoint` handle
/// ends it.
async fn accept_loop(
    weak: Weak<Inner>,
    mut shutdown: watch::Receiver<bool>,
    socket: noq::Endpoint,
    acceptor: Arc<dyn Acceptor>,
) {
    let mut tasks = JoinSet::new();
    loop {
        let incoming = tokio::select! {
            incoming = socket.accept() => match incoming {
                Some(incoming) => incoming,
                None => break,
            },
            Some(_) = tasks.join_next(), if !tasks.is_empty() => continue,
            _ = shutdown.wait_for(|stop| *stop) => break,
        };
        let Some(inner) = weak.upgrade() else {
            break;
        };
        // This strong handle lives for one attempt only; the handshake task
        // gets the weak one (API-7).
        let endpoint = QuicEndpoint { inner };
        let _construction = endpoint
            .inner
            .admission
            .construction
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let info = IncomingInfo {
            remote: canonical_addr(incoming.remote_address()),
            validated: incoming.remote_address_validated(),
            pending_total: endpoint.inner.pending.total(),
            pending_from_ip: endpoint
                .inner
                .pending
                .for_ip(canonical_addr(incoming.remote_address()).ip()),
        };
        let decision = decide(&endpoint.inner.config, acceptor.as_ref(), &info);
        tracing::trace!(target: "zakura_quic", remote = %info.remote, ?decision, "incoming");
        match decision {
            Admit::Accept => {
                let Ok(reservation) = endpoint
                    .inner
                    .admission
                    .reserve(true, endpoint.open_connections())
                else {
                    metrics::counter!("zakura.quic.incoming.refused").increment(1);
                    incoming.refuse();
                    continue;
                };
                // ADM-7: count the handshake from Accept until it finishes.
                let pending = endpoint.inner.pending.enter(info.remote.ip());
                match incoming.accept() {
                    Ok(connecting) => {
                        metrics::counter!("zakura.quic.incoming.accepted").increment(1);
                        tasks.spawn(handshake(
                            weak.clone(),
                            endpoint.inner.config.handshake_timeout(),
                            ConnectionAttempt::incoming(connecting, reservation)
                                .charge_ip_after_close(
                                    endpoint.inner.pending.clone(),
                                    info.remote.ip(),
                                ),
                            info.remote,
                            pending,
                            acceptor.clone(),
                        ));
                    }
                    // noq frees this attempt's state inside `accept`, so its IP stays uncharged.
                    Err(error) => {
                        metrics::counter!("zakura.quic.handshake.failed").increment(1);
                        tracing::trace!(target: "zakura_quic", %error, "accept failed");
                    }
                }
            }
            Admit::Refuse => {
                metrics::counter!("zakura.quic.incoming.refused").increment(1);
                incoming.refuse();
            }
            Admit::Retry => match incoming.retry() {
                Ok(()) => metrics::counter!("zakura.quic.incoming.retried").increment(1),
                Err(error) => {
                    metrics::counter!("zakura.quic.incoming.refused").increment(1);
                    error.into_incoming().refuse();
                }
            },
            Admit::Ignore => {
                metrics::counter!("zakura.quic.incoming.ignored").increment(1);
                incoming.ignore();
            }
        }
    }
    // Dropping the set aborts every handshake and handler still running.
    tasks.shutdown().await;
}

/// Applies the acceptor's rules, then this crate's optional limits (ADM-3).
///
/// The acceptor owns rules 1, 2 and 4 (bans and Zakura's connection limits).
/// The endpoint owns rules 3 and 5 because their keys live in `QuicConfig`.
fn decide(config: &QuicConfig, acceptor: &dyn Acceptor, info: &IncomingInfo) -> Admit {
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

/// Finishes one inbound handshake and hands the connection to the acceptor.
///
/// The task holds the endpoint weakly: a peer that stalls its handshake must
/// not keep a dropped endpoint and its sockets alive (API-7).
async fn handshake(
    weak: Weak<Inner>,
    deadline: Option<Duration>,
    mut connecting: ConnectionAttempt,
    remote: SocketAddr,
    pending: PendingGuard,
    acceptor: Arc<dyn Acceptor>,
) {
    let started = Instant::now();
    let attempt = connecting
        .weak_handle()
        .expect("inbound attempts expose their transport state");
    let result = match deadline {
        Some(deadline) => match tokio::time::timeout(deadline, &mut connecting).await {
            Ok(result) => result.map_err(ConnectError::from_handshake),
            Err(_) => Err(ConnectError::HandshakeTimeout),
        },
        None => (&mut connecting)
            .await
            .map_err(ConnectError::from_handshake),
    };
    record_handshake(&result, started);
    let Ok(connection) = result else {
        // ADM-7: noq keeps a failed attempt's state while it drains, so its IP
        // charge and owner permit last until the state is freed.
        // ADM-6: dropping the attempt closes a timed-out connection.
        let _reservation = connecting.abandon().map(Reservation::into_draining);
        let _charge = pending.into_hold();
        admission::until_freed(&attempt).await;
        return;
    };
    drop(pending);
    let (Some(remote_id), Some(alpn)) = (
        tls::remote_node_id(&connection),
        tls::negotiated_alpn(&connection),
    ) else {
        connection.close(VarInt::from_u32(0), b"identity");
        return;
    };
    let Some(inner) = weak.upgrade() else {
        connection.close(VarInt::from_u32(0), b"");
        return;
    };
    let endpoint = QuicEndpoint { inner };
    let conn = endpoint.register_conn(connection, remote_id, remote, alpn);
    // A connection handler must not keep a dropped endpoint alive.
    drop(endpoint);
    acceptor.handle(conn).await;
}

async fn dial_once(
    endpoint: &QuicEndpoint,
    socket: &noq::Endpoint,
    client: noq::ClientConfig,
    target: SocketAddr,
    deadline: Option<Duration>,
) -> Result<noq::Connection, ConnectError> {
    let connecting = {
        let _construction = endpoint
            .inner
            .admission
            .construction
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let reservation = endpoint
            .inner
            .admission
            .reserve(false, endpoint.open_connections())?;
        ConnectionAttempt::new(
            socket.connect_with(client, target, tls::UNSENT_SERVER_NAME)?,
            reservation,
        )
    };
    match deadline {
        Some(deadline) => match tokio::time::timeout(deadline, connecting).await {
            Ok(result) => result.map_err(ConnectError::from_handshake),
            Err(_) => Err(ConnectError::HandshakeTimeout),
        },
        None => connecting.await.map_err(ConnectError::from_handshake),
    }
}

fn record_handshake<T>(result: &Result<T, ConnectError>, started: Instant) {
    match result {
        Ok(_) => {
            metrics::counter!("zakura.quic.handshake.completed").increment(1);
            metrics::histogram!("zakura.quic.handshake.duration_seconds")
                .record(started.elapsed().as_secs_f64());
        }
        Err(ConnectError::HandshakeTimeout) => {
            metrics::counter!("zakura.quic.handshake.timed_out").increment(1);
        }
        Err(_) => metrics::counter!("zakura.quic.handshake.failed").increment(1),
    }
}

/// Handshakes in progress, in total and per source IP (ADM-7), plus per-IP
/// charges for attempts whose transport state may remain (ADM-7, ADM-8).
#[derive(Clone, Default)]
pub(crate) struct PendingTable(Arc<Mutex<PendingCounts>>);

#[derive(Default)]
struct PendingCounts {
    total: usize,
    by_ip: HashMap<IpAddr, usize>,
    draining: HashMap<IpAddr, usize>,
}

/// Removes one count for `ip`, dropping the entry at zero.
fn release(counts: &mut HashMap<IpAddr, usize>, ip: IpAddr) {
    if let Some(count) = counts.get_mut(&ip) {
        *count -= 1;
        if *count == 0 {
            counts.remove(&ip);
        }
    }
}

impl PendingTable {
    fn total(&self) -> usize {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).total
    }

    /// Handshakes in progress from `ip` plus its draining charges.
    fn for_ip(&self, ip: IpAddr) -> usize {
        let counts = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        counts.by_ip.get(&ip).copied().unwrap_or(0) + counts.draining.get(&ip).copied().unwrap_or(0)
    }

    fn enter(&self, ip: IpAddr) -> PendingGuard {
        let mut counts = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        counts.total += 1;
        *counts.by_ip.entry(ip).or_default() += 1;
        PendingGuard {
            table: self.clone(),
            ip,
        }
    }

    /// Charges `ip` until the guard drops, without counting a handshake (ADM-8).
    pub(crate) fn hold(&self, ip: IpAddr) -> DrainingGuard {
        let mut counts = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        *counts.draining.entry(ip).or_default() += 1;
        DrainingGuard {
            table: self.clone(),
            ip,
        }
    }
}

/// Releases one pending handshake on drop.
struct PendingGuard {
    table: PendingTable,
    ip: IpAddr,
}

impl PendingGuard {
    /// Ends the handshake but keeps charging its IP until the hold drops (ADM-7).
    fn into_hold(self) -> DrainingGuard {
        // Charge before releasing, so the IP is never briefly undercounted.
        self.table.hold(self.ip)
    }
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        let mut counts = self.table.0.lock().unwrap_or_else(PoisonError::into_inner);
        counts.total = counts.total.saturating_sub(1);
        release(&mut counts.by_ip, self.ip);
    }
}

/// Keeps one closed connection's IP charged until dropped (ADM-8).
pub(crate) struct DrainingGuard {
    table: PendingTable,
    ip: IpAddr,
}

impl Drop for DrainingGuard {
    fn drop(&mut self) {
        let mut counts = self.table.0.lock().unwrap_or_else(PoisonError::into_inner);
        release(&mut counts.draining, self.ip);
    }
}
