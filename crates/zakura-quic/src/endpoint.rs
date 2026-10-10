//! The endpoint: bind, serve, dial and shut down (API-2, SPEC §6 to §8).

use std::{
    collections::HashMap,
    fmt,
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, PoisonError, Weak,
    },
    time::{Duration, Instant},
};

use futures::{future::BoxFuture, stream::FuturesUnordered, StreamExt as _};
use quinn_proto::{ClientConfig, ServerConfig, TransportConfig, VarInt};
use tokio::sync::{mpsc, oneshot, watch};

use crate::{
    config::{QuicBindConfig, QuicConfig},
    congestion::{CongestionProbe, InstrumentedFactory},
    conn::{Conn, ConnObserver, Hooks},
    driver::{
        endpoint::{EndpointCmd, EndpointTask, SHUTDOWN_DRAIN},
        ConnRef, Connecting, Handshake,
    },
    error::{BindError, ConnectError},
    key::{NodeAddr, NodeId, NodeSecretKey},
    socket::{bind_udp, SocketBuffers, UdpIo},
    sys::{canonical_addr, local_interface_ips},
    tls::TlsConfig,
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
    /// Datagrams dropped because their connection's queue was full (SPEC §6a).
    pub queue_drops: u64,
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
}

/// State the handles and every endpoint task share. It holds no command
/// sender, so it never keeps an endpoint task alive.
pub(crate) struct Shared {
    pub(crate) id: NodeId,
    pub(crate) config: QuicConfig,
    pub(crate) max_bidi_streams: u32,
    pub(crate) tls: TlsConfig,
    pub(crate) pending: PendingTable,
    pub(crate) hooks: Arc<Hooks>,
    pub(crate) shutdown: watch::Sender<bool>,
    pub(crate) network: watch::Sender<u64>,
    pub(crate) network_changes: AtomicU64,
}

impl Shared {
    /// A connection's transport config, with its own instrumented controller
    /// (OBS-12) and, with the `qlog` feature, its own qlog file (CTRL-25).
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
        #[cfg(feature = "qlog")]
        if let Some(dir) = &self.config.qlog_dir {
            transport.qlog_stream(qlog_stream(dir));
        }
        Arc::new(transport)
    }
}

/// Opens a fresh qlog file in `dir` (CTRL-25).
#[cfg(feature = "qlog")]
fn qlog_stream(dir: &std::path::Path) -> Option<quinn_proto::QlogStream> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_millis());
    let path = dir.join(format!(
        "zakura-{millis}-{}.qlog",
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let file = std::fs::File::create(&path)
        .inspect_err(
            |error| tracing::warn!(target: "zakura_quic", ?path, %error, "can't create qlog file"),
        )
        .ok()?;
    let mut config = quinn_proto::QlogConfig::default();
    config
        .writer(Box::new(std::io::BufWriter::new(file)))
        .title(Some("zakura".into()));
    config.into_stream()
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
        config.validate()?;
        if bind.addrs.is_empty() {
            return Err(BindError::NoAddress);
        }
        let shared = Arc::new(Shared {
            id: secret.public(),
            config: config.clone(),
            max_bidi_streams: bind.max_bidi_streams,
            tls: TlsConfig::new(&secret),
            pending: PendingTable::default(),
            hooks: Arc::new(Hooks::default()),
            shutdown: watch::Sender::new(false),
            network: watch::Sender::new(0),
            network_changes: AtomicU64::new(0),
        });
        let mut bound = Vec::with_capacity(bind.addrs.len());
        for addr in &bind.addrs {
            let socket_error = |source| BindError::Socket {
                addr: *addr,
                source,
            };
            let socket = bind_udp(
                *addr,
                config.recv_buffer_bytes as usize,
                config.send_buffer_bytes as usize,
            )
            .map_err(socket_error)?;
            let io = UdpIo::new(socket.socket, config.gso).map_err(socket_error)?;
            bound.push((socket.local_addr, socket.buffers, socket.inode, io));
        }
        let mut sockets = Vec::with_capacity(bound.len());
        for (local_addr, buffers, inode, io) in bound {
            // SOCK-5.
            metrics::gauge!("zakura.quic.socket.recv_buffer_bytes")
                .set(buffers.recv_effective as f64);
            metrics::gauge!("zakura.quic.socket.send_buffer_bytes")
                .set(buffers.send_effective as f64);
            let slot = Arc::new(SocketSlot {
                local_addr,
                buffers,
                gso: io.gso(),
                rebinds: AtomicU64::new(0),
                inode: AtomicU64::new(inode.unwrap_or(0)),
                recv_calls: AtomicU64::new(0),
                datagrams_received: AtomicU64::new(0),
                queue_drops: AtomicU64::new(0),
            });
            let (cmds, cmds_rx) = mpsc::unbounded_channel();
            tokio::spawn(EndpointTask::new(shared.clone(), slot.clone(), io, cmds_rx).run());
            #[cfg(target_os = "linux")]
            tokio::spawn(kernel_drop_poller(
                Arc::downgrade(&slot),
                config.kernel_drop_poll_interval(),
            ));
            sockets.push(SocketHandle { cmds, slot });
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
                }
            })
            .collect()
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
        let mut server = ServerConfig::with_crypto(Arc::new(crypto));
        self.inner.shared.config.apply_server_limits(&mut server);
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

    /// Dials `addr` and negotiates `alpn` (SPEC §8).
    pub async fn connect(&self, addr: NodeAddr, alpn: &[u8]) -> Result<Conn, ConnectError> {
        let shared = &self.inner.shared;
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

        let crypto = Arc::new(shared.tls.client_config(addr.id, alpn)?);

        // DIAL-3: one handshake per address, `dial_stagger` apart.
        let stagger = shared.config.dial_stagger();
        let deadline = shared.config.handshake_timeout();
        let mut attempts = FuturesUnordered::new();
        for (index, (target, socket)) in targets.into_iter().enumerate() {
            let crypto = crypto.clone();
            // Bounded by the 8 addresses discovery admits per record; the cast can't truncate.
            let delay = stagger * index as u32;
            attempts.push(async move {
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                metrics::counter!("zakura.quic.dial.attempts").increment(1);
                let started = Instant::now();
                let result = self.dial_once(socket, crypto, target, deadline).await;
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
        let error = best.expect("at least one attempt ran because targets was non-empty");
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
    ) -> Result<(ConnRef, Handshake), ConnectError> {
        let probe = Arc::new(CongestionProbe::default());
        let mut client = ClientConfig::new(crypto);
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
        let _ = tokio::time::timeout(SHUTDOWN_DRAIN + Duration::from_secs(1), all).await;
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
        self.dial_once(socket, Arc::new(crypto), target, deadline)
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
        Some(deadline) => match tokio::time::timeout(deadline, established).await {
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
    let result = establish(connecting, deadline).await;
    drop(pending);
    record_handshake(&result, started);
    let Ok((conn, handshake)) = result else {
        return;
    };
    let (Some(remote_id), Some(alpn)) = (handshake.remote_id, handshake.alpn) else {
        let _ = conn.send(crate::driver::ConnCmd::Close {
            code: VarInt::from_u32(0),
            reason: bytes::Bytes::from_static(b"identity"),
        });
        return;
    };
    acceptor
        .handle(Conn::new(conn, remote_id, remote, alpn))
        .await;
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
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
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
    let mut ticker = tokio::time::interval(interval);
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
