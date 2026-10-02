//! Authenticated connections (API-4) and their per-connection monitor
//! (PATH-1 to PATH-5, OBS-1, OBS-3).

use std::{
    collections::BTreeSet,
    fmt,
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use futures::StreamExt as _;
use noq::{ConnectionError, ConnectionStats, PathEvent, PathId, PathStats, VarInt};

use crate::{key::NodeId, sys::canonical_ip};

/// How often the monitor samples stats and re-checks path 0's address.
pub(crate) const SAMPLE_INTERVAL: Duration = Duration::from_secs(10);

/// Decides whether a peer-opened path or a migrated address must close (PATH-2, PATH-3).
pub type BanCheck = Arc<dyn Fn(IpAddr) -> bool + Send + Sync>;

/// One per-connection sample, for the `quic_conn` trace table (OBS-3).
#[derive(Clone, Debug)]
pub struct ConnSample {
    /// The TLS-proven remote node ID.
    pub remote_id: NodeId,
    /// The admitted IP (PATH-4).
    pub admitted_ip: IpAddr,
    /// Path 0's smoothed RTT.
    pub rtt: Duration,
    /// Path 0's congestion window in bytes.
    pub cwnd: u64,
    /// Packets lost on all paths.
    pub lost_packets: u64,
    /// Congestion events on all open paths.
    pub congestion_events: u64,
    /// UDP payload bytes sent.
    pub bytes_sent: u64,
    /// UDP payload bytes received.
    pub bytes_received: u64,
    /// Path 0's current MTU.
    pub current_mtu: u16,
    /// Paths currently open.
    pub paths_open: usize,
    /// The close reason, set only on the final sample.
    pub close_reason: Option<String>,
}

/// Receives per-connection samples every 10 s and on close (OBS-3).
pub type ConnObserver = Arc<dyn Fn(&ConnSample) + Send + Sync>;

/// Connection statistics (OBS-4).
#[derive(Clone, Debug)]
pub struct ConnStats {
    /// noq's connection-wide counters.
    pub connection: ConnectionStats,
    /// Per-path counters for each open path.
    pub paths: Vec<(PathId, PathStats)>,
}

/// An authenticated QUIC connection.
///
/// Cloning shares the connection. Dropping the last clone closes it with
/// application code 0, as noq does (API-4).
#[derive(Clone)]
pub struct Conn {
    inner: noq::Connection,
    remote_id: NodeId,
    admitted_addr: SocketAddr,
    alpn: Arc<[u8]>,
    paths: OpenPaths,
}

impl fmt::Debug for Conn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Conn")
            .field("remote_id", &self.remote_id)
            .field("admitted_addr", &self.admitted_addr)
            .field("alpn", &String::from_utf8_lossy(&self.alpn))
            .finish()
    }
}

impl Conn {
    pub(crate) fn new(
        inner: noq::Connection,
        remote_id: NodeId,
        admitted_addr: SocketAddr,
        alpn: Vec<u8>,
        paths: OpenPaths,
    ) -> Self {
        Self {
            inner,
            remote_id,
            admitted_addr,
            alpn: alpn.into(),
            paths,
        }
    }

    /// The peer's node ID, proven by the TLS handshake (TLS-8).
    pub fn remote_id(&self) -> NodeId {
        self.remote_id
    }

    /// The IP admitted at accept or dial time. It never changes (PATH-4).
    pub fn admitted_ip(&self) -> IpAddr {
        self.admitted_addr.ip()
    }

    /// The socket address admitted at accept or dial time.
    pub fn admitted_addr(&self) -> SocketAddr {
        self.admitted_addr
    }

    /// The negotiated ALPN.
    pub fn alpn(&self) -> &[u8] {
        &self.alpn
    }

    /// Opens a bidirectional stream.
    pub async fn open_bi(&self) -> Result<(noq::SendStream, noq::RecvStream), ConnectionError> {
        self.inner.open_bi().await
    }

    /// Accepts the peer's next bidirectional stream.
    pub async fn accept_bi(&self) -> Result<(noq::SendStream, noq::RecvStream), ConnectionError> {
        self.inner.accept_bi().await
    }

    /// Closes the connection with an application error code and reason.
    pub fn close(&self, code: VarInt, reason: &[u8]) {
        self.inner.close(code, reason);
    }

    /// Resolves when the connection closes, with the reason.
    pub async fn closed(&self) -> ConnectionError {
        self.inner.closed().await
    }

    /// The close reason, if the connection has closed.
    pub fn close_reason(&self) -> Option<ConnectionError> {
        self.inner.close_reason()
    }

    /// Connection and per-path statistics (OBS-4).
    pub fn stats(&self) -> ConnStats {
        let paths = self
            .paths
            .snapshot()
            .into_iter()
            .filter_map(|id| Some((id, self.inner.path_stats(id)?)))
            .collect();
        ConnStats {
            connection: self.inner.stats(),
            paths,
        }
    }

    /// A process-unique ID for this connection.
    pub fn stable_id(&self) -> usize {
        self.inner.stable_id()
    }

    /// The underlying noq connection, for tests that need raw QUIC access.
    #[doc(hidden)]
    pub fn noq(&self) -> &noq::Connection {
        &self.inner
    }
}

/// The set of open path IDs, kept current by the monitor from path events.
#[derive(Clone, Debug)]
pub(crate) struct OpenPaths(Arc<Mutex<BTreeSet<PathId>>>);

impl OpenPaths {
    pub(crate) fn new() -> Self {
        Self(Arc::new(Mutex::new(BTreeSet::from([PathId::ZERO]))))
    }

    fn snapshot(&self) -> Vec<PathId> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .copied()
            .collect()
    }

    fn insert(&self, id: PathId) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(id);
    }

    fn remove(&self, id: PathId) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&id);
    }
}

/// Watches one connection without keeping it alive.
///
/// The monitor holds only a weak handle, a path-event subscription and an
/// `on_closed` future, so dropping every `Conn` still closes the connection.
pub(crate) async fn monitor(
    conn: noq::Connection,
    remote_id: NodeId,
    admitted_ip: IpAddr,
    paths: OpenPaths,
    ban: Option<BanCheck>,
    observer: Option<ConnObserver>,
) {
    let weak = conn.weak_handle();
    let mut events = conn.path_events().boxed();
    let closed = conn.on_closed();
    let mut last_path0 = conn
        .path(PathId::ZERO)
        .and_then(|path| path.remote_address().ok())
        .map(|addr| canonical_ip(addr.ip()))
        .unwrap_or(admitted_ip);
    // Drop the strong handle so the monitor never keeps the connection open.
    drop(conn);
    let mut previous = Counters::default();
    let mut ticker = tokio::time::interval(SAMPLE_INTERVAL);
    ticker.tick().await;

    metrics::gauge!("zakura.quic.connections").increment(1.0);
    tokio::pin!(closed);
    loop {
        tokio::select! {
            closed = &mut closed => {
                let (sample, counters) = final_sample(&closed, remote_id, admitted_ip);
                previous.export_delta(&counters);
                if let Some(observer) = &observer {
                    observer(&sample);
                }
                break;
            }
            event = events.next() => {
                let event = match event {
                    Some(Ok(event)) => event,
                    // Lagged: resynchronize from the next stats sample.
                    Some(Err(_)) => continue,
                    // The connection's state is gone; `closed` resolves next.
                    None => {
                        events = futures::stream::pending().boxed();
                        continue;
                    }
                };
                let Some(conn) = weak.upgrade() else { continue };
                on_path_event(&conn, event, &paths, ban.as_ref());
            }
            _ = ticker.tick() => {
                let Some(conn) = weak.upgrade() else { continue };
                check_path0(&conn, &mut last_path0, ban.as_ref());
                let (sample, counters) = sample(&conn, &paths, remote_id, admitted_ip);
                previous.export_delta(&counters);
                previous = counters;
                if let Some(observer) = &observer {
                    observer(&sample);
                }
            }
        }
    }
    metrics::gauge!("zakura.quic.connections").decrement(1.0);
}

fn on_path_event(
    conn: &noq::Connection,
    event: PathEvent,
    paths: &OpenPaths,
    ban: Option<&BanCheck>,
) {
    match event {
        PathEvent::Abandoned { id, .. } | PathEvent::Discarded { id, .. } => paths.remove(id),
        PathEvent::Established { id, .. } if id != PathId::ZERO => {
            // DIAL-7: Zakura never opens extra paths, so every other path is peer-opened.
            paths.insert(id);
            metrics::counter!("zakura.quic.paths.peer_opened").increment(1);
            let Some(path) = conn.path(id) else { return };
            let Ok(remote) = path.remote_address() else {
                return;
            };
            if ban.is_some_and(|banned| banned(canonical_ip(remote.ip()))) {
                close_banned_path(conn, &path);
            }
        }
        _ => {}
    }
}

/// PATH-2: close a path from a banned IP, or the connection if it's the last path.
fn close_banned_path(conn: &noq::Connection, path: &noq::Path) {
    metrics::counter!("zakura.quic.paths.closed_banned").increment(1);
    tracing::debug!(target: "zakura_quic", path = ?path.id(), "closing a path from a banned IP");
    if let Err(noq::ClosePathError::LastOpenPath) = path.close() {
        conn.close(VarInt::from_u32(0), b"banned path");
    }
}

/// PATH-3: noq emits no event when path 0 migrates, so compare its address.
fn check_path0(conn: &noq::Connection, last: &mut IpAddr, ban: Option<&BanCheck>) {
    let Some(path) = conn.path(PathId::ZERO) else {
        return;
    };
    let Ok(remote) = path.remote_address() else {
        return;
    };
    let ip = canonical_ip(remote.ip());
    if ip != *last {
        *last = ip;
        if ban.is_some_and(|banned| banned(ip)) {
            close_banned_path(conn, &path);
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct Counters {
    lost_packets: u64,
    congestion_events: u64,
    bytes_sent: u64,
    bytes_received: u64,
}

impl Counters {
    /// Exports the growth since `self` as counter increments (OBS-1).
    fn export_delta(&self, next: &Counters) {
        metrics::counter!("zakura.quic.packets.lost")
            .increment(next.lost_packets.saturating_sub(self.lost_packets));
        metrics::counter!("zakura.quic.congestion_events").increment(
            next.congestion_events
                .saturating_sub(self.congestion_events),
        );
        metrics::counter!("zakura.quic.bytes.sent")
            .increment(next.bytes_sent.saturating_sub(self.bytes_sent));
        metrics::counter!("zakura.quic.bytes.received")
            .increment(next.bytes_received.saturating_sub(self.bytes_received));
    }
}

fn sample(
    conn: &noq::Connection,
    open: &OpenPaths,
    remote_id: NodeId,
    admitted_ip: IpAddr,
) -> (ConnSample, Counters) {
    let stats = conn.stats();
    let paths: Vec<PathStats> = open
        .snapshot()
        .into_iter()
        .filter_map(|id| conn.path_stats(id))
        .collect();
    for path in &paths {
        metrics::histogram!("zakura.quic.path.rtt_seconds").record(path.rtt.as_secs_f64());
        // Precision loss above 2^53 bytes doesn't matter for a histogram.
        metrics::histogram!("zakura.quic.path.cwnd_bytes").record(path.cwnd as f64);
    }
    let path0 = conn.path_stats(PathId::ZERO).unwrap_or_default();
    let counters = Counters {
        lost_packets: stats.lost_packets,
        congestion_events: paths.iter().map(|path| path.congestion_events).sum(),
        bytes_sent: stats.udp_tx.bytes,
        bytes_received: stats.udp_rx.bytes,
    };
    let sample = ConnSample {
        remote_id,
        admitted_ip,
        rtt: path0.rtt,
        cwnd: path0.cwnd,
        lost_packets: counters.lost_packets,
        congestion_events: counters.congestion_events,
        bytes_sent: counters.bytes_sent,
        bytes_received: counters.bytes_received,
        current_mtu: path0.current_mtu,
        paths_open: paths.len(),
        close_reason: None,
    };
    (sample, counters)
}

fn final_sample(
    closed: &noq::Closed,
    remote_id: NodeId,
    admitted_ip: IpAddr,
) -> (ConnSample, Counters) {
    let stats = &closed.stats;
    let path0 = closed
        .path_stats
        .iter()
        .find(|(id, _)| *id == PathId::ZERO)
        .map(|(_, stats)| *stats)
        .unwrap_or_default();
    let counters = Counters {
        lost_packets: stats.lost_packets,
        congestion_events: closed
            .path_stats
            .iter()
            .map(|(_, path)| path.congestion_events)
            .sum(),
        bytes_sent: stats.udp_tx.bytes,
        bytes_received: stats.udp_rx.bytes,
    };
    let sample = ConnSample {
        remote_id,
        admitted_ip,
        rtt: path0.rtt,
        cwnd: path0.cwnd,
        lost_packets: counters.lost_packets,
        congestion_events: counters.congestion_events,
        bytes_sent: counters.bytes_sent,
        bytes_received: counters.bytes_received,
        current_mtu: path0.current_mtu,
        paths_open: 0,
        close_reason: Some(closed.reason.to_string()),
    };
    (sample, counters)
}
